use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use slab::Slab;
use tracing::metadata::LevelFilter;
use tracing::{Level, Metadata};

// ── Interest ────────────────────────────────────────────────────────────────

pub enum Interest {
    Never,
    Sometimes,
    Always,
}

// ── EnabledPredicate ────────────────────────────────────────────────────────

pub trait EnabledPredicate: Send + Sync + 'static {
    fn max_level_hint(&self) -> Option<LevelFilter>;
    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest;
    fn enabled(&self, metadata: &Metadata<'_>) -> bool;
    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool;
}

// ── LevelPredicate ──────────────────────────────────────────────────────────

pub struct LevelPredicate {
    level: Level,
}

impl LevelPredicate {
    pub fn new(level: Level) -> Self {
        Self { level }
    }
}

impl EnabledPredicate for LevelPredicate {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::from_level(self.level))
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.level() <= &self.level {
            Interest::Always
        } else {
            Interest::Never
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= &self.level
    }

    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
        span.metadata().level() <= &self.level
    }
}

// ── EventRecord ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct EventRecord {
    pub metadata: &'static Metadata<'static>,
    pub fields: HashMap<&'static str, String>,
    pub recorded_at: Instant,
}

// ── SpanRecord ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SpanRecord {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub metadata: &'static Metadata<'static>,
    pub fields: HashMap<&'static str, String>,
    pub events: Vec<EventRecord>,
    pub opened_at: Instant,
    pub closed_at: Option<Instant>,
}

// ── SpanCache & Driver ───────────────────────────────────────────────────────

// Two disjoint id namespaces share the same u64 carried by `tracing::span::Id`:
//
//   1                       — DISABLED sentinel
//   [2, ID_THRESHOLD)       — active tracing id (slab_idx + 2), assigned at
//                             new_span and used in SPAN_STACK / try_close /
//                             record / event.  Reused as slabs are reclaimed.
//   [ID_THRESHOLD, u64::MAX)— actual_id (from AtomicU64), monotonic.  Stored
//                             in `SpanRecord.id` and `parent_id`; used as the
//                             BTreeMap key after a span closes.
//
// Disjoint ranges let `get_span(id)` dispatch by value without a separate flag.
const DISABLED: u64 = 1;
const ID_THRESHOLD: u64 = 1 << 32;

// Flush the thread-local PENDING buffer to the spillway after this many closes.
const PENDING_BATCH: usize = 32;

thread_local! {
    // Active tracing ids on this thread — DISABLED or `slab_idx + 2`.
    static SPAN_STACK: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
    // Closed spans waiting to be sent to the driver via spillway.
    static PENDING: std::cell::RefCell<Vec<SpanRecord>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn stack_top() -> Option<u64> {
    SPAN_STACK.with(|s| s.borrow().last().copied())
}

fn id_to_u64(id: &tracing::span::Id) -> u64 {
    id.into_u64()
}

fn u64_to_id(n: u64) -> tracing::span::Id {
    tracing::span::Id::from_u64(n)
}

/// Encode a slab index as the u64 carried by `tracing::span::Id`.
fn slab_to_tracing(idx: usize) -> u64 {
    (idx as u64) + 2
}

/// Decode a tracing id back to a slab index.  Returns `None` for `DISABLED`,
/// for an actual_id (>= `ID_THRESHOLD`), or for the unused value `0`.
fn tracing_to_slab(id: u64) -> Option<usize> {
    if id < 2 || id >= ID_THRESHOLD {
        None
    } else {
        Some((id - 2) as usize)
    }
}

/// A `tracing::Subscriber` that holds spans in memory for inspection.
///
/// In-flight spans live in a `RwLock<Slab>`.  When a span closes it moves to
/// a per-thread buffer; the buffer is flushed to the [`Driver`] via a spillway
/// channel, keeping the hot path free of shared-map write contention.
///
/// Create with [`SpanCache::new`] or [`SpanCache::with_predicate`]; both return
/// `(SpanCache, Driver)`.  Spawn the [`Driver`] as a background task to commit
/// closed spans to the shared readable map.
pub struct SpanCache<P: EnabledPredicate = LevelPredicate> {
    in_flight: RwLock<Slab<SpanRecord>>,
    map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    // Generates the actual_id stored in `SpanRecord.id`.  Starts at
    // `ID_THRESHOLD` so actual ids never collide with slab-encoded tracing ids.
    next_actual_id: AtomicU64,
    predicate: P,
    capacity: usize,
    sender: spillway::Sender<SpanRecord>,
}

/// Background task that receives closed spans from the spillway and writes
/// them to the shared `BTreeMap` in batches.
pub struct Driver {
    map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    receiver: spillway::Receiver<SpanRecord>,
    capacity: usize,
}

impl SpanCache<LevelPredicate> {
    pub fn new(capacity: usize) -> (Self, Driver) {
        Self::with_predicate(capacity, LevelPredicate::new(Level::TRACE))
    }
}

impl<P: EnabledPredicate> SpanCache<P> {
    pub fn with_predicate(capacity: usize, predicate: P) -> (Self, Driver) {
        let (sender, receiver) = spillway::channel();
        let map = Arc::new(RwLock::new(BTreeMap::new()));
        let cache = SpanCache {
            in_flight: RwLock::new(Slab::with_capacity(capacity)),
            map: Arc::clone(&map),
            next_actual_id: AtomicU64::new(ID_THRESHOLD),
            predicate,
            capacity,
            sender,
        };
        let driver = Driver { map, receiver, capacity };
        (cache, driver)
    }

    /// Returns a span by either its active tracing id (while in-flight, fast
    /// slab lookup) or its actual id (after closure, BTreeMap lookup).  The
    /// two id namespaces are disjoint by value, so a single argument works
    /// for both — see the comment on `ID_THRESHOLD`.
    pub fn get_span(&self, id: u64) -> Option<SpanRecord> {
        if id == DISABLED {
            return None;
        }
        if id >= ID_THRESHOLD {
            return self.map.read().unwrap().get(&id).cloned();
        }
        let slab_idx = tracing_to_slab(id)?;
        self.in_flight.read().unwrap().get(slab_idx).cloned()
    }

    /// Returns closed spans in ascending actual_id order.  Open spans are not
    /// included; call [`flush_pending`] + [`Driver::drain_sync`] first if you
    /// need just-closed spans to appear.
    pub fn page(&self, after_id: u64, limit: usize) -> Vec<SpanRecord> {
        let map = self.map.read().unwrap();
        if after_id == 0 {
            map.values().take(limit).cloned().collect()
        } else {
            map.range((Excluded(after_id), Unbounded))
                .take(limit)
                .map(|(_, v)| v.clone())
                .collect()
        }
    }

    /// Drains the calling thread's PENDING buffer into the spillway channel.
    /// Must be called before [`Driver::drain_sync`] in tests to ensure all
    /// recently-closed spans are delivered.
    pub fn flush_pending(&self) {
        PENDING.with(|p| {
            for record in p.borrow_mut().drain(..) {
                let _ = self.sender.send(record);
            }
        });
    }
}

// ── Subscriber impl ──────────────────────────────────────────────────────────

impl<P: EnabledPredicate> tracing::Subscriber for SpanCache<P> {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        self.predicate.max_level_hint()
    }

    fn register_callsite(
        &self,
        metadata: &'static Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        match self.predicate.callsite_enabled(metadata) {
            Interest::Never => tracing::subscriber::Interest::never(),
            Interest::Sometimes => tracing::subscriber::Interest::sometimes(),
            Interest::Always => tracing::subscriber::Interest::always(),
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        if stack_top() == Some(DISABLED) {
            return false;
        }
        self.predicate.enabled(metadata)
    }

    fn event_enabled(&self, event: &tracing::Event<'_>) -> bool {
        self.predicate.enabled(event.metadata())
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let disabled_id = || u64_to_id(DISABLED);

        // Step A: validate the parent context using the SPAN_STACK / explicit
        // parent.  We only have a slab_idx here — translating to the parent's
        // actual_id needs the in_flight lock, deferred to step D.
        enum ParentSource {
            Root,
            ParentSlab(usize),
        }
        let source = if attrs.is_contextual() {
            match stack_top() {
                None | Some(DISABLED) => return disabled_id(),
                Some(top) => match tracing_to_slab(top) {
                    Some(idx) => ParentSource::ParentSlab(idx),
                    None => return disabled_id(),
                },
            }
        } else if attrs.is_root() {
            if stack_top().is_some() {
                log::warn!("root span created with an active span on the stack; disabling");
                return disabled_id();
            }
            ParentSource::Root
        } else {
            let explicit = id_to_u64(attrs.parent().unwrap());
            if explicit == DISABLED {
                return disabled_id();
            }
            match tracing_to_slab(explicit) {
                Some(idx) => ParentSource::ParentSlab(idx),
                None => return disabled_id(),
            }
        };

        // Step B: predicate check.
        if !self.predicate.new_span_enabled(attrs) {
            return disabled_id();
        }

        // Step C: build the record outside the lock so field-visitor work
        // doesn't extend the critical section.
        let actual_id = self.next_actual_id.fetch_add(1, Relaxed);
        let mut record = SpanRecord {
            id: actual_id,
            parent_id: None,
            metadata: attrs.metadata(),
            fields: HashMap::new(),
            events: Vec::new(),
            opened_at: Instant::now(),
            closed_at: None,
        };
        attrs.record(&mut FieldVisitor { fields: &mut record.fields });

        // Step D: single-lock — translate parent's slab_idx to its actual_id,
        // capacity-check, and insert.
        let mut in_flight = self.in_flight.write().unwrap();
        record.parent_id = match source {
            ParentSource::Root => None,
            ParentSource::ParentSlab(idx) => match in_flight.get(idx) {
                Some(parent) => Some(parent.id),
                None => return disabled_id(), // parent vanished — race
            },
        };
        if in_flight.len() >= self.capacity {
            log::warn!(
                "span buffer full; new span disabled. \
                 Increase capacity or reduce span rate."
            );
            return disabled_id();
        }
        let slab_idx = in_flight.insert(record);
        u64_to_id(slab_to_tracing(slab_idx))
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let slab_idx = match tracing_to_slab(id_to_u64(span)) {
            Some(i) => i,
            None => return,
        };
        let mut in_flight = self.in_flight.write().unwrap();
        if let Some(rec) = in_flight.get_mut(slab_idx) {
            values.record(&mut FieldVisitor { fields: &mut rec.fields });
        }
    }

    fn record_follows_from(
        &self,
        _span: &tracing::span::Id,
        _follows: &tracing::span::Id,
    ) {
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let parent_top = event.parent().map(id_to_u64).or_else(stack_top);
        let parent_top = match parent_top {
            Some(id) if id != DISABLED => id,
            Some(_) => {
                log::debug!("event dropped: parent span is disabled");
                return;
            }
            None => {
                log::debug!("event dropped: no active span");
                return;
            }
        };
        let parent_idx = match tracing_to_slab(parent_top) {
            Some(i) => i,
            None => return,
        };

        let mut fields = HashMap::new();
        event.record(&mut FieldVisitor { fields: &mut fields });
        let record = EventRecord {
            metadata: event.metadata(),
            fields,
            recorded_at: Instant::now(),
        };

        let mut in_flight = self.in_flight.write().unwrap();
        if let Some(span) = in_flight.get_mut(parent_idx) {
            span.events.push(record);
        } else {
            log::debug!("event dropped: parent span at slab {} not in cache", parent_idx);
        }
    }

    fn enter(&self, span: &tracing::span::Id) {
        SPAN_STACK.with(|s| s.borrow_mut().push(id_to_u64(span)));
    }

    fn exit(&self, _span: &tracing::span::Id) {
        SPAN_STACK.with(|s| {
            s.borrow_mut().pop();
        });
    }

    fn try_close(&self, id: tracing::span::Id) -> bool {
        let slab_idx = match tracing_to_slab(id_to_u64(&id)) {
            Some(i) => i,
            None => return false,
        };

        let record = {
            let mut in_flight = self.in_flight.write().unwrap();
            if in_flight.contains(slab_idx) {
                let mut r = in_flight.remove(slab_idx);
                r.closed_at = Some(Instant::now());
                Some(r)
            } else {
                None
            }
        };

        if let Some(record) = record {
            let should_flush = PENDING.with(|p| {
                let mut p = p.borrow_mut();
                p.push(record);
                p.len() >= PENDING_BATCH
            });
            if should_flush {
                self.flush_pending();
            }
            true
        } else {
            false
        }
    }
}

// ── Driver ───────────────────────────────────────────────────────────────────

impl Driver {
    /// Runs the driver loop.  Flushes the accumulated batch into the shared
    /// map whenever 600 spans accumulate or one second elapses.
    ///
    /// Terminates when all `Sender` clones are dropped (spillway channel closed).
    pub async fn run(self) {
        let Driver { map, mut receiver, capacity } = self;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut batch: Vec<SpanRecord> = Vec::new();

        loop {
            tokio::select! {
                record = receiver.next() => {
                    match record {
                        Some(r) => {
                            batch.push(r);
                            if batch.len() >= 600 {
                                Self::flush_batch(&map, capacity, &mut batch);
                            }
                        }
                        None => {
                            // All senders dropped; flush remaining and exit.
                            Self::flush_batch(&map, capacity, &mut batch);
                            break;
                        }
                    }
                }
                _ = tick.tick() => {
                    Self::flush_batch(&map, capacity, &mut batch);
                }
            }
        }
    }

    /// Synchronously drains all spans currently available in the spillway and
    /// flushes them into the map.  Use in tests after [`SpanCache::flush_pending`].
    pub fn drain_sync(self) {
        let Driver { map, mut receiver, capacity } = self;
        let mut batch = Vec::new();
        while let Some(record) = receiver.try_next() {
            batch.push(record);
        }
        Self::flush_batch(&map, capacity, &mut batch);
    }

    fn flush_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        capacity: usize,
        batch: &mut Vec<SpanRecord>,
    ) {
        if batch.is_empty() {
            return;
        }
        // Only closed spans are ever sent to the driver, so all entries in the
        // map are already closed — pop_first() always evicts a finished span.
        let mut m = map.write().unwrap();
        for record in batch.drain(..) {
            if m.len() >= capacity {
                m.pop_first();
            }
            m.insert(record.id, record);
        }
    }
}

// ── FieldVisitor ─────────────────────────────────────────────────────────────

struct FieldVisitor<'a> {
    fields: &'a mut HashMap<&'static str, String>,
}

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(field.name(), format!("{:?}", value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields.insert(field.name(), format!("{}", value));
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tracing::Level;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_cache(capacity: usize) -> (Arc<SpanCache>, Driver) {
        let (cache, driver) = SpanCache::new(capacity);
        (Arc::new(cache), driver)
    }

    /// Runs `f` under `cache` as the active subscriber, then flushes and drains
    /// so all closed spans are committed to the map before returning.
    fn run_with_drain<F, T>(cache: &Arc<SpanCache>, driver: Driver, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let result = tracing::subscriber::with_default(Arc::clone(cache), f);
        cache.flush_pending();
        driver.drain_sync();
        result
    }

    fn span_id(span: &tracing::Span) -> Option<u64> {
        span.id().map(|id| id.into_u64())
    }

    /// Captures the actual_id of an in-flight span — required to look it up
    /// in the closed-span map after drain (the tracing id and actual id live
    /// in disjoint namespaces).
    fn actual_id_of(cache: &Arc<SpanCache>, span: &tracing::Span) -> u64 {
        cache.get_span(span_id(span).unwrap()).unwrap().id
    }

    struct DisableByName(pub &'static str);

    impl EnabledPredicate for DisableByName {
        fn max_level_hint(&self) -> Option<LevelFilter> {
            None
        }
        fn callsite_enabled(&self, _: &'static Metadata<'static>) -> Interest {
            Interest::Sometimes
        }
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
            span.metadata().name() != self.0
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    #[test]
    fn basic_span_creation_and_retrieval() {
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(parent: None, Level::INFO, "root", field = "value");
            let actual_id = actual_id_of(&cache, &span);
            let _g = span.enter();
            actual_id
        });
        let record = cache.get_span(actual_id).unwrap();
        assert_eq!(record.id, actual_id);
        assert_eq!(record.metadata.name(), "root");
        assert_eq!(record.fields.get("field").map(String::as_str), Some("value"));
    }

    #[test]
    fn closed_at_set_after_drop() {
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(parent: None, Level::INFO, "root");
            let tracing_id = span_id(&span).unwrap();
            let actual_id = cache.get_span(tracing_id).unwrap().id;
            {
                let _g = span.enter();
            }
            // While alive: lookup by tracing id finds the slab entry, closed_at is None.
            assert!(
                cache.get_span(tracing_id).unwrap().closed_at.is_none(),
                "not closed while span is alive"
            );
            actual_id
            // span drops here → try_close → PENDING
        });
        // After drain: lookup by actual_id finds the BTreeMap entry.
        assert!(
            cache.get_span(actual_id).unwrap().closed_at.is_some(),
            "should be closed after Span drops"
        );
    }

    #[test]
    fn child_of_disabled_is_disabled() {
        let (cache_inner, driver) = SpanCache::with_predicate(10, DisableByName("bad_parent"));
        let cache = Arc::new(cache_inner);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let parent = tracing::span!(parent: None, Level::INFO, "bad_parent");
            assert_eq!(span_id(&parent), Some(DISABLED), "predicate disables this span");
            let _g = parent.enter(); // pushes DISABLED onto thread-local stack
            let child = tracing::span!(Level::INFO, "child");
            assert_eq!(child.id(), None, "child of DISABLED is a tracing no-op");
        });
        drop(driver);
    }

    #[test]
    fn contextual_span_with_empty_stack_is_disabled() {
        let (cache, driver) = make_cache(10);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let span = tracing::span!(Level::INFO, "contextual");
            assert_eq!(span_id(&span), Some(DISABLED));
        });
        drop(driver);
    }

    #[test]
    fn root_span_with_active_stack_is_disabled() {
        let (cache, driver) = make_cache(10);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let root_a = tracing::span!(parent: None, Level::INFO, "root_a");
            let _g = root_a.enter();
            let root_b = tracing::span!(parent: None, Level::INFO, "root_b");
            assert_eq!(span_id(&root_b), Some(DISABLED));
        });
        drop(driver);
    }

    #[test]
    fn eviction_removes_closed_spans() {
        let (cache, driver) = make_cache(2);
        let (a, b, c) = run_with_drain(&cache, driver, || {
            let span_a = tracing::span!(parent: None, Level::INFO, "a");
            let a = actual_id_of(&cache, &span_a);
            let span_b = tracing::span!(parent: None, Level::INFO, "b");
            let b = actual_id_of(&cache, &span_b);
            drop(span_a);
            drop(span_b);
            // in_flight is now empty; C is allowed.
            let span_c = tracing::span!(parent: None, Level::INFO, "c");
            assert_ne!(span_id(&span_c), Some(DISABLED), "C should be enabled");
            let c = actual_id_of(&cache, &span_c);
            (a, b, c)
        });
        // Driver inserted A, B, then C: capacity=2, so A was evicted when C was inserted.
        assert!(cache.get_span(a).is_none(), "A should have been evicted");
        assert!(cache.get_span(b).is_some(), "B should still be in cache");
        assert!(cache.get_span(c).is_some(), "C should be in cache");
        let page_ids: Vec<u64> = cache.page(0, 10).iter().map(|s| s.id).collect();
        assert!(!page_ids.contains(&a));
        assert!(page_ids.contains(&b));
        assert!(page_ids.contains(&c));
    }

    #[test]
    fn eviction_full_of_open_spans_returns_disabled() {
        let (cache, driver) = make_cache(2);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let span_a = tracing::span!(parent: None, Level::INFO, "a");
            let span_b = tracing::span!(parent: None, Level::INFO, "b");
            assert_ne!(span_id(&span_a), Some(DISABLED));
            assert_ne!(span_id(&span_b), Some(DISABLED));
            // Both are in in_flight; capacity reached → C is disabled.
            let span_c = tracing::span!(parent: None, Level::INFO, "c");
            assert_eq!(span_id(&span_c), Some(DISABLED));
        });
        drop(driver);
    }

    #[test]
    fn pagination() {
        let (cache, driver) = make_cache(10);
        let ids: Vec<u64> = run_with_drain(&cache, driver, || {
            let mut ids = Vec::new();
            for _ in 0..5usize {
                let span = tracing::span!(parent: None, Level::INFO, "s");
                ids.push(actual_id_of(&cache, &span));
                // span drops here → closed via try_close → PENDING
            }
            ids
        });
        assert_eq!(ids.len(), 5);

        let p1 = cache.page(0, 3);
        assert_eq!(p1.len(), 3);
        assert_eq!(p1[0].id, ids[0]);
        assert_eq!(p1[2].id, ids[2]);

        let last = p1.last().unwrap().id;
        let p2 = cache.page(last, 3);
        assert_eq!(p2.len(), 2);
        assert_eq!(p2[0].id, ids[3]);
        assert_eq!(p2[1].id, ids[4]);

        assert!(cache.page(ids[4] + 1000, 3).is_empty());
    }

    #[test]
    fn event_attached_to_current_span() {
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(parent: None, Level::INFO, "root");
            let actual_id = actual_id_of(&cache, &span);
            let _g = span.enter();
            tracing::event!(Level::INFO, "test event happened");
            actual_id
        });
        let record = cache.get_span(actual_id).unwrap();
        assert_eq!(record.events.len(), 1);
        assert!(
            record.events[0].fields.contains_key("message"),
            "event should have a message field"
        );
    }

    #[test]
    fn event_dropped_with_no_active_span() {
        let (cache, driver) = make_cache(10);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            tracing::event!(Level::INFO, "orphan event");
        });
        drop(driver);
        assert!(cache.page(0, 10).is_empty());
    }

    #[test]
    fn field_capture() {
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(
                parent: None,
                Level::INFO,
                "fields",
                str_field = "hello",
                int_field = 42i64,
                bool_field = true,
            );
            actual_id_of(&cache, &span)
        });
        let record = cache.get_span(actual_id).unwrap();
        assert_eq!(record.fields.get("str_field").map(String::as_str), Some("hello"));
        assert_eq!(record.fields.get("int_field").map(String::as_str), Some("42"));
        assert_eq!(record.fields.get("bool_field").map(String::as_str), Some("true"));
    }

    // ── async overlap test ────────────────────────────────────────────────────

    #[test]
    fn async_instrumented_tasks_with_overlapping_spans() {
        use tracing_futures::Instrument;

        let (cache, driver) = make_cache(20);

        tracing::subscriber::with_default(Arc::clone(&cache), || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async {
                    let sem = Arc::new(tokio::sync::Semaphore::new(0));
                    let sem_a = Arc::clone(&sem);
                    let sem_b = Arc::clone(&sem);

                    let root_a = tracing::span!(parent: None, Level::INFO, "task_a");
                    let root_b = tracing::span!(parent: None, Level::INFO, "task_b");

                    let h_a = tokio::spawn(
                        async move {
                            async move {
                                sem_a.acquire().await.unwrap().forget();
                            }
                            .instrument(tracing::span!(Level::INFO, "acquire"))
                            .await;
                        }
                        .instrument(root_a),
                    );

                    let h_b = tokio::spawn(
                        async move {
                            async move {
                                sem_b.add_permits(1);
                            }
                            .instrument(tracing::span!(Level::INFO, "release"))
                            .await;
                        }
                        .instrument(root_b),
                    );

                    h_a.await.unwrap();
                    h_b.await.unwrap();
                });
        });

        cache.flush_pending();
        driver.drain_sync();

        let all = cache.page(0, 20);
        assert_eq!(all.len(), 4, "task_a, acquire, task_b, release");
        assert!(all.iter().all(|s| s.closed_at.is_some()), "all spans must close");

        let find = |name: &str| all.iter().find(|s| s.metadata.name() == name).unwrap();
        let task_a = find("task_a");
        let task_b = find("task_b");
        let acquire = find("acquire");
        let release = find("release");

        assert_eq!(acquire.parent_id, Some(task_a.id), "acquire is child of task_a");
        assert_eq!(release.parent_id, Some(task_b.id), "release is child of task_b");

        assert!(
            acquire.opened_at < release.closed_at.unwrap(),
            "acquire started before release ended"
        );
        assert!(
            release.opened_at < acquire.closed_at.unwrap(),
            "release started before acquire closed"
        );
    }
}
