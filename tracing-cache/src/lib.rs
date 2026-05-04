use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

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

impl SpanRecord {
    fn is_closed(&self) -> bool {
        self.closed_at.is_some()
    }
}

// ── State & SpanCache ────────────────────────────────────────────────────────

struct State {
    spans: BTreeMap<u64, SpanRecord>,
}

pub struct SpanCache<P: EnabledPredicate = LevelPredicate> {
    state: Mutex<State>,
    next_id: AtomicU64,
    predicate: P,
    capacity: usize,
}

impl SpanCache<LevelPredicate> {
    pub fn new(capacity: usize) -> Self {
        Self::with_predicate(capacity, LevelPredicate::new(Level::TRACE))
    }
}

impl<P: EnabledPredicate> SpanCache<P> {
    pub fn with_predicate(capacity: usize, predicate: P) -> Self {
        SpanCache {
            state: Mutex::new(State { spans: BTreeMap::new() }),
            next_id: AtomicU64::new(10),
            predicate,
            capacity,
        }
    }

    pub fn get_span(&self, id: u64) -> Option<SpanRecord> {
        self.state.lock().unwrap().spans.get(&id).cloned()
    }

    pub fn page(&self, after_id: u64, limit: usize) -> Vec<SpanRecord> {
        let state = self.state.lock().unwrap();
        if after_id == 0 {
            state.spans.values().take(limit).cloned().collect()
        } else {
            state
                .spans
                .range((Excluded(after_id), Unbounded))
                .take(limit)
                .map(|(_, v)| v.clone())
                .collect()
        }
    }
}

// ── Thread-local span stack ──────────────────────────────────────────────────

const DISABLED: u64 = 1;

thread_local! {
    static SPAN_STACK: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
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

        // Step A: resolve parent_id and check for DISABLED propagation.
        let parent_id: Option<u64>;

        if attrs.is_contextual() {
            match stack_top() {
                None | Some(DISABLED) => return disabled_id(),
                Some(id) => parent_id = Some(id),
            }
        } else if attrs.is_root() {
            if stack_top().is_some() {
                log::warn!("root span created with an active span on the stack; disabling");
                return disabled_id();
            }
            parent_id = None;
        } else {
            // Explicit parent.
            let explicit = id_to_u64(attrs.parent().unwrap());
            if explicit == DISABLED {
                return disabled_id();
            }
            parent_id = Some(explicit);
        }

        // Step B: predicate check.
        if !self.predicate.new_span_enabled(attrs) {
            return disabled_id();
        }

        // Step C: generate ID and insert with eviction.
        let id = self.next_id.fetch_add(1, Relaxed);
        let mut state = self.state.lock().unwrap();

        while self.capacity <= state.spans.len() {
            let oldest_closed = state
                .spans
                .first_key_value()
                .map(|(_, s)| s.is_closed())
                .unwrap_or(false);
            if oldest_closed {
                state.spans.pop_first();
            } else {
                log::warn!(
                    "span buffer full; new span disabled. \
                     Increase capacity or reduce span rate."
                );
                return disabled_id();
            }
        }

        state.spans.insert(
            id,
            SpanRecord {
                id,
                parent_id,
                metadata: attrs.metadata(),
                fields: HashMap::new(),
                events: Vec::new(),
                opened_at: Instant::now(),
                closed_at: None,
            },
        );

        // Capture fields supplied at creation time.
        let span = state.spans.get_mut(&id).unwrap();
        let mut visitor = FieldVisitor { fields: &mut span.fields };
        attrs.record(&mut visitor);

        u64_to_id(id)
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let id = id_to_u64(span);
        if id == DISABLED {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if let Some(rec) = state.spans.get_mut(&id) {
            let mut visitor = FieldVisitor { fields: &mut rec.fields };
            values.record(&mut visitor);
        }
    }

    fn record_follows_from(
        &self,
        _span: &tracing::span::Id,
        _follows: &tracing::span::Id,
    ) {
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let parent_id = event.parent().map(id_to_u64).or_else(stack_top);

        let parent_id = match parent_id {
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

        let mut fields = HashMap::new();
        event.record(&mut FieldVisitor { fields: &mut fields });

        let record = EventRecord {
            metadata: event.metadata(),
            fields,
            recorded_at: Instant::now(),
        };

        let mut state = self.state.lock().unwrap();
        if let Some(span) = state.spans.get_mut(&parent_id) {
            span.events.push(record);
        } else {
            log::debug!("event dropped: parent span {} not in cache", parent_id);
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
        let id = id_to_u64(&id);
        if id == DISABLED {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        if let Some(rec) = state.spans.get_mut(&id) {
            if rec.closed_at.is_none() {
                rec.closed_at = Some(Instant::now());
            }
            true
        } else {
            false
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

    fn run<F, T>(cache: &Arc<SpanCache>, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        tracing::subscriber::with_default(Arc::clone(cache), f)
    }

    fn span_id(span: &tracing::Span) -> Option<u64> {
        span.id().map(|id| id.into_u64())
    }

    // Custom predicate: returns Sometimes for callsite_enabled so that
    // enabled() is consulted per-call. Disables spans whose name matches.
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
        let cache = Arc::new(SpanCache::new(10));
        let id = run(&cache, || {
            let span = tracing::span!(parent: None, Level::INFO, "root", field = "value");
            let id = span_id(&span).unwrap();
            let _g = span.enter();
            id
        });
        let record = cache.get_span(id).unwrap();
        assert_eq!(record.id, id);
        assert_eq!(record.metadata.name(), "root");
        assert_eq!(record.fields.get("field").map(String::as_str), Some("value"));
    }

    #[test]
    fn closed_at_set_after_drop() {
        let cache = Arc::new(SpanCache::new(10));
        let id = run(&cache, || {
            let span = tracing::span!(parent: None, Level::INFO, "root");
            let id = span_id(&span).unwrap();
            {
                let _g = span.enter();
            }
            assert!(
                cache.get_span(id).unwrap().closed_at.is_none(),
                "not closed while span is alive"
            );
            id
            // span drops here inside with_default → try_close called on our subscriber
        });
        assert!(
            cache.get_span(id).unwrap().closed_at.is_some(),
            "should be closed after Span drops"
        );
    }

    #[test]
    fn child_of_disabled_is_disabled() {
        // DisableByName returns Sometimes from callsite_enabled so enabled() is
        // called each time. When the parent is on the stack as DISABLED, enabled()
        // returns false and tracing produces a no-op Span (id = None).
        let cache = Arc::new(SpanCache::with_predicate(10, DisableByName("bad_parent")));
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let parent = tracing::span!(parent: None, Level::INFO, "bad_parent");
            assert_eq!(span_id(&parent), Some(DISABLED), "predicate disables this span");
            let _g = parent.enter(); // pushes DISABLED onto thread-local stack
            let child = tracing::span!(Level::INFO, "child");
            assert_eq!(child.id(), None, "child of DISABLED is a tracing no-op");
        });
    }

    #[test]
    fn contextual_span_with_empty_stack_is_disabled() {
        let cache = Arc::new(SpanCache::new(10));
        run(&cache, || {
            // No spans on the stack → contextual span is disabled.
            let span = tracing::span!(Level::INFO, "contextual");
            assert_eq!(span_id(&span), Some(DISABLED));
        });
    }

    #[test]
    fn root_span_with_active_stack_is_disabled() {
        let cache = Arc::new(SpanCache::new(10));
        run(&cache, || {
            let root_a = tracing::span!(parent: None, Level::INFO, "root_a");
            let _g = root_a.enter();
            let root_b = tracing::span!(parent: None, Level::INFO, "root_b");
            assert_eq!(span_id(&root_b), Some(DISABLED));
        });
    }

    #[test]
    fn eviction_removes_closed_spans() {
        let cache = Arc::new(SpanCache::new(2));
        let (id_a, id_b, id_c) = run(&cache, || {
            let span_a = tracing::span!(parent: None, Level::INFO, "a");
            let id_a = span_id(&span_a).unwrap();
            let span_b = tracing::span!(parent: None, Level::INFO, "b");
            let id_b = span_b.id().map(|id| id.into_u64()).unwrap();
            drop(span_a); // closes A
            drop(span_b); // closes B
            // Buffer is full (2/2), both closed. Creating C evicts A (oldest).
            let span_c = tracing::span!(parent: None, Level::INFO, "c");
            let id_c = span_id(&span_c).unwrap();
            assert_ne!(id_c, DISABLED, "C should be enabled after evicting A");
            (id_a, id_b, id_c)
        });
        assert!(cache.get_span(id_a).is_none(), "A should have been evicted");
        assert!(cache.get_span(id_b).is_some(), "B should still be in cache");
        assert!(cache.get_span(id_c).is_some(), "C should be in cache");
        let page_ids: Vec<u64> = cache.page(0, 10).iter().map(|s| s.id).collect();
        assert!(!page_ids.contains(&id_a));
        assert!(page_ids.contains(&id_b));
        assert!(page_ids.contains(&id_c));
    }

    #[test]
    fn eviction_full_of_open_spans_returns_disabled() {
        let cache = Arc::new(SpanCache::new(2));
        run(&cache, || {
            // Create two root spans without entering them (so they stay open).
            let span_a = tracing::span!(parent: None, Level::INFO, "a");
            let span_b = tracing::span!(parent: None, Level::INFO, "b");
            assert_ne!(span_id(&span_a), Some(DISABLED));
            assert_ne!(span_id(&span_b), Some(DISABLED));
            // Buffer full, no closed spans to evict → C is DISABLED.
            let span_c = tracing::span!(parent: None, Level::INFO, "c");
            assert_eq!(span_id(&span_c), Some(DISABLED));
        });
    }

    #[test]
    fn pagination() {
        let cache = Arc::new(SpanCache::new(10));
        let ids: Vec<u64> = run(&cache, || {
            let mut ids = Vec::new();
            for _ in 0..5usize {
                let span = tracing::span!(parent: None, Level::INFO, "s");
                ids.push(span_id(&span).unwrap());
                // span drops at end of loop body → closed via try_close
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
        let cache = Arc::new(SpanCache::new(10));
        let id = run(&cache, || {
            let span = tracing::span!(parent: None, Level::INFO, "root");
            let id = span_id(&span).unwrap();
            let _g = span.enter();
            tracing::event!(Level::INFO, "test event happened");
            id
        });
        let record = cache.get_span(id).unwrap();
        assert_eq!(record.events.len(), 1);
        assert!(
            record.events[0].fields.contains_key("message"),
            "event should have a message field"
        );
    }

    #[test]
    fn event_dropped_with_no_active_span() {
        let cache = Arc::new(SpanCache::new(10));
        run(&cache, || {
            // No active span — event is silently dropped, no panic.
            tracing::event!(Level::INFO, "orphan event");
        });
        assert!(cache.page(0, 10).is_empty());
    }

    #[test]
    fn field_capture() {
        let cache = Arc::new(SpanCache::new(10));
        let id = run(&cache, || {
            let span = tracing::span!(
                parent: None,
                Level::INFO,
                "fields",
                str_field = "hello",
                int_field = 42i64,
                bool_field = true,
            );
            span_id(&span).unwrap()
        });
        let record = cache.get_span(id).unwrap();
        assert_eq!(record.fields.get("str_field").map(String::as_str), Some("hello"));
        assert_eq!(record.fields.get("int_field").map(String::as_str), Some("42"));
        assert_eq!(record.fields.get("bool_field").map(String::as_str), Some("true"));
    }

    // ── async overlap test ────────────────────────────────────────────────────

    #[test]
    fn async_instrumented_tasks_with_overlapping_spans() {
        // tracing_futures::Instrument enters/exits the span on every poll, so the
        // thread-local stack is always clean between suspension points even on a
        // single-threaded executor.
        use tracing_futures::Instrument;

        let cache = Arc::new(SpanCache::new(20));
        let sem = Arc::new(tokio::sync::Semaphore::new(0));

        tracing::subscriber::with_default(Arc::clone(&cache), || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async {
                    let sem_a = Arc::clone(&sem);
                    let sem_b = Arc::clone(&sem);

                    // Root spans are created here, before the tasks run.
                    // They are root spans (parent: None) so our subscriber accepts them
                    // even with an empty stack.
                    let root_a = tracing::span!(parent: None, Level::INFO, "task_a");
                    let root_b = tracing::span!(parent: None, Level::INFO, "task_b");

                    let h_a = tokio::spawn(
                        async move {
                            // root_a is entered by the outer .instrument() on every poll.
                            // The subspan is created here, inside that entry, so it sees
                            // root_a on the stack and gets parent_id = root_a.id.
                            async move {
                                // Suspends until task_b adds a permit.
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
                                // Unblocks task_a. Both "acquire" and "release" spans are
                                // open in the cache right now — they overlap in wall time.
                                sem_b.add_permits(1);
                            }
                            .instrument(tracing::span!(Level::INFO, "release"))
                            .await;
                        }
                        .instrument(root_b),
                    );

                    // With a single-threaded executor, awaiting h_a drives both tasks:
                    // task_a suspends → task_b runs and unblocks it → task_a resumes.
                    h_a.await.unwrap();
                    h_b.await.unwrap();
                });
        });

        let all = cache.page(0, 20);
        assert_eq!(all.len(), 4, "task_a, acquire, task_b, release");
        assert!(all.iter().all(|s| s.closed_at.is_some()), "all spans must close");

        let find = |name: &str| all.iter().find(|s| s.metadata.name() == name).unwrap();
        let task_a  = find("task_a");
        let task_b  = find("task_b");
        let acquire = find("acquire");
        let release = find("release");

        assert_eq!(acquire.parent_id, Some(task_a.id), "acquire is child of task_a");
        assert_eq!(release.parent_id, Some(task_b.id), "release is child of task_b");

        // "acquire" was still open (not closed) when "release" ran.
        // Formally: the two half-open intervals [opened_at, closed_at) overlap.
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
