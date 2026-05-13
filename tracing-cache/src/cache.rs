//! The `SpanCache` subscriber and its in-flight slab shards.
//!
//! Open spans live in `Box<[ShardLane]>`, each lane a `Mutex<Slab>` plus
//! a parallel sidecar of `actual_id`s readable without locking.  When a
//! span closes, its `SpanRecord` is moved to the per-thread `PENDING`
//! buffer (`tls::pending_push`) and eventually flushed to the spillway
//! channel that `Driver` consumes.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use slab::Slab;
use tracing::metadata::LevelFilter;
use tracing::{Level, Metadata};

use crate::config::CacheConfig;
use crate::driver::{Driver, EventMessage};
use crate::id_encoding::{DISABLED, SLAB_OFFSET, disabled_id, id_to_u64, u64_to_id};
use crate::object_pool::ObjectPool;
use crate::predicate::{EnabledPredicate, Interest, LevelPredicate};
use crate::record::{EventRecord, FieldList, FieldVisitor, SpanRecord};
use crate::thread_state::{
    ID_BATCH, ID_CURSOR, StackedSpan, THREAD_SENDERS, ThreadSenders, ensure_thread_shard_key,
    pending_drain_events, pending_drain_spans, pending_push_event, pending_push_span, stack_pop,
    stack_push, stack_top,
};

/// One slab shard plus a parallel sidecar of `actual_id`s that's readable
/// without locking the slab.  The sidecar is sized to `shard_capacity`
/// (the slab's growth bound) and indexed by `slab_idx`.  `new_span`
/// writes the new span's `actual_id` with `Release` ordering before
/// publishing the resulting tracing id; `enter` reads with `Acquire` to
/// see that value.
pub(crate) struct ShardLane {
    pub(crate) slab: Mutex<Slab<SpanRecord>>,
    pub(crate) actual_ids: Box<[AtomicU64]>,
}

/// A `tracing::Subscriber` that holds spans in memory for inspection.
///
/// Open spans live in a sharded `Box<[Mutex<Slab<SpanRecord>>]>`, with
/// the lane count set by [`CacheConfig::lane_count`] (default
/// [`crate::DEFAULT_LANE_COUNT`]).  The shard is picked by a thread-local
/// key; the slab gives an O(1) cache-friendly index, and the user-facing
/// `tracing::span::Id` packs `(shard, slab_idx+2)` into a single u64 so
/// `SPAN_STACK` push/pop and trait-method dispatch don't need a separate
/// lookup.  When a span closes it moves to a per-thread buffer and is
/// flushed to the [`Driver`] via a spillway channel.
///
/// `SpanRecord.id` is an `actual_id` (separate from the tracing id) that
/// serves as the `BTreeMap` key, since slab indices are reused.  IDs are
/// monotonic within a thread's `ID_BATCH`-sized reservation; across
/// threads they interleave, so map order reflects allocation order
/// per-thread but not strict global creation order.
///
/// Create with [`SpanCache::new`] / [`SpanCache::with_predicate`]
/// (defaults) or [`SpanCache::with_config`] /
/// [`SpanCache::with_predicate_and_config`] (custom batch sizes & lane
/// count).  Each returns `(SpanCache, Driver)`; spawn the [`Driver`] as
/// a background task to commit closed spans.
pub struct SpanCache<P: EnabledPredicate = LevelPredicate> {
    pub(crate) in_flight: Box<[ShardLane]>,
    pub(crate) map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    /// High-water mark for the `actual_id` space (the `SpanRecord.id` /
    /// `BTreeMap` key, disjoint from the encoded tracing id space).
    /// Threads claim `ID_BATCH`-sized slices via `fetch_add` and hand IDs
    /// out from a thread-local reservation, so this counter is touched
    /// roughly once per `ID_BATCH` spans rather than once per span.
    pub(crate) id_high_water: AtomicU64,
    pub(crate) predicate: P,
    /// Per-shard capacity.  Total open-span budget is
    /// `shard_capacity * lane_count`.
    pub(crate) shard_capacity: usize,
    pub(crate) span_sender: spillway::Sender<SpanRecord>,
    pub(crate) event_sender: spillway::Sender<EventMessage>,
    pub(crate) pending_batch: usize,
    pub(crate) shard_mask: u64,  // lane_count - 1
    pub(crate) shard_shift: u32, // 64 - log2(lane_count); shard at top of id
    /// Shared pool of pre-allocated `EventRecord`s.  `event()` acquires
    /// from the per-thread shard, fills the record, and pushes the
    /// `ReuseRef` onto the parent span's events vec.  When the
    /// `SpanRecord` finally drops (BTreeMap eviction), each `ReuseRef`
    /// returns its allocation to the pool — amortising away the
    /// per-event `Box::new` cost the previous `Vec<EventRecord>` paid.
    pub(crate) event_pool: Arc<ObjectPool<EventRecord>>,
}

impl SpanCache<LevelPredicate> {
    /// Default predicate (TRACE), default config.
    pub fn new(capacity: usize) -> (Self, Driver) {
        Self::with_predicate(capacity, LevelPredicate::new(Level::TRACE))
    }

    /// Default predicate (TRACE) with a custom [`CacheConfig`].
    pub fn with_config(capacity: usize, config: CacheConfig) -> (Self, Driver) {
        Self::with_predicate_and_config(capacity, LevelPredicate::new(Level::TRACE), config)
    }
}

impl<P: EnabledPredicate> SpanCache<P> {
    /// Custom predicate, default [`CacheConfig`].
    pub fn with_predicate(capacity: usize, predicate: P) -> (Self, Driver) {
        Self::with_predicate_and_config(capacity, predicate, CacheConfig::default())
    }

    /// Custom predicate and custom [`CacheConfig`].
    pub fn with_predicate_and_config(
        capacity: usize,
        predicate: P,
        config: CacheConfig,
    ) -> (Self, Driver) {
        // Silently clamp to [1, 256] and round up to the next power of two.
        let lane_count = config.lane_count.clamp(1, 256).next_power_of_two();
        let shard_bits = lane_count.trailing_zeros();
        let shard_mask = (lane_count as u64) - 1;
        // Reserve at least one bit at the top so `(shard as u64) << shift`
        // is well-defined even when lane_count == 1.
        let shard_shift = 64 - shard_bits.max(1);

        // Bound both channels so a faster producer than consumer (e.g. a
        // 16-core Graviton with 4 async workers vs. one driver task) can't
        // grow spillway's internal buffers without bound and exhaust RAM.
        // `send_many` rejects the whole batch with `Error::Full` when the
        // limit is exceeded; `flush_pending` discards the rejected drain.
        // Concurrency matches `lane_count` so each lane's threads tend to
        // land on their own chute and contend less with peers (spillway's
        // chute count caps useful per-clone parallelism).
        let (span_sender, span_receiver) =
            spillway::channel_with_capacity_and_concurrency(config.channel_capacity, lane_count);
        let (event_sender, event_receiver) =
            spillway::channel_with_capacity_and_concurrency(config.channel_capacity, lane_count);
        let map = Arc::new(RwLock::new(BTreeMap::new()));
        let shard_capacity = capacity.div_ceil(lane_count);
        let in_flight: Box<[ShardLane]> = (0..lane_count)
            .map(|_| ShardLane {
                slab: Mutex::new(Slab::with_capacity(shard_capacity)),
                actual_ids: (0..shard_capacity)
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        // Event pool sharded the same way as the slab (one per lane);
        // per-shard capacity is generous so steady-state event traffic
        // never spills.  These are tiny structs (~360 B with the
        // inline-8 FieldList), 256 × 16 lanes = ~1.5 MB worst case.
        let event_pool = ObjectPool::<EventRecord>::new(lane_count, 256);

        let cache = SpanCache {
            in_flight,
            map: Arc::clone(&map),
            // Initialise to ID_BATCH so every `fetch_add(ID_BATCH)`
            // returns a batch-aligned start; that's what makes the
            // mask-based "cursor at boundary ⇒ refill" check work in
            // `allocate_actual_id`.
            id_high_water: AtomicU64::new(ID_BATCH),
            predicate,
            shard_capacity,
            span_sender,
            event_sender,
            pending_batch: config.pending_batch,
            shard_mask,
            shard_shift,
            event_pool,
        };
        let driver = Driver {
            map,
            span_receiver,
            event_receiver,
            capacity,
            batch_size: config.driver_batch,
            tick_interval: config.driver_interval,
            side_events: std::collections::HashMap::new(),
        };
        (cache, driver)
    }

    /// Number of in-flight slab shards this cache uses.
    pub fn lane_count(&self) -> usize {
        self.in_flight.len()
    }

    #[inline]
    pub(crate) fn pick_shard(&self) -> usize {
        (ensure_thread_shard_key() & self.shard_mask) as usize
    }

    /// Claim the next `actual_id` from this thread's reservation against
    /// `id_high_water`, refilling via `fetch_add(ID_BATCH)` when the
    /// reservation is exhausted (`cursor & (ID_BATCH - 1) == 0`).
    /// Touches the shared atomic ~once per `ID_BATCH` spans rather than
    /// once per span.
    #[inline]
    pub(crate) fn allocate_actual_id(&self) -> u64 {
        ID_CURSOR.with(|cell| {
            let cursor = cell.get();
            if (cursor & (ID_BATCH - 1)) != 0 {
                cell.set(cursor + 1);
                cursor
            } else {
                let start = self.id_high_water.fetch_add(ID_BATCH, Relaxed);
                cell.set(start + 1);
                start
            }
        })
    }

    #[inline]
    pub(crate) fn encode_tracing_id(&self, shard: usize, slab_idx: usize) -> u64 {
        ((shard as u64) << self.shard_shift) | ((slab_idx as u64) + SLAB_OFFSET)
    }

    /// Decode a tracing id into `(shard, slab_idx)`.  Returns `None` for
    /// `DISABLED` or anything outside the encoding scheme.
    #[inline]
    pub(crate) fn decode_tracing_id(&self, id: u64) -> Option<(usize, usize)> {
        if id == DISABLED {
            return None;
        }
        let slab_mask = (1u64 << self.shard_shift) - 1;
        let raw = id & slab_mask;
        if raw < SLAB_OFFSET {
            return None;
        }
        let shard = ((id >> self.shard_shift) & self.shard_mask) as usize;
        Some((shard, (raw - SLAB_OFFSET) as usize))
    }

    /// Read a slab slot's `actual_id` from the lock-free sidecar.
    /// `Acquire` pairs with the `Release` store in `new_span` so the
    /// value is visible once the encoded tracing id has been published
    /// to the caller.
    #[inline]
    pub(crate) fn load_actual_id(&self, shard: usize, slab_idx: usize) -> u64 {
        self.in_flight[shard].actual_ids[slab_idx].load(Ordering::Acquire)
    }

    /// Returns a closed span by its actual_id (`SpanRecord.id`).  This is
    /// the id stored in `parent_id` and used as the BTreeMap key.  For
    /// in-flight spans, use [`get_active_span`].
    pub fn get_span(&self, actual_id: u64) -> Option<SpanRecord> {
        self.map.read().unwrap().get(&actual_id).cloned()
    }

    /// Drop every closed span currently in the BTreeMap.  Called by
    /// the host when the cache-recording level transitions to `OFF`
    /// so a paused host doesn't keep stale data around to confuse the
    /// next session.  In-flight spans (still open in the slabs) are
    /// not affected; if any close after this call they'll repopulate
    /// the map as normal.
    pub fn clear(&self) {
        self.map.write().unwrap().clear();
    }

    /// Resolve the `actual_id` (i.e. the [`SpanRecord::id`] used as the
    /// `BTreeMap` key after close) for an in-flight span addressed by
    /// its `tracing::span::Id` u64.  Lock-free `Acquire` load from the
    /// per-shard sidecar — does not touch the slab `Mutex`.
    pub fn actual_id_for(&self, tracing_id: u64) -> Option<u64> {
        let (shard, slab_idx) = self.decode_tracing_id(tracing_id)?;
        Some(self.load_actual_id(shard, slab_idx))
    }

    /// Returns closed spans in ascending actual_id order.  Open spans are
    /// not included; call [`flush_pending`] + [`Driver::drain_sync`]
    /// first if you need just-closed spans to appear.
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

    /// Drains the calling thread's two PENDING buffers (spans + events)
    /// into their respective spillway channels.  Must be called before
    /// [`Driver::drain_sync`] in tests to ensure all recently-closed
    /// spans and emitted events are delivered.
    pub fn flush_pending(&self) {
        THREAD_SENDERS.with(|sc| {
            // SAFETY: cell is thread-local and we only hold the &mut for
            // the duration of this closure; nothing inside re-enters
            // THREAD_SENDERS.
            let slot = unsafe { &mut *sc.get() };
            let cache_addr = self as *const _ as usize;
            let needs_init = !matches!(slot, Some(t) if t.cache_addr == cache_addr);
            if needs_init {
                *slot = Some(ThreadSenders {
                    cache_addr,
                    span: self.span_sender.clone(),
                    event: self.event_sender.clone(),
                });
            }
            // SAFETY: `slot` was just guaranteed to be `Some`.
            let senders = unsafe { slot.as_ref().unwrap_unchecked() };
            // Avoid send_many on empty drains — spillway's chute
            // invariant rejects sender clones that publish without
            // having ever held content.  On `Error::Full`, the rejected
            // drain is bound to the match arm and dropped, which drops
            // each unsent record (and runs `ReuseRef::Drop` for events,
            // returning the EventRecord allocation to the pool).
            pending_drain_events(|events| {
                if events.len() > 0 {
                    if let Err(spillway::Error::Full(_dropped)) = senders.event.send_many(events) {
                        log::debug!("event channel full; dropping a batch — driver is behind");
                    }
                }
            });
            pending_drain_spans(|spans| {
                if spans.len() > 0 {
                    if let Err(spillway::Error::Full(_dropped)) = senders.span.send_many(spans) {
                        log::debug!("span channel full; dropping a batch — driver is behind");
                    }
                }
            });
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
        if matches!(stack_top(), Some(s) if s.tracing_id == DISABLED) {
            return false;
        }
        self.predicate.enabled(metadata)
    }

    fn event_enabled(&self, event: &tracing::Event<'_>) -> bool {
        self.predicate.enabled(event.metadata())
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // Step A: resolve parent's actual_id from a side-channel — no
        // slab lock.  Contextual: the parent's `actual_id` is right there
        // on the SPAN_STACK entry.  Explicit: the parent's tracing id
        // encodes its slab address, and the sidecar holds its actual_id.
        let parent_actual_id: Option<u64> = if attrs.is_contextual() {
            match stack_top() {
                None => return disabled_id(),
                Some(top) if top.tracing_id == DISABLED => return disabled_id(),
                Some(top) => Some(top.actual_id),
            }
        } else if attrs.is_root() {
            if stack_top().is_some() {
                log::warn!("root span created with an active span on the stack; disabling");
                return disabled_id();
            }
            None
        } else {
            let explicit = id_to_u64(attrs.parent().unwrap());
            match self.decode_tracing_id(explicit) {
                // Lock-free read from the sidecar: the parent's actual_id
                // was published by a `Release` store before the parent's
                // tracing id was returned to the caller, so this `Acquire`
                // load sees it.
                Some((p_shard, p_slab)) => Some(self.load_actual_id(p_shard, p_slab)),
                None => return disabled_id(),
            }
        };

        // Step B: predicate check.
        if !self.predicate.new_span_enabled(attrs) {
            return disabled_id();
        }

        // Step C: build record outside the lock so field-visitor work
        // doesn't extend the critical section.
        let actual_id = self.allocate_actual_id();
        let mut record = SpanRecord {
            id: actual_id,
            parent_id: parent_actual_id,
            metadata: attrs.metadata(),
            fields: FieldList::new(),
            events: Vec::new(),
            opened_at: Instant::now(),
            closed_at: None,
        };
        attrs.record(&mut FieldVisitor {
            fields: &mut record.fields,
        });

        // Step D: pick our shard, capacity-check + slab.insert under the
        // Mutex, then drop the guard before the sidecar store and the
        // pure-arithmetic id encoding.  The sidecar is a separate atomic
        // and the Release/Acquire pair on `actual_ids[slab_idx]` is its
        // own happens-before — nobody can observe this slot until we
        // return the tracing id below, so the store doesn't need to be
        // sequenced under the slab Mutex.
        let shard = self.pick_shard();
        let lane = &self.in_flight[shard];
        let slab_idx = {
            let mut slab = lane.slab.lock().unwrap();
            if slab.len() >= self.shard_capacity {
                log::warn!(
                    "span shard {shard} full; new span disabled. \
                     Increase capacity or reduce span rate."
                );
                return disabled_id();
            }
            slab.insert(record)
        };
        // The sidecar is sized to `shard_capacity` and indexed by
        // slab_idx, which is bounded by capacity per the check above.
        lane.actual_ids[slab_idx].store(actual_id, Ordering::Release);
        u64_to_id(self.encode_tracing_id(shard, slab_idx))
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let (shard, slab_idx) = match self.decode_tracing_id(id_to_u64(span)) {
            Some(t) => t,
            None => return,
        };
        let mut shard_lock = self.in_flight[shard].slab.lock().unwrap();
        if let Some(rec) = shard_lock.get_mut(slab_idx) {
            values.record(&mut FieldVisitor {
                fields: &mut rec.fields,
            });
        }
    }

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        // Resolve the parent's `actual_id` lock-free.  Contextual events
        // get it straight off the SPAN_STACK entry; events with an
        // explicit parent decode the tracing id and `Acquire`-load from
        // the per-shard sidecar — no slab lock either way.
        let parent_actual_id = match event.parent().map(id_to_u64) {
            Some(tracing_id) => {
                if tracing_id == DISABLED {
                    log::debug!("event dropped: parent span is disabled");
                    return;
                }
                match self.decode_tracing_id(tracing_id) {
                    Some((shard, slab_idx)) => self.load_actual_id(shard, slab_idx),
                    None => return,
                }
            }
            None => match stack_top() {
                Some(top) if top.tracing_id == DISABLED => {
                    log::debug!("event dropped: parent span is disabled");
                    return;
                }
                Some(top) => top.actual_id,
                None => {
                    log::debug!("event dropped: no active span");
                    return;
                }
            },
        };

        // Acquire a pooled EventRecord, fill it in place.  The pooled
        // FieldList allocation is preserved across reuse.
        let mut record = self.event_pool.acquire();
        record.metadata = Some(event.metadata());
        record.recorded_at = Some(Instant::now());
        record.fields.clear();
        event.record(&mut FieldVisitor {
            fields: &mut record.fields,
        });

        // Hand off to the driver via the event PENDING — no slab lock
        // here.  The driver attaches to the parent's `events` vec
        // (directly if the parent's already in the map, or via the
        // side buffer if the event raced ahead of the span).
        if pending_push_event(EventMessage {
            parent_actual_id,
            record,
        }) >= self.pending_batch
        {
            self.flush_pending();
        }
    }

    fn enter(&self, span: &tracing::span::Id) {
        // Resolve actual_id once, lock-free, and stash it on the stack so
        // a contextual `new_span` underneath can read its parent's
        // actual_id without locking the parent's slab.
        let tracing_id = id_to_u64(span);
        let actual_id = match self.decode_tracing_id(tracing_id) {
            Some((shard, slab_idx)) => self.load_actual_id(shard, slab_idx),
            None => 0, // DISABLED entry — actual_id is never read.
        };
        stack_push(StackedSpan {
            tracing_id,
            actual_id,
        });
    }

    fn exit(&self, _span: &tracing::span::Id) {
        stack_pop();
    }

    fn try_close(&self, id: tracing::span::Id) -> bool {
        let (shard, slab_idx) = match self.decode_tracing_id(id_to_u64(&id)) {
            Some(t) => t,
            None => return false,
        };

        // Single slab lookup via `try_remove` (no contains-then-remove
        // double hash), and `Instant::now()` lives outside the critical
        // section — only paid on the success path.
        let record = self.in_flight[shard]
            .slab
            .lock()
            .unwrap()
            .try_remove(slab_idx);

        if let Some(mut record) = record {
            record.closed_at = Some(Instant::now());
            if pending_push_span(record) >= self.pending_batch {
                self.flush_pending();
            }
            true
        } else {
            false
        }
    }
}
