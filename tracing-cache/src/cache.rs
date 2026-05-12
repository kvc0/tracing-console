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
use crate::driver::Driver;
use crate::id_encoding::{disabled_id, id_to_u64, u64_to_id, DISABLED, SLAB_OFFSET};
use crate::object_pool::ObjectPool;
use crate::predicate::{EnabledPredicate, Interest, LevelPredicate};
use crate::record::{EventRecord, FieldList, FieldVisitor, SpanRecord};
use crate::thread_state::{
    ensure_thread_shard_key, pending_drain, pending_push, stack_pop, stack_push, stack_top,
    StackedSpan, ID_BATCH, ID_CURSOR, THREAD_SENDER,
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
    pub(crate) sender: spillway::Sender<SpanRecord>,
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

        let (sender, receiver) = spillway::channel();
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
            sender,
            pending_batch: config.pending_batch,
            shard_mask,
            shard_shift,
            event_pool,
        };
        let driver = Driver {
            map,
            receiver,
            capacity,
            batch_size: config.driver_batch,
            tick_interval: config.driver_interval,
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

    /// Returns an in-flight span by its tracing id (the value carried in
    /// `tracing::span::Id`).  Returns `None` if the span has closed; once
    /// closed it is reachable only via [`get_span`] using its `actual_id`.
    pub fn get_active_span(&self, tracing_id: u64) -> Option<SpanRecord> {
        let (shard, slab_idx) = self.decode_tracing_id(tracing_id)?;
        self.in_flight[shard].slab.lock().unwrap().get(slab_idx).cloned()
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

    /// Drains the calling thread's PENDING buffer into the spillway
    /// channel.  Must be called before [`Driver::drain_sync`] in tests
    /// to ensure all recently-closed spans are delivered.
    pub fn flush_pending(&self) {
        THREAD_SENDER.with(|sc| {
            // SAFETY: cell is thread-local and we only hold the &mut for
            // the duration of this closure; nothing inside re-enters
            // THREAD_SENDER.
            let slot = unsafe { &mut *sc.get() };
            let sender_ptr = &self.sender as *const _ as usize;
            let needs_init = !matches!(slot, Some((p, _)) if *p == sender_ptr);
            if needs_init {
                *slot = Some((sender_ptr, self.sender.clone()));
            }
            // SAFETY: `slot` was just guaranteed to be `Some`.
            let sender = unsafe { &slot.as_ref().unwrap_unchecked().1 };
            pending_drain(|records| {
                let _ = sender.send_many(records);
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
        attrs.record(&mut FieldVisitor { fields: &mut record.fields });

        // Step D: pick our shard, capacity-check, insert into the slab,
        // and publish actual_id into the sidecar (Release) so a future
        // contextual child sees it via the Acquire load above (or
        // `enter`'s lookup).
        let shard = self.pick_shard();
        let lane = &self.in_flight[shard];
        let mut slab = lane.slab.lock().unwrap();
        if slab.len() >= self.shard_capacity {
            log::warn!(
                "span shard {shard} full; new span disabled. \
                 Increase capacity or reduce span rate."
            );
            return disabled_id();
        }
        let slab_idx = slab.insert(record);
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
        // Parent: explicit parent on the event, else the current span on
        // the SPAN_STACK.  Either way we want a tracing id (the actual_id
        // sitting on the stack entry isn't useful here — we need the slab
        // address).
        let parent_id = match event.parent().map(id_to_u64) {
            Some(id) => id,
            None => match stack_top() {
                Some(top) => top.tracing_id,
                None => {
                    log::debug!("event dropped: no active span");
                    return;
                }
            },
        };
        if parent_id == DISABLED {
            log::debug!("event dropped: parent span is disabled");
            return;
        }

        // Acquire a pooled EventRecord, fill it in place.  The
        // FieldList capacity is preserved across pool reuse, so events
        // with similar field counts amortise their per-event allocation
        // to zero.
        let mut record = self.event_pool.acquire();
        record.metadata = Some(event.metadata());
        record.recorded_at = Some(Instant::now());
        record.fields.clear();
        event.record(&mut FieldVisitor { fields: &mut record.fields });

        let (shard, slab_idx) = match self.decode_tracing_id(parent_id) {
            Some(t) => t,
            None => return,
        };
        let mut shard_lock = self.in_flight[shard].slab.lock().unwrap();
        if let Some(span) = shard_lock.get_mut(slab_idx) {
            span.events.push(record);
        } else {
            log::debug!("event dropped: parent span at shard {shard} slab {slab_idx} not in cache");
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
        stack_push(StackedSpan { tracing_id, actual_id });
    }

    fn exit(&self, _span: &tracing::span::Id) {
        stack_pop();
    }

    fn try_close(&self, id: tracing::span::Id) -> bool {
        let (shard, slab_idx) = match self.decode_tracing_id(id_to_u64(&id)) {
            Some(t) => t,
            None => return false,
        };

        let record = {
            let mut shard_lock = self.in_flight[shard].slab.lock().unwrap();
            if shard_lock.contains(slab_idx) {
                let mut r = shard_lock.remove(slab_idx);
                r.closed_at = Some(Instant::now());
                Some(r)
            } else {
                None
            }
        };

        if let Some(record) = record {
            if pending_push(record) >= self.pending_batch {
                self.flush_pending();
            }
            true
        } else {
            false
        }
    }
}
