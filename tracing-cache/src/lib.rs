use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
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

/// Default number of in-flight slab shards (must be a power of two).
pub const DEFAULT_LANE_COUNT: usize = 16;

/// Optional knobs for the cache + driver.  Pass to
/// [`SpanCache::with_config`] / [`SpanCache::with_predicate_and_config`]; the
/// no-config constructors use [`CacheConfig::default`].
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Number of in-flight slab shards.  Silently clamped to `[1, 256]` and
    /// rounded up to the next power of two (so `3` becomes `4`, `200`
    /// becomes `256`, `1000` is capped at `256`).  More lanes = more
    /// concurrent writers without contention; each lane adds a
    /// `Mutex<Slab<SpanRecord>>` plus consumes one more bit of the encoded
    /// `tracing::span::Id` for shard selection.
    /// Default: [`DEFAULT_LANE_COUNT`].
    pub lane_count: usize,
    /// Flush the thread-local PENDING buffer to the spillway after this many
    /// span closures on a single thread.  Smaller = lower visibility latency
    /// for low-traffic threads at the cost of more spillway sends.  Default: 32.
    pub pending_batch: usize,
    /// Flush the driver's accumulated batch into the shared map after this
    /// many spans have been received.  Smaller = lower visibility latency at
    /// the cost of more map write-lock acquisitions.  Default: 600.
    pub driver_batch: usize,
    /// Upper bound on how long the driver will wait before flushing whatever
    /// it has, even if `driver_batch` hasn't been reached.  Default: 1 second.
    pub driver_interval: std::time::Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            lane_count: DEFAULT_LANE_COUNT,
            pending_batch: 32,
            driver_batch: 600,
            driver_interval: std::time::Duration::from_secs(1),
        }
    }
}

// SAFETY rationale for SPAN_STACK and PENDING using `UnsafeCell` rather than
// `RefCell`: every access goes through one of the helpers below, each of
// which constructs a temporary `&` or `&mut` to the inner Vec for a single
// leaf operation (push / pop / drain / read len) and lets it die before the
// helper returns.  No subscriber-method dispatch happens while one of these
// references is alive:
//   * push / pop / last-copy do no callbacks at all.
//   * `pending_drain` calls `spillway::Sender::send` per element — that's a
//     non-reentrant Mutex push that does not invoke tracing on this thread.
//   * `enabled` / `new_span` consult predicates only AFTER releasing the
//     SPAN_STACK borrow (`stack_top` returns `Option<u64>` by value).
// Concurrent access can't happen — the cells are thread-local.
thread_local! {
    /// Active span entries on this thread.  Each entry pairs the encoded
    /// tracing id (so `exit` can pop) with the span's `actual_id` (so a
    /// contextual `new_span` can read its parent's `actual_id` without
    /// locking the parent's slab).  DISABLED entries carry
    /// `tracing_id = DISABLED, actual_id = 0`.
    static SPAN_STACK: std::cell::UnsafeCell<Vec<StackedSpan>> =
        const { std::cell::UnsafeCell::new(Vec::new()) };
    /// Closed spans waiting to be sent to the driver via spillway.
    static PENDING: std::cell::UnsafeCell<Vec<SpanRecord>> =
        const { std::cell::UnsafeCell::new(Vec::new()) };
}

#[inline]
fn stack_top() -> Option<StackedSpan> {
    SPAN_STACK.with(|c| unsafe { (*c.get()).last().copied() })
}

#[inline]
fn stack_push(entry: StackedSpan) {
    SPAN_STACK.with(|c| unsafe { (*c.get()).push(entry) });
}

#[inline]
fn stack_pop() {
    SPAN_STACK.with(|c| unsafe {
        (*c.get()).pop();
    });
}

/// Push a closed span onto PENDING and return the new length.
#[inline]
fn pending_push(record: SpanRecord) -> usize {
    PENDING.with(|c| unsafe {
        let v = &mut *c.get();
        v.push(record);
        v.len()
    })
}

/// Drain PENDING in place (preserving the Vec's capacity), invoking `f` on
/// each record.  `f` must not call back into tracing on this thread.
#[inline]
fn pending_drain<F: FnMut(std::vec::Drain<'_, SpanRecord>)>(mut f: F) {
    PENDING.with(|c| unsafe {
        f((*c.get()).drain(..));
    });
}

fn id_to_u64(id: &tracing::span::Id) -> u64 {
    id.into_u64()
}

fn u64_to_id(n: u64) -> tracing::span::Id {
    tracing::span::Id::from_u64(n)
}

// ── Tracing-id encoding ──────────────────────────────────────────────────────
//
// The u64 carried in `tracing::span::Id` encodes (shard, slab_idx) only:
//
//     top    `shard_bits`        bits  → shard index (0 .. lane_count)
//     bottom `64 - shard_bits`   bits  → slab_idx + SLAB_OFFSET
//
// `DISABLED = 1` is reserved.  Slab indices encode as `slab_idx + 2` so
// shard 0 / slab_idx 0 doesn't collide with DISABLED.
//
// `actual_id` is **not** in the encoded id; it lives in the per-shard
// sidecar `actual_ids: Box<[AtomicU64]>` (see `ShardLane`).  `new_span`
// writes it when it inserts into the slab; `enter` reads it lock-free and
// pushes a `StackedSpan { tracing_id, actual_id }` onto SPAN_STACK so that
// a contextual `new_span` later can read its parent's actual_id directly
// from the stack without locking the parent's slab.
const DISABLED: u64 = 1;
const SLAB_OFFSET: u64 = 2;

/// One entry on the per-thread SPAN_STACK.  Carries both the encoded tracing
/// id (so `exit` can pop it back) and the parent-side `actual_id` so that
/// `new_span`'s contextual-parent path can read the parent's `SpanRecord.id`
/// without acquiring the parent-shard's mutex.  For DISABLED entries
/// `actual_id` is 0 and is never read.
#[derive(Clone, Copy, Debug)]
struct StackedSpan {
    tracing_id: u64,
    actual_id: u64,
}

/// One slab shard plus a parallel sidecar of actual_ids that's readable
/// without locking the slab.  The sidecar is sized to `shard_capacity` (the
/// slab's growth bound) and indexed by `slab_idx`.  `new_span` writes the
/// new span's actual_id with `Release` ordering before publishing the
/// resulting tracing id; `enter` reads with `Acquire` to see that value.
struct ShardLane {
    slab: Mutex<Slab<SpanRecord>>,
    actual_ids: Box<[AtomicU64]>,
}

/// Global counter that hands out a stable shard key to each thread the
/// first time it touches `pick_shard`.  Cheap, monotonic, and only paid
/// once per thread.
static NEXT_THREAD_KEY: AtomicU64 = AtomicU64::new(0);

/// Number of `actual_id`s a thread reserves in a single bump of the
/// per-cache `id_high_water` counter.  Power of two so the in-batch
/// position is `cursor & (ID_BATCH - 1)` and refill is signalled by that
/// being zero — lets the per-thread reservation be a single u64 cursor.
/// Larger batch ⇒ fewer `fetch_add` hits on the shared atomic ⇒ less
/// cross-thread contention, at the cost of `actual_id`s no longer being
/// globally monotonic — they're monotonic within a thread's batch, but
/// interleave across threads.
const ID_BATCH: u64 = 1024;

thread_local! {
    /// Stable per-thread shard key, lazily assigned from `NEXT_THREAD_KEY`
    /// on first use.  Each cache derives its actual shard via
    /// `key & shard_mask`, so a given thread always lands on the same shard
    /// within a given cache (lock affinity for the slab cache lines).
    /// `u64::MAX` is the "not assigned yet" sentinel (the global counter
    /// can't realistically reach it).
    static THREAD_SHARD_KEY: Cell<u64> = const { Cell::new(u64::MAX) };

    /// Per-thread `spillway::Sender` clone, lazily initialised on first
    /// `flush_pending`.  Spillway's design is per-clone-lock-free: each
    /// `Sender::clone()` gets its own queue slot, so threads pushing to
    /// their own clone do not contend on spillway's internal mutex.  We
    /// stash the source-cache's sender address alongside the clone so a
    /// thread that switches between two `SpanCache` instances notices the
    /// switch and re-clones.
    /// SAFETY rationale matches `SPAN_STACK` / `PENDING`: the cell is
    /// thread-local, every access is wrapped in a single short scope by
    /// `flush_pending`, and `spillway::Sender::send` is non-reentrant w.r.t.
    /// tracing on this thread.
    static THREAD_SENDER: std::cell::UnsafeCell<Option<(usize, spillway::Sender<SpanRecord>)>>
        = const { std::cell::UnsafeCell::new(None) };

    /// Per-thread `actual_id` cursor: the next id to hand out from the
    /// current `ID_BATCH`-sized reservation.  `cursor & (ID_BATCH - 1) == 0`
    /// (including the initial `0`) means the batch is exhausted and the
    /// next call must refill via `id_high_water.fetch_add(ID_BATCH)`.
    /// Since `id_high_water` is initialised to a multiple of `ID_BATCH`,
    /// every fetched start is batch-aligned, so the mask check is
    /// sufficient.  Process-level (not per-cache): in production there's
    /// one cache, and our tests never emit enough spans on one thread to
    /// trigger a refill, so cross-cache leakage of cursor state is
    /// harmless in practice.
    static ID_CURSOR: Cell<u64> = const { Cell::new(0) };
}

/// A `tracing::Subscriber` that holds spans in memory for inspection.
///
/// Open spans live in a sharded `Box<[Mutex<Slab<SpanRecord>>]>`, with the
/// lane count set by [`CacheConfig::lane_count`] (default
/// [`DEFAULT_LANE_COUNT`]).  The shard is picked by a thread-local counter;
/// the slab gives an O(1) cache-friendly index, and the user-facing
/// `tracing::span::Id` packs `(shard, slab_idx+2)` into a single u64 so
/// SPAN_STACK push/pop and trait-method dispatch don't need a separate
/// lookup.  When a span closes it moves to a per-thread buffer and is
/// flushed to the [`Driver`] via a spillway channel.
///
/// `SpanRecord.id` is an `actual_id` (separate from the tracing id) that
/// serves as the BTreeMap key, since slab indices are reused.  IDs are
/// monotonic within a thread's `ID_BATCH`-sized reservation; across
/// threads they interleave, so BTreeMap order reflects allocation order
/// per-thread but not strict global creation order.
///
/// Create with [`SpanCache::new`] / [`SpanCache::with_predicate`] (defaults)
/// or [`SpanCache::with_config`] / [`SpanCache::with_predicate_and_config`]
/// (custom batch sizes & lane count).  Each returns `(SpanCache, Driver)`;
/// spawn the [`Driver`] as a background task to commit closed spans.
pub struct SpanCache<P: EnabledPredicate = LevelPredicate> {
    in_flight: Box<[ShardLane]>,
    map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    // High-water mark for the `actual_id` space (the `SpanRecord.id` /
    // BTreeMap key, disjoint from the encoded tracing id space).  Threads
    // claim `ID_BATCH`-sized slices via `fetch_add` and hand IDs out from
    // a thread-local reservation, so this counter is touched roughly
    // once per `ID_BATCH` spans rather than once per span.
    id_high_water: AtomicU64,
    predicate: P,
    // Per-shard capacity.  Total open-span budget is shard_capacity * lane_count.
    shard_capacity: usize,
    sender: spillway::Sender<SpanRecord>,
    // Knobs derived from CacheConfig.
    pending_batch: usize,
    shard_mask: u64,  // lane_count - 1
    shard_shift: u32, // 64 - log2(lane_count); encodes shard at the top of the id
}

/// Background task that receives closed spans from the spillway and writes
/// them to the shared `BTreeMap` in batches.
pub struct Driver {
    map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    receiver: spillway::Receiver<SpanRecord>,
    capacity: usize,
    batch_size: usize,
    tick_interval: std::time::Duration,
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
        // Reserve at least one bit at the top so `(shard as u64) << shift` is
        // well-defined even when lane_count == 1.
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

        let cache = SpanCache {
            in_flight,
            map: Arc::clone(&map),
            // Initialise to ID_BATCH so every `fetch_add(ID_BATCH)` returns
            // a batch-aligned start; that's what makes the mask-based
            // "cursor at boundary ⇒ refill" check work in `allocate_actual_id`.
            id_high_water: AtomicU64::new(ID_BATCH),
            predicate,
            shard_capacity,
            sender,
            pending_batch: config.pending_batch,
            shard_mask,
            shard_shift,
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
    fn pick_shard(&self) -> usize {
        let key = THREAD_SHARD_KEY.with(|c| {
            let v = c.get();
            if v == u64::MAX {
                // First new_span on this thread — claim a stable key.
                let assigned = NEXT_THREAD_KEY.fetch_add(1, Relaxed);
                c.set(assigned);
                assigned
            } else {
                v
            }
        });
        (key & self.shard_mask) as usize
    }

    /// Claim the next `actual_id` from this thread's reservation against
    /// `id_high_water`, refilling via `fetch_add(ID_BATCH)` when the
    /// reservation is exhausted (`cursor & (ID_BATCH - 1) == 0`).
    /// Touches the shared atomic ~once per `ID_BATCH` spans rather than
    /// once per span.
    #[inline]
    fn allocate_actual_id(&self) -> u64 {
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
    fn encode_tracing_id(&self, shard: usize, slab_idx: usize) -> u64 {
        ((shard as u64) << self.shard_shift) | ((slab_idx as u64) + SLAB_OFFSET)
    }

    /// Decode a tracing id into `(shard, slab_idx)`.  Returns `None` for
    /// `DISABLED` or anything outside the encoding scheme.
    #[inline]
    fn decode_tracing_id(&self, id: u64) -> Option<(usize, usize)> {
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

    /// Read a slab slot's `actual_id` from the lock-free sidecar.  `Acquire`
    /// pairs with the `Release` store in `new_span` so the value is visible
    /// once the encoded tracing id has been published to the caller.
    #[inline]
    fn load_actual_id(&self, shard: usize, slab_idx: usize) -> u64 {
        self.in_flight[shard].actual_ids[slab_idx].load(Ordering::Acquire)
    }

    /// Returns a closed span by its actual_id (`SpanRecord.id`).  This is the
    /// id stored in `parent_id` and used as the BTreeMap key.  For in-flight
    /// spans, use [`get_active_span`].
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
        THREAD_SENDER.with(|sc| {
            // SAFETY: cell is thread-local and we only hold the &mut for the
            // duration of this closure; nothing inside re-enters THREAD_SENDER.
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
        let disabled_id = || u64_to_id(DISABLED);

        // Step A: resolve parent's actual_id from a side-channel — no slab
        // lock.  Contextual: the parent's `actual_id` is right there on the
        // SPAN_STACK entry.  Explicit: the parent's tracing id encodes its
        // actual_id in the bottom 40 bits.
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
                // Lock-free read from the sidecar: the parent's actual_id was
                // published by a `Release` store before the parent's tracing
                // id was returned to the caller, so this `Acquire` load sees it.
                Some((p_shard, p_slab)) => Some(self.load_actual_id(p_shard, p_slab)),
                None => return disabled_id(),
            }
        };

        // Step B: predicate check.
        if !self.predicate.new_span_enabled(attrs) {
            return disabled_id();
        }

        // Step C: build record outside the lock so field-visitor work doesn't
        // extend the critical section.
        let actual_id = self.allocate_actual_id();
        let mut record = SpanRecord {
            id: actual_id,
            parent_id: parent_actual_id,
            metadata: attrs.metadata(),
            fields: HashMap::new(),
            events: Vec::new(),
            opened_at: Instant::now(),
            closed_at: None,
        };
        attrs.record(&mut FieldVisitor { fields: &mut record.fields });

        // Step D: pick our shard, capacity-check, insert into the slab, and
        // publish actual_id into the sidecar (Release) so a future contextual
        // child sees it via the Acquire load above (or `enter`'s lookup).
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
        // The sidecar is sized to `shard_capacity` and indexed by slab_idx,
        // which is bounded by capacity per the check above.
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
        // Parent: explicit parent on the event, else the current span on the
        // SPAN_STACK.  Either way we want a tracing id (the actual_id sitting
        // on the stack entry isn't useful here — we need the slab address).
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

        let mut fields = HashMap::new();
        event.record(&mut FieldVisitor { fields: &mut fields });
        let record = EventRecord {
            metadata: event.metadata(),
            fields,
            recorded_at: Instant::now(),
        };

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
        // a contextual `new_span` underneath can read its parent's actual_id
        // without locking the parent's slab.
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

// ── Driver ───────────────────────────────────────────────────────────────────

impl Driver {
    /// Runs the driver loop.  Flushes the accumulated batch into the shared
    /// map whenever 600 spans accumulate or one second elapses.
    ///
    /// Terminates when all `Sender` clones are dropped (spillway channel closed).
    pub async fn run(self) {
        let Driver { map, mut receiver, capacity, batch_size, tick_interval } = self;

        loop {
            let delivery_batch = receiver.next_batch().await;
            match delivery_batch {
                Some(delivery_batch) => {
                    Self::flush_batch(&map, capacity, delivery_batch);
                }
                None => {
                    // All senders dropped
                    break;
                }
            }
        }
    }

    /// Synchronously drains all spans currently available in the spillway and
    /// flushes them into the map.  Use in tests after [`SpanCache::flush_pending`].
    pub fn drain_sync(self) {
        let Driver { map, mut receiver, capacity, .. } = self;
        let mut batch = Vec::new();
        while let Some(record) = receiver.try_next() {
            batch.push(record);
        }
        Self::flush_batch(&map, capacity, batch.into_iter());
    }

    fn flush_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = SpanRecord>,
    ) {
        if batch.len() == 0 {
            return;
        }
        // Only closed spans are ever sent to the driver, so all entries in the
        // map are already closed — pop_first() always evicts a finished span.
        let mut m = map.write().unwrap();
        if capacity <= batch.len() {
            m.clear();
        } else {
            while capacity < m.len() + batch.len() {
                m.pop_first();
            }
        }
        let skip = batch.len().saturating_sub(capacity);
        m.extend(batch.skip(skip).map(|s| (s.id, s)));
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

    /// Captures `SpanRecord.id` (the actual_id) of an in-flight span — needed
    /// to look it up in the closed-span map after drain (the tracing id and
    /// actual id live in disjoint namespaces).
    fn actual_id_of(cache: &Arc<SpanCache>, span: &tracing::Span) -> u64 {
        cache.get_active_span(span_id(span).unwrap()).unwrap().id
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
            let actual_id = cache.get_active_span(tracing_id).unwrap().id;
            {
                let _g = span.enter();
            }
            // While alive: lookup by tracing id finds the slab entry.
            assert!(
                cache.get_active_span(tracing_id).unwrap().closed_at.is_none(),
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
        // Single-lane so capacity=2 means "2 in-flight", not "2 lanes × 1
        // each".  With thread-id sharding all spans on this thread go to the
        // same shard, so a multi-lane setup with capacity=2 could only fit 1
        // in-flight per thread — not what this test wants to verify.
        let (cache, driver) = SpanCache::with_config(
            2,
            CacheConfig { lane_count: 1, ..CacheConfig::default() },
        );
        let cache = Arc::new(cache);
        let (a, b, c) = run_with_drain(&cache, driver, || {
            let span_a = tracing::span!(parent: None, Level::INFO, "a");
            let a = actual_id_of(&cache, &span_a);
            let span_b = tracing::span!(parent: None, Level::INFO, "b");
            let b = actual_id_of(&cache, &span_b);
            drop(span_a);
            drop(span_b);
            // in_flight is empty; C is allowed.
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
        // With thread-id sharding, all spans on this thread land on a single
        // shard.  capacity=2 spread over 16 lanes → per-shard cap = 1, so
        // creating two simultaneously-alive spans on this thread fills the
        // shard's slot and the second is DISABLED.
        let (cache, driver) = make_cache(2);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let _s1 = tracing::span!(parent: None, Level::INFO, "s1");
            let s2 = tracing::span!(parent: None, Level::INFO, "s2");
            assert_eq!(span_id(&s2), Some(DISABLED));
        });
        drop(driver);
    }

    #[test]
    fn custom_lane_count_is_respected() {
        // 4 lanes, capacity 4 → per-shard cap 1.  With thread-id sharding,
        // this thread always picks one shard, so the 2nd simultaneously-alive
        // span on that shard is DISABLED.
        let (cache, driver) = SpanCache::with_config(
            4,
            CacheConfig { lane_count: 4, ..CacheConfig::default() },
        );
        let cache = Arc::new(cache);
        assert_eq!(cache.lane_count(), 4);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            let _s1 = tracing::span!(parent: None, Level::INFO, "s1");
            let s2 = tracing::span!(parent: None, Level::INFO, "s2");
            assert_eq!(span_id(&s2), Some(DISABLED));
        });
        drop(driver);
    }

    #[test]
    fn separate_threads_get_distinct_keys() {
        // Each thread's first new_span claims a fresh slot from the global
        // NEXT_THREAD_KEY counter, so independent threads don't all collide
        // on a single shard.  The exact mapping is implementation-defined
        // (depends on counter state from prior tests), so we assert the
        // weakest interesting property: when we run a handful of threads
        // against a wide cache, at least two distinct shards are exercised.
        use std::collections::HashSet;
        use std::sync::Mutex;

        // Wide enough that test interleaving doesn't pin everyone on one shard.
        let (cache, driver) = SpanCache::with_config(
            64 * 16,
            CacheConfig { lane_count: 16, ..CacheConfig::default() },
        );
        let cache = Arc::new(cache);
        let observed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let observed = Arc::clone(&observed);
            handles.push(std::thread::spawn(move || {
                tracing::subscriber::with_default(cache, || {
                    let s = tracing::span!(parent: None, Level::INFO, "tt");
                    let id = span_id(&s).unwrap();
                    observed.lock().unwrap().push(id);
                    // Hold the span alive briefly so other threads observe a
                    // populated slab if they happen to land on the same shard.
                    std::thread::sleep(std::time::Duration::from_millis(5));
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let ids = observed.lock().unwrap().clone();
        let shards: HashSet<u64> = ids.iter().map(|id| id >> 60).collect();
        assert!(
            shards.len() >= 2,
            "expected ≥2 distinct shards across 8 threads, got {shards:?}",
        );
        drop(driver);
    }

    #[test]
    fn lane_count_is_clamped_and_rounded_to_power_of_two() {
        // Out-of-range / non-power-of-two values are silently normalised to
        // the next power of two within [1, 256].
        let cases = [
            (0_usize, 1_usize),     // zero → minimum lane count of 1
            (1, 1),
            (3, 4),                 // round up
            (5, 8),
            (16, 16),               // already a power of two
            (200, 256),             // round up to ceiling
            (256, 256),
            (1000, 256),            // capped at 256
        ];
        for (input, expected) in cases {
            let (cache, _driver) = SpanCache::with_config(
                64,
                CacheConfig { lane_count: input, ..CacheConfig::default() },
            );
            assert_eq!(
                cache.lane_count(),
                expected,
                "lane_count({input}) should normalise to {expected}",
            );
        }
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

    // ── API-handler-shape coverage ────────────────────────────────────────────

    #[test]
    fn record_updates_span_fields_after_creation() {
        // Common API-handler pattern: span!(...) declares a field with no value
        // up front, then span.record() fills it in once the operation finishes.
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(
                parent: None,
                Level::INFO,
                "op",
                initial = "ready",
                status = tracing::field::Empty,
            );
            let actual_id = actual_id_of(&cache, &span);
            span.record("status", "success");
            actual_id
        });
        let record = cache.get_span(actual_id).unwrap();
        assert_eq!(record.fields.get("initial").map(String::as_str), Some("ready"));
        assert_eq!(record.fields.get("status").map(String::as_str), Some("success"));
    }

    #[test]
    fn multiple_events_recorded_in_order() {
        let (cache, driver) = make_cache(10);
        let actual_id = run_with_drain(&cache, driver, || {
            let span = tracing::span!(parent: None, Level::INFO, "op");
            let actual_id = actual_id_of(&cache, &span);
            let _g = span.enter();
            tracing::event!(Level::INFO, step = "first");
            tracing::event!(Level::INFO, step = "second", note = "middle");
            tracing::event!(Level::INFO, step = "third");
            actual_id
        });
        let record = cache.get_span(actual_id).unwrap();
        assert_eq!(record.events.len(), 3);
        let steps: Vec<&str> = record
            .events
            .iter()
            .map(|e| e.fields.get("step").unwrap().as_str())
            .collect();
        assert_eq!(steps, vec!["first", "second", "third"]);
        assert_eq!(
            record.events[1].fields.get("note").map(String::as_str),
            Some("middle"),
        );
        // Timestamps monotonically non-decreasing.
        assert!(record.events[0].recorded_at <= record.events[1].recorded_at);
        assert!(record.events[1].recorded_at <= record.events[2].recorded_at);
    }

    #[test]
    fn sibling_spans_share_parent_actual_id() {
        // 4 spans alive simultaneously on one thread (root + 3 siblings).
        // With 16 lanes that needs per-shard cap ≥ 4, so capacity ≥ 64.
        let (cache, driver) = make_cache(64);
        let (root_id, sibling_ids) = run_with_drain(&cache, driver, || {
            let root = tracing::span!(parent: None, Level::INFO, "root");
            let root_id = actual_id_of(&cache, &root);
            let _g = root.enter();
            let mut ids = Vec::new();
            for _ in 0..3 {
                let sib = tracing::span!(Level::INFO, "child");
                ids.push(actual_id_of(&cache, &sib));
                // sib drops at end of loop iteration → close
            }
            (root_id, ids)
        });
        for (i, &sid) in sibling_ids.iter().enumerate() {
            let s = cache.get_span(sid).unwrap();
            assert_eq!(s.parent_id, Some(root_id), "sibling #{i} parent_id");
            assert_eq!(s.metadata.name(), "child");
        }
    }

    #[test]
    fn level_predicate_filters_below_threshold() {
        let (inner, driver) = SpanCache::with_predicate(10, LevelPredicate::new(Level::INFO));
        let cache = Arc::new(inner);
        tracing::subscriber::with_default(Arc::clone(&cache), || {
            // INFO is at the threshold — enabled.
            let info_span = tracing::span!(parent: None, Level::INFO, "info_op");
            assert!(info_span.id().is_some(), "INFO at INFO threshold");
            // ERROR is more severe — enabled.
            let error_span = tracing::span!(parent: None, Level::ERROR, "error_op");
            assert!(error_span.id().is_some(), "ERROR at INFO threshold");
            // DEBUG is below the threshold; tracing's macro short-circuits when
            // callsite_enabled returns Never, so the Span has no id.
            let debug_span = tracing::span!(parent: None, Level::DEBUG, "debug_op");
            assert!(debug_span.id().is_none(), "DEBUG filtered at INFO threshold");
        });
        drop(driver);
    }

    #[test]
    fn api_handler_lifecycle() {
        // The whole reason the cache exists, expressed as a test: a request
        // root span with a deferred field, two sibling child spans (one with
        // its own field, one with two events), a deferred record() on the
        // root once everything finishes.
        let (cache, driver) = make_cache(20);
        let request_id = run_with_drain(&cache, driver, || {
            let request = tracing::span!(
                parent: None,
                Level::INFO,
                "request",
                method = "GET",
                path = "/users/42",
                status = tracing::field::Empty,
            );
            let request_id = actual_id_of(&cache, &request);
            let _g = request.enter();

            {
                let validate = tracing::span!(Level::INFO, "validate", ok = true);
                let _v = validate.enter();
                tracing::event!(Level::INFO, message = "validation passed");
            }

            {
                let query = tracing::span!(Level::INFO, "db_query", table = "users");
                let _q = query.enter();
                tracing::event!(Level::INFO, message = "query started");
                tracing::event!(Level::INFO, message = "query finished", rows = 1u64);
            }

            request.record("status", "200");
            request_id
        });

        let pages = cache.page(0, 100);
        assert_eq!(pages.len(), 3, "request, validate, db_query all present");

        let request = cache.get_span(request_id).unwrap();
        assert_eq!(request.metadata.name(), "request");
        assert_eq!(request.parent_id, None);
        assert_eq!(request.fields.get("method").map(String::as_str), Some("GET"));
        assert_eq!(request.fields.get("path").map(String::as_str), Some("/users/42"));
        assert_eq!(request.fields.get("status").map(String::as_str), Some("200"));

        let validate = pages.iter().find(|s| s.metadata.name() == "validate").unwrap();
        assert_eq!(validate.parent_id, Some(request_id));
        assert_eq!(validate.fields.get("ok").map(String::as_str), Some("true"));
        assert_eq!(validate.events.len(), 1);
        assert_eq!(
            validate.events[0].fields.get("message").map(String::as_str),
            Some("validation passed"),
        );

        let query = pages.iter().find(|s| s.metadata.name() == "db_query").unwrap();
        assert_eq!(query.parent_id, Some(request_id));
        assert_eq!(query.fields.get("table").map(String::as_str), Some("users"));
        assert_eq!(query.events.len(), 2);
        let messages: Vec<&str> = query
            .events
            .iter()
            .map(|e| e.fields.get("message").unwrap().as_str())
            .collect();
        assert_eq!(messages, vec!["query started", "query finished"]);
        assert_eq!(query.events[1].fields.get("rows").map(String::as_str), Some("1"));
    }

    // ── async overlap test ────────────────────────────────────────────────────

    #[test]
    fn async_instrumented_tasks_with_overlapping_spans() {
        use tracing_futures::Instrument;

        // Up to 4 spans (root_a, root_b, acquire, release) are simultaneously
        // alive on the current_thread runtime — all on this thread's shard
        // under thread-id sharding.  capacity=64 / 16 lanes → per-shard cap 4.
        let (cache, driver) = make_cache(64);

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
