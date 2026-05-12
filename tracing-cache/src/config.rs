//! Tunable knobs for the cache + driver.

/// Default number of in-flight slab shards (must be a power of two).
pub const DEFAULT_LANE_COUNT: usize = 16;

/// Optional knobs for the cache + driver.  Pass to
/// [`crate::SpanCache::with_config`] /
/// [`crate::SpanCache::with_predicate_and_config`]; the no-config
/// constructors use [`CacheConfig::default`].
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
    /// Soft upper bound on each spillway channel (span + event) that moves
    /// closed records from producer threads to the driver.  When producers
    /// outrun the driver (e.g. many cores + few async workers), this caps
    /// spillway's internal buffers so the producer can't grow process
    /// memory unboundedly.  `send_many` rejects whole batches when the
    /// limit is hit and `flush_pending` drops the rejected drain (logging
    /// at DEBUG).  Decoupled from the user-facing `capacity` (the
    /// BTreeMap bound): the channel is intermediate buffering and only
    /// needs enough headroom for normal bursts.  Default: 65536.
    pub channel_capacity: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            lane_count: DEFAULT_LANE_COUNT,
            pending_batch: 32,
            driver_batch: 600,
            driver_interval: std::time::Duration::from_secs(1),
            channel_capacity: 65536,
        }
    }
}
