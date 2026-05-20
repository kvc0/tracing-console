//! protosocket-rpc server that streams closed spans from a [`tracing_cache::SpanCache`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use protosocket::TcpSocketListener;
use protosocket_messagepack::{MessagePackDecoder, MessagePackSerializer};
use protosocket_rpc::Message;
use protosocket_rpc::server::{ConnectionService, RpcResponder, SocketRpcServer, SocketService};
use tokio::sync::watch;
use tracing::metadata::LevelFilter;
use tracing_cache::{
    ChanceHandle, ChancePredicate, EnabledPredicate, LevelHandle, SpanCache, SpanRecord,
};

use crate::protocol::{Request, RequestBody, Response, WireLevel, WireLevelFilter};
use crate::wire::{TimeBase, span_to_wire};

// One messagepack frame per direction:
//   server reads `Request`, writes `Response`.
type ServerCodec = (MessagePackSerializer<Response>, MessagePackDecoder<Request>);

// Tunables — kept inline since this is the only place they're used.
//
// Polling adapts toward `STREAM_TARGET_BATCH` spans per poll: each tick the
// interval is scaled by `target / observed`, then clamped to ±20% of the
// previous interval and bounded by `STREAM_MIN_INTERVAL` / `STREAM_MAX_INTERVAL`.
// `STREAM_BATCH` is the per-page cap fed to `cache.page()`; it must exceed
// the target so observed counts above target signal a real backlog.
const STREAM_POLL_INTERVAL_INITIAL: Duration = Duration::from_millis(50);
// Per-poll page cap.  Sets the maximum throughput the loop can move per
// tick — at high arrival rates the adaptive interval bottoms out near
// the timer granularity (~1 ms on macOS, ~10–100 µs on Linux), so the
// per-tick cap times the timer rate is the throughput ceiling.  Set big
// enough that a single tick can drain the typical inter-tick backlog.
const STREAM_BATCH: usize = 4096;
// Target spans per poll.  Adaptive controller scales the interval to
// keep observed batch ≈ target — small target = low visibility latency
// at typical loads.  Under sustained overload the interval bottoms out
// at `STREAM_MIN_INTERVAL` and the per-tick `STREAM_BATCH` cap takes
// over as the throughput limit.
const STREAM_TARGET_BATCH: usize = 32;
/// Floor for the adaptive page interval.  Set to `ZERO` so that
/// under sustained overload the controller drops to "no sleep at
/// all" — the polling loop just yields to the scheduler and pages
/// again, which is the highest sustainable throughput.
const STREAM_MIN_INTERVAL: Duration = Duration::ZERO;
const STREAM_MAX_INTERVAL: Duration = Duration::from_millis(250);
const STREAM_ADJUST_RATIO: f64 = 0.2; // ±20% per tick

/// Per-connection mutable state.  Read by the streaming RPC, mutated by the
/// `Set*` RPCs and `StartStream` / `StopStream`.
#[derive(Debug, Default)]
struct StreamState {
    streaming: bool,
    min_level: Option<WireLevel>,
    sampling_rate: f64,
    /// Substring filter applied to root span name + field values.
    root_filter: Option<String>,
}

impl StreamState {
    fn new() -> Self {
        Self {
            streaming: false,
            min_level: None,
            sampling_rate: 1.0,
            root_filter: None,
        }
    }
}

// ── Cache-level broadcaster ──────────────────────────────────────────────────

/// Holds the `LevelHandle` for the cache's `LevelPredicate` plus a
/// `tokio::sync::watch` channel so every active streaming connection
/// can observe level changes without polling the handle.  A
/// `SetCacheLevel` from any client flips the handle and pushes the
/// new value into the watch — receivers wake up and forward a
/// `CacheLevel` message down their span stream.
///
/// Also tracks the number of active streaming sessions via
/// [`StreamGuard`] so the host can fall back to `OFF` when the last
/// console drops — the cache costs zero work when nobody's watching.
#[derive(Clone)]
pub struct CacheLevelBroadcast {
    level_handle: LevelHandle,
    level_tx: watch::Sender<WireLevelFilter>,
    chance_handle: ChanceHandle,
    chance_tx: watch::Sender<f64>,
    /// Trait-erased "clear the cache's BTreeMap of closed spans"
    /// hook.  Fired whenever the level transitions to `OFF` —
    /// explicit `SetCacheLevel` or the last-disconnect reset — so
    /// a paused host doesn't keep stale recordings around.
    clear_cache: Arc<dyn Fn() + Send + Sync>,
    active_streams: Arc<AtomicUsize>,
}

impl CacheLevelBroadcast {
    pub fn new(
        level_handle: LevelHandle,
        chance_handle: ChanceHandle,
        clear_cache: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        let initial_level = WireLevelFilter::from_tracing(level_handle.get());
        let initial_chance = chance_handle.get();
        let (level_tx, _) = watch::channel(initial_level);
        let (chance_tx, _) = watch::channel(initial_chance);
        Self {
            level_handle,
            level_tx,
            chance_handle,
            chance_tx,
            clear_cache,
            active_streams: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set_level(&self, filter: WireLevelFilter) {
        if filter == WireLevelFilter::Off {
            (self.clear_cache)();
        }
        self.level_handle.set(filter.to_tracing());
        let _ = self.level_tx.send(filter);
    }

    fn set_chance(&self, pct: f64) {
        // Clamp to a sensible range — the cache also clamps, but we
        // broadcast the *effective* value so clients show what the
        // host is actually applying.
        let pct = if pct.is_nan() {
            0.0
        } else {
            pct.clamp(0.0, 100.0)
        };
        self.chance_handle.set(pct);
        let _ = self.chance_tx.send(pct);
    }

    fn subscribe_level(&self) -> watch::Receiver<WireLevelFilter> {
        self.level_tx.subscribe()
    }

    fn subscribe_chance(&self) -> watch::Receiver<f64> {
        self.chance_tx.subscribe()
    }

    /// Register a new console *streaming session* (one StartStream
    /// RPC).  Returns a guard whose `Drop` decrements the counter;
    /// when the counter hits zero (the last streaming RPC ended),
    /// the level resets to `OFF`, the cache is cleared, and the
    /// new state is broadcast so any still-active stream picks it
    /// up.  Scoped to the streaming RPC (not the connection) so
    /// liveness probes that open + close a TCP socket without
    /// issuing StartStream don't trigger a spurious reset.
    fn enter_stream(&self) -> StreamGuard {
        self.active_streams.fetch_add(1, Ordering::SeqCst);
        StreamGuard {
            broadcast: self.clone(),
        }
    }
}

/// RAII guard tracking a single active StartStream RPC.  Held by
/// the `span_stream` async generator, so its `Drop` runs when the
/// generator (and thus the spawned responder.stream future) ends —
/// e.g. when the client cancels the streaming RPC by dropping the
/// `StreamingCompletion`.
pub struct StreamGuard {
    broadcast: CacheLevelBroadcast,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let prev = self.broadcast.active_streams.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // Last active streaming session — drop the cache back to
            // OFF so an idle host pays nothing for tracing dispatch,
            // wipe recorded spans, and reset chance to 100% so the
            // next console reconnects to a clean slate.
            (self.broadcast.clear_cache)();
            self.broadcast.level_handle.set(LevelFilter::OFF);
            let _ = self.broadcast.level_tx.send(WireLevelFilter::Off);
            self.broadcast.chance_handle.set(100.0);
            let _ = self.broadcast.chance_tx.send(100.0);
        }
    }
}

// ── Per-connection service ───────────────────────────────────────────────────

/// One per active client connection.  Holds an `Arc` to the shared cache so
/// it can page closed spans, plus its own filter / sampling / level state.
pub struct ConnectionState<P: EnabledPredicate> {
    cache: Arc<SpanCache<P>>,
    base: TimeBase,
    state: Arc<RwLock<StreamState>>,
    level_bus: CacheLevelBroadcast,
    /// Lazily set on the first StartStream RPC.  Liveness probes that
    /// open + close a TCP connection without ever issuing a streaming
    /// RPC leave this `None`, so their drop doesn't decrement the
    /// active-stream counter and trigger a spurious reset.  A real
    /// console always sends StartStream once, so its connection's
    /// drop reliably fires the reset.
    stream_guard: Option<StreamGuard>,
    /// Memo of which root-span actual_ids the filter accepted (so descendants
    /// inherit transitively without rechecking the filter against fields the
    /// child doesn't carry).  Bounded — drops oldest entries when over budget.
    root_decisions: Arc<RwLock<HashMap<u64, bool>>>,
}

impl<P: EnabledPredicate> ConnectionState<P> {
    fn new(cache: Arc<SpanCache<P>>, base: TimeBase, level_bus: CacheLevelBroadcast) -> Self {
        Self {
            cache,
            base,
            state: Arc::new(RwLock::new(StreamState::new())),
            level_bus,
            stream_guard: None,
            root_decisions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<P: EnabledPredicate> ConnectionService for ConnectionState<P> {
    type Request = Request;
    type Response = Response;

    fn new_rpc(&mut self, msg: Request, responder: RpcResponder<'_, Response>) {
        // Every Response must echo the request id so the client's
        // completion registry (keyed by id) routes it back to the
        // right pending RPC — see `Response::with_id`.
        let request_id = msg.message_id();
        match msg.body {
            RequestBody::StartStream => {
                self.state.write().unwrap().streaming = true;
                // First StartStream on this connection — register a
                // stream guard tied to the connection's lifetime.
                // Subsequent StartStreams are idempotent: the guard
                // already exists, the counter doesn't move twice.
                if self.stream_guard.is_none() {
                    self.stream_guard = Some(self.level_bus.enter_stream());
                }
                let cache = Arc::clone(&self.cache);
                let state = Arc::clone(&self.state);
                let roots = Arc::clone(&self.root_decisions);
                let base = self.base;
                let level_rx = self.level_bus.subscribe_level();
                let chance_rx = self.level_bus.subscribe_chance();
                tokio::spawn(responder.stream(span_stream(
                    cache, state, roots, base, level_rx, chance_rx, request_id,
                )));
            }
            RequestBody::StopStream => {
                self.state.write().unwrap().streaming = false;
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::SetLevel(level) => {
                self.state.write().unwrap().min_level = Some(level);
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::SetCacheLevel(filter) => {
                self.level_bus.set_level(filter);
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::SetCacheChance(pct) => {
                self.level_bus.set_chance(pct);
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::SetSamplingRate(rate) => {
                if !(0.0..=1.0).contains(&rate) || rate.is_nan() {
                    responder.immediate(
                        Response::error(format!("sampling rate {rate} out of range [0.0, 1.0]"))
                            .with_id(request_id),
                    );
                    return;
                }
                self.state.write().unwrap().sampling_rate = rate;
                self.root_decisions.write().unwrap().clear();
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::SetFilter(f) => {
                self.state.write().unwrap().root_filter = f;
                self.root_decisions.write().unwrap().clear();
                responder.immediate(Response::ack().with_id(request_id));
            }
            RequestBody::Noop => {}
        }
    }
}

/// Build the async stream of `Response` messages that satisfies a
/// `StartStream` RPC.  Yields:
///
/// * an initial `CacheLevel` carrying the current cache level (so the
///   client's UI is in sync the moment streaming begins),
/// * every span the cache produces (after per-connection level /
///   sampling / filter), and
/// * a fresh `CacheLevel` every time the level changes (broadcast
///   from any client's `SetCacheLevel`).
fn span_stream<P: EnabledPredicate>(
    cache: Arc<SpanCache<P>>,
    state: Arc<RwLock<StreamState>>,
    roots: Arc<RwLock<HashMap<u64, bool>>>,
    base: TimeBase,
    mut level_rx: watch::Receiver<WireLevelFilter>,
    mut chance_rx: watch::Receiver<f64>,
    request_id: u64,
) -> impl futures_core::Stream<Item = Response> {
    async_stream::stream! {
        // Push current level + chance first so the client renders
        // its switcher / chance UI before any spans land.
        let initial_level = *level_rx.borrow_and_update();
        yield Response::cache_level(initial_level).with_id(request_id);
        let initial_chance = *chance_rx.borrow_and_update();
        yield Response::cache_chance(initial_chance).with_id(request_id);

        let mut cursor: u64 = 0;
        let mut interval = STREAM_POLL_INTERVAL_INITIAL;
        loop {
            if interval.is_zero() {
                // Saturated path: don't sleep at all.  Poll the
                // watches non-blocking and immediately page again.
                // `yield_now` is the only cooperative point so we
                // don't starve other tokio tasks on this runtime.
                if level_rx.has_changed().unwrap_or(false) {
                    let lvl = *level_rx.borrow_and_update();
                    yield Response::cache_level(lvl).with_id(request_id);
                    continue;
                }
                if chance_rx.has_changed().unwrap_or(false) {
                    let pct = *chance_rx.borrow_and_update();
                    yield Response::cache_chance(pct).with_id(request_id);
                    continue;
                }
                tokio::task::yield_now().await;
            } else {
                tokio::select! {
                    changed = level_rx.changed() => {
                        if changed.is_err() { break; }
                        let lvl = *level_rx.borrow_and_update();
                        yield Response::cache_level(lvl).with_id(request_id);
                        continue;
                    }
                    changed = chance_rx.changed() => {
                        if changed.is_err() { break; }
                        let pct = *chance_rx.borrow_and_update();
                        yield Response::cache_chance(pct).with_id(request_id);
                        continue;
                    }
                    _ = tokio::time::sleep(interval) => {}
                }
            }
            // Snapshot the streaming flag + filter under the lock, then drop
            // it before paging the cache so the page isn't holding two locks.
            let (streaming, min_level, sampling_rate, root_filter) = {
                let s = state.read().unwrap();
                (s.streaming, s.min_level, s.sampling_rate, s.root_filter.clone())
            };
            if !streaming {
                // Don't adapt while paused — `count == 0` here means the
                // client said stop, not "nothing was produced".
                continue;
            }
            let batch = cache.page(cursor, STREAM_BATCH);
            let count = batch.len();
            for record in batch {
                cursor = record.id;
                if let Some(min) = min_level {
                    if !level_at_least(record.metadata.level(), min) {
                        continue;
                    }
                }
                if !sampling_passes(&record, sampling_rate) {
                    continue;
                }
                if !filter_passes(&record, &root_filter, &roots) {
                    continue;
                }
                yield Response::span(span_to_wire(&record, base)).with_id(request_id);
            }
            interval = adjust_interval(interval, count);
        }
    }
}

/// Scale `current` toward yielding `STREAM_TARGET_BATCH` spans on the next
/// poll, capped at ±`STREAM_ADJUST_RATIO` per call and clamped to
/// `[STREAM_MIN_INTERVAL, STREAM_MAX_INTERVAL]`.  An observation of zero
/// spans treats the ratio as if `count = 1` (i.e., grow by the maximum
/// allowed amount).
fn adjust_interval(current: Duration, count: usize) -> Duration {
    // ZERO is a fixed point for multiplicative scaling — `ZERO.mul_f64(x)`
    // is always ZERO, so once the controller lands on the no-sleep path it
    // can't climb back out by itself.  That's only the right steady state
    // when pages are genuinely full; otherwise we burn per-page lock cost
    // on 1-span batches.  Seed back to 1 µs so the next adjustment has a
    // non-zero base to scale from.
    if current.is_zero() {
        return if count >= STREAM_TARGET_BATCH {
            Duration::ZERO
        } else {
            Duration::from_micros(1)
        };
    }
    let ratio = STREAM_TARGET_BATCH as f64 / count.max(1) as f64;
    let raw = current.mul_f64(ratio);
    let max_up = current.mul_f64(1.0 + STREAM_ADJUST_RATIO);
    let min_down = current.mul_f64(1.0 - STREAM_ADJUST_RATIO);
    let clamped = raw.clamp(min_down, max_up);
    let bounded = clamped.clamp(STREAM_MIN_INTERVAL, STREAM_MAX_INTERVAL);
    // Below the OS's practical sleep resolution (~1 µs is a hard
    // floor for tokio anyway), snap to ZERO so the loop hits the
    // no-sleep fast path.  Without this, `mul_f64`'s round-to-
    // -nearest gets stuck at 1 ns when shrinking by a constant
    // ratio and the controller never crosses into "saturated".
    if bounded < Duration::from_micros(1) {
        Duration::ZERO
    } else {
        bounded
    }
}

/// True iff `record_level` is at least as severe as `floor`.  In tracing's
/// reversed `Ord`, lower-severity levels compare *greater* (ERROR < WARN <
/// INFO < DEBUG < TRACE), so the "at least as severe" relation is `<=`.
/// E.g. with floor=INFO: INFO/WARN/ERROR pass, DEBUG/TRACE don't.
fn level_at_least(record_level: &tracing::Level, floor: WireLevel) -> bool {
    record_level <= &floor.to_tracing()
}

/// Hash-based sampling so the same root id deterministically passes/fails.
/// Descendants follow the root's decision (the cache feeds us spans in
/// increasing actual_id order, so a root is always streamed before its kids
/// — but we still memoise via `root_decisions` to handle late-arriving ids).
fn sampling_passes(record: &SpanRecord, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    // Use the root id (or this id, if a root) to pick the bucket.
    let bucket_id = record.parent_id.unwrap_or(record.id);
    // Cheap deterministic hash — splitmix-style.
    let mut x = bucket_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    x ^= x >> 29;
    let frac = (x as f64) / (u64::MAX as f64);
    frac < rate
}

/// Filter applies to root spans only and propagates transitively.  A span
/// passes if its root passed.  We memoise the per-root decision in
/// `root_decisions` so descendants don't re-evaluate.
fn filter_passes(
    record: &SpanRecord,
    filter: &Option<String>,
    roots: &Arc<RwLock<HashMap<u64, bool>>>,
) -> bool {
    let needle = match filter {
        None => return true,
        Some(s) if s.is_empty() => return true,
        Some(s) => s.as_str(),
    };
    // Spans without a parent are roots.  For descendants we look up the root's
    // decision — but `parent_id` only points one level up, not at the root.
    // Use the cache walk: if we don't have a memo for this id, and it has a
    // parent, inherit the parent's decision (which itself was memoised when
    // the parent was streamed earlier in this monotonic-id-ordered scan).
    if record.parent_id.is_none() {
        let decision = root_matches(record, needle);
        roots.write().unwrap().insert(record.id, decision);
        return decision;
    }
    // Descendant: inherit the chain.
    let parent_id = record.parent_id.unwrap();
    let memo = roots.read().unwrap();
    let parent_decision = memo.get(&parent_id).copied();
    drop(memo);
    let decision = parent_decision.unwrap_or(false);
    // Propagate the decision down so this span's children find it directly.
    roots.write().unwrap().insert(record.id, decision);
    decision
}

fn root_matches(record: &SpanRecord, needle: &str) -> bool {
    if record.metadata.name().contains(needle) {
        return true;
    }
    record.fields.iter().any(|(_, v)| v.contains(needle))
}

// ── Top-level acceptor ───────────────────────────────────────────────────────

struct Service<P: EnabledPredicate> {
    cache: Arc<SpanCache<P>>,
    base: TimeBase,
    level_bus: CacheLevelBroadcast,
}

impl<P: EnabledPredicate> SocketService for Service<P> {
    type Codec = ServerCodec;
    type ConnectionService = ConnectionState<P>;
    type SocketListener = TcpSocketListener;

    fn codec(&self) -> Self::Codec {
        (
            MessagePackSerializer::default(),
            MessagePackDecoder::default(),
        )
    }

    fn new_stream_service(
        &self,
        _stream: &<Self::SocketListener as protosocket::SocketListener>::Stream,
    ) -> Self::ConnectionService {
        ConnectionState::new(Arc::clone(&self.cache), self.base, self.level_bus.clone())
    }
}

/// Errors returned by [`serve`].
#[derive(Debug)]
pub enum ServeError {
    Io(std::io::Error),
    Rpc(protosocket_rpc::Error),
}
impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::Io(e) => write!(f, "io: {e}"),
            ServeError::Rpc(e) => write!(f, "rpc: {e}"),
        }
    }
}
impl std::error::Error for ServeError {}
impl From<std::io::Error> for ServeError {
    fn from(e: std::io::Error) -> Self {
        ServeError::Io(e)
    }
}
impl From<protosocket_rpc::Error> for ServeError {
    fn from(e: protosocket_rpc::Error) -> Self {
        ServeError::Rpc(e)
    }
}

/// Bind to `addr` and serve the console RPC protocol against `cache`.
///
/// `level_handle` is the `LevelHandle` returned by the cache's
/// `LevelPredicate`; the server uses it to apply `SetCacheLevel`
/// requests and to broadcast the resulting level to every connected
/// stream.  Caller is responsible for spawning the cache's `Driver`
/// and for keeping `level_handle` consistent with what the cache
/// actually uses.  The future runs until the listener errors out.
pub async fn serve<P: EnabledPredicate>(
    cache: Arc<SpanCache<P>>,
    level_handle: LevelHandle,
    chance_handle: ChanceHandle,
    addr: SocketAddr,
) -> Result<(), ServeError> {
    // listen(addr, listen_backlog, accept_timeout) — last two are optional knobs.
    let listener = TcpSocketListener::listen(addr, 1024, None)?;

    let clear_cache: Arc<dyn Fn() + Send + Sync> = {
        let cache = Arc::clone(&cache);
        Arc::new(move || cache.clear())
    };
    let service = Service {
        cache,
        base: TimeBase::now(),
        level_bus: CacheLevelBroadcast::new(level_handle, chance_handle, clear_cache),
    };
    let server: SocketRpcServer<Service<P>, _> = SocketRpcServer::new(
        listener,
        service,
        /* max_buffer_length */ 16 * 1024 * 1024,
        /* buffer_allocation_increment */ 64 * 1024,
        /* max_queued_outbound_messages */ 4096,
    )?;
    server.await?;
    Ok(())
}

// ── Integration tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener as StdTcpListener;
    use std::time::Duration;

    use futures::StreamExt;
    use protosocket_messagepack::{MessagePackDecoder, MessagePackSerializer};
    use protosocket_rpc::client::{self, Configuration, RpcClient, TcpStreamConnector};
    use tracing_cache::SpanCache;

    use crate::protocol::{ResponseBody, WireLevel};

    type ClientCodec = (MessagePackSerializer<Request>, MessagePackDecoder<Response>);

    // ── adjust_interval unit tests ───────────────────────────────────────────

    #[test]
    fn adjust_interval_holds_steady_at_target() {
        let i = Duration::from_millis(50);
        // count == target: ratio == 1.0, no change.
        assert_eq!(adjust_interval(i, STREAM_TARGET_BATCH), i);
    }

    #[test]
    fn adjust_interval_speeds_up_when_over_target_capped_at_20pct() {
        let i = Duration::from_millis(100);
        // count >> target: raw ratio < 0.8, so clamped to 0.8 × current.
        let next = adjust_interval(i, STREAM_TARGET_BATCH * 100);
        assert_eq!(next, Duration::from_millis(80));
    }

    #[test]
    fn adjust_interval_slows_down_when_under_target_capped_at_20pct() {
        let i = Duration::from_millis(100);
        // count << target (or zero): raw ratio > 1.2, so clamped to 1.2 × current.
        let next_zero = adjust_interval(i, 0);
        assert_eq!(next_zero, Duration::from_millis(120));
        let next_one = adjust_interval(i, 1);
        assert_eq!(next_one, Duration::from_millis(120));
    }

    #[test]
    fn adjust_interval_takes_ratio_when_inside_20pct_band() {
        // count just above target: ratio inside [0.8, 1.2].
        let i = Duration::from_millis(110);
        let count = STREAM_TARGET_BATCH + STREAM_TARGET_BATCH / 10;
        let next = adjust_interval(i, count);
        // 110 ms × target / count.
        let expected = i.mul_f64(STREAM_TARGET_BATCH as f64 / count as f64);
        assert_eq!(next, expected);
    }

    #[test]
    fn adjust_interval_clamps_to_min_floor() {
        // At the ZERO floor with saturated batches: stays at ZERO.
        let i = STREAM_MIN_INTERVAL;
        let next = adjust_interval(i, STREAM_TARGET_BATCH * 100);
        assert_eq!(next, STREAM_MIN_INTERVAL);
    }

    #[test]
    fn adjust_interval_escapes_zero_when_batches_undersized() {
        // At ZERO but pages are tiny: re-seed at 1 µs so multiplicative
        // scaling has a non-zero base to climb from.
        let next = adjust_interval(Duration::ZERO, 1);
        assert_eq!(next, Duration::from_micros(1));
    }

    #[test]
    fn adjust_interval_clamps_to_max_ceiling() {
        // Already at the ceiling and observing zero spans: stays at ceiling.
        let i = STREAM_MAX_INTERVAL;
        let next = adjust_interval(i, 0);
        assert_eq!(next, STREAM_MAX_INTERVAL);
    }

    #[test]
    fn adjust_interval_reaches_min_in_bounded_steps_under_overload() {
        // Sustained overload: each step shrinks by exactly 20%, so
        // from 50 ms down to `STREAM_MIN_INTERVAL` (= ZERO) takes
        // ~log_0.8(1 ns / 50 ms) ≈ 80 steps before nanosecond
        // precision rounds the result to zero.
        let mut i = Duration::from_millis(50);
        let mut steps = 0;
        while i > STREAM_MIN_INTERVAL && steps < 1000 {
            i = adjust_interval(i, STREAM_TARGET_BATCH * 1000);
            steps += 1;
        }
        assert_eq!(i, STREAM_MIN_INTERVAL);
        assert!(steps < 100, "took {steps} steps to reach floor");
    }

    /// Bind a std listener to ephemeral port, capture the port, drop it.  The
    /// next bind on this port (by `serve`) reuses it (SO_REUSEADDR is set).
    /// There's a tiny race window — fine on a developer box and CI.
    fn pick_addr() -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    /// Build a SpanCache, emit spans by running `f` under it, then synchronously
    /// drain so the BTreeMap has every closed span before any test assertion.
    fn cache_with_spans<F>(
        f: F,
    ) -> (
        Arc<SpanCache<ChancePredicate<tracing_cache::LevelPredicate>>>,
        LevelHandle,
        ChanceHandle,
    )
    where
        F: FnOnce(),
    {
        let level = tracing_cache::LevelPredicate::with_filter(
            tracing::metadata::LevelFilter::TRACE,
        );
        let level_handle = level.handle();
        let predicate = ChancePredicate::new(level, 100.0);
        let chance_handle = predicate.handle();
        let (cache, driver) = SpanCache::with_predicate(1024, predicate);
        let cache = Arc::new(cache);
        tracing::subscriber::with_default(Arc::clone(&cache), f);
        cache.flush_pending();
        driver.drain_sync();
        (cache, level_handle, chance_handle)
    }

    /// Spawn `serve` on a free port; return the address and a JoinHandle for
    /// abort-on-drop semantics.  Briefly retries connect to confirm bind.
    async fn spawn_server<P: EnabledPredicate>(
        cache: Arc<SpanCache<P>>,
        level_handle: LevelHandle,
        chance_handle: ChanceHandle,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let addr = pick_addr();
        let server_cache = Arc::clone(&cache);
        let serve_level = level_handle.clone();
        let serve_chance = chance_handle.clone();
        let handle = tokio::spawn(async move {
            // Discard the result; the test aborts this task at the end.
            let _ = serve(server_cache, serve_level, serve_chance, addr).await;
        });
        // Wait for the server to actually be listening.
        for _ in 0..50 {
            if std::net::TcpStream::connect(addr).is_ok() {
                return (addr, handle);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("server never came up on {addr}");
    }

    async fn connect_client(addr: SocketAddr) -> RpcClient<Request, Response> {
        let cfg = Configuration::new(TcpStreamConnector);
        let (rpc_client, conn) = client::connect::<ClientCodec, _>(addr, &cfg).await.unwrap();
        // Drive the connection's I/O loop in the background.
        tokio::spawn(conn);
        rpc_client
    }

    /// Try to receive `n` Span responses from the stream within `total_timeout`.
    async fn collect_spans(
        stream: &mut (impl futures::Stream<Item = Result<Response, protosocket_rpc::Error>> + Unpin),
        n: usize,
        total_timeout: Duration,
    ) -> Vec<crate::WireSpan> {
        let mut out = Vec::with_capacity(n);
        let deadline = tokio::time::Instant::now() + total_timeout;
        while out.len() < n {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(resp))) => {
                    if let ResponseBody::Span(s) = resp.body {
                        out.push(s);
                    }
                }
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => break,
            }
        }
        out
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn start_stream_delivers_closed_spans() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            for _ in 0..3 {
                let span = tracing::span!(parent: None, tracing::Level::INFO, "test_a");
                let _g = span.enter();
            }
        });

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;
        let mut stream = client
            .send_streaming(Request::new(RequestBody::StartStream))
            .unwrap();

        let received = collect_spans(&mut stream, 3, Duration::from_secs(2)).await;
        assert_eq!(received.len(), 3);
        assert!(received.iter().all(|s| s.name == "test_a"));
        // Times present and consistent.
        assert!(received.iter().all(|s| s.closed_at_ns.is_some()));

        server.abort();
    }

    #[tokio::test]
    async fn stop_stream_halts_delivery() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            for _ in 0..5 {
                let span = tracing::span!(parent: None, tracing::Level::INFO, "test_b");
                let _g = span.enter();
            }
        });

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;
        let mut stream = client
            .send_streaming(Request::new(RequestBody::StartStream))
            .unwrap();

        // Drain at least one span so we know streaming is live.
        let initial = collect_spans(&mut stream, 1, Duration::from_secs(2)).await;
        assert_eq!(initial.len(), 1);

        // Send StopStream as a unary RPC, expect Ack.
        let ack = client
            .send_unary(Request::new(RequestBody::StopStream))
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(ack.body, ResponseBody::Ack));

        // Two ticks (50ms each) should be enough for the loop to read
        // streaming=false. Then ensure no further spans arrive in 300ms.
        let drained_after_stop = collect_spans(&mut stream, 100, Duration::from_millis(300)).await;
        // The race window allows up to a single tick's batch in flight; assert
        // the stream eventually quiesces rather than that exactly zero land.
        assert!(
            drained_after_stop.len() < 5,
            "stream did not stop: got {} more spans after StopStream",
            drained_after_stop.len()
        );

        server.abort();
    }

    #[tokio::test]
    async fn set_level_filters_below_threshold() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            // The cache predicate is TRACE so DEBUG is captured; the host's
            // SetLevel must be the thing that filters DEBUG on the wire.
            let span_info = tracing::span!(parent: None, tracing::Level::INFO, "info_span");
            drop(span_info);
            let span_debug = tracing::span!(parent: None, tracing::Level::DEBUG, "debug_span");
            drop(span_debug);
        });

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;

        let ack = client
            .send_unary(Request::new(RequestBody::SetLevel(WireLevel::Info)))
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(ack.body, ResponseBody::Ack));

        let mut stream = client
            .send_streaming(Request::new(RequestBody::StartStream))
            .unwrap();
        let received = collect_spans(&mut stream, 5, Duration::from_millis(500)).await;

        let names: Vec<_> = received.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["info_span"], "got: {names:?}");

        server.abort();
    }

    #[tokio::test]
    async fn set_sampling_rate_zero_drops_all() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            for _ in 0..5 {
                let span = tracing::span!(parent: None, tracing::Level::INFO, "sampled");
                let _g = span.enter();
            }
        });

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;

        client
            .send_unary(Request::new(RequestBody::SetSamplingRate(0.0)))
            .unwrap()
            .await
            .unwrap();
        let mut stream = client
            .send_streaming(Request::new(RequestBody::StartStream))
            .unwrap();

        let received = collect_spans(&mut stream, 5, Duration::from_millis(400)).await;
        assert!(
            received.is_empty(),
            "rate=0 should drop everything; got {received:?}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn set_filter_matches_root_and_inherits_to_children() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            // Two trees; "alpha" matches the filter, "beta" doesn't.
            {
                let root = tracing::span!(parent: None, tracing::Level::INFO, "alpha");
                let _g = root.enter();
                let _child = tracing::span!(tracing::Level::INFO, "alpha_child");
            }
            {
                let root = tracing::span!(parent: None, tracing::Level::INFO, "beta");
                let _g = root.enter();
                let _child = tracing::span!(tracing::Level::INFO, "beta_child");
            }
        });

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;

        client
            .send_unary(Request::new(RequestBody::SetFilter(Some(
                "alpha".to_string(),
            ))))
            .unwrap()
            .await
            .unwrap();
        let mut stream = client
            .send_streaming(Request::new(RequestBody::StartStream))
            .unwrap();

        let received = collect_spans(&mut stream, 4, Duration::from_millis(500)).await;
        let mut names: Vec<_> = received.iter().map(|s| s.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["alpha".to_string(), "alpha_child".to_string()]);

        server.abort();
    }

    /// Setting the cache level must not end the streaming RPC — the
    /// server should push a fresh `CacheLevel` notification on the
    /// existing stream and continue streaming spans after.
    #[tokio::test]
    async fn set_cache_level_keeps_stream_open() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            // Pre-populate a span so the stream has content to deliver.
            let s = tracing::span!(parent: None, tracing::Level::INFO, "pre_level");
            drop(s);
        });
        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;
        // Distinct ids — the framework matches responses to RPCs by
        // request id, and id=0 would clobber on the client side.
        let mut start = Request::new(RequestBody::StartStream);
        start.id = 100;
        let mut stream = client.send_streaming(start).unwrap();

        // First push is always the initial CacheLevel snapshot.
        let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(first.body, ResponseBody::CacheLevel(_)),
            "first message should be CacheLevel, got {:?}",
            first.body
        );

        // Change the level — the unary should ack while the streaming
        // RPC stays open.
        let mut set = Request::new(RequestBody::SetCacheLevel(WireLevelFilter::Off));
        set.id = 101;
        let ack = client.send_unary(set).unwrap().await.unwrap();
        assert!(matches!(ack.body, ResponseBody::Ack));

        // Next stream item must be the updated CacheLevel; it must
        // arrive (not end-of-stream) within a generous window.
        let mut next_level: Option<WireLevelFilter> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline && next_level.is_none() {
            let item = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
            let Ok(Some(Ok(resp))) = item else { continue };
            match resp.body {
                ResponseBody::CacheLevel(l) => next_level = Some(l),
                // Initial chance push + any chance broadcasts are fine
                // — we just don't care about them in this test.
                ResponseBody::CacheChance(_) => continue,
                ResponseBody::Span(_) => continue,
                other => panic!("unexpected stream item: {other:?}"),
            }
        }
        assert_eq!(
            next_level,
            Some(WireLevelFilter::Off),
            "stream did not yield the updated CacheLevel (probably ended)",
        );

        server.abort();
    }

    /// When the last streaming RPC drops, the server should reset
    /// the cache level to `OFF`.  Verified by: connect a client at
    /// non-OFF level, drop the client, then reconnect and observe
    /// the initial CacheLevel is OFF.
    #[tokio::test]
    async fn level_resets_to_off_when_last_console_disconnects() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {
            let s = tracing::span!(parent: None, tracing::Level::INFO, "anchor");
            drop(s);
        });
        // Start at INFO via the cache predicate handle.
        level_handle.set(LevelFilter::INFO);

        let (addr, server) = spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;

        // Open a streaming RPC; drop it immediately to mimic a console
        // disconnect.
        {
            let client = connect_client(addr).await;
            let mut start = Request::new(RequestBody::StartStream);
            start.id = 200;
            let _stream = client.send_streaming(start).unwrap();
            // Wait a beat for the StartStream to register on the
            // server side, then drop everything.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Give the server time to notice the disconnect and run the
        // StreamGuard's Drop.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Level handle should now read OFF.
        assert_eq!(
            level_handle.get(),
            LevelFilter::OFF,
            "level should have reset to OFF after last console disconnected",
        );

        server.abort();
    }

    // ── sampling_passes / filter_passes unit tests ───────────────────────────

    use std::time::Instant;
    use tracing::callsite::{Callsite, DefaultCallsite, Identifier};
    use tracing::field::FieldSet;
    use tracing::metadata::Kind;
    use tracing_cache::{FieldList, SpanRecord};

    static SAMPLING_CALLSITE: DefaultCallsite = {
        static META: tracing::Metadata<'static> = tracing::Metadata::new(
            "sampling_test",
            "sampling::test",
            tracing::Level::INFO,
            None,
            None,
            None,
            FieldSet::new(&[], Identifier(&SAMPLING_CALLSITE)),
            Kind::SPAN,
        );
        DefaultCallsite::new(&META)
    };

    fn synth_span(id: u64, parent_id: Option<u64>) -> SpanRecord {
        SpanRecord {
            id,
            parent_id,
            metadata: SAMPLING_CALLSITE.metadata(),
            fields: FieldList::default(),
            events: Vec::new(),
            opened_at: Instant::now(),
            closed_at: Some(Instant::now()),
        }
    }

    #[test]
    fn sampling_passes_rate_one_short_circuits_true() {
        // rate >= 1.0 must accept every span regardless of id hash.
        for id in [0u64, 1, 17, u64::MAX, 0x9E37_79B9_7F4A_7C15] {
            assert!(sampling_passes(&synth_span(id, None), 1.0));
        }
    }

    #[test]
    fn sampling_passes_rate_zero_short_circuits_false() {
        for id in [0u64, 1, 17, u64::MAX] {
            assert!(!sampling_passes(&synth_span(id, None), 0.0));
        }
    }

    #[test]
    fn sampling_passes_is_deterministic_per_root_id() {
        // Repeating the call with the same record must yield the same
        // answer — otherwise children inheriting a root's decision
        // would race against their root's hash.
        for id in 1u64..=20 {
            let r = synth_span(id, None);
            let first = sampling_passes(&r, 0.5);
            for _ in 0..3 {
                assert_eq!(sampling_passes(&r, 0.5), first, "id={id}");
            }
        }
    }

    #[test]
    fn sampling_passes_children_inherit_parents_root_id_bucket() {
        // Children with `parent_id = Some(root)` must hash on the
        // root, not on their own id.  Pick a root id that does pass
        // at rate=0.5 and demonstrate the child gets the same answer.
        let root = synth_span(7, None);
        let want = sampling_passes(&root, 0.5);
        // Several different child ids, all under root=7 → all match.
        for child_id in [100u64, 200, 300, u64::MAX] {
            let child = synth_span(child_id, Some(7));
            assert_eq!(sampling_passes(&child, 0.5), want);
        }
    }

    #[test]
    fn sampling_passes_partitions_population_near_target_rate() {
        // Coarse distribution sanity-check — splitmix should produce
        // close-to-uniform fractions, so rate=0.3 over a large pool
        // should pass roughly 30% of distinct root ids.
        let rate = 0.3;
        let n = 5_000u64;
        let mut passed = 0usize;
        for id in 1..=n {
            if sampling_passes(&synth_span(id, None), rate) {
                passed += 1;
            }
        }
        let frac = passed as f64 / n as f64;
        assert!(
            (frac - rate).abs() < 0.03,
            "frac={frac} rate={rate} — hash distribution drifted",
        );
    }

    #[test]
    fn filter_passes_descendant_inherits_root_decision_via_memo() {
        let filter = Some("alpha".to_string());
        let roots: Arc<RwLock<HashMap<u64, bool>>> =
            Arc::new(RwLock::new(HashMap::new()));
        // Pre-populate the memo with parent=42 mapped to `false`.
        roots.write().unwrap().insert(42, false);

        // A child whose `parent_id = Some(42)` inherits the parent's
        // `false` regardless of its own metadata name.
        let child = synth_span(43, Some(42));
        assert!(!filter_passes(&child, &filter, &roots));
        // The child's id is now also memoised so its children inherit.
        assert_eq!(roots.read().unwrap().get(&43).copied(), Some(false));
    }

    #[test]
    fn filter_passes_root_caches_match_in_memo() {
        // Root with name == "sampling_test" matches "sampling".
        let filter = Some("sampling".to_string());
        let roots: Arc<RwLock<HashMap<u64, bool>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let root = synth_span(10, None);
        assert!(filter_passes(&root, &filter, &roots));
        assert_eq!(roots.read().unwrap().get(&10).copied(), Some(true));
    }

    #[test]
    fn filter_passes_empty_or_none_filter_accepts_everything() {
        let roots: Arc<RwLock<HashMap<u64, bool>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let s = synth_span(1, None);
        assert!(filter_passes(&s, &None, &roots));
        assert!(filter_passes(&s, &Some(String::new()), &roots));
        // Neither path should touch the memo.
        assert!(roots.read().unwrap().is_empty());
    }

    // ── SetSamplingRate RPC validation ───────────────────────────────────────

    #[tokio::test]
    async fn set_sampling_rate_rejects_out_of_range() {
        let (cache, level_handle, chance_handle) = cache_with_spans(|| {});
        let (addr, server) =
            spawn_server(Arc::clone(&cache), level_handle.clone(), chance_handle.clone()).await;
        let client = connect_client(addr).await;

        for bad in [1.5_f64, -0.1, f64::NAN] {
            let resp = client
                .send_unary(Request::new(RequestBody::SetSamplingRate(bad)))
                .unwrap()
                .await
                .unwrap();
            match resp.body {
                ResponseBody::Error(msg) => {
                    assert!(
                        msg.contains("sampling rate"),
                        "unexpected error message for {bad}: {msg}",
                    );
                }
                other => panic!("expected Error for rate={bad}, got {other:?}"),
            }
        }
        server.abort();
    }
}
