//! protosocket-rpc server that streams closed spans from a [`tracing_cache::SpanCache`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use protosocket::TcpSocketListener;
use protosocket_messagepack::{MessagePackDecoder, MessagePackSerializer};
use protosocket_rpc::server::{ConnectionService, RpcResponder, SocketRpcServer, SocketService};
use tracing_cache::{EnabledPredicate, SpanCache, SpanRecord};

use crate::protocol::{Request, RequestBody, Response, WireLevel};
use crate::wire::{span_to_wire, TimeBase};

// One messagepack frame per direction:
//   server reads `Request`, writes `Response`.
type ServerCodec = (
    MessagePackSerializer<Response>,
    MessagePackDecoder<Request>,
);

// Tunables — kept inline since this is the only place they're used.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STREAM_BATCH: usize = 256;

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

// ── Per-connection service ───────────────────────────────────────────────────

/// One per active client connection.  Holds an `Arc` to the shared cache so
/// it can page closed spans, plus its own filter / sampling / level state.
pub struct ConnectionState<P: EnabledPredicate> {
    cache: Arc<SpanCache<P>>,
    base: TimeBase,
    state: Arc<RwLock<StreamState>>,
    /// Memo of which root-span actual_ids the filter accepted (so descendants
    /// inherit transitively without rechecking the filter against fields the
    /// child doesn't carry).  Bounded — drops oldest entries when over budget.
    root_decisions: Arc<RwLock<HashMap<u64, bool>>>,
}

impl<P: EnabledPredicate> ConnectionState<P> {
    fn new(cache: Arc<SpanCache<P>>, base: TimeBase) -> Self {
        Self {
            cache,
            base,
            state: Arc::new(RwLock::new(StreamState::new())),
            root_decisions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<P: EnabledPredicate> ConnectionService for ConnectionState<P> {
    type Request = Request;
    type Response = Response;

    fn new_rpc(&mut self, msg: Request, responder: RpcResponder<'_, Response>) {
        match msg.body {
            RequestBody::StartStream => {
                self.state.write().unwrap().streaming = true;
                let cache = Arc::clone(&self.cache);
                let state = Arc::clone(&self.state);
                let roots = Arc::clone(&self.root_decisions);
                let base = self.base;
                tokio::spawn(responder.stream(span_stream(cache, state, roots, base)));
            }
            RequestBody::StopStream => {
                self.state.write().unwrap().streaming = false;
                responder.immediate(Response::ack());
            }
            RequestBody::SetLevel(level) => {
                self.state.write().unwrap().min_level = Some(level);
                responder.immediate(Response::ack());
            }
            RequestBody::SetSamplingRate(rate) => {
                if !(0.0..=1.0).contains(&rate) || rate.is_nan() {
                    responder.immediate(Response::error(format!(
                        "sampling rate {rate} out of range [0.0, 1.0]"
                    )));
                    return;
                }
                self.state.write().unwrap().sampling_rate = rate;
                // Sampling decisions are made at root-span creation time —
                // reset the memo so the new rate applies prospectively.
                self.root_decisions.write().unwrap().clear();
                responder.immediate(Response::ack());
            }
            RequestBody::SetFilter(f) => {
                self.state.write().unwrap().root_filter = f;
                self.root_decisions.write().unwrap().clear();
                responder.immediate(Response::ack());
            }
            RequestBody::Noop => {
                // Cancel / End control frames land here; nothing to do.
            }
        }
    }
}

/// Build the async stream of `Response::Span` messages that satisfies a
/// `StartStream` RPC.  Polls the cache's BTreeMap on a tick, applies the
/// connection's filter / level / sampling, and yields each accepted span.
fn span_stream<P: EnabledPredicate>(
    cache: Arc<SpanCache<P>>,
    state: Arc<RwLock<StreamState>>,
    roots: Arc<RwLock<HashMap<u64, bool>>>,
    base: TimeBase,
) -> impl futures_core::Stream<Item = Response> {
    async_stream::stream! {
        let mut cursor: u64 = 0;
        let mut tick = tokio::time::interval(STREAM_POLL_INTERVAL);
        loop {
            tick.tick().await;
            // Snapshot the streaming flag + filter under the lock, then drop
            // it before paging the cache so the page isn't holding two locks.
            let (streaming, min_level, sampling_rate, root_filter) = {
                let s = state.read().unwrap();
                (s.streaming, s.min_level, s.sampling_rate, s.root_filter.clone())
            };
            if !streaming {
                continue;
            }
            let batch = cache.page(cursor, STREAM_BATCH);
            if batch.is_empty() {
                continue;
            }
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
                yield Response::span(span_to_wire(&record, base));
            }
        }
    }
}

/// `record_level` is at least as severe as `floor` (per tracing's reversed
/// `Ord`: ERROR > WARN > INFO > DEBUG > TRACE).  `record_level >= floor`.
fn level_at_least(record_level: &tracing::Level, floor: WireLevel) -> bool {
    record_level >= &floor.to_tracing()
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
    record.fields.values().any(|v| v.contains(needle))
}

// ── Top-level acceptor ───────────────────────────────────────────────────────

struct Service<P: EnabledPredicate> {
    cache: Arc<SpanCache<P>>,
    base: TimeBase,
}

impl<P: EnabledPredicate> SocketService for Service<P> {
    type Codec = ServerCodec;
    type ConnectionService = ConnectionState<P>;
    type SocketListener = TcpSocketListener;

    fn codec(&self) -> Self::Codec {
        (MessagePackSerializer::default(), MessagePackDecoder::default())
    }

    fn new_stream_service(
        &self,
        _stream: &<Self::SocketListener as protosocket::SocketListener>::Stream,
    ) -> Self::ConnectionService {
        ConnectionState::new(Arc::clone(&self.cache), self.base)
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
    fn from(e: std::io::Error) -> Self { ServeError::Io(e) }
}
impl From<protosocket_rpc::Error> for ServeError {
    fn from(e: protosocket_rpc::Error) -> Self { ServeError::Rpc(e) }
}

/// Bind to `addr` and serve the console RPC protocol against `cache`.
///
/// Caller is responsible for spawning the cache's `Driver`.  The future
/// returned here runs until the listener errors out.
pub async fn serve<P: EnabledPredicate>(
    cache: Arc<SpanCache<P>>,
    addr: SocketAddr,
) -> Result<(), ServeError> {
    // listen(addr, listen_backlog, accept_timeout) — last two are optional knobs.
    let listener = TcpSocketListener::listen(addr, 1024, None)?;

    let service = Service { cache, base: TimeBase::now() };
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
