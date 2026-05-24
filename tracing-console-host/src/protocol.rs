//! Wire protocol between the host (this crate) and the console client.
//!
//! Encodes via messagepack (per-message length prefix supplied by
//! `protosocket-messagepack`).  The protosocket-rpc framework needs each
//! message to carry an id and a control code, so [`Request`] and [`Response`]
//! both wrap an enum body next to those framework fields.

use protosocket_rpc::{Message, ProtosocketControlCode};
use serde::{Deserialize, Serialize};

/// Wire representation of a captured field value.  Mirrors
/// [`tracing_cache::FieldValue`] but collapses the four string variants
/// (`Str` / `SmallString` / `SharedString` / `String`) to a single
/// `Str(String)` — the heap is unavoidable once we cross the network
/// boundary anyway.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WireFieldValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
}

impl WireFieldValue {
    /// Render the value as its printable string form.
    pub fn to_string_value(&self) -> String {
        match self {
            WireFieldValue::U64(v) => v.to_string(),
            WireFieldValue::I64(v) => v.to_string(),
            WireFieldValue::F64(v) => v.to_string(),
            WireFieldValue::Bool(v) => v.to_string(),
            WireFieldValue::Str(s) => s.clone(),
        }
    }

    /// Substring-match the printable representation.
    pub fn contains(&self, needle: &str) -> bool {
        match self {
            WireFieldValue::Str(s) => s.contains(needle),
            other => other.to_string_value().contains(needle),
        }
    }
}

impl std::fmt::Display for WireFieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireFieldValue::U64(v) => write!(f, "{v}"),
            WireFieldValue::I64(v) => write!(f, "{v}"),
            WireFieldValue::F64(v) => write!(f, "{v}"),
            WireFieldValue::Bool(v) => write!(f, "{v}"),
            WireFieldValue::Str(s) => f.write_str(s),
        }
    }
}

// ── Wire-friendly span / event types ─────────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WireLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl WireLevel {
    pub fn from_tracing(level: &tracing::Level) -> Self {
        // Match exhaustively so a tracing-core change forces an update here.
        match *level {
            tracing::Level::TRACE => WireLevel::Trace,
            tracing::Level::DEBUG => WireLevel::Debug,
            tracing::Level::INFO => WireLevel::Info,
            tracing::Level::WARN => WireLevel::Warn,
            tracing::Level::ERROR => WireLevel::Error,
        }
    }

    pub fn to_tracing(self) -> tracing::Level {
        match self {
            WireLevel::Trace => tracing::Level::TRACE,
            WireLevel::Debug => tracing::Level::DEBUG,
            WireLevel::Info => tracing::Level::INFO,
            WireLevel::Warn => tracing::Level::WARN,
            WireLevel::Error => tracing::Level::ERROR,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEvent {
    pub name: String,
    pub level: WireLevel,
    pub fields: Vec<(String, WireFieldValue)>,
    /// Nanoseconds since the Unix epoch.
    pub recorded_at_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireSpan {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub name: String,
    pub target: String,
    pub level: WireLevel,
    pub fields: Vec<(String, WireFieldValue)>,
    pub events: Vec<WireEvent>,
    /// Nanoseconds since the Unix epoch.
    pub opened_at_ns: u64,
    /// `None` if still in flight at snapshot time (currently only closed spans
    /// are streamed, so this is `Some` in practice).
    pub closed_at_ns: Option<u64>,
}

impl WireSpan {
    /// Look up a field by name; O(N) over the typically-small field list.
    pub fn field(&self, name: &str) -> Option<&WireFieldValue> {
        self.fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
}

impl WireEvent {
    pub fn field(&self, name: &str) -> Option<&WireFieldValue> {
        self.fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
}

// ── Level filter (mirrors tracing::LevelFilter, OFF + 5 levels) ─────────────

/// Wire counterpart to `tracing::level_filters::LevelFilter`.  Includes
/// `Off` because the cache-side global level can be fully disabled —
/// distinct from `WireLevel` (which is the per-span/event level and
/// therefore can't be "off").
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WireLevelFilter {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl WireLevelFilter {
    pub fn from_tracing(filter: tracing::metadata::LevelFilter) -> Self {
        use tracing::metadata::LevelFilter as L;
        if filter == L::OFF {
            WireLevelFilter::Off
        } else if filter == L::ERROR {
            WireLevelFilter::Error
        } else if filter == L::WARN {
            WireLevelFilter::Warn
        } else if filter == L::INFO {
            WireLevelFilter::Info
        } else if filter == L::DEBUG {
            WireLevelFilter::Debug
        } else {
            WireLevelFilter::Trace
        }
    }

    pub fn to_tracing(self) -> tracing::metadata::LevelFilter {
        use tracing::metadata::LevelFilter as L;
        match self {
            WireLevelFilter::Off => L::OFF,
            WireLevelFilter::Error => L::ERROR,
            WireLevelFilter::Warn => L::WARN,
            WireLevelFilter::Info => L::INFO,
            WireLevelFilter::Debug => L::DEBUG,
            WireLevelFilter::Trace => L::TRACE,
        }
    }
}

// ── Request body — every command the client can send ─────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestBody {
    /// Filler used by the framework for control-only frames (cancel / end).
    Noop,
    /// Begin streaming closed spans to the client.  Server keeps streaming
    /// until the client cancels the RPC (drops its handle).
    StartStream,
    /// Stop the current stream.  In protosocket-rpc terms, the cleaner way
    /// is to drop the streaming-RPC handle on the client; this command is
    /// here for completeness and explicit shutdown.
    StopStream,
    /// Per-connection minimum span level filter.  Only spans whose level is
    /// at least this severe (per `tracing` ordering) are streamed.
    SetLevel(WireLevel),
    /// Server-wide cache-recording level.  Mutates the subscriber's
    /// `LevelPredicate`, so it affects what the cache *records*, not
    /// just what gets streamed to one client.  The server pushes the
    /// resulting level back to every connected stream as
    /// [`ResponseBody::CacheLevel`].
    SetCacheLevel(WireLevelFilter),
    /// Server-wide chance percentage `[0.0, 100.0]` that a root
    /// span passes the cache's `ChancePredicate`.  Out-of-range or
    /// NaN values are clamped server-side; the resulting effective
    /// chance is broadcast as [`ResponseBody::CacheChance`].
    SetCacheChance(f64),
    /// Sample the stream — `1.0` = every span, `0.5` = ~half.  Applied per
    /// root-span family so a sampled root drops its whole subtree.
    SetSamplingRate(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub control: u8,
    pub body: RequestBody,
}

impl Request {
    pub fn new(body: RequestBody) -> Self {
        Self {
            id: 0,
            control: ProtosocketControlCode::Normal.as_u8(),
            body,
        }
    }
}

impl Message for Request {
    fn message_id(&self) -> u64 {
        self.id
    }
    fn control_code(&self) -> ProtosocketControlCode {
        ProtosocketControlCode::from_u8(self.control)
    }
    fn set_message_id(&mut self, id: u64) {
        self.id = id
    }
    fn cancelled(id: u64) -> Self {
        Self {
            id,
            control: ProtosocketControlCode::Cancel.as_u8(),
            body: RequestBody::Noop,
        }
    }
    fn ended(id: u64) -> Self {
        Self {
            id,
            control: ProtosocketControlCode::End.as_u8(),
            body: RequestBody::Noop,
        }
    }
}

// ── Response body — what the server can send ─────────────────────────────────

/// One-shot server-pushed handshake describing the host binary the
/// client is talking to.  Sent as the very first response on every
/// `StartStream` so the client can verify it's connected to a
/// compatible version (the host crate's version is workspace-pinned
/// to the same number as the client binary).  Kept as a struct so
/// future fields (build sha, supported features, …) don't require a
/// wire-protocol break.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WireServerInfo {
    /// `CARGO_PKG_VERSION` of the `tracing-console-host` crate on the
    /// server.  Use it to spot mismatched client/server pairs.
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseBody {
    /// Filler used by the framework for control-only frames (cancel / end).
    Noop,
    /// First message of every `StartStream` — identifies the server
    /// binary.  See [`WireServerInfo`].
    ServerInfo(WireServerInfo),
    /// One closed span snapshot.  The streaming response side of `StartStream`
    /// emits these one at a time as the host's span cache produces them.
    Span(WireSpan),
    /// Server-pushed notification of the current cache-recording level.
    /// Sent once when a `StartStream` begins and again every time the
    /// level changes (e.g. another client sent `SetCacheLevel`).  The
    /// client treats this as the source of truth for its UI display.
    CacheLevel(WireLevelFilter),
    /// Server-pushed notification of the current effective chance
    /// percentage `[0.0, 100.0]` for the cache's `ChancePredicate`.
    /// Same lifecycle as [`ResponseBody::CacheLevel`]: sent on
    /// `StartStream` and on every change.
    CacheChance(f64),
    /// Acknowledgement of a unary command (Set*, StopStream).
    Ack,
    /// Server-side error message for a command.
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub control: u8,
    pub body: ResponseBody,
}

impl Response {
    pub fn new(body: ResponseBody) -> Self {
        Self {
            id: 0,
            control: ProtosocketControlCode::Normal.as_u8(),
            body,
        }
    }
    pub fn ack() -> Self {
        Self::new(ResponseBody::Ack)
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self::new(ResponseBody::Error(msg.into()))
    }
    pub fn span(s: WireSpan) -> Self {
        Self::new(ResponseBody::Span(s))
    }
    pub fn cache_level(level: WireLevelFilter) -> Self {
        Self::new(ResponseBody::CacheLevel(level))
    }
    pub fn cache_chance(pct: f64) -> Self {
        Self::new(ResponseBody::CacheChance(pct))
    }
    pub fn server_info(version: impl Into<String>) -> Self {
        Self::new(ResponseBody::ServerInfo(WireServerInfo {
            version: version.into(),
        }))
    }
    /// Set the message id and return `self` so the call chains.  The
    /// server must echo the request id on every response so the
    /// client's completion registry (keyed by id) can route the
    /// response back to the right pending RPC.  protosocket-rpc does
    /// NOT auto-assign ids — both endpoints must do it manually, and
    /// in particular an RPC at id=0 will clobber any other RPC at
    /// id=0 on the same client connection.
    pub fn with_id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }
}

impl Message for Response {
    fn message_id(&self) -> u64 {
        self.id
    }
    fn control_code(&self) -> ProtosocketControlCode {
        ProtosocketControlCode::from_u8(self.control)
    }
    fn set_message_id(&mut self, id: u64) {
        self.id = id
    }
    fn cancelled(id: u64) -> Self {
        Self {
            id,
            control: ProtosocketControlCode::Cancel.as_u8(),
            body: ResponseBody::Noop,
        }
    }
    fn ended(id: u64) -> Self {
        Self {
            id,
            control: ProtosocketControlCode::End.as_u8(),
            body: ResponseBody::Noop,
        }
    }
}
