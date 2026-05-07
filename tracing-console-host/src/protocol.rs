//! Wire protocol between the host (this crate) and the console client.
//!
//! Encodes via messagepack (per-message length prefix supplied by
//! `protosocket-messagepack`).  The protosocket-rpc framework needs each
//! message to carry an id and a control code, so [`Request`] and [`Response`]
//! both wrap an enum body next to those framework fields.

use std::collections::HashMap;

use protosocket_rpc::{Message, ProtosocketControlCode};
use serde::{Deserialize, Serialize};

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
    pub fields: HashMap<String, String>,
    /// Nanoseconds since the host's `Instant` reference.
    pub recorded_at_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireSpan {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub name: String,
    pub target: String,
    pub level: WireLevel,
    pub fields: HashMap<String, String>,
    pub events: Vec<WireEvent>,
    /// Nanoseconds since the host's `Instant` reference.
    pub opened_at_ns: u64,
    /// `None` if still in flight at snapshot time (currently only closed spans
    /// are streamed, so this is `Some` in practice).
    pub closed_at_ns: Option<u64>,
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
    /// Sample the stream — `1.0` = every span, `0.5` = ~half.  Applied per
    /// root-span family so a sampled root drops its whole subtree.
    SetSamplingRate(f64),
    /// Substring filter applied to root span name + fields.  Applies
    /// transitively: if the root matches, descendants stream too; if not,
    /// none of the family is streamed.
    SetFilter(Option<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub control: u8,
    pub body: RequestBody,
}

impl Request {
    pub fn new(body: RequestBody) -> Self {
        Self { id: 0, control: ProtosocketControlCode::Normal.as_u8(), body }
    }
}

impl Message for Request {
    fn message_id(&self) -> u64 { self.id }
    fn control_code(&self) -> ProtosocketControlCode {
        ProtosocketControlCode::from_u8(self.control)
    }
    fn set_message_id(&mut self, id: u64) { self.id = id }
    fn cancelled(id: u64) -> Self {
        Self { id, control: ProtosocketControlCode::Cancel.as_u8(), body: RequestBody::Noop }
    }
    fn ended(id: u64) -> Self {
        Self { id, control: ProtosocketControlCode::End.as_u8(), body: RequestBody::Noop }
    }
}

// ── Response body — what the server can send ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseBody {
    /// Filler used by the framework for control-only frames (cancel / end).
    Noop,
    /// One closed span snapshot.  The streaming response side of `StartStream`
    /// emits these one at a time as the host's span cache produces them.
    Span(WireSpan),
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
        Self { id: 0, control: ProtosocketControlCode::Normal.as_u8(), body }
    }
    pub fn ack() -> Self { Self::new(ResponseBody::Ack) }
    pub fn error(msg: impl Into<String>) -> Self { Self::new(ResponseBody::Error(msg.into())) }
    pub fn span(s: WireSpan) -> Self { Self::new(ResponseBody::Span(s)) }
}

impl Message for Response {
    fn message_id(&self) -> u64 { self.id }
    fn control_code(&self) -> ProtosocketControlCode {
        ProtosocketControlCode::from_u8(self.control)
    }
    fn set_message_id(&mut self, id: u64) { self.id = id }
    fn cancelled(id: u64) -> Self {
        Self { id, control: ProtosocketControlCode::Cancel.as_u8(), body: ResponseBody::Noop }
    }
    fn ended(id: u64) -> Self {
        Self { id, control: ProtosocketControlCode::End.as_u8(), body: ResponseBody::Noop }
    }
}
