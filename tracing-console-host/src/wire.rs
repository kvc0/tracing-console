//! Conversion from in-memory `SpanRecord` / `EventRecord` to the wire types.

use std::time::Instant;

use tracing_cache::{EventRecord, SpanRecord};

use crate::protocol::{WireEvent, WireLevel, WireSpan};

/// Reference point used to serialize `Instant`s as `u64` nanoseconds.  Each
/// host owns one of these, captured at server start.  Clients see absolute
/// "ns since this host started" timestamps.
#[derive(Debug, Clone, Copy)]
pub struct TimeBase(pub Instant);

impl TimeBase {
    pub fn now() -> Self { Self(Instant::now()) }

    fn ns(self, t: Instant) -> u64 {
        // `Instant` arithmetic saturates at zero — fine: events captured
        // pre-server-start (impossible in practice) would just appear at 0.
        t.saturating_duration_since(self.0).as_nanos() as u64
    }
}

pub fn span_to_wire(record: &SpanRecord, base: TimeBase) -> WireSpan {
    let metadata = record.metadata;
    WireSpan {
        id: record.id,
        parent_id: record.parent_id,
        name: metadata.name().to_string(),
        target: metadata.target().to_string(),
        level: WireLevel::from_tracing(metadata.level()),
        fields: record
            .fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        events: record.events.iter().map(|e| event_to_wire(e, base)).collect(),
        opened_at_ns: base.ns(record.opened_at),
        closed_at_ns: record.closed_at.map(|t| base.ns(t)),
    }
}

pub fn event_to_wire(event: &EventRecord, base: TimeBase) -> WireEvent {
    let metadata = event.metadata;
    WireEvent {
        name: metadata.name().to_string(),
        level: WireLevel::from_tracing(metadata.level()),
        fields: event
            .fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        recorded_at_ns: base.ns(event.recorded_at),
    }
}
