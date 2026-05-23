//! Conversion from in-memory `SpanRecord` / `EventRecord` to the wire types.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tracing_cache::{EventRecord, FieldValue, SpanRecord};

use crate::protocol::{WireEvent, WireFieldValue, WireLevel, WireSpan};

fn field_to_wire(value: &FieldValue) -> WireFieldValue {
    match value {
        FieldValue::U64(v) => WireFieldValue::U64(*v),
        FieldValue::I64(v) => WireFieldValue::I64(*v),
        FieldValue::F64(v) => WireFieldValue::F64(*v),
        FieldValue::Bool(v) => WireFieldValue::Bool(*v),
        FieldValue::Str(s) => WireFieldValue::Str((*s).to_string()),
        FieldValue::SmallString(s) => WireFieldValue::Str(s.to_string()),
        FieldValue::SharedString(s) => WireFieldValue::Str((**s).clone()),
        FieldValue::String(s) => WireFieldValue::Str(s.clone()),
    }
}

/// Reference points used to serialize `Instant`s as Unix-epoch
/// nanoseconds.  Each host owns one of these, captured at server
/// start.  `Instant` is monotonic but not wall-clock-anchored, so
/// we also snapshot `SystemTime` at start and add the
/// instant-delta to get a wall-clock value.  Clients receive
/// nanoseconds since the Unix epoch — convertible directly via
/// `chrono::DateTime::from_timestamp_nanos`.
#[derive(Debug, Clone, Copy)]
pub struct TimeBase {
    instant_at_start: Instant,
    systime_at_start: SystemTime,
}

impl TimeBase {
    pub fn now() -> Self {
        Self {
            instant_at_start: Instant::now(),
            systime_at_start: SystemTime::now(),
        }
    }

    fn ns(self, t: Instant) -> u64 {
        // Convert the monotonic Instant into a wall-clock SystemTime
        // by adding its offset from `instant_at_start` to the wall
        // clock at start.  Result is ns since Unix epoch; events
        // pre-server-start (impossible in practice) saturate to 0.
        let offset = t.saturating_duration_since(self.instant_at_start);
        let wall = self.systime_at_start + offset;
        wall.duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
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
            .map(|(k, v)| ((*k).to_string(), field_to_wire(v)))
            .collect(),
        events: record
            .events
            .iter()
            .map(|e| event_to_wire(e, base))
            .collect(),
        opened_at_ns: base.ns(record.opened_at),
        closed_at_ns: record.closed_at.map(|t| base.ns(t)),
    }
}

pub fn event_to_wire(event: &EventRecord, base: TimeBase) -> WireEvent {
    let metadata = event.metadata();
    WireEvent {
        name: metadata.name().to_string(),
        level: WireLevel::from_tracing(metadata.level()),
        fields: event
            .fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), field_to_wire(v)))
            .collect(),
        recorded_at_ns: base.ns(event.recorded_at()),
    }
}
