//! Background task that drains closed spans and emitted events from two
//! spillway channels, attaching events to their parent span and writing
//! the result into the shared `BTreeMap`.
//!
//! Two channels (rather than one enum-typed channel) keep each pipeline
//! type-pure: span-only workloads pay no enum-match cost on the driver
//! side, and each spillway carries a homogeneous payload of the
//! natural per-payload size.  Ordering across channels isn't preserved,
//! but the side buffer below handles temporal misordering: if an event
//! arrives before its parent has been inserted into the map, it parks
//! in `side_events` keyed by `parent_actual_id`, and the span's
//! arrival drains the buffer and attaches the events.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use crate::object_pool::ReuseRef;
use crate::record::{EventRecord, SpanRecord};

/// Event payload on the event spillway channel.
pub struct EventMessage {
    pub parent_actual_id: u64,
    pub record: ReuseRef<EventRecord>,
}

pub struct Driver {
    pub(crate) map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    pub(crate) span_receiver: spillway::Receiver<SpanRecord>,
    pub(crate) event_receiver: spillway::Receiver<EventMessage>,
    pub(crate) capacity: usize,
    pub(crate) batch_size: usize,
    pub(crate) tick_interval: std::time::Duration,
    /// Events whose parent `SpanRecord` hasn't been inserted yet.
    /// Bounded by `capacity` distinct parent ids so a runaway emitter
    /// targeting a never-closing or already-evicted parent can't grow
    /// it unboundedly.
    pub(crate) side_events: HashMap<u64, Vec<ReuseRef<EventRecord>>>,
}

impl Driver {
    /// Runs the driver loop.  `tokio::select!` pulls whichever channel
    /// has a batch ready next; terminates when both channels are closed.
    pub async fn run(self) {
        let Driver {
            map,
            mut span_receiver,
            mut event_receiver,
            capacity,
            batch_size: _,
            tick_interval: _,
            mut side_events,
        } = self;

        let mut span_closed = false;
        let mut event_closed = false;
        loop {
            tokio::select! {
                span_batch = span_receiver.next_batch(), if !span_closed => {
                    match span_batch {
                        Some(batch) => Self::flush_span_batch(
                            &map, &mut side_events, capacity, batch,
                        ),
                        None => span_closed = true,
                    }
                }
                event_batch = event_receiver.next_batch(), if !event_closed => {
                    match event_batch {
                        Some(batch) => Self::flush_event_batch(
                            &map, &mut side_events, capacity, batch,
                        ),
                        None => event_closed = true,
                    }
                }
                else => break,
            }
            if span_closed && event_closed {
                break;
            }
        }
    }

    /// Synchronously drain everything currently available on both
    /// channels.  Used by tests after `cache.flush_pending()`.  Events
    /// are drained first so that any event whose parent is in this
    /// drain's span batch lands in the side buffer in time.
    pub fn drain_sync(self) {
        let Driver {
            map,
            mut span_receiver,
            mut event_receiver,
            capacity,
            mut side_events,
            ..
        } = self;

        let mut events = Vec::new();
        while let Some(e) = event_receiver.try_next() {
            events.push(e);
        }
        Self::flush_event_batch(&map, &mut side_events, capacity, events.into_iter());

        let mut spans = Vec::new();
        while let Some(s) = span_receiver.try_next() {
            spans.push(s);
        }
        Self::flush_span_batch(&map, &mut side_events, capacity, spans.into_iter());
    }

    pub(crate) fn flush_span_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        side_events: &mut HashMap<u64, Vec<ReuseRef<EventRecord>>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = SpanRecord>,
    ) {
        if batch.len() == 0 {
            return;
        }
        let mut m = map.write().unwrap();
        let any_side = !side_events.is_empty();
        for mut span in batch {
            // Fast-path: skip the hash lookup when the side buffer has
            // nothing in it at all (span-only workloads).
            if any_side {
                if let Some(events) = side_events.remove(&span.id) {
                    span.events.extend(events);
                }
            }
            while m.len() >= capacity {
                if m.pop_first().is_none() {
                    break;
                }
            }
            m.insert(span.id, span);
        }
    }

    pub(crate) fn flush_event_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        side_events: &mut HashMap<u64, Vec<ReuseRef<EventRecord>>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = EventMessage>,
    ) {
        if batch.len() == 0 {
            return;
        }
        let mut m = map.write().unwrap();
        for EventMessage {
            parent_actual_id,
            record,
        } in batch
        {
            if let Some(span) = m.get_mut(&parent_actual_id) {
                span.events.push(record);
            } else if side_events.len() < capacity {
                side_events
                    .entry(parent_actual_id)
                    .or_default()
                    .push(record);
            }
            // else: side buffer at capacity — drop.  `ReuseRef::Drop`
            // returns the EventRecord allocation to the pool.
        }
    }
}
