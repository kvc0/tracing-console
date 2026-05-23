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

use std::collections::BTreeMap;
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
    /// Events whose parent `SpanRecord` hasn't been inserted yet,
    /// keyed by `parent_actual_id`.  Bounded by `capacity` distinct
    /// parent ids; once full, a new parent's first event evicts the
    /// oldest entry via `BTreeMap::pop_first` — and since
    /// `parent_actual_id`s are monotonically allocated, the smallest
    /// key is the oldest span.  Evicted `ReuseRef`s drop back into
    /// the event pool.
    pub(crate) side_events: BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
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
        side_events: &mut BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = SpanRecord>,
    ) {
        if batch.len() == 0 {
            return;
        }
        let mut m = map.write().unwrap();
        let any_side = !side_events.is_empty();
        for mut span in batch {
            // Fast-path: skip the lookup when the side buffer has
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
        side_events: &mut BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
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
                continue;
            }
            if let Some(events) = side_events.get_mut(&parent_actual_id) {
                events.push(record);
                continue;
            }
            // New parent.  If we're at capacity, evict the oldest
            // bucket — `BTreeMap::pop_first` returns the smallest
            // `parent_actual_id`, which is the oldest span by virtue
            // of monotonic id allocation.  Then park the new bucket.
            if side_events.len() >= capacity {
                side_events.pop_first();
            }
            side_events.insert(parent_actual_id, vec![record]);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use tracing::callsite::{Callsite, DefaultCallsite, Identifier};
    use tracing::field::FieldSet;
    use tracing::metadata::Kind;
    use tracing::{Level, Metadata};

    use super::*;
    use crate::object_pool::ObjectPool;
    use crate::record::FieldList;

    // Static metadata so we can build `SpanRecord`s without spinning up
    // the tracing subscriber.  Pattern lifted from tracing-core's own
    // tests (`missed_register_callsite.rs`).
    static CALLSITE: DefaultCallsite = {
        static META: Metadata<'static> = Metadata::new(
            "driver_test_span",
            "driver::test",
            Level::INFO,
            None,
            None,
            None,
            FieldSet::new(&[], Identifier(&CALLSITE)),
            Kind::SPAN,
        );
        DefaultCallsite::new(&META)
    };

    fn test_metadata() -> &'static Metadata<'static> {
        CALLSITE.metadata()
    }

    fn make_event(pool: &ObjectPool<EventRecord>, parent_id: u64) -> EventMessage {
        let mut record = pool.acquire();
        record.metadata = Some(test_metadata());
        record.recorded_at = Some(Instant::now());
        record.fields = FieldList::default();
        EventMessage {
            parent_actual_id: parent_id,
            record,
        }
    }

    fn make_span(id: u64) -> SpanRecord {
        SpanRecord {
            id,
            parent_id: None,
            metadata: test_metadata(),
            fields: FieldList::default(),
            events: Vec::new(),
            opened_at: Instant::now(),
            closed_at: Some(Instant::now()),
        }
    }

    fn empty_map() -> Arc<RwLock<BTreeMap<u64, SpanRecord>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    type Side = BTreeMap<u64, Vec<ReuseRef<EventRecord>>>;

    fn bucket_len(side: &Side, parent_id: u64) -> Option<usize> {
        side.get(&parent_id).map(Vec::len)
    }

    #[test]
    fn event_orphan_below_capacity_stashes_for_parent() {
        // Events for an unknown parent should park in `side_events`
        // and survive there until the matching span arrives.
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let map = empty_map();
        let mut side: Side = BTreeMap::new();

        let events = vec![make_event(&pool, 99), make_event(&pool, 99)];
        Driver::flush_event_batch(&map, &mut side, 8, events.into_iter());
        assert_eq!(bucket_len(&side, 99), Some(2));
        assert!(
            map.read().unwrap().is_empty(),
            "events must not insert spans"
        );

        // Parent arrives → orphans attach and the side bucket is drained.
        Driver::flush_span_batch(&map, &mut side, 8, std::iter::once(make_span(99)));
        assert!(
            side.is_empty(),
            "side bucket for 99 must drain on span arrival"
        );
        let m = map.read().unwrap();
        let span = m.get(&99).expect("span 99 inserted");
        assert_eq!(span.events.len(), 2);
    }

    #[test]
    fn event_orphan_at_capacity_evicts_oldest_parent_id() {
        // Fill the buffer with CAP distinct parents (ids 10, 20, 30, 40).
        // A new parent (999) arriving at capacity should bump the
        // smallest id (10) — which is the oldest span by virtue of
        // monotonic actual_id allocation — and keep the rest.
        const CAP: usize = 4;
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let map = empty_map();
        let mut side: Side = BTreeMap::new();

        let mut fill: Vec<EventMessage> = Vec::new();
        for parent in [10u64, 20, 30, 40] {
            fill.push(make_event(&pool, parent));
        }
        Driver::flush_event_batch(&map, &mut side, CAP, fill.into_iter());
        assert_eq!(side.len(), CAP);
        let ids: Vec<u64> = side.keys().copied().collect();
        assert_eq!(ids, vec![10, 20, 30, 40]);

        Driver::flush_event_batch(
            &map,
            &mut side,
            CAP,
            std::iter::once(make_event(&pool, 999)),
        );
        let ids: Vec<u64> = side.keys().copied().collect();
        assert_eq!(ids, vec![20, 30, 40, 999], "smallest id must be evicted");
        assert_eq!(bucket_len(&side, 999), Some(1));
        assert!(bucket_len(&side, 10).is_none());
    }

    #[test]
    fn event_orphan_at_capacity_grows_existing_parent_without_eviction() {
        // Events for a parent already in the buffer should append to
        // its vec — no eviction, since no new parent slot is claimed.
        const CAP: usize = 2;
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let map = empty_map();
        let mut side: Side = BTreeMap::new();

        Driver::flush_event_batch(
            &map,
            &mut side,
            CAP,
            vec![make_event(&pool, 1), make_event(&pool, 2)].into_iter(),
        );
        assert_eq!(side.len(), CAP);
        assert_eq!(bucket_len(&side, 1), Some(1));

        // Two more events for the *existing* parent 1.  Buffer length
        // stays at CAP; parent 1's bucket grows to 3.  Parent 2 is
        // untouched.
        Driver::flush_event_batch(
            &map,
            &mut side,
            CAP,
            vec![make_event(&pool, 1), make_event(&pool, 1)].into_iter(),
        );
        assert_eq!(side.len(), CAP);
        assert_eq!(bucket_len(&side, 1), Some(3));
        assert_eq!(bucket_len(&side, 2), Some(1));
    }

    #[test]
    fn event_orphan_appends_to_existing_parent_below_capacity() {
        // Below the cap, repeated events for the same parent id
        // accumulate in its vec without growing the buffer length.
        const CAP: usize = 8;
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let map = empty_map();
        let mut side: Side = BTreeMap::new();

        Driver::flush_event_batch(
            &map,
            &mut side,
            CAP,
            vec![
                make_event(&pool, 7),
                make_event(&pool, 7),
                make_event(&pool, 7),
            ]
            .into_iter(),
        );
        assert_eq!(side.len(), 1);
        assert_eq!(bucket_len(&side, 7), Some(3));
    }

    #[test]
    fn event_orphan_eviction_drops_entire_bucket_not_just_one_event() {
        // When the oldest bucket holds multiple events, eviction
        // discards the whole bucket — those events drop back into
        // the pool together.
        const CAP: usize = 2;
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let map = empty_map();
        let mut side: Side = BTreeMap::new();

        // Parent 1 accumulates 3 events; parent 2 has 1.  Both
        // buckets exist; buffer is at CAP.
        Driver::flush_event_batch(
            &map,
            &mut side,
            CAP,
            vec![
                make_event(&pool, 1),
                make_event(&pool, 1),
                make_event(&pool, 1),
                make_event(&pool, 2),
            ]
            .into_iter(),
        );
        assert_eq!(bucket_len(&side, 1), Some(3));

        // New parent 7 evicts parent 1's *entire* bucket — all
        // three events go, not just one.
        Driver::flush_event_batch(&map, &mut side, CAP, std::iter::once(make_event(&pool, 7)));
        let ids: Vec<u64> = side.keys().copied().collect();
        assert_eq!(ids, vec![2, 7]);
        assert!(bucket_len(&side, 1).is_none());
    }
}
