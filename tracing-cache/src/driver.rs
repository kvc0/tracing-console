//! Background task that drains closed spans and emitted events from
//! two spillway channels, attaches events to their parent span, and
//! fans the resulting `SpanRecord`s out to every live subscriber.
//!
//! Two channels (rather than one enum-typed channel) keep each
//! pipeline type-pure: span-only workloads pay no enum-match cost on
//! the driver side, and each spillway carries a homogeneous payload
//! of the natural per-payload size.  Ordering across channels isn't
//! preserved, but the side buffer below handles temporal misordering:
//! if an event arrives at the driver before its parent span, it
//! parks in `side_events` keyed by `parent_actual_id`, and the span's
//! arrival drains the buffer and attaches the events.  Events that
//! arrive *after* the parent has been fanned out have nowhere to land
//! and are dropped — within a single thread `flush_pending` always
//! sends events before the closing span, so this is rare in practice.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::object_pool::ReuseRef;
use crate::record::{EventRecord, SpanRecord};

/// Event payload on the event spillway channel.
pub struct EventMessage {
    pub parent_actual_id: u64,
    pub record: ReuseRef<EventRecord>,
}

pub struct Driver {
    pub(crate) span_receiver: spillway::Receiver<SpanRecord>,
    pub(crate) event_receiver: spillway::Receiver<EventMessage>,
    /// Cap on distinct parent ids the orphan-event buffer can hold
    /// while waiting for their span to land.  Once full, a new
    /// parent's first event evicts the oldest entry via
    /// `BTreeMap::pop_first` — and since `parent_actual_id`s are
    /// monotonically allocated, the smallest key is the oldest span.
    /// Evicted `ReuseRef`s drop back into the event pool.
    pub(crate) capacity: usize,
    /// Events whose parent `SpanRecord` hasn't been fanned out yet,
    /// keyed by `parent_actual_id`.  See `capacity` for the bound.
    pub(crate) side_events: BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
    /// Shared with [`crate::SpanCache::subscribers`].  Each closed
    /// span the driver processes is cloned out to every entry; senders
    /// that return `Error::Closed` are removed in place.
    pub(crate) subscribers: Arc<Mutex<Vec<spillway::Sender<SpanRecord>>>>,
}

impl Driver {
    /// Runs the driver loop.  `tokio::select!` pulls whichever channel
    /// has a batch ready next; terminates when both channels are closed.
    pub async fn run(self) {
        let Driver {
            mut span_receiver,
            mut event_receiver,
            capacity,
            mut side_events,
            subscribers,
        } = self;

        let mut span_closed = false;
        let mut event_closed = false;
        loop {
            tokio::select! {
                biased;
                event_batch = event_receiver.next_batch(), if !event_closed => {
                    match event_batch {
                        Some(batch) => Self::flush_event_batch(
                            &mut side_events, capacity, batch,
                        ),
                        None => event_closed = true,
                    }
                }
                span_batch = span_receiver.next_batch(), if !span_closed => {
                    match span_batch {
                        Some(batch) => Self::flush_span_batch(
                            &mut side_events, &subscribers, batch,
                        ),
                        None => span_closed = true,
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
            mut span_receiver,
            mut event_receiver,
            capacity,
            mut side_events,
            subscribers,
        } = self;

        let mut events = Vec::new();
        while let Some(e) = event_receiver.try_next() {
            events.push(e);
        }
        Self::flush_event_batch(&mut side_events, capacity, events.into_iter());

        let mut spans = Vec::new();
        while let Some(s) = span_receiver.try_next() {
            spans.push(s);
        }
        Self::flush_span_batch(&mut side_events, &subscribers, spans.into_iter());
    }

    pub(crate) fn flush_span_batch(
        side_events: &mut BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
        subscribers: &Mutex<Vec<spillway::Sender<SpanRecord>>>,
        batch: impl ExactSizeIterator<Item = SpanRecord>,
    ) {
        if batch.len() == 0 {
            return;
        }
        // Attach parked orphan events (if any) to each span before
        // fan-out.  Done outside the subscribers lock so the visible
        // critical section is just the send loop.
        let mut prepared: Vec<SpanRecord> = Vec::with_capacity(batch.len());
        let any_side = !side_events.is_empty();
        for mut span in batch {
            if any_side && let Some(events) = side_events.remove(&span.id) {
                span.events.extend(events);
            }
            prepared.push(span);
        }

        #[allow(clippy::expect_used, reason = "poisoned lock")]
        let mut subs = subscribers.lock().expect("lock must not be poisoned");
        fanout_under_lock(&mut subs, prepared);
    }

    pub(crate) fn flush_event_batch(
        side_events: &mut BTreeMap<u64, Vec<ReuseRef<EventRecord>>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = EventMessage>,
    ) {
        if batch.len() == 0 {
            return;
        }
        for EventMessage {
            parent_actual_id,
            record,
        } in batch
        {
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

/// Send each prepared span to every live subscriber.  Caller already
/// holds the subscribers lock.  Slow consumers (`Error::Full`) drop a
/// whole batch with a debug log; dropped receivers (`Error::Closed`)
/// are removed in place.  The typical case is one subscriber per
/// console, so the unconditional `clone()` is a non-issue — it's the
/// `Vec<ReuseRef<EventRecord>>` clones that dominate.
fn fanout_under_lock(subs: &mut Vec<spillway::Sender<SpanRecord>>, prepared: Vec<SpanRecord>) {
    if prepared.is_empty() {
        return;
    }
    subs.retain(|sender| match sender.send_many(prepared.iter().cloned()) {
        Ok(()) => true,
        Err(spillway::Error::Closed(_)) => false,
        Err(spillway::Error::Full(_)) => {
            log::debug!("subscriber channel full; dropping a batch of closed spans");
            true
        }
    });
}

#[cfg(test)]
mod tests {
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

    fn no_subscribers() -> Mutex<Vec<spillway::Sender<SpanRecord>>> {
        Mutex::new(Vec::new())
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
        let mut side: Side = BTreeMap::new();

        let events = vec![make_event(&pool, 99), make_event(&pool, 99)];
        Driver::flush_event_batch(&mut side, 8, events.into_iter());
        assert_eq!(bucket_len(&side, 99), Some(2));

        // Parent arrives → orphans attach (and the span is fanned out
        // to subscribers — none here, so we just check side drains).
        Driver::flush_span_batch(&mut side, &no_subscribers(), std::iter::once(make_span(99)));
        assert!(
            side.is_empty(),
            "side bucket for 99 must drain on span arrival"
        );
    }

    #[test]
    fn span_arrival_attaches_parked_events_to_fanout() {
        // The span the subscriber receives carries the side-buffer
        // events that were parked before it arrived — proving the
        // events flow without going through a historical map.
        let pool = ObjectPool::<EventRecord>::new(1, 16);
        let mut side: Side = BTreeMap::new();
        let subs = Mutex::new(Vec::new());

        // Park two events for parent 99.
        Driver::flush_event_batch(
            &mut side,
            8,
            vec![make_event(&pool, 99), make_event(&pool, 99)].into_iter(),
        );

        // Subscriber connects, then parent 99 arrives.
        let (sender, mut rx) = spillway::channel_with_capacity_and_concurrency(64, 1);
        #[allow(clippy::expect_used, reason = "test")]
        subs.lock().expect("test").push(sender);
        Driver::flush_span_batch(&mut side, &subs, std::iter::once(make_span(99)));

        let span = rx.try_next().expect("subscriber should receive span 99");
        assert_eq!(span.id, 99);
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
        let mut side: Side = BTreeMap::new();

        let mut fill: Vec<EventMessage> = Vec::new();
        for parent in [10u64, 20, 30, 40] {
            fill.push(make_event(&pool, parent));
        }
        Driver::flush_event_batch(&mut side, CAP, fill.into_iter());
        assert_eq!(side.len(), CAP);
        let ids: Vec<u64> = side.keys().copied().collect();
        assert_eq!(ids, vec![10, 20, 30, 40]);

        Driver::flush_event_batch(&mut side, CAP, std::iter::once(make_event(&pool, 999)));
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
        let mut side: Side = BTreeMap::new();

        Driver::flush_event_batch(
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
        let mut side: Side = BTreeMap::new();

        Driver::flush_event_batch(
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
        let mut side: Side = BTreeMap::new();

        // Parent 1 accumulates 3 events; parent 2 has 1.  Both
        // buckets exist; buffer is at CAP.
        Driver::flush_event_batch(
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
        Driver::flush_event_batch(&mut side, CAP, std::iter::once(make_event(&pool, 7)));
        let ids: Vec<u64> = side.keys().copied().collect();
        assert_eq!(ids, vec![2, 7]);
        assert!(bucket_len(&side, 1).is_none());
    }
}
