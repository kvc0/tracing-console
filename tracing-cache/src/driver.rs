//! Background task that consumes a unified stream of span closures and
//! event emissions, materialising each into the shared `BTreeMap`.
//!
//! The cache sends [`DriverMsg`]s through the spillway:
//!   * `Span(SpanRecord)` — a span closed.  If any events for it landed
//!     here earlier (their parent hadn't been inserted yet), drain them
//!     out of `side_events` onto the span's `events` vec, then insert
//!     the span into the map.
//!   * `Event { parent_actual_id, record }` — an event was emitted with
//!     that parent.  If the parent is already in the map, append in
//!     place.  Otherwise stash the event in `side_events` keyed by the
//!     parent id; the `Span` arrival will pick it up.
//!
//! This pattern eliminates the cross-thread per-shard `Mutex<Slab>` lock
//! that the prior in-place `events.push` path took inside `event()` —
//! the driver is the only writer to the map, and the cache hot path is
//! pure pending-buffer enqueue.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use crate::object_pool::ReuseRef;
use crate::record::{EventRecord, SpanRecord};

/// Items pushed onto a thread's `PENDING` buffer and routed through the
/// spillway to the driver task.
pub enum DriverMsg {
    /// A closed span.  Its `events` vec is empty at this point — the
    /// driver fills it from `side_events` before inserting into the map.
    Span(SpanRecord),
    /// An emitted event.  `parent_actual_id` is the parent span's
    /// `SpanRecord.id`, looked up lock-free from the per-shard sidecar
    /// (or read straight off the `SPAN_STACK` entry for contextual
    /// events).
    Event {
        parent_actual_id: u64,
        record: ReuseRef<EventRecord>,
    },
}

pub struct Driver {
    pub(crate) map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    pub(crate) receiver: spillway::Receiver<DriverMsg>,
    pub(crate) capacity: usize,
    pub(crate) batch_size: usize,
    pub(crate) tick_interval: std::time::Duration,
    /// Events whose parent `SpanRecord` hasn't been inserted into the
    /// map yet.  Bounded by `capacity` distinct parent ids so a runaway
    /// emitter targeting a never-closing or already-evicted parent
    /// can't grow it unboundedly; overflow events drop on the floor
    /// (`ReuseRef::Drop` returns their box to the pool).
    pub(crate) side_events: HashMap<u64, Vec<ReuseRef<EventRecord>>>,
}

impl Driver {
    /// Runs the driver loop.  Blocks on the spillway receiver and
    /// processes each delivered batch; terminates when all `Sender`
    /// clones are dropped (channel closed).
    pub async fn run(self) {
        let Driver {
            map, mut receiver, capacity,
            batch_size: _, tick_interval: _,
            mut side_events,
        } = self;

        loop {
            match receiver.next_batch().await {
                Some(delivery_batch) => {
                    Self::flush_batch(&map, &mut side_events, capacity, delivery_batch);
                }
                None => break,
            }
        }
    }

    /// Synchronously drains all messages currently available in the
    /// spillway and flushes them into the map.  Use in tests after
    /// [`crate::SpanCache::flush_pending`].
    pub fn drain_sync(self) {
        let Driver { map, mut receiver, capacity, mut side_events, .. } = self;
        let mut batch = Vec::new();
        while let Some(msg) = receiver.try_next() {
            batch.push(msg);
        }
        Self::flush_batch(&map, &mut side_events, capacity, batch.into_iter());
    }

    pub(crate) fn flush_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        side_events: &mut HashMap<u64, Vec<ReuseRef<EventRecord>>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = DriverMsg>,
    ) {
        if batch.len() == 0 {
            return;
        }
        let mut m = map.write().unwrap();
        for msg in batch {
            match msg {
                DriverMsg::Span(mut span) => {
                    // Attach any events that landed before the span did.
                    // The `is_empty` check is a fast-path for workloads
                    // that never emit events — skips the per-Span hash +
                    // bucket probe that `HashMap::remove` otherwise pays.
                    if !side_events.is_empty() {
                        if let Some(events) = side_events.remove(&span.id) {
                            span.events.extend(events);
                        }
                    }
                    // Enforce capacity before inserting so we don't blow
                    // the bound between the check and the insert.
                    while m.len() >= capacity {
                        if m.pop_first().is_none() {
                            break;
                        }
                    }
                    m.insert(span.id, span);
                }
                DriverMsg::Event { parent_actual_id, record } => {
                    if let Some(span) = m.get_mut(&parent_actual_id) {
                        span.events.push(record);
                    } else if side_events.len() < capacity {
                        side_events
                            .entry(parent_actual_id)
                            .or_default()
                            .push(record);
                    }
                    // else: side buffer full — drop the record.  Its
                    // `Drop` returns the EventRecord box to the pool.
                }
            }
        }
    }
}
