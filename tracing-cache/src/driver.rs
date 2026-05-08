//! Background task that drains closed spans from the spillway and writes
//! them into the shared `BTreeMap`.
//!
//! `Driver` is paired with a `SpanCache` at construction time; spawn it
//! once (typically as a tokio task) and the cache routes every closed
//! `SpanRecord` to it via the spillway channel.  When all `Sender` clones
//! are dropped, the receiver returns `None` and the driver exits cleanly.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::record::SpanRecord;

pub struct Driver {
    pub(crate) map: Arc<RwLock<BTreeMap<u64, SpanRecord>>>,
    pub(crate) receiver: spillway::Receiver<SpanRecord>,
    pub(crate) capacity: usize,
    pub(crate) batch_size: usize,
    pub(crate) tick_interval: std::time::Duration,
}

impl Driver {
    /// Runs the driver loop.  Blocks on the spillway receiver and flushes
    /// each delivered batch into the shared map; terminates when all
    /// `Sender` clones are dropped (channel closed).
    pub async fn run(self) {
        let Driver {
            map, mut receiver, capacity,
            batch_size: _, tick_interval: _,
        } = self;

        loop {
            match receiver.next_batch().await {
                Some(delivery_batch) => {
                    Self::flush_batch(&map, capacity, delivery_batch);
                }
                None => break, // all senders dropped
            }
        }
    }

    /// Synchronously drains all spans currently available in the spillway
    /// and flushes them into the map.  Use in tests after
    /// [`crate::SpanCache::flush_pending`].
    pub fn drain_sync(self) {
        let Driver { map, mut receiver, capacity, .. } = self;
        let mut batch = Vec::new();
        while let Some(record) = receiver.try_next() {
            batch.push(record);
        }
        Self::flush_batch(&map, capacity, batch.into_iter());
    }

    pub(crate) fn flush_batch(
        map: &RwLock<BTreeMap<u64, SpanRecord>>,
        capacity: usize,
        batch: impl ExactSizeIterator<Item = SpanRecord>,
    ) {
        if batch.len() == 0 {
            return;
        }
        // Only closed spans are ever sent to the driver, so all entries in
        // the map are already closed — pop_first() always evicts a
        // finished span.
        let mut m = map.write().unwrap();
        if capacity <= batch.len() {
            m.clear();
        } else {
            while capacity < m.len() + batch.len() {
                m.pop_first();
            }
        }
        let skip = batch.len().saturating_sub(capacity);
        m.extend(batch.skip(skip).map(|s| (s.id, s)));
    }
}
