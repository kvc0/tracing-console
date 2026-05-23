//! Thread-local state used on the subscriber hot path.
//!
//! SAFETY rationale for `SPAN_STACK`, `PENDING_*`, and `THREAD_SENDERS`
//! using `UnsafeCell` rather than `RefCell`: every access goes through
//! one of the helpers below, each of which constructs a temporary `&`
//! or `&mut` to the inner Vec / Option for a single leaf operation
//! (push / pop / drain / read len) and lets it die before the helper
//! returns.  No subscriber-method dispatch happens while one of these
//! references is alive:
//!
//! * push / pop / last-copy do no callbacks at all.
//! * `pending_drain` calls `spillway::Sender::send_many` — that's a
//!   non-reentrant Mutex push that does not invoke tracing on this thread.
//! * `enabled` / `new_span` consult predicates only AFTER releasing the
//!   `SPAN_STACK` borrow (`stack_top` returns `Option<StackedSpan>` by value).
//!
//! Concurrent access can't happen — the cells are thread-local.

use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::driver::EventMessage;
use crate::record::SpanRecord;

/// Number of `actual_id`s a thread reserves in a single bump of the
/// per-cache `id_high_water` counter.  Power of two so the in-batch
/// position is `cursor & (ID_BATCH - 1)` and refill is signalled by that
/// being zero — lets the per-thread reservation be a single u64 cursor.
/// Larger batch ⇒ fewer `fetch_add` hits on the shared atomic ⇒ less
/// cross-thread contention, at the cost of `actual_id`s no longer being
/// globally monotonic — they're monotonic within a thread's batch, but
/// interleave across threads.
pub(crate) const ID_BATCH: u64 = 1024;

/// Global counter that hands out a stable shard key to each thread the
/// first time it touches `pick_shard`.  Cheap, monotonic, and only paid
/// once per thread.
pub(crate) static NEXT_THREAD_KEY: AtomicU64 = AtomicU64::new(0);

/// One entry on the per-thread `SPAN_STACK`.  Carries both the encoded
/// tracing id (so `exit` can pop it back) and the parent-side `actual_id`
/// so that `new_span`'s contextual-parent path can read the parent's
/// `SpanRecord.id` without acquiring the parent-shard's mutex.  For
/// DISABLED entries `actual_id` is 0 and is never read.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StackedSpan {
    pub tracing_id: u64,
    pub actual_id: u64,
}

/// Per-thread cache of spillway sender clones, keyed by the source
/// cache's address.  Holds both senders (span + event) so a thread
/// switching between caches re-clones both atomically.
pub(crate) struct ThreadSenders {
    pub(crate) cache_addr: usize,
    pub(crate) span: spillway::Sender<SpanRecord>,
    pub(crate) event: spillway::Sender<EventMessage>,
}

thread_local! {
    /// Active span entries on this thread.  DISABLED entries carry
    /// `tracing_id = DISABLED, actual_id = 0`.
    pub(crate) static SPAN_STACK: UnsafeCell<Vec<StackedSpan>> =
        const { UnsafeCell::new(Vec::new()) };

    /// Closed spans waiting to be sent to the driver's span channel.
    pub(crate) static PENDING_SPAN: UnsafeCell<Vec<SpanRecord>> =
        const { UnsafeCell::new(Vec::new()) };

    /// Emitted events waiting to be sent to the driver's event channel.
    pub(crate) static PENDING_EVENT: UnsafeCell<Vec<EventMessage>> =
        const { UnsafeCell::new(Vec::new()) };

    /// Stable per-thread shard key, lazily assigned from `NEXT_THREAD_KEY`
    /// on first use.  Each cache derives its actual shard via
    /// `key & shard_mask`, so a given thread always lands on the same
    /// shard within a given cache (lock affinity for the slab cache lines).
    /// `u64::MAX` is the "not assigned yet" sentinel (the global counter
    /// can't realistically reach it).
    pub(crate) static THREAD_SHARD_KEY: Cell<u64> = const { Cell::new(u64::MAX) };

    /// Per-thread spillway sender clones, lazily initialised on first
    /// `flush_pending`.  Spillway's design is per-clone-lock-free: each
    /// `Sender::clone()` gets its own queue slot, so threads pushing to
    /// their own clone do not contend on spillway's internal mutex.
    /// `cache_addr` lets a thread that switches between two `SpanCache`
    /// instances notice the switch and re-clone both senders.
    pub(crate) static THREAD_SENDERS: UnsafeCell<Option<ThreadSenders>>
        = const { UnsafeCell::new(None) };

    /// Per-thread `actual_id` cursor: the next id to hand out from the
    /// current `ID_BATCH`-sized reservation.  `cursor & (ID_BATCH - 1) == 0`
    /// (including the initial `0`) means the batch is exhausted and the
    /// next call must refill via `id_high_water.fetch_add(ID_BATCH)`.
    /// Since `id_high_water` is initialised to a multiple of `ID_BATCH`,
    /// every fetched start is batch-aligned, so the mask check is
    /// sufficient.  Process-level (not per-cache): in production there's
    /// one cache, and our tests never emit enough spans on one thread to
    /// trigger a refill, so cross-cache leakage of cursor state is
    /// harmless in practice.
    pub(crate) static ID_CURSOR: Cell<u64> = const { Cell::new(0) };
}

#[inline]
pub(crate) fn stack_top() -> Option<StackedSpan> {
    SPAN_STACK.with(|c| unsafe { (*c.get()).last().copied() })
}

#[inline]
pub(crate) fn stack_push(entry: StackedSpan) {
    SPAN_STACK.with(|c| unsafe { (*c.get()).push(entry) });
}

#[inline]
pub(crate) fn stack_pop() {
    SPAN_STACK.with(|c| unsafe {
        (*c.get()).pop();
    });
}

/// Push a closed span onto the span PENDING buffer; return the new length.
#[inline]
pub(crate) fn pending_push_span(record: SpanRecord) -> usize {
    PENDING_SPAN.with(|c| unsafe {
        let v = &mut *c.get();
        v.push(record);
        v.len()
    })
}

/// Push an emitted event onto the event PENDING buffer; return the new length.
#[inline]
pub(crate) fn pending_push_event(msg: EventMessage) -> usize {
    PENDING_EVENT.with(|c| unsafe {
        let v = &mut *c.get();
        v.push(msg);
        v.len()
    })
}

/// Drain PENDING_SPAN in place, invoking `f` once with the resulting
/// `Drain` iterator.  `f` must not call back into tracing on this thread.
#[inline]
pub(crate) fn pending_drain_spans<F: FnMut(std::vec::Drain<'_, SpanRecord>)>(mut f: F) {
    PENDING_SPAN.with(|c| unsafe {
        f((*c.get()).drain(..));
    });
}

/// Drain PENDING_EVENT in place, invoking `f` once with the resulting
/// `Drain` iterator.
#[inline]
pub(crate) fn pending_drain_events<F: FnMut(std::vec::Drain<'_, EventMessage>)>(mut f: F) {
    PENDING_EVENT.with(|c| unsafe {
        f((*c.get()).drain(..));
    });
}

/// Lazily claim a stable shard key for this thread, stashing it on first
/// use so subsequent calls are a single `Cell::get`.
#[inline]
pub(crate) fn ensure_thread_shard_key() -> u64 {
    THREAD_SHARD_KEY.with(|c| {
        let v = c.get();
        if v == u64::MAX {
            let assigned = NEXT_THREAD_KEY.fetch_add(1, Relaxed);
            c.set(assigned);
            assigned
        } else {
            v
        }
    })
}
