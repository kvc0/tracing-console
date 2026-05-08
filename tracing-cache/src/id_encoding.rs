//! Encoding for the u64 carried in `tracing::span::Id`.
//!
//! Layout:
//!
//! ```text
//!     top    `shard_bits`        bits  → shard index (0 .. lane_count)
//!     bottom `64 - shard_bits`   bits  → slab_idx + SLAB_OFFSET
//! ```
//!
//! `DISABLED = 1` is reserved for spans the predicate or capacity checks
//! rejected.  Slab indices encode as `slab_idx + 2` (`SLAB_OFFSET`) so
//! shard 0 / slab_idx 0 doesn't collide with `DISABLED`.
//!
//! `actual_id` is **not** in the encoded id; it lives in the per-shard
//! sidecar `actual_ids: Box<[AtomicU64]>` (see `cache::ShardLane`).
//! `new_span` writes it when it inserts into the slab; `enter` reads it
//! lock-free and pushes a `StackedSpan { tracing_id, actual_id }` onto
//! `SPAN_STACK` so that a contextual `new_span` later can read its
//! parent's `actual_id` directly from the stack without locking the
//! parent's slab.

pub(crate) const DISABLED: u64 = 1;
pub(crate) const SLAB_OFFSET: u64 = 2;

#[inline]
pub(crate) fn id_to_u64(id: &tracing::span::Id) -> u64 {
    id.into_u64()
}

#[inline]
pub(crate) fn u64_to_id(n: u64) -> tracing::span::Id {
    tracing::span::Id::from_u64(n)
}

#[inline]
pub(crate) fn disabled_id() -> tracing::span::Id {
    u64_to_id(DISABLED)
}
