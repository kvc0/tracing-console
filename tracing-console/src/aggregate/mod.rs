//! Incremental span aggregation.
//!
//! `Aggregator` is the rolling-window bucketer behind both the TUI's
//! `visible_rows` and the `--stats` table.  Every span that flows in
//! from the wire goes through `absorb`; the aggregator maintains a
//! bounded `VecDeque` of recent spans plus per-bucket aggregates
//! that are updated in place on each insertion / eviction.
//!
//! The per-flush cost is `O(|buckets|)` (only the projection +
//! sort), independent of how many spans are in the ring.
//!
//! Out-of-order arrival is a routine case.  Within a single page
//! batch parents arrive before children, but across batches a child
//! can arrive whose parent closes in a later batch.  Such "orphans"
//! park in a parent-id-keyed pending pool sized at
//! `history_budget / 4`; when the parent arrives, the pool's waiters
//! are drained back through `absorb` and join the appropriate
//! bucket.  Pool overflow evicts by ascending parent id (oldest
//! missing parent first — it's least likely to ever materialise).
//!
//! Submodule layout:
//! * [`types`] — `StackStats`, `BucketKey`.
//! * [`state`] — `Aggregator` and its per-bucket internals.
//! * [`util`] — `fmt_ns`, `tree_label`, `candidate_split_keys_for`,
//!   and helpers shared with the rendering code.

mod state;
mod types;
mod util;

#[cfg(test)]
mod tests;

pub use state::Aggregator;
pub use types::{BucketKey, StackStats};
pub use util::{candidate_split_keys_for, fmt_ns, tree_label};
