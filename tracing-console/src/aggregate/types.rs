//! Per-bucket aggregation primitives shared between the table and
//! graph views.  The aggregator (see `state.rs`) keys its rolling
//! `HashMap<BucketKey, BucketState>` by these types; downstream
//! renderers consume the projected `(BucketKey, StackStats)` rows.

use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackStats {
    pub count: u64,
    pub total_min_ns: u64,
    pub total_max_ns: u64,
    pub total_sum_ns: u128,
    pub self_min_ns: u64,
    pub self_max_ns: u64,
    pub self_sum_ns: u128,
}

impl StackStats {
    pub fn total_avg_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            (self.total_sum_ns / self.count as u128) as u64
        }
    }

    pub fn self_avg_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            (self.self_sum_ns / self.count as u128) as u64
        }
    }
}

/// Identity of an aggregation row.  Two spans land in the same bucket
/// iff they share the same full ancestry stack and the same resolved
/// values for every user-selected split key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BucketKey {
    pub stack: Vec<String>,
    /// `(key, value)` pairs in sorted-by-key order so two buckets that
    /// agree on the same set of split values compare equal regardless
    /// of insertion order.
    pub splits: Vec<(String, String)>,
}
