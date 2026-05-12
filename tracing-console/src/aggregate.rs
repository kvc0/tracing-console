//! Span aggregation primitives shared by `--stats` and the TUI.
//!
//! `bucket_by_stack` does three things in one pass:
//!
//! 1. **Drop incomplete chains.**  A span is renderable only when every
//!    ancestor in its parent chain is present in the input.  Spans
//!    whose parent has been evicted from the buffer are dropped — we
//!    refuse to render them with partial context.
//! 2. **Group by `(stack, splits)`** where `stack` is the chain of
//!    span names root → leaf and `splits` is the resolved values for
//!    each user-selected split key, picked up from the span itself or
//!    its closest ancestor that carries the key.
//! 3. **Compute total / self stats per bucket.**  `self_ns` for a span
//!    is its total minus the sum of *direct children's* totals that
//!    also made it through the filter.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use tracing_console_host::WireSpan;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
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
    pub fn record(&mut self, total_ns: u64, self_ns: u64) {
        if self.count == 0 {
            self.total_min_ns = total_ns;
            self.total_max_ns = total_ns;
            self.self_min_ns = self_ns;
            self.self_max_ns = self_ns;
        } else {
            self.total_min_ns = self.total_min_ns.min(total_ns);
            self.total_max_ns = self.total_max_ns.max(total_ns);
            self.self_min_ns = self.self_min_ns.min(self_ns);
            self.self_max_ns = self.self_max_ns.max(self_ns);
        }
        self.count += 1;
        self.total_sum_ns += total_ns as u128;
        self.self_sum_ns += self_ns as u128;
    }

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

/// Bucket renderable spans by `(stack, splits)`.  Rows are sorted
/// lexicographically — stack first, then splits — so DFS traversal
/// (parent before its children) just walks the slice in order.
pub fn bucket_by_stack<'a, I>(
    spans: I,
    split_keys: &BTreeSet<String>,
) -> Vec<(BucketKey, StackStats)>
where
    I: IntoIterator<Item = &'a WireSpan>,
{
    let spans: Vec<&WireSpan> = spans.into_iter().collect();

    // Index by id for chain walks.  Single-pass; ids collide is a
    // wire-protocol bug, so last write wins.
    let by_id: HashMap<u64, &WireSpan> = spans.iter().map(|s| (s.id, *s)).collect();

    // Renderability: a span's full parent chain must be in `by_id`.
    // Memoised via a HashMap so a deep chain is only walked once.
    let mut renderable: HashMap<u64, bool> = HashMap::with_capacity(spans.len());
    for s in &spans {
        is_renderable(s.id, &by_id, &mut renderable);
    }

    // Pass 1: sum direct children's totals per parent id, restricted
    // to renderable + closed spans.  `self_ns = total − sum_children`.
    let mut child_sum: HashMap<u64, u64> = HashMap::new();
    for s in &spans {
        if !renderable.get(&s.id).copied().unwrap_or(false) {
            continue;
        }
        if let Some(closed) = s.closed_at_ns {
            let total = closed.saturating_sub(s.opened_at_ns);
            if let Some(p) = s.parent_id {
                *child_sum.entry(p).or_default() += total;
            }
        }
    }

    // Pass 2: bucket by resolved (stack, splits).
    let mut by_bucket: HashMap<BucketKey, StackStats> = HashMap::new();
    for s in &spans {
        if !renderable.get(&s.id).copied().unwrap_or(false) {
            continue;
        }
        let Some(closed) = s.closed_at_ns else {
            continue;
        };
        let total = closed.saturating_sub(s.opened_at_ns);
        let self_ns = total.saturating_sub(*child_sum.get(&s.id).unwrap_or(&0));
        let key = bucket_key(s, &by_id, split_keys);
        by_bucket.entry(key).or_default().record(total, self_ns);
    }

    let mut rows: Vec<(BucketKey, StackStats)> = by_bucket.into_iter().collect();
    // Sort by `(splits, stack)` — splits group first so each split's
    // tree is contiguous (parent immediately followed by its
    // descendants in DFS order), then by stack within the group.
    // Sorting by stack first would interleave the parents of every
    // split before any child rendered, so an expanded subtree would
    // draw below all the sibling-split rows instead of under its
    // parent.
    rows.sort_by(|a, b| {
        a.0.splits
            .cmp(&b.0.splits)
            .then_with(|| a.0.stack.cmp(&b.0.stack))
    });
    rows
}

/// Walk a span's parent chain via `by_id` and resolve the stack +
/// split values.  Splits inherit from the closest ancestor that
/// carries each split key (the span itself overrides ancestors).
pub fn bucket_key(
    span: &WireSpan,
    by_id: &HashMap<u64, &WireSpan>,
    split_keys: &BTreeSet<String>,
) -> BucketKey {
    let mut stack = vec![span.name.clone()];
    let mut splits: Vec<(String, String)> = Vec::new();
    let take_splits = |s: &WireSpan, splits: &mut Vec<(String, String)>| {
        for (k, v) in &s.fields {
            if split_keys.contains(k) && !splits.iter().any(|(kk, _)| kk == k) {
                splits.push((k.clone(), v.to_string_value()));
            }
        }
    };
    take_splits(span, &mut splits);

    let mut p = span.parent_id;
    while let Some(id) = p {
        // Defensive: stop on absurdly deep chains.
        if stack.len() > 64 {
            break;
        }
        match by_id.get(&id) {
            Some(parent) => {
                stack.push(parent.name.clone());
                take_splits(parent, &mut splits);
                p = parent.parent_id;
            }
            None => break, // shouldn't happen for renderable spans
        }
    }
    stack.reverse();
    splits.sort_by(|a, b| a.0.cmp(&b.0));
    BucketKey { stack, splits }
}

/// Memoised "full parent chain present in `by_id`" check.
fn is_renderable(id: u64, by_id: &HashMap<u64, &WireSpan>, memo: &mut HashMap<u64, bool>) -> bool {
    if let Some(&hit) = memo.get(&id) {
        return hit;
    }
    let result = match by_id.get(&id) {
        None => false,
        Some(s) => match s.parent_id {
            None => true,
            Some(pid) => {
                if !by_id.contains_key(&pid) {
                    false
                } else {
                    is_renderable(pid, by_id, memo)
                }
            }
        },
    };
    memo.insert(id, result);
    result
}

/// Render row `i`'s leaf with a Unicode tree prefix derived from the
/// DFS-sorted row list.  Used by the `--stats` flat table.
pub fn tree_label<T>(rows: &[(BucketKey, T)], i: usize) -> String {
    let stack = &rows[i].0.stack;
    let n = stack.len();
    if n == 0 {
        return String::new();
    }
    if n == 1 {
        return stack[0].clone();
    }
    let mut prefix = String::with_capacity(3 * (n - 1));
    for d in 2..=n {
        let has_sibling = has_sibling_after(rows, i, d);
        if d < n {
            prefix.push_str(if has_sibling { "│  " } else { "   " });
        } else {
            prefix.push_str(if has_sibling { "├─ " } else { "└─ " });
        }
    }
    prefix.push_str(&stack[n - 1]);
    prefix
}

fn has_sibling_after<T>(rows: &[(BucketKey, T)], i: usize, d: usize) -> bool {
    let s = &rows[i].0.stack;
    if d == 0 || d > s.len() {
        return false;
    }
    let prefix_len = d - 1;
    let prefix = &s[..prefix_len];
    let needle = &s[d - 1];
    for (_, sj) in rows
        .iter()
        .enumerate()
        .skip(i + 1)
        .map(|(j, r)| (j, &r.0.stack))
    {
        if sj.len() < prefix_len {
            return false;
        }
        if &sj[..prefix_len] != prefix {
            return false;
        }
        if sj.len() < d {
            continue;
        }
        if &sj[d - 1] != needle {
            return true;
        }
    }
    false
}

pub fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.1}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1e3)
    } else {
        format!("{}ns", ns)
    }
}
