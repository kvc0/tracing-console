//! Helpers used by both the rendering code and the aggregator's
//! own tests: nanosecond formatting, tree-prefix rendering, and the
//! split-key candidate scan that both the table and graph details
//! panes need.

use std::collections::BTreeSet;

use super::{Aggregator, BucketKey};

/// Field keys observed on spans whose resolved stack matches
/// `target_stack`.  Used by both `Model::candidate_split_keys` (for
/// the table view) and the graph view's Details pane.
pub fn candidate_split_keys_for(agg: &Aggregator, target_stack: &[String]) -> Vec<String> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for (s, stack) in agg.iter_with_stack() {
        if stack.as_slice() == target_stack {
            for (field_name, _) in &s.fields {
                keys.insert(field_name.clone());
            }
        }
    }
    keys.into_iter().collect()
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
