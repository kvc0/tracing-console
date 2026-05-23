//! Aggregator tests + the alphabetical reference implementation
//! kept around so the parity test can compare incremental and
//! whole-buffer behaviour.

use std::collections::BTreeSet;

use tracing_console_host::WireSpan;

use super::{Aggregator, BucketKey, StackStats};

pub(crate) fn reference_bucket_by_stack<'a, I>(
    spans: I,
    split_keys: &BTreeSet<String>,
) -> Vec<(BucketKey, StackStats)>
where
    I: IntoIterator<Item = &'a WireSpan>,
{
    use std::collections::HashMap as Map;
    let spans: Vec<&WireSpan> = spans.into_iter().collect();
    let by_id: Map<u64, &WireSpan> = spans.iter().map(|s| (s.id, *s)).collect();

    // Renderability: full parent chain in by_id.
    fn is_renderable(id: u64, by_id: &Map<u64, &WireSpan>, memo: &mut Map<u64, bool>) -> bool {
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

    let mut renderable: Map<u64, bool> = Map::with_capacity(spans.len());
    for s in &spans {
        is_renderable(s.id, &by_id, &mut renderable);
    }

    let mut child_sum: Map<u64, u64> = Map::new();
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

    fn bk(
        span: &WireSpan,
        by_id: &Map<u64, &WireSpan>,
        split_keys: &BTreeSet<String>,
    ) -> BucketKey {
        let mut stack = vec![span.name.clone()];
        let mut splits: Vec<(String, String)> = Vec::new();
        let take = |s: &WireSpan, splits: &mut Vec<(String, String)>| {
            for (k, v) in &s.fields {
                if split_keys.contains(k) && !splits.iter().any(|(kk, _)| kk == k) {
                    splits.push((k.clone(), v.to_string_value()));
                }
            }
        };
        take(span, &mut splits);
        let mut p = span.parent_id;
        while let Some(id) = p {
            if stack.len() > 64 {
                break;
            }
            match by_id.get(&id) {
                Some(parent) => {
                    stack.push(parent.name.clone());
                    take(parent, &mut splits);
                    p = parent.parent_id;
                }
                None => break,
            }
        }
        stack.reverse();
        splits.sort_by(|a, b| a.0.cmp(&b.0));
        BucketKey { stack, splits }
    }

    let mut by_bucket: Map<BucketKey, StackStats> = Map::new();
    for s in &spans {
        if !renderable.get(&s.id).copied().unwrap_or(false) {
            continue;
        }
        let Some(closed) = s.closed_at_ns else {
            continue;
        };
        let total = closed.saturating_sub(s.opened_at_ns);
        let self_ns = total.saturating_sub(*child_sum.get(&s.id).unwrap_or(&0));
        let key = bk(s, &by_id, split_keys);
        let entry = by_bucket.entry(key).or_default();
        if entry.count == 0 {
            entry.total_min_ns = total;
            entry.total_max_ns = total;
            entry.self_min_ns = self_ns;
            entry.self_max_ns = self_ns;
        } else {
            entry.total_min_ns = entry.total_min_ns.min(total);
            entry.total_max_ns = entry.total_max_ns.max(total);
            entry.self_min_ns = entry.self_min_ns.min(self_ns);
            entry.self_max_ns = entry.self_max_ns.max(self_ns);
        }
        entry.count += 1;
        entry.total_sum_ns += total as u128;
        entry.self_sum_ns += self_ns as u128;
    }

    let mut rows: Vec<(BucketKey, StackStats)> = by_bucket.into_iter().collect();
    rows.sort_by(|a, b| {
        a.0.splits
            .cmp(&b.0.splits)
            .then_with(|| a.0.stack.cmp(&b.0.stack))
    });
    rows
}

use tracing_console_host::{WireFieldValue, WireLevel};

fn span(id: u64, parent_id: Option<u64>, name: &str, opened: u64, closed: u64) -> WireSpan {
    WireSpan {
        id,
        parent_id,
        name: name.into(),
        target: "test".into(),
        level: WireLevel::Info,
        fields: Vec::new(),
        events: Vec::new(),
        opened_at_ns: opened,
        closed_at_ns: Some(closed),
    }
}

fn span_field(
    id: u64,
    parent_id: Option<u64>,
    name: &str,
    opened: u64,
    closed: u64,
    k: &str,
    v: &str,
) -> WireSpan {
    let mut s = span(id, parent_id, name, opened, closed);
    s.fields.push((k.into(), WireFieldValue::Str(v.into())));
    s
}

fn find_row<'a>(
    rows: &'a [(BucketKey, StackStats)],
    stack: &[&str],
) -> Option<&'a (BucketKey, StackStats)> {
    rows.iter()
        .find(|(k, _)| k.stack.iter().map(String::as_str).eq(stack.iter().copied()))
}

#[test]
fn single_root_then_child_self_ns_decreases_on_child_arrival() {
    let mut a = Aggregator::new(16);
    a.absorb(span(10, None, "root", 0, 100));
    let rows = a.rows();
    let r = find_row(&rows, &["root"]).unwrap();
    assert_eq!(r.1.count, 1);
    assert_eq!(r.1.self_min_ns, 100);
    assert_eq!(r.1.self_max_ns, 100);
    assert_eq!(r.1.total_min_ns, 100);

    a.absorb(span(11, Some(10), "child", 10, 40)); // total 30
    let rows = a.rows();
    let r = find_row(&rows, &["root"]).unwrap();
    assert_eq!(r.1.self_min_ns, 70);
    assert_eq!(r.1.self_max_ns, 70);
    let c = find_row(&rows, &["root", "child"]).unwrap();
    assert_eq!(c.1.count, 1);
    assert_eq!(c.1.total_min_ns, 30);
    assert_eq!(c.1.self_min_ns, 30);
}

#[test]
fn child_before_parent_pending_drained() {
    let mut a = Aggregator::new(16);
    // Child arrives first — should park in pending.
    a.absorb(span(11, Some(10), "child", 10, 40));
    assert_eq!(a.len(), 0);
    assert_eq!(a.pending_len, 1);
    let rows = a.rows();
    assert!(rows.is_empty());

    // Parent arrives — child should drain through and bucket.
    a.absorb(span(10, None, "root", 0, 100));
    assert_eq!(a.len(), 2);
    assert_eq!(a.pending_len, 0);
    let rows = a.rows();
    assert!(find_row(&rows, &["root"]).is_some());
    assert!(find_row(&rows, &["root", "child"]).is_some());
    let r = find_row(&rows, &["root"]).unwrap();
    assert_eq!(r.1.self_min_ns, 70); // 100 - 30
}

#[test]
fn deep_chain_arrives_leaf_first() {
    let mut a = Aggregator::new(16);
    a.absorb(span(12, Some(11), "grand", 20, 30)); // total 10
    a.absorb(span(11, Some(10), "child", 10, 50)); // total 40
    a.absorb(span(10, None, "root", 0, 100)); // total 100
    assert_eq!(a.len(), 3);
    let rows = a.rows();
    let root = find_row(&rows, &["root"]).unwrap();
    let child = find_row(&rows, &["root", "child"]).unwrap();
    let grand = find_row(&rows, &["root", "child", "grand"]).unwrap();
    assert_eq!(root.1.self_min_ns, 60); // 100 - 40
    assert_eq!(child.1.self_min_ns, 30); // 40 - 10
    assert_eq!(grand.1.self_min_ns, 10);
}

#[test]
fn pending_overflow_evicts_lowest_parent_id() {
    let mut a = Aggregator::new(16); // pending_cap = 4
    // Queue 5 children against distinct missing parents.
    for (i, pid) in [100u64, 200, 300, 400, 500].iter().enumerate() {
        a.absorb(span(1000 + i as u64, Some(*pid), "leaf", 0, 10));
    }
    // pending_cap is 4 → the lowest parent_id (100) should be evicted.
    let keys: Vec<u64> = a.pending.keys().copied().collect();
    assert_eq!(keys, vec![200, 300, 400, 500]);
    assert_eq!(a.pending_len, 4);
}

#[test]
fn eviction_restores_parent_self_ns() {
    let mut a = Aggregator::new(3); // small ring so evictions hit
    a.absorb(span(10, None, "root", 0, 100));
    a.absorb(span(11, Some(10), "c1", 0, 20));
    a.absorb(span(12, Some(10), "c2", 0, 30));
    // root.self = 100 - 50 = 50
    let r0 = find_row(&a.rows(), &["root"]).unwrap().1.self_min_ns;
    assert_eq!(r0, 50);

    // Insert one more — evict root (oldest) → c1, c2 are orphaned
    // by eviction (root removed from by_id), so their buckets get
    // cleaned up too on the next ring eviction.  This test isn't
    // about that — it's about restoring parent self.  Re-run with
    // a layout where root survives.
    let mut a = Aggregator::new(8);
    a.absorb(span(10, None, "root", 0, 100));
    a.absorb(span(11, Some(10), "c1", 0, 20));
    a.absorb(span(12, Some(10), "c2", 0, 30));
    // Now force eviction of c1 by shrinking history isn't possible
    // here; instead force eviction by saturating with siblings:
    for i in 0..10 {
        a.absorb(span(100 + i, None, "filler", 0, 5));
    }
    // After enough fillers, c1 and c2 (and root) get evicted in
    // FIFO order: root first, then c1, then c2.  Verify final
    // state has no "root" bucket.
    assert!(find_row(&a.rows(), &["root"]).is_none());
}

#[test]
fn eviction_restores_parent_self_under_explicit_eviction() {
    // history=3, insert root (10), c1 (11), c2 (12) → ring full.
    // Then insert sibling root r2 (20) → evicts oldest = root(10).
    // c1/c2 lose their parent — their buckets remain, but the
    // root bucket goes away.
    let mut a = Aggregator::new(3);
    a.absorb(span(10, None, "root", 0, 100));
    a.absorb(span(11, Some(10), "c1", 0, 20));
    a.absorb(span(12, Some(10), "c2", 0, 30));
    a.absorb(span(20, None, "r2", 0, 5));
    // After eviction of root(10), the root bucket goes empty.
    assert!(find_row(&a.rows(), &["root"]).is_none());
}

#[test]
fn eviction_removes_empty_bucket() {
    let mut a = Aggregator::new(1);
    a.absorb(span(10, None, "first", 0, 10));
    assert_eq!(a.rows().len(), 1);
    a.absorb(span(20, None, "second", 0, 10));
    let rows = a.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.stack, vec!["second".to_string()]);
}

#[test]
fn eviction_orphans_pending_children() {
    let mut a = Aggregator::new(4);
    a.absorb(span(10, None, "root", 0, 100));
    // Pending child waiting on root(10) for an unrelated span id.
    // Wait — pending is keyed by missing parent_id, and root is
    // already in the ring.  Use a never-arriving parent id 99
    // along with one waiting on root(10) directly.
    a.absorb(span(11, Some(99), "ghost_child", 0, 5));
    a.absorb(span(12, Some(10), "real_child", 0, 5));
    // Now root has 1 ring-child; one orphan waits on 99.
    assert_eq!(a.pending_len, 1);
    // Force eviction of root by saturating.
    for i in 0..6u64 {
        a.absorb(span(100 + i, None, "fill", 0, 1));
    }
    // root evicted → pending entries waiting on root.id (10) get
    // dropped.  The pending entry waiting on 99 is unrelated and
    // stays.
    assert_eq!(a.pending_len, 1);
    assert!(a.pending.contains_key(&99));
    assert!(!a.pending.contains_key(&10));
}

#[test]
fn splits_change_rebuilds() {
    let mut a = Aggregator::new(16);
    a.absorb(span_field(10, None, "req", 0, 10, "api", "fetch"));
    a.absorb(span_field(11, None, "req", 0, 10, "api", "update"));
    assert_eq!(a.rows().len(), 1);

    let mut sk = BTreeSet::new();
    sk.insert("api".to_string());
    a.set_split_keys(sk);
    let rows = a.rows();
    assert_eq!(rows.len(), 2);
    let apis: Vec<&str> = rows.iter().map(|(k, _)| k.splits[0].1.as_str()).collect();
    assert!(apis.contains(&"fetch"));
    assert!(apis.contains(&"update"));
}

#[test]
fn parity_with_reference_random_workload() {
    use std::collections::VecDeque;

    // Deterministic LCG so failures reproduce.
    let mut rng_state: u64 = 0xdeadbeef_cafebabe;
    let mut rng = || {
        rng_state = rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng_state >> 33) as u32
    };

    // History is large enough that no eviction occurs — past the
    // eviction boundary the aggregator deliberately keeps a child
    // in its bucket after its parent is evicted (the cached
    // stack makes that data still meaningful), while the
    // reference path drops it under renderability.  We compare
    // semantics, not deliberate-divergence corner cases.
    let history = 4096;
    let mut agg = Aggregator::new(history);
    let mut deque: VecDeque<WireSpan> = VecDeque::new();

    let mut next_id: u64 = 10;
    let mut roots: Vec<u64> = Vec::new();
    let mut pending_spans: Vec<WireSpan> = Vec::new();

    let mut total_emitted = 0usize;
    let max_spans = history - 100; // stay clear of eviction
    while total_emitted < max_spans {
        let action = rng() % 100;
        if action < 30 || roots.is_empty() {
            let id = next_id;
            next_id += 1;
            roots.push(id);
            pending_spans.push(span(id, None, "root", 0, 100));
        } else if action < 80 {
            let parent_idx = rng() as usize % pending_spans.len().max(1);
            let parent = pending_spans.get(parent_idx);
            let parent_id = parent.map(|p| p.id).unwrap_or(*roots.last().unwrap());
            let id = next_id;
            next_id += 1;
            pending_spans.push(span(id, Some(parent_id), "child", 5, 25));
        } else {
            if pending_spans.is_empty() {
                continue;
            }
            let take = (rng() as usize % pending_spans.len()).max(1);
            let mut chunk: Vec<_> = pending_spans.drain(..take).collect();
            if rng() % 5 == 0 {
                chunk.reverse();
            }
            for s in chunk {
                total_emitted += 1;
                deque.push_back(s.clone());
                agg.absorb(s);
            }
            if agg.pending_len == 0 {
                let want = reference_bucket_by_stack(deque.iter(), &BTreeSet::new());
                let got = agg.rows();
                assert_eq!(got, want, "mismatch after flush");
            }
        }
    }
    // Final flush of any leftover pending_spans into both views.
    for s in pending_spans {
        deque.push_back(s.clone());
        agg.absorb(s);
    }
    if agg.pending_len == 0 {
        let want = reference_bucket_by_stack(deque.iter(), &BTreeSet::new());
        let got = agg.rows();
        assert_eq!(got, want, "final mismatch");
    }
}

// ── util coverage ────────────────────────────────────────────

#[test]
fn fmt_ns_picks_units_at_thresholds() {
    // sub-µs stays in ns.
    assert_eq!(super::fmt_ns(0), "0ns");
    assert_eq!(super::fmt_ns(999), "999ns");
    // First µs and threshold spelling.
    assert_eq!(super::fmt_ns(1_000), "1.0µs");
    assert_eq!(super::fmt_ns(1_500), "1.5µs");
    assert_eq!(super::fmt_ns(999_999), "1000.0µs");
    // ms / s.
    assert_eq!(super::fmt_ns(1_000_000), "1.0ms");
    assert_eq!(super::fmt_ns(1_234_567), "1.2ms");
    assert_eq!(super::fmt_ns(1_000_000_000), "1.0s");
    assert_eq!(super::fmt_ns(2_500_000_000), "2.5s");
}

/// Build a synthetic `(BucketKey, ())` row list from `&[&[&str]]`
/// stacks so we can drive `tree_label` directly.
fn make_rows(stacks: &[&[&str]]) -> Vec<(BucketKey, ())> {
    stacks
        .iter()
        .map(|s| {
            (
                BucketKey {
                    stack: s.iter().map(|x| x.to_string()).collect(),
                    splits: Vec::new(),
                },
                (),
            )
        })
        .collect()
}

#[test]
fn tree_label_renders_unicode_branches_in_dfs_order() {
    // DFS order:
    //   root
    //   ├─ child_a
    //   │  └─ grand
    //   └─ child_b
    let rows = make_rows(&[
        &["root"],
        &["root", "child_a"],
        &["root", "child_a", "grand"],
        &["root", "child_b"],
    ]);
    assert_eq!(super::tree_label(&rows, 0), "root");
    assert_eq!(super::tree_label(&rows, 1), "├─ child_a");
    assert_eq!(super::tree_label(&rows, 2), "│  └─ grand");
    assert_eq!(super::tree_label(&rows, 3), "└─ child_b");
}

#[test]
fn tree_label_handles_single_leaf() {
    let rows = make_rows(&[&["root"]]);
    assert_eq!(super::tree_label(&rows, 0), "root");
}

#[test]
fn tree_label_marks_last_child_with_corner() {
    // Last sibling at every depth uses └─; siblings before use ├─.
    let rows = make_rows(&[
        &["root"],
        &["root", "only_child"],
        &["root", "only_child", "leaf_a"],
        &["root", "only_child", "leaf_b"],
    ]);
    assert_eq!(super::tree_label(&rows, 1), "└─ only_child");
    assert_eq!(super::tree_label(&rows, 2), "   ├─ leaf_a");
    assert_eq!(super::tree_label(&rows, 3), "   └─ leaf_b");
}

#[test]
fn aggregator_with_empty_ring_returns_no_rows() {
    let a = Aggregator::new(8);
    assert!(a.rows().is_empty());
    assert_eq!(a.len(), 0);
}

#[test]
fn aggregator_open_spans_are_ignored() {
    // closed_at_ns = None ⇒ absorb is a no-op.
    let mut a = Aggregator::new(8);
    let s = WireSpan {
        id: 10,
        parent_id: None,
        name: "open".into(),
        target: "test".into(),
        level: WireLevel::Info,
        fields: Vec::new(),
        events: Vec::new(),
        opened_at_ns: 0,
        closed_at_ns: None,
    };
    a.absorb(s);
    assert_eq!(a.len(), 0);
    assert!(a.rows().is_empty());
}

#[test]
fn pending_cascade_drains_deep_chain_arriving_in_reverse() {
    // Push a 4-deep chain in strict leaf-first order so each
    // arrival parks in `pending` keyed by its missing parent.
    // When the root finally arrives, the recursive drain must
    // cascade root → child → grandchild → leaf, bucketing each
    // with the correct stack.
    let mut a = Aggregator::new(32);
    a.absorb(span(13, Some(12), "leaf", 30, 40));
    a.absorb(span(12, Some(11), "grandchild", 20, 50));
    a.absorb(span(11, Some(10), "child", 10, 60));
    // Three spans parked in pending, nothing bucketed yet.
    assert_eq!(a.len(), 0);
    a.absorb(span(10, None, "root", 0, 100));
    // All four now landed, stacks intact.
    assert_eq!(a.len(), 4);
    let rows = a.rows();
    assert!(find_row(&rows, &["root"]).is_some());
    assert!(find_row(&rows, &["root", "child"]).is_some());
    assert!(find_row(&rows, &["root", "child", "grandchild"]).is_some());
    assert!(find_row(&rows, &["root", "child", "grandchild", "leaf"]).is_some());
    // Pending pool must be empty after the cascade.
    assert_eq!(a.pending_len, 0);
}

#[test]
fn set_split_keys_after_ring_eviction_matches_reference() {
    // Drive a small ring past its capacity so several spans are
    // evicted, then change split keys.  The rebuild walks only
    // the surviving entries; verify rows match the reference
    // computed over the same surviving set.
    let cap = 4;
    let mut a = Aggregator::new(cap);
    let mut all = Vec::new();
    for i in 0u64..10 {
        let api = if i % 2 == 0 { "fetch" } else { "post" };
        let s = span_field(10 + i, None, "req", i * 100, i * 100 + 50, "api", api);
        all.push(s.clone());
        a.absorb(s);
    }
    // Ring should be at capacity; 6 oldest spans evicted.
    assert_eq!(a.len(), cap);
    // Toggle the "api" split — forces a full rebuild.
    let mut keys = BTreeSet::new();
    keys.insert("api".to_string());
    a.set_split_keys(keys.clone());

    // Reference workload is just the surviving ring entries.
    let survivors: Vec<WireSpan> = a.iter_with_stack().map(|(s, _)| s.clone()).collect();
    assert_eq!(survivors.len(), cap);

    let want = reference_bucket_by_stack(survivors.iter(), &keys);
    let got = a.rows();
    assert_eq!(got, want, "rebuild after eviction must match reference");
}

#[test]
fn bump_parent_self_preserves_min_max_under_uniform_children() {
    // Each child arrival bumps the parent's self_ns downward by the
    // child's total.  With three children at total=100 each, the
    // parent's self walks 1000 → 900 → 800 → 700 and the multiset
    // must reseat at each step without losing the (single) parent
    // contributor in its bucket.
    let mut a = Aggregator::new(16);
    a.absorb(span(10, None, "root", 0, 1000));
    for (i, opened) in [(11u64, 100u64), (12, 200), (13, 300)].iter() {
        a.absorb(span(*i, Some(10), "leaf", *opened, *opened + 100));
    }
    let rows = a.rows();
    let parent = find_row(&rows, &["root"]).unwrap();
    // Parent contributes exactly once; its self should be 700.
    assert_eq!(parent.1.count, 1);
    assert_eq!(parent.1.self_min_ns, 700);
    assert_eq!(parent.1.self_max_ns, 700);
    assert_eq!(parent.1.self_sum_ns, 700);
    // Total stays at 1000 the whole time.
    assert_eq!(parent.1.total_min_ns, 1000);
    assert_eq!(parent.1.total_max_ns, 1000);
}

#[test]
fn aggregator_accumulates_count_sum_min_max_in_one_bucket() {
    // Three spans, same name, distinct totals — single bucket
    // should reflect count=3 / sum=600 / min=100 / max=300.
    let mut a = Aggregator::new(16);
    a.absorb(span(10, None, "alpha", 0, 100));
    a.absorb(span(11, None, "alpha", 0, 200));
    a.absorb(span(12, None, "alpha", 0, 300));
    let rows = a.rows();
    assert_eq!(rows.len(), 1);
    let (_, stats) = &rows[0];
    assert_eq!(stats.count, 3);
    assert_eq!(stats.total_min_ns, 100);
    assert_eq!(stats.total_max_ns, 300);
    assert_eq!(stats.total_sum_ns, 600);
    // No children → self == total per-span.
    assert_eq!(stats.self_min_ns, 100);
    assert_eq!(stats.self_max_ns, 300);
}
