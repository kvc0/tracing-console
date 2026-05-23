//! The `Aggregator` itself + its bucket-level internals.  Everything
//! private to this file is intentional: the rolling per-bucket
//! state (`BucketState`, the multiset helpers, `Entry`) is never
//! exposed.  Read access lives behind `Aggregator::rows`,
//! `iter_with_stack`, etc.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use tracing_console_host::WireSpan;

use super::types::{BucketKey, StackStats};

#[derive(Debug, Clone, Default)]
struct BucketState {
    count: u64,
    total_sum_ns: u128,
    self_sum_ns: u128,
    /// Multisets over current contributors.  `min = first_key_value`,
    /// `max = last_key_value`.  Both ops are `O(log n)`.
    total_ns_multiset: BTreeMap<u64, u32>,
    self_ns_multiset: BTreeMap<u64, u32>,
}

impl BucketState {
    fn add(&mut self, total_ns: u64, self_ns: u64) {
        self.count += 1;
        self.total_sum_ns += total_ns as u128;
        self.self_sum_ns += self_ns as u128;
        *self.total_ns_multiset.entry(total_ns).or_insert(0) += 1;
        *self.self_ns_multiset.entry(self_ns).or_insert(0) += 1;
    }

    /// Reverse a prior `add`.  Caller guarantees the (total_ns,
    /// self_ns) pair was previously recorded — anything else would
    /// corrupt counters.
    fn remove(&mut self, total_ns: u64, self_ns: u64) {
        debug_assert!(self.count > 0);
        self.count -= 1;
        self.total_sum_ns -= total_ns as u128;
        self.self_sum_ns -= self_ns as u128;
        ms_dec(&mut self.total_ns_multiset, total_ns);
        ms_dec(&mut self.self_ns_multiset, self_ns);
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn to_stack_stats(&self) -> StackStats {
        let (total_min, total_max) = ms_min_max(&self.total_ns_multiset);
        let (self_min, self_max) = ms_min_max(&self.self_ns_multiset);
        StackStats {
            count: self.count,
            total_min_ns: total_min,
            total_max_ns: total_max,
            total_sum_ns: self.total_sum_ns,
            self_min_ns: self_min,
            self_max_ns: self_max,
            self_sum_ns: self.self_sum_ns,
        }
    }
}

fn ms_dec(ms: &mut BTreeMap<u64, u32>, key: u64) {
    let mut empty = false;
    if let Some(c) = ms.get_mut(&key) {
        *c -= 1;
        empty = *c == 0;
    }
    if empty {
        ms.remove(&key);
    }
}

fn ms_min_max(ms: &BTreeMap<u64, u32>) -> (u64, u64) {
    match (ms.keys().next().copied(), ms.keys().next_back().copied()) {
        (Some(min), Some(max)) => (min, max),
        _ => (0, 0),
    }
}

#[derive(Debug, Clone)]
struct Entry {
    span: WireSpan,
    bucket: BucketKey,
    total_ns: u64,
    /// Current self-time contribution recorded in `buckets[bucket]`.
    /// Updated in place when this span's children arrive or are
    /// evicted; written back here so eviction knows what to subtract.
    self_ns: u64,
}

pub struct Aggregator {
    /// Span ids in insertion order — drives the ring eviction.
    order: VecDeque<u64>,
    /// Resolved entries keyed by span id.  Stays in lock-step with
    /// `order`: every id in `order` has an entry here, and vice versa.
    by_id: HashMap<u64, Entry>,
    /// For each id in `by_id` whose children have arrived, the sum of
    /// those children's `total_ns`.  Used to update the parent's
    /// self_ns when a child arrives or is evicted.
    child_sum: HashMap<u64, u64>,
    /// Children that arrived before their parent, keyed by the
    /// missing parent id.  Drained when the parent shows up.  Bounded
    /// by `pending_cap`; overflow evicts the lowest-keyed bucket
    /// (oldest missing parent — least likely to ever arrive).
    ///
    /// `pub(super)` so the sibling `tests` submodule can inspect the
    /// pool's contents directly; the field stays effectively private
    /// to the `aggregate` module from the outside.
    pub(super) pending: BTreeMap<u64, Vec<WireSpan>>,
    pub(super) pending_len: usize,
    pending_cap: usize,
    /// Per-bucket aggregate state.
    buckets: HashMap<BucketKey, BucketState>,
    /// User-selected split keys.  Mutating this triggers a full
    /// rebuild — handled in `set_split_keys`.
    split_keys: BTreeSet<String>,
    history_budget: usize,
}

impl Aggregator {
    pub fn new(history_budget: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(history_budget.min(4096)),
            by_id: HashMap::new(),
            child_sum: HashMap::new(),
            pending: BTreeMap::new(),
            pending_len: 0,
            pending_cap: (history_budget / 4).max(1),
            buckets: HashMap::new(),
            split_keys: BTreeSet::new(),
            history_budget,
        }
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn split_keys(&self) -> &BTreeSet<String> {
        &self.split_keys
    }

    pub fn set_split_keys(&mut self, new_keys: BTreeSet<String>) {
        if new_keys == self.split_keys {
            return;
        }
        self.split_keys = new_keys;
        self.rebuild_buckets();
    }

    /// Iterate `(span, resolved_stack)` for every entry in the ring,
    /// in insertion order.  Used by `candidate_split_keys` — the
    /// resolved stack is the same value `bucket.stack` would carry
    /// with `split_keys = ∅`, since stacks don't depend on splits.
    pub fn iter_with_stack(&self) -> impl Iterator<Item = (&WireSpan, &Vec<String>)> + '_ {
        self.order
            .iter()
            .filter_map(move |id| self.by_id.get(id).map(|e| (&e.span, &e.bucket.stack)))
    }

    /// Resolved stack for a span currently in the ring.  Used by
    /// the graph view to check whether a freshly-absorbed span
    /// belongs to the locked bucket in `O(1)`.  Returns `None` if
    /// the span isn't in the ring (parked in pending, evicted, or
    /// never made it past the `closed_at_ns` filter).
    pub fn resolved_stack(&self, id: u64) -> Option<&[String]> {
        self.by_id.get(&id).map(|e| e.bucket.stack.as_slice())
    }

    /// Sum of direct-children totals currently in the ring for the
    /// given span id; 0 if none.  Used by the graph view to derive
    /// `self_ns = total - child_sum` for the metric toggle.
    pub fn child_sum_for(&self, id: u64) -> u64 {
        self.child_sum.get(&id).copied().unwrap_or(0)
    }

    /// Fetch a span by id from the ring, if present.  Used by the
    /// graph view's rehydrate path so it can re-record every
    /// matching ring entry without holding the iterator borrow.
    pub fn span_by_id(&self, id: u64) -> Option<&WireSpan> {
        self.by_id.get(&id).map(|e| &e.span)
    }

    /// Resolve splits for the span with `id` against an arbitrary
    /// key set (unrelated to `self.split_keys`).  Walks the parent
    /// chain via `by_id`, taking fields closest-to-leaf-first; same
    /// algorithm as `collect_splits` but parametrised so the graph
    /// view can produce its own series keys without touching the
    /// aggregator's own split state.
    pub fn collect_splits_for(&self, id: u64, keys: &BTreeSet<String>) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        let mut cursor = self.by_id.get(&id);
        let mut depth = 0;
        while let Some(entry) = cursor {
            if depth > 64 {
                break;
            }
            depth += 1;
            for (k, v) in &entry.span.fields {
                if keys.contains(k) && !out.iter().any(|(kk, _)| kk == k) {
                    out.push((k.clone(), v.to_string_value()));
                }
            }
            cursor = entry.span.parent_id.and_then(|pid| self.by_id.get(&pid));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn absorb(&mut self, span: WireSpan) {
        // Discard open spans — same as the old `bucket_by_stack`.
        if span.closed_at_ns.is_none() {
            return;
        }
        self.absorb_resolved(span);
    }

    fn absorb_resolved(&mut self, span: WireSpan) {
        let parent_id = span.parent_id;

        // 1. Try to resolve this span's bucket.
        let (bucket, parent_in_ring) = match parent_id {
            None => (self.resolve_root(&span), false),
            Some(p) => match self.by_id.get(&p) {
                Some(parent_entry) => {
                    let b = self.resolve_with_parent(&span, parent_entry);
                    (b, true)
                }
                None => {
                    // Parent missing — park in pending.
                    self.park_pending(p, span);
                    return;
                }
            },
        };

        // 2. Compute self_ns from already-arrived children.
        let total_ns = span
            .closed_at_ns
            .unwrap_or(span.opened_at_ns)
            .saturating_sub(span.opened_at_ns);
        let already_summed = self.child_sum.get(&span.id).copied().unwrap_or(0);
        let self_ns = total_ns.saturating_sub(already_summed);

        // 3. Make room.
        while self.order.len() >= self.history_budget {
            self.evict_oldest();
        }

        // 4. Update the parent's contribution if it's in the ring.
        let id = span.id;
        if parent_in_ring {
            // Safety on the unwrap: `parent_in_ring` was set from a
            // `by_id.get` hit above; `evict_oldest` only fires when the
            // ring is at capacity, and a parent fresh from a hit will
            // not be evicted to make room for its own child (the front
            // of the queue is the oldest; the parent was just observed,
            // so it's not at the front unless the ring is size 1 and
            // this span itself is the only thing — in which case there
            // is no parent_in_ring).  Still, be defensive: if eviction
            // somehow dropped the parent, treat this span as parentless.
            let parent_id = parent_id.unwrap();
            if self.by_id.contains_key(&parent_id) {
                let new_sum = self.child_sum.entry(parent_id).or_default();
                *new_sum += total_ns;
                let new_sum = *new_sum;
                self.bump_parent_self(parent_id, new_sum);
            }
        }

        // 5. Add this span's contribution to its bucket.
        self.buckets
            .entry(bucket.clone())
            .or_default()
            .add(total_ns, self_ns);

        // 6. Insert the entry.
        self.order.push_back(id);
        self.by_id.insert(
            id,
            Entry {
                span,
                bucket,
                total_ns,
                self_ns,
            },
        );

        // 7. Drain any pending children of this id.
        if let Some(waiters) = self.pending.remove(&id) {
            self.pending_len -= waiters.len();
            for w in waiters {
                self.absorb_resolved(w);
            }
        }
    }

    fn park_pending(&mut self, missing_parent: u64, span: WireSpan) {
        self.pending.entry(missing_parent).or_default().push(span);
        self.pending_len += 1;
        while self.pending_len > self.pending_cap {
            match self.pending.pop_first() {
                Some((_, dropped)) => {
                    self.pending_len -= dropped.len();
                }
                None => break,
            }
        }
    }

    fn evict_oldest(&mut self) {
        let Some(id) = self.order.pop_front() else {
            return;
        };
        let Some(entry) = self.by_id.remove(&id) else {
            return;
        };
        // Any pending entries waiting on this span as parent are now
        // permanently orphaned — same fate as the old renderability
        // check.  Drop them.
        if let Some(orphans) = self.pending.remove(&id) {
            self.pending_len -= orphans.len();
        }
        // Drop this span's child_sum bookkeeping — it can't accept
        // more children now that it's gone.
        self.child_sum.remove(&id);

        // Remove its bucket contribution.
        let now_empty = {
            let state = self
                .buckets
                .get_mut(&entry.bucket)
                .expect("entry's bucket must be present while entry is in the ring");
            state.remove(entry.total_ns, entry.self_ns);
            state.is_empty()
        };
        if now_empty {
            self.buckets.remove(&entry.bucket);
        }

        // Bump the parent back up: this span's total was subtracted
        // from the parent's self_ns when it arrived; now that this
        // span is leaving, give that subtraction back.
        if let Some(parent_id) = entry.span.parent_id {
            if self.by_id.contains_key(&parent_id) {
                if let Some(sum) = self.child_sum.get_mut(&parent_id) {
                    *sum = sum.saturating_sub(entry.total_ns);
                    let new_sum = *sum;
                    if new_sum == 0 {
                        self.child_sum.remove(&parent_id);
                    }
                    self.bump_parent_self(parent_id, new_sum);
                }
            }
        }
    }

    /// Rewrite the bucket contribution of `parent_id` so its
    /// `self_ns` reflects the new child_sum.  Same bucket — only the
    /// self component shifts.
    fn bump_parent_self(&mut self, parent_id: u64, new_child_sum: u64) {
        let Some(parent_entry) = self.by_id.get_mut(&parent_id) else {
            return;
        };
        let new_self = parent_entry.total_ns.saturating_sub(new_child_sum);
        let old_self = parent_entry.self_ns;
        if new_self == old_self {
            return;
        }
        let bucket = parent_entry.bucket.clone();
        let total = parent_entry.total_ns;
        parent_entry.self_ns = new_self;
        let state = self
            .buckets
            .get_mut(&bucket)
            .expect("parent's bucket must be present");
        state.remove(total, old_self);
        state.add(total, new_self);
        // The bucket can't be empty here because the parent itself is
        // still contributing.
    }

    fn resolve_root(&self, span: &WireSpan) -> BucketKey {
        let stack = vec![span.name.clone()];
        let splits = collect_splits(span, None, &self.split_keys);
        BucketKey { stack, splits }
    }

    fn resolve_with_parent(&self, span: &WireSpan, parent_entry: &Entry) -> BucketKey {
        let mut stack = parent_entry.bucket.stack.clone();
        stack.push(span.name.clone());
        let splits = collect_splits(span, Some(&parent_entry.bucket.splits), &self.split_keys);
        BucketKey { stack, splits }
    }

    /// Snapshot every bucket as `(BucketKey, StackStats)` sorted in
    /// the same `(splits, stack)` order the old `bucket_by_stack`
    /// returned.  Cheap because `|buckets|` is small.
    pub fn rows(&self) -> Vec<(BucketKey, StackStats)> {
        let mut out: Vec<(BucketKey, StackStats)> = self
            .buckets
            .iter()
            .map(|(k, s)| (k.clone(), s.to_stack_stats()))
            .collect();
        out.sort_by(|a, b| {
            a.0.splits
                .cmp(&b.0.splits)
                .then_with(|| a.0.stack.cmp(&b.0.stack))
        });
        out
    }

    /// Wipe all buckets and re-bucket every entry in insertion order.
    /// One-time `O(N)` work; called when the user toggles a split
    /// key.  `pending` does not need rebuilding — those spans never
    /// made it into a bucket in the first place.
    fn rebuild_buckets(&mut self) {
        self.buckets.clear();
        self.child_sum.clear();
        // We need to walk `order` and rebuild each Entry's bucket
        // against the new split_keys, while ensuring parent bucket
        // lookups continue to work.  Strategy: pop all entries (clear
        // by_id), then re-absorb each one in insertion order through
        // the same code path used for live arrivals.  This re-runs
        // parent resolution and child_sum updates against the new
        // split_keys.
        let order = std::mem::take(&mut self.order);
        let mut by_id = std::mem::take(&mut self.by_id);
        let saved_pending = std::mem::take(&mut self.pending);
        let saved_pending_len = self.pending_len;
        self.pending_len = 0;
        for id in &order {
            if let Some(entry) = by_id.remove(id) {
                // Re-feed the WireSpan back through absorb.  Since
                // every parent appears before its children in `order`,
                // the bucket resolution succeeds without paging
                // through pending.
                self.absorb_resolved(entry.span);
            }
        }
        // Restore any pre-existing pending entries that never made it
        // into the ring.  Their parents still aren't here, so they
        // remain parked.
        self.pending = saved_pending;
        self.pending_len = saved_pending_len;
    }
}

/// Collect splits for `span`, inheriting from the parent's already-
/// resolved split list.  Closest-to-leaf wins: keys present on the
/// span itself override those of any ancestor.
fn collect_splits(
    span: &WireSpan,
    parent_splits: Option<&[(String, String)]>,
    split_keys: &BTreeSet<String>,
) -> Vec<(String, String)> {
    let mut splits: Vec<(String, String)> = Vec::new();
    // Take splits from the span itself first — these win over ancestors.
    for (k, v) in &span.fields {
        if split_keys.contains(k) && !splits.iter().any(|(kk, _)| kk == k) {
            splits.push((k.clone(), v.to_string_value()));
        }
    }
    if let Some(parent) = parent_splits {
        for (k, v) in parent {
            if !splits.iter().any(|(kk, _)| kk == k) {
                splits.push((k.clone(), v.clone()));
            }
        }
    }
    splits.sort_by(|a, b| a.0.cmp(&b.0));
    splits
}

// ── Manual Debug + Clone + Serialize/Deserialize ────────────────────

impl std::fmt::Debug for Aggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregator")
            .field("len", &self.order.len())
            .field("buckets", &self.buckets.len())
            .field("pending_len", &self.pending_len)
            .field("history_budget", &self.history_budget)
            .field("split_keys", &self.split_keys)
            .finish()
    }
}

impl Clone for Aggregator {
    fn clone(&self) -> Self {
        Self {
            order: self.order.clone(),
            by_id: self.by_id.clone(),
            child_sum: self.child_sum.clone(),
            pending: self.pending.clone(),
            pending_len: self.pending_len,
            pending_cap: self.pending_cap,
            buckets: self.buckets.clone(),
            split_keys: self.split_keys.clone(),
            history_budget: self.history_budget,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct AggregatorSnapshot {
    spans_in_order: Vec<WireSpan>,
    pending: BTreeMap<u64, Vec<WireSpan>>,
    split_keys: BTreeSet<String>,
    history_budget: usize,
}

impl Serialize for Aggregator {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let spans_in_order: Vec<WireSpan> = self
            .order
            .iter()
            .filter_map(|id| self.by_id.get(id).map(|e| e.span.clone()))
            .collect();
        let snap = AggregatorSnapshot {
            spans_in_order,
            pending: self.pending.clone(),
            split_keys: self.split_keys.clone(),
            history_budget: self.history_budget,
        };
        snap.serialize(ser)
    }
}

impl<'de> Deserialize<'de> for Aggregator {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let snap = AggregatorSnapshot::deserialize(de)?;
        let mut agg = Aggregator::new(snap.history_budget);
        agg.split_keys = snap.split_keys;
        for span in snap.spans_in_order {
            agg.absorb(span);
        }
        // Replay parked pending spans last so their orphan state is
        // preserved (their parents truly are missing).
        for (pid, waiters) in snap.pending {
            for w in waiters {
                agg.park_pending(pid, w);
            }
        }
        Ok(agg)
    }
}
