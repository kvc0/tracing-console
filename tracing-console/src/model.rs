//! Application state for the console TUI.
//!
//! Decoupled from rendering so it can be tested without touching
//! ratatui / crossterm.  Both [`Model`] and [`Update`] are
//! `Serialize` + `Deserialize` so integration tests can construct
//! sequences of updates (or replay a captured `--states` dump) and
//! assert on the resulting model.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing_console_host::{WireLevelFilter, WireSpan};

use crate::aggregate::{BucketKey, StackStats, bucket_by_stack};

/// Half-second-bucket rolling-rate counter.  The model holds 9
/// buckets — the in-progress one plus 8 older half-seconds (4 s of
/// completed history).  `rate_hz` reports the average over the 8
/// completed buckets only; the in-progress bucket is ignored so
/// partial fills don't drag the displayed Hz down.
///
/// Skipped from serialization because it's purely transient
/// display state — round-trip tests would otherwise have to deal
/// with timing-dependent `Instant`s.
#[derive(Debug, Clone, Default)]
pub struct RateTracker {
    /// Per-half-second counts.  `head` indexes the in-progress
    /// (current) bucket.  `(head + 1) % BUCKETS` is the oldest.
    buckets: [u32; Self::BUCKETS],
    head: usize,
    /// `Instant` when the current bucket started; `None` until the
    /// first sample lands.
    bucket_start: Option<Instant>,
}

impl RateTracker {
    /// 8 completed half-second buckets + 1 in-progress = 4 s of
    /// completed window.
    pub const BUCKETS: usize = 9;
    /// Length of the completed window in seconds — `(BUCKETS - 1) / 2`.
    pub const WINDOW_SECS: f64 = 4.0;

    /// Record one event at `now`, advancing buckets as needed.
    pub fn record(&mut self, now: Instant) {
        match self.bucket_start {
            None => {
                self.bucket_start = Some(now);
            }
            Some(start) => {
                let elapsed_ms = now.duration_since(start).as_millis() as u64;
                if elapsed_ms >= 500 {
                    // How many half-seconds have rolled past since
                    // the current bucket opened.  Cap at `BUCKETS`
                    // since beyond that every bucket is stale anyway.
                    let advances = ((elapsed_ms / 500) as usize).min(Self::BUCKETS);
                    for _ in 0..advances {
                        self.head = (self.head + 1) % Self::BUCKETS;
                        self.buckets[self.head] = 0;
                    }
                    self.bucket_start =
                        Some(start + Duration::from_millis(500 * advances as u64));
                }
            }
        }
        self.buckets[self.head] = self.buckets[self.head].saturating_add(1);
    }

    /// Rate in Hz over the completed buckets, ignoring the in-progress
    /// one.  Returns 0.0 before any sample has landed.
    pub fn rate_hz(&self) -> f64 {
        if self.bucket_start.is_none() {
            return 0.0;
        }
        let mut sum: u64 = 0;
        for (i, &v) in self.buckets.iter().enumerate() {
            if i != self.head {
                sum += v as u64;
            }
        }
        sum as f64 / Self::WINDOW_SECS
    }
}

/// One visible line in the hierarchical tree view.
#[derive(Debug, Clone)]
pub struct VisibleRow {
    pub key: BucketKey,
    pub stats: StackStats,
    pub depth: usize,
    pub has_children: bool,
    pub is_expanded: bool,
}

/// Which pane currently owns keyboard navigation.  `Tab` toggles
/// between the two.  The level switcher is driven by global
/// Shift+letter shortcuts (Shift-O/I/D/T), so it doesn't take focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Focus {
    Stacks,
    Details,
}

/// The four level choices the switcher exposes, in display order.
pub const LEVEL_OPTIONS: &[WireLevelFilter] = &[
    WireLevelFilter::Off,
    WireLevelFilter::Info,
    WireLevelFilter::Debug,
    WireLevelFilter::Trace,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub spans: VecDeque<WireSpan>,
    /// Stack prefixes whose children should be revealed.  A row whose
    /// bucket-key proper prefixes are all in this set is visible.
    pub expanded: HashSet<BucketKey>,
    /// Field keys the user has selected to split the aggregation by.
    /// Empty by default → spans bucket purely by stack.
    pub split_keys: BTreeSet<String>,
    /// Selection index into the visible-row list (Stacks focus) or
    /// the details key list (Details focus).
    pub selected: usize,
    pub details_selected: usize,
    pub focus: Focus,
    pub connection: ConnectionStatus,
    pub status: Option<String>,
    pub history_budget: usize,
    /// Server-confirmed current cache-recording level — `None` until
    /// the server pushes its first `CacheLevel` message.  Only
    /// `Update::CacheLevelReceived` writes this; the user's
    /// Shift+letter shortcuts do *not* update it optimistically —
    /// confirmation flows from the server.
    pub cache_level: Option<WireLevelFilter>,
    /// Server-confirmed current chance percentage `[0.0, 100.0]`.
    /// Same lifecycle as `cache_level`: `None` until the server
    /// pushes its first `CacheChance`, and updates only via
    /// `Update::CacheChanceReceived`.
    pub cache_chance: Option<f64>,
    /// When `Some`, the user is typing a new chance value (after
    /// pressing `c`).  Only digits and `.` are accepted; `Enter`
    /// commits and emits [`Effect::RequestSetChance`]; `Esc`
    /// cancels and discards the buffer.  Empty string means the
    /// user pressed `c` and hasn't typed anything yet.
    pub chance_input: Option<String>,
    /// Rolling 10-second receive-rate counter, displayed as
    /// `N spans / NHz` in the header.  Transient — skipped from
    /// serialization.
    #[serde(skip)]
    pub rate: RateTracker,
}

impl Model {
    pub fn new(history_budget: usize) -> Self {
        Self {
            spans: VecDeque::new(),
            expanded: HashSet::new(),
            split_keys: BTreeSet::new(),
            selected: 0,
            details_selected: 0,
            focus: Focus::Stacks,
            connection: ConnectionStatus::Connecting,
            status: None,
            history_budget,
            cache_level: None,
            cache_chance: None,
            chance_input: None,
            rate: RateTracker::default(),
        }
    }

    pub fn apply(&mut self, update: Update) -> Effect {
        match update {
            Update::SpanReceived(span) => {
                if self.spans.len() >= self.history_budget {
                    self.spans.pop_front();
                }
                self.spans.push_back(span);
                self.rate.record(Instant::now());
                Effect::None
            }
            Update::SelectUp => {
                let n = self.current_pane_len();
                if n == 0 {
                    return Effect::None;
                }
                let cur = self.current_selected();
                let new = cur.saturating_sub(1);
                self.set_current_selected(new);
                Effect::None
            }
            Update::SelectDown => {
                let n = self.current_pane_len();
                if n == 0 {
                    return Effect::None;
                }
                let cur = self.current_selected();
                let new = (cur + 1).min(n - 1);
                self.set_current_selected(new);
                Effect::None
            }
            Update::ExpandSelected => {
                if self.focus == Focus::Stacks {
                    let rows = self.visible_rows();
                    if let Some(r) = rows.get(self.selected) {
                        if r.has_children {
                            self.expanded.insert(r.key.clone());
                        }
                    }
                }
                Effect::None
            }
            Update::ExpandAllSelected => {
                if self.focus != Focus::Stacks {
                    return Effect::None;
                }
                let rows = self.visible_rows();
                if let Some(r) = rows.get(self.selected) {
                    // Expand every descendant of the selected bucket
                    // that has its own children.  Cheap: one pass over
                    // every bucket in the (unfiltered) tree.
                    let root_stack = r.key.stack.clone();
                    let root_splits = r.key.splits.clone();
                    let all = bucket_by_stack(self.spans.iter(), &self.split_keys);
                    for (k, _) in &all {
                        if k.stack.starts_with(&root_stack)
                            && k.stack.len() > root_stack.len()
                            && k.splits == root_splits
                        {
                            for len in root_stack.len()..k.stack.len() {
                                self.expanded.insert(BucketKey {
                                    stack: k.stack[..len].to_vec(),
                                    splits: root_splits.clone(),
                                });
                            }
                        }
                    }
                }
                Effect::None
            }
            Update::CollapseSelected => {
                if self.focus != Focus::Stacks {
                    return Effect::None;
                }
                let rows = self.visible_rows();
                if let Some(r) = rows.get(self.selected) {
                    if r.is_expanded {
                        let root = r.key.clone();
                        self.expanded.retain(|k| {
                            !(k.splits == root.splits && k.stack.starts_with(&root.stack))
                        });
                    } else if r.depth > 0 {
                        // Jump to parent and collapse it.
                        let parent_stack: Vec<String> =
                            r.key.stack[..r.key.stack.len() - 1].to_vec();
                        let parent_splits = r.key.splits.clone();
                        self.expanded.retain(|k| {
                            !(k.splits == parent_splits && k.stack.starts_with(&parent_stack))
                        });
                        let new_rows = self.visible_rows();
                        if let Some((idx, _)) = new_rows.iter().enumerate().find(|(_, row)| {
                            row.key.stack == parent_stack && row.key.splits == parent_splits
                        }) {
                            self.selected = idx;
                        }
                    }
                }
                Effect::None
            }
            Update::SwitchFocus => {
                self.focus = match self.focus {
                    Focus::Stacks => Focus::Details,
                    Focus::Details => Focus::Stacks,
                };
                if self.focus == Focus::Details {
                    self.details_selected = 0;
                }
                Effect::None
            }
            Update::ToggleSplitSelected => {
                if self.focus != Focus::Details {
                    return Effect::None;
                }
                let keys = self.candidate_split_keys();
                if let Some(k) = keys.get(self.details_selected).cloned() {
                    if !self.split_keys.remove(&k) {
                        self.split_keys.insert(k);
                    }
                    // Splits changed → row identities change.  Drop
                    // selection / expansion to avoid stale references.
                    self.expanded.clear();
                    self.selected = 0;
                }
                Effect::None
            }
            Update::Connected => {
                self.connection = ConnectionStatus::Connected;
                self.status = None;
                Effect::None
            }
            Update::Disconnected(reason) => {
                self.connection = ConnectionStatus::Disconnected(reason);
                Effect::None
            }
            Update::Status(msg) => {
                self.status = Some(msg);
                Effect::None
            }
            Update::CacheLevelReceived(filter) => {
                self.cache_level = Some(filter);
                Effect::None
            }
            Update::RequestCacheLevel(filter) => Effect::RequestSetLevel(filter),
            Update::CacheChanceReceived(pct) => {
                self.cache_chance = Some(pct);
                Effect::None
            }
            Update::BeginChanceInput => {
                self.chance_input = Some(String::new());
                Effect::None
            }
            Update::ChanceInputChar(c) => {
                if let Some(buf) = self.chance_input.as_mut() {
                    // Accept only digits and a single decimal point.
                    // Anything else (whitespace, letters, etc.) is
                    // silently dropped — input mode never holds
                    // garbage that the user could commit.
                    if c.is_ascii_digit() || (c == '.' && !buf.contains('.')) {
                        buf.push(c);
                    }
                }
                Effect::None
            }
            Update::ChanceInputBackspace => {
                if let Some(buf) = self.chance_input.as_mut() {
                    buf.pop();
                }
                Effect::None
            }
            Update::ChanceInputCancel => {
                self.chance_input = None;
                Effect::None
            }
            Update::ChanceInputCommit => {
                let Some(buf) = self.chance_input.take() else {
                    return Effect::None;
                };
                // Empty buffer / invalid parse / out-of-range →
                // revert silently, keep `cache_chance` untouched.
                match buf.parse::<f64>() {
                    Ok(v) if v.is_finite() && (0.0..=100.0).contains(&v) => {
                        Effect::RequestSetChance(v)
                    }
                    _ => Effect::None,
                }
            }
            Update::Quit => Effect::Quit,
        }
    }

    /// Recompute the visible (post-expansion) row list.
    pub fn visible_rows(&self) -> Vec<VisibleRow> {
        let rows = bucket_by_stack(self.spans.iter(), &self.split_keys);
        if rows.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(rows.len());
        for (i, (key, stats)) in rows.iter().enumerate() {
            // Visibility: every proper-stack prefix of this bucket's
            // stack must also be expanded.  Splits are shared between
            // parent and child within a subtree (children inherit), so
            // the expanded entry is keyed on (prefix_stack, splits).
            let mut visible = true;
            for k in 1..key.stack.len() {
                let parent_key = BucketKey {
                    stack: key.stack[..k].to_vec(),
                    splits: key.splits.clone(),
                };
                if !self.expanded.contains(&parent_key) {
                    visible = false;
                    break;
                }
            }
            if !visible {
                continue;
            }
            // has_children = some later row extends this stack by ≥1
            // level *within the same splits group*.  Rows are sorted
            // by (splits, stack), so once splits change we've left
            // this group entirely.
            let mut has_children = false;
            for (_, (next_key, _)) in rows.iter().enumerate().skip(i + 1) {
                if next_key.splits != key.splits {
                    break;
                }
                if next_key.stack.len() > key.stack.len() && next_key.stack.starts_with(&key.stack)
                {
                    has_children = true;
                    break;
                }
                if !next_key.stack.starts_with(&key.stack) {
                    break;
                }
            }
            let is_expanded = self.expanded.contains(key);
            out.push(VisibleRow {
                key: key.clone(),
                stats: stats.clone(),
                depth: key.stack.len() - 1,
                has_children,
                is_expanded,
            });
        }
        out
    }

    /// Field keys observed on spans whose resolved stack matches the
    /// currently-selected row.  Used by the Details pane to populate
    /// the togglable split-key list.
    pub fn candidate_split_keys(&self) -> Vec<String> {
        let Some(row) = self.selected_visible_row() else {
            return Vec::new();
        };
        let target_stack = &row.key.stack;
        let mut keys: BTreeSet<String> = BTreeSet::new();
        // Build id_to_span once so we resolve stacks the same way the
        // aggregator does.
        let by_id: std::collections::HashMap<u64, &WireSpan> =
            self.spans.iter().map(|s| (s.id, s)).collect();
        let split_empty: BTreeSet<String> = BTreeSet::new();
        for s in &self.spans {
            let k = crate::aggregate::bucket_key(s, &by_id, &split_empty);
            if k.stack == *target_stack {
                for (field_name, _) in &s.fields {
                    keys.insert(field_name.clone());
                }
            }
        }
        keys.into_iter().collect()
    }

    pub fn selected_visible_row(&self) -> Option<VisibleRow> {
        let rows = self.visible_rows();
        if rows.is_empty() {
            return None;
        }
        rows.get(self.selected.min(rows.len() - 1)).cloned()
    }

    fn current_pane_len(&self) -> usize {
        match self.focus {
            Focus::Stacks => self.visible_rows().len(),
            Focus::Details => self.candidate_split_keys().len(),
        }
    }

    fn current_selected(&self) -> usize {
        match self.focus {
            Focus::Stacks => self.selected,
            Focus::Details => self.details_selected,
        }
    }

    fn set_current_selected(&mut self, idx: usize) {
        match self.focus {
            Focus::Stacks => self.selected = idx,
            Focus::Details => self.details_selected = idx,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected(String),
}

/// Every state-change message that can move the model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    SpanReceived(WireSpan),
    SelectUp,
    SelectDown,
    /// Expand the highlighted row's direct children (one level).
    ExpandSelected,
    /// Expand every descendant of the highlighted row (recursive).
    ExpandAllSelected,
    /// Collapse the highlighted row, or if already collapsed, jump
    /// up to and collapse the parent.
    CollapseSelected,
    /// Tab: swap focus between Stacks and Details panes.
    SwitchFocus,
    /// In Details focus: toggle the highlighted metadata key in/out
    /// of `split_keys`.
    ToggleSplitSelected,
    /// Server pushed the current cache-recording level — display
    /// state is updated to reflect this (and only this).
    CacheLevelReceived(WireLevelFilter),
    /// User pressed a Shift+letter shortcut to request a new cache
    /// level.  The model returns `Effect::RequestSetLevel`, which
    /// the runtime turns into an outgoing `SetCacheLevel` RPC.
    /// `cache_level` does *not* change here — it only flips when the
    /// server pushes its `CacheLevel` reply back.
    RequestCacheLevel(WireLevelFilter),
    /// Server pushed the current cache-recording chance percentage.
    CacheChanceReceived(f64),
    /// User pressed `C` (with the level switcher visible) to begin
    /// editing the chance percentage.  Initialises `chance_input` to
    /// an empty buffer.
    BeginChanceInput,
    /// User typed a digit / `.` while editing the chance.  Anything
    /// else is silently ignored.
    ChanceInputChar(char),
    /// User pressed Backspace while editing the chance.
    ChanceInputBackspace,
    /// User pressed `Esc` — cancel chance input without commit.
    ChanceInputCancel,
    /// User pressed `Enter` while editing the chance.  If the buffer
    /// parses as an `f64` in `[0.0, 100.0]`, the model emits
    /// `Effect::RequestSetChance(value)`; otherwise it silently
    /// reverts (buffer is cleared, `cache_chance` stays unchanged).
    ChanceInputCommit,
    Connected,
    Disconnected(String),
    Status(String),
    Quit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    None,
    Quit,
    /// User committed a tentative level selection (Enter on the Level
    /// pane).  The runtime translates this into an outgoing
    /// `SetCacheLevel` RPC.  The model itself does not update
    /// `cache_level` — that only flips when the server confirms.
    RequestSetLevel(WireLevelFilter),
    /// User committed a chance-input buffer.  The runtime turns this
    /// into an outgoing `SetCacheChance` RPC.  `cache_chance` does
    /// not change locally — the server's `CacheChance` confirms.
    RequestSetChance(f64),
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_console_host::{WireLevel, WireSpan};

    fn span(id: u64, name: &str) -> WireSpan {
        span_with_parent(id, name, None)
    }

    fn span_with_parent(id: u64, name: &str, parent_id: Option<u64>) -> WireSpan {
        WireSpan {
            id,
            parent_id,
            name: name.into(),
            target: "test".into(),
            level: WireLevel::Info,
            fields: Vec::new(),
            events: vec![],
            opened_at_ns: 0,
            closed_at_ns: Some(1000),
        }
    }

    fn span_with_field(id: u64, name: &str, parent_id: Option<u64>, k: &str, v: &str) -> WireSpan {
        let mut s = span_with_parent(id, name, parent_id);
        s.fields.push((
            k.to_string(),
            tracing_console_host::WireFieldValue::Str(v.into()),
        ));
        s
    }

    #[test]
    fn span_received_appears_as_root_row() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "a")));
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.stack, vec!["a"]);
        assert_eq!(rows[0].depth, 0);
        assert!(!rows[0].has_children);
    }

    #[test]
    fn span_with_evicted_parent_is_dropped() {
        // history budget 1 → adding the child evicts the parent;
        // child should not render.
        let mut m = Model::new(1);
        m.apply(Update::SpanReceived(span(10, "parent")));
        m.apply(Update::SpanReceived(span_with_parent(
            11,
            "child",
            Some(10),
        )));
        let rows = m.visible_rows();
        // Parent was evicted, child has missing parent → both dropped.
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn child_is_hidden_until_parent_is_expanded() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "a")));
        m.apply(Update::SpanReceived(span_with_parent(11, "b", Some(10))));
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].has_children);

        m.apply(Update::ExpandSelected);
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].key.stack, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn tab_toggles_focus_between_two_panes() {
        let mut m = Model::new(4);
        assert_eq!(m.focus, Focus::Stacks);
        m.apply(Update::SwitchFocus);
        assert_eq!(m.focus, Focus::Details);
        m.apply(Update::SwitchFocus);
        assert_eq!(m.focus, Focus::Stacks);
    }

    #[test]
    fn request_cache_level_emits_effect_without_updating_state() {
        let mut m = Model::new(4);
        let effect = m.apply(Update::RequestCacheLevel(WireLevelFilter::Debug));
        assert_eq!(effect, Effect::RequestSetLevel(WireLevelFilter::Debug));
        // Local state is untouched — the server hasn't confirmed yet.
        assert!(m.cache_level.is_none());
        // Server confirms.
        m.apply(Update::CacheLevelReceived(WireLevelFilter::Debug));
        assert_eq!(m.cache_level, Some(WireLevelFilter::Debug));
    }

    #[test]
    fn toggle_split_only_works_under_details_focus() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span_with_field(
            10, "a", None, "api", "fetch",
        )));
        // Toggle while Stacks-focused: no-op.
        m.apply(Update::ToggleSplitSelected);
        assert!(m.split_keys.is_empty());
        // Switch to Details, then toggle: api becomes a split key.
        m.apply(Update::SwitchFocus);
        m.apply(Update::ToggleSplitSelected);
        assert!(m.split_keys.contains("api"));
        // Toggle again removes.
        m.apply(Update::ToggleSplitSelected);
        assert!(m.split_keys.is_empty());
    }

    #[test]
    fn splits_separate_buckets() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(span_with_field(
            10, "req", None, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(span_with_field(
            11, "req", None, "api", "update",
        )));
        // No splits yet: 2 spans bucket into one row.
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stats.count, 2);

        m.split_keys.insert("api".to_string());
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.key.stack == vec!["req".to_string()]));
        let apis: Vec<&str> = rows.iter().map(|r| r.key.splits[0].1.as_str()).collect();
        assert!(apis.contains(&"fetch"));
        assert!(apis.contains(&"update"));
    }

    #[test]
    fn split_inherits_from_ancestor() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(span_with_field(
            10, "req", None, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(span_with_parent(
            11,
            "validate",
            Some(10),
        )));
        m.split_keys.insert("api".to_string());
        let rows = m.visible_rows();
        // root + child = 1 row visible (root); expand and we should
        // see the child carry the same `api=fetch` split inherited
        // from its parent.
        assert_eq!(rows.len(), 1);
        m.apply(Update::ExpandSelected);
        let rows = m.visible_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[1].key.stack,
            vec!["req".to_string(), "validate".to_string()]
        );
        assert_eq!(
            rows[1].key.splits,
            vec![("api".to_string(), "fetch".to_string())]
        );
    }

    #[test]
    fn select_navigation_clamps() {
        let mut m = Model::new(8);
        m.apply(Update::SelectDown);
        assert_eq!(m.selected, 0);
        m.apply(Update::SpanReceived(span(10, "a")));
        m.apply(Update::SpanReceived(span(20, "b")));
        m.apply(Update::SelectDown);
        m.apply(Update::SelectDown);
        assert_eq!(m.selected, 1);
        m.apply(Update::SelectUp);
        m.apply(Update::SelectUp);
        assert_eq!(m.selected, 0);
    }

    #[test]
    fn quit_returns_quit_effect() {
        let mut m = Model::new(8);
        assert_eq!(m.apply(Update::Quit), Effect::Quit);
    }

    #[test]
    fn updates_round_trip_through_json() {
        let updates = vec![
            Update::Status("hi".into()),
            Update::Connected,
            Update::SpanReceived(span(7, "round_trip")),
            Update::SelectDown,
            Update::ExpandSelected,
            Update::ExpandAllSelected,
            Update::CollapseSelected,
            Update::SwitchFocus,
            Update::ToggleSplitSelected,
            Update::Disconnected("eof".into()),
            Update::Quit,
        ];
        for u in updates {
            let json = serde_json::to_string(&u).unwrap();
            let back: Update = serde_json::from_str(&json).unwrap();
            let mut a = Model::new(4);
            let mut b = Model::new(4);
            a.apply(u.clone());
            b.apply(back);
            assert_eq!(
                serde_json::to_string(&a).unwrap(),
                serde_json::to_string(&b).unwrap(),
            );
        }
    }
}
