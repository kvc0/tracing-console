//! Core model: `Model`, `Focus`, `ConnectionStatus`, `RateTracker`,
//! `VisibleRow`, and the gigantic `Model::apply` reducer.  The
//! reducer covers both the table and graph paths in one place
//! because every update lands here.

use std::collections::{BTreeSet, HashSet};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing_console_host::WireLevelFilter;

use crate::aggregate::{Aggregator, BucketKey, StackStats, candidate_split_keys_for};

use super::graph::{
    GraphFocus, GraphState, Metric, SortColumn, ViewMode, parse_agg_input, parse_lookback_input,
};
use super::update::{Effect, Update};

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
    /// Rolling-window aggregator.  Owns the bounded span ring, the
    /// per-bucket aggregates, and the active `split_keys` — all kept
    /// up to date incrementally by `apply(SpanReceived)`.
    pub agg: Aggregator,
    /// Stack prefixes whose children should be revealed.  A row whose
    /// bucket-key proper prefixes are all in this set is visible.
    pub expanded: HashSet<BucketKey>,
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
    /// Top-level view dispatch.  Default `Table`; pressing `g` on a
    /// highlighted row swaps to `Graph(GraphState{..})` locked onto
    /// that row's bucket.  Pressing `g` again returns to `Table`.
    pub view: ViewMode,
    /// Rolling 10-second receive-rate counter, displayed as
    /// `N spans / NHz` in the header.  Transient — skipped from
    /// serialization.
    #[serde(skip)]
    pub rate: RateTracker,
}

impl Model {
    pub fn new(history_budget: usize) -> Self {
        Self {
            agg: Aggregator::new(history_budget),
            expanded: HashSet::new(),
            selected: 0,
            details_selected: 0,
            focus: Focus::Stacks,
            connection: ConnectionStatus::Connecting,
            status: None,
            history_budget,
            cache_level: None,
            cache_chance: None,
            chance_input: None,
            view: ViewMode::Table,
            rate: RateTracker::default(),
        }
    }

    pub fn split_keys(&self) -> &BTreeSet<String> {
        self.agg.split_keys()
    }

    pub fn apply(&mut self, update: Update) -> Effect {
        match update {
            Update::SpanReceived(span) => {
                // If the graph view is active, clone the span up
                // front so we can feed the per-bucket time series
                // store after the aggregator has resolved its
                // stack.  Cost is one clone per span, paid only
                // while the user is actively watching a graph.
                let graph_clone = matches!(self.view, ViewMode::Graph(_)).then(|| span.clone());
                let span_id = span.id;
                self.agg.absorb(span);
                self.rate.record(Instant::now());
                if let (ViewMode::Graph(gs), Some(cloned)) = (&mut self.view, graph_clone) {
                    if let Some(stack) = self.agg.resolved_stack(span_id) {
                        if stack == gs.locked_stack.as_slice() {
                            gs.record_span(&self.agg, &cloned);
                        }
                    }
                }
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
                    let all = self.agg.rows();
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
                    let mut new_keys = self.agg.split_keys().clone();
                    if !new_keys.remove(&k) {
                        new_keys.insert(k);
                    }
                    self.agg.set_split_keys(new_keys);
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
            // ── Graph view ──────────────────────────────────────
            Update::ToggleGraph => {
                self.view = match std::mem::replace(&mut self.view, ViewMode::Table) {
                    ViewMode::Table => match self.selected_visible_row() {
                        Some(row) => ViewMode::Graph(GraphState::new(row.key.stack)),
                        None => ViewMode::Table,
                    },
                    ViewMode::Graph(_) => ViewMode::Table,
                };
                // If we just entered graph mode, prime the store
                // from the aggregator's ring so the chart isn't empty
                // until new spans arrive.
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.rehydrate(&self.agg);
                }
                Effect::None
            }
            Update::SetGraphAgg(mode) => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.aggregation = mode;
                }
                Effect::None
            }
            Update::ToggleGraphMetric => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.metric = match gs.metric {
                        Metric::Total => Metric::SelfTime,
                        Metric::SelfTime => Metric::Total,
                    };
                    gs.rehydrate(&self.agg);
                }
                Effect::None
            }
            Update::BeginGraphAggInput => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.agg_input = Some(String::new());
                }
                Effect::None
            }
            Update::GraphAggInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.agg_input.as_mut() {
                        // Accept lowercase letters, digits, and a
                        // single `.`.  Anything else is silently
                        // dropped so the user can't escape the modal
                        // with a stray keystroke.  Uppercase letters
                        // are lowered so `Min` / `MAX` / `Avg` work.
                        let c = c.to_ascii_lowercase();
                        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' {
                            if c == '.' && buf.contains('.') {
                                // Already has a decimal point; drop.
                            } else {
                                buf.push(c);
                            }
                        }
                    }
                }
                Effect::None
            }
            Update::GraphAggInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.agg_input.as_mut() {
                        buf.pop();
                    }
                }
                Effect::None
            }
            Update::GraphAggInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.agg_input = None;
                }
                Effect::None
            }
            Update::GraphAggInputCommit => {
                let parsed = if let ViewMode::Graph(gs) = &mut self.view {
                    let Some(buf) = gs.agg_input.take() else {
                        return Effect::None;
                    };
                    parse_agg_input(&buf)
                } else {
                    None
                };
                if let (Some(mode), ViewMode::Graph(gs)) = (parsed, &mut self.view) {
                    gs.aggregation = mode;
                }
                Effect::None
            }
            Update::BeginGraphWindowInput => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.window_input = Some(String::new());
                }
                Effect::None
            }
            Update::GraphWindowInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.window_input.as_mut() {
                        if c.is_ascii_digit() || (c == '.' && !buf.contains('.')) {
                            buf.push(c);
                        }
                    }
                }
                Effect::None
            }
            Update::GraphWindowInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.window_input.as_mut() {
                        buf.pop();
                    }
                }
                Effect::None
            }
            Update::GraphWindowInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.window_input = None;
                }
                Effect::None
            }
            Update::GraphWindowInputCommit => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    let Some(buf) = gs.window_input.take() else {
                        return Effect::None;
                    };
                    if let Ok(v) = buf.parse::<f64>() {
                        if v.is_finite() && v > 0.0 {
                            gs.window_secs = v;
                            gs.rehydrate(&self.agg);
                        }
                    }
                }
                Effect::None
            }
            Update::BeginGraphLookbackInput => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.lookback_input = Some(String::new());
                }
                Effect::None
            }
            Update::GraphLookbackInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.lookback_input.as_mut() {
                        // Already terminated by a unit suffix → reject
                        // further input so the user can't type "5sm".
                        let has_suffix =
                            buf.ends_with('s') || buf.ends_with('m');
                        if has_suffix {
                            // no-op
                        } else if c.is_ascii_digit() {
                            buf.push(c);
                        } else if c == '.' && !buf.contains('.') {
                            buf.push(c);
                        } else if (c == 's' || c == 'm') && !buf.is_empty() {
                            buf.push(c);
                        }
                    }
                }
                Effect::None
            }
            Update::GraphLookbackInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    if let Some(buf) = gs.lookback_input.as_mut() {
                        buf.pop();
                    }
                }
                Effect::None
            }
            Update::GraphLookbackInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.lookback_input = None;
                }
                Effect::None
            }
            Update::GraphLookbackInputCommit => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    let Some(buf) = gs.lookback_input.take() else {
                        return Effect::None;
                    };
                    if let Some(secs) = parse_lookback_input(&buf) {
                        // Lookback is a pure projection knob — no
                        // rehydrate needed; the next render reads
                        // it as `x_max_secs`.
                        gs.lookback_secs = secs;
                    }
                }
                Effect::None
            }
            Update::GraphSwitchFocus => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.focus = match gs.focus {
                        GraphFocus::Chart => GraphFocus::Details,
                        GraphFocus::Details => GraphFocus::Chart,
                    };
                    // When returning to Chart (compact details), the
                    // cursor can only walk series rows.  Clamp it
                    // back into that range; preserving the relative
                    // series position is friendlier than resetting.
                    if gs.focus == GraphFocus::Chart {
                        let series_count = gs.series_keys().len();
                        gs.details_selected =
                            gs.details_selected.min(series_count.saturating_sub(1));
                    }
                }
                Effect::None
            }
            Update::GraphSelectUp => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.details_selected = gs.details_selected.saturating_sub(1);
                }
                Effect::None
            }
            Update::GraphSelectDown => {
                // The cursor walks the series list always, plus the
                // key-candidate list when Details is expanded.
                // Borrow self.agg first so the &mut gs.view borrow
                // below doesn't conflict.
                let locked_stack = if let ViewMode::Graph(gs) = &self.view {
                    gs.locked_stack.clone()
                } else {
                    return Effect::None;
                };
                let focus = if let ViewMode::Graph(gs) = &self.view {
                    gs.focus
                } else {
                    return Effect::None;
                };
                let n_keys = match focus {
                    GraphFocus::Chart => 0,
                    GraphFocus::Details => {
                        candidate_split_keys_for(&self.agg, &locked_stack).len()
                    }
                };
                if let ViewMode::Graph(gs) = &mut self.view {
                    let total = gs.series_keys().len() + n_keys;
                    if total == 0 {
                        return Effect::None;
                    }
                    gs.details_selected =
                        (gs.details_selected + 1).min(total - 1);
                }
                Effect::None
            }
            Update::GraphToggleSplit => {
                // Space dispatches differently depending on which
                // section the cursor is in: series-row → toggle
                // visibility, key-row → toggle split (which forces
                // a rehydrate).  Key rows only exist in Details
                // focus; Chart focus always operates on series.
                let locked_stack = if let ViewMode::Graph(gs) = &self.view {
                    gs.locked_stack.clone()
                } else {
                    return Effect::None;
                };
                let focus = if let ViewMode::Graph(gs) = &self.view {
                    gs.focus
                } else {
                    return Effect::None;
                };
                let candidates = match focus {
                    GraphFocus::Chart => Vec::new(),
                    GraphFocus::Details => {
                        candidate_split_keys_for(&self.agg, &locked_stack)
                    }
                };

                enum Target {
                    Series(Vec<(String, String)>),
                    Key(String),
                }
                let target = if let ViewMode::Graph(gs) = &self.view {
                    let series = gs.series_keys();
                    let series_count = series.len();
                    if gs.details_selected < series_count {
                        Some(Target::Series(series[gs.details_selected].clone()))
                    } else {
                        let key_idx = gs.details_selected - series_count;
                        candidates.get(key_idx).cloned().map(Target::Key)
                    }
                } else {
                    None
                };

                match target {
                    Some(Target::Series(key)) => {
                        if let ViewMode::Graph(gs) = &mut self.view {
                            if !gs.hidden_series.remove(&key) {
                                gs.hidden_series.insert(key);
                            }
                        }
                    }
                    Some(Target::Key(k)) => {
                        if let ViewMode::Graph(gs) = &mut self.view {
                            if !gs.split_keys.remove(&k) {
                                gs.split_keys.insert(k.clone());
                            }
                            // Series identities change when the
                            // split set changes — old visibility
                            // marks no longer correspond to
                            // anything meaningful.
                            gs.hidden_series.clear();
                            // Sort might have been on this split's
                            // column; if the split is no longer in
                            // the active set, fall back to `n`.
                            if let SortColumn::SplitKey(sk) = &gs.sort_column {
                                if !gs.split_keys.contains(sk) {
                                    gs.sort_column = SortColumn::Count;
                                }
                            }
                        }
                        if let ViewMode::Graph(gs) = &mut self.view {
                            gs.rehydrate(&self.agg);
                            // Keep the cursor pinned on the key the
                            // user just toggled: the series count
                            // typically grows on a key toggle, and
                            // without this fix-up the cursor would
                            // slide off the key onto a freshly-
                            // appeared series row.
                            let new_series_count = gs.series_keys().len();
                            if let Some(pos) = candidates.iter().position(|c| c == &k) {
                                gs.details_selected = new_series_count + pos;
                            }
                        }
                    }
                    None => {}
                }
                Effect::None
            }
            Update::GraphSortColumnLeft => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    let cols = gs.series_table_columns();
                    let idx = cols.iter().position(|c| c == &gs.sort_column);
                    let next = match idx {
                        Some(i) if i > 0 => cols[i - 1].clone(),
                        _ => cols.last().cloned().unwrap_or(SortColumn::Count),
                    };
                    gs.sort_column = next;
                }
                Effect::None
            }
            Update::GraphSortColumnRight => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    let cols = gs.series_table_columns();
                    let idx = cols.iter().position(|c| c == &gs.sort_column);
                    let next = match idx {
                        Some(i) if i + 1 < cols.len() => cols[i + 1].clone(),
                        _ => cols.first().cloned().unwrap_or(SortColumn::Count),
                    };
                    gs.sort_column = next;
                }
                Effect::None
            }
            Update::Quit => Effect::Quit,
        }
    }

    /// Recompute the visible (post-expansion) row list.
    pub fn visible_rows(&self) -> Vec<VisibleRow> {
        let rows = self.agg.rows();
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
        candidate_split_keys_for(&self.agg, &row.key.stack)
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
