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
    GraphFocus, GraphState, SortColumn, ViewMode, parse_agg_input, parse_lookback_input,
};
use super::update::{Effect, Update};

// ── Modal-input slot helpers ────────────────────────────────────
//
// Every text-input modal — chance, graph agg, graph window, graph
// lookback — owns an `Option<String>` buffer that follows the same
// Begin/Backspace/Cancel/Commit lifecycle.  These helpers collapse
// the boilerplate so each reducer arm carries only the parser/
// validator that's actually unique to its modal.

/// Open a modal: set the slot to `Some("")`.
fn open_modal(slot: &mut Option<String>) {
    *slot = Some(String::new());
}

/// Close a modal without consuming the buffer.  Used for both
/// Cancel and any "give up" path.
fn close_modal(slot: &mut Option<String>) {
    *slot = None;
}

/// Pop one character off the modal buffer if it's open.
fn backspace_modal(slot: &mut Option<String>) {
    if let Some(buf) = slot.as_mut() {
        buf.pop();
    }
}

/// Resolve the span id at the trace-detail cursor.  Returns
/// `None` if the cursor is parked on an event row, or if the
/// trace tree is empty (e.g. the root has been evicted).
/// Split out so the Expand / Collapse arms can borrow the model
/// immutably for `visible_trace_rows`, then re-borrow mutably to
/// flip the collapsed set.
fn trace_selected_span_id(model: &Model) -> Option<u64> {
    let ViewMode::TraceDetail(td) = &model.view else {
        return None;
    };
    let rows = super::explore::visible_trace_rows(model, td);
    if rows.is_empty() {
        return None;
    }
    let idx = td.selected_idx.min(rows.len() - 1);
    match rows[idx] {
        super::explore::TraceRow::Span { id, .. } => Some(id),
        super::explore::TraceRow::Event { .. } => None,
    }
}

/// Append `c` to `buf` if it's a digit, or if it's `.` and `buf`
/// doesn't already have one.  Returns whether the char was kept.
/// Shared by the chance / window / lookback char filters.
fn push_digit_or_decimal(buf: &mut String, c: char) -> bool {
    if c.is_ascii_digit() || (c == '.' && !buf.contains('.')) {
        buf.push(c);
        true
    } else {
        false
    }
}

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
                    self.bucket_start = Some(start + Duration::from_millis(500 * advances as u64));
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

/// Open-state of the version-mismatch confirm modal.  Stored on
/// the model as `Option<ConfirmVersionSwitch>`; when `Some`, the
/// modal is rendered and the keyboard loop routes `y`/`n`/`Esc`
/// into it.  `server_version` is snapshotted at open time so the
/// modal text and the emitted effect can't be torn by a
/// concurrent `ServerInfo` push.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmVersionSwitch {
    pub server_version: String,
    /// Lifecycle of the modal — starts as `Confirming`; flips to
    /// `Running` while the installer subprocess runs; ends as
    /// `Failed` if the installer non-zero-exits (or as a process
    /// `exit()` if it succeeds — the modal never reaches a "Done"
    /// state, since success means the binary's been replaced and
    /// the user wants the new one).
    #[serde(default)]
    pub status: ConfirmStatus,
}

/// Stage of the version-switch modal.  See [`ConfirmVersionSwitch`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ConfirmStatus {
    /// Initial state — showing the y/n prompt.
    #[default]
    Confirming,
    /// `y` was pressed; the installer is running in the background.
    /// All keystrokes are dropped while in this state — there's no
    /// safe mid-install cancel that wouldn't leave a half-installed
    /// binary on disk.
    Running,
    /// Installer exited non-zero or couldn't be launched.  The
    /// captured stdout+stderr is shown in the modal; `n`/`Esc`
    /// dismisses.
    Failed(String),
    /// Installer succeeded.  The new binary is on disk at
    /// ~/.local/bin/tracing-console but this process is still the
    /// old one — prompt the user to restart so the upgrade takes
    /// effect.  `y` execs the new binary with the same argv;
    /// `n`/`Esc` keeps running the stale process.
    Restart,
}

/// Workspace-pinned crate version this binary was built from
/// (`CARGO_PKG_VERSION`).  Compared against the server's
/// `ServerInfo` to decide whether the version-mismatch UI shows.
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

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
    /// Stacks the user has expanded in the tree.  Keyed on stack
    /// alone (not `BucketKey`) so multiple split-variants of the
    /// same stack expand together — and so that expanding a parent
    /// row reveals every child stack-extension regardless of which
    /// split values those children resolve to (issue: split key on
    /// a sub-span used to leave the child rows hidden because the
    /// child's `(stack, splits)` didn't match the parent's).
    pub expanded: HashSet<Vec<String>>,
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
    /// Server's advertised `tracing-console-host` crate version, as
    /// of the most recent `ServerInfo` handshake.  `None` until
    /// `StartStream` has produced its first message.  Surfaced in the
    /// header so a client/server version mismatch is visible at a
    /// glance — both binaries are workspace-pinned to the same value.
    #[serde(default)]
    pub server_version: Option<String>,
    /// When `Some`, the y/n version-switch confirm modal is open
    /// (the user pressed `v` while a server/client version mismatch
    /// was visible).  Carries the server version so the modal
    /// renders the right number and so committing emits an
    /// installer effect pinned to that version.
    #[serde(default)]
    pub confirm_version_switch: Option<ConfirmVersionSwitch>,
    /// When `Some`, the "press q again to quit" modal is up; the
    /// inner `Instant` is the auto-dismiss deadline.  A second `q`
    /// before the deadline returns `Effect::Quit`; `Esc` clears the
    /// modal; the runtime's ticker calls
    /// [`Model::expire_quit_confirm_if_due`] every render so the
    /// modal vanishes on its own after 2 s.
    ///
    /// Skipped from serde because `Instant` isn't serialisable and
    /// the prompt is purely transient UI state.
    #[serde(skip)]
    pub quit_confirm_deadline: Option<Instant>,
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
            server_version: None,
            confirm_version_switch: None,
            quit_confirm_deadline: None,
            chance_input: None,
            view: ViewMode::Table,
            rate: RateTracker::default(),
        }
    }

    pub fn split_keys(&self) -> &BTreeSet<String> {
        self.agg.split_keys()
    }

    /// Drop the quit-confirm prompt if its 2s deadline has passed.
    /// Called by the runtime on every render tick so an idle user
    /// who pressed `q` once doesn't see the prompt forever.
    pub fn expire_quit_confirm_if_due(&mut self) {
        if let Some(deadline) = self.quit_confirm_deadline
            && Instant::now() >= deadline
        {
            self.quit_confirm_deadline = None;
        }
    }

    pub fn apply(&mut self, update: Update) -> Effect {
        match update {
            Update::SpanReceived(span) => {
                self.rate.record(Instant::now());
                // The aggregator may absorb more than just `span` —
                // a parent arrival drains any pending children that
                // were parked waiting on it.  We want the graph
                // store to see every newly-committed span (so a
                // graph locked on a sub-span keeps updating when
                // its parent shows up after it), so iterate the
                // full inserted-ids list, not just the original
                // span.id.
                let inserted_ids = self.agg.absorb(span);
                if let ViewMode::Graph(gs) = &mut self.view {
                    for id in inserted_ids {
                        if matches!(
                            self.agg.resolved_stack(id),
                            Some(stack) if stack == gs.locked_stack.as_slice()
                        ) && let Some(span_ref) = self.agg.span_by_id(id)
                        {
                            // Cloning here is per-matched-span only —
                            // a graph locked on a leaf stack sees a
                            // handful of spans per second, not the
                            // whole firehose.
                            let span_clone = span_ref.clone();
                            gs.record_span(&self.agg, &span_clone);
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
                    if let Some(r) = rows.get(self.selected)
                        && r.has_children
                    {
                        self.expanded.insert(r.key.stack.clone());
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
                    // Expand every stack-prefix descendant of the
                    // selected row.  Splits are irrelevant here:
                    // expanding a stack reveals every split-variant
                    // child below it.
                    let root_stack = r.key.stack.clone();
                    let all = self.agg.rows();
                    for (k, _) in &all {
                        if k.stack.starts_with(&root_stack) && k.stack.len() > root_stack.len() {
                            for len in root_stack.len()..k.stack.len() {
                                self.expanded.insert(k.stack[..len].to_vec());
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
                        let root_stack = r.key.stack.clone();
                        self.expanded.retain(|s| !s.starts_with(&root_stack));
                    } else if r.depth > 0 {
                        // Jump to parent and collapse it.
                        let parent_stack: Vec<String> =
                            r.key.stack[..r.key.stack.len() - 1].to_vec();
                        self.expanded.retain(|s| !s.starts_with(&parent_stack));
                        let new_rows = self.visible_rows();
                        if let Some((idx, _)) = new_rows
                            .iter()
                            .enumerate()
                            .find(|(_, row)| row.key.stack == parent_stack)
                        {
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
            Update::ServerInfoReceived(info) => {
                self.server_version = Some(info.version);
                Effect::None
            }
            Update::RequestCacheLevel(filter) => Effect::RequestSetLevel(filter),
            Update::CacheChanceReceived(pct) => {
                self.cache_chance = Some(pct);
                Effect::None
            }
            Update::BeginChanceInput => {
                open_modal(&mut self.chance_input);
                Effect::None
            }
            Update::ChanceInputChar(c) => {
                if let Some(buf) = self.chance_input.as_mut() {
                    push_digit_or_decimal(buf, c);
                }
                Effect::None
            }
            Update::ChanceInputBackspace => {
                backspace_modal(&mut self.chance_input);
                Effect::None
            }
            Update::ChanceInputCancel => {
                close_modal(&mut self.chance_input);
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
                    // From any non-table view, `g` returns to the stack table.
                    _ => ViewMode::Table,
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
                    gs.metric = gs.metric.next();
                    gs.rehydrate(&self.agg);
                }
                Effect::None
            }
            Update::BeginGraphAggInput => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    open_modal(&mut gs.agg_input);
                }
                Effect::None
            }
            Update::GraphAggInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view
                    && let Some(buf) = gs.agg_input.as_mut()
                {
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
                Effect::None
            }
            Update::GraphAggInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    backspace_modal(&mut gs.agg_input);
                }
                Effect::None
            }
            Update::GraphAggInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    close_modal(&mut gs.agg_input);
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
                    open_modal(&mut gs.window_input);
                }
                Effect::None
            }
            Update::GraphWindowInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view
                    && let Some(buf) = gs.window_input.as_mut()
                {
                    push_digit_or_decimal(buf, c);
                }
                Effect::None
            }
            Update::GraphWindowInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    backspace_modal(&mut gs.window_input);
                }
                Effect::None
            }
            Update::GraphWindowInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    close_modal(&mut gs.window_input);
                }
                Effect::None
            }
            Update::GraphWindowInputCommit => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    let Some(buf) = gs.window_input.take() else {
                        return Effect::None;
                    };
                    if let Ok(v) = buf.parse::<f64>()
                        && v.is_finite()
                        && v > 0.0
                    {
                        gs.window_secs = v;
                        gs.rehydrate(&self.agg);
                    }
                }
                Effect::None
            }
            Update::BeginGraphLookbackInput => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    open_modal(&mut gs.lookback_input);
                }
                Effect::None
            }
            Update::GraphLookbackInputChar(c) => {
                if let ViewMode::Graph(gs) = &mut self.view
                    && let Some(buf) = gs.lookback_input.as_mut()
                {
                    // Once a unit suffix has landed, reject further
                    // input so the user can't type "5sm".  Otherwise
                    // accept a digit / single `.` / single trailing
                    // `s`/`m` (the suffix needs a leading number).
                    let has_suffix = buf.ends_with('s') || buf.ends_with('m');
                    if !has_suffix
                        && !push_digit_or_decimal(buf, c)
                        && (c == 's' || c == 'm')
                        && !buf.is_empty()
                    {
                        buf.push(c);
                    }
                }
                Effect::None
            }
            Update::GraphLookbackInputBackspace => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    backspace_modal(&mut gs.lookback_input);
                }
                Effect::None
            }
            Update::GraphLookbackInputCancel => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    close_modal(&mut gs.lookback_input);
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
            Update::ToggleGraphTimeLabels => {
                if let ViewMode::Graph(gs) = &mut self.view {
                    gs.time_labels = gs.time_labels.next();
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
                    GraphFocus::Details => candidate_split_keys_for(&self.agg, &locked_stack).len(),
                };
                if let ViewMode::Graph(gs) = &mut self.view {
                    let total = gs.series_keys().len() + n_keys;
                    if total == 0 {
                        return Effect::None;
                    }
                    gs.details_selected = (gs.details_selected + 1).min(total - 1);
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
                    GraphFocus::Details => candidate_split_keys_for(&self.agg, &locked_stack),
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
                        if let ViewMode::Graph(gs) = &mut self.view
                            && !gs.hidden_series.remove(&key)
                        {
                            gs.hidden_series.insert(key);
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
                            if let SortColumn::SplitKey(sk) = &gs.sort_column
                                && !gs.split_keys.contains(sk)
                            {
                                gs.sort_column = SortColumn::Count;
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
            // ── Explore view ────────────────────────────────────
            Update::EnterExplore => {
                // Resolve the locked stack from whichever view we're
                // in, so `e` works consistently from anywhere:
                //   * Table — the cursor row's bucket.
                //   * Graph — the graph's locked stack.
                //   * TraceDetail — the embedded explore state, just
                //     pop back up (no fresh level capture).
                //   * Explore — no-op.
                let locked_stack = match &self.view {
                    ViewMode::Table => {
                        let Some(row) = self.selected_visible_row() else {
                            return Effect::None;
                        };
                        row.key.stack
                    }
                    ViewMode::Graph(gs) => gs.locked_stack.clone(),
                    ViewMode::TraceDetail(_) => {
                        if let ViewMode::TraceDetail(td) =
                            std::mem::replace(&mut self.view, ViewMode::Table)
                        {
                            self.view = ViewMode::Explore(td.explore);
                        }
                        return Effect::None;
                    }
                    ViewMode::Explore(_) => return Effect::None,
                };
                let restore = self.cache_level;
                let es = super::explore::ExploreState::new(locked_stack, restore);
                self.view = ViewMode::Explore(es);
                // Set the cache level to Off so the screen doesn't
                // churn while the user explores.  Restored on exit.
                Effect::RequestSetLevel(WireLevelFilter::Off)
            }
            Update::ExitExplore => {
                let es = match std::mem::replace(&mut self.view, ViewMode::Table) {
                    ViewMode::Explore(es) => Some(es),
                    ViewMode::TraceDetail(td) => Some(td.explore),
                    other => {
                        self.view = other;
                        None
                    }
                };
                if let Some(es) = es {
                    let restore = es.restore_level.unwrap_or(WireLevelFilter::Trace);
                    return Effect::RequestSetLevel(restore);
                }
                Effect::None
            }
            Update::ExploreSelectUp => {
                if let ViewMode::Explore(es) = &mut self.view {
                    es.selected = es.selected.saturating_sub(1);
                }
                Effect::None
            }
            Update::ExploreSelectDown => {
                let row_count = if let ViewMode::Explore(es) = &self.view {
                    super::explore::matching_spans(self, es).len()
                } else {
                    return Effect::None;
                };
                if let ViewMode::Explore(es) = &mut self.view
                    && row_count > 0
                    && es.selected + 1 < row_count
                {
                    es.selected += 1;
                }
                Effect::None
            }
            Update::ExploreSortLeft | Update::ExploreSortRight => {
                let delta_right = matches!(update, Update::ExploreSortRight);
                let fields = if let ViewMode::Explore(es) = &self.view {
                    let spans = super::explore::matching_spans(self, es);
                    super::explore::distinguishing_fields(&spans)
                } else {
                    return Effect::None;
                };
                if let ViewMode::Explore(es) = &mut self.view {
                    if delta_right {
                        super::explore::cycle_sort_right(es, &fields);
                    } else {
                        super::explore::cycle_sort_left(es, &fields);
                    }
                }
                Effect::None
            }
            Update::ExploreInvertSort => {
                if let ViewMode::Explore(es) = &mut self.view {
                    es.direction = es.direction.flip();
                }
                Effect::None
            }
            Update::BeginExploreSearch => {
                if let ViewMode::Explore(es) = &mut self.view {
                    open_modal(&mut es.search_input);
                    es.selected = 0;
                }
                Effect::None
            }
            Update::ExploreSearchChar(c) => {
                if let ViewMode::Explore(es) = &mut self.view
                    && let Some(buf) = es.search_input.as_mut()
                    && !c.is_control()
                {
                    buf.push(c);
                }
                Effect::None
            }
            Update::ExploreSearchBackspace => {
                if let ViewMode::Explore(es) = &mut self.view {
                    backspace_modal(&mut es.search_input);
                }
                Effect::None
            }
            Update::ExploreSearchCancel => {
                if let ViewMode::Explore(es) = &mut self.view {
                    close_modal(&mut es.search_input);
                    es.selected = 0;
                }
                Effect::None
            }
            Update::ExploreSearchCommit => {
                if let ViewMode::Explore(es) = &mut self.view
                    && let Some(buf) = es.search_input.take()
                {
                    es.query = buf;
                    es.selected = 0;
                }
                Effect::None
            }
            Update::ExploreOpenTrace => {
                let (span_id, es) = if let ViewMode::Explore(es) = &self.view {
                    let spans = super::explore::matching_spans(self, es);
                    if spans.is_empty() {
                        return Effect::None;
                    }
                    let idx = es.selected.min(spans.len() - 1);
                    (spans[idx].id, es.clone())
                } else {
                    return Effect::None;
                };
                let Some(root_id) = super::explore::find_root_id(self, span_id) else {
                    return Effect::None;
                };
                self.view = ViewMode::TraceDetail(super::explore::TraceDetailState {
                    root_id,
                    selected_idx: 0,
                    collapsed: std::collections::BTreeSet::new(),
                    explore: es,
                });
                Effect::None
            }
            Update::ExitTraceDetail => {
                if let ViewMode::TraceDetail(td) =
                    std::mem::replace(&mut self.view, ViewMode::Table)
                {
                    self.view = ViewMode::Explore(td.explore);
                }
                Effect::None
            }
            Update::TraceDetailSelectUp => {
                if let ViewMode::TraceDetail(td) = &mut self.view {
                    td.selected_idx = td.selected_idx.saturating_sub(1);
                }
                Effect::None
            }
            Update::TraceDetailSelectDown => {
                // Cap against the current visible-row count.  Without
                // this, holding ↓ past the bottom grows `selected_idx`
                // unboundedly — and then ↑ has to undo every overshoot
                // step before the cursor visibly moves, because the
                // render-time `.min(rows.len() - 1)` keeps it pinned
                // to the bottom row.
                let row_count = if let ViewMode::TraceDetail(td) = &self.view {
                    super::explore::visible_trace_rows(self, td).len()
                } else {
                    return Effect::None;
                };
                if let ViewMode::TraceDetail(td) = &mut self.view
                    && row_count > 0
                    && td.selected_idx + 1 < row_count
                {
                    td.selected_idx += 1;
                }
                Effect::None
            }
            Update::TraceDetailExpand => {
                if let Some(id) = trace_selected_span_id(self)
                    && let ViewMode::TraceDetail(td) = &mut self.view
                {
                    td.collapsed.remove(&id);
                }
                Effect::None
            }
            Update::TraceDetailCollapse => {
                if let Some(id) = trace_selected_span_id(self)
                    && let ViewMode::TraceDetail(td) = &mut self.view
                {
                    td.collapsed.insert(id);
                }
                Effect::None
            }
            Update::EnterTable => {
                let prior = std::mem::replace(&mut self.view, ViewMode::Table);
                match prior {
                    ViewMode::Explore(es) => {
                        let restore = es.restore_level.unwrap_or(WireLevelFilter::Trace);
                        Effect::RequestSetLevel(restore)
                    }
                    ViewMode::TraceDetail(td) => {
                        let restore = td.explore.restore_level.unwrap_or(WireLevelFilter::Trace);
                        Effect::RequestSetLevel(restore)
                    }
                    _ => Effect::None,
                }
            }
            Update::EnterGraph => {
                // Source: current view's locked stack.  Also
                // captures whether we need to restore the cache
                // level (only if leaving Explore / TraceDetail).
                let (locked, restore) = match &self.view {
                    ViewMode::Table => match self.selected_visible_row() {
                        Some(row) => (row.key.stack, None),
                        None => return Effect::None,
                    },
                    ViewMode::Graph(_) => return Effect::None,
                    ViewMode::Explore(es) => (
                        es.locked_stack.clone(),
                        Some(es.restore_level.unwrap_or(WireLevelFilter::Trace)),
                    ),
                    ViewMode::TraceDetail(td) => (
                        td.explore.locked_stack.clone(),
                        Some(td.explore.restore_level.unwrap_or(WireLevelFilter::Trace)),
                    ),
                };
                let mut gs = GraphState::new(locked);
                gs.rehydrate(&self.agg);
                self.view = ViewMode::Graph(gs);
                if let Some(level) = restore {
                    return Effect::RequestSetLevel(level);
                }
                Effect::None
            }
            Update::Quit => {
                // Two-step confirm.  First `q` arms the modal with a
                // 2s deadline; second `q` before that deadline
                // returns Effect::Quit.  After the deadline the
                // modal has been cleared by the runtime ticker and
                // the next `q` re-arms — never "click-through" the
                // confirm by accident.
                match self.quit_confirm_deadline {
                    Some(deadline) if Instant::now() < deadline => {
                        self.quit_confirm_deadline = None;
                        Effect::Quit
                    }
                    _ => {
                        self.quit_confirm_deadline = Some(Instant::now() + Duration::from_secs(2));
                        Effect::None
                    }
                }
            }
            Update::QuitConfirmDismiss => {
                self.quit_confirm_deadline = None;
                Effect::None
            }
            Update::BeginConfirmVersionSwitch => {
                // Only meaningful when the server and client crate
                // versions disagree.  Silently ignored otherwise so
                // an unbound `v` keystroke can't accidentally pop a
                // modal with nothing to confirm.  If a modal is
                // already open (Running or Failed), don't reset its
                // status — `v` is a no-op in those states.
                if self.confirm_version_switch.is_none()
                    && let Some(server_version) = self.server_version.clone()
                    && server_version != CLIENT_VERSION
                {
                    self.confirm_version_switch = Some(ConfirmVersionSwitch {
                        server_version,
                        status: ConfirmStatus::Confirming,
                    });
                }
                Effect::None
            }
            Update::ConfirmVersionSwitchYes => {
                // `y` means different things at different stages of
                // the modal: in `Confirming` it commits the install;
                // in `Restart` it execs the new binary; in `Running`
                // and `Failed` it's a no-op (running can't be
                // cancelled mid-stream, failed retries dismiss-then-
                // -reopen so the user sees the prior error first).
                let Some(c) = self.confirm_version_switch.as_mut() else {
                    return Effect::None;
                };
                match &c.status {
                    ConfirmStatus::Confirming => {
                        let version = c.server_version.clone();
                        c.status = ConfirmStatus::Running;
                        Effect::RunUpdateInstaller {
                            version: Some(version),
                        }
                    }
                    ConfirmStatus::Restart => Effect::Restart,
                    ConfirmStatus::Running | ConfirmStatus::Failed(_) => Effect::None,
                }
            }
            Update::ConfirmVersionSwitchNo => {
                // Dismiss only when there isn't a live subprocess —
                // dropping the modal mid-install would orphan an
                // unfinished install on disk with nothing on screen
                // to tell the user it's still going.
                if let Some(c) = self.confirm_version_switch.as_ref()
                    && !matches!(c.status, ConfirmStatus::Running)
                {
                    self.confirm_version_switch = None;
                }
                Effect::None
            }
            Update::InstallerSucceeded => {
                // New binary is on disk at ~/.local/bin/tracing-console,
                // but the running process is still the old one.  Park
                // in the Restart prompt so the user explicitly opts
                // into the exec — accidentally execing while typing
                // is more surprising than asking once.
                if let Some(c) = self.confirm_version_switch.as_mut() {
                    c.status = ConfirmStatus::Restart;
                }
                Effect::None
            }
            Update::InstallerFailed(output) => {
                if let Some(c) = self.confirm_version_switch.as_mut() {
                    c.status = ConfirmStatus::Failed(output);
                }
                Effect::None
            }
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
            // Visibility: every proper-stack prefix of this row's
            // stack must be in `expanded` (which is keyed on stack
            // only).  Splits are intentionally ignored — a split key
            // that appears on a sub-span should still let the child
            // render when its parent bucket has different splits.
            let mut visible = true;
            for k in 1..key.stack.len() {
                let prefix = &key.stack[..k];
                if !self.expanded.contains(prefix) {
                    visible = false;
                    break;
                }
            }
            if !visible {
                continue;
            }
            // has_children = some later row extends this row's stack
            // by ≥1 level.  Rows are sorted `(stack, splits)`, so
            // children share a stack-prefix with the parent and sit
            // contiguously after it — scanning until the prefix
            // breaks is enough.  Splits intentionally don't bound
            // the scan: a split-only child still counts as a child.
            let mut has_children = false;
            for (next_key, _) in rows.iter().skip(i + 1) {
                if !next_key.stack.starts_with(&key.stack) {
                    break;
                }
                if next_key.stack.len() > key.stack.len() {
                    has_children = true;
                    break;
                }
            }
            let is_expanded = self.expanded.contains(&key.stack);
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
