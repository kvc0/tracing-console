//! Graph-view types and the per-bucket time-series store.  Lives
//! in its own submodule so the table-only path doesn't have to
//! page in any chart machinery.

use std::collections::{BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use tracing_console_host::WireSpan;

use crate::aggregate::Aggregator;

// ── Graph view ───────────────────────────────────────────────────────

/// Top-level view dispatch.  Table is the default two-pane stacks +
/// details layout.  Graph replaces the stacks table with a line
/// chart of the locked bucket's metric over time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViewMode {
    Table,
    Graph(GraphState),
    /// `e` from the stacks table — lists each individual span
    /// instance whose stack matches the highlighted bucket.
    Explore(super::explore::ExploreState),
    /// Enter on an explore row — the full trace tree rooted at
    /// the selected span's root.
    TraceDetail(super::explore::TraceDetailState),
}

/// Which sample a span contributes to the graph.  `Total` is the
/// span's wall-clock duration; `SelfTime` subtracts the totals of
/// direct children that the aggregator currently knows about (the
/// same self-time the stacks table shows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    Total,
    SelfTime,
}

/// Aggregation applied per (series, time-bin) before rendering.
/// `Percentile(p)` carries `p` in `(0.0, 100.0)`; the modal commits
/// `0` and `100` as `Min` / `Max` so the chart path doesn't need to
/// special-case them.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AggMode {
    Min,
    Max,
    Avg,
    Percentile(f64),
}

impl PartialEq for AggMode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AggMode::Min, AggMode::Min)
            | (AggMode::Max, AggMode::Max)
            | (AggMode::Avg, AggMode::Avg) => true,
            (AggMode::Percentile(a), AggMode::Percentile(b)) => a == b,
            _ => false,
        }
    }
}

/// How the chart's X-axis tick labels are rendered.  Toggled via
/// `u` (cycles `Delta → Unix → Local → Delta`).  Affects only
/// the chart pane labels; the lookback math is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeLabels {
    /// Seconds before "now" — `-30s`, `-1m`, etc.
    Delta,
    /// UTC wall clock — `14:37:58Z`.
    Unix,
    /// Local wall clock — `14:37:58` (no suffix).
    Local,
}

impl TimeLabels {
    /// Cycle to the next mode in the `Delta → Unix → Local → Delta`
    /// order driven by the `u` key.
    pub fn next(self) -> Self {
        match self {
            TimeLabels::Delta => TimeLabels::Unix,
            TimeLabels::Unix => TimeLabels::Local,
            TimeLabels::Local => TimeLabels::Delta,
        }
    }
}

impl Default for TimeLabels {
    fn default() -> Self {
        TimeLabels::Delta
    }
}

/// Which pane has focus inside the graph view.  Mirrors `Focus` but
/// scoped to graph-mode keys (`Tab` swaps; in Details the user
/// navigates the split-key candidate list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphFocus {
    Chart,
    Details,
}

/// Which column the series table is currently sorted by.  Left /
/// Right (in Details focus) cycles through the list of columns
/// returned by [`GraphState::series_table_columns`].  Direction is
/// implicit per column type — alphabetical ascending for string
/// columns (split values), descending for the numeric stat
/// columns where "biggest first" is usually the interesting view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortColumn {
    SplitKey(String),
    Count,
    Min,
    Avg,
    Max,
    Last,
}

/// Per-(series, bin) accumulator.  `samples` is populated only as
/// large as the bin reservoir cap so percentile aggregation has
/// something to sort; min/max/avg are derived from the scalar
/// counters and don't need samples.
#[derive(Debug, Clone)]
struct Bin {
    count: u64,
    sum_ns: u128,
    min_ns: u64,
    max_ns: u64,
    samples: Vec<u64>,
}

impl Bin {
    fn record(&mut self, value: u64) {
        if self.count == 0 {
            self.min_ns = value;
            self.max_ns = value;
        } else {
            self.min_ns = self.min_ns.min(value);
            self.max_ns = self.max_ns.max(value);
        }
        self.count += 1;
        self.sum_ns += value as u128;
        if self.samples.len() < GraphSeriesStore::BIN_SAMPLE_CAP {
            self.samples.push(value);
        }
    }

    fn aggregate(&self, mode: AggMode) -> u64 {
        if self.count == 0 {
            return 0;
        }
        match mode {
            AggMode::Min => self.min_ns,
            AggMode::Max => self.max_ns,
            AggMode::Avg => (self.sum_ns / self.count as u128) as u64,
            AggMode::Percentile(p) => {
                if self.samples.is_empty() {
                    return 0;
                }
                let mut sorted = self.samples.clone();
                sorted.sort_unstable();
                let n = sorted.len();
                let idx = ((p / 100.0) * (n.saturating_sub(1) as f64)).round() as usize;
                sorted[idx.min(n - 1)]
            }
        }
    }
}

/// Rolling per-series bin buffer for one chart series.  Bins are
/// stored densely with `None` slots for time windows that received
/// no sample; the absolute `base_idx` lets the chart code translate
/// the deque back to wall-clock offsets without scanning.
#[derive(Debug, Clone, Default)]
pub(super) struct Series {
    bins: VecDeque<Option<Bin>>,
    base_idx: u64,
}

/// Per-locked-bucket time-series storage feeding the graph chart.
/// Fed incrementally from `Model::apply(SpanReceived)` so the
/// visible time horizon is bounded by wall-clock minutes, not by
/// the aggregator's span-count-bounded ring.
#[derive(Debug, Clone, Default)]
pub struct GraphSeriesStore {
    /// Absolute nanoseconds (host clock, same epoch as
    /// `WireSpan.opened_at_ns`) of bin index 0.  Initialised on the
    /// first recorded sample so the first bin is centered on the
    /// arrival time of the first matching span.
    origin_ns: u64,
    window_ns: u64,
    /// Most-recently advanced-into bin (absolute index).  Tracks the
    /// "now" cursor for axis labels even when series are sparse.
    latest_bin_idx: u64,
    /// `pub(super)` so the sibling `tests` submodule can inspect
    /// the per-series bin layout directly; otherwise this field is
    /// effectively private to the `model` module.
    pub(super) series: HashMap<Vec<(String, String)>, Series>,
}

impl GraphSeriesStore {
    /// Max bins retained per series.  600 ≈ 10 minutes at 1 s
    /// windows; older bins drop out the front when the cursor
    /// advances past the cap.
    pub const BIN_CAP: usize = 600;

    /// Per-bin sample reservoir cap.  Hit only when computing
    /// percentile aggregation; otherwise samples are unused.
    pub const BIN_SAMPLE_CAP: usize = 4096;

    fn new(window_secs: f64) -> Self {
        Self {
            origin_ns: 0,
            window_ns: window_ns_from_secs(window_secs),
            latest_bin_idx: 0,
            series: HashMap::new(),
        }
    }

    fn window_secs(&self) -> f64 {
        self.window_ns as f64 / 1.0e9
    }

    fn record(&mut self, series_key: Vec<(String, String)>, t_ns: u64, value: u64) {
        if self.origin_ns == 0 {
            // Center the origin one half-window before the first
            // sample so the first bin contains it.
            self.origin_ns = t_ns.saturating_sub(self.window_ns / 2);
        }
        if t_ns < self.origin_ns {
            // Shouldn't happen — spans arrive monotonically — but
            // guard against a clock blip rather than panic.
            return;
        }
        let bin_idx = (t_ns - self.origin_ns) / self.window_ns;
        self.latest_bin_idx = self.latest_bin_idx.max(bin_idx);

        let series = self.series.entry(series_key).or_default();
        if series.bins.is_empty() {
            series.base_idx = bin_idx;
        }
        // Forward-fill empty slots between the deque's tail and the
        // new bin, then evict from the front to honour the cap.
        let needed_back = bin_idx + 1 - series.base_idx;
        while series.bins.len() < needed_back as usize {
            series.bins.push_back(None);
        }
        while series.bins.len() > Self::BIN_CAP {
            series.bins.pop_front();
            series.base_idx += 1;
        }
        let slot = (bin_idx - series.base_idx) as usize;
        let bin = series.bins[slot].get_or_insert_with(|| Bin {
            count: 0,
            sum_ns: 0,
            min_ns: 0,
            max_ns: 0,
            samples: Vec::new(),
        });
        bin.record(value);
    }

    /// Discard all bins; called when an inputs change that
    /// invalidates the bin layout (window resize, splits or metric
    /// toggle, bucket relock).
    pub fn wipe(&mut self) {
        self.series.clear();
        self.origin_ns = 0;
        self.latest_bin_idx = 0;
    }

    /// Reseed the window size; equivalent to a wipe followed by a
    /// new origin on the next sample.
    pub fn reset_window(&mut self, window_secs: f64) {
        self.window_ns = window_ns_from_secs(window_secs);
        self.wipe();
    }

    /// Project the stored bins into ratatui-friendly per-series
    /// `(x = seconds-relative-to-now, y = agg_ns_as_f64)` lists.
    /// `x_max_secs` is the visible window, set by the caller; the
    /// chart's x axis runs `[-x_max_secs, 0]`.  Older bins outside
    /// that window are clipped.
    pub fn project(&self, agg: AggMode, x_max_secs: f64) -> Vec<SeriesProjection> {
        if self.series.is_empty() || self.window_ns == 0 {
            return Vec::new();
        }
        let window_secs = self.window_secs();
        let visible_bins = ((x_max_secs / window_secs).ceil() as u64).max(1);
        let oldest = self.latest_bin_idx.saturating_sub(visible_bins);
        let mut out: Vec<SeriesProjection> = self
            .series
            .iter()
            .map(|(key, series)| {
                let mut pts: Vec<(f64, f64)> = Vec::with_capacity(series.bins.len());
                for (offset, slot) in series.bins.iter().enumerate() {
                    let abs_idx = series.base_idx + offset as u64;
                    if abs_idx < oldest {
                        continue;
                    }
                    let bin = match slot {
                        Some(b) => b,
                        None => continue,
                    };
                    // x = signed seconds vs. latest bin's right edge.
                    let bins_back = self.latest_bin_idx.saturating_sub(abs_idx) as f64;
                    let x = -(bins_back * window_secs);
                    let y = bin.aggregate(agg) as f64;
                    pts.push((x, y));
                }
                pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                SeriesProjection {
                    key: key.clone(),
                    points: pts,
                }
            })
            .collect();
        // Stable order — sort by series key — so colours stay
        // consistent across renders.
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }

    /// Per-series rollup used by the details-pane columnar legend.
    /// `count`/`min_ns`/`max_ns`/`avg_ns` are sample-statistics over
    /// every bin in the retained window — they're independent of the
    /// current [`AggMode`] so the user can read the actual data
    /// extremes without first changing modes.  `last_ns` *is* under
    /// the active agg, because it lines up with the right edge of
    /// the chart line.
    pub fn series_summary(&self, agg: AggMode) -> Vec<SeriesSummary> {
        let mut out: Vec<SeriesSummary> = self
            .series
            .iter()
            .map(|(key, series)| {
                let mut count: u64 = 0;
                let mut sum: u128 = 0;
                let mut min_ns: u64 = u64::MAX;
                let mut max_ns: u64 = 0;
                for slot in &series.bins {
                    if let Some(bin) = slot {
                        count += bin.count;
                        sum += bin.sum_ns;
                        min_ns = min_ns.min(bin.min_ns);
                        max_ns = max_ns.max(bin.max_ns);
                    }
                }
                let last_ns = series
                    .bins
                    .iter()
                    .rev()
                    .flatten()
                    .next()
                    .map(|b| b.aggregate(agg))
                    .unwrap_or(0);
                let avg_ns = if count == 0 { 0 } else { (sum / count as u128) as u64 };
                let min_ns = if count == 0 { 0 } else { min_ns };
                SeriesSummary {
                    key: key.clone(),
                    count,
                    min_ns,
                    max_ns,
                    avg_ns,
                    last_ns,
                }
            })
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }
}

/// One series' rollup over the retained time window — used by the
/// details-pane columnar legend.
#[derive(Debug, Clone)]
pub struct SeriesSummary {
    pub key: Vec<(String, String)>,
    pub count: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub avg_ns: u64,
    pub last_ns: u64,
}

fn window_ns_from_secs(secs: f64) -> u64 {
    if !secs.is_finite() || secs <= 0.0 {
        // Fallback: 1-second window.  `apply(Update::GraphWindowInputCommit)`
        // already rejects garbage, so this is just defence-in-depth.
        1_000_000_000
    } else {
        (secs * 1.0e9).round() as u64
    }
}

/// Parse the lookback-input modal buffer.  Grammar:
///
/// * `<number>`       → seconds (the default unit)
/// * `<number>s`      → seconds
/// * `<number>m`      → minutes
///
/// `<number>` must be a finite positive `f64`.  Returns `None` for
/// any other input (empty, non-positive, unknown suffix, garbage);
/// the caller treats `None` as "leave the existing lookback
/// unchanged".  Result is always in **seconds** so callers can
/// assign directly to `lookback_secs`.
pub fn parse_lookback_input(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, multiplier) = if let Some(rest) = s.strip_suffix('m') {
        (rest, 60.0)
    } else if let Some(rest) = s.strip_suffix('s') {
        (rest, 1.0)
    } else {
        (s, 1.0)
    };
    let v: f64 = num.parse().ok()?;
    if !v.is_finite() || v <= 0.0 {
        return None;
    }
    Some(v * multiplier)
}

/// Parse the aggregation-input modal buffer.  Grammar:
///
/// * `a` / `avg`                  → `Avg`
/// * `min`                        → `Min`
/// * `max`                        → `Max`
/// * `p0`                         → `Min`
/// * `p100` (or any `pN` with N ≥ 100) → `Max`
/// * `pX[.XX]`, 0 < X < 100       → `Percentile(X)`
///
/// Returns `None` for any other input — the caller treats `None`
/// as "leave the existing aggregation unchanged".
pub fn parse_agg_input(s: &str) -> Option<AggMode> {
    let s = s.trim().to_ascii_lowercase();
    match s.as_str() {
        "a" | "avg" => return Some(AggMode::Avg),
        "min" => return Some(AggMode::Min),
        "max" => return Some(AggMode::Max),
        _ => {}
    }
    let rest = s.strip_prefix('p')?;
    if rest.is_empty() {
        return None;
    }
    let v: f64 = rest.parse().ok()?;
    if !v.is_finite() || v < 0.0 {
        return None;
    }
    if v == 0.0 {
        Some(AggMode::Min)
    } else if v >= 100.0 {
        Some(AggMode::Max)
    } else {
        Some(AggMode::Percentile(v))
    }
}

/// One series' worth of `(x, y)` points ready to hand to ratatui.
#[derive(Debug, Clone)]
pub struct SeriesProjection {
    pub key: Vec<(String, String)>,
    pub points: Vec<(f64, f64)>,
}

/// Graph-mode state.  Allocated when the user presses `g`, dropped
/// when they press it again to return to the table view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphState {
    /// Locked at entry time so the chart doesn't follow the table
    /// cursor's selection.
    pub locked_stack: Vec<String>,
    pub aggregation: AggMode,
    pub metric: Metric,
    pub window_secs: f64,
    /// Width of the chart's visible X-axis, in seconds.  Independent
    /// of `window_secs` (which sets the bin width); lookback only
    /// affects how far back in time the chart shows.  Edited via the
    /// `l` modal — see [`parse_lookback_input`].
    pub lookback_secs: f64,
    pub split_keys: BTreeSet<String>,
    /// Series the user has unchecked in the details pane.  Anything
    /// in this set is omitted from the chart but still recorded
    /// into the store (so re-checking is instant).  Cleared
    /// whenever `split_keys` changes because the series identities
    /// themselves change shape.
    pub hidden_series: BTreeSet<Vec<(String, String)>>,
    /// Cursor index walking the *combined* details list:
    /// `[series rows..., metadata-key rows...]`.  Used by both
    /// `GraphSelectUp`/`Down` (clamped against the combined length)
    /// and `GraphToggleSplit` (which dispatches based on whether the
    /// cursor lands in the series or keys section).
    pub details_selected: usize,
    pub focus: GraphFocus,
    /// Leading sort column for the series table.  Secondary sort
    /// remains alphabetical by series key, so ties stay stable.
    pub sort_column: SortColumn,
    /// `Some(buffer)` while the user is typing an aggregation
    /// expression into the "agg:" input box.  Buffer accepts
    /// lowercase letters + digits + `.`; the modal commit parses
    /// it as one of `a`, `avg`, `min`, `max`, or `pX[.XX]` per
    /// [`parse_agg_input`].
    pub agg_input: Option<String>,
    pub window_input: Option<String>,
    pub lookback_input: Option<String>,
    /// X-axis label format; cycled by `u`.
    #[serde(default)]
    pub time_labels: TimeLabels,
    /// Per-(series, bin) accumulators — see [`GraphSeriesStore`].
    /// Skipped from serde because it's transient: round-tripping
    /// the Model rebuilds an empty store and re-fills as spans
    /// arrive.
    #[serde(skip)]
    pub store: GraphSeriesStore,
}

impl GraphState {
    pub fn new(locked_stack: Vec<String>) -> Self {
        let window_secs = 1.0;
        Self {
            locked_stack,
            aggregation: AggMode::Avg,
            metric: Metric::Total,
            window_secs,
            // Default to one minute of history shown on the chart.
            // Independent of the bin window — the user can pick a
            // long lookback with a coarse window, or vice versa.
            lookback_secs: 60.0,
            split_keys: BTreeSet::new(),
            hidden_series: BTreeSet::new(),
            details_selected: 0,
            focus: GraphFocus::Chart,
            sort_column: SortColumn::Count,
            agg_input: None,
            window_input: None,
            lookback_input: None,
            time_labels: TimeLabels::default(),
            store: GraphSeriesStore::new(window_secs),
        }
    }

    /// Columns shown in the series table, left-to-right.  Used by
    /// the renderer for the header row and by Left/Right cursor
    /// keys to cycle the active sort column.
    pub fn series_table_columns(&self) -> Vec<SortColumn> {
        let mut cols: Vec<SortColumn> = self
            .split_keys
            .iter()
            .map(|k| SortColumn::SplitKey(k.clone()))
            .collect();
        cols.push(SortColumn::Count);
        cols.push(SortColumn::Min);
        cols.push(SortColumn::Avg);
        cols.push(SortColumn::Max);
        cols.push(SortColumn::Last);
        cols
    }

    /// Alphabetical order of series keys.  This is the canonical
    /// order the chart uses for colour assignment — display
    /// reorderings (e.g. sort by max-duration) do *not* shuffle
    /// colours, so a series remains visually identifiable as the
    /// user re-sorts the table.
    pub fn alpha_series_keys(&self) -> Vec<Vec<(String, String)>> {
        let mut keys: Vec<Vec<(String, String)>> =
            self.store.series.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Stable colour-slot index for `key` — deterministic hash of
    /// the series identity (locked stack + split values).  Identical
    /// series names produce identical colours across runs, across
    /// re-entries to graph mode, and across whatever order the user
    /// happens to enable splits in — the alphabetical-rank scheme
    /// this replaces lost stability the moment a new split key
    /// shifted everyone else's rank by one.
    pub fn color_index_of(&self, key: &[(String, String)]) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        // Anchor colour to the locked stack as well, so the same
        // (no-splits) series colour persists when the user toggles
        // a split on then off.
        for name in &self.locked_stack {
            name.hash(&mut hasher);
        }
        for (k, v) in key {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish() as usize
    }

    /// Sorted list of series keys for display.  Primary order is
    /// `self.sort_column` (alphabetical ascending for string-valued
    /// split columns; numeric descending for `n`/`avg`/`max`/`last`;
    /// ascending for `min`).  Ties fall back to alphabetical by
    /// full series key.
    pub fn series_keys(&self) -> Vec<Vec<(String, String)>> {
        use std::cmp::Ordering;
        let summaries = self.store.series_summary(self.aggregation);
        let summary_by_key: std::collections::HashMap<
            Vec<(String, String)>,
            SeriesSummary,
        > = summaries.into_iter().map(|s| (s.key.clone(), s)).collect();
        let mut keys = self.alpha_series_keys();
        keys.sort_by(|a, b| {
            let sa = summary_by_key.get(a);
            let sb = summary_by_key.get(b);
            let primary = match &self.sort_column {
                SortColumn::SplitKey(k) => {
                    let av = a
                        .iter()
                        .find(|(kk, _)| kk == k)
                        .map(|(_, v)| v.as_str())
                        .unwrap_or("");
                    let bv = b
                        .iter()
                        .find(|(kk, _)| kk == k)
                        .map(|(_, v)| v.as_str())
                        .unwrap_or("");
                    av.cmp(bv)
                }
                SortColumn::Count => sb
                    .map(|s| s.count)
                    .unwrap_or(0)
                    .cmp(&sa.map(|s| s.count).unwrap_or(0)),
                SortColumn::Min => sa
                    .map(|s| s.min_ns)
                    .unwrap_or(0)
                    .cmp(&sb.map(|s| s.min_ns).unwrap_or(0)),
                SortColumn::Avg => sb
                    .map(|s| s.avg_ns)
                    .unwrap_or(0)
                    .cmp(&sa.map(|s| s.avg_ns).unwrap_or(0)),
                SortColumn::Max => sb
                    .map(|s| s.max_ns)
                    .unwrap_or(0)
                    .cmp(&sa.map(|s| s.max_ns).unwrap_or(0)),
                SortColumn::Last => sb
                    .map(|s| s.last_ns)
                    .unwrap_or(0)
                    .cmp(&sa.map(|s| s.last_ns).unwrap_or(0)),
            };
            if primary == Ordering::Equal {
                a.cmp(b)
            } else {
                primary
            }
        });
        keys
    }


    /// Record one span into the series store.  Caller has already
    /// verified the span's resolved stack matches `locked_stack`.
    /// Reads child-sum from the aggregator when the active metric
    /// is `SelfTime`.
    pub fn record_span(&mut self, agg: &Aggregator, span: &WireSpan) {
        let Some(closed) = span.closed_at_ns else {
            return;
        };
        let total = closed.saturating_sub(span.opened_at_ns);
        let value = match self.metric {
            Metric::Total => total,
            Metric::SelfTime => total.saturating_sub(agg.child_sum_for(span.id)),
        };
        let series_key = agg.collect_splits_for(span.id, &self.split_keys);
        self.store.record(series_key, closed, value);
    }

    /// Discard the store and re-walk the aggregator's ring to
    /// re-populate it under the current `window_secs`, `metric`, and
    /// `split_keys`.  Called when any of those parameters change,
    /// instead of just wiping — so the user can flip splits or
    /// aggregation modes (or even turn tracing off and resplit) and
    /// still see the existing data re-bucketed under the new
    /// parameters.  Bounded by the ring (≤ `history_budget` spans).
    pub fn rehydrate(&mut self, agg: &Aggregator) {
        self.store.reset_window(self.window_secs);
        // Snapshot the matching spans first; can't hold the
        // `agg.iter_with_stack()` borrow while we mutate `self.store`
        // via `record_span(agg, ..)` (each call borrows agg again).
        let ids: Vec<u64> = agg
            .iter_with_stack()
            .filter(|(_, stack)| stack.as_slice() == self.locked_stack.as_slice())
            .map(|(s, _)| s.id)
            .collect();
        for id in ids {
            // Re-fetch the span from the aggregator to keep the
            // child_sum read consistent with what record_span uses.
            if let Some(span) = agg.span_by_id(id) {
                self.record_span(agg, span);
            }
        }
    }
}
