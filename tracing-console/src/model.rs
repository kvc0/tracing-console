//! Application state for the console TUI.
//!
//! Decoupled from rendering so it can be tested without touching
//! ratatui / crossterm.  Both [`Model`] and [`Update`] are
//! `Serialize` + `Deserialize` so integration tests can construct
//! sequences of updates (or replay a captured `--states` dump) and
//! assert on the resulting model.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing_console_host::{WireLevelFilter, WireSpan};

use crate::aggregate::{Aggregator, BucketKey, StackStats, candidate_split_keys_for};

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

// ── Graph view ───────────────────────────────────────────────────────

/// Top-level view dispatch.  Table is the default two-pane stacks +
/// details layout.  Graph replaces the stacks table with a line
/// chart of the locked bucket's metric over time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViewMode {
    Table,
    Graph(GraphState),
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
struct Series {
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
    series: HashMap<Vec<(String, String)>, Series>,
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
            split_keys: BTreeSet::new(),
            hidden_series: BTreeSet::new(),
            details_selected: 0,
            focus: GraphFocus::Chart,
            sort_column: SortColumn::Count,
            agg_input: None,
            window_input: None,
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

    /// Stable colour-slot index for `key` — its position in
    /// alphabetical order.  Returns `0` if the key isn't currently
    /// in the store (shouldn't happen for keys that were just
    /// looked up out of [`Self::series_keys`]).
    pub fn color_index_of(&self, key: &[(String, String)]) -> usize {
        self.alpha_series_keys()
            .iter()
            .position(|k| k.as_slice() == key)
            .unwrap_or(0)
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
    // ── Graph view ──────────────────────────────────────────────
    /// `g`: enter graph mode locked onto the currently-highlighted
    /// row, or leave graph mode if already in it.  No-op if Table
    /// mode is active and nothing is highlighted.
    ToggleGraph,
    /// Replace graph aggregation outright.  Issued by the
    /// aggregation-input modal commit and by the test suite.
    SetGraphAgg(AggMode),
    /// `t`: flip metric between `Total` and `SelfTime`.  Wipes the
    /// store because the bin scalars are metric-specific.
    ToggleGraphMetric,
    /// `a`: open the aggregation-expression modal.  The buffer
    /// accepts a freeform expression — `a`/`avg`, `min`, `max`,
    /// or `pX[.XX]` — that's parsed at commit time via
    /// [`parse_agg_input`].
    BeginGraphAggInput,
    GraphAggInputChar(char),
    GraphAggInputBackspace,
    GraphAggInputCancel,
    GraphAggInputCommit,
    /// `w`: open the window-size-input modal.
    BeginGraphWindowInput,
    GraphWindowInputChar(char),
    GraphWindowInputBackspace,
    GraphWindowInputCancel,
    GraphWindowInputCommit,
    /// `Tab` inside graph mode.  Swaps focus between Chart and
    /// Details panes (graph-mode analogue of `SwitchFocus`).
    GraphSwitchFocus,
    /// `j`/`k` inside graph Details.  Moves the split-key candidate
    /// cursor.
    GraphSelectUp,
    GraphSelectDown,
    /// `Space` inside graph Details.  Toggles the currently-cursor
    /// split key.  Wipes the store because the series partitioning
    /// changed.
    GraphToggleSplit,
    /// `Left` / `Right` inside graph Details.  Cycles the active
    /// sort column for the series table.  Underlined in the header
    /// of the expanded details pane.
    GraphSortColumnLeft,
    GraphSortColumnRight,
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
        assert!(m.split_keys().is_empty());
        // Switch to Details, then toggle: api becomes a split key.
        m.apply(Update::SwitchFocus);
        m.apply(Update::ToggleSplitSelected);
        assert!(m.split_keys().contains("api"));
        // Toggle again removes.
        m.apply(Update::ToggleSplitSelected);
        assert!(m.split_keys().is_empty());
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

        let mut sk = BTreeSet::new();
        sk.insert("api".to_string());
        m.agg.set_split_keys(sk);
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
        let mut sk = BTreeSet::new();
        sk.insert("api".to_string());
        m.agg.set_split_keys(sk);
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

    // ── Graph view ──────────────────────────────────────────────

    fn timed_span(id: u64, parent: Option<u64>, name: &str, opened: u64, closed: u64) -> WireSpan {
        let mut s = span_with_parent(id, name, parent);
        s.opened_at_ns = opened;
        s.closed_at_ns = Some(closed);
        s
    }

    fn timed_span_with_field(
        id: u64,
        parent: Option<u64>,
        name: &str,
        opened: u64,
        closed: u64,
        k: &str,
        v: &str,
    ) -> WireSpan {
        let mut s = timed_span(id, parent, name, opened, closed);
        s.fields.push((
            k.to_string(),
            tracing_console_host::WireFieldValue::Str(v.into()),
        ));
        s
    }

    fn current_graph(m: &Model) -> &GraphState {
        match &m.view {
            ViewMode::Graph(gs) => gs,
            ViewMode::Table => panic!("expected graph view, got table"),
        }
    }

    #[test]
    fn toggle_graph_locks_onto_selected_bucket() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::SpanReceived(span(11, "beta")));
        m.apply(Update::SelectDown); // highlight `beta`
        m.apply(Update::ToggleGraph);
        let gs = current_graph(&m);
        assert_eq!(gs.locked_stack, vec!["beta".to_string()]);
        assert!(matches!(gs.aggregation, AggMode::Avg));
        assert_eq!(gs.metric, Metric::Total);
        assert!((gs.window_secs - 1.0).abs() < 1e-9);
    }

    #[test]
    fn toggle_graph_round_trip_returns_to_table() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        assert!(matches!(m.view, ViewMode::Graph(_)));
        m.apply(Update::ToggleGraph);
        assert!(matches!(m.view, ViewMode::Table));
    }

    #[test]
    fn toggle_graph_with_nothing_selected_stays_in_table() {
        let mut m = Model::new(8);
        m.apply(Update::ToggleGraph);
        assert!(matches!(m.view, ViewMode::Table));
    }

    /// Drive the agg-input modal by typing `input` and pressing
    /// Enter; returns the resulting aggregation.
    fn type_agg(m: &mut Model, input: &str) -> AggMode {
        m.apply(Update::BeginGraphAggInput);
        for c in input.chars() {
            m.apply(Update::GraphAggInputChar(c));
        }
        m.apply(Update::GraphAggInputCommit);
        current_graph(m).aggregation
    }

    #[test]
    fn graph_agg_input_avg_via_a_and_avg() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        // Force off Avg first so the assertion is meaningful.
        m.apply(Update::SetGraphAgg(AggMode::Min));
        assert_eq!(type_agg(&mut m, "a"), AggMode::Avg);
        m.apply(Update::SetGraphAgg(AggMode::Min));
        assert_eq!(type_agg(&mut m, "avg"), AggMode::Avg);
    }

    #[test]
    fn graph_agg_input_min_max_keywords() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        assert_eq!(type_agg(&mut m, "min"), AggMode::Min);
        assert_eq!(type_agg(&mut m, "max"), AggMode::Max);
    }

    #[test]
    fn graph_agg_input_percentile() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        assert_eq!(type_agg(&mut m, "p50"), AggMode::Percentile(50.0));
        assert_eq!(type_agg(&mut m, "p99.5"), AggMode::Percentile(99.5));
        // Single-digit percentile is fine too.
        assert_eq!(type_agg(&mut m, "p5"), AggMode::Percentile(5.0));
    }

    #[test]
    fn graph_agg_input_p0_is_min_p100_or_higher_is_max() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        assert_eq!(type_agg(&mut m, "p0"), AggMode::Min);
        assert_eq!(type_agg(&mut m, "p100"), AggMode::Max);
        // Anything above 100 also clamps to Max.
        assert_eq!(type_agg(&mut m, "p9999"), AggMode::Max);
    }

    #[test]
    fn graph_agg_input_uppercase_is_normalised() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        assert_eq!(type_agg(&mut m, "AVG"), AggMode::Avg);
        assert_eq!(type_agg(&mut m, "Min"), AggMode::Min);
        assert_eq!(type_agg(&mut m, "P50"), AggMode::Percentile(50.0));
    }

    #[test]
    fn graph_agg_input_rejects_garbage_silently() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        let original = current_graph(&m).aggregation;
        // Stray `x` is filtered out by the char handler; the remaining
        // buffer is "" which is not parseable, so the commit no-ops.
        m.apply(Update::BeginGraphAggInput);
        m.apply(Update::GraphAggInputChar('x'));
        m.apply(Update::GraphAggInputChar('!'));
        m.apply(Update::GraphAggInputCommit);
        assert_eq!(current_graph(&m).aggregation, original);

        // A typo'd keyword also no-ops cleanly.
        m.apply(Update::BeginGraphAggInput);
        for c in "minx".chars() {
            m.apply(Update::GraphAggInputChar(c));
        }
        m.apply(Update::GraphAggInputCommit);
        assert_eq!(current_graph(&m).aggregation, original);
    }

    #[test]
    fn graph_agg_input_cancel_drops_buffer() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        let original = current_graph(&m).aggregation;
        m.apply(Update::BeginGraphAggInput);
        for c in "p99".chars() {
            m.apply(Update::GraphAggInputChar(c));
        }
        m.apply(Update::GraphAggInputCancel);
        assert!(current_graph(&m).agg_input.is_none());
        assert_eq!(current_graph(&m).aggregation, original);
    }

    #[test]
    fn graph_window_input_rejects_zero_and_negatives() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "a")));
        m.apply(Update::ToggleGraph);
        let original = current_graph(&m).window_secs;

        for bad in ["0", ""] {
            m.apply(Update::BeginGraphWindowInput);
            for c in bad.chars() {
                m.apply(Update::GraphWindowInputChar(c));
            }
            m.apply(Update::GraphWindowInputCommit);
            assert!((current_graph(&m).window_secs - original).abs() < 1e-9);
        }
    }

    #[test]
    fn graph_window_input_commits_positive_float() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "a")));
        m.apply(Update::ToggleGraph);
        m.apply(Update::BeginGraphWindowInput);
        for c in "0.5".chars() {
            m.apply(Update::GraphWindowInputChar(c));
        }
        m.apply(Update::GraphWindowInputCommit);
        assert!((current_graph(&m).window_secs - 0.5).abs() < 1e-9);
    }

    #[test]
    fn graph_metric_toggle_rehydrates_store() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 100)));
        m.apply(Update::ToggleGraph);
        m.apply(Update::SpanReceived(timed_span(11, None, "a", 100, 250)));
        let before = current_graph(&m).store.series.len();
        assert!(before > 0);

        m.apply(Update::ToggleGraphMetric);
        // Metric flipped + store re-populated from the ring, not
        // wiped, so the user can compare Total vs SelfTime without
        // losing their existing history.
        assert_eq!(current_graph(&m).metric, Metric::SelfTime);
        assert_eq!(current_graph(&m).store.series.len(), before);
    }

    /// Drive the details cursor down past every series row so it
    /// lands on the first key candidate.  Used by tests that want
    /// to operate on the split-keys section without enumerating the
    /// exact series count by hand.
    fn move_cursor_to_first_key(m: &mut Model) {
        if let ViewMode::Graph(gs) = &m.view {
            assert_eq!(gs.focus, GraphFocus::Details, "tab into Details first");
            let target = gs.series_keys().len();
            // Down from wherever; clamped at total - 1.  Drive past
            // any series rows so the cursor lands on candidate[0].
            for _ in 0..target {
                m.apply(Update::GraphSelectDown);
            }
        }
    }

    #[test]
    fn graph_toggle_split_re_partitions_existing_history() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::SpanReceived(timed_span_with_field(
            12, None, "req", 200, 300, "api", "fetch",
        )));
        // No split yet → one merged series.
        assert_eq!(current_graph(&m).store.series.len(), 1);

        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);
        let gs = current_graph(&m);
        assert!(gs.split_keys.contains("api"));
        // Store rehydrated from the ring under the new split → two
        // series ("fetch" + "update"), not empty.
        assert_eq!(gs.store.series.len(), 2);

        // Cursor should now be pinned at the `api` key row, not
        // dragged onto a newly-appeared series row.
        m.apply(Update::GraphToggleSplit);
        let gs = current_graph(&m);
        assert!(!gs.split_keys.contains("api"));
        assert_eq!(gs.store.series.len(), 1);
    }

    #[test]
    fn graph_split_change_works_after_tracing_stops() {
        // Mirrors the user-facing scenario: stream a few spans,
        // freeze the input (no more SpanReceived), then change the
        // split and confirm the series count actually changes.  The
        // rehydrate path walks the aggregator's ring rather than
        // waiting for new arrivals.
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            12, None, "req", 200, 300, "api", "post",
        )));
        m.apply(Update::ToggleGraph);
        // No more spans arrive past this point — simulates tracing
        // being toggled off.
        assert_eq!(current_graph(&m).store.series.len(), 1);

        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);
        // Now three series, all derived from the same frozen ring.
        assert_eq!(current_graph(&m).store.series.len(), 3);
    }

    #[test]
    fn graph_record_partitions_by_split_keys() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::ToggleGraph);
        // Enable the api split via Details → first key row.
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);
        // Feed two spans with distinct `api` values.
        m.apply(Update::SpanReceived(timed_span_with_field(
            20, None, "req", 100, 200, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            21, None, "req", 200, 300, "api", "post",
        )));
        let gs = current_graph(&m);
        assert_eq!(gs.store.series.len(), 2);
    }

    #[test]
    fn graph_state_round_trips_through_json() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "alpha")));
        m.apply(Update::ToggleGraph);
        let json = serde_json::to_string(&m).unwrap();
        let back: Model = serde_json::from_str(&json).unwrap();
        match &back.view {
            ViewMode::Graph(gs) => {
                assert_eq!(gs.locked_stack, vec!["alpha".to_string()]);
                // Store is #[serde(skip)] — it deserialises empty.
                assert!(gs.store.series.is_empty());
            }
            _ => panic!("expected graph view"),
        }
    }

    #[test]
    fn graph_window_resize_rehydrates_under_new_window() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 100)));
        m.apply(Update::ToggleGraph);
        m.apply(Update::SpanReceived(timed_span(11, None, "a", 100, 200)));
        let before = current_graph(&m).store.series.len();
        assert!(before > 0);

        m.apply(Update::BeginGraphWindowInput);
        for c in "5".chars() {
            m.apply(Update::GraphWindowInputChar(c));
        }
        m.apply(Update::GraphWindowInputCommit);
        let gs = current_graph(&m);
        assert!((gs.window_secs - 5.0).abs() < 1e-9);
        // Bins were re-laid-out under the new window, not wiped.
        assert_eq!(gs.store.series.len(), before);
    }

    #[test]
    fn graph_details_cursor_walks_series_then_keys() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        // Walk past the merged "(all)" series to land on the api key,
        // then toggle the split on so we have two series + one key.
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);
        assert_eq!(current_graph(&m).series_keys().len(), 2);

        // After the toggle, the cursor is pinned on the api key at
        // index 2 (= 2 series + 0 key offset).
        assert_eq!(current_graph(&m).details_selected, 2);

        // Down past the end clamps; Up retraces.
        m.apply(Update::GraphSelectDown);
        assert_eq!(current_graph(&m).details_selected, 2, "clamped at last row");
        m.apply(Update::GraphSelectUp);
        assert_eq!(current_graph(&m).details_selected, 1);
        m.apply(Update::GraphSelectUp);
        assert_eq!(current_graph(&m).details_selected, 0);
        m.apply(Update::GraphSelectUp);
        assert_eq!(current_graph(&m).details_selected, 0, "clamped at first row");
    }

    #[test]
    fn graph_space_on_series_row_toggles_visibility() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // enable api split → 2 series

        let series = current_graph(&m).series_keys();
        assert_eq!(series.len(), 2);

        // Move cursor back up onto the first series row.
        m.apply(Update::GraphSelectUp);
        m.apply(Update::GraphSelectUp);
        assert_eq!(current_graph(&m).details_selected, 0);

        m.apply(Update::GraphToggleSplit);
        let gs = current_graph(&m);
        assert!(gs.hidden_series.contains(&series[0]));
        assert_eq!(gs.series_keys().len(), 2, "store keeps the data");

        // Second press un-hides.
        m.apply(Update::GraphToggleSplit);
        assert!(!current_graph(&m).hidden_series.contains(&series[0]));
    }

    #[test]
    fn graph_space_on_key_row_still_toggles_split() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // turn split on → 2 series

        // After the toggle the cursor is pinned on the api key.
        // Another Space on that same row should turn the split off.
        m.apply(Update::GraphToggleSplit);
        assert!(!current_graph(&m).split_keys.contains("api"));
        assert!(
            current_graph(&m).hidden_series.is_empty(),
            "split change clears hidden_series"
        );
    }

    #[test]
    fn graph_split_toggle_clears_hidden_series() {
        // Hide a series, then change splits — the hidden flag
        // should not leak into a different series space.
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // enable split

        // Cursor pinned at the api key.  Walk it back to series[0]
        // and hide it.
        m.apply(Update::GraphSelectUp);
        m.apply(Update::GraphSelectUp);
        m.apply(Update::GraphToggleSplit);
        assert!(!current_graph(&m).hidden_series.is_empty());

        // Now move back to the key row and toggle the split off.
        m.apply(Update::GraphSelectDown);
        m.apply(Update::GraphSelectDown);
        m.apply(Update::GraphToggleSplit);
        let gs = current_graph(&m);
        assert!(gs.split_keys.is_empty());
        assert!(gs.hidden_series.is_empty());
    }

    #[test]
    fn graph_series_summary_aggregates_per_series() {
        // Three matching spans, distinct durations + one split value;
        // confirm `series_summary` rolls them up correctly.
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 1_000_000_000, 1_000_000_500, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            12, None, "req", 2_000_000_000, 2_000_000_200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);

        let summary = current_graph(&m).store.series_summary(AggMode::Avg);
        assert_eq!(summary.len(), 2);
        // Sorted by key — `api=fetch` first, `api=update` second.
        let fetch = &summary[0];
        let update_ = &summary[1];
        assert_eq!(fetch.key, vec![("api".into(), "fetch".into())]);
        assert_eq!(fetch.count, 2);
        assert_eq!(fetch.min_ns, 100);          // 100 - 0
        assert_eq!(fetch.max_ns, 500);          // 1_000_000_500 - 1_000_000_000
        // avg = (100 + 500) / 2 = 300
        assert_eq!(fetch.avg_ns, 300);

        assert_eq!(update_.key, vec![("api".into(), "update".into())]);
        assert_eq!(update_.count, 1);
        assert_eq!(update_.min_ns, 200);
        assert_eq!(update_.max_ns, 200);
        assert_eq!(update_.avg_ns, 200);
        assert_eq!(update_.last_ns, 200);
    }

    #[test]
    fn graph_default_sort_is_count() {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(timed_span(10, None, "alpha", 0, 100)));
        m.apply(Update::ToggleGraph);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Count);
    }

    #[test]
    fn graph_left_right_cycle_sort_column() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // enable api split

        // Columns are: SplitKey(api), Count, Min, Avg, Max, Last.
        // Default sort is Count.
        assert_eq!(current_graph(&m).sort_column, SortColumn::Count);

        // Right cycles through Min, Avg, Max, Last, then wraps to SplitKey(api).
        m.apply(Update::GraphSortColumnRight);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Min);
        m.apply(Update::GraphSortColumnRight);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Avg);
        m.apply(Update::GraphSortColumnRight);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Max);
        m.apply(Update::GraphSortColumnRight);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Last);
        m.apply(Update::GraphSortColumnRight);
        assert_eq!(
            current_graph(&m).sort_column,
            SortColumn::SplitKey("api".into())
        );

        // Left wraps the other way.
        m.apply(Update::GraphSortColumnLeft);
        assert_eq!(current_graph(&m).sort_column, SortColumn::Last);
    }

    #[test]
    fn graph_series_keys_order_follows_sort_column() {
        // Three series with predictable counts so we can verify the
        // resulting order under different sort columns.  series order
        // by count desc: fetch(3), update(2), post(1).
        let mut m = Model::new(32);
        for id in 0..3u64 {
            m.apply(Update::SpanReceived(timed_span_with_field(
                10 + id,
                None,
                "req",
                id * 10,
                id * 10 + 50,
                "api",
                "fetch",
            )));
        }
        for id in 0..2u64 {
            m.apply(Update::SpanReceived(timed_span_with_field(
                20 + id,
                None,
                "req",
                id * 10,
                id * 10 + 60,
                "api",
                "update",
            )));
        }
        m.apply(Update::SpanReceived(timed_span_with_field(
            30, None, "req", 0, 70, "api", "post",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // turn split on → 3 series

        // Count is descending: fetch (3), update (2), post (1).
        let order: Vec<String> = current_graph(&m)
            .series_keys()
            .iter()
            .map(|k| k[0].1.clone())
            .collect();
        assert_eq!(order, vec!["fetch", "update", "post"]);

        // Switch to SplitKey(api) — alphabetical ascending: fetch, post, update.
        m.apply(Update::GraphSortColumnLeft); // Count → SplitKey(api)
        let order: Vec<String> = current_graph(&m)
            .series_keys()
            .iter()
            .map(|k| k[0].1.clone())
            .collect();
        assert_eq!(order, vec!["fetch", "post", "update"]);

        // Switch to Max (descending): fetch=50, update=70, post=70.
        // Update first by alpha within max-tie at 70.
        m.apply(Update::GraphSortColumnRight); // → Count
        m.apply(Update::GraphSortColumnRight); // → Min
        m.apply(Update::GraphSortColumnRight); // → Avg
        m.apply(Update::GraphSortColumnRight); // → Max
        assert_eq!(current_graph(&m).sort_column, SortColumn::Max);
        let order: Vec<String> = current_graph(&m)
            .series_keys()
            .iter()
            .map(|k| k[0].1.clone())
            .collect();
        assert_eq!(order, vec!["post", "update", "fetch"]);
    }

    #[test]
    fn graph_sort_falls_back_when_split_removed() {
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit); // enable split → SplitKey(api) now selectable
        m.apply(Update::GraphSortColumnLeft); // Count → SplitKey(api)
        assert_eq!(
            current_graph(&m).sort_column,
            SortColumn::SplitKey("api".into())
        );

        // Disable the split — sort column should fall back to Count.
        m.apply(Update::GraphToggleSplit);
        assert!(current_graph(&m).split_keys.is_empty());
        assert_eq!(current_graph(&m).sort_column, SortColumn::Count);
    }

    #[test]
    fn graph_chart_focus_cursor_walks_series_only() {
        // Two series + one candidate key.  In Chart focus the cursor
        // must stop at series[1] (index 1); it must not slip onto
        // the key row (which is only reachable when Details is
        // expanded).
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span_with_field(
            10, None, "req", 0, 100, "api", "fetch",
        )));
        m.apply(Update::SpanReceived(timed_span_with_field(
            11, None, "req", 100, 200, "api", "update",
        )));
        m.apply(Update::ToggleGraph);
        // Enable api split via Details.
        m.apply(Update::GraphSwitchFocus);
        move_cursor_to_first_key(&mut m);
        m.apply(Update::GraphToggleSplit);
        // Tab back to Chart focus.
        m.apply(Update::GraphSwitchFocus);
        assert_eq!(current_graph(&m).focus, GraphFocus::Chart);
        // Cursor was at the key row (index 2) but got clamped to
        // series_count - 1 = 1 on the focus switch.
        assert_eq!(current_graph(&m).details_selected, 1);
        // Down clamps at series count - 1.
        m.apply(Update::GraphSelectDown);
        assert_eq!(current_graph(&m).details_selected, 1, "no key rows in Chart focus");
        m.apply(Update::GraphSelectUp);
        assert_eq!(current_graph(&m).details_selected, 0);
    }

    #[test]
    fn graph_chart_focus_space_toggles_series_visibility() {
        // From compact (Chart focus) the user should still be able
        // to navigate the series list with j/k and toggle visibility
        // with Space.  Re-uses the merged "(all)" series as the
        // simplest workable bucket.
        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(timed_span(10, None, "alpha", 0, 100)));
        m.apply(Update::SpanReceived(timed_span(11, None, "alpha", 100, 200)));
        m.apply(Update::ToggleGraph);
        assert_eq!(current_graph(&m).focus, GraphFocus::Chart);

        let series = current_graph(&m).series_keys();
        assert_eq!(series.len(), 1);
        m.apply(Update::GraphToggleSplit);
        assert!(current_graph(&m).hidden_series.contains(&series[0]));
        m.apply(Update::GraphToggleSplit);
        assert!(!current_graph(&m).hidden_series.contains(&series[0]));
    }

    #[test]
    fn graph_entry_rehydrates_from_existing_ring() {
        let mut m = Model::new(8);
        // Build up some ring history before entering graph mode.
        for i in 0..5u64 {
            m.apply(Update::SpanReceived(timed_span(
                10 + i,
                None,
                "alpha",
                i * 100,
                i * 100 + 50,
            )));
        }
        m.apply(Update::ToggleGraph);
        // Chart should be non-empty immediately — populated from
        // the ring rather than waiting for new arrivals.
        assert!(!current_graph(&m).store.series.is_empty());
    }
}
