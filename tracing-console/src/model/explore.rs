//! Explore mode: lists individual span instances of a chosen
//! bucket, with a `/`-search filter and sortable columns.  Enter
//! on a row opens [`TraceDetailState`] — the full trace tree
//! starting at that span's root.
//!
//! Entry from the table view sets the cache level to `Off` so
//! the screen doesn't churn while the user reads; exit restores
//! whatever level the server was confirmed to be on at entry
//! time (stored in [`ExploreState::restore_level`]).

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use tracing_console_host::{WireLevelFilter, WireSpan};

use super::core::Model;

/// Sortable columns on the explore-mode list.  Direction is
/// carried separately on [`ExploreState`] so `i` can invert it
/// without changing columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExploreSortColumn {
    /// `opened_at_ns` — defaults to descending (newest first).
    Timestamp,
    /// `closed_at_ns − opened_at_ns` — defaults to descending so
    /// the slowest spans sit at the top.
    Latency,
    /// Display value of a field — defaults to ascending alphabetical.
    Field(String),
}

impl ExploreSortColumn {
    /// Direction the column reads most naturally — applied
    /// whenever the user *cycles* to this column.  `i` then
    /// flips that without changing column.
    pub fn default_direction(&self) -> SortDirection {
        match self {
            ExploreSortColumn::Timestamp | ExploreSortColumn::Latency => SortDirection::Desc,
            ExploreSortColumn::Field(_) => SortDirection::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    pub fn flip(self) -> Self {
        match self {
            SortDirection::Asc => SortDirection::Desc,
            SortDirection::Desc => SortDirection::Asc,
        }
    }
    /// Up arrow for ascending, down arrow for descending — drawn
    /// next to the active column header in the explore table.
    pub fn arrow(self) -> &'static str {
        match self {
            SortDirection::Asc => "▲",
            SortDirection::Desc => "▼",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExploreState {
    /// Stack we entered exploring (snapshot of the table cursor
    /// at entry time).  Filters the cache to span instances
    /// whose resolved stack matches exactly.
    pub locked_stack: Vec<String>,
    /// Level the server was confirmed at when we entered;
    /// restored on exit so we don't strand the user with
    /// tracing off.  `None` means "we never knew" → exit
    /// requests `Trace` as a sane default.
    pub restore_level: Option<WireLevelFilter>,
    /// Cursor index into the filtered + sorted row list.
    pub selected: usize,
    pub sort: ExploreSortColumn,
    /// Sort direction for the active column.  Reset to the
    /// column's `default_direction` on every cycle; `i` flips it
    /// in place.
    #[serde(default = "default_sort_direction")]
    pub direction: SortDirection,
    /// Vim-style search.  `Some` while the user is typing into
    /// the `/` modal; everything in the buffer applies as a
    /// live filter.  On Enter the buffer commits to `query`.
    pub search_input: Option<String>,
    /// Committed search query — `""` means no filter.
    pub query: String,
}

fn default_sort_direction() -> SortDirection {
    ExploreSortColumn::Timestamp.default_direction()
}

impl ExploreState {
    pub fn new(locked_stack: Vec<String>, restore_level: Option<WireLevelFilter>) -> Self {
        let sort = ExploreSortColumn::Timestamp;
        let direction = sort.default_direction();
        Self {
            locked_stack,
            restore_level,
            selected: 0,
            sort,
            direction,
            search_input: None,
            query: String::new(),
        }
    }

    /// Effective search string — the input buffer while a search
    /// modal is open (so the user sees live filtering as they
    /// type), or the committed `query` otherwise.
    pub fn effective_query(&self) -> &str {
        self.search_input.as_deref().unwrap_or(&self.query)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetailState {
    /// Root span id — the trace tree starts here.
    pub root_id: u64,
    /// Cursor into [`visible_trace_rows`] — moved by ↑/↓.
    #[serde(default)]
    pub selected_idx: usize,
    /// Spans whose subtrees the user has collapsed.  Defaults to
    /// empty so the whole tree is visible on entry.
    #[serde(default)]
    pub collapsed: std::collections::BTreeSet<u64>,
    /// Explore state carried through so Esc returns the user
    /// straight back to where they came from with the same
    /// cursor / search / sort.
    pub explore: ExploreState,
}

/// A row in the post-collapse, ready-to-render trace tree.
/// Spans and events share the same row coordinate space so the
/// cursor in `selected_idx` can land on either.
#[derive(Debug, Clone)]
pub enum TraceRow {
    Span {
        id: u64,
        depth: usize,
        has_children: bool,
        expanded: bool,
    },
    Event {
        parent_id: u64,
        idx: usize,
        depth: usize,
    },
}

/// Build the flat post-collapse row list in DFS order.  Children
/// of a span in `td.collapsed` are skipped entirely (the
/// collapsed span itself stays visible).
pub fn visible_trace_rows(model: &Model, td: &TraceDetailState) -> Vec<TraceRow> {
    use std::collections::HashMap;
    let mut by_parent: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut span_by_id: HashMap<u64, &WireSpan> = HashMap::new();
    for (s, _) in model.agg.iter_with_stack() {
        span_by_id.insert(s.id, s);
        if let Some(p) = s.parent_id {
            by_parent.entry(p).or_default().push(s.id);
        }
    }
    for kids in by_parent.values_mut() {
        kids.sort_by_key(|id| span_by_id.get(id).map(|s| s.opened_at_ns).unwrap_or(0));
    }

    let Some(root) = span_by_id.get(&td.root_id).copied() else {
        return Vec::new();
    };
    let mut out: Vec<TraceRow> = Vec::new();
    walk_trace(&mut out, root, 0, &by_parent, &span_by_id, &td.collapsed);
    out
}

fn walk_trace<'a>(
    out: &mut Vec<TraceRow>,
    span: &'a WireSpan,
    depth: usize,
    by_parent: &std::collections::HashMap<u64, Vec<u64>>,
    span_by_id: &std::collections::HashMap<u64, &'a WireSpan>,
    collapsed: &std::collections::BTreeSet<u64>,
) {
    let kids = by_parent.get(&span.id).map(|v| v.as_slice()).unwrap_or(&[]);
    let has_children = !kids.is_empty() || !span.events.is_empty();
    let expanded = !collapsed.contains(&span.id);
    out.push(TraceRow::Span {
        id: span.id,
        depth,
        has_children,
        expanded,
    });
    if !expanded {
        return;
    }
    // Interleave child spans and events chronologically.
    enum Item {
        Event(usize, u64), // (idx, recorded_at_ns)
        Child(u64, u64),   // (id, opened_at_ns)
    }
    let mut items: Vec<Item> = Vec::new();
    for (i, e) in span.events.iter().enumerate() {
        items.push(Item::Event(i, e.recorded_at_ns));
    }
    for &cid in kids {
        let ts = span_by_id.get(&cid).map(|s| s.opened_at_ns).unwrap_or(0);
        items.push(Item::Child(cid, ts));
    }
    items.sort_by_key(|item| match item {
        Item::Event(_, t) | Item::Child(_, t) => *t,
    });
    for item in items {
        match item {
            Item::Event(idx, _) => {
                out.push(TraceRow::Event {
                    parent_id: span.id,
                    idx,
                    depth: depth + 1,
                });
            }
            Item::Child(cid, _) => {
                if let Some(child) = span_by_id.get(&cid) {
                    walk_trace(out, child, depth + 1, by_parent, span_by_id, collapsed);
                }
            }
        }
    }
}

// ── Pure data helpers (called from both the reducer and the
//   view layer) ─────────────────────────────────────────────────

/// Every span in the aggregator whose resolved stack matches
/// `es.locked_stack`, *before* applying the `/`-search filter.
/// The renderer uses this for distinguishing-field column
/// discovery, which has to stay stable as the search narrows the
/// row list (otherwise a single-result search collapses columns
/// because each field now has only one distinct value).
pub fn locked_stack_spans<'a>(model: &'a Model, es: &ExploreState) -> Vec<&'a WireSpan> {
    model
        .agg
        .iter_with_stack()
        .filter(|(_, stack)| stack.as_slice() == es.locked_stack.as_slice())
        .map(|(s, _)| s)
        .collect()
}

/// `locked_stack_spans` then narrowed by `es.effective_query` and
/// sorted by `(es.sort, es.direction)` — the rows the table
/// actually renders.
pub fn matching_spans<'a>(model: &'a Model, es: &ExploreState) -> Vec<&'a WireSpan> {
    let q = es.effective_query().to_ascii_lowercase();
    let mut out: Vec<&WireSpan> = locked_stack_spans(model, es)
        .into_iter()
        .filter(|s| q.is_empty() || span_matches_query(s, &q))
        .collect();
    sort_spans(&mut out, &es.sort, es.direction);
    out
}

fn span_matches_query(s: &WireSpan, q: &str) -> bool {
    if s.name.to_ascii_lowercase().contains(q) {
        return true;
    }
    for (k, v) in &s.fields {
        if k.to_ascii_lowercase().contains(q)
            || v.to_string_value().to_ascii_lowercase().contains(q)
        {
            return true;
        }
    }
    for e in &s.events {
        if e.name.to_ascii_lowercase().contains(q) {
            return true;
        }
        for (k, v) in &e.fields {
            if k.to_ascii_lowercase().contains(q)
                || v.to_string_value().to_ascii_lowercase().contains(q)
            {
                return true;
            }
        }
    }
    false
}

fn sort_spans(spans: &mut [&WireSpan], by: &ExploreSortColumn, direction: SortDirection) {
    match by {
        ExploreSortColumn::Timestamp => {
            spans.sort_by_key(|a| a.opened_at_ns);
        }
        ExploreSortColumn::Latency => {
            spans.sort_by_key(|a| latency_ns(a));
        }
        ExploreSortColumn::Field(k) => {
            spans.sort_by_key(|a| field_string(a, k));
        }
    }
    if matches!(direction, SortDirection::Desc) {
        spans.reverse();
    }
}

pub fn latency_ns(s: &WireSpan) -> u64 {
    s.closed_at_ns
        .map(|c| c.saturating_sub(s.opened_at_ns))
        .unwrap_or(0)
}

pub fn field_string(s: &WireSpan, key: &str) -> String {
    s.field(key)
        .map(|v| v.to_string_value())
        .unwrap_or_default()
}

/// Field keys whose values differ across the visible span set —
/// these become the column headers in the explore table.
pub fn distinguishing_fields(spans: &[&WireSpan]) -> Vec<String> {
    let mut by_key: HashMap<String, BTreeSet<String>> = HashMap::new();
    for s in spans {
        for (k, v) in &s.fields {
            by_key
                .entry(k.clone())
                .or_default()
                .insert(v.to_string_value());
        }
    }
    let mut keys: Vec<String> = by_key
        .into_iter()
        .filter(|(_, vals)| vals.len() > 1)
        .map(|(k, _)| k)
        .collect();
    keys.sort();
    keys
}

/// Cycle the sort column.  `delta` is `+1` for right, `-1` for
/// left.  Order: `Timestamp → Latency → Field[0] → Field[1] → …`.
pub fn cycle_sort(es: &mut ExploreState, fields: &[String], delta: isize) {
    let mut all: Vec<ExploreSortColumn> =
        vec![ExploreSortColumn::Timestamp, ExploreSortColumn::Latency];
    for f in fields {
        all.push(ExploreSortColumn::Field(f.clone()));
    }
    if all.is_empty() {
        return;
    }
    let idx = all.iter().position(|c| c == &es.sort).unwrap_or(0);
    let n = all.len() as isize;
    let next = ((idx as isize + delta).rem_euclid(n)) as usize;
    es.sort = all[next].clone();
    // New column → reset to that column's natural direction.
    es.direction = es.sort.default_direction();
}

pub fn cycle_sort_left(es: &mut ExploreState, fields: &[String]) {
    cycle_sort(es, fields, -1);
}

pub fn cycle_sort_right(es: &mut ExploreState, fields: &[String]) {
    cycle_sort(es, fields, 1);
}

/// Walk up the parent chain from `span_id` to find the trace
/// root — the first ancestor with `parent_id == None`.  Cap the
/// walk at 256 steps to bound depth even on pathological inputs.
pub fn find_root_id(model: &Model, span_id: u64) -> Option<u64> {
    let mut cur = span_id;
    for _ in 0..256 {
        let s = model.agg.span_by_id(cur)?;
        match s.parent_id {
            None => return Some(cur),
            Some(p) => cur = p,
        }
    }
    None
}
