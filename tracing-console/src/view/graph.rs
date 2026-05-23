//! Graph view: chart + columnar series-toggle legend.  Driven by
//! `Model::view == ViewMode::Graph(_)`.

use chrono::{DateTime, Local, Utc};
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span as TuiSpan};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph};

use crate::model::{
    AggMode, GraphFocus, GraphState, Metric, Model, SeriesProjection, SeriesSummary, SortColumn,
    TimeLabels,
};

use super::header::{focused_border_style, render_header_row};

/// Same palette as the rest of the TUI, rotated round-robin per
/// series so each line stays a stable colour across renders.
const SERIES_PALETTE: &[Color] = &[
    Color::Cyan,
    Color::Magenta,
    Color::Yellow,
    Color::Green,
    Color::Red,
    Color::LightBlue,
    Color::LightGreen,
    Color::LightYellow,
];

fn series_color(idx: usize, colorize: bool) -> Color {
    if !colorize {
        Color::White
    } else {
        SERIES_PALETTE[idx % SERIES_PALETTE.len()]
    }
}

/// Deterministic palette-slot assignment for the current series
/// set.  Each series asks for its hash-preferred slot
/// (`gs.color_index_of` mod palette length) and a linear probe
/// resolves collisions — alphabetical order decides who wins.
///
/// **Invariant**: when the series count is ≤ palette length, no
/// two series share a slot.  Returning a `HashMap` keyed by the
/// series identity so both the chart loop and the legend table
/// can look up the same slot for the same key.
fn assign_series_colors(
    gs: &GraphState,
) -> std::collections::HashMap<Vec<(String, String)>, usize> {
    let palette = SERIES_PALETTE.len();
    let mut occupied = vec![false; palette];
    let mut assignment = std::collections::HashMap::new();
    for key in gs.alpha_series_keys() {
        let preferred = gs.color_index_of(&key) % palette;
        let mut slot = preferred;
        for _ in 0..palette {
            if !occupied[slot] {
                break;
            }
            slot = (slot + 1) % palette;
        }
        occupied[slot] = true;
        assignment.insert(key, slot);
    }
    assignment
}

fn agg_label(mode: AggMode) -> String {
    match mode {
        AggMode::Min => "min".into(),
        AggMode::Max => "max".into(),
        AggMode::Avg => "avg".into(),
        AggMode::Percentile(p) => {
            if (p.round() - p).abs() < 1e-6 {
                format!("p{:.0}", p)
            } else {
                format!("p{p}")
            }
        }
    }
}

fn metric_label(metric: Metric) -> &'static str {
    match metric {
        Metric::Total => "total",
        Metric::SelfTime => "self",
    }
}

fn series_legend(key: &[(String, String)]) -> String {
    if key.is_empty() {
        "(all)".into()
    } else {
        key.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Pick a "nice" axis step for a span of `span_secs`.  Returns
/// the step (in seconds) and labels positioned at multiples of
/// the step, ending at `now` (= 0).
fn wall_clock_labels(
    span_secs: f64,
    mode: TimeLabels,
    now: DateTime<Utc>,
) -> Vec<ratatui::text::Span<'static>> {
    let (step, n) = wall_clock_label_step(span_secs);
    let mut out = Vec::with_capacity(n + 1);
    for i in 0..=n {
        let t = i as f64 * step;
        out.push(ratatui::text::Span::raw(format_axis_label(t, mode, now)));
    }
    out.reverse();
    out
}

/// Render one X-axis tick label.  `secs_ago` is how far back from
/// `now` the tick sits; modes either format it as a duration
/// (`Delta`) or as a wall-clock time (`Unix` UTC, `Local`).
fn format_axis_label(secs_ago: f64, mode: TimeLabels, now: DateTime<Utc>) -> String {
    match mode {
        TimeLabels::Delta => {
            if secs_ago < 1e-9 {
                // Right-edge tick reads as a number (`0s`) rather
                // than the special word `now` so the axis labels
                // are uniformly numeric across the whole row.
                "0s".to_string()
            } else {
                format!("-{}", format_seconds(secs_ago))
            }
        }
        TimeLabels::Unix => {
            let instant = now - chrono::Duration::nanoseconds((secs_ago * 1e9) as i64);
            instant.format("%H:%M:%SZ").to_string()
        }
        TimeLabels::Local => {
            let instant: DateTime<Local> =
                (now - chrono::Duration::nanoseconds((secs_ago * 1e9) as i64)).into();
            instant.format("%H:%M:%S").to_string()
        }
    }
}

/// Right-aligned hint row shown on the **expanded** graph details
/// border.  Each entry writes the full action name (`agg`,
/// `window`, `lookback`, `metric`, `unix`) with the shortcut
/// letter underlined, and follows with the current value.
/// Separator is a dim `│`.
///
/// In the minimized (chart-focused) state the same fields already
/// appear in the compact status row, so this hint row is omitted
/// there — see [`render_graph_details`].
fn graph_details_hints(gs: &GraphState) -> Line<'static> {
    let sep = TuiSpan::styled(" │ ", Style::default().add_modifier(Modifier::DIM));
    let mut spans: Vec<TuiSpan<'static>> = vec![TuiSpan::raw(" ")];

    let fields: [Vec<TuiSpan<'static>>; 5] = [
        labeled_modal_field("a", "gg", &gs.agg_input, agg_label(gs.aggregation)),
        labeled_modal_field(
            "w",
            "indow",
            &gs.window_input,
            format!("{:.2}s", gs.window_secs),
        ),
        labeled_modal_field(
            "l",
            "ookback",
            &gs.lookback_input,
            format_lookback(gs.lookback_secs),
        ),
        labeled_toggle_field("m", "etric", metric_label(gs.metric).to_string()),
        labeled_toggle_field("u", "nix", time_labels_label(gs.time_labels).to_string()),
    ];
    for (i, group) in fields.into_iter().enumerate() {
        if i > 0 {
            spans.push(sep.clone());
        }
        spans.extend(group);
    }
    spans.push(TuiSpan::raw(" "));
    Line::from(spans)
}

fn time_labels_label(mode: TimeLabels) -> &'static str {
    match mode {
        TimeLabels::Delta => "delta",
        TimeLabels::Unix => "unix",
        TimeLabels::Local => "local",
    }
}

/// Pick a `(step, n)` such that `n * step == span_secs` exactly and
/// `n` falls in `[3, 8]`, preferring `n ≈ 4`.  The product
/// equality is load-bearing: ratatui distributes axis labels
/// evenly across the bounds, so if the labels' stated values
/// don't span the bounds exactly, the leftmost label reads
/// something smaller than the actual leftmost edge (the
/// regression that made "lookback 2m" display as "-1.7m").
///
/// Falls back to `(span_secs / 4, 4)` when no nice round step
/// divides `span_secs` — labels won't be at round numbers but
/// they'll line up with the axis correctly.
pub(super) fn wall_clock_label_step(span_secs: f64) -> (f64, usize) {
    const NICE_STEPS: &[f64] = &[
        0.01, 0.02, 0.05, 0.1, 0.2, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0, 120.0,
        300.0, 600.0, 900.0, 1800.0, 3600.0,
    ];
    const TARGET_N: f64 = 4.0;
    const MIN_N: usize = 3;
    const MAX_N: usize = 8;

    let mut best: Option<(f64, usize)> = None;
    for &step in NICE_STEPS {
        if step > span_secs {
            break;
        }
        let ratio = span_secs / step;
        let n_round = ratio.round();
        if !(MIN_N as f64..=MAX_N as f64).contains(&n_round) {
            continue;
        }
        // Step must divide span_secs exactly (within float noise)
        // — otherwise n*step ≠ span_secs and the leftmost label
        // gets placed at the wrong axis position.
        let tol = 1e-6 * span_secs.max(1.0);
        if (ratio - n_round).abs() > tol {
            continue;
        }
        let n = n_round as usize;
        let score = (n_round - TARGET_N).abs();
        let better = best.is_none_or(|(_, prev_n)| (prev_n as f64 - TARGET_N).abs() > score);
        if better {
            best = Some((step, n));
        }
    }
    best.unwrap_or_else(|| (span_secs / TARGET_N, TARGET_N as usize))
}

fn format_seconds(s: f64) -> String {
    if s >= 60.0 {
        let m = (s / 60.0).round();
        if (m * 60.0 - s).abs() < 0.5 {
            format!("{m:.0}m")
        } else {
            format!("{:.1}m", s / 60.0)
        }
    } else if s >= 1.0 {
        if (s.round() - s).abs() < 0.05 {
            format!("{s:.0}s")
        } else {
            format!("{s:.1}s")
        }
    } else {
        format!("{}ms", (s * 1000.0).round() as u64)
    }
}

fn ns_axis_labels(y_max: f64) -> Vec<ratatui::text::Span<'static>> {
    let n_ticks = 4;
    let step = if y_max <= 0.0 {
        1.0
    } else {
        y_max / n_ticks as f64
    };
    (0..=n_ticks)
        .map(|i| {
            let v = (i as f64) * step;
            ratatui::text::Span::raw(crate::aggregate::fmt_ns(v as u64))
        })
        .collect()
}

pub(super) fn render_graph(
    f: &mut ratatui::Frame<'_>,
    area: ratatui::layout::Rect,
    model: &Model,
    gs: &GraphState,
    colorize: bool,
) {
    let (chart_c, details_c) = match gs.focus {
        GraphFocus::Chart => (Constraint::Min(8), Constraint::Length(12)),
        GraphFocus::Details => (Constraint::Length(8), Constraint::Min(12)),
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), chart_c, details_c])
        .split(area);

    // Header: same connection/level/chance line as the table view.
    render_header_row(f, chunks[0], model);

    // Chart pane.  Project the series store into ratatui datasets;
    // x_max is "this many seconds of history we're willing to
    // show" — driven by the `l` lookback knob and clamped to be
    // at least one bin wide.
    let x_max_secs = gs.lookback_secs.max(gs.window_secs);
    let projections = gs.store.project(gs.aggregation, x_max_secs);
    // Collision-avoiding palette assignment: when ≤ palette
    // length series exist, every one is guaranteed a unique
    // colour.  Both the chart and the legend pull from the same
    // map so a line's hue matches its checkbox row.
    let color_slots = assign_series_colors(gs);
    let series: Vec<(SeriesProjection, String, Color)> = projections
        .into_iter()
        .filter_map(|p| {
            if gs.hidden_series.contains(&p.key) {
                None
            } else {
                let label = series_legend(&p.key);
                let slot = color_slots.get(&p.key).copied().unwrap_or(0);
                let color = series_color(slot, colorize);
                Some((p, label, color))
            }
        })
        .collect();
    let datasets: Vec<Dataset<'_>> = series
        .iter()
        .map(|(proj, label, color)| {
            Dataset::default()
                .name(label.as_str())
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(*color))
                .data(proj.points.as_slice())
        })
        .collect();
    let y_max = series
        .iter()
        .flat_map(|(p, ..)| p.points.iter().map(|(_, y)| *y))
        .fold(0.0_f64, f64::max);

    // Verbose title for screenshot-friendliness: stack path, the
    // active aggregation, metric, window, lookback, split keys (if
    // any), and the number of currently-visible series.
    let visible_series = series.len();
    let title = {
        let mut parts: Vec<String> = vec![format!(
            " {label} — {agg} {metric} · window {win:.2}s · lookback {lookback}",
            label = gs.locked_stack.join(" › "),
            agg = agg_label(gs.aggregation),
            metric = metric_label(gs.metric),
            win = gs.window_secs,
            lookback = format_lookback(gs.lookback_secs),
        )];
        if !gs.split_keys.is_empty() {
            let keys: Vec<&str> = gs.split_keys.iter().map(String::as_str).collect();
            parts.push(format!(
                "split by {} ({} series)",
                keys.join(", "),
                visible_series,
            ));
        } else {
            parts.push(format!("{visible_series} series"));
        }
        format!("{} ", parts.join(" · "))
    };

    let now = Utc::now();
    let x_axis = Axis::default()
        .style(Style::default().add_modifier(Modifier::DIM))
        .bounds([-x_max_secs, 0.0])
        .labels(wall_clock_labels(x_max_secs, gs.time_labels, now));
    let y_axis = Axis::default()
        .style(Style::default().add_modifier(Modifier::DIM))
        .bounds([0.0, y_max.max(1.0)])
        .labels(ns_axis_labels(y_max.max(1.0)));

    let chart_focused = gs.focus == GraphFocus::Chart;
    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(focused_border_style(chart_focused)),
        )
        .x_axis(x_axis)
        .y_axis(y_axis);
    f.render_widget(chart, chunks[1]);

    render_graph_details(f, chunks[2], model, gs, colorize, &color_slots);
}

// Header rendering lives in `super::header` — both views share it.

/// Shared "modal value cell" renderer used by every detail-pane
/// field that has an inline-edit modal (agg / window / lookback /
/// chance).  Emits the white-bg + reversed-cursor input box when
/// `buf.is_some()`, otherwise returns the idle-state span as
/// produced by `idle()`.  Returning a `Vec<TuiSpan>` so callers
/// can splice it into a larger line.
fn modal_value_spans<F>(buf: &Option<String>, idle: F) -> Vec<TuiSpan<'static>>
where
    F: FnOnce() -> TuiSpan<'static>,
{
    match buf {
        Some(buf) => {
            let body = if buf.is_empty() {
                " ".to_string()
            } else {
                buf.clone()
            };
            let base = Style::default()
                .bg(Color::White)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD);
            vec![
                TuiSpan::styled(body, base),
                TuiSpan::styled("_", base.add_modifier(Modifier::REVERSED)),
            ]
        }
        None => vec![idle()],
    }
}

/// Render `<u>letter</u>rest value` with the shortcut letter
/// underlined.  When the modal `buf` is open (`Some`), the *whole*
/// label gets the same white-bg highlight as the input box, so
/// the active region reads as one contiguous "you're typing here"
/// block — the visual cue we promised when the letter was pressed.
fn labeled_modal_field(
    letter: &'static str,
    rest: &'static str,
    buf: &Option<String>,
    idle_value: String,
) -> Vec<TuiSpan<'static>> {
    let active = buf.is_some();
    let label_style = if active {
        Style::default()
            .bg(Color::White)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let mut spans = vec![
        TuiSpan::styled(letter, label_style.add_modifier(Modifier::UNDERLINED)),
        TuiSpan::styled(rest, label_style),
        TuiSpan::raw(" "),
    ];
    spans.extend(modal_value_spans(buf, move || TuiSpan::raw(idle_value)));
    spans
}

/// Render `<u>letter</u>rest value` for keys that *toggle* state
/// rather than open a modal (metric, time-labels mode).  No
/// active-input highlight because there's no input mode to enter.
fn labeled_toggle_field(
    letter: &'static str,
    rest: &'static str,
    value: String,
) -> Vec<TuiSpan<'static>> {
    vec![
        TuiSpan::styled(letter, Style::default().add_modifier(Modifier::UNDERLINED)),
        TuiSpan::raw(rest),
        TuiSpan::raw(" "),
        TuiSpan::raw(value),
    ]
}

/// Render the "agg:   …" detail-pane row.  When the input modal
/// is active the value cell is shown as a white-on-default
/// highlighted input box with a trailing cursor; otherwise the
/// current aggregation label plus a short hint.
/// Detail-pane label width — all rows pad to this column so
/// values align vertically.  `lookback` is the widest word (8) +
/// 2-space gap → 10.
const LABEL_W: usize = 10;

fn format_lookback(secs: f64) -> String {
    if secs >= 60.0 {
        format!("{:.2}m", secs / 60.0)
    } else {
        format!("{:.2}s", secs)
    }
}

/// Build the columnar series-toggle table shown in both the
/// compact and expanded details pane.  Returns the rendered
/// lines and, when `cursor_series_idx` points at a series row,
/// the absolute line index within the returned vec so the
/// caller can drive `Paragraph::scroll` to keep the cursor in
/// view.
/// Build the columnar series-toggle table.  Returns
/// `(header_line, body_rows, body_cursor_index)`.  The header
/// is `None` only when there are no series — callers then
/// surface the placeholder message that lives in `body_rows`.
/// The cursor index is into `body_rows`, so callers can offset
/// it against whatever they prepend.
fn series_table_lines(
    gs: &GraphState,
    cursor_series_idx: Option<usize>,
    colorize: bool,
    color_slots: &std::collections::HashMap<Vec<(String, String)>, usize>,
) -> (Option<Line<'static>>, Vec<Line<'static>>, Option<usize>) {
    use std::fmt::Write;

    let series_keys = gs.series_keys();
    if series_keys.is_empty() {
        return (None, vec![Line::from("  (no series yet)")], None);
    }

    let summaries = gs.store.series_summary(gs.aggregation);
    let summary_by_key: std::collections::HashMap<Vec<(String, String)>, SeriesSummary> =
        summaries.into_iter().map(|s| (s.key.clone(), s)).collect();

    let split_cols: Vec<String> = gs.split_keys.iter().cloned().collect();
    let mut split_widths: Vec<usize> = split_cols.iter().map(|k| k.chars().count()).collect();

    // Colour slot per series comes from the chart's already-
    // computed assignment map, so legend and chart line agree
    // even with the collision-avoiding probe.
    let color_idx_of =
        |key: &[(String, String)]| -> usize { color_slots.get(key).copied().unwrap_or(0) };

    struct Row {
        color_idx: usize,
        visible: bool,
        split_vals: Vec<String>,
        n: String,
        min: String,
        avg: String,
        max: String,
        last: String,
    }
    let mut rows: Vec<Row> = Vec::with_capacity(series_keys.len());
    for key in &series_keys {
        let s = summary_by_key.get(key);
        let split_vals: Vec<String> = split_cols
            .iter()
            .map(|sk| {
                key.iter()
                    .find(|(k, _)| k == sk)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_else(|| "—".to_string())
            })
            .collect();
        for (i, v) in split_vals.iter().enumerate() {
            split_widths[i] = split_widths[i].max(v.chars().count());
        }
        let n = s.map(|s| s.count.to_string()).unwrap_or_else(|| "0".into());
        let min = crate::aggregate::fmt_ns(s.map(|s| s.min_ns).unwrap_or(0));
        let avg = crate::aggregate::fmt_ns(s.map(|s| s.avg_ns).unwrap_or(0));
        let max = crate::aggregate::fmt_ns(s.map(|s| s.max_ns).unwrap_or(0));
        let last = crate::aggregate::fmt_ns(s.map(|s| s.last_ns).unwrap_or(0));
        rows.push(Row {
            color_idx: color_idx_of(key),
            visible: !gs.hidden_series.contains(key),
            split_vals,
            n,
            min,
            avg,
            max,
            last,
        });
    }

    let stat_headers = ["n", "min", "avg", "max", "last"];
    let stat_columns = [
        SortColumn::Count,
        SortColumn::Min,
        SortColumn::Avg,
        SortColumn::Max,
        SortColumn::Last,
    ];
    // Reserve enough width for the worst-case `fmt_ns` formatting at
    // any single scale — `XXX.Xms` / `XXX.Xµs` is 7 chars — so the
    // time columns don't flap when values waffle between tens and
    // hundreds.  `n` (count) stays purely content-driven.
    const TIME_COL_MIN: usize = 7;
    let mut stat_widths: [usize; 5] = [
        stat_headers[0].len(),
        stat_headers[1].len().max(TIME_COL_MIN),
        stat_headers[2].len().max(TIME_COL_MIN),
        stat_headers[3].len().max(TIME_COL_MIN),
        stat_headers[4].len().max(TIME_COL_MIN),
    ];
    for r in &rows {
        stat_widths[0] = stat_widths[0].max(r.n.chars().count());
        stat_widths[1] = stat_widths[1].max(r.min.chars().count());
        stat_widths[2] = stat_widths[2].max(r.avg.chars().count());
        stat_widths[3] = stat_widths[3].max(r.max.chars().count());
        stat_widths[4] = stat_widths[4].max(r.last.chars().count());
    }

    // Header — one span per cell so we can selectively underline
    // the active sort column while keeping the rest dim.
    let dim = Style::default().add_modifier(Modifier::DIM);
    let underline = Style::default().add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
    let mut header_spans: Vec<TuiSpan<'static>> = vec![TuiSpan::styled("      ", dim)];
    for (i, c) in split_cols.iter().enumerate() {
        let cell = format!("{:<w$}", c, w = split_widths[i]);
        let is_active = matches!(&gs.sort_column, SortColumn::SplitKey(k) if k == c);
        let style = if is_active { underline } else { dim };
        header_spans.push(TuiSpan::styled(cell, style));
        header_spans.push(TuiSpan::styled("  ", dim));
    }
    for (i, h) in stat_headers.iter().enumerate() {
        let cell = format!("{:>w$}", h, w = stat_widths[i]);
        let is_active = gs.sort_column == stat_columns[i];
        let style = if is_active { underline } else { dim };
        header_spans.push(TuiSpan::styled(cell, style));
        if i + 1 < stat_headers.len() {
            header_spans.push(TuiSpan::styled("  ", dim));
        }
    }
    let header_line = Line::from(header_spans);

    // Data rows go into a separate vec so the caller can keep
    // the header sticky while only the rows scroll.
    let mut body_rows: Vec<Line<'static>> = Vec::with_capacity(rows.len());
    let mut cursor_line: Option<usize> = None;
    for (i, r) in rows.iter().enumerate() {
        let mark = if r.visible { "[✓]" } else { "[ ]" };
        let mut row = format!("  {mark} ");
        for (j, v) in r.split_vals.iter().enumerate() {
            let _ = write!(row, "{:<w$}  ", v, w = split_widths[j]);
        }
        let _ = write!(row, "{:>w$}  ", r.n, w = stat_widths[0]);
        let _ = write!(row, "{:>w$}  ", r.min, w = stat_widths[1]);
        let _ = write!(row, "{:>w$}  ", r.avg, w = stat_widths[2]);
        let _ = write!(row, "{:>w$}  ", r.max, w = stat_widths[3]);
        let _ = write!(row, "{:>w$}", r.last, w = stat_widths[4]);

        let color = series_color(r.color_idx, colorize);
        let on_cursor = cursor_series_idx == Some(i);
        if on_cursor {
            cursor_line = Some(body_rows.len());
        }
        let mut style = Style::default().fg(color);
        if !r.visible {
            style = style.add_modifier(Modifier::DIM);
        }
        if on_cursor {
            style = style.add_modifier(Modifier::REVERSED);
        }
        body_rows.push(Line::from(TuiSpan::styled(row, style)));
    }

    (Some(header_line), body_rows, cursor_line)
}

fn render_graph_details(
    f: &mut ratatui::Frame<'_>,
    area: ratatui::layout::Rect,
    model: &Model,
    gs: &GraphState,
    colorize: bool,
    color_slots: &std::collections::HashMap<Vec<(String, String)>, usize>,
) {
    let focused = gs.focus == GraphFocus::Details;
    let title = format!(" graph details{} ", if focused { " ◆" } else { "" });

    // Sticky lines stay pinned at the top of the pane; body
    // lines scroll beneath them so the agg / column-header rows
    // remain visible even as the user scrolls through a long
    // series list.
    let mut sticky: Vec<Line<'static>> = Vec::new();
    let mut body: Vec<Line<'static>> = Vec::new();
    let mut body_cursor: Option<usize> = None;

    if focused {
        // Sticky: the locked stack + active splits (the only bits
        // not already on the border hint row), then the series
        // help line and table header.  awltu state lives on the
        // border so we don't repeat it here.
        sticky.push(Line::from(format!(
            "{:<w$}{stack}",
            "stack",
            w = LABEL_W,
            stack = gs.locked_stack.join(" › "),
        )));
        if !gs.split_keys.is_empty() {
            sticky.push(Line::from(format!(
                "{:<w$}{}",
                "splits",
                gs.split_keys.iter().cloned().collect::<Vec<_>>().join(", "),
                w = LABEL_W,
            )));
        }
        sticky.push(Line::from(""));
        sticky.push(Line::from(
            "series  (Space toggles visibility; ←/→ change sort column):",
        ));

        let series_keys = gs.series_keys();
        let candidates = crate::aggregate::candidate_split_keys_for(&model.agg, &gs.locked_stack);
        let combined_len = series_keys.len() + candidates.len();
        let sel = if combined_len == 0 {
            usize::MAX
        } else {
            gs.details_selected.min(combined_len - 1)
        };
        let series_cursor = if sel != usize::MAX && sel < series_keys.len() {
            Some(sel)
        } else {
            None
        };

        // Table.  Header goes into sticky; data rows + the
        // metadata-keys section go into body.
        let (table_header, table_rows, table_cursor) =
            series_table_lines(gs, series_cursor, colorize, color_slots);
        if let Some(h) = table_header {
            sticky.push(h);
        }
        let body_start = body.len();
        body.extend(table_rows);
        if let Some(rel) = table_cursor {
            body_cursor = Some(body_start + rel);
        }
        body.push(Line::from(""));
        body.push(Line::from(
            "metadata keys  (Space splits/unsplits, Tab to leave):",
        ));
        if candidates.is_empty() {
            body.push(Line::from("  (no metadata keys present on matching spans)"));
        } else {
            let series_count = series_keys.len();
            for (i, k) in candidates.iter().enumerate() {
                let checked = gs.split_keys.contains(k);
                let mark = if checked { "[✓]" } else { "[ ]" };
                let line_text = format!("  {mark} {k}");
                let combined_idx = series_count + i;
                let on_cursor = combined_idx == sel;
                if on_cursor {
                    body_cursor = Some(body.len());
                }
                let style = if on_cursor {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                body.push(Line::from(TuiSpan::styled(line_text, style)));
            }
        }
    } else {
        // Compact: the awltu state lives on the border hint row;
        // no need to repeat it inline.  Series body fills the pane.
        let series_count = gs.series_keys().len();
        let cursor_idx = if series_count == 0 {
            None
        } else {
            Some(gs.details_selected.min(series_count - 1))
        };
        let (table_header, table_rows, table_cursor) =
            series_table_lines(gs, cursor_idx, colorize, color_slots);
        if let Some(h) = table_header {
            sticky.push(h);
        }
        let body_start = body.len();
        body.extend(table_rows);
        if let Some(rel) = table_cursor {
            body_cursor = Some(body_start + rel);
        }
    }

    // Draw the outer block first; subsequent paragraphs draw
    // inside its inner rect.  Right-side title carries the
    // keyboard shortcuts + their current values, separated by
    // dim `│` glyphs — visible in both minimized and expanded
    // states so the user can always read the active config off
    // the pane chrome.  Bottom-of-block title surfaces the
    // modal-help bar when an input is open, on the border itself
    // so the surrounding panes don't re-layout.
    let mut block = Block::default()
        .title(title)
        .title(graph_details_hints(gs).alignment(Alignment::Right))
        .borders(Borders::ALL)
        .border_style(focused_border_style(focused));
    if let Some(help) = crate::view::header::modal_status_bar(model) {
        block = block.title_bottom(help.right_aligned());
    }
    let inner = block.inner(area);
    f.render_widget(block, area);

    // Vertical split: sticky on top, body fills the rest.  Cap
    // the sticky region so we never starve the body entirely on
    // tiny panes.
    let sticky_h = sticky.len().min(inner.height as usize).min(
        // Reserve at least one line for the body when both can fit;
        // otherwise let the sticky take everything.
        inner.height as usize,
    ) as u16;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(sticky_h), Constraint::Min(0)])
        .split(inner);

    f.render_widget(Paragraph::new(sticky), chunks[0]);

    if chunks[1].height > 0 {
        let body_h = chunks[1].height as usize;
        let total_body = body.len();
        let scroll = match body_cursor {
            Some(line) if total_body > body_h && body_h > 0 => {
                let half = body_h / 2;
                let max_scroll = total_body - body_h;
                line.saturating_sub(half).min(max_scroll) as u16
            }
            _ => 0,
        };
        f.render_widget(Paragraph::new(body).scroll((scroll, 0)), chunks[1]);
    }
}

#[cfg(test)]
mod tests {
    use super::{format_axis_label, graph_details_hints, wall_clock_label_step};
    use crate::model::GraphState;
    use crate::model::TimeLabels;
    use chrono::{DateTime, TimeZone, Utc};

    fn fixed_now() -> DateTime<Utc> {
        // 2026-05-19 14:37:58 UTC — matches the example in the
        // user-facing description so tests document the format.
        Utc.with_ymd_and_hms(2026, 5, 19, 14, 37, 58).unwrap()
    }

    /// Direct values from the user-reported regression:
    /// "with window 0.25s, lookback 2m shows -1.7m, 3m → -2.5m,
    /// 4m → -3.3m".  Each of these must produce a step that
    /// divides span_secs exactly, otherwise ratatui places the
    /// leftmost label at the wrong axis position.
    #[test]
    fn wall_clock_label_step_evenly_divides_user_lookback_inputs() {
        for &span in &[60.0_f64, 120.0, 180.0, 240.0, 300.0, 600.0] {
            let (step, n) = wall_clock_label_step(span);
            assert!(
                (step * n as f64 - span).abs() < 1e-6,
                "step={step} n={n} span={span}: step*n must equal span",
            );
            assert!((3..=8).contains(&n), "n={n} out of [3,8] for span={span}",);
        }
    }

    #[test]
    fn wall_clock_label_step_picks_nice_minutes_for_minute_spans() {
        // The specific outputs the user expects to see.
        assert_eq!(wall_clock_label_step(60.0), (15.0, 4));
        assert_eq!(wall_clock_label_step(120.0), (30.0, 4));
        assert_eq!(wall_clock_label_step(180.0), (60.0, 3));
        assert_eq!(wall_clock_label_step(240.0), (60.0, 4));
        assert_eq!(wall_clock_label_step(300.0), (60.0, 5));
        assert_eq!(wall_clock_label_step(600.0), (120.0, 5));
    }

    #[test]
    fn wall_clock_label_step_handles_sub_second_spans() {
        // Window=0.25, lookback at floor — same correctness rule.
        let (step, n) = wall_clock_label_step(0.25);
        assert!((step * n as f64 - 0.25).abs() < 1e-9);
        assert!((3..=8).contains(&n));

        let (step, n) = wall_clock_label_step(1.0);
        assert!((step * n as f64 - 1.0).abs() < 1e-9);
        assert!((3..=8).contains(&n));
    }

    #[test]
    fn wall_clock_label_step_fallback_when_no_nice_divisor() {
        // A span that no nice step divides exactly (67s is prime
        // among the candidates) — the fallback must still satisfy
        // n*step == span so labels line up with the axis.
        let span = 67.0_f64;
        let (step, n) = wall_clock_label_step(span);
        assert!((step * n as f64 - span).abs() < 1e-6);
    }

    #[test]
    fn format_axis_label_delta_mode_matches_legacy_format() {
        let now = fixed_now();
        assert_eq!(format_axis_label(0.0, TimeLabels::Delta, now), "0s");
        assert_eq!(format_axis_label(30.0, TimeLabels::Delta, now), "-30s");
        assert_eq!(format_axis_label(60.0, TimeLabels::Delta, now), "-1m");
    }

    #[test]
    fn format_axis_label_unix_mode_emits_utc_with_z_suffix() {
        let now = fixed_now();
        // The "now" tick (secs_ago = 0) is the literal current time.
        assert_eq!(format_axis_label(0.0, TimeLabels::Unix, now), "14:37:58Z");
        // 30s and 1m earlier — pure subtraction, no tz involvement.
        assert_eq!(format_axis_label(30.0, TimeLabels::Unix, now), "14:37:28Z");
        assert_eq!(format_axis_label(60.0, TimeLabels::Unix, now), "14:36:58Z");
        // Span backward across the minute boundary.
        assert_eq!(format_axis_label(120.0, TimeLabels::Unix, now), "14:35:58Z",);
    }

    #[test]
    fn format_axis_label_local_mode_omits_z_suffix() {
        // The Local clock depends on the host's tz; we can't assert
        // the literal HH:MM:SS without controlling tz.  What we can
        // assert: the string has no Z suffix (distinguishing it from
        // Unix mode) and matches an HH:MM:SS shape.
        let now = fixed_now();
        let s = format_axis_label(0.0, TimeLabels::Local, now);
        assert!(!s.ends_with('Z'), "Local mode must drop the Z suffix: {s}");
        assert_eq!(s.len(), 8, "expected HH:MM:SS, got {s}");
        let mut chars = s.chars();
        // HH:MM:SS layout.
        for (i, c) in s.char_indices() {
            if i == 2 || i == 5 {
                assert_eq!(c, ':', "colon at {i}: {s}");
            } else {
                assert!(c.is_ascii_digit(), "digit at {i}: {s}");
            }
        }
        let _ = chars.next();
    }

    fn plain_line(line: ratatui::text::Line<'_>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn graph_details_hints_carry_the_active_time_mode() {
        let mut gs = GraphState::new(vec!["root".into()]);
        for mode in [TimeLabels::Delta, TimeLabels::Unix, TimeLabels::Local] {
            gs.time_labels = mode;
            let rendered = plain_line(graph_details_hints(&gs));
            // The `unix` label always appears as the toggle word.
            assert!(rendered.contains("unix"), "missing unix label: {rendered}");
            let expected_value = match mode {
                TimeLabels::Delta => "delta",
                TimeLabels::Unix => "unix",
                TimeLabels::Local => "local",
            };
            // The current mode value sits next to the unix label.
            assert!(
                rendered.contains(&format!("unix {expected_value}")),
                "expected `unix {expected_value}` in: {rendered}",
            );
        }
    }

    #[test]
    fn graph_details_hints_include_all_shortcut_words() {
        let gs = GraphState::new(vec!["root".into()]);
        let rendered = plain_line(graph_details_hints(&gs));
        // Every keybinding's full word appears.  The shortcut
        // letter is the first character of each (underlined in
        // the rendered output — invisible to plain_line, but the
        // word presence is what users see).
        for word in ["agg", "window", "lookback", "metric", "unix"] {
            assert!(
                rendered.contains(word),
                "missing word {word:?} in: {rendered}",
            );
        }
    }

    #[test]
    fn graph_details_hints_use_m_for_metric_not_t() {
        // Regression: after the t→m rebinding, the hints must not
        // surface a `t metric` or any other `t-` style.
        let gs = GraphState::new(vec!["root".into()]);
        let rendered = plain_line(graph_details_hints(&gs));
        assert!(rendered.contains("metric"));
        assert!(
            !rendered.contains("t metric"),
            "stale `t metric` in: {rendered}",
        );
    }

    #[test]
    fn assign_series_colors_gives_distinct_slots_when_below_palette_size() {
        // Regression: hashing alone collided even with 3 series.
        // The probing assignment must spread up-to-palette-length
        // series across distinct slots.
        use crate::model::{Model, Update, ViewMode};
        use tracing_console_host::{WireFieldValue, WireLevel, WireSpan};

        fn span_with_api(id: u64, api: &str) -> WireSpan {
            WireSpan {
                id,
                parent_id: None,
                name: "req".into(),
                target: "test".into(),
                level: WireLevel::Info,
                fields: vec![("api".into(), WireFieldValue::Str(api.into()))],
                events: vec![],
                opened_at_ns: 0,
                closed_at_ns: Some(100),
            }
        }

        let mut m = Model::new(16);
        m.apply(Update::SpanReceived(span_with_api(10, "fetch")));
        m.apply(Update::ToggleGraph);
        // Enable the api split so each value becomes its own series.
        m.apply(Update::GraphSwitchFocus);
        // Advance the cursor to the first split-key row and toggle.
        // `series_keys().len()` is the cursor offset for the first
        // candidate row in the combined Details cursor.
        let series_offset = match &m.view {
            ViewMode::Graph(gs) => gs.series_keys().len(),
            _ => panic!("expected graph mode"),
        };
        for _ in 0..series_offset {
            m.apply(Update::GraphSelectDown);
        }
        m.apply(Update::GraphToggleSplit);
        // Three distinct series.
        for (i, api) in ["fetch", "post", "delete"].iter().enumerate() {
            m.apply(Update::SpanReceived(span_with_api(20 + i as u64, api)));
        }
        let gs = match &m.view {
            ViewMode::Graph(gs) => gs,
            _ => panic!("expected graph mode"),
        };
        assert_eq!(gs.series_keys().len(), 3);
        let slots = super::assign_series_colors(gs);
        let values: std::collections::HashSet<usize> = slots.values().copied().collect();
        assert_eq!(
            values.len(),
            3,
            "expected 3 distinct slots for 3 series, got {slots:?}",
        );
        // All slots must be valid palette indices.
        for s in &values {
            assert!(*s < super::SERIES_PALETTE.len());
        }
    }
}
