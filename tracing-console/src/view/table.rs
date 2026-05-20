//! Table view: stacks + details two-pane layout.  Driven by
//! `Model::view == ViewMode::Table`.

use std::collections::BTreeMap;

use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use tracing_console_host::{WireLevel, WireSpan};

use crate::aggregate::fmt_ns;
use crate::model::{ConnectionStatus, Focus, Model};

use super::header::{
    GRAPH_HINT_WIDTH, chance_switcher_spans, format_span_rate, graph_toggle_hint,
    level_switcher_spans,
};

fn level_str(level: WireLevel) -> &'static str {
    match level {
        WireLevel::Trace => "T",
        WireLevel::Debug => "D",
        WireLevel::Info => "I",
        WireLevel::Warn => "W",
        WireLevel::Error => "E",
    }
}

// Solarized accent palette — used as a heat ramp from cool (low
// values) to hot (column max).  Pulled into the bin function so
// the colours live next to the thresholds that select them.
const SOL_CYAN: Color = Color::Rgb(0x2a, 0xa1, 0x98);
const SOL_GREEN: Color = Color::Rgb(0x85, 0x99, 0x00);
const SOL_YELLOW: Color = Color::Rgb(0xb5, 0x89, 0x00);
const SOL_ORANGE: Color = Color::Rgb(0xcb, 0x4b, 0x16);
const SOL_RED: Color = Color::Rgb(0xdc, 0x32, 0x2f);

/// Map a 0..1 intensity to a heat colour, or `None` for "leave the
/// terminal default".  Low intensities stay uncoloured so the eye
/// only catches the warm cells.
fn heat(intensity: f64) -> Option<Color> {
    if intensity < 0.40 {
        None
    } else if intensity < 0.60 {
        Some(SOL_CYAN)
    } else if intensity < 0.78 {
        Some(SOL_GREEN)
    } else if intensity < 0.90 {
        Some(SOL_YELLOW)
    } else if intensity < 0.99 {
        Some(SOL_ORANGE)
    } else {
        Some(SOL_RED)
    }
}

/// For each visible row's immediate-ancestor index in the same
/// list (`None` for roots).  Rows are in DFS order, so the
/// ancestor index for row `r` is the most recent earlier row
/// whose stack length is exactly `r.depth`.  Computed in one pass.
fn parent_indices(rows: &[crate::model::VisibleRow]) -> Vec<Option<usize>> {
    let mut parents = vec![None; rows.len()];
    // (depth, index) stack of in-scope ancestors as we DFS.
    let mut anc: Vec<(usize, usize)> = Vec::new();
    for (i, r) in rows.iter().enumerate() {
        while anc.last().map(|(d, _)| *d >= r.depth).unwrap_or(false) {
            anc.pop();
        }
        parents[i] = anc.last().map(|(_, idx)| *idx);
        anc.push((r.depth, i));
    }
    parents
}

/// Build the per-cell colour map: `colors[row][column]` for the
/// 7 numeric columns (count, total min/avg/max, self min/avg/max).
/// Each column is normalised against its own max; per-row effective
/// intensity is `min(self_intensity, parent_effective)` so a
/// descendant never reads as hotter than its ancestor — the
/// ancestor's own intensity caps the descendant.  Roots show
/// their own intensity unchanged.
fn build_color_map(rows: &[crate::model::VisibleRow]) -> Vec<[Option<Color>; 7]> {
    let n = rows.len();
    if n == 0 {
        return Vec::new();
    }
    // Pull values per column.
    let mut vals: [Vec<u64>; 7] = Default::default();
    for r in rows {
        vals[0].push(r.stats.count);
        vals[1].push(r.stats.total_min_ns);
        vals[2].push(r.stats.total_avg_ns());
        vals[3].push(r.stats.total_max_ns);
        vals[4].push(r.stats.self_min_ns);
        vals[5].push(r.stats.self_avg_ns());
        vals[6].push(r.stats.self_max_ns);
    }
    let max: [u64; 7] = std::array::from_fn(|c| *vals[c].iter().max().unwrap_or(&0));

    let parents = parent_indices(rows);
    let mut effective: Vec<[f64; 7]> = vec![[0.0; 7]; n];
    for i in 0..n {
        for c in 0..7 {
            let self_int = if max[c] == 0 {
                0.0
            } else {
                vals[c][i] as f64 / max[c] as f64
            };
            effective[i][c] = match parents[i] {
                Some(p) => self_int.min(effective[p][c]),
                None => self_int,
            };
        }
    }
    effective
        .into_iter()
        .map(|row| std::array::from_fn(|c| heat(row[c])))
        .collect()
}

pub(super) fn render_table(
    f: &mut ratatui::Frame<'_>,
    area: ratatui::layout::Rect,
    model: &Model,
    colorize: bool,
) {
    // Pane proportions swap based on focus: when Details is
    // focused it grabs the larger pane so the user can browse all
    // candidate split keys + full distinguishing-value lists with
    // room.  Header keeps its fixed 1-line slot.
    let (stacks_constraint, details_constraint) = match model.focus {
        Focus::Stacks => (Constraint::Min(8), Constraint::Length(10)),
        Focus::Details => (Constraint::Length(10), Constraint::Min(8)),
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), stacks_constraint, details_constraint])
        .split(area);

    // Top: connection status + cache-level switcher.
    //
    // When connected, the header reads:
    //   [connected]  Off Info Debug Trace   N spans buffered
    //
    // * The label matching `model.cache_level` is bold + green
    //   (the server-confirmed current level).  Until the server
    //   pushes its first `CacheLevel`, no label is highlighted.
    // * Each label's first letter is underlined as a hint for
    //   the Shift+letter shortcut that requests that level.  The
    //   green selection only moves when the server confirms.
    let header: Line = match &model.connection {
        ConnectionStatus::Connecting => Line::from(vec![
            TuiSpan::raw("[connecting] "),
            TuiSpan::styled(
                model.status.clone().unwrap_or_default(),
                Style::default().add_modifier(Modifier::DIM),
            ),
        ]),
        ConnectionStatus::Connected => {
            let mut spans: Vec<TuiSpan<'static>> =
                vec![TuiSpan::raw("[connected]  ")];
            level_switcher_spans(&mut spans, model);
            spans.push(TuiSpan::raw("  "));
            chance_switcher_spans(&mut spans, model);
            spans.push(TuiSpan::raw(format!(
                "   {n} spans / {rate}",
                n = model.agg.len(),
                rate = format_span_rate(model),
            )));
            Line::from(spans)
        }
        ConnectionStatus::Disconnected(reason) => {
            Line::from(format!("[disconnected] {reason}"))
        }
    };
    let header_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(GRAPH_HINT_WIDTH)])
        .split(chunks[0]);
    f.render_widget(Paragraph::new(header), header_chunks[0]);
    f.render_widget(
        Paragraph::new(graph_toggle_hint(model)).alignment(Alignment::Right),
        header_chunks[1],
    );

    // Middle: hierarchical aggregated tree as a Table so the
    // measurement columns sit at fixed positions regardless of
    // label width.  Same bucketing as `--stats`; depth-based
    // indentation + ▶/▼ markers in the label column.
    let rows = model.visible_rows();
    let selected = if rows.is_empty() {
        None
    } else {
        Some(model.selected.min(rows.len() - 1))
    };

    let dim = Style::default().add_modifier(Modifier::DIM);
    let right = |s: String, color: Option<Color>| {
        let mut style = Style::default();
        if let Some(c) = color {
            style = style.fg(c);
        }
        Cell::from(Line::from(s).alignment(Alignment::Right)).style(style)
    };
    let color_map = if colorize {
        build_color_map(&rows)
    } else {
        Vec::new()
    };
    let cell_color = |row_idx: usize, col: usize| -> Option<Color> {
        if !colorize {
            return None;
        }
        color_map.get(row_idx).and_then(|cs| cs[col])
    };
    // Soft visual hint between column groups — a dim "│" lives in
    // its own 1-wide column.  Eye sees the break without a hard
    // rule running the full table height.
    let sep_cell = || Cell::from(Line::from("│").alignment(Alignment::Center)).style(dim);
    // Two-line header cell.  Top line carries the section label
    // ("total" / "self") above the middle of its group; bottom
    // line carries the per-column label (min/avg/max etc).
    let hcell = |top: &'static str, bot: &'static str, align: Alignment| -> Cell<'static> {
        Cell::from(Text::from(vec![
            Line::from(top).alignment(align),
            Line::from(bot).alignment(align),
        ]))
    };

    let header = Row::new(vec![
        hcell("", "stack", Alignment::Left),
        hcell("", "n", Alignment::Right),
        hcell("", "│", Alignment::Center),
        hcell("", "min", Alignment::Right),
        // "total" lands above the middle (avg) column so it reads
        // as a label for the whole 3-column group.
        hcell("total", "avg", Alignment::Right),
        hcell("", "max", Alignment::Right),
        hcell("", "│", Alignment::Center),
        hcell("", "min", Alignment::Right),
        hcell("self", "avg", Alignment::Right),
        hcell("", "max", Alignment::Right),
    ])
    .height(2)
    .style(dim);

    let table_rows: Vec<Row> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            // ▶/▼ — standard tree disclosure pair, both 1-cell.
            let marker = if r.has_children {
                if r.is_expanded { "▼ " } else { "▶ " }
            } else {
                "  "
            };
            let indent = "  ".repeat(r.depth);
            let leaf = r.key.stack.last().map(String::as_str).unwrap_or("");
            // Splits annotate the row that *introduces* a new
            // splits-group, not every descendant.  Rows are sorted
            // by `(splits, stack)` so the first row of each group
            // (and only that row) carries the `[k=v, …]` suffix —
            // children inherit silently, matching the user's
            // mental model that the distinguishing key lives on
            // the span where it was actually set.
            let introduces_splits =
                !r.key.splits.is_empty() && (i == 0 || rows[i - 1].key.splits != r.key.splits);
            let splits_suffix = if introduces_splits {
                let parts: Vec<String> = r
                    .key
                    .splits
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                format!("  [{}]", parts.join(", "))
            } else {
                String::new()
            };
            let label = format!("{indent}{marker}{leaf}{splits_suffix}");
            Row::new(vec![
                Cell::from(label),
                right(r.stats.count.to_string(), cell_color(i, 0)),
                sep_cell(),
                right(fmt_ns(r.stats.total_min_ns), cell_color(i, 1)),
                right(fmt_ns(r.stats.total_avg_ns()), cell_color(i, 2)),
                right(fmt_ns(r.stats.total_max_ns), cell_color(i, 3)),
                sep_cell(),
                right(fmt_ns(r.stats.self_min_ns), cell_color(i, 4)),
                right(fmt_ns(r.stats.self_avg_ns()), cell_color(i, 5)),
                right(fmt_ns(r.stats.self_max_ns), cell_color(i, 6)),
            ])
        })
        .collect();

    let stacks_focused = model.focus == Focus::Stacks;
    let title = format!(
        " stacks{focus_marker}  ({n}) ",
        focus_marker = if stacks_focused { " ◆" } else { "" },
        n = rows.len(),
    );
    let table = Table::new(
        table_rows,
        [
            Constraint::Min(20),   // stack label — takes remaining width
            Constraint::Length(7), // n
            Constraint::Length(1), // sep
            Constraint::Length(8), // tot min
            Constraint::Length(8), // tot avg
            Constraint::Length(8), // tot max
            Constraint::Length(1), // sep
            Constraint::Length(8), // self min
            Constraint::Length(8), // self avg
            Constraint::Length(8), // self max
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(Block::default().title(title).borders(Borders::ALL))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = TableState::default();
    state.select(selected);
    f.render_stateful_widget(table, chunks[1], &mut state);

    // Bottom: details pane.  When Stacks is focused, it shows a
    // compact summary (stack path, distinguishing/constant fields,
    // event bucket totals).  When Details is focused, the pane
    // grows and renders the candidate-split-keys list; the user
    // navigates it with j/k and toggles a key in/out of
    // `split_keys` with Space.
    let details_focused = model.focus == Focus::Details;
    let details_title = format!(
        " details{focus_marker} ",
        focus_marker = if details_focused { " ◆" } else { "" },
    );

    // Build matching-span set + field/event distributions for the
    // selected stack — shared between both focus modes.
    let mut detail_lines: Vec<Line> = Vec::new();
    if let Some(idx) = selected {
        let r = &rows[idx];

        // Resolve which spans match the selected stack via the
        // aggregator's cached resolved stacks — same result as
        // the old `bucket_key` walk, but already computed at
        // span-arrival time.
        let matching: Vec<&WireSpan> = model
            .agg
            .iter_with_stack()
            .filter(|(_, stack)| stack == &&r.key.stack)
            .map(|(s, _)| s)
            .collect();

        detail_lines.push(Line::from(format!("stack:  {}", r.key.stack.join(" › "))));
        detail_lines.push(Line::from(format!(
            "n={}  matched={}  total avg: {}  self avg: {}",
            r.stats.count,
            matching.len(),
            fmt_ns(r.stats.total_avg_ns()),
            fmt_ns(r.stats.self_avg_ns()),
        )));
        if !model.split_keys().is_empty() {
            let split_list: Vec<String> = model.split_keys().iter().cloned().collect();
            detail_lines.push(Line::from(format!("split by: {}", split_list.join(", "),)));
        }

        // Field distribution.
        let mut field_dist: BTreeMap<&str, BTreeMap<String, u32>> = BTreeMap::new();
        for s in &matching {
            for (k, v) in &s.fields {
                *field_dist
                    .entry(k.as_str())
                    .or_default()
                    .entry(v.to_string_value())
                    .or_default() += 1;
            }
        }
        let (distinguishing, constant): (Vec<_>, Vec<_>) =
            field_dist.iter().partition(|(_, vals)| vals.len() > 1);

        if !distinguishing.is_empty() {
            detail_lines.push(Line::from("fields (distinguishing):"));
            let show_per_key = if details_focused { 20 } else { 5 };
            for (k, vals) in &distinguishing {
                let mut entries: Vec<(&String, &u32)> = vals.iter().collect();
                // Alphabetical by value — stable order across
                // renders is more useful than count-rank when
                // the user's scanning for a specific value.
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let shown = entries
                    .iter()
                    .take(show_per_key)
                    .map(|(v, c)| format!("{v}×{c}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let suffix = if entries.len() > show_per_key {
                    format!(", +{} more", entries.len() - show_per_key)
                } else {
                    String::new()
                };
                detail_lines.push(Line::from(format!("  {k} = {shown}{suffix}")));
            }
        }
        if !constant.is_empty() {
            let summary = constant
                .iter()
                .map(|(k, vals)| {
                    let only = vals.keys().next().map(String::as_str).unwrap_or("");
                    format!("{k}={only}")
                })
                .collect::<Vec<_>>()
                .join("  ");
            detail_lines.push(Line::from(format!("fields (constant): {summary}")));
        }

        // Event summary.
        let mut event_dist: BTreeMap<&str, (u32, &str)> = BTreeMap::new();
        for s in &matching {
            for e in &s.events {
                let entry = event_dist
                    .entry(e.name.as_str())
                    .or_insert((0, level_str(e.level)));
                entry.0 += 1;
            }
        }
        if !event_dist.is_empty() {
            let total: u32 = event_dist.values().map(|(c, _)| c).sum();
            let mut entries: Vec<(&&str, &(u32, &str))> = event_dist.iter().collect();
            entries.sort_by(|a, b| b.1.0.cmp(&a.1.0).then(a.0.cmp(b.0)));
            let summary = entries
                .iter()
                .map(|(name, (count, lvl))| format!("{name}[{lvl}]×{count}"))
                .collect::<Vec<_>>()
                .join(", ");
            detail_lines.push(Line::from(format!("events ({total}): {summary}")));
        }

        // When Details is focused, append the candidate-keys
        // section: a list the user navigates with j/k and toggles
        // with Space.  Selected key is reversed; checked keys
        // (already in split_keys) get a [✓] marker.
        if details_focused {
            detail_lines.push(Line::from(""));
            detail_lines.push(Line::from(
                "metadata keys (Space to split/unsplit, Tab to leave):",
            ));
            let candidates = model.candidate_split_keys();
            let sel = model
                .details_selected
                .min(candidates.len().saturating_sub(1));
            if candidates.is_empty() {
                detail_lines.push(Line::from("  (no metadata keys present on matching spans)"));
            } else {
                for (i, k) in candidates.iter().enumerate() {
                    let checked = model.split_keys().contains(k);
                    let mark = if checked { "[✓]" } else { "[ ]" };
                    let line_text = format!("  {mark} {k}");
                    let style = if i == sel {
                        Style::default().add_modifier(Modifier::REVERSED)
                    } else {
                        Style::default()
                    };
                    detail_lines.push(Line::from(TuiSpan::styled(line_text, style)));
                }
            }
        }
    } else {
        detail_lines.push(Line::from(
            "(no spans yet — q quit, j/k move, →/l expand, Enter expand all, ←/h collapse, Tab focus details)",
        ));
    };
    let detail = Paragraph::new(detail_lines)
        .block(Block::default().title(details_title).borders(Borders::ALL));
    f.render_widget(detail, chunks[2]);
}
