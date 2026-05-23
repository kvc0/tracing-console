//! Explore + trace-detail rendering.  Explore lists span
//! instances matching a locked stack with `/`-search and
//! sortable columns; trace detail renders one root's full
//! span tree.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};

use crate::aggregate::fmt_ns;
use crate::model::{
    ExploreSortColumn, ExploreState, Model, TraceDetailState, TraceRow,
    explore::{
        distinguishing_fields, field_string, latency_ns, matching_spans, visible_trace_rows,
    },
};

use super::header::{focused_border_style, modal_status_bar, render_header_row};

pub fn render_explore(
    f: &mut Frame<'_>,
    area: Rect,
    model: &Model,
    es: &ExploreState,
    _colorize: bool,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(2), Constraint::Min(0)])
        .split(area);
    render_header_row(f, chunks[0], model);

    // Status row: locked stack + active search.
    let mut status: Vec<TuiSpan<'static>> = vec![
        TuiSpan::styled("explore ", Style::default().add_modifier(Modifier::BOLD)),
        TuiSpan::raw(es.locked_stack.join(" › ")),
    ];
    if !es.effective_query().is_empty() {
        status.push(TuiSpan::styled(
            "   /",
            Style::default().add_modifier(Modifier::DIM),
        ));
        status.push(TuiSpan::raw(es.effective_query().to_string()));
    }
    f.render_widget(Paragraph::new(Line::from(status)), chunks[1]);
    // Second status line: search modal cursor + key hints.  Sort
    // column is no longer announced here — the table header
    // underlines the active column and shows its direction
    // arrow, matching the graph view's pattern.
    let mut sub: Vec<TuiSpan<'static>> = vec![TuiSpan::styled(
        "(←/→ sort col, i invert, / search, Enter open, e/Esc back)",
        Style::default().add_modifier(Modifier::DIM),
    )];
    if let Some(buf) = &es.search_input {
        sub.push(TuiSpan::raw("   /"));
        let body = if buf.is_empty() { " ".into() } else { buf.clone() };
        sub.push(TuiSpan::styled(
            body,
            Style::default()
                .bg(Color::White)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ));
        sub.push(TuiSpan::styled(
            "_",
            Style::default()
                .bg(Color::White)
                .fg(Color::Black)
                .add_modifier(Modifier::REVERSED | Modifier::BOLD),
        ));
    }
    let header_row = Line::from(sub);
    f.render_widget(Paragraph::new(header_row), Rect {
        x: chunks[1].x,
        y: chunks[1].y + 1,
        width: chunks[1].width,
        height: 1,
    });

    // Body: span instance table.
    let spans = matching_spans(model, es);
    let fields = distinguishing_fields(&spans);
    // Display up to 3 distinguishing-field columns to leave room
    // for time + latency on narrow terminals.
    let max_field_cols = ((chunks[2].width as usize).saturating_sub(28) / 12).min(3);
    let visible_fields = &fields[..fields.len().min(max_field_cols)];

    // Header cells: the active sort column is underlined+bold
    // and shows a `▲`/`▼` arrow for direction.  Inactive columns
    // render dim, matching the graph view's series-table header.
    let dim_header = Style::default().add_modifier(Modifier::DIM);
    let active_header =
        Style::default().add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
    let header_cell = |label: &str, active: bool| -> Cell<'static> {
        if active {
            let arrow = es.direction.arrow();
            Cell::from(format!("{label} {arrow}")).style(active_header)
        } else {
            Cell::from(label.to_string()).style(dim_header)
        }
    };
    let header_cells: Vec<Cell<'_>> = {
        let mut cells: Vec<Cell<'_>> = vec![
            header_cell("time", matches!(es.sort, ExploreSortColumn::Timestamp)),
            header_cell("latency", matches!(es.sort, ExploreSortColumn::Latency)),
        ];
        for k in visible_fields {
            let active = matches!(&es.sort, ExploreSortColumn::Field(s) if s == k);
            cells.push(header_cell(k, active));
        }
        cells
    };
    // +2 chars per column for the active-sort `▲`/`▼` suffix.
    let mut widths: Vec<Constraint> = vec![
        Constraint::Length(14), // time HH:MM:SS.mmm + arrow
        Constraint::Length(12), // latency + arrow
    ];
    for _ in visible_fields {
        widths.push(Constraint::Length(14));
    }

    let rows: Vec<Row<'_>> = spans
        .iter()
        .map(|s| {
            let mut cells: Vec<Cell<'_>> = vec![
                Cell::from(format_span_timestamp(s.opened_at_ns)),
                Cell::from(fmt_ns(latency_ns(s))),
            ];
            for k in visible_fields {
                cells.push(Cell::from(field_string(s, k)));
            }
            Row::new(cells)
        })
        .collect();

    let title = format!(
        " {} matching span{} ",
        spans.len(),
        if spans.len() == 1 { "" } else { "s" },
    );
    let mut block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(focused_border_style(true));
    if let Some(help) = modal_status_bar(model) {
        block = block.title_bottom(help.right_aligned());
    }
    let table = Table::new(rows, widths)
        .header(Row::new(header_cells))
        .column_spacing(1)
        .block(block)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = TableState::default();
    if !spans.is_empty() {
        state.select(Some(es.selected.min(spans.len() - 1)));
    }
    f.render_stateful_widget(table, chunks[2], &mut state);
}

/// Format a span's `opened_at_ns` (nanoseconds since the Unix
/// epoch, as the wire protocol now defines it) as a local-time
/// `HH:MM:SS.mmm` string.  Mirrors the graph view's `Local`
/// time-labels mode, just with millisecond precision.
fn format_span_timestamp(opened_at_ns: u64) -> String {
    let dt = chrono::DateTime::<chrono::Local>::from(
        chrono::DateTime::from_timestamp_nanos(opened_at_ns as i64),
    );
    dt.format("%H:%M:%S%.3f").to_string()
}

// ── Trace detail view ─────────────────────────────────────────────────────

pub fn render_trace_detail(
    f: &mut Frame<'_>,
    area: Rect,
    model: &Model,
    td: &TraceDetailState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);
    render_header_row(f, chunks[0], model);

    let rows = visible_trace_rows(model, td);
    let root_open = model
        .agg
        .span_by_id(td.root_id)
        .map(|s| s.opened_at_ns)
        .unwrap_or(0);

    let title = format!(
        " trace {root_name} ({n} {label}) ",
        root_name = model
            .agg
            .span_by_id(td.root_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| format!("#{}", td.root_id)),
        n = rows.len(),
        label = if rows.len() == 1 { "row" } else { "rows" },
    );
    let block = Block::default()
        .title(title)
        .title(
            Line::from(" ↑↓ select │ ←/→ collapse / expand │ Esc back ")
                .alignment(Alignment::Right)
                .style(Style::default().add_modifier(Modifier::DIM)),
        )
        .borders(Borders::ALL)
        .border_style(focused_border_style(true));

    if rows.is_empty() {
        let para = Paragraph::new(Line::from(
            "(root span has fallen out of the cache)",
        ))
        .block(block);
        f.render_widget(para, chunks[1]);
        return;
    }

    let dim = Style::default().add_modifier(Modifier::DIM);
    let header = Row::new(vec![
        Cell::from("time").style(dim),
        Cell::from("total").style(dim),
        Cell::from("self").style(dim),
        Cell::from("trace").style(dim),
    ]);

    let table_rows: Vec<Row<'_>> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| trace_row_to_row(r, root_open, i == 0, model))
        .collect();

    let widths = [
        // Local clock for the root row needs HH:MM:SS.mmm (12).
        // Relative `+X.Xms` lines fit comfortably in 12 too.
        Constraint::Length(12),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Min(20),
    ];
    let table = Table::new(table_rows, widths)
        .header(header)
        .column_spacing(2)
        .block(block)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = TableState::default();
    state.select(Some(td.selected_idx.min(rows.len() - 1)));
    f.render_stateful_widget(table, chunks[1], &mut state);
}

/// One trace-detail table row.  `root_open` is the trace root's
/// `opened_at_ns`; rows after the root render their time column
/// as `+relative-to-root`.  The root row gets the local
/// wall-clock timestamp (HH:MM:SS.mmm), anchored to the most
/// recent known host-ns timestamp.
fn trace_row_to_row<'a>(
    row: &'a TraceRow,
    root_open: u64,
    is_root: bool,
    model: &'a Model,
) -> Row<'a> {
    let empty_cells = || {
        vec![
            Cell::from(""),
            Cell::from(""),
            Cell::from(""),
            Cell::from(""),
        ]
    };
    match row {
        TraceRow::Span { id, depth, has_children, expanded } => {
            let Some(s) = model.agg.span_by_id(*id) else {
                let mut cells = empty_cells();
                cells[3] = Cell::from("(missing span)");
                return Row::new(cells);
            };
            let time = if is_root {
                format_span_timestamp(s.opened_at_ns)
            } else {
                format!("+{}", fmt_ns(s.opened_at_ns.saturating_sub(root_open)))
            };
            let total = latency_ns(s);
            let self_ns = total.saturating_sub(model.agg.child_sum_for(*id));
            let marker = if !has_children {
                "· "
            } else if *expanded {
                "▼ "
            } else {
                "▶ "
            };
            let indent = "  ".repeat(*depth);
            let mut name_spans = vec![
                TuiSpan::styled(format!("{indent}{marker}"), Style::default().add_modifier(Modifier::DIM)),
                TuiSpan::styled(s.name.clone(), Style::default().add_modifier(Modifier::BOLD)),
            ];
            if !s.fields.is_empty() {
                let summary: Vec<String> = s
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v.to_string_value()))
                    .collect();
                name_spans.push(TuiSpan::raw("  "));
                name_spans.push(TuiSpan::styled(
                    summary.join(" "),
                    Style::default().add_modifier(Modifier::DIM),
                ));
            }
            Row::new(vec![
                Cell::from(time),
                Cell::from(fmt_ns(total)),
                Cell::from(fmt_ns(self_ns)),
                Cell::from(Line::from(name_spans)),
            ])
        }
        TraceRow::Event { parent_id, idx, depth } => {
            let Some(parent) = model.agg.span_by_id(*parent_id) else {
                let mut cells = empty_cells();
                cells[3] = Cell::from("(missing event)");
                return Row::new(cells);
            };
            let Some(e) = parent.events.get(*idx) else {
                let mut cells = empty_cells();
                cells[3] = Cell::from("(missing event)");
                return Row::new(cells);
            };
            let time = format!("+{}", fmt_ns(e.recorded_at_ns.saturating_sub(root_open)));
            let level_glyph = match e.level {
                tracing_console_host::WireLevel::Error => "E",
                tracing_console_host::WireLevel::Warn => "W",
                tracing_console_host::WireLevel::Info => "I",
                tracing_console_host::WireLevel::Debug => "D",
                tracing_console_host::WireLevel::Trace => "T",
            };
            let indent = "  ".repeat(*depth);
            let mut name_spans = vec![
                TuiSpan::styled(
                    format!("{indent}· [{level_glyph}] "),
                    Style::default().add_modifier(Modifier::DIM),
                ),
                TuiSpan::raw(e.name.clone()),
            ];
            if !e.fields.is_empty() {
                let summary: Vec<String> = e
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v.to_string_value()))
                    .collect();
                name_spans.push(TuiSpan::raw("  "));
                name_spans.push(TuiSpan::styled(
                    summary.join(" "),
                    Style::default().add_modifier(Modifier::DIM),
                ));
            }
            // Events have no total/self latency of their own.
            Row::new(vec![
                Cell::from(time),
                Cell::from(""),
                Cell::from(""),
                Cell::from(Line::from(name_spans)),
            ])
        }
    }
}
