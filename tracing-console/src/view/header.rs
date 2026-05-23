//! Header-row helpers shared between the table and graph views:
//! the level switcher, the chance-input switcher, the two
//! formatters they call, plus the full one-line connection
//! header and its right-aligned `g graph` / `g stack` hint.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan};
use ratatui::widgets::Paragraph;
use tracing_console_host::WireLevelFilter;

use crate::model::{ConnectionStatus, Model, ViewMode};

/// Faint vertical separator between logical groups in the header
/// row.  Same DIM `│` glyph used by the stacks-table column
/// separators, so the eye doesn't have to learn two patterns.
fn header_separator() -> TuiSpan<'static> {
    TuiSpan::styled(" │ ", Style::default().add_modifier(Modifier::DIM))
}

/// Coloured status dot for the connection state.  Green when
/// connected, red when disconnected, dim grey while connecting.
fn connection_status_dot(model: &Model) -> TuiSpan<'static> {
    let (glyph, style) = match &model.connection {
        ConnectionStatus::Connected => (
            "●",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        ConnectionStatus::Disconnected(_) => (
            "●",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        ConnectionStatus::Connecting => ("●", Style::default().add_modifier(Modifier::DIM)),
    };
    TuiSpan::styled(glyph, style)
}

/// Build the one-line top-of-screen header for either view.
/// Connecting / Connected / Disconnected branches; the Connected
/// branch composes [`level_switcher_spans`] + [`chance_switcher_spans`]
/// with the buffered-span count and rolling rate appended.
pub(super) fn connection_header_line(model: &Model) -> Line<'static> {
    let mut spans: Vec<TuiSpan<'static>> = vec![connection_status_dot(model), TuiSpan::raw(" ")];
    match &model.connection {
        ConnectionStatus::Connecting => {
            spans.push(TuiSpan::raw("connecting "));
            spans.push(TuiSpan::styled(
                model.status.clone().unwrap_or_default(),
                Style::default().add_modifier(Modifier::DIM),
            ));
        }
        ConnectionStatus::Connected => {
            level_switcher_spans(&mut spans, model);
            spans.push(header_separator());
            chance_switcher_spans(&mut spans, model);
            spans.push(header_separator());
            spans.push(TuiSpan::raw(format!(
                "{n} spans / {rate}",
                n = model.agg.len(),
                rate = format_span_rate(model),
            )));
        }
        ConnectionStatus::Disconnected(reason) => {
            // Highlight just the word so a glance across the screen
            // immediately reads as broken.
            spans.push(TuiSpan::styled(
                "disconnected",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ));
            if !reason.is_empty() {
                spans.push(TuiSpan::raw(format!(" {reason}")));
            }
        }
    }
    Line::from(spans)
}

/// Border style used by panes that can take keyboard focus.
/// Bright + bold when focused, dim when not — the unmissable
/// signal that the next keystroke targets this pane.
pub(super) fn focused_border_style(is_focused: bool) -> Style {
    if is_focused {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::DIM)
    }
}

/// One-character underlined shortcut hint (used by the right-side
/// border hints).  `"a"` → `a` (underlined).
pub(super) fn shortcut(c: &'static str) -> TuiSpan<'static> {
    TuiSpan::styled(c, Style::default().add_modifier(Modifier::UNDERLINED))
}

/// Build the bottom-of-screen modal-help bar.  Returns `None` when
/// no modal is open; otherwise a one-line reminder of what's
/// accepted plus Enter/Esc.
pub(super) fn modal_status_bar(model: &Model) -> Option<Line<'static>> {
    let mut spans: Vec<TuiSpan<'static>> = Vec::new();
    let accept: &str = if model.chance_input.is_some() {
        "digits and ."
    } else if let ViewMode::Graph(gs) = &model.view {
        if gs.agg_input.is_some() {
            "a, avg, min, max, p0–p100"
        } else if gs.window_input.is_some() {
            "positive seconds"
        } else if gs.lookback_input.is_some() {
            "Ns or Nm"
        } else {
            return None;
        }
    } else if let ViewMode::Explore(es) = &model.view {
        if es.search_input.is_some() {
            "search: name / field / event"
        } else {
            return None;
        }
    } else {
        return None;
    };
    spans.push(TuiSpan::styled(
        "  input: ",
        Style::default().add_modifier(Modifier::DIM),
    ));
    spans.push(TuiSpan::raw(accept));
    spans.push(header_separator());
    spans.push(shortcut("Enter"));
    spans.push(TuiSpan::raw(" commit"));
    spans.push(header_separator());
    spans.push(shortcut("Esc"));
    spans.push(TuiSpan::raw(" cancel"));
    Some(Line::from(spans))
}

/// Render the header line into `area` with the `g graph` / `g stack`
/// hint right-aligned in the rightmost [`GRAPH_HINT_WIDTH`] columns.
/// Used by both the table and graph views — same layout, same
/// content rules.
pub(super) fn render_header_row(f: &mut Frame<'_>, area: Rect, model: &Model) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(GRAPH_HINT_WIDTH)])
        .split(area);
    f.render_widget(Paragraph::new(connection_header_line(model)), chunks[0]);
    f.render_widget(
        Paragraph::new(graph_toggle_hint(model)).alignment(Alignment::Right),
        chunks[1],
    );
}

/// Right-aligned view-switcher hint shown in the top-right of
/// the header.  Lists the *other* top-level views as `<letter>
/// <word>` with the shortcut letter underlined.  The current
/// view is omitted so the user can't accidentally try to "switch
/// to where they already are."  Trace detail counts as a sub-
/// view of explore for hint purposes — all three options stay
/// visible to surface the way back up.
pub(super) fn graph_toggle_hint(model: &Model) -> Line<'static> {
    let current = match &model.view {
        ViewMode::Table => Some("stack"),
        ViewMode::Graph(_) => Some("graph"),
        ViewMode::Explore(_) => Some("explore"),
        ViewMode::TraceDetail(_) => None,
    };
    let entries: &[(&'static str, &'static str)] =
        &[("s", "stack"), ("g", "graph"), ("e", "explore")];
    let sep = TuiSpan::styled(" │ ", Style::default().add_modifier(Modifier::DIM));
    let mut spans: Vec<TuiSpan<'static>> = vec![TuiSpan::raw(" ")];
    let mut first = true;
    for (letter, word) in entries {
        if Some(*word) == current {
            continue;
        }
        if !first {
            spans.push(sep.clone());
        }
        first = false;
        spans.push(TuiSpan::styled(
            *letter,
            Style::default().add_modifier(Modifier::UNDERLINED),
        ));
        spans.push(TuiSpan::raw(format!(" {word}")));
    }
    spans.push(TuiSpan::raw(" "));
    Line::from(spans)
}

/// Width reserved on the right of the header for
/// [`graph_toggle_hint`].  Worst case is `s stack │ g graph │ e
/// explore` ≈ 30 cells (trace detail / sub-view); allow a small
/// gutter past that.
pub(super) const GRAPH_HINT_WIDTH: u16 = 32;

/// Format a chance percentage like the user asked: `100%`, `2%`,
/// `.001%`, `0.5%` — drop trailing zeros, drop the leading `0`
/// for sub-1% values.  Clamps NaN to `0%` defensively.
pub(super) fn format_chance(pct: f64) -> String {
    if !pct.is_finite() || pct <= 0.0 {
        return "0%".to_string();
    }
    if pct >= 100.0 {
        return "100%".to_string();
    }
    // 3 decimals is more than enough — the user types only with
    // dots and digits and we clamp at 100 anyway.
    let s = format!("{:.3}", pct);
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    if pct < 1.0
        && let Some(rest) = trimmed.strip_prefix("0.")
    {
        format!(".{rest}%")
    } else {
        format!("{trimmed}%")
    }
}

/// Format the rolling 10-second receive rate (from `Model::rate`)
/// in k/M/Hz units for the header line.
pub(super) fn format_span_rate(model: &Model) -> String {
    let hz = model.rate.rate_hz();
    if hz >= 1_000_000.0 {
        format!("{:.1}MHz", hz / 1e6)
    } else if hz >= 1_000.0 {
        format!("{:.1}kHz", hz / 1e3)
    } else if hz >= 10.0 {
        format!("{:.0}Hz", hz)
    } else {
        format!("{:.1}Hz", hz)
    }
}

/// Push the `Chance <value>` widget into the header span buffer.
/// The `C` in `Chance` is always underlined as the keyboard
/// shortcut.  When the user is typing (model.chance_input is
/// Some), the area renders with a reversed-background highlight
/// and shows the buffer plus a `_` cursor.
pub(super) fn chance_switcher_spans(out: &mut Vec<TuiSpan<'static>>, model: &Model) {
    let editing = model.chance_input.is_some();
    let label_base = if editing {
        Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
    } else {
        Style::default()
    };
    out.push(TuiSpan::styled(
        "C",
        label_base.add_modifier(Modifier::UNDERLINED),
    ));
    out.push(TuiSpan::styled("hance ", label_base));
    if let Some(buf) = &model.chance_input {
        let body = if buf.is_empty() {
            "_".to_string()
        } else {
            format!("{buf}_")
        };
        out.push(TuiSpan::styled(
            body,
            Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD),
        ));
    } else {
        let txt = match model.cache_chance {
            Some(pct) => format_chance(pct),
            None => "—".to_string(),
        };
        out.push(TuiSpan::raw(txt));
    }
}

/// Push the `Off Info Debug Trace` switcher into a span buffer.
/// * The label whose level matches `model.cache_level` (server-
///   confirmed) renders with a reversed background highlight.
/// * Each label's first letter is the Shift+letter shortcut —
///   always underlined so the keybinding is discoverable.
pub(super) fn level_switcher_spans(out: &mut Vec<TuiSpan<'static>>, model: &Model) {
    for (idx, level) in crate::model::LEVEL_OPTIONS.iter().enumerate() {
        if idx > 0 {
            out.push(TuiSpan::raw(" "));
        }
        let label = match level {
            WireLevelFilter::Off => "Off",
            WireLevelFilter::Error => "Error",
            WireLevelFilter::Warn => "Warn",
            WireLevelFilter::Info => "Info",
            WireLevelFilter::Debug => "Debug",
            WireLevelFilter::Trace => "Trace",
        };
        let confirmed = model.cache_level == Some(*level);
        let base = if confirmed {
            Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD)
        } else {
            Style::default().add_modifier(Modifier::DIM)
        };
        let shortcut_style = base.add_modifier(Modifier::UNDERLINED);
        let mut chars = label.chars();
        let first = chars.next().unwrap_or(' ').to_string();
        let rest: String = chars.collect();
        out.push(TuiSpan::styled(first, shortcut_style));
        out.push(TuiSpan::styled(rest, base));
    }
}
