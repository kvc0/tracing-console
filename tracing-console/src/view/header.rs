//! Header-row helpers shared between the table and graph views:
//! the level switcher, the chance-input switcher, and the two
//! formatters they call.

use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan};
use tracing_console_host::WireLevelFilter;

use crate::model::{Model, ViewMode};

/// Right-aligned `g graph` / `g stack` hint shown in the top-right
/// of the header.  The `g` is underlined as the shortcut key; the
/// label names the *destination* view (so the table shows "graph"
/// and the graph shows "stack").
pub(super) fn graph_toggle_hint(model: &Model) -> Line<'static> {
    let label = match model.view {
        ViewMode::Graph(_) => "stack",
        ViewMode::Table => "graph",
    };
    Line::from(vec![
        TuiSpan::styled("g", Style::default().add_modifier(Modifier::UNDERLINED)),
        TuiSpan::raw(" "),
        TuiSpan::raw(label),
    ])
}

/// Fixed column width reserved on the right of the header for
/// [`graph_toggle_hint`].  Long-form label is `g stack` / `g graph`
/// — both 7 chars; round up to 8 for a one-cell gutter.
pub(super) const GRAPH_HINT_WIDTH: u16 = 8;

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
    if pct < 1.0 && let Some(rest) = trimmed.strip_prefix("0.") {
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
