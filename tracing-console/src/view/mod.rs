//! Ratatui rendering for the tracing-console TUI.
//!
//! The only public surface is [`render`], which inspects
//! `model.view` and dispatches to the table or graph submodule.
//!
//! * [`header`] — connection-status / level-switcher / chance-input
//!   row used by both views.
//! * [`table`] — the stacks + details two-pane layout.
//! * [`graph`] — the chart + columnar legend layout.

mod explore;
mod graph;
mod header;
mod table;

use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::model::{CLIENT_VERSION, ConfirmStatus, ConfirmVersionSwitch, Model, ViewMode};

pub fn render(f: &mut ratatui::Frame<'_>, model: &Model, colorize: bool) {
    let area = f.area();
    // Modal-help text lives on the bottom border of the active
    // pane (see `modal_status_bar` callers in `table.rs` and
    // `graph.rs`) — never as a bottom-of-screen strip, because
    // that would re-layout the panes the moment an input opens.
    match &model.view {
        ViewMode::Table => table::render_table(f, area, model, colorize),
        ViewMode::Graph(gs) => graph::render_graph(f, area, model, gs, colorize),
        ViewMode::Explore(es) => explore::render_explore(f, area, model, es, colorize),
        ViewMode::TraceDetail(td) => explore::render_trace_detail(f, area, model, td),
    }
    // Global overlays — drawn LAST so they sit on top of whatever
    // the per-view renderer painted.  Only one overlay is ever live
    // at a time today (the version-mismatch confirm modal), but
    // funnel it through one helper so adding the next is mechanical.
    render_overlays(f, area, model);
}

fn render_overlays(f: &mut ratatui::Frame<'_>, area: Rect, model: &Model) {
    if let Some(c) = &model.confirm_version_switch {
        render_confirm_version_switch(f, area, c);
    }
    if model.quit_confirm_deadline.is_some() {
        render_quit_confirm(f, area);
    }
}

/// Centred two-line "press q again to quit" prompt.  Same `Clear +
/// Block` overlay pattern as the version-switch modal but much
/// smaller — the message is the whole UI.  Dismissed by a second
/// `q` (which the keyboard loop turns into `Update::Quit`, and the
/// reducer interprets against the live deadline), by `Esc` (the
/// runtime intercepts and sends `QuitConfirmDismiss`), or by the
/// runtime ticker expiring the 2 s deadline.
fn render_quit_confirm(f: &mut ratatui::Frame<'_>, area: Rect) {
    let msg = "press q again to quit";
    // 4 cells of padding around the message.
    let w = area
        .width
        .min((msg.chars().count() as u16).saturating_add(8));
    let h = area.height.min(3);
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    let modal_area = Rect {
        x,
        y,
        width: w,
        height: h,
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().add_modifier(Modifier::BOLD));
    let inner = block.inner(modal_area);
    f.render_widget(Clear, modal_area);
    f.render_widget(block, modal_area);
    let line = Line::from(msg).alignment(Alignment::Center);
    f.render_widget(Paragraph::new(line), inner);
}

/// Centred modal for the server/client version mismatch — renders
/// one of three contents depending on `status`:
///
///   * `Confirming` — small y/n prompt.
///   * `Running`    — same prompt area, replaced with "installing…".
///   * `Failed(o)`  — taller box with the installer's captured
///     stderr/stdout, dismissible with `n` / `Esc`.
///
/// Drawn over a `Clear` so it doesn't pick up underlying pane
/// content.  Yellow border to echo the header's mismatch line.
fn render_confirm_version_switch(f: &mut ratatui::Frame<'_>, area: Rect, c: &ConfirmVersionSwitch) {
    let server_version = c.server_version.as_str();

    // The Failed box wants more room so the captured installer
    // output is legible; the y/n and Running boxes are small.
    let (target_w, target_h) = match &c.status {
        ConfirmStatus::Failed(_) => (90u16, 20u16),
        _ => (60, 9),
    };
    let w = area.width.min(target_w);
    let h = area.height.min(target_h);
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    let modal_area = Rect {
        x,
        y,
        width: w,
        height: h,
    };

    let yellow = Style::default().fg(Color::Yellow);
    let dim = Style::default().add_modifier(Modifier::DIM);

    let title = match &c.status {
        ConfirmStatus::Confirming => " switch versions? ",
        ConfirmStatus::Running => " installing… ",
        ConfirmStatus::Failed(_) => " installer failed ",
        ConfirmStatus::Restart => " restart? ",
    };
    let block = Block::default()
        .title(Line::from(title).alignment(Alignment::Center))
        .borders(Borders::ALL)
        .border_style(yellow);
    let inner = block.inner(modal_area);
    f.render_widget(Clear, modal_area);
    f.render_widget(block, modal_area);

    match &c.status {
        ConfirmStatus::Confirming => {
            let lines: Vec<Line<'static>> = vec![
                Line::from(""),
                Line::from(vec![
                    TuiSpan::raw("Server is at "),
                    TuiSpan::styled(format!("v{server_version}"), yellow),
                    TuiSpan::raw(", client is at "),
                    TuiSpan::styled(format!("v{CLIENT_VERSION}"), yellow),
                    TuiSpan::raw("."),
                ])
                .alignment(Alignment::Center),
                Line::from(""),
                Line::from(vec![
                    TuiSpan::raw("Switch this binary to "),
                    TuiSpan::styled(format!("v{server_version}"), yellow),
                    TuiSpan::raw(" via the public installer?"),
                ])
                .alignment(Alignment::Center),
                Line::from(""),
                Line::from(vec![
                    TuiSpan::styled("y", Style::default().add_modifier(Modifier::UNDERLINED)),
                    TuiSpan::raw(" yes  "),
                    TuiSpan::styled("n", Style::default().add_modifier(Modifier::UNDERLINED)),
                    TuiSpan::raw(" no"),
                ])
                .alignment(Alignment::Center),
            ];
            f.render_widget(Paragraph::new(lines), inner);
        }
        ConfirmStatus::Running => {
            let lines: Vec<Line<'static>> = vec![
                Line::from(""),
                Line::from(vec![
                    TuiSpan::raw("Running the installer for "),
                    TuiSpan::styled(format!("v{server_version}"), yellow),
                    TuiSpan::raw("…"),
                ])
                .alignment(Alignment::Center),
                Line::from(""),
                Line::from(TuiSpan::styled("this may take a few seconds", dim))
                    .alignment(Alignment::Center),
            ];
            f.render_widget(Paragraph::new(lines), inner);
        }
        ConfirmStatus::Failed(output) => {
            // Render the captured output verbatim under a one-line
            // header.  `Wrap { trim: false }` preserves leading
            // whitespace inside log lines.  No "press n to dismiss"
            // hint — Esc is a universal back-out, calling it out
            // here just adds noise.
            let mut lines: Vec<Line<'static>> = Vec::new();
            lines.push(
                Line::from(vec![
                    TuiSpan::raw("Tried to install "),
                    TuiSpan::styled(format!("v{server_version}"), yellow),
                    TuiSpan::raw(", but the installer reported:"),
                ])
                .alignment(Alignment::Left),
            );
            lines.push(Line::from(""));
            for raw in output.lines() {
                lines.push(Line::from(raw.to_string()));
            }
            f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
        }
        ConfirmStatus::Restart => {
            let lines: Vec<Line<'static>> = vec![
                Line::from(""),
                Line::from(vec![
                    TuiSpan::raw("Installed "),
                    TuiSpan::styled(format!("v{server_version}"), yellow),
                    TuiSpan::raw("."),
                ])
                .alignment(Alignment::Center),
                Line::from(""),
                Line::from("Restart this process to use the new binary?")
                    .alignment(Alignment::Center),
                Line::from(""),
                Line::from(vec![
                    TuiSpan::styled("y", Style::default().add_modifier(Modifier::UNDERLINED)),
                    TuiSpan::raw(" yes  "),
                    TuiSpan::styled("n", Style::default().add_modifier(Modifier::UNDERLINED)),
                    TuiSpan::raw(" no"),
                ])
                .alignment(Alignment::Center),
            ];
            f.render_widget(Paragraph::new(lines), inner);
        }
    }
}
