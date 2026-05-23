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

use crate::model::{Model, ViewMode};

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
}
