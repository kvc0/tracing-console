//! Application state for the console TUI.
//!
//! Decoupled from rendering so it can be tested without touching
//! ratatui / crossterm.  Both [`Model`] and [`Update`] are
//! `Serialize` + `Deserialize` so integration tests can construct
//! sequences of updates (or replay a captured `--states` dump) and
//! assert on the resulting model.
//!
//! Submodule layout:
//! * [`core`] — `Model`, `ConnectionStatus`, `Focus`, `RateTracker`,
//!   `VisibleRow`, `Model::apply`.
//! * [`update`] — `Update` (every state-change message) and the
//!   `Effect` enum the reducer returns.
//! * [`graph`] — everything graph-view related: `ViewMode`,
//!   `GraphState`, `GraphSeriesStore`, `AggMode`, `Metric`,
//!   `GraphFocus`, `SortColumn`, `SeriesSummary`, `SeriesProjection`,
//!   `parse_agg_input`.

mod core;
pub(crate) mod explore;
mod graph;
mod update;

#[cfg(test)]
mod tests;

pub use core::{ConnectionStatus, Focus, LEVEL_OPTIONS, Model, RateTracker, VisibleRow};
pub use explore::{ExploreSortColumn, ExploreState, TraceDetailState, TraceRow};
pub use graph::{
    AggMode, GraphFocus, GraphState, Metric, SeriesProjection, SeriesSummary, SortColumn,
    TimeLabels, ViewMode,
};
// Test-only convenience re-exports — the reducer reaches into
// `super::graph` directly; tests grab these via `use super::*`.
#[cfg(test)]
pub use graph::{parse_agg_input, parse_lookback_input};
pub use update::{Effect, Update};
