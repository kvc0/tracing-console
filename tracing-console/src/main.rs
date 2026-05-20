//! Console TUI for browsing live spans streamed from a
//! `tracing-console-host`.
//!
//! Layout:
//!
//! * [`aggregate`] — incremental rolling-window span aggregator.
//! * [`model`] — application state + `Update` reducer.  Pure logic,
//!   no UI or I/O dependencies.
//! * [`view`] — ratatui rendering, split into table and graph
//!   submodules.  Pure UI, no model mutation.
//! * [`runtime`] — the network task, the per-mode entry points
//!   (`run_tui` / `run_states` / `run_stats`), and the keyboard
//!   thread.  Glues the others together.
//!
//! `main` itself just parses CLI args and hands control to
//! [`runtime::run`].

mod aggregate;
mod args;
mod model;
mod runtime;
mod stats;
mod view;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    runtime::run(args::Args::from_cli()).await
}
