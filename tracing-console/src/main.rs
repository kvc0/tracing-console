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

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::panic_in_result_fn,)
)]

mod aggregate;
mod args;
mod installer;
mod model;
mod runtime;
mod stats;
mod view;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = args::Args::from_cli();

    // `--update` is a side-channel: skip the async runtime entirely
    // and shell out to the public installer.  Doing this before the
    // tokio runtime starts avoids spinning up infrastructure we'd
    // tear down immediately, and lets the installer's output share
    // the user's terminal with no contention.
    if args.update {
        let status = installer::run(None)?;
        std::process::exit(status.code().unwrap_or(1));
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(runtime::run(args))
}
