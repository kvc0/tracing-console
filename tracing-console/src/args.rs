//! Command-line argument parsing for `tracing-console`, via clap.

use clap::{Parser, ValueEnum};

pub const DEFAULT_HOST: &str = "127.0.0.1:7777";
/// Default rolling-buffer size for received spans.
pub const DEFAULT_HISTORY_BUDGET: usize = 4096;
/// Default capacity for the spillway channel that moves `Update`s
/// from the network task and keyboard thread to the runtime loop.
pub const DEFAULT_STREAM_BUFFER: usize = 4096;

#[derive(Parser, Debug)]
#[command(
    name = "tracing-console",
    about = "Interactive console for browsing live spans from a tracing-console-host",
    version
)]
pub struct Args {
    /// Address of the host to connect to (e.g. `127.0.0.1:7777`).
    #[arg(default_value = DEFAULT_HOST)]
    pub addr: std::net::SocketAddr,

    /// Output mode: interactive TUI (default), JSON state dump, or
    /// periodic stats.
    #[arg(long, value_enum, default_value_t = ModeFlag::Tui)]
    pub mode: ModeFlag,

    /// For `--mode stats`: refresh frequency in Hz (e.g. `1`, `0.1`).
    #[arg(long, default_value_t = 1.0)]
    pub stats_hz: f64,

    /// Disable terminal colour output in the TUI.
    #[arg(long)]
    pub no_color: bool,

    /// Rolling-buffer size for received spans (bucketing input).
    #[arg(long, default_value_t = DEFAULT_HISTORY_BUDGET)]
    pub history: usize,

    /// Capacity of the spillway channel that carries `Update`s from
    /// the network / keyboard task to the runtime loop.  Sets an
    /// upper bound on in-flight unprocessed messages; once full,
    /// further sends are dropped rather than blocking the producer.
    #[arg(long, default_value_t = DEFAULT_STREAM_BUFFER)]
    pub stream_buffer: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ModeFlag {
    /// Interactive ratatui TUI (default).
    Tui,
    /// Print each `Update` as a JSON line to stdout — useful for
    /// snapshot-style integration tests.
    States,
    /// Print rolling stats tables at a configurable refresh rate
    /// (see `--stats-hz`).
    Stats,
}

impl Args {
    /// Parse from the process's CLI, exiting (per clap convention)
    /// on `--help` / `--version` / parse errors.
    pub fn from_cli() -> Self {
        <Self as Parser>::parse()
    }
}
