//! Command-line argument parsing for `tracing-console`, via clap.

use clap::{Parser, ValueEnum};
use tracing_console_host::WireLevelFilter;

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
    version,
    help_template = "\
{about-with-newline}
v{version}

{usage-heading} {usage}

{all-args}{after-help}"
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

    /// Immediately on connect, request the host set its cache-
    /// recording level to this value.  Useful for `--mode stats`
    /// benchmarking and any other non-interactive run where you
    /// don't want to type Shift+I/D/T to start recording.
    #[arg(long, value_enum)]
    pub set_level: Option<LevelArg>,

    /// Immediately on connect, request the host set its
    /// `ChancePredicate` percentage to this value (0.0–100.0).
    #[arg(long)]
    pub set_chance: Option<f64>,

    /// Re-run the public installer to upgrade to the latest published
    /// release.  Shells out to `curl … | bash` and exits; does not
    /// connect to a host.  All other flags are ignored.
    #[arg(long, exclusive = true)]
    pub update: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum LevelArg {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LevelArg {
    pub fn to_wire(self) -> WireLevelFilter {
        match self {
            LevelArg::Off => WireLevelFilter::Off,
            LevelArg::Error => WireLevelFilter::Error,
            LevelArg::Warn => WireLevelFilter::Warn,
            LevelArg::Info => WireLevelFilter::Info,
            LevelArg::Debug => WireLevelFilter::Debug,
            LevelArg::Trace => WireLevelFilter::Trace,
        }
    }
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
