//! Console TUI for browsing live spans streamed from a `tracing-console-host`.
//!
//! Architecture:
//!
//! * [`Model`] holds all state.  It is `Serialize`/`Deserialize` and has its
//!   own unit tests in [`model`] — no UI required.
//! * The runtime owns a single `mpsc::UnboundedReceiver<Update>`.  The network
//!   task and (in TUI mode) the keyboard thread both push `Update`s into it.
//! * Two runtimes: [`run_tui`] renders the model with ratatui.  [`run_states`]
//!   prints each `Update` to stdout as JSON instead of rendering — handy as a
//!   `--states` flag for snapshot-style integration tests.
//!
//! TODO(flamegraph / heatmap / filter input): the TUI view is the v0
//! list-and-details layout.  The full spec calls for a flamegraph upper panel
//! and a heatmap when the filter narrows to a single span name; those land
//! once this skeleton is happy.

mod aggregate;
mod args;
mod model;
mod stats;

use std::io::Stdout;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::{FutureExt, StreamExt};
use protosocket_messagepack::{MessagePackDecoder, MessagePackSerializer};
use protosocket_rpc::client::{self, Configuration, TcpStreamConnector};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use tracing_console_host::{Request, RequestBody, Response, ResponseBody, WireLevelFilter};

use crate::args::{Args, ModeFlag};
use crate::model::{ConnectionStatus, Effect, Model, Update, ViewMode};

/// Outgoing commands that the runtime queues for the network task.
/// Separate from `Update` so the model never sees them — the model
/// only reflects server-confirmed state (e.g. `CacheLevelReceived`).
#[derive(Debug, Clone)]
enum Outgoing {
    SetCacheLevel(WireLevelFilter),
    SetCacheChance(f64),
}

/// Number of producer chutes on the Update spillway.  We have two
/// fan-in points (network task + keyboard thread), so 2 is the
/// minimum-contention setting.
const UPDATE_CHANNEL_CONCURRENCY: usize = 2;

/// Outgoing-request spillway is single-producer (the runtime), so
/// concurrency = 1.  Capacity is small — outgoing requests are
/// driven by keystrokes, not high-rate traffic.
const OUTGOING_CHANNEL_CAPACITY: u64 = 64;
const OUTGOING_CHANNEL_CONCURRENCY: usize = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_cli();

    let stream_buffer = args.stream_buffer as u64;
    let (tx, rx) = spillway::channel_with_capacity_and_concurrency::<Update>(
        stream_buffer,
        UPDATE_CHANNEL_CONCURRENCY,
    );
    let (out_tx, out_rx) = spillway::channel_with_capacity_and_concurrency::<Outgoing>(
        OUTGOING_CHANNEL_CAPACITY,
        OUTGOING_CHANNEL_CONCURRENCY,
    );

    // Network task: feeds Updates into the channel.  Owns no UI concerns.
    {
        let tx = tx.clone();
        tokio::spawn(async move { run_network(args.addr, tx, out_rx).await });
    }

    // Queue the optional auto-configure RPCs *before* dispatching to
    // the per-mode loop.  Drained from `out_rx` by the network task
    // as soon as it connects — so `--mode stats --set-level trace`
    // can benchmark a host that defaults to OFF without needing a
    // separate TUI session to flip it.
    if let Some(level) = args.set_level {
        let _ = out_tx.send(Outgoing::SetCacheLevel(level.to_wire()));
    }
    if let Some(chance) = args.set_chance {
        let _ = out_tx.send(Outgoing::SetCacheChance(chance));
    }

    match args.mode {
        ModeFlag::States => run_states(rx, args.history).await,
        ModeFlag::Stats => stats::run_stats(rx, args.stats_hz, args.history).await,
        ModeFlag::Tui => run_tui(tx, rx, out_tx, !args.no_color, args.history).await,
    }
}

// ── network task ─────────────────────────────────────────────────────────────

type ClientCodec = (MessagePackSerializer<Request>, MessagePackDecoder<Response>);

async fn run_network(
    addr: std::net::SocketAddr,
    tx: spillway::Sender<Update>,
    mut out_rx: spillway::Receiver<Outgoing>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};

    // protosocket-rpc routes responses by `Message::message_id()` and
    // does not auto-assign ids.  `Request::new` defaults to id=0, so
    // two coexisting RPCs at id=0 (streaming + unary) would collide
    // in the client's completion registry and the second would
    // clobber the first.  A monotonic counter survives reconnects,
    // so the same request never re-uses an id.
    let next_id = AtomicU64::new(1);
    let configuration: Configuration<TcpStreamConnector> = Configuration::new(TcpStreamConnector);
    // 1 Hz reconnect cadence — every failed dial or dropped stream
    // sleeps for `RECONNECT_DELAY` before the next attempt.
    const RECONNECT_DELAY: Duration = Duration::from_secs(1);

    'reconnect: loop {
        let _ = tx.send(Update::Status(format!("connecting to {addr}…")));
        let (rpc_client, conn) =
            match client::connect::<ClientCodec, _>(addr, &configuration).await {
                Ok(pair) => pair,
                Err(e) => {
                    let _ = tx.send(Update::Disconnected(format!("connect failed: {e}")));
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue 'reconnect;
                }
            };
        let conn_task = tokio::spawn(conn);
        let _ = tx.send(Update::Connected);

        let mut start = Request::new(RequestBody::StartStream);
        start.id = next_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = match rpc_client.send_streaming(start) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Update::Disconnected(format!("StartStream failed: {e}")));
                conn_task.abort();
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue 'reconnect;
            }
        };

        let mut runtime_gone = false;
        // Convert one streaming Response into the matching Update, or
        // None for body variants the client ignores (Ack / Noop /
        // Error).  Pulled out of the select arm so the batch path
        // below can reuse it.
        let resp_to_update = |resp: Response| -> Option<Update> {
            match resp.body {
                ResponseBody::Span(s) => Some(Update::SpanReceived(s)),
                ResponseBody::CacheLevel(filter) => Some(Update::CacheLevelReceived(filter)),
                ResponseBody::CacheChance(pct) => Some(Update::CacheChanceReceived(pct)),
                _ => None,
            }
        };
        // Cap batch size so `send_many` (which atomically rejects the
        // whole batch on Full) stays well under the spillway capacity
        // and the inner drain loop doesn't starve `out_rx`.
        const MAX_BATCH: usize = 256;
        'stream: loop {
            tokio::select! {
                // Stream of pushes from the server.  Take the first
                // item via select, then opportunistically drain
                // additional ready items into a batch.  One
                // `send_many` per burst replaces N atomic sends —
                // big win at 10k+ spans/s where the runtime task
                // was getting woken N times per tick.
                item = stream.next() => {
                    let mut batch: Vec<Update> = Vec::new();
                    let mut stream_ended_at = None::<String>;
                    let mut stream_dropped = false;
                    match item {
                        None => stream_dropped = true,
                        Some(Err(e)) => {
                            stream_ended_at = Some(format!("stream error: {e}"));
                        }
                        Some(Ok(resp)) => {
                            if let Some(u) = resp_to_update(resp) {
                                batch.push(u);
                            }
                        }
                    }
                    if !stream_dropped && stream_ended_at.is_none() {
                        while batch.len() < MAX_BATCH {
                            match stream.next().now_or_never() {
                                Some(None) => {
                                    stream_dropped = true;
                                    break;
                                }
                                Some(Some(Err(e))) => {
                                    stream_ended_at = Some(format!("stream error: {e}"));
                                    break;
                                }
                                Some(Some(Ok(resp))) => {
                                    if let Some(u) = resp_to_update(resp) {
                                        batch.push(u);
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                    if !batch.is_empty() {
                        match tx.send_many(batch) {
                            Ok(()) | Err(spillway::Error::Full(_)) => {}
                            Err(spillway::Error::Closed(_)) => {
                                runtime_gone = true;
                                break 'stream;
                            }
                        }
                    }
                    if let Some(msg) = stream_ended_at {
                        let _ = tx.send(Update::Disconnected(msg));
                        break 'stream;
                    }
                    if stream_dropped {
                        break 'stream;
                    }
                }
                // Outgoing requests from the keyboard / runtime.
                // `None` means all senders dropped → runtime is
                // shutting down for good; don't reconnect.
                req = out_rx.next() => {
                    let Some(req) = req else {
                        runtime_gone = true;
                        break 'stream;
                    };
                    let mut request = match req {
                        Outgoing::SetCacheLevel(filter) => {
                            Request::new(RequestBody::SetCacheLevel(filter))
                        }
                        Outgoing::SetCacheChance(pct) => {
                            Request::new(RequestBody::SetCacheChance(pct))
                        }
                    };
                    request.id = next_id.fetch_add(1, Ordering::Relaxed);
                    if let Ok(unary) = rpc_client.send_unary(request) {
                        // Server-pushed CacheLevel / CacheChance on
                        // the stream is the source of truth — the
                        // ack here is just signal that the request
                        // landed.
                        tokio::spawn(async move { let _ = unary.await; });
                    }
                }
            }
        }
        let _ = tx.send(Update::Disconnected("stream ended".into()));
        conn_task.abort();
        if runtime_gone {
            return;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

// ── --states mode ────────────────────────────────────────────────────────────

async fn run_states(
    mut rx: spillway::Receiver<Update>,
    history_budget: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut model = Model::new(history_budget);
    while let Some(update) = rx.next().await {
        // Print the update first so an integration test sees the cause line
        // followed by an effect line if needed.
        println!("{}", serde_json::to_string(&update)?);
        // --states mode has no upstream pipe; ignore any non-Quit
        // side-effects (e.g. `RequestSetLevel` requires a `out_tx`).
        if model.apply(update) == Effect::Quit {
            break;
        }
    }
    Ok(())
}

// ── TUI mode ─────────────────────────────────────────────────────────────────

async fn run_tui(
    tx: spillway::Sender<Update>,
    mut rx: spillway::Receiver<Update>,
    out_tx: spillway::Sender<Outgoing>,
    colorize: bool,
    history_budget: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

    // Two flags the keyboard loop reads to dispatch correctly:
    //
    //   modal_kind — which numeric-input modal (if any) currently
    //   owns the keystroke set: digits / `.` / Backspace / Enter /
    //   Esc.  Encoded as a small enum (see MODAL_* constants).
    //
    //   in_graph_view — whether the table or graph view is active.
    //   Controls which top-level binding table applies (e.g. `a`
    //   is bound in graph mode but free in table mode).
    //
    // The runtime keeps both in sync with `model` after each
    // `model.apply`.
    let modal_kind = Arc::new(AtomicU8::new(MODAL_NONE));
    let in_graph_view = Arc::new(AtomicBool::new(false));

    let kb_tx = tx.clone();
    let kb_modal = Arc::clone(&modal_kind);
    let kb_graph = Arc::clone(&in_graph_view);
    std::thread::spawn(move || keyboard_loop(kb_tx, kb_modal, kb_graph));

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal: Terminal<CrosstermBackend<Stdout>> = Terminal::new(backend)?;

    let mut model = Model::new(history_budget);
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    let result: Result<(), Box<dyn std::error::Error>> = (async {
        // Continuous drain via `select!`: the rx.next_batch() arm
        // absorbs Updates as fast as the producer pushes them, and
        // the ticker arm renders exactly once per interval.  An
        // earlier tick-then-drain design paced consumption at
        // `stream_buffer × tick_hz` because draining only happened
        // at tick boundaries — that pinned throughput at a small
        // multiple of the channel capacity even when the model
        // itself could absorb orders of magnitude more.
        loop {
            tokio::select! {
                batch = rx.next_batch() => {
                    match batch {
                        Some(items) => {
                            for update in items {
                                match model.apply(update) {
                                    Effect::None => {}
                                    Effect::Quit => return Ok(()),
                                    Effect::RequestSetLevel(level) => {
                                        let _ = out_tx.send(Outgoing::SetCacheLevel(level));
                                    }
                                    Effect::RequestSetChance(pct) => {
                                        let _ = out_tx.send(Outgoing::SetCacheChance(pct));
                                    }
                                }
                                let kind = if model.chance_input.is_some() {
                                    MODAL_CHANCE
                                } else if let ViewMode::Graph(gs) = &model.view {
                                    if gs.agg_input.is_some() {
                                        MODAL_GRAPH_AGG
                                    } else if gs.window_input.is_some() {
                                        MODAL_GRAPH_WINDOW
                                    } else {
                                        MODAL_NONE
                                    }
                                } else {
                                    MODAL_NONE
                                };
                                modal_kind.store(kind, Ordering::Relaxed);
                                in_graph_view.store(
                                    matches!(model.view, ViewMode::Graph(_)),
                                    Ordering::Relaxed,
                                );
                            }
                        }
                        None => return Ok(()),
                    }
                }
                _ = ticker.tick() => {
                    terminal.draw(|f| view::render(f, &model, colorize))?;
                }
            }
        }
    })
    .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

/// Modal-kind sentinels for the shared `AtomicU8` between the
/// runtime and the keyboard thread.  The runtime stores the
/// currently-open modal after every `model.apply`; the keyboard
/// thread routes keys accordingly.
const MODAL_NONE: u8 = 0;
const MODAL_CHANCE: u8 = 1;
const MODAL_GRAPH_AGG: u8 = 2;
const MODAL_GRAPH_WINDOW: u8 = 3;

fn keyboard_loop(
    tx: spillway::Sender<Update>,
    modal_kind: std::sync::Arc<std::sync::atomic::AtomicU8>,
    in_graph_view: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;

    let send_or_exit = |update: Update| -> std::ops::ControlFlow<()> {
        // Closed → runtime is shutting down, exit.  Full →
        // keystroke dropped (rare with the configured buffer
        // size), keep going.
        if matches!(tx.send(update), Err(spillway::Error::Closed(_))) {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    };

    loop {
        // Short poll so we can notice the runtime shutting down via send-fail.
        match event::poll(Duration::from_millis(200)) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(_) => return,
        }
        let Ok(Event::Key(k)) = event::read() else {
            continue;
        };

        // Modal owns the keystroke set: digits + `.` for characters,
        // Backspace to edit, Enter to commit, Esc to cancel.  Which
        // modal — chance, graph percentile, or graph window — is
        // tracked by the runtime via the shared `modal_kind` flag.
        // Everything else is silently dropped so the user can't
        // escape into a normal command while typing a number.
        let modal = modal_kind.load(Ordering::Relaxed);
        if modal != MODAL_NONE {
            let update = match (modal, k.code) {
                (MODAL_CHANCE, KeyCode::Char(c)) => Update::ChanceInputChar(c),
                (MODAL_CHANCE, KeyCode::Backspace) => Update::ChanceInputBackspace,
                (MODAL_CHANCE, KeyCode::Enter) => Update::ChanceInputCommit,
                (MODAL_CHANCE, KeyCode::Esc) => Update::ChanceInputCancel,
                (MODAL_GRAPH_AGG, KeyCode::Char(c)) => Update::GraphAggInputChar(c),
                (MODAL_GRAPH_AGG, KeyCode::Backspace) => Update::GraphAggInputBackspace,
                (MODAL_GRAPH_AGG, KeyCode::Enter) => Update::GraphAggInputCommit,
                (MODAL_GRAPH_AGG, KeyCode::Esc) => Update::GraphAggInputCancel,
                (MODAL_GRAPH_WINDOW, KeyCode::Char(c)) => Update::GraphWindowInputChar(c),
                (MODAL_GRAPH_WINDOW, KeyCode::Backspace) => Update::GraphWindowInputBackspace,
                (MODAL_GRAPH_WINDOW, KeyCode::Enter) => Update::GraphWindowInputCommit,
                (MODAL_GRAPH_WINDOW, KeyCode::Esc) => Update::GraphWindowInputCancel,
                _ => continue,
            };
            if send_or_exit(update).is_break() {
                return;
            }
            continue;
        }

        // Shift+letter as cache-level meta key.  Terminals report a
        // shifted letter as the uppercase char (with or without the
        // SHIFT modifier flag, depending on protocol) — match on the
        // uppercase character so both wirings work.
        let level = match k.code {
            KeyCode::Char('O') => Some(WireLevelFilter::Off),
            KeyCode::Char('I') => Some(WireLevelFilter::Info),
            KeyCode::Char('D') => Some(WireLevelFilter::Debug),
            KeyCode::Char('T') => Some(WireLevelFilter::Trace),
            _ => None,
        };
        if let Some(level) = level {
            if send_or_exit(Update::RequestCacheLevel(level)).is_break() {
                return;
            }
            continue;
        }

        let in_graph = in_graph_view.load(Ordering::Relaxed);
        // Graph-mode bindings.  `g` and `Esc` exit graph mode; the
        // rest configure the view.  When `gs.focus == Details` the
        // model itself routes j/k/Space to the split-keys cursor;
        // we always emit GraphSelectUp/Down/Toggle here.  When in
        // Chart focus, `Tab` switches focus into Details.
        if in_graph {
            let update = match k.code {
                KeyCode::Char('q') => Update::Quit,
                KeyCode::Char('g') | KeyCode::Esc => Update::ToggleGraph,
                // `a` opens the aggregation-input modal; the buffer
                // accepts `a`/`avg`, `min`, `max`, or `pX[.XX]`.
                KeyCode::Char('a') => Update::BeginGraphAggInput,
                KeyCode::Char('t') => Update::ToggleGraphMetric,
                KeyCode::Char('w') => Update::BeginGraphWindowInput,
                KeyCode::Tab | KeyCode::BackTab => Update::GraphSwitchFocus,
                KeyCode::Down | KeyCode::Char('j') => Update::GraphSelectDown,
                KeyCode::Up | KeyCode::Char('k') => Update::GraphSelectUp,
                // Left/Right cycle the series table's leading sort
                // column.  Underlined in the expanded details pane.
                KeyCode::Left | KeyCode::Char('h') => Update::GraphSortColumnLeft,
                KeyCode::Right | KeyCode::Char('l') => Update::GraphSortColumnRight,
                KeyCode::Char(' ') => Update::GraphToggleSplit,
                _ => continue,
            };
            if send_or_exit(update).is_break() {
                return;
            }
            continue;
        }

        // Table-mode bindings (the original set + the new `g`).
        let update = match k.code {
            KeyCode::Char('q') | KeyCode::Esc => Update::Quit,
            KeyCode::Down | KeyCode::Char('j') => Update::SelectDown,
            KeyCode::Up | KeyCode::Char('k') => Update::SelectUp,
            KeyCode::Right | KeyCode::Char('l') => Update::ExpandSelected,
            KeyCode::Enter => Update::ExpandAllSelected,
            KeyCode::Left | KeyCode::Char('h') => Update::CollapseSelected,
            KeyCode::Tab | KeyCode::BackTab => Update::SwitchFocus,
            KeyCode::Char(' ') => Update::ToggleSplitSelected,
            // `Shift+C` (i.e. uppercase `C`) opens the chance-input
            // area.  The runtime echoes model.chance_input.is_some()
            // back into the shared flag so subsequent keys (digits,
            // `.`, Backspace, Enter, Esc) route into input mode.
            KeyCode::Char('C') => Update::BeginChanceInput,
            // `g` enters graph mode locked onto the current row.
            KeyCode::Char('g') => Update::ToggleGraph,
            _ => continue,
        };
        if send_or_exit(update).is_break() {
            return;
        }
    }
}

// ── view (ratatui) ───────────────────────────────────────────────────────────

mod view {
    use super::*;
    use std::collections::BTreeMap;

    use tracing_console_host::{WireLevel, WireLevelFilter, WireSpan};

    use crate::aggregate::fmt_ns;
    use crate::model::Focus;

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

    /// Format the rolling 10-second receive rate (from [`Model::rate`])
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

    fn level_str(level: WireLevel) -> &'static str {
        match level {
            WireLevel::Trace => "T",
            WireLevel::Debug => "D",
            WireLevel::Info => "I",
            WireLevel::Warn => "W",
            WireLevel::Error => "E",
        }
    }

    // Solarized accent palette — used as a heat ramp from cool (low
    // values) to hot (column max).  Pulled into the bin function so
    // the colours live next to the thresholds that select them.
    const SOL_CYAN: Color = Color::Rgb(0x2a, 0xa1, 0x98);
    const SOL_GREEN: Color = Color::Rgb(0x85, 0x99, 0x00);
    const SOL_YELLOW: Color = Color::Rgb(0xb5, 0x89, 0x00);
    const SOL_ORANGE: Color = Color::Rgb(0xcb, 0x4b, 0x16);
    const SOL_RED: Color = Color::Rgb(0xdc, 0x32, 0x2f);

    /// Map a 0..1 intensity to a heat colour, or `None` for "leave the
    /// terminal default".  Low intensities stay uncoloured so the eye
    /// only catches the warm cells.
    fn heat(intensity: f64) -> Option<Color> {
        if intensity < 0.40 {
            None
        } else if intensity < 0.60 {
            Some(SOL_CYAN)
        } else if intensity < 0.78 {
            Some(SOL_GREEN)
        } else if intensity < 0.90 {
            Some(SOL_YELLOW)
        } else if intensity < 0.99 {
            Some(SOL_ORANGE)
        } else {
            Some(SOL_RED)
        }
    }

    /// For each visible row's immediate-ancestor index in the same
    /// list (`None` for roots).  Rows are in DFS order, so the
    /// ancestor index for row `r` is the most recent earlier row
    /// whose stack length is exactly `r.depth`.  Computed in one pass.
    fn parent_indices(rows: &[crate::model::VisibleRow]) -> Vec<Option<usize>> {
        let mut parents = vec![None; rows.len()];
        // (depth, index) stack of in-scope ancestors as we DFS.
        let mut anc: Vec<(usize, usize)> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            while anc.last().map(|(d, _)| *d >= r.depth).unwrap_or(false) {
                anc.pop();
            }
            parents[i] = anc.last().map(|(_, idx)| *idx);
            anc.push((r.depth, i));
        }
        parents
    }

    /// Build the per-cell colour map: `colors[row][column]` for the
    /// 7 numeric columns (count, total min/avg/max, self min/avg/max).
    /// Each column is normalised against its own max; per-row effective
    /// intensity is `min(self_intensity, parent_effective)` so a
    /// descendant never reads as hotter than its ancestor — the
    /// ancestor's own intensity caps the descendant.  Roots show
    /// their own intensity unchanged.
    fn build_color_map(rows: &[crate::model::VisibleRow]) -> Vec<[Option<Color>; 7]> {
        let n = rows.len();
        if n == 0 {
            return Vec::new();
        }
        // Pull values per column.
        let mut vals: [Vec<u64>; 7] = Default::default();
        for r in rows {
            vals[0].push(r.stats.count);
            vals[1].push(r.stats.total_min_ns);
            vals[2].push(r.stats.total_avg_ns());
            vals[3].push(r.stats.total_max_ns);
            vals[4].push(r.stats.self_min_ns);
            vals[5].push(r.stats.self_avg_ns());
            vals[6].push(r.stats.self_max_ns);
        }
        let max: [u64; 7] = std::array::from_fn(|c| *vals[c].iter().max().unwrap_or(&0));

        let parents = parent_indices(rows);
        let mut effective: Vec<[f64; 7]> = vec![[0.0; 7]; n];
        for i in 0..n {
            for c in 0..7 {
                let self_int = if max[c] == 0 {
                    0.0
                } else {
                    vals[c][i] as f64 / max[c] as f64
                };
                effective[i][c] = match parents[i] {
                    Some(p) => self_int.min(effective[p][c]),
                    None => self_int,
                };
            }
        }
        effective
            .into_iter()
            .map(|row| std::array::from_fn(|c| heat(row[c])))
            .collect()
    }

    pub fn render(f: &mut ratatui::Frame<'_>, model: &Model, colorize: bool) {
        let area = f.area();
        match &model.view {
            ViewMode::Table => render_table(f, area, model, colorize),
            ViewMode::Graph(gs) => render_graph(f, area, model, gs, colorize),
        }
    }

    fn render_table(
        f: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        model: &Model,
        colorize: bool,
    ) {
        // Pane proportions swap based on focus: when Details is
        // focused it grabs the larger pane so the user can browse all
        // candidate split keys + full distinguishing-value lists with
        // room.  Header keeps its fixed 1-line slot.
        let (stacks_constraint, details_constraint) = match model.focus {
            Focus::Stacks => (Constraint::Min(8), Constraint::Length(10)),
            Focus::Details => (Constraint::Length(10), Constraint::Min(8)),
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), stacks_constraint, details_constraint])
            .split(area);

        // Top: connection status + cache-level switcher.
        //
        // When connected, the header reads:
        //   [connected]  Off Info Debug Trace   N spans buffered
        //
        // * The label matching `model.cache_level` is bold + green
        //   (the server-confirmed current level).  Until the server
        //   pushes its first `CacheLevel`, no label is highlighted.
        // * Each label's first letter is underlined as a hint for
        //   the Shift+letter shortcut that requests that level.  The
        //   green selection only moves when the server confirms.
        let header: Line = match &model.connection {
            ConnectionStatus::Connecting => Line::from(vec![
                TuiSpan::raw("[connecting] "),
                TuiSpan::styled(
                    model.status.clone().unwrap_or_default(),
                    Style::default().add_modifier(Modifier::DIM),
                ),
            ]),
            ConnectionStatus::Connected => {
                let mut spans: Vec<TuiSpan<'static>> =
                    vec![TuiSpan::raw("[connected]  ")];
                level_switcher_spans(&mut spans, model);
                spans.push(TuiSpan::raw("  "));
                chance_switcher_spans(&mut spans, model);
                spans.push(TuiSpan::raw(format!(
                    "   {n} spans / {rate}",
                    n = model.agg.len(),
                    rate = format_span_rate(model),
                )));
                Line::from(spans)
            }
            ConnectionStatus::Disconnected(reason) => {
                Line::from(format!("[disconnected] {reason}"))
            }
        };
        f.render_widget(Paragraph::new(header), chunks[0]);

        // Middle: hierarchical aggregated tree as a Table so the
        // measurement columns sit at fixed positions regardless of
        // label width.  Same bucketing as `--stats`; depth-based
        // indentation + ▶/▼ markers in the label column.
        let rows = model.visible_rows();
        let selected = if rows.is_empty() {
            None
        } else {
            Some(model.selected.min(rows.len() - 1))
        };

        let dim = Style::default().add_modifier(Modifier::DIM);
        let right = |s: String, color: Option<Color>| {
            let mut style = Style::default();
            if let Some(c) = color {
                style = style.fg(c);
            }
            Cell::from(Line::from(s).alignment(Alignment::Right)).style(style)
        };
        let color_map = if colorize {
            build_color_map(&rows)
        } else {
            Vec::new()
        };
        let cell_color = |row_idx: usize, col: usize| -> Option<Color> {
            if !colorize {
                return None;
            }
            color_map.get(row_idx).and_then(|cs| cs[col])
        };
        // Soft visual hint between column groups — a dim "│" lives in
        // its own 1-wide column.  Eye sees the break without a hard
        // rule running the full table height.
        let sep_cell = || Cell::from(Line::from("│").alignment(Alignment::Center)).style(dim);
        // Two-line header cell.  Top line carries the section label
        // ("total" / "self") above the middle of its group; bottom
        // line carries the per-column label (min/avg/max etc).
        let hcell = |top: &'static str, bot: &'static str, align: Alignment| -> Cell<'static> {
            Cell::from(Text::from(vec![
                Line::from(top).alignment(align),
                Line::from(bot).alignment(align),
            ]))
        };

        let header = Row::new(vec![
            hcell("", "stack", Alignment::Left),
            hcell("", "n", Alignment::Right),
            hcell("", "│", Alignment::Center),
            hcell("", "min", Alignment::Right),
            // "total" lands above the middle (avg) column so it reads
            // as a label for the whole 3-column group.
            hcell("total", "avg", Alignment::Right),
            hcell("", "max", Alignment::Right),
            hcell("", "│", Alignment::Center),
            hcell("", "min", Alignment::Right),
            hcell("self", "avg", Alignment::Right),
            hcell("", "max", Alignment::Right),
        ])
        .height(2)
        .style(dim);

        let table_rows: Vec<Row> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                // ▶/▼ — standard tree disclosure pair, both 1-cell.
                let marker = if r.has_children {
                    if r.is_expanded { "▼ " } else { "▶ " }
                } else {
                    "  "
                };
                let indent = "  ".repeat(r.depth);
                let leaf = r.key.stack.last().map(String::as_str).unwrap_or("");
                // Splits annotate the row that *introduces* a new
                // splits-group, not every descendant.  Rows are sorted
                // by `(splits, stack)` so the first row of each group
                // (and only that row) carries the `[k=v, …]` suffix —
                // children inherit silently, matching the user's
                // mental model that the distinguishing key lives on
                // the span where it was actually set.
                let introduces_splits =
                    !r.key.splits.is_empty() && (i == 0 || rows[i - 1].key.splits != r.key.splits);
                let splits_suffix = if introduces_splits {
                    let parts: Vec<String> = r
                        .key
                        .splits
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect();
                    format!("  [{}]", parts.join(", "))
                } else {
                    String::new()
                };
                let label = format!("{indent}{marker}{leaf}{splits_suffix}");
                Row::new(vec![
                    Cell::from(label),
                    right(r.stats.count.to_string(), cell_color(i, 0)),
                    sep_cell(),
                    right(fmt_ns(r.stats.total_min_ns), cell_color(i, 1)),
                    right(fmt_ns(r.stats.total_avg_ns()), cell_color(i, 2)),
                    right(fmt_ns(r.stats.total_max_ns), cell_color(i, 3)),
                    sep_cell(),
                    right(fmt_ns(r.stats.self_min_ns), cell_color(i, 4)),
                    right(fmt_ns(r.stats.self_avg_ns()), cell_color(i, 5)),
                    right(fmt_ns(r.stats.self_max_ns), cell_color(i, 6)),
                ])
            })
            .collect();

        let stacks_focused = model.focus == Focus::Stacks;
        let title = format!(
            " stacks{focus_marker}  ({n}) ",
            focus_marker = if stacks_focused { " ◆" } else { "" },
            n = rows.len(),
        );
        let table = Table::new(
            table_rows,
            [
                Constraint::Min(20),   // stack label — takes remaining width
                Constraint::Length(7), // n
                Constraint::Length(1), // sep
                Constraint::Length(8), // tot min
                Constraint::Length(8), // tot avg
                Constraint::Length(8), // tot max
                Constraint::Length(1), // sep
                Constraint::Length(8), // self min
                Constraint::Length(8), // self avg
                Constraint::Length(8), // self max
            ],
        )
        .header(header)
        .column_spacing(1)
        .block(Block::default().title(title).borders(Borders::ALL))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        let mut state = TableState::default();
        state.select(selected);
        f.render_stateful_widget(table, chunks[1], &mut state);

        // Bottom: details pane.  When Stacks is focused, it shows a
        // compact summary (stack path, distinguishing/constant fields,
        // event bucket totals).  When Details is focused, the pane
        // grows and renders the candidate-split-keys list; the user
        // navigates it with j/k and toggles a key in/out of
        // `split_keys` with Space.
        let details_focused = model.focus == Focus::Details;
        let details_title = format!(
            " details{focus_marker} ",
            focus_marker = if details_focused { " ◆" } else { "" },
        );

        // Build matching-span set + field/event distributions for the
        // selected stack — shared between both focus modes.
        let mut detail_lines: Vec<Line> = Vec::new();
        if let Some(idx) = selected {
            let r = &rows[idx];

            // Resolve which spans match the selected stack via the
            // aggregator's cached resolved stacks — same result as
            // the old `bucket_key` walk, but already computed at
            // span-arrival time.
            let matching: Vec<&WireSpan> = model
                .agg
                .iter_with_stack()
                .filter(|(_, stack)| stack == &&r.key.stack)
                .map(|(s, _)| s)
                .collect();

            detail_lines.push(Line::from(format!("stack:  {}", r.key.stack.join(" › "))));
            detail_lines.push(Line::from(format!(
                "n={}  matched={}  total avg: {}  self avg: {}",
                r.stats.count,
                matching.len(),
                fmt_ns(r.stats.total_avg_ns()),
                fmt_ns(r.stats.self_avg_ns()),
            )));
            if !model.split_keys().is_empty() {
                let split_list: Vec<String> = model.split_keys().iter().cloned().collect();
                detail_lines.push(Line::from(format!("split by: {}", split_list.join(", "),)));
            }

            // Field distribution.
            let mut field_dist: BTreeMap<&str, BTreeMap<String, u32>> = BTreeMap::new();
            for s in &matching {
                for (k, v) in &s.fields {
                    *field_dist
                        .entry(k.as_str())
                        .or_default()
                        .entry(v.to_string_value())
                        .or_default() += 1;
                }
            }
            let (distinguishing, constant): (Vec<_>, Vec<_>) =
                field_dist.iter().partition(|(_, vals)| vals.len() > 1);

            if !distinguishing.is_empty() {
                detail_lines.push(Line::from("fields (distinguishing):"));
                let show_per_key = if details_focused { 20 } else { 5 };
                for (k, vals) in &distinguishing {
                    let mut entries: Vec<(&String, &u32)> = vals.iter().collect();
                    // Alphabetical by value — stable order across
                    // renders is more useful than count-rank when
                    // the user's scanning for a specific value.
                    entries.sort_by(|a, b| a.0.cmp(b.0));
                    let shown = entries
                        .iter()
                        .take(show_per_key)
                        .map(|(v, c)| format!("{v}×{c}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let suffix = if entries.len() > show_per_key {
                        format!(", +{} more", entries.len() - show_per_key)
                    } else {
                        String::new()
                    };
                    detail_lines.push(Line::from(format!("  {k} = {shown}{suffix}")));
                }
            }
            if !constant.is_empty() {
                let summary = constant
                    .iter()
                    .map(|(k, vals)| {
                        let only = vals.keys().next().map(String::as_str).unwrap_or("");
                        format!("{k}={only}")
                    })
                    .collect::<Vec<_>>()
                    .join("  ");
                detail_lines.push(Line::from(format!("fields (constant): {summary}")));
            }

            // Event summary.
            let mut event_dist: BTreeMap<&str, (u32, &str)> = BTreeMap::new();
            for s in &matching {
                for e in &s.events {
                    let entry = event_dist
                        .entry(e.name.as_str())
                        .or_insert((0, level_str(e.level)));
                    entry.0 += 1;
                }
            }
            if !event_dist.is_empty() {
                let total: u32 = event_dist.values().map(|(c, _)| c).sum();
                let mut entries: Vec<(&&str, &(u32, &str))> = event_dist.iter().collect();
                entries.sort_by(|a, b| b.1.0.cmp(&a.1.0).then(a.0.cmp(b.0)));
                let summary = entries
                    .iter()
                    .map(|(name, (count, lvl))| format!("{name}[{lvl}]×{count}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                detail_lines.push(Line::from(format!("events ({total}): {summary}")));
            }

            // When Details is focused, append the candidate-keys
            // section: a list the user navigates with j/k and toggles
            // with Space.  Selected key is reversed; checked keys
            // (already in split_keys) get a [✓] marker.
            if details_focused {
                detail_lines.push(Line::from(""));
                detail_lines.push(Line::from(
                    "metadata keys (Space to split/unsplit, Tab to leave):",
                ));
                let candidates = model.candidate_split_keys();
                let sel = model
                    .details_selected
                    .min(candidates.len().saturating_sub(1));
                if candidates.is_empty() {
                    detail_lines.push(Line::from("  (no metadata keys present on matching spans)"));
                } else {
                    for (i, k) in candidates.iter().enumerate() {
                        let checked = model.split_keys().contains(k);
                        let mark = if checked { "[✓]" } else { "[ ]" };
                        let line_text = format!("  {mark} {k}");
                        let style = if i == sel {
                            Style::default().add_modifier(Modifier::REVERSED)
                        } else {
                            Style::default()
                        };
                        detail_lines.push(Line::from(TuiSpan::styled(line_text, style)));
                    }
                }
            }
        } else {
            detail_lines.push(Line::from(
                "(no spans yet — q quit, j/k move, →/l expand, Enter expand all, ←/h collapse, Tab focus details)",
            ));
        };
        let detail = Paragraph::new(detail_lines)
            .block(Block::default().title(details_title).borders(Borders::ALL));
        f.render_widget(detail, chunks[2]);
    }

    // ── Graph view ──────────────────────────────────────────────

    use ratatui::widgets::{Axis, Chart, Dataset, GraphType};
    use ratatui::symbols;
    use crate::model::{
        AggMode, GraphFocus, GraphState, Metric, SeriesProjection, SeriesSummary, SortColumn,
    };

    /// Same palette as the rest of the TUI, rotated round-robin per
    /// series so each line stays a stable colour across renders.
    const SERIES_PALETTE: &[Color] = &[
        Color::Cyan,
        Color::Magenta,
        Color::Yellow,
        Color::Green,
        Color::Red,
        Color::LightBlue,
        Color::LightGreen,
        Color::LightYellow,
    ];

    fn series_color(idx: usize, colorize: bool) -> Color {
        if !colorize {
            Color::White
        } else {
            SERIES_PALETTE[idx % SERIES_PALETTE.len()]
        }
    }

    fn agg_label(mode: AggMode) -> String {
        match mode {
            AggMode::Min => "min".into(),
            AggMode::Max => "max".into(),
            AggMode::Avg => "avg".into(),
            AggMode::Percentile(p) => {
                if (p.round() - p).abs() < 1e-6 {
                    format!("p{:.0}", p)
                } else {
                    format!("p{p}")
                }
            }
        }
    }

    fn metric_label(metric: Metric) -> &'static str {
        match metric {
            Metric::Total => "total",
            Metric::SelfTime => "self",
        }
    }

    fn series_legend(key: &[(String, String)]) -> String {
        if key.is_empty() {
            "(all)".into()
        } else {
            key.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }

    /// Pick a "nice" axis step for a span of `span_secs`.  Returns
    /// the step (in seconds) and labels positioned at multiples of
    /// the step, ending at `now` (= 0).
    fn wall_clock_labels(span_secs: f64) -> Vec<ratatui::text::Span<'static>> {
        let target_steps = 4.0;
        let raw = (span_secs / target_steps).max(0.001);
        let pow = 10f64.powf(raw.log10().floor());
        let candidates = [1.0, 2.0, 5.0, 10.0];
        let step = candidates
            .iter()
            .map(|c| c * pow)
            .find(|s| span_secs / *s <= target_steps + 0.5)
            .unwrap_or(raw);

        let mut out = Vec::new();
        let mut t = 0.0_f64;
        while t <= span_secs + 1e-6 {
            let label = if t < 1e-6 {
                "now".into()
            } else {
                format_seconds(t)
            };
            out.push(ratatui::text::Span::raw(format!("-{}", label).replace("-now", "now")));
            t += step;
        }
        out.reverse();
        out
    }

    fn format_seconds(s: f64) -> String {
        if s >= 60.0 {
            let m = (s / 60.0).round();
            if (m * 60.0 - s).abs() < 0.5 {
                format!("{m:.0}m")
            } else {
                format!("{:.1}m", s / 60.0)
            }
        } else if s >= 1.0 {
            if (s.round() - s).abs() < 0.05 {
                format!("{s:.0}s")
            } else {
                format!("{s:.1}s")
            }
        } else {
            format!("{}ms", (s * 1000.0).round() as u64)
        }
    }

    fn ns_axis_labels(y_max: f64) -> Vec<ratatui::text::Span<'static>> {
        let n_ticks = 4;
        let step = if y_max <= 0.0 { 1.0 } else { y_max / n_ticks as f64 };
        (0..=n_ticks)
            .map(|i| {
                let v = (i as f64) * step;
                ratatui::text::Span::raw(crate::aggregate::fmt_ns(v as u64))
            })
            .collect()
    }

    fn render_graph(
        f: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        model: &Model,
        gs: &GraphState,
        colorize: bool,
    ) {
        let (chart_c, details_c) = match gs.focus {
            GraphFocus::Chart => (Constraint::Min(8), Constraint::Length(12)),
            GraphFocus::Details => (Constraint::Length(8), Constraint::Min(12)),
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), chart_c, details_c])
            .split(area);

        // Header: same connection/level/chance line as the table view.
        render_header(f, chunks[0], model);

        // Chart pane.  Project the series store into ratatui datasets;
        // x_max is "this many seconds of history we're willing to
        // show" — capped so the chart isn't dominated by empty bins
        // after the user just relocked / reset.
        let x_max_secs = (gs.window_secs * 60.0).max(gs.window_secs);
        let projections = gs.store.project(gs.aggregation, x_max_secs);
        // Use `color_index_of` (= alphabetical rank) for colour
        // assignment so the chart's colours stay stable regardless
        // of how the user sorts the details table or whether they
        // hide some series — toggling visibility doesn't reshuffle
        // the rest, and the chart line for a given series always
        // matches its detail-pane row colour.
        let series: Vec<(SeriesProjection, String, Color)> = projections
            .into_iter()
            .filter_map(|p| {
                if gs.hidden_series.contains(&p.key) {
                    None
                } else {
                    let label = series_legend(&p.key);
                    let color = series_color(gs.color_index_of(&p.key), colorize);
                    Some((p, label, color))
                }
            })
            .collect();
        let datasets: Vec<Dataset<'_>> = series
            .iter()
            .map(|(proj, label, color)| {
                Dataset::default()
                    .name(label.as_str())
                    .marker(symbols::Marker::Braille)
                    .graph_type(GraphType::Line)
                    .style(Style::default().fg(*color))
                    .data(proj.points.as_slice())
            })
            .collect();
        let y_max = series
            .iter()
            .flat_map(|(p, ..)| p.points.iter().map(|(_, y)| *y))
            .fold(0.0_f64, f64::max);

        let title = format!(
            " {label} — {agg} {metric} / {win:.2}s window ",
            label = gs.locked_stack.join(" › "),
            agg = agg_label(gs.aggregation),
            metric = metric_label(gs.metric),
            win = gs.window_secs,
        );

        let x_axis = Axis::default()
            .style(Style::default().add_modifier(Modifier::DIM))
            .bounds([-x_max_secs, 0.0])
            .labels(wall_clock_labels(x_max_secs));
        let y_axis = Axis::default()
            .style(Style::default().add_modifier(Modifier::DIM))
            .bounds([0.0, y_max.max(1.0)])
            .labels(ns_axis_labels(y_max.max(1.0)));

        let chart = Chart::new(datasets)
            .block(Block::default().title(title).borders(Borders::ALL))
            .x_axis(x_axis)
            .y_axis(y_axis);
        f.render_widget(chart, chunks[1]);

        render_graph_details(f, chunks[2], model, gs, colorize);
    }

    /// Re-render the header line (connection status + cache-level +
    /// chance + span count) used by both table and graph views.
    fn render_header(f: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, model: &Model) {
        let header = match &model.connection {
            ConnectionStatus::Connecting => Line::from(vec![
                TuiSpan::raw("[connecting] "),
                TuiSpan::styled(
                    model.status.clone().unwrap_or_default(),
                    Style::default().add_modifier(Modifier::DIM),
                ),
            ]),
            ConnectionStatus::Connected => {
                let mut spans: Vec<TuiSpan<'static>> = vec![TuiSpan::raw("[connected]  ")];
                level_switcher_spans(&mut spans, model);
                spans.push(TuiSpan::raw("  "));
                chance_switcher_spans(&mut spans, model);
                spans.push(TuiSpan::raw(format!(
                    "   {n} spans / {rate}",
                    n = model.agg.len(),
                    rate = format_span_rate(model),
                )));
                Line::from(spans)
            }
            ConnectionStatus::Disconnected(reason) => {
                Line::from(format!("[disconnected] {reason}"))
            }
        };
        f.render_widget(Paragraph::new(header), area);
    }

    /// Render the "agg:   …" detail-pane row.  When the input modal
    /// is active the value cell is shown as a white-on-default
    /// highlighted input box with a trailing cursor; otherwise the
    /// current aggregation label plus a short hint.
    fn agg_field_line(gs: &GraphState) -> Line<'static> {
        let mut spans = vec![TuiSpan::raw("agg:      ")];
        match &gs.agg_input {
            Some(buf) => {
                let body = if buf.is_empty() {
                    " ".to_string()
                } else {
                    buf.clone()
                };
                spans.push(TuiSpan::styled(
                    body,
                    Style::default()
                        .bg(Color::White)
                        .fg(Color::Black)
                        .add_modifier(Modifier::BOLD),
                ));
                spans.push(TuiSpan::styled(
                    "_",
                    Style::default()
                        .bg(Color::White)
                        .fg(Color::Black)
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD),
                ));
                spans.push(TuiSpan::raw("   (a/avg, min, max, pX[.XX]; Enter commit, Esc cancel)"));
            }
            None => {
                spans.push(TuiSpan::raw(format!(
                    "{}            (press a to edit)",
                    agg_label(gs.aggregation)
                )));
            }
        }
        Line::from(spans)
    }

    /// Render the "window: …" detail-pane row, with the same
    /// highlighted-input treatment when its modal is active.
    fn window_field_line(gs: &GraphState) -> Line<'static> {
        let mut spans = vec![TuiSpan::raw("window:   ")];
        match &gs.window_input {
            Some(buf) => {
                let body = if buf.is_empty() {
                    " ".to_string()
                } else {
                    buf.clone()
                };
                spans.push(TuiSpan::styled(
                    body,
                    Style::default()
                        .bg(Color::White)
                        .fg(Color::Black)
                        .add_modifier(Modifier::BOLD),
                ));
                spans.push(TuiSpan::styled(
                    "_",
                    Style::default()
                        .bg(Color::White)
                        .fg(Color::Black)
                        .add_modifier(Modifier::REVERSED | Modifier::BOLD),
                ));
                spans.push(TuiSpan::raw("   (positive seconds; Enter commit, Esc cancel)"));
            }
            None => {
                spans.push(TuiSpan::raw(format!(
                    "{:.2}s            (press w to edit)",
                    gs.window_secs
                )));
            }
        }
        Line::from(spans)
    }

    /// Build the columnar series-toggle table shown in both the
    /// compact and expanded details pane.  Returns the rendered
    /// lines and, when `cursor_series_idx` points at a series row,
    /// the absolute line index within the returned vec so the
    /// caller can drive `Paragraph::scroll` to keep the cursor in
    /// view.
    /// Build the columnar series-toggle table.  Returns
    /// `(header_line, body_rows, body_cursor_index)`.  The header
    /// is `None` only when there are no series — callers then
    /// surface the placeholder message that lives in `body_rows`.
    /// The cursor index is into `body_rows`, so callers can offset
    /// it against whatever they prepend.
    fn series_table_lines(
        gs: &GraphState,
        cursor_series_idx: Option<usize>,
        colorize: bool,
        show_sort_underline: bool,
    ) -> (Option<Line<'static>>, Vec<Line<'static>>, Option<usize>) {
        use std::fmt::Write;

        let series_keys = gs.series_keys();
        if series_keys.is_empty() {
            return (
                None,
                vec![Line::from("  (no series yet)")],
                None,
            );
        }

        let summaries = gs.store.series_summary(gs.aggregation);
        let summary_by_key: std::collections::HashMap<
            Vec<(String, String)>,
            SeriesSummary,
        > = summaries.into_iter().map(|s| (s.key.clone(), s)).collect();

        let split_cols: Vec<String> = gs.split_keys.iter().cloned().collect();
        let mut split_widths: Vec<usize> =
            split_cols.iter().map(|k| k.chars().count()).collect();

        // Stable colour slot per series (alphabetical rank) so
        // re-sorting the table doesn't reshuffle colours on the
        // chart.
        let alpha = gs.alpha_series_keys();
        let color_idx_of = |key: &[(String, String)]| -> usize {
            alpha
                .iter()
                .position(|k| k.as_slice() == key)
                .unwrap_or(0)
        };

        struct Row {
            color_idx: usize,
            visible: bool,
            split_vals: Vec<String>,
            n: String,
            min: String,
            avg: String,
            max: String,
            last: String,
        }
        let mut rows: Vec<Row> = Vec::with_capacity(series_keys.len());
        for key in &series_keys {
            let s = summary_by_key.get(key);
            let split_vals: Vec<String> = split_cols
                .iter()
                .map(|sk| {
                    key.iter()
                        .find(|(k, _)| k == sk)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| "—".to_string())
                })
                .collect();
            for (i, v) in split_vals.iter().enumerate() {
                split_widths[i] = split_widths[i].max(v.chars().count());
            }
            let n = s.map(|s| s.count.to_string()).unwrap_or_else(|| "0".into());
            let min = crate::aggregate::fmt_ns(s.map(|s| s.min_ns).unwrap_or(0));
            let avg = crate::aggregate::fmt_ns(s.map(|s| s.avg_ns).unwrap_or(0));
            let max = crate::aggregate::fmt_ns(s.map(|s| s.max_ns).unwrap_or(0));
            let last = crate::aggregate::fmt_ns(s.map(|s| s.last_ns).unwrap_or(0));
            rows.push(Row {
                color_idx: color_idx_of(key),
                visible: !gs.hidden_series.contains(key),
                split_vals,
                n,
                min,
                avg,
                max,
                last,
            });
        }

        let stat_headers = ["n", "min", "avg", "max", "last"];
        let stat_columns =
            [SortColumn::Count, SortColumn::Min, SortColumn::Avg, SortColumn::Max, SortColumn::Last];
        let mut stat_widths: [usize; 5] = [
            stat_headers[0].len(),
            stat_headers[1].len(),
            stat_headers[2].len(),
            stat_headers[3].len(),
            stat_headers[4].len(),
        ];
        for r in &rows {
            stat_widths[0] = stat_widths[0].max(r.n.chars().count());
            stat_widths[1] = stat_widths[1].max(r.min.chars().count());
            stat_widths[2] = stat_widths[2].max(r.avg.chars().count());
            stat_widths[3] = stat_widths[3].max(r.max.chars().count());
            stat_widths[4] = stat_widths[4].max(r.last.chars().count());
        }

        // Header — one span per cell so we can selectively underline
        // the active sort column while keeping the rest dim.
        let dim = Style::default().add_modifier(Modifier::DIM);
        let underline =
            Style::default().add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
        let mut header_spans: Vec<TuiSpan<'static>> =
            vec![TuiSpan::styled("      ", dim)];
        for (i, c) in split_cols.iter().enumerate() {
            let cell = format!("{:<w$}", c, w = split_widths[i]);
            let is_active = show_sort_underline
                && matches!(&gs.sort_column, SortColumn::SplitKey(k) if k == c);
            let style = if is_active { underline } else { dim };
            header_spans.push(TuiSpan::styled(cell, style));
            header_spans.push(TuiSpan::styled("  ", dim));
        }
        for (i, h) in stat_headers.iter().enumerate() {
            let cell = format!("{:>w$}", h, w = stat_widths[i]);
            let is_active =
                show_sort_underline && gs.sort_column == stat_columns[i];
            let style = if is_active { underline } else { dim };
            header_spans.push(TuiSpan::styled(cell, style));
            if i + 1 < stat_headers.len() {
                header_spans.push(TuiSpan::styled("  ", dim));
            }
        }
        let header_line = Line::from(header_spans);

        // Data rows go into a separate vec so the caller can keep
        // the header sticky while only the rows scroll.
        let mut body_rows: Vec<Line<'static>> = Vec::with_capacity(rows.len());
        let mut cursor_line: Option<usize> = None;
        for (i, r) in rows.iter().enumerate() {
            let mark = if r.visible { "[✓]" } else { "[ ]" };
            let mut row = format!("  {mark} ");
            for (j, v) in r.split_vals.iter().enumerate() {
                let _ = write!(row, "{:<w$}  ", v, w = split_widths[j]);
            }
            let _ = write!(row, "{:>w$}  ", r.n, w = stat_widths[0]);
            let _ = write!(row, "{:>w$}  ", r.min, w = stat_widths[1]);
            let _ = write!(row, "{:>w$}  ", r.avg, w = stat_widths[2]);
            let _ = write!(row, "{:>w$}  ", r.max, w = stat_widths[3]);
            let _ = write!(row, "{:>w$}", r.last, w = stat_widths[4]);

            let color = series_color(r.color_idx, colorize);
            let on_cursor = cursor_series_idx == Some(i);
            if on_cursor {
                cursor_line = Some(body_rows.len());
            }
            let mut style = Style::default().fg(color);
            if !r.visible {
                style = style.add_modifier(Modifier::DIM);
            }
            if on_cursor {
                style = style.add_modifier(Modifier::REVERSED);
            }
            body_rows.push(Line::from(TuiSpan::styled(row, style)));
        }

        (Some(header_line), body_rows, cursor_line)
    }

    fn render_graph_details(
        f: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        model: &Model,
        gs: &GraphState,
        colorize: bool,
    ) {
        let focused = gs.focus == GraphFocus::Details;
        let title = format!(" graph details{} ", if focused { " ◆" } else { "" });

        // Sticky lines stay pinned at the top of the pane; body
        // lines scroll beneath them so the agg / column-header rows
        // remain visible even as the user scrolls through a long
        // series list.
        let mut sticky: Vec<Line<'static>> = Vec::new();
        let mut body: Vec<Line<'static>> = Vec::new();
        let mut body_cursor: Option<usize> = None;

        if focused {
            // Sticky: the legend's config rows + the series help
            // line + the table header.
            sticky.push(Line::from(format!(
                "stack:    {}",
                gs.locked_stack.join(" › ")
            )));
            sticky.push(agg_field_line(gs));
            sticky.push(Line::from(format!(
                "metric:   {}            (press t to swap)",
                metric_label(gs.metric)
            )));
            sticky.push(window_field_line(gs));
            if !gs.split_keys.is_empty() {
                sticky.push(Line::from(format!(
                    "splits:   {}",
                    gs.split_keys.iter().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
            sticky.push(Line::from(""));
            sticky.push(Line::from(
                "series  (Space toggles visibility; ←/→ change sort column):",
            ));

            let series_keys = gs.series_keys();
            let candidates = crate::aggregate::candidate_split_keys_for(
                &model.agg,
                &gs.locked_stack,
            );
            let combined_len = series_keys.len() + candidates.len();
            let sel = if combined_len == 0 {
                usize::MAX
            } else {
                gs.details_selected.min(combined_len - 1)
            };
            let series_cursor = if sel != usize::MAX && sel < series_keys.len() {
                Some(sel)
            } else {
                None
            };

            // Table.  Header goes into sticky; data rows + the
            // metadata-keys section go into body.
            let (table_header, table_rows, table_cursor) = series_table_lines(
                gs,
                series_cursor,
                colorize,
                /* show_sort_underline */ true,
            );
            if let Some(h) = table_header {
                sticky.push(h);
            }
            let body_start = body.len();
            body.extend(table_rows);
            if let Some(rel) = table_cursor {
                body_cursor = Some(body_start + rel);
            }
            body.push(Line::from(""));
            body.push(Line::from(
                "metadata keys  (Space splits/unsplits, Tab to leave):",
            ));
            if candidates.is_empty() {
                body.push(Line::from(
                    "  (no metadata keys present on matching spans)",
                ));
            } else {
                let series_count = series_keys.len();
                for (i, k) in candidates.iter().enumerate() {
                    let checked = gs.split_keys.contains(k);
                    let mark = if checked { "[✓]" } else { "[ ]" };
                    let line_text = format!("  {mark} {k}");
                    let combined_idx = series_count + i;
                    let on_cursor = combined_idx == sel;
                    if on_cursor {
                        body_cursor = Some(body.len());
                    }
                    let style = if on_cursor {
                        Style::default().add_modifier(Modifier::REVERSED)
                    } else {
                        Style::default()
                    };
                    body.push(Line::from(TuiSpan::styled(line_text, style)));
                }
            }
        } else {
            // Compact: agg/metric/window status line + table header
            // are sticky; data rows scroll.
            let mut row: Vec<TuiSpan<'static>> = Vec::new();
            row.push(TuiSpan::raw("agg: "));
            match &gs.agg_input {
                Some(buf) => {
                    let buf_body = if buf.is_empty() {
                        " ".into()
                    } else {
                        buf.clone()
                    };
                    row.push(TuiSpan::styled(
                        buf_body,
                        Style::default()
                            .bg(Color::White)
                            .fg(Color::Black)
                            .add_modifier(Modifier::BOLD),
                    ));
                    row.push(TuiSpan::styled(
                        "_",
                        Style::default()
                            .bg(Color::White)
                            .fg(Color::Black)
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD),
                    ));
                }
                None => row.push(TuiSpan::raw(agg_label(gs.aggregation))),
            }
            row.push(TuiSpan::raw(format!(
                "   metric: {}   window: ",
                metric_label(gs.metric)
            )));
            match &gs.window_input {
                Some(buf) => {
                    let buf_body = if buf.is_empty() {
                        " ".into()
                    } else {
                        buf.clone()
                    };
                    row.push(TuiSpan::styled(
                        buf_body,
                        Style::default()
                            .bg(Color::White)
                            .fg(Color::Black)
                            .add_modifier(Modifier::BOLD),
                    ));
                    row.push(TuiSpan::styled(
                        "_",
                        Style::default()
                            .bg(Color::White)
                            .fg(Color::Black)
                            .add_modifier(Modifier::REVERSED | Modifier::BOLD),
                    ));
                }
                None => row.push(TuiSpan::raw(format!("{:.2}s", gs.window_secs))),
            }
            row.push(TuiSpan::raw(
                "   (a/w to edit, t to swap metric, Tab to split)",
            ));
            sticky.push(Line::from(row));

            let series_count = gs.series_keys().len();
            let cursor_idx = if series_count == 0 {
                None
            } else {
                Some(gs.details_selected.min(series_count - 1))
            };
            let (table_header, table_rows, table_cursor) = series_table_lines(
                gs,
                cursor_idx,
                colorize,
                /* show_sort_underline */ false,
            );
            if let Some(h) = table_header {
                sticky.push(h);
            }
            let body_start = body.len();
            body.extend(table_rows);
            if let Some(rel) = table_cursor {
                body_cursor = Some(body_start + rel);
            }
        }

        // Draw the outer block first; subsequent paragraphs draw
        // inside its inner rect.
        let block = Block::default().title(title).borders(Borders::ALL);
        let inner = block.inner(area);
        f.render_widget(block, area);

        // Vertical split: sticky on top, body fills the rest.  Cap
        // the sticky region so we never starve the body entirely on
        // tiny panes.
        let sticky_h = sticky.len().min(inner.height as usize).min(
            // Reserve at least one line for the body when both can fit;
            // otherwise let the sticky take everything.
            inner.height as usize,
        ) as u16;
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(sticky_h), Constraint::Min(0)])
            .split(inner);

        f.render_widget(Paragraph::new(sticky), chunks[0]);

        if chunks[1].height > 0 {
            let body_h = chunks[1].height as usize;
            let total_body = body.len();
            let scroll = match body_cursor {
                Some(line) if total_body > body_h && body_h > 0 => {
                    let half = body_h / 2;
                    let max_scroll = total_body - body_h;
                    line.saturating_sub(half).min(max_scroll) as u16
                }
                _ => 0,
            };
            f.render_widget(Paragraph::new(body).scroll((scroll, 0)), chunks[1]);
        }
    }
}
