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
use futures::StreamExt;
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
use crate::model::{ConnectionStatus, Effect, Model, Update};

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

    match args.mode {
        ModeFlag::States => run_states(rx, args.history).await,
        ModeFlag::Stats => stats::run_stats(rx, args.stats_hz).await,
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

    let _ = tx.send(Update::Status(format!("connecting to {addr}…")));

    let configuration: Configuration<TcpStreamConnector> = Configuration::new(TcpStreamConnector);

    let (rpc_client, conn) = match client::connect::<ClientCodec, _>(addr, &configuration).await {
        Ok(pair) => pair,
        Err(e) => {
            let _ = tx.send(Update::Disconnected(format!("connect failed: {e}")));
            return;
        }
    };
    // Drive the connection's I/O loop in the background.
    let conn_task = tokio::spawn(conn);
    let _ = tx.send(Update::Connected);

    // protosocket-rpc routes responses by `Message::message_id()` and
    // does not auto-assign ids.  `Request::new` defaults to id=0, so
    // two coexisting RPCs at id=0 (streaming + unary) would collide in
    // the client's completion registry and the second would clobber
    // the first.  Use a unique id per outgoing request — that keeps
    // the streaming completion alive while unary `SetCacheLevel`s
    // come and go.
    let next_id = AtomicU64::new(1);
    let mut start = Request::new(RequestBody::StartStream);
    start.id = next_id.fetch_add(1, Ordering::Relaxed);
    let stream = match rpc_client.send_streaming(start) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Update::Disconnected(format!("StartStream failed: {e}")));
            return;
        }
    };

    let mut stream = stream;
    loop {
        tokio::select! {
            // Stream of pushes from the server (spans + CacheLevel notices).
            item = stream.next() => {
                let Some(item) = item else { break };
                match item {
                    Ok(resp) => match resp.body {
                        ResponseBody::Span(s) => {
                            if tx.send(Update::SpanReceived(s)).is_err() {
                                break;
                            }
                        }
                        ResponseBody::CacheLevel(filter) => {
                            if tx.send(Update::CacheLevelReceived(filter)).is_err() {
                                break;
                            }
                        }
                        ResponseBody::CacheChance(pct) => {
                            if tx.send(Update::CacheChanceReceived(pct)).is_err() {
                                break;
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        let _ = tx.send(Update::Disconnected(format!("stream error: {e}")));
                        break;
                    }
                }
            }
            // Outgoing requests from the keyboard / runtime.  None
            // means all senders dropped → runtime is shutting down.
            req = out_rx.next() => {
                let Some(req) = req else { break };
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
                    // We don't depend on the unary Ack — the
                    // server-side stream will push the new state as
                    // the source of truth.
                    tokio::spawn(async move { let _ = unary.await; });
                }
            }
        }
    }
    let _ = tx.send(Update::Disconnected("stream ended".into()));
    conn_task.abort();
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
    use std::sync::atomic::{AtomicBool, Ordering};

    // Shared flag: when set, the keyboard loop routes keys as chance
    // input (digits/./Backspace/Enter/Esc → ChanceInput* Updates)
    // rather than the normal navigation bindings.  The runtime keeps
    // it in sync with `model.chance_input.is_some()` after each
    // `model.apply`.
    let chance_input_active = Arc::new(AtomicBool::new(false));

    let kb_tx = tx.clone();
    let kb_flag = Arc::clone(&chance_input_active);
    std::thread::spawn(move || keyboard_loop(kb_tx, kb_flag));

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal: Terminal<CrosstermBackend<Stdout>> = Terminal::new(backend)?;

    let mut model = Model::new(history_budget);
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    let result: Result<(), Box<dyn std::error::Error>> = (async {
        // Redraws ONLY on the ticker; updates accumulate between ticks.
        // Without this gate, a 1k+ spans/s producer turns every redraw
        // into another `bucket_by_stack` over the full 4096-span buffer
        // and starves the keyboard mpsc behind it.
        loop {
            tokio::select! {
                maybe_update = rx.next() => {
                    match maybe_update {
                        Some(update) => {
                            match model.apply(update) {
                                Effect::None => {}
                                Effect::Quit => break,
                                Effect::RequestSetLevel(level) => {
                                    let _ = out_tx.send(Outgoing::SetCacheLevel(level));
                                }
                                Effect::RequestSetChance(pct) => {
                                    let _ = out_tx.send(Outgoing::SetCacheChance(pct));
                                }
                            }
                            chance_input_active.store(
                                model.chance_input.is_some(),
                                Ordering::Relaxed,
                            );
                        }
                        None => break, // all senders dropped
                    }
                }
                _ = ticker.tick() => {
                    terminal.draw(|f| view::render(f, &model, colorize))?;
                }
            }
        }
        Ok(())
    })
    .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn keyboard_loop(
    tx: spillway::Sender<Update>,
    chance_input_active: std::sync::Arc<std::sync::atomic::AtomicBool>,
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

        // Chance-input mode owns the keystroke set: digits + `.` for
        // characters, Backspace to edit, Enter to commit, Esc to
        // cancel.  Everything else is silently dropped so the user
        // can't escape into a normal command while typing a number.
        if chance_input_active.load(Ordering::Relaxed) {
            let update = match k.code {
                KeyCode::Char(c) => Update::ChanceInputChar(c),
                KeyCode::Backspace => Update::ChanceInputBackspace,
                KeyCode::Enter => Update::ChanceInputCommit,
                KeyCode::Esc => Update::ChanceInputCancel,
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
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use tracing_console_host::{WireLevel, WireLevelFilter, WireSpan};

    use crate::aggregate::{bucket_key, fmt_ns};
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
                    n = model.spans.len(),
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

            // Resolve which spans match the selected stack.  Use the
            // same bucket_key path as the aggregator so we don't drift.
            let by_id: HashMap<u64, &WireSpan> = model.spans.iter().map(|s| (s.id, s)).collect();
            let split_empty: BTreeSet<String> = BTreeSet::new();
            let matching: Vec<&WireSpan> = model
                .spans
                .iter()
                .filter(|s| {
                    let k = bucket_key(s, &by_id, &split_empty);
                    k.stack == r.key.stack
                })
                .collect();

            detail_lines.push(Line::from(format!("stack:  {}", r.key.stack.join(" › "))));
            detail_lines.push(Line::from(format!(
                "n={}  matched={}  total avg: {}  self avg: {}",
                r.stats.count,
                matching.len(),
                fmt_ns(r.stats.total_avg_ns()),
                fmt_ns(r.stats.self_avg_ns()),
            )));
            if !model.split_keys.is_empty() {
                let split_list: Vec<String> = model.split_keys.iter().cloned().collect();
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
                    entries.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
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
                        let checked = model.split_keys.contains(k);
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
}
