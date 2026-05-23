//! Process-level glue between the UI and the host: spillway channels,
//! the network task that talks to the host over protosocket-rpc, and
//! the three mode entry points (`run_tui` / `run_states` /
//! `run_stats` is in `stats.rs`).  The keyboard thread + the modal
//! routing flags also live here since they only matter while a TUI
//! is on screen.
//!
//! `main` is intentionally thin — it parses args, builds the
//! channels, spawns the network task, and dispatches to whichever
//! mode the user picked.

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
use tracing_console_host::{Request, RequestBody, Response, ResponseBody, WireLevelFilter};

use crate::args::{Args, ModeFlag};
use crate::model::{Effect, Model, Update, ViewMode};
use crate::stats;
use crate::view;

/// Outgoing commands that the runtime queues for the network task.
/// Separate from `Update` so the model never sees them — the model
/// only reflects server-confirmed state (e.g. `CacheLevelReceived`).
#[derive(Debug, Clone)]
pub enum Outgoing {
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

/// Top-level entry point.  Spawns the network task, queues optional
/// auto-configure RPCs, and dispatches to the requested mode.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
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
        let (rpc_client, conn) = match client::connect::<ClientCodec, _>(addr, &configuration).await
        {
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
    use std::sync::atomic::{AtomicU8, Ordering};

    // Two flags the keyboard loop reads to dispatch correctly:
    //
    //   modal_kind — which text-input modal (if any) currently
    //   owns the keystroke set: digits / `.` / Backspace / Enter /
    //   Esc, plus the free-form explore search.  Encoded as a
    //   small enum (see MODAL_* constants).
    //
    //   current_view — which top-level view is active.  Controls
    //   which top-level binding table applies (e.g. `a` is bound
    //   in graph mode, free in table, search-input in explore).
    //
    // The runtime keeps both in sync with `model` after each
    // `model.apply`.
    let modal_kind = Arc::new(AtomicU8::new(MODAL_NONE));
    let current_view = Arc::new(AtomicU8::new(VIEW_TABLE));

    let kb_tx = tx.clone();
    let kb_modal = Arc::clone(&modal_kind);
    let kb_view = Arc::clone(&current_view);
    std::thread::spawn(move || keyboard_loop(kb_tx, kb_modal, kb_view));

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
                                    } else if gs.lookback_input.is_some() {
                                        MODAL_GRAPH_LOOKBACK
                                    } else {
                                        MODAL_NONE
                                    }
                                } else if let ViewMode::Explore(es) = &model.view {
                                    if es.search_input.is_some() {
                                        MODAL_EXPLORE_SEARCH
                                    } else {
                                        MODAL_NONE
                                    }
                                } else {
                                    MODAL_NONE
                                };
                                modal_kind.store(kind, Ordering::Relaxed);
                                let view_tag = match &model.view {
                                    ViewMode::Table => VIEW_TABLE,
                                    ViewMode::Graph(_) => VIEW_GRAPH,
                                    ViewMode::Explore(_) => VIEW_EXPLORE,
                                    ViewMode::TraceDetail(_) => VIEW_TRACE_DETAIL,
                                };
                                current_view.store(view_tag, Ordering::Relaxed);
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
const MODAL_GRAPH_LOOKBACK: u8 = 4;
const MODAL_EXPLORE_SEARCH: u8 = 5;

// `current_view` mirrors `model.view`'s variant tag.  The
// keyboard loop reads this to pick the right binding table for
// non-modal keys.
const VIEW_TABLE: u8 = 0;
const VIEW_GRAPH: u8 = 1;
const VIEW_EXPLORE: u8 = 2;
const VIEW_TRACE_DETAIL: u8 = 3;

fn keyboard_loop(
    tx: spillway::Sender<Update>,
    modal_kind: std::sync::Arc<std::sync::atomic::AtomicU8>,
    current_view: std::sync::Arc<std::sync::atomic::AtomicU8>,
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
                (MODAL_GRAPH_LOOKBACK, KeyCode::Char(c)) => Update::GraphLookbackInputChar(c),
                (MODAL_GRAPH_LOOKBACK, KeyCode::Backspace) => Update::GraphLookbackInputBackspace,
                (MODAL_GRAPH_LOOKBACK, KeyCode::Enter) => Update::GraphLookbackInputCommit,
                (MODAL_GRAPH_LOOKBACK, KeyCode::Esc) => Update::GraphLookbackInputCancel,
                (MODAL_EXPLORE_SEARCH, KeyCode::Char(c)) => Update::ExploreSearchChar(c),
                (MODAL_EXPLORE_SEARCH, KeyCode::Backspace) => Update::ExploreSearchBackspace,
                (MODAL_EXPLORE_SEARCH, KeyCode::Enter) => Update::ExploreSearchCommit,
                (MODAL_EXPLORE_SEARCH, KeyCode::Esc) => Update::ExploreSearchCancel,
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

        let view = current_view.load(Ordering::Relaxed);

        // Graph-mode bindings.  `g` and `Esc` exit graph mode; the
        // rest configure the view.  When `gs.focus == Details` the
        // model itself routes j/k/Space to the split-keys cursor;
        // we always emit GraphSelectUp/Down/Toggle here.  When in
        // Chart focus, `Tab` switches focus into Details.
        if view == VIEW_GRAPH {
            let update = match k.code {
                KeyCode::Char('q') => Update::Quit,
                KeyCode::Esc => Update::ToggleGraph,
                // View switcher: `s` → stacks, `e` → explore on the
                // graph's currently-locked stack.  The hint hides
                // `g` (current view) but shows both `s` and `e`.
                KeyCode::Char('s') => Update::EnterTable,
                KeyCode::Char('e') => Update::EnterExplore,
                // `a` opens the aggregation-input modal; the buffer
                // accepts `a`/`avg`, `min`, `max`, or `pX[.XX]`.
                KeyCode::Char('a') => Update::BeginGraphAggInput,
                KeyCode::Char('m') => Update::ToggleGraphMetric,
                KeyCode::Char('w') => Update::BeginGraphWindowInput,
                // `l` opens the lookback-input modal — sets how far
                // back the chart's X axis extends.  Reserved for the
                // modal rather than the sort cursor so the letter
                // matches its label in the details pane.
                KeyCode::Char('l') => Update::BeginGraphLookbackInput,
                // `u` cycles the chart's X-axis label format
                // (delta → unix UTC → local clock → delta).
                KeyCode::Char('u') => Update::ToggleGraphTimeLabels,
                KeyCode::Tab | KeyCode::BackTab => Update::GraphSwitchFocus,
                KeyCode::Down | KeyCode::Char('j') => Update::GraphSelectDown,
                KeyCode::Up | KeyCode::Char('k') => Update::GraphSelectUp,
                // Left/Right cycle the series table's leading sort
                // column.  Underlined in the expanded details pane.
                KeyCode::Left | KeyCode::Char('h') => Update::GraphSortColumnLeft,
                KeyCode::Right => Update::GraphSortColumnRight,
                KeyCode::Char(' ') => Update::GraphToggleSplit,
                _ => continue,
            };
            if send_or_exit(update).is_break() {
                return;
            }
            continue;
        }

        // Explore mode: list of span instances + `/`-search +
        // sortable columns.  Enter opens trace-detail on the
        // current row; `Esc` returns to stacks (one pop up).
        // View switcher: `s` → stacks, `g` → graph on the same
        // locked stack.  `e` is the *current* view so it has no
        // action here (the hint hides it).
        if view == VIEW_EXPLORE {
            let update = match k.code {
                KeyCode::Char('q') => Update::Quit,
                KeyCode::Esc => Update::ExitExplore,
                KeyCode::Char('s') => Update::EnterTable,
                KeyCode::Char('g') => Update::EnterGraph,
                KeyCode::Char('/') => Update::BeginExploreSearch,
                KeyCode::Char('i') => Update::ExploreInvertSort,
                KeyCode::Down | KeyCode::Char('j') => Update::ExploreSelectDown,
                KeyCode::Up | KeyCode::Char('k') => Update::ExploreSelectUp,
                KeyCode::Left | KeyCode::Char('h') => Update::ExploreSortLeft,
                KeyCode::Right | KeyCode::Char('l') => Update::ExploreSortRight,
                KeyCode::Enter => Update::ExploreOpenTrace,
                _ => continue,
            };
            if send_or_exit(update).is_break() {
                return;
            }
            continue;
        }

        // Trace-detail view: collapsible single-trace tree with
        // selection.  Arrows navigate (no j/k — they pun against
        // the tree's selection model), Right/Left expand/collapse,
        // Esc pops back up to explore.  `s` / `g` / `e` jump to
        // their respective views.
        if view == VIEW_TRACE_DETAIL {
            let update = match k.code {
                KeyCode::Char('q') => Update::Quit,
                KeyCode::Esc => Update::ExitTraceDetail,
                KeyCode::Char('s') => Update::EnterTable,
                KeyCode::Char('g') => Update::EnterGraph,
                KeyCode::Char('e') => Update::ExitTraceDetail,
                KeyCode::Down => Update::TraceDetailSelectDown,
                KeyCode::Up => Update::TraceDetailSelectUp,
                KeyCode::Right => Update::TraceDetailExpand,
                KeyCode::Left => Update::TraceDetailCollapse,
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
            // `e` enters explore mode locked onto the current row.
            // The reducer requests RequestSetLevel(Off) so the
            // screen doesn't keep churning while you read.
            KeyCode::Char('e') => Update::EnterExplore,
            _ => continue,
        };
        if send_or_exit(update).is_break() {
            return;
        }
    }
}
