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

mod model;

use std::io::Stdout;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures::StreamExt;
use protosocket_messagepack::{MessagePackDecoder, MessagePackSerializer};
use protosocket_rpc::client::{self, Configuration, TcpStreamConnector};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span as TuiSpan};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Terminal;
use tokio::sync::mpsc;
use tracing_console_host::{Request, RequestBody, Response, ResponseBody};

use crate::model::{ConnectionStatus, Effect, Model, Update};

const DEFAULT_HOST: &str = "127.0.0.1:7777";
const HISTORY_BUDGET: usize = 4096;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(std::env::args().skip(1));

    let (tx, rx) = mpsc::unbounded_channel::<Update>();

    // Network task: feeds Updates into the channel.  Owns no UI concerns.
    {
        let tx = tx.clone();
        tokio::spawn(async move { run_network(args.addr, tx).await });
    }

    if args.dump_states {
        run_states(rx).await
    } else {
        run_tui(tx, rx).await
    }
}

// ── argv ─────────────────────────────────────────────────────────────────────

struct Args {
    addr: std::net::SocketAddr,
    /// Hidden flag: skip TUI and instead print every Update as JSON to stdout.
    /// Used by integration tests that assert on the model's update stream
    /// without running a terminal.
    dump_states: bool,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Self {
        let mut addr_arg: Option<String> = None;
        let mut dump_states = false;
        for a in args {
            match a.as_str() {
                "--states" => dump_states = true,
                other if !other.starts_with("--") => addr_arg = Some(other.to_string()),
                other => eprintln!("ignoring unknown flag: {other}"),
            }
        }
        let addr_str = addr_arg.unwrap_or_else(|| DEFAULT_HOST.to_string());
        let addr = addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid address {addr_str:?}: {e}"));
        Args { addr, dump_states }
    }
}

// ── network task ─────────────────────────────────────────────────────────────

type ClientCodec = (
    MessagePackSerializer<Request>,
    MessagePackDecoder<Response>,
);

async fn run_network(addr: std::net::SocketAddr, tx: mpsc::UnboundedSender<Update>) {
    let _ = tx.send(Update::Status(format!("connecting to {addr}…")));

    let configuration: Configuration<TcpStreamConnector> =
        Configuration::new(TcpStreamConnector);

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

    let stream = match rpc_client.send_streaming(Request::new(RequestBody::StartStream)) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Update::Disconnected(format!("StartStream failed: {e}")));
            return;
        }
    };

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(resp) => {
                if let ResponseBody::Span(s) = resp.body {
                    if tx.send(Update::SpanReceived(s)).is_err() {
                        break; // receiver dropped — runtime is shutting down
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Update::Disconnected(format!("stream error: {e}")));
                break;
            }
        }
    }
    let _ = tx.send(Update::Disconnected("stream ended".into()));
    conn_task.abort();
}

// ── --states mode ────────────────────────────────────────────────────────────

async fn run_states(
    mut rx: mpsc::UnboundedReceiver<Update>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut model = Model::new(HISTORY_BUDGET);
    while let Some(update) = rx.recv().await {
        // Print the update first so an integration test sees the cause line
        // followed by an effect line if needed.
        println!("{}", serde_json::to_string(&update)?);
        if model.apply(update) == Effect::Quit {
            break;
        }
    }
    Ok(())
}

// ── TUI mode ─────────────────────────────────────────────────────────────────

async fn run_tui(
    tx: mpsc::UnboundedSender<Update>,
    mut rx: mpsc::UnboundedReceiver<Update>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Keyboard thread: sync (crossterm is sync) and pushes Updates via the
    // tokio channel.  Exits when the receiver drops or the user quits.
    let kb_tx = tx.clone();
    std::thread::spawn(move || keyboard_loop(kb_tx));

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal: Terminal<CrosstermBackend<Stdout>> = Terminal::new(backend)?;

    let mut model = Model::new(HISTORY_BUDGET);
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    let result: Result<(), Box<dyn std::error::Error>> = (async {
        loop {
            // Wake on either an update or the redraw tick.
            tokio::select! {
                maybe_update = rx.recv() => {
                    match maybe_update {
                        Some(update) => {
                            if model.apply(update) == Effect::Quit { break; }
                        }
                        None => break, // all senders dropped
                    }
                }
                _ = ticker.tick() => {}
            }
            terminal.draw(|f| view::render(f, &model))?;
        }
        Ok(())
    })
    .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn keyboard_loop(tx: mpsc::UnboundedSender<Update>) {
    loop {
        // Short poll so we can notice the runtime shutting down via send-fail.
        match event::poll(Duration::from_millis(200)) {
            Ok(true) => {}
            Ok(false) => {
                if tx.is_closed() {
                    return;
                }
                continue;
            }
            Err(_) => return,
        }
        let Ok(Event::Key(k)) = event::read() else { continue };
        let update = match k.code {
            KeyCode::Char('q') | KeyCode::Esc => Update::Quit,
            KeyCode::Down | KeyCode::Char('j') => Update::SelectDown,
            KeyCode::Up | KeyCode::Char('k') => Update::SelectUp,
            _ => continue,
        };
        if tx.send(update).is_err() {
            return;
        }
    }
}

// ── view (ratatui) ───────────────────────────────────────────────────────────

mod view {
    use super::*;

    pub fn render(f: &mut ratatui::Frame<'_>, model: &Model) {
        let area = f.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(8), Constraint::Length(10)])
            .split(area);

        // Top: connection status line.
        let header = match &model.connection {
            ConnectionStatus::Connecting => Line::from(vec![
                TuiSpan::raw("[connecting] "),
                TuiSpan::styled(
                    model.status.clone().unwrap_or_default(),
                    Style::default().add_modifier(Modifier::DIM),
                ),
            ]),
            ConnectionStatus::Connected => Line::from(format!(
                "[connected]  {} spans buffered",
                model.spans.len()
            )),
            ConnectionStatus::Disconnected(reason) => Line::from(format!("[disconnected] {reason}")),
        };
        f.render_widget(Paragraph::new(header), chunks[0]);

        // Middle: span list.
        // TODO(flamegraph): replace with flamegraph rendering of the
        // selected span's stack.  Today: list view is the placeholder.
        let items: Vec<ListItem> = model
            .spans
            .iter()
            .map(|s| {
                let dur_us = s
                    .closed_at_ns
                    .map(|c| (c.saturating_sub(s.opened_at_ns)) / 1_000)
                    .unwrap_or(0);
                ListItem::new(Line::from(vec![
                    TuiSpan::styled(
                        format!("{:>10}µs ", dur_us),
                        Style::default().add_modifier(Modifier::DIM),
                    ),
                    TuiSpan::raw(format!("[{:?}] {}::{}", s.level, s.target, s.name)),
                ]))
            })
            .collect();
        let title = format!(" spans  ({}) ", model.spans.len());
        let list = List::new(items)
            .block(Block::default().title(title).borders(Borders::ALL))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        let mut state = ratatui::widgets::ListState::default();
        state.select(if model.spans.is_empty() { None } else { Some(model.selected) });
        f.render_stateful_widget(list, chunks[1], &mut state);

        // Bottom: details for selected span.
        // TODO(filter/heatmap): filter input + alternate render that shows
        // a latency heatmap / configurable percentile line when the filter
        // collapses to one span name.
        let detail_lines: Vec<Line> = if let Some(s) = model.selected_span() {
            let mut lines = vec![
                Line::from(format!("name:   {}::{}", s.target, s.name)),
                Line::from(format!("level:  {:?}", s.level)),
                Line::from(format!("id:     {}  parent: {:?}", s.id, s.parent_id)),
            ];
            for (k, v) in &s.fields {
                lines.push(Line::from(format!("  {k} = {v}")));
            }
            if !s.events.is_empty() {
                lines.push(Line::from(format!("events: {}", s.events.len())));
                for e in &s.events {
                    lines.push(Line::from(format!(
                        "  · {} {:?} {:?}",
                        e.name, e.level, e.fields
                    )));
                }
            }
            lines
        } else {
            vec![Line::from("(no span selected — q to quit, j/k to navigate)")]
        };
        let detail = Paragraph::new(detail_lines)
            .block(Block::default().title(" details ").borders(Borders::ALL));
        f.render_widget(detail, chunks[2]);
    }
}
