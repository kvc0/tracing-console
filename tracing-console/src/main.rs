//! Console TUI for browsing live spans streamed from a `tracing-console-host`
//! server.
//!
//! The full UI vision (per the project README/spec) is a flamegraph upper
//! panel + filter/details lower panel + latency heatmap when the filter
//! collapses to one span kind.  This file currently delivers the working
//! end-to-end skeleton — RPC connection, StartStream, and a scrolling list
//! of incoming spans — so the host protocol can be exercised and iterated
//! against.  Each TODO below marks a piece the spec calls for that's not
//! yet implemented.

use std::collections::VecDeque;
use std::io::Stdout;
use std::sync::{Arc, Mutex};
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
use tracing_console_host::{Request, RequestBody, Response, ResponseBody, WireSpan};

const DEFAULT_HOST: &str = "127.0.0.1:7777";
const HISTORY_BUDGET: usize = 4096;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_arg = std::env::args().nth(1).unwrap_or_else(|| DEFAULT_HOST.to_string());
    let addr: std::net::SocketAddr = addr_arg.parse()?;

    // Shared, append-only history of received spans.  Bounded by HISTORY_BUDGET.
    let history: Arc<Mutex<VecDeque<WireSpan>>> = Arc::new(Mutex::new(VecDeque::new()));

    // Spawn the network task: connect, send StartStream, push spans into history.
    let history_for_net = Arc::clone(&history);
    let net = tokio::spawn(async move {
        if let Err(e) = run_client(addr, history_for_net).await {
            // Surface to stderr post-restore so the user sees it on exit.
            eprintln!("client task ended: {e}");
        }
    });

    // Run the TUI on the current thread.
    let result = run_tui(history);

    net.abort();

    result
}

// ── network half ─────────────────────────────────────────────────────────────

/// Codec used by the client connection: encodes Request, decodes Response.
type ClientCodec = (
    MessagePackSerializer<Request>,
    MessagePackDecoder<Response>,
);

async fn run_client(
    addr: std::net::SocketAddr,
    history: Arc<Mutex<VecDeque<WireSpan>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configuration: Configuration<TcpStreamConnector> =
        Configuration::new(TcpStreamConnector);

    let (client, connection) =
        client::connect::<ClientCodec, _>(addr, &configuration).await?;

    // The Connection future drives the socket I/O loop — spawn it.
    let _conn_task = tokio::spawn(connection);

    // Open the streaming RPC.  StreamingCompletion implements futures::Stream.
    let mut completion = client.send_streaming(Request::new(RequestBody::StartStream))?;

    while let Some(item) = completion.next().await {
        let resp = item?;
        if let ResponseBody::Span(s) = resp.body {
            let mut h = history.lock().unwrap();
            if h.len() >= HISTORY_BUDGET {
                h.pop_front();
            }
            h.push_back(s);
        }
    }
    Ok(())
}

// ── TUI half ─────────────────────────────────────────────────────────────────

fn run_tui(history: Arc<Mutex<VecDeque<WireSpan>>>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal: Terminal<CrosstermBackend<Stdout>> = Terminal::new(backend)?;

    let mut selected: usize = 0;

    loop {
        // Snapshot history so we don't hold the lock across rendering.
        let snap: Vec<WireSpan> = {
            let h = history.lock().unwrap();
            h.iter().cloned().collect()
        };
        let visible_count = snap.len();
        if visible_count > 0 && selected >= visible_count {
            selected = visible_count - 1;
        }

        terminal.draw(|f| {
            let area = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(8), Constraint::Length(10)])
                .split(area);

            // Upper: live span list.
            //
            // TODO(flamegraph): replace with flamegraph rendering of the
            // selected span's stack.  The list view is the v0 stand-in.
            let items: Vec<ListItem> = snap
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
                        TuiSpan::raw(format!(
                            "[{:?}] {}::{}",
                            s.level, s.target, s.name
                        )),
                    ]))
                })
                .collect();
            let title = format!(" spans  ({}) ", snap.len());
            let list = List::new(items)
                .block(Block::default().title(title).borders(Borders::ALL))
                .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
            let mut state = ratatui::widgets::ListState::default();
            state.select(if visible_count == 0 { None } else { Some(selected) });
            f.render_stateful_widget(list, chunks[0], &mut state);

            // Lower: details for selected span.
            //
            // TODO(filter/heatmap): add a filter input here, plus an
            // alternate render that shows latency heatmap / percentile
            // line when the filter collapses to one span name.
            let detail_lines: Vec<Line> = if let Some(s) = snap.get(selected) {
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
                vec![Line::from("(no span selected — press q to quit)")]
            };
            let detail = Paragraph::new(detail_lines)
                .block(Block::default().title(" details ").borders(Borders::ALL));
            f.render_widget(detail, chunks[1]);
        })?;

        // Poll for input with a short timeout so the screen refreshes as
        // new spans arrive even when the user isn't typing.
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(k) = event::read()? {
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Down | KeyCode::Char('j') => {
                        if visible_count > 0 && selected + 1 < visible_count {
                            selected += 1;
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        selected = selected.saturating_sub(1);
                    }
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

