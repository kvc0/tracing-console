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

    match args.mode {
        Mode::States => run_states(rx).await,
        Mode::Stats(hz) => stats::run_stats(rx, hz).await,
        Mode::Tui => run_tui(tx, rx, args.colorize).await,
    }
}

enum Mode {
    Tui,
    States,
    Stats(f64),
}

// ── argv ─────────────────────────────────────────────────────────────────────

struct Args {
    addr: std::net::SocketAddr,
    mode: Mode,
    colorize: bool,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Self {
        let mut addr_arg: Option<String> = None;
        let mut mode = Mode::Tui;
        let mut colorize = true;
        let mut iter = args.peekable();
        while let Some(a) = iter.next() {
            match a.as_str() {
                "--states" => mode = Mode::States,
                "--no-color" => colorize = false,
                "--stats" => {
                    let val = iter.next().unwrap_or_else(|| {
                        eprintln!("--stats expects a Hz argument (e.g. --stats 1, --stats 0.1)");
                        std::process::exit(2);
                    });
                    let hz: f64 = val.parse().unwrap_or_else(|_| {
                        eprintln!("--stats: invalid Hz value {val:?}");
                        std::process::exit(2);
                    });
                    if !hz.is_finite() || hz <= 0.0 {
                        eprintln!("--stats: Hz must be > 0, got {hz}");
                        std::process::exit(2);
                    }
                    mode = Mode::Stats(hz);
                }
                other if !other.starts_with("--") => addr_arg = Some(other.to_string()),
                other => eprintln!("ignoring unknown flag: {other}"),
            }
        }
        let addr_str = addr_arg.unwrap_or_else(|| DEFAULT_HOST.to_string());
        let addr = addr_str
            .parse()
            .unwrap_or_else(|e| panic!("invalid address {addr_str:?}: {e}"));
        Args {
            addr,
            mode,
            colorize,
        }
    }
}

// ── network task ─────────────────────────────────────────────────────────────

type ClientCodec = (MessagePackSerializer<Request>, MessagePackDecoder<Response>);

async fn run_network(addr: std::net::SocketAddr, tx: mpsc::UnboundedSender<Update>) {
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
    colorize: bool,
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
        // Redraws ONLY on the ticker; updates accumulate between ticks.
        // Without this gate, a 1k+ spans/s producer turns every redraw
        // into another `bucket_by_stack` over the full 4096-span buffer
        // and starves the keyboard mpsc behind it.
        loop {
            tokio::select! {
                maybe_update = rx.recv() => {
                    match maybe_update {
                        Some(update) => {
                            if model.apply(update) == Effect::Quit { break; }
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
        let Ok(Event::Key(k)) = event::read() else {
            continue;
        };
        let update = match k.code {
            KeyCode::Char('q') | KeyCode::Esc => Update::Quit,
            KeyCode::Down | KeyCode::Char('j') => Update::SelectDown,
            KeyCode::Up | KeyCode::Char('k') => Update::SelectUp,
            KeyCode::Right | KeyCode::Char('l') => Update::ExpandSelected,
            KeyCode::Enter => Update::ExpandAllSelected,
            KeyCode::Left | KeyCode::Char('h') => Update::CollapseSelected,
            KeyCode::Tab | KeyCode::BackTab => Update::SwitchFocus,
            KeyCode::Char(' ') => Update::ToggleSplitSelected,
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
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use tracing_console_host::{WireLevel, WireSpan};

    use crate::aggregate::{bucket_key, fmt_ns};
    use crate::model::Focus;

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

        // Top: connection status line.
        let header = match &model.connection {
            ConnectionStatus::Connecting => Line::from(vec![
                TuiSpan::raw("[connecting] "),
                TuiSpan::styled(
                    model.status.clone().unwrap_or_default(),
                    Style::default().add_modifier(Modifier::DIM),
                ),
            ]),
            ConnectionStatus::Connected => {
                Line::from(format!("[connected]  {} spans buffered", model.spans.len()))
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
