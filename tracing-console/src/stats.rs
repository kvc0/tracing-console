//! `--stats <Hz>` mode: aggregate observed spans per stack and print
//! min/avg/max for total and self (= total − sum of direct children's
//! totals) durations on a configurable cadence.

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing_console_host::WireSpan;

use crate::model::Update;

/// Cap on the rolling parent-name lookup so memory doesn't grow forever
/// across many windows.  Eviction is "oldest-id-first" since ids are
/// monotonic per source thread.
const NAME_TABLE_CAP: usize = 100_000;

pub async fn run_stats(
    mut rx: mpsc::UnboundedReceiver<Update>,
    hz: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let period = Duration::from_secs_f64(1.0 / hz);
    let mut acc = StatsAccumulator::new();
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let started = Instant::now();
    let mut last_tick = started;

    // Tick-then-drain pattern: every period we drain everything pending
    // on the mpsc with non-blocking `try_recv`, absorb each Update, then
    // flush stats.  This avoids a `select!{tick, rx.recv()}` starvation
    // pattern in which a flooded rx and the timer alternate windows.
    loop {
        let instant = tick.tick().await;
        let now: Instant = instant.into_std();
        let mut closed = false;
        loop {
            match rx.try_recv() {
                Ok(update) => acc.absorb(update),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        let elapsed_window = now.saturating_duration_since(last_tick);
        let elapsed_total = now.saturating_duration_since(started);
        acc.flush(elapsed_total, elapsed_window);
        last_tick = now;
        if closed {
            break;
        }
    }
    Ok(())
}

// ── accumulator ──────────────────────────────────────────────────────────

#[derive(Default)]
struct StackStats {
    count: u64,
    total_min_ns: u64,
    total_max_ns: u64,
    total_sum_ns: u128,
    self_min_ns: u64,
    self_max_ns: u64,
    self_sum_ns: u128,
}

impl StackStats {
    fn record(&mut self, total_ns: u64, self_ns: u64) {
        if self.count == 0 {
            self.total_min_ns = total_ns;
            self.total_max_ns = total_ns;
            self.self_min_ns = self_ns;
            self.self_max_ns = self_ns;
        } else {
            self.total_min_ns = self.total_min_ns.min(total_ns);
            self.total_max_ns = self.total_max_ns.max(total_ns);
            self.self_min_ns = self.self_min_ns.min(self_ns);
            self.self_max_ns = self.self_max_ns.max(self_ns);
        }
        self.count += 1;
        self.total_sum_ns += total_ns as u128;
        self.self_sum_ns += self_ns as u128;
    }
}

struct StatsAccumulator {
    /// Spans received this window (those with closed_at — i.e., complete).
    window: Vec<WireSpan>,
    /// id → (name, parent_id) — kept across windows so a child whose
    /// parent landed in a previous window can still resolve its full
    /// stack via O(depth) ancestor walk.  Bounded.
    ancestry: HashMap<u64, (String, Option<u64>)>,
    /// Parallel insertion order so eviction picks the oldest entry.
    name_order: std::collections::VecDeque<u64>,
    /// Connection / status updates for the header.
    last_status: Option<String>,
    connected: bool,
    /// Counts that ignore the per-window reset.
    total_received: u64,
    total_dropped_unfinished: u64,
}

impl StatsAccumulator {
    fn new() -> Self {
        Self {
            window: Vec::new(),
            ancestry: HashMap::new(),
            name_order: std::collections::VecDeque::new(),
            last_status: None,
            connected: false,
            total_received: 0,
            total_dropped_unfinished: 0,
        }
    }

    fn absorb(&mut self, update: Update) {
        match update {
            Update::SpanReceived(span) => {
                self.total_received += 1;
                // Remember name + parent for cross-window stack resolution.
                self.remember_ancestry(span.id, span.name.clone(), span.parent_id);
                if span.closed_at_ns.is_none() {
                    // Open spans don't contribute to duration stats; they
                    // can come from the cache if a window happens to catch
                    // mid-flight (rare on this stream).
                    self.total_dropped_unfinished += 1;
                    return;
                }
                self.window.push(span);
            }
            Update::Connected => {
                self.connected = true;
                self.last_status = None;
            }
            Update::Disconnected(reason) => {
                self.connected = false;
                self.last_status = Some(reason);
            }
            Update::Status(s) => {
                self.last_status = Some(s);
            }
            Update::SelectUp | Update::SelectDown | Update::Quit => {}
        }
    }

    fn remember_ancestry(&mut self, id: u64, name: String, parent_id: Option<u64>) {
        if self.ancestry.insert(id, (name, parent_id)).is_none() {
            self.name_order.push_back(id);
            while self.ancestry.len() > NAME_TABLE_CAP {
                if let Some(oldest) = self.name_order.pop_front() {
                    self.ancestry.remove(&oldest);
                } else {
                    break;
                }
            }
        }
    }

    fn flush(&mut self, elapsed_total: Duration, elapsed_window: Duration) {
        let received_this_window = self.window.len() as u64;
        let span_rate = if elapsed_window.as_secs_f64() > 0.0 {
            received_this_window as f64 / elapsed_window.as_secs_f64()
        } else {
            0.0
        };

        // First pass: build a children sum table by parent_id, accumulating
        // direct children's totals so each parent's self can be computed.
        let mut child_sum: HashMap<u64, u64> = HashMap::new();
        for s in &self.window {
            if let (Some(opened), Some(closed)) =
                (Some(s.opened_at_ns), s.closed_at_ns)
            {
                let total = closed.saturating_sub(opened);
                if let Some(p) = s.parent_id {
                    *child_sum.entry(p).or_default() += total;
                }
            }
        }

        // Second pass: bucket by stack, recording total + self.
        let mut by_stack: HashMap<Vec<String>, StackStats> = HashMap::new();
        for s in &self.window {
            let total = s.closed_at_ns.unwrap().saturating_sub(s.opened_at_ns);
            let self_ns = total.saturating_sub(*child_sum.get(&s.id).unwrap_or(&0));
            let stack = self.resolve_stack(s);
            by_stack.entry(stack).or_default().record(total, self_ns);
        }

        // Print header + table.
        let header_status = if self.connected {
            "[connected]".to_string()
        } else {
            self.last_status
                .as_deref()
                .map(|s| format!("[{s}]"))
                .unwrap_or_else(|| "[disconnected]".into())
        };
        println!(
            "=== stats @ {:.2}s — {recv} spans ({rate:.0} spans/s) over {win:.3?} {st} ===",
            elapsed_total.as_secs_f64(),
            recv = received_this_window,
            rate = span_rate,
            win = elapsed_window,
            st = header_status,
        );

        if by_stack.is_empty() {
            println!("  (no closed spans this window; received={} dropped_open={})",
                self.total_received, self.total_dropped_unfinished);
            self.window.clear();
            let _ = std::io::stdout().flush();
            return;
        }

        // Sort rows by stack name for stable output.
        let mut rows: Vec<(Vec<String>, StackStats)> = by_stack.into_iter().collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        let stack_width = rows
            .iter()
            .map(|(s, _)| stack_label(s).chars().count())
            .max()
            .unwrap_or(0)
            .max(20);

        println!(
            "  {stack:<sw$}  {n:>7}  │ {tlbl:^25} │ {slbl:^25}",
            stack = "stack",
            n = "n",
            tlbl = "total (min · avg · max)",
            slbl = "self  (min · avg · max)",
            sw = stack_width,
        );
        for (stack, st) in &rows {
            println!(
                "  {label:<sw$}  {n:>7}  │ {tmin:>7} {tavg:>7} {tmax:>7} │ {smin:>7} {savg:>7} {smax:>7}",
                label = stack_label(stack),
                n = st.count,
                tmin = fmt_ns(st.total_min_ns),
                tavg = fmt_ns((st.total_sum_ns / st.count as u128) as u64),
                tmax = fmt_ns(st.total_max_ns),
                smin = fmt_ns(st.self_min_ns),
                savg = fmt_ns((st.self_sum_ns / st.count as u128) as u64),
                smax = fmt_ns(st.self_max_ns),
                sw = stack_width,
            );
        }

        self.window.clear();
        let _ = std::io::stdout().flush();
    }

    /// Walk the parent chain via the rolling ancestry map.  O(depth)
    /// per span — every step is one HashMap lookup.  Returns root-first.
    fn resolve_stack(&self, span: &WireSpan) -> Vec<String> {
        let mut chain = vec![span.name.clone()];
        let mut p = span.parent_id;
        while let Some(id) = p {
            // Defensive: stop on absurdly deep chains.
            if chain.len() > 64 {
                break;
            }
            match self.ancestry.get(&id) {
                Some((name, next_parent)) => {
                    chain.push(name.clone());
                    p = *next_parent;
                }
                None => break,
            }
        }
        chain.reverse();
        chain
    }
}

fn stack_label(stack: &[String]) -> String {
    stack.join("/")
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.1}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}µs", ns as f64 / 1e3)
    } else {
        format!("{}ns", ns)
    }
}
