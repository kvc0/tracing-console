//! `--stats <Hz>` mode: aggregate observed spans per stack and print
//! min/avg/max for total and self durations on a configurable cadence.

use std::collections::BTreeSet;
use std::io::Write;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing_console_host::WireSpan;

use crate::aggregate::{StackStats, bucket_by_stack, fmt_ns, tree_label};
use crate::model::Update;

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

struct StatsAccumulator {
    /// Spans received this window.  `bucket_by_stack` drops any whose
    /// full parent chain isn't also in this window — same "don't
    /// render without context" rule as the TUI.
    window: Vec<WireSpan>,
    last_status: Option<String>,
    connected: bool,
    total_received: u64,
    total_dropped_unfinished: u64,
}

impl StatsAccumulator {
    fn new() -> Self {
        Self {
            window: Vec::new(),
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
                if span.closed_at_ns.is_none() {
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
            Update::SelectUp
            | Update::SelectDown
            | Update::ExpandSelected
            | Update::ExpandAllSelected
            | Update::CollapseSelected
            | Update::SwitchFocus
            | Update::ToggleSplitSelected
            | Update::Quit => {}
        }
    }

    fn flush(&mut self, elapsed_total: Duration, elapsed_window: Duration) {
        let received_this_window = self.window.len() as u64;
        let span_rate = if elapsed_window.as_secs_f64() > 0.0 {
            received_this_window as f64 / elapsed_window.as_secs_f64()
        } else {
            0.0
        };

        let split_keys: BTreeSet<String> = BTreeSet::new();
        let rows = bucket_by_stack(self.window.iter(), &split_keys);

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

        if rows.is_empty() {
            println!(
                "  (no closed spans this window; received={} dropped_open={})",
                self.total_received, self.total_dropped_unfinished
            );
            self.window.clear();
            let _ = std::io::stdout().flush();
            return;
        }

        let labels: Vec<String> = (0..rows.len()).map(|i| tree_label(&rows, i)).collect();
        let stack_width = labels
            .iter()
            .map(|l| l.chars().count())
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
        for (i, (_key, st)) in rows.iter().enumerate() {
            println!(
                "  {label:<sw$}  {n:>7}  │ {tmin:>7} {tavg:>7} {tmax:>7} │ {smin:>7} {savg:>7} {smax:>7}",
                label = labels[i],
                n = st.count,
                tmin = fmt_ns(st.total_min_ns),
                tavg = fmt_ns(StackStats::total_avg_ns(st)),
                tmax = fmt_ns(st.total_max_ns),
                smin = fmt_ns(st.self_min_ns),
                savg = fmt_ns(StackStats::self_avg_ns(st)),
                smax = fmt_ns(st.self_max_ns),
                sw = stack_width,
            );
        }

        self.window.clear();
        let _ = std::io::stdout().flush();
    }
}
