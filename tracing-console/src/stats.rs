//! `--stats <Hz>` mode: aggregate observed spans per stack and print
//! min/avg/max for total and self durations on a configurable cadence.
//!
//! Uses the same aggregation strategy as the TUI: a bounded rolling
//! `VecDeque<WireSpan>` of size `history_budget`, re-bucketed on each
//! flush via `aggregate::bucket_by_stack`.  Per-tick work is therefore
//! O(history_budget), not O(spans received this window), so the
//! flush cost is bounded regardless of input rate.

use std::collections::{BTreeSet, VecDeque};
use std::io::Write;
use std::time::{Duration, Instant};

use tracing_console_host::WireSpan;

use crate::aggregate::{StackStats, bucket_by_stack, fmt_ns, tree_label};
use crate::model::{Update, RateTracker};

pub async fn run_stats(
    mut rx: spillway::Receiver<Update>,
    hz: f64,
    history_budget: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let period = Duration::from_secs_f64(1.0 / hz);
    let mut acc = StatsAccumulator::new(history_budget);
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let started = Instant::now();

    // Continuous drain + tick-based flush.  Earlier this awaited
    // the tick first and then drained, which paced consumption at
    // `stream_buffer × stats_hz` spans/s (e.g. 65,536/s at
    // --stats-hz 1).  Now `select!` lets the drain run as fast as
    // the producer fills the channel, and the tick arm only fires
    // the print/flush.
    loop {
        tokio::select! {
            batch = rx.next_batch() => {
                match batch {
                    Some(items) => {
                        for update in items {
                            acc.absorb(update);
                        }
                    }
                    None => break, // all senders dropped
                }
            }
            instant = tick.tick() => {
                let now: Instant = instant.into_std();
                let elapsed_total = now.saturating_duration_since(started);
                acc.flush(elapsed_total);
            }
        }
    }
    Ok(())
}

struct StatsAccumulator {
    /// Rolling history of closed spans, capped at `history_budget`.
    /// Aggregation runs over this buffer on every flush, the same way
    /// the TUI's `Model::visible_rows` runs over `Model::spans`.
    spans: VecDeque<WireSpan>,
    history_budget: usize,
    last_status: Option<String>,
    connected: bool,
    rate: RateTracker,
    total_received: u64,
    total_dropped_unfinished: u64,
}

impl StatsAccumulator {
    fn new(history_budget: usize) -> Self {
        Self {
            spans: VecDeque::with_capacity(history_budget),
            history_budget,
            last_status: None,
            connected: false,
            rate: RateTracker::default(),
            total_received: 0,
            total_dropped_unfinished: 0,
        }
    }

    fn absorb(&mut self, update: Update) {
        match update {
            Update::SpanReceived(span) => {
                self.total_received += 1;
                self.rate.record(Instant::now());
                if span.closed_at_ns.is_none() {
                    self.total_dropped_unfinished += 1;
                    return;
                }
                if self.spans.len() >= self.history_budget {
                    self.spans.pop_front();
                }
                self.spans.push_back(span);
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
            | Update::CacheLevelReceived(_)
            | Update::RequestCacheLevel(_)
            | Update::CacheChanceReceived(_)
            | Update::BeginChanceInput
            | Update::ChanceInputChar(_)
            | Update::ChanceInputBackspace
            | Update::ChanceInputCancel
            | Update::ChanceInputCommit
            | Update::Quit => {}
        }
    }

    fn flush(&mut self, elapsed_total: Duration) {
        let buffered = self.spans.len();
        let span_rate = self.rate.rate_hz();

        let split_keys: BTreeSet<String> = BTreeSet::new();
        let rows = bucket_by_stack(self.spans.iter(), &split_keys);

        let header_status = if self.connected {
            "[connected]".to_string()
        } else {
            self.last_status
                .as_deref()
                .map(|s| format!("[{s}]"))
                .unwrap_or_else(|| "[disconnected]".into())
        };
        println!(
            "=== stats @ {:.2}s — {buf} buffered ({rate:.0} spans/s, recv={recv}) {st} ===",
            elapsed_total.as_secs_f64(),
            buf = buffered,
            rate = span_rate,
            recv = self.total_received,
            st = header_status,
        );

        if rows.is_empty() {
            println!(
                "  (no renderable spans in buffer; received={} dropped_open={})",
                self.total_received, self.total_dropped_unfinished
            );
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

        let _ = std::io::stdout().flush();
    }
}
