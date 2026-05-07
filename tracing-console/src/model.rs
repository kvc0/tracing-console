//! Application state for the console TUI.
//!
//! Decoupled from rendering so it can be tested without ever touching
//! ratatui / crossterm.  Both [`Model`] and [`Update`] are `Serialize` +
//! `Deserialize` so integration tests can construct sequences of updates
//! (or replay a captured `--states` dump) and assert on the resulting
//! model.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use tracing_console_host::WireSpan;

/// Bounded history of incoming spans + UI selection + connection state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub spans: VecDeque<WireSpan>,
    pub selected: usize,
    pub connection: ConnectionStatus,
    /// Optional human-readable status line (errors, info messages).
    pub status: Option<String>,
    /// History budget — once reached, oldest span is dropped on each new arrival.
    pub history_budget: usize,
}

impl Model {
    pub fn new(history_budget: usize) -> Self {
        Self {
            spans: VecDeque::new(),
            selected: 0,
            connection: ConnectionStatus::Connecting,
            status: None,
            history_budget,
        }
    }

    /// Apply a state-change message; return any side-effect the runtime
    /// must observe (e.g. quit).
    pub fn apply(&mut self, update: Update) -> Effect {
        match update {
            Update::SpanReceived(span) => {
                if self.spans.len() >= self.history_budget {
                    self.spans.pop_front();
                    // Keep selection pinned to the same logical row.
                    self.selected = self.selected.saturating_sub(1);
                }
                self.spans.push_back(span);
                Effect::None
            }
            Update::SelectUp => {
                self.selected = self.selected.saturating_sub(1);
                Effect::None
            }
            Update::SelectDown => {
                if !self.spans.is_empty() && self.selected + 1 < self.spans.len() {
                    self.selected += 1;
                }
                Effect::None
            }
            Update::Connected => {
                self.connection = ConnectionStatus::Connected;
                self.status = None;
                Effect::None
            }
            Update::Disconnected(reason) => {
                self.connection = ConnectionStatus::Disconnected(reason);
                Effect::None
            }
            Update::Status(msg) => {
                self.status = Some(msg);
                Effect::None
            }
            Update::Quit => Effect::Quit,
        }
    }

    pub fn selected_span(&self) -> Option<&WireSpan> {
        self.spans.get(self.selected)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected(String),
}

/// Every state-change message that can move the model.  Tests build
/// vectors of these and call [`Model::apply`] in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    SpanReceived(WireSpan),
    SelectUp,
    SelectDown,
    Connected,
    Disconnected(String),
    Status(String),
    Quit,
}

/// Side-effects that the model runtime must observe.  Kept narrow on
/// purpose: anything richer (re-issue an RPC, etc.) belongs in the
/// runtime, not the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    None,
    Quit,
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tracing_console_host::{WireLevel, WireSpan};

    fn span(id: u64, name: &str) -> WireSpan {
        WireSpan {
            id,
            parent_id: None,
            name: name.into(),
            target: "test".into(),
            level: WireLevel::Info,
            fields: HashMap::new(),
            events: vec![],
            opened_at_ns: 0,
            closed_at_ns: Some(1000),
        }
    }

    #[test]
    fn initial_model_is_connecting_and_empty() {
        let m = Model::new(8);
        assert!(m.spans.is_empty());
        assert_eq!(m.selected, 0);
        assert!(matches!(m.connection, ConnectionStatus::Connecting));
        assert!(m.status.is_none());
        assert!(m.selected_span().is_none());
    }

    #[test]
    fn span_received_appends_and_selection_stays_at_zero() {
        let mut m = Model::new(8);
        for i in 0..3 {
            assert_eq!(m.apply(Update::SpanReceived(span(i, "s"))), Effect::None);
        }
        assert_eq!(m.spans.len(), 3);
        // No SelectDown was sent → selection is still 0.
        assert_eq!(m.selected, 0);
        assert_eq!(m.selected_span().map(|s| s.id), Some(0));
    }

    #[test]
    fn budget_drops_oldest_and_keeps_selection_anchored() {
        let mut m = Model::new(3);
        for i in 0..3 {
            m.apply(Update::SpanReceived(span(i, "s")));
        }
        m.apply(Update::SelectDown);
        m.apply(Update::SelectDown); // selected = 2 → span id 2

        // Overflow: oldest (id 0) gets evicted; selection slides to 1 so it
        // still points at the same logical row (now span id 2).
        m.apply(Update::SpanReceived(span(99, "s")));
        let ids: Vec<u64> = m.spans.iter().map(|s| s.id).collect();
        assert_eq!(ids, vec![1, 2, 99]);
        assert_eq!(m.selected, 1);
        assert_eq!(m.selected_span().map(|s| s.id), Some(2));
    }

    #[test]
    fn select_navigation_clamps() {
        let mut m = Model::new(8);
        m.apply(Update::SelectDown); // empty list — no-op
        assert_eq!(m.selected, 0);
        m.apply(Update::SpanReceived(span(0, "a")));
        m.apply(Update::SpanReceived(span(1, "b")));
        m.apply(Update::SelectDown);
        m.apply(Update::SelectDown); // already at end
        assert_eq!(m.selected, 1);
        m.apply(Update::SelectUp);
        m.apply(Update::SelectUp); // already at top
        assert_eq!(m.selected, 0);
    }

    #[test]
    fn connection_transitions_clear_and_set_status() {
        let mut m = Model::new(8);
        m.apply(Update::Status("connecting…".into()));
        assert_eq!(m.status.as_deref(), Some("connecting…"));
        m.apply(Update::Connected);
        assert!(matches!(m.connection, ConnectionStatus::Connected));
        assert!(m.status.is_none(), "Connected clears the transient status");
        m.apply(Update::Disconnected("bye".into()));
        assert!(matches!(
            m.connection,
            ConnectionStatus::Disconnected(ref s) if s == "bye"
        ));
    }

    #[test]
    fn quit_returns_quit_effect() {
        let mut m = Model::new(8);
        assert_eq!(m.apply(Update::Quit), Effect::Quit);
    }

    #[test]
    fn updates_round_trip_through_json() {
        // Anchor that the wire format is stable for replay tests / states dump.
        let updates = vec![
            Update::Status("hi".into()),
            Update::Connected,
            Update::SpanReceived(span(7, "round_trip")),
            Update::SelectDown,
            Update::Disconnected("eof".into()),
            Update::Quit,
        ];
        for u in updates {
            let json = serde_json::to_string(&u).unwrap();
            let back: Update = serde_json::from_str(&json).unwrap();
            // Apply both, compare resulting models — easier than deriving Eq
            // on the whole tree (WireSpan's HashMap doesn't impl Hash).
            let mut a = Model::new(4);
            let mut b = Model::new(4);
            a.apply(u.clone());
            b.apply(back);
            assert_eq!(
                serde_json::to_string(&a).unwrap(),
                serde_json::to_string(&b).unwrap(),
            );
        }
    }
}
