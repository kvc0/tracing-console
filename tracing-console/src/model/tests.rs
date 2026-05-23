//! Tests for `Model`, `Update`, and the graph state machine.
//! Uses `super::*` so it reaches into the sibling submodules just
//! as the old in-file `mod tests` did.

use std::collections::BTreeSet;

use super::*;
use tracing_console_host::{WireLevel, WireLevelFilter, WireSpan};


fn span(id: u64, name: &str) -> WireSpan {
    span_with_parent(id, name, None)
}

fn span_with_parent(id: u64, name: &str, parent_id: Option<u64>) -> WireSpan {
    WireSpan {
        id,
        parent_id,
        name: name.into(),
        target: "test".into(),
        level: WireLevel::Info,
        fields: Vec::new(),
        events: vec![],
        opened_at_ns: 0,
        closed_at_ns: Some(1000),
    }
}

fn span_with_field(id: u64, name: &str, parent_id: Option<u64>, k: &str, v: &str) -> WireSpan {
    let mut s = span_with_parent(id, name, parent_id);
    s.fields.push((
        k.to_string(),
        tracing_console_host::WireFieldValue::Str(v.into()),
    ));
    s
}

#[test]
fn span_received_appears_as_root_row() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key.stack, vec!["a"]);
    assert_eq!(rows[0].depth, 0);
    assert!(!rows[0].has_children);
}

#[test]
fn span_with_evicted_parent_is_dropped() {
    // history budget 1 → adding the child evicts the parent;
    // child should not render.
    let mut m = Model::new(1);
    m.apply(Update::SpanReceived(span(10, "parent")));
    m.apply(Update::SpanReceived(span_with_parent(
        11,
        "child",
        Some(10),
    )));
    let rows = m.visible_rows();
    // Parent was evicted, child has missing parent → both dropped.
    assert_eq!(rows.len(), 0);
}

#[test]
fn child_is_hidden_until_parent_is_expanded() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::SpanReceived(span_with_parent(11, "b", Some(10))));
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].has_children);

    m.apply(Update::ExpandSelected);
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].key.stack, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn tab_toggles_focus_between_two_panes() {
    let mut m = Model::new(4);
    assert_eq!(m.focus, Focus::Stacks);
    m.apply(Update::SwitchFocus);
    assert_eq!(m.focus, Focus::Details);
    m.apply(Update::SwitchFocus);
    assert_eq!(m.focus, Focus::Stacks);
}

#[test]
fn request_cache_level_emits_effect_without_updating_state() {
    let mut m = Model::new(4);
    let effect = m.apply(Update::RequestCacheLevel(WireLevelFilter::Debug));
    assert_eq!(effect, Effect::RequestSetLevel(WireLevelFilter::Debug));
    // Local state is untouched — the server hasn't confirmed yet.
    assert!(m.cache_level.is_none());
    // Server confirms.
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Debug));
    assert_eq!(m.cache_level, Some(WireLevelFilter::Debug));
}

#[test]
fn toggle_split_only_works_under_details_focus() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span_with_field(
        10, "a", None, "api", "fetch",
    )));
    // Toggle while Stacks-focused: no-op.
    m.apply(Update::ToggleSplitSelected);
    assert!(m.split_keys().is_empty());
    // Switch to Details, then toggle: api becomes a split key.
    m.apply(Update::SwitchFocus);
    m.apply(Update::ToggleSplitSelected);
    assert!(m.split_keys().contains("api"));
    // Toggle again removes.
    m.apply(Update::ToggleSplitSelected);
    assert!(m.split_keys().is_empty());
}

#[test]
fn splits_separate_buckets() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span_with_field(
        10, "req", None, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(span_with_field(
        11, "req", None, "api", "update",
    )));
    // No splits yet: 2 spans bucket into one row.
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].stats.count, 2);

    let mut sk = BTreeSet::new();
    sk.insert("api".to_string());
    m.agg.set_split_keys(sk);
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.key.stack == vec!["req".to_string()]));
    let apis: Vec<&str> = rows.iter().map(|r| r.key.splits[0].1.as_str()).collect();
    assert!(apis.contains(&"fetch"));
    assert!(apis.contains(&"update"));
}

#[test]
fn split_inherits_from_ancestor() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span_with_field(
        10, "req", None, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(span_with_parent(
        11,
        "validate",
        Some(10),
    )));
    let mut sk = BTreeSet::new();
    sk.insert("api".to_string());
    m.agg.set_split_keys(sk);
    let rows = m.visible_rows();
    // root + child = 1 row visible (root); expand and we should
    // see the child carry the same `api=fetch` split inherited
    // from its parent.
    assert_eq!(rows.len(), 1);
    m.apply(Update::ExpandSelected);
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1].key.stack,
        vec!["req".to_string(), "validate".to_string()]
    );
    assert_eq!(
        rows[1].key.splits,
        vec![("api".to_string(), "fetch".to_string())]
    );
}

#[test]
fn select_navigation_clamps() {
    let mut m = Model::new(8);
    m.apply(Update::SelectDown);
    assert_eq!(m.selected, 0);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::SpanReceived(span(20, "b")));
    m.apply(Update::SelectDown);
    m.apply(Update::SelectDown);
    assert_eq!(m.selected, 1);
    m.apply(Update::SelectUp);
    m.apply(Update::SelectUp);
    assert_eq!(m.selected, 0);
}

#[test]
fn quit_returns_quit_effect() {
    let mut m = Model::new(8);
    assert_eq!(m.apply(Update::Quit), Effect::Quit);
}

#[test]
fn updates_round_trip_through_json() {
    let updates = vec![
        Update::Status("hi".into()),
        Update::Connected,
        Update::SpanReceived(span(7, "round_trip")),
        Update::SelectDown,
        Update::ExpandSelected,
        Update::ExpandAllSelected,
        Update::CollapseSelected,
        Update::SwitchFocus,
        Update::ToggleSplitSelected,
        Update::Disconnected("eof".into()),
        Update::Quit,
    ];
    for u in updates {
        let json = serde_json::to_string(&u).unwrap();
        let back: Update = serde_json::from_str(&json).unwrap();
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

// ── Graph view ──────────────────────────────────────────────

fn timed_span(id: u64, parent: Option<u64>, name: &str, opened: u64, closed: u64) -> WireSpan {
    let mut s = span_with_parent(id, name, parent);
    s.opened_at_ns = opened;
    s.closed_at_ns = Some(closed);
    s
}

fn timed_span_with_field(
    id: u64,
    parent: Option<u64>,
    name: &str,
    opened: u64,
    closed: u64,
    k: &str,
    v: &str,
) -> WireSpan {
    let mut s = timed_span(id, parent, name, opened, closed);
    s.fields.push((
        k.to_string(),
        tracing_console_host::WireFieldValue::Str(v.into()),
    ));
    s
}

fn current_graph(m: &Model) -> &GraphState {
    match &m.view {
        ViewMode::Graph(gs) => gs,
        other => panic!("expected graph view, got {other:?}"),
    }
}

#[test]
fn toggle_graph_locks_onto_selected_bucket() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::SpanReceived(span(11, "beta")));
    m.apply(Update::SelectDown); // highlight `beta`
    m.apply(Update::ToggleGraph);
    let gs = current_graph(&m);
    assert_eq!(gs.locked_stack, vec!["beta".to_string()]);
    assert!(matches!(gs.aggregation, AggMode::Avg));
    assert_eq!(gs.metric, Metric::Total);
    assert!((gs.window_secs - 1.0).abs() < 1e-9);
}

#[test]
fn toggle_graph_round_trip_returns_to_table() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    assert!(matches!(m.view, ViewMode::Graph(_)));
    m.apply(Update::ToggleGraph);
    assert!(matches!(m.view, ViewMode::Table));
}

#[test]
fn toggle_graph_with_nothing_selected_stays_in_table() {
    let mut m = Model::new(8);
    m.apply(Update::ToggleGraph);
    assert!(matches!(m.view, ViewMode::Table));
}

/// Drive the agg-input modal by typing `input` and pressing
/// Enter; returns the resulting aggregation.
fn type_agg(m: &mut Model, input: &str) -> AggMode {
    m.apply(Update::BeginGraphAggInput);
    for c in input.chars() {
        m.apply(Update::GraphAggInputChar(c));
    }
    m.apply(Update::GraphAggInputCommit);
    current_graph(m).aggregation
}

#[test]
fn graph_agg_input_avg_via_a_and_avg() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    // Force off Avg first so the assertion is meaningful.
    m.apply(Update::SetGraphAgg(AggMode::Min));
    assert_eq!(type_agg(&mut m, "a"), AggMode::Avg);
    m.apply(Update::SetGraphAgg(AggMode::Min));
    assert_eq!(type_agg(&mut m, "avg"), AggMode::Avg);
}

#[test]
fn graph_agg_input_min_max_keywords() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    assert_eq!(type_agg(&mut m, "min"), AggMode::Min);
    assert_eq!(type_agg(&mut m, "max"), AggMode::Max);
}

#[test]
fn graph_agg_input_percentile() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    assert_eq!(type_agg(&mut m, "p50"), AggMode::Percentile(50.0));
    assert_eq!(type_agg(&mut m, "p99.5"), AggMode::Percentile(99.5));
    // Single-digit percentile is fine too.
    assert_eq!(type_agg(&mut m, "p5"), AggMode::Percentile(5.0));
}

#[test]
fn graph_agg_input_p0_is_min_p100_or_higher_is_max() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    assert_eq!(type_agg(&mut m, "p0"), AggMode::Min);
    assert_eq!(type_agg(&mut m, "p100"), AggMode::Max);
    // Anything above 100 also clamps to Max.
    assert_eq!(type_agg(&mut m, "p9999"), AggMode::Max);
}

#[test]
fn graph_agg_input_uppercase_is_normalised() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    assert_eq!(type_agg(&mut m, "AVG"), AggMode::Avg);
    assert_eq!(type_agg(&mut m, "Min"), AggMode::Min);
    assert_eq!(type_agg(&mut m, "P50"), AggMode::Percentile(50.0));
}

#[test]
fn graph_agg_input_rejects_garbage_silently() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    let original = current_graph(&m).aggregation;
    // Stray `x` is filtered out by the char handler; the remaining
    // buffer is "" which is not parseable, so the commit no-ops.
    m.apply(Update::BeginGraphAggInput);
    m.apply(Update::GraphAggInputChar('x'));
    m.apply(Update::GraphAggInputChar('!'));
    m.apply(Update::GraphAggInputCommit);
    assert_eq!(current_graph(&m).aggregation, original);

    // A typo'd keyword also no-ops cleanly.
    m.apply(Update::BeginGraphAggInput);
    for c in "minx".chars() {
        m.apply(Update::GraphAggInputChar(c));
    }
    m.apply(Update::GraphAggInputCommit);
    assert_eq!(current_graph(&m).aggregation, original);
}

#[test]
fn graph_agg_input_cancel_drops_buffer() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    let original = current_graph(&m).aggregation;
    m.apply(Update::BeginGraphAggInput);
    for c in "p99".chars() {
        m.apply(Update::GraphAggInputChar(c));
    }
    m.apply(Update::GraphAggInputCancel);
    assert!(current_graph(&m).agg_input.is_none());
    assert_eq!(current_graph(&m).aggregation, original);
}

#[test]
fn graph_window_input_rejects_zero_and_negatives() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    let original = current_graph(&m).window_secs;

    for bad in ["0", ""] {
        m.apply(Update::BeginGraphWindowInput);
        for c in bad.chars() {
            m.apply(Update::GraphWindowInputChar(c));
        }
        m.apply(Update::GraphWindowInputCommit);
        assert!((current_graph(&m).window_secs - original).abs() < 1e-9);
    }
}

#[test]
fn graph_window_input_commits_positive_float() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphWindowInput);
    for c in "0.5".chars() {
        m.apply(Update::GraphWindowInputChar(c));
    }
    m.apply(Update::GraphWindowInputCommit);
    assert!((current_graph(&m).window_secs - 0.5).abs() < 1e-9);
}

// ── Lookback (l) ────────────────────────────────────────────

#[test]
fn parse_lookback_input_accepts_bare_seconds_and_s_suffix() {
    assert_eq!(parse_lookback_input("30"), Some(30.0));
    assert_eq!(parse_lookback_input("30s"), Some(30.0));
    assert_eq!(parse_lookback_input("0.5"), Some(0.5));
    assert_eq!(parse_lookback_input("0.5s"), Some(0.5));
    assert_eq!(parse_lookback_input("  45 "), Some(45.0));
}

#[test]
fn parse_lookback_input_m_suffix_converts_to_seconds() {
    assert_eq!(parse_lookback_input("1m"), Some(60.0));
    assert_eq!(parse_lookback_input("5m"), Some(300.0));
    assert_eq!(parse_lookback_input("1.5m"), Some(90.0));
}

#[test]
fn parse_lookback_input_rejects_garbage_and_non_positive() {
    assert!(parse_lookback_input("").is_none());
    assert!(parse_lookback_input("0").is_none());
    assert!(parse_lookback_input("0s").is_none());
    assert!(parse_lookback_input("0m").is_none());
    assert!(parse_lookback_input("-5").is_none());
    assert!(parse_lookback_input("-5s").is_none());
    assert!(parse_lookback_input("nan").is_none());
    assert!(parse_lookback_input("infs").is_none());
    assert!(parse_lookback_input("abc").is_none());
    assert!(parse_lookback_input("5x").is_none());
    assert!(parse_lookback_input("s").is_none());
    assert!(parse_lookback_input("m").is_none());
}

#[test]
fn graph_lookback_input_commits_bare_number_as_seconds() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    for c in "120".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    m.apply(Update::GraphLookbackInputCommit);
    let gs = current_graph(&m);
    assert!((gs.lookback_secs - 120.0).abs() < 1e-9);
    assert!(gs.lookback_input.is_none());
}

#[test]
fn graph_lookback_input_commits_minutes_suffix() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    for c in "5m".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    m.apply(Update::GraphLookbackInputCommit);
    assert!((current_graph(&m).lookback_secs - 300.0).abs() < 1e-9);
}

#[test]
fn graph_lookback_input_commits_seconds_suffix_same_as_bare() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    for c in "45s".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    m.apply(Update::GraphLookbackInputCommit);
    assert!((current_graph(&m).lookback_secs - 45.0).abs() < 1e-9);
}

#[test]
fn graph_lookback_input_rejects_zero_and_keeps_prior_value() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    let original = current_graph(&m).lookback_secs;
    for bad in ["0", "0s", "0m", ""] {
        m.apply(Update::BeginGraphLookbackInput);
        for c in bad.chars() {
            m.apply(Update::GraphLookbackInputChar(c));
        }
        m.apply(Update::GraphLookbackInputCommit);
        assert!(
            (current_graph(&m).lookback_secs - original).abs() < 1e-9,
            "lookback changed on {:?}",
            bad,
        );
        assert!(current_graph(&m).lookback_input.is_none());
    }
}

#[test]
fn graph_lookback_input_char_filter_drops_letters_and_extra_dots() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    // Mix in invalid characters; only digits, one '.', and a single
    // trailing 's'/'m' should land in the buffer.
    for c in "1z.2y.3xs".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    let buf = current_graph(&m).lookback_input.clone();
    assert_eq!(buf.as_deref(), Some("1.23s"));
    // Once a suffix is present, no further chars (including digits
    // or another suffix) sneak in.
    m.apply(Update::GraphLookbackInputChar('5'));
    m.apply(Update::GraphLookbackInputChar('m'));
    assert_eq!(
        current_graph(&m).lookback_input.as_deref(),
        Some("1.23s"),
    );
}

#[test]
fn graph_lookback_input_suffix_rejected_at_empty_buffer() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    // Leading 's' or 'm' alone is meaningless — must follow a digit.
    m.apply(Update::GraphLookbackInputChar('s'));
    m.apply(Update::GraphLookbackInputChar('m'));
    assert_eq!(current_graph(&m).lookback_input.as_deref(), Some(""));
}

#[test]
fn graph_lookback_input_backspace_and_cancel() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::BeginGraphLookbackInput);
    for c in "5m".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    m.apply(Update::GraphLookbackInputBackspace);
    assert_eq!(current_graph(&m).lookback_input.as_deref(), Some("5"));
    // Cancel drops the buffer without touching `lookback_secs`.
    let original = current_graph(&m).lookback_secs;
    m.apply(Update::GraphLookbackInputCancel);
    assert!(current_graph(&m).lookback_input.is_none());
    assert!((current_graph(&m).lookback_secs - original).abs() < 1e-9);
}

#[test]
fn graph_lookback_input_does_not_rehydrate_store() {
    // Lookback is a pure projection knob — changing it must NOT
    // wipe or re-walk the series store (unlike window/metric which
    // do call rehydrate).
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 100)));
    m.apply(Update::ToggleGraph);
    m.apply(Update::SpanReceived(timed_span(11, None, "a", 100, 250)));
    let before = current_graph(&m).store.series.len();
    let series_id_before = current_graph(&m)
        .store
        .series
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert!(before > 0);

    m.apply(Update::BeginGraphLookbackInput);
    for c in "5m".chars() {
        m.apply(Update::GraphLookbackInputChar(c));
    }
    m.apply(Update::GraphLookbackInputCommit);
    let gs = current_graph(&m);
    assert_eq!(gs.store.series.len(), before, "store must not be wiped");
    let series_id_after: Vec<_> = gs.store.series.keys().cloned().collect();
    assert_eq!(series_id_before, series_id_after);
}

// ── Time-labels (u) ─────────────────────────────────────────

#[test]
fn graph_time_labels_default_is_delta() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Delta);
}

#[test]
fn graph_time_labels_toggle_cycles_delta_unix_local() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Delta);
    m.apply(Update::ToggleGraphTimeLabels);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Unix);
    m.apply(Update::ToggleGraphTimeLabels);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Local);
    m.apply(Update::ToggleGraphTimeLabels);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Delta);
}

#[test]
fn graph_time_labels_toggle_is_noop_outside_graph_mode() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    // Not in graph view — toggle should do nothing observable.
    m.apply(Update::ToggleGraphTimeLabels);
    assert!(matches!(m.view, ViewMode::Table));
}

#[test]
fn graph_time_labels_survive_json_round_trip() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "a")));
    m.apply(Update::ToggleGraph);
    m.apply(Update::ToggleGraphTimeLabels);
    m.apply(Update::ToggleGraphTimeLabels);
    assert_eq!(current_graph(&m).time_labels, TimeLabels::Local);
    let json = serde_json::to_string(&m).unwrap();
    let back: Model = serde_json::from_str(&json).unwrap();
    if let ViewMode::Graph(gs) = &back.view {
        assert_eq!(gs.time_labels, TimeLabels::Local);
    } else {
        panic!("expected graph view");
    }
}

#[test]
fn graph_metric_toggle_rehydrates_store() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 100)));
    m.apply(Update::ToggleGraph);
    m.apply(Update::SpanReceived(timed_span(11, None, "a", 100, 250)));
    let before = current_graph(&m).store.series.len();
    assert!(before > 0);

    m.apply(Update::ToggleGraphMetric);
    // Metric flipped + store re-populated from the ring, not
    // wiped, so the user can compare Total vs SelfTime without
    // losing their existing history.
    assert_eq!(current_graph(&m).metric, Metric::SelfTime);
    assert_eq!(current_graph(&m).store.series.len(), before);
}

/// Drive the details cursor down past every series row so it
/// lands on the first key candidate.  Used by tests that want
/// to operate on the split-keys section without enumerating the
/// exact series count by hand.
fn move_cursor_to_first_key(m: &mut Model) {
    if let ViewMode::Graph(gs) = &m.view {
        assert_eq!(gs.focus, GraphFocus::Details, "tab into Details first");
        let target = gs.series_keys().len();
        // Down from wherever; clamped at total - 1.  Drive past
        // any series rows so the cursor lands on candidate[0].
        for _ in 0..target {
            m.apply(Update::GraphSelectDown);
        }
    }
}

#[test]
fn graph_toggle_split_re_partitions_existing_history() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::SpanReceived(timed_span_with_field(
        12, None, "req", 200, 300, "api", "fetch",
    )));
    // No split yet → one merged series.
    assert_eq!(current_graph(&m).store.series.len(), 1);

    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    let gs = current_graph(&m);
    assert!(gs.split_keys.contains("api"));
    // Store rehydrated from the ring under the new split → two
    // series ("fetch" + "update"), not empty.
    assert_eq!(gs.store.series.len(), 2);

    // Cursor should now be pinned at the `api` key row, not
    // dragged onto a newly-appeared series row.
    m.apply(Update::GraphToggleSplit);
    let gs = current_graph(&m);
    assert!(!gs.split_keys.contains("api"));
    assert_eq!(gs.store.series.len(), 1);
}

#[test]
fn graph_split_change_works_after_tracing_stops() {
    // Mirrors the user-facing scenario: stream a few spans,
    // freeze the input (no more SpanReceived), then change the
    // split and confirm the series count actually changes.  The
    // rehydrate path walks the aggregator's ring rather than
    // waiting for new arrivals.
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        12, None, "req", 200, 300, "api", "post",
    )));
    m.apply(Update::ToggleGraph);
    // No more spans arrive past this point — simulates tracing
    // being toggled off.
    assert_eq!(current_graph(&m).store.series.len(), 1);

    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    // Now three series, all derived from the same frozen ring.
    assert_eq!(current_graph(&m).store.series.len(), 3);
}

#[test]
fn graph_record_partitions_by_split_keys() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::ToggleGraph);
    // Enable the api split via Details → first key row.
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    // Feed two spans with distinct `api` values.
    m.apply(Update::SpanReceived(timed_span_with_field(
        20, None, "req", 100, 200, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        21, None, "req", 200, 300, "api", "post",
    )));
    let gs = current_graph(&m);
    assert_eq!(gs.store.series.len(), 2);
}

#[test]
fn graph_state_round_trips_through_json() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "alpha")));
    m.apply(Update::ToggleGraph);
    let json = serde_json::to_string(&m).unwrap();
    let back: Model = serde_json::from_str(&json).unwrap();
    match &back.view {
        ViewMode::Graph(gs) => {
            assert_eq!(gs.locked_stack, vec!["alpha".to_string()]);
            // Store is #[serde(skip)] — it deserialises empty.
            assert!(gs.store.series.is_empty());
        }
        _ => panic!("expected graph view"),
    }
}

#[test]
fn graph_window_resize_rehydrates_under_new_window() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 100)));
    m.apply(Update::ToggleGraph);
    m.apply(Update::SpanReceived(timed_span(11, None, "a", 100, 200)));
    let before = current_graph(&m).store.series.len();
    assert!(before > 0);

    m.apply(Update::BeginGraphWindowInput);
    for c in "5".chars() {
        m.apply(Update::GraphWindowInputChar(c));
    }
    m.apply(Update::GraphWindowInputCommit);
    let gs = current_graph(&m);
    assert!((gs.window_secs - 5.0).abs() < 1e-9);
    // Bins were re-laid-out under the new window, not wiped.
    assert_eq!(gs.store.series.len(), before);
}

#[test]
fn graph_details_cursor_walks_series_then_keys() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    // Walk past the merged "(all)" series to land on the api key,
    // then toggle the split on so we have two series + one key.
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    assert_eq!(current_graph(&m).series_keys().len(), 2);

    // After the toggle, the cursor is pinned on the api key at
    // index 2 (= 2 series + 0 key offset).
    assert_eq!(current_graph(&m).details_selected, 2);

    // Down past the end clamps; Up retraces.
    m.apply(Update::GraphSelectDown);
    assert_eq!(current_graph(&m).details_selected, 2, "clamped at last row");
    m.apply(Update::GraphSelectUp);
    assert_eq!(current_graph(&m).details_selected, 1);
    m.apply(Update::GraphSelectUp);
    assert_eq!(current_graph(&m).details_selected, 0);
    m.apply(Update::GraphSelectUp);
    assert_eq!(current_graph(&m).details_selected, 0, "clamped at first row");
}

#[test]
fn graph_space_on_series_row_toggles_visibility() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // enable api split → 2 series

    let series = current_graph(&m).series_keys();
    assert_eq!(series.len(), 2);

    // Move cursor back up onto the first series row.
    m.apply(Update::GraphSelectUp);
    m.apply(Update::GraphSelectUp);
    assert_eq!(current_graph(&m).details_selected, 0);

    m.apply(Update::GraphToggleSplit);
    let gs = current_graph(&m);
    assert!(gs.hidden_series.contains(&series[0]));
    assert_eq!(gs.series_keys().len(), 2, "store keeps the data");

    // Second press un-hides.
    m.apply(Update::GraphToggleSplit);
    assert!(!current_graph(&m).hidden_series.contains(&series[0]));
}

#[test]
fn graph_space_on_key_row_still_toggles_split() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // turn split on → 2 series

    // After the toggle the cursor is pinned on the api key.
    // Another Space on that same row should turn the split off.
    m.apply(Update::GraphToggleSplit);
    assert!(!current_graph(&m).split_keys.contains("api"));
    assert!(
        current_graph(&m).hidden_series.is_empty(),
        "split change clears hidden_series"
    );
}

#[test]
fn graph_split_toggle_clears_hidden_series() {
    // Hide a series, then change splits — the hidden flag
    // should not leak into a different series space.
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // enable split

    // Cursor pinned at the api key.  Walk it back to series[0]
    // and hide it.
    m.apply(Update::GraphSelectUp);
    m.apply(Update::GraphSelectUp);
    m.apply(Update::GraphToggleSplit);
    assert!(!current_graph(&m).hidden_series.is_empty());

    // Now move back to the key row and toggle the split off.
    m.apply(Update::GraphSelectDown);
    m.apply(Update::GraphSelectDown);
    m.apply(Update::GraphToggleSplit);
    let gs = current_graph(&m);
    assert!(gs.split_keys.is_empty());
    assert!(gs.hidden_series.is_empty());
}

#[test]
fn graph_series_summary_aggregates_per_series() {
    // Three matching spans, distinct durations + one split value;
    // confirm `series_summary` rolls them up correctly.
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 1_000_000_000, 1_000_000_500, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        12, None, "req", 2_000_000_000, 2_000_000_200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);

    let summary = current_graph(&m).store.series_summary(AggMode::Avg);
    assert_eq!(summary.len(), 2);
    // Sorted by key — `api=fetch` first, `api=update` second.
    let fetch = &summary[0];
    let update_ = &summary[1];
    assert_eq!(fetch.key, vec![("api".into(), "fetch".into())]);
    assert_eq!(fetch.count, 2);
    assert_eq!(fetch.min_ns, 100);          // 100 - 0
    assert_eq!(fetch.max_ns, 500);          // 1_000_000_500 - 1_000_000_000
    // avg = (100 + 500) / 2 = 300
    assert_eq!(fetch.avg_ns, 300);

    assert_eq!(update_.key, vec![("api".into(), "update".into())]);
    assert_eq!(update_.count, 1);
    assert_eq!(update_.min_ns, 200);
    assert_eq!(update_.max_ns, 200);
    assert_eq!(update_.avg_ns, 200);
    assert_eq!(update_.last_ns, 200);
}

#[test]
fn graph_default_sort_is_count() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(timed_span(10, None, "alpha", 0, 100)));
    m.apply(Update::ToggleGraph);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Count);
}

#[test]
fn graph_left_right_cycle_sort_column() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // enable api split

    // Columns are: SplitKey(api), Count, Min, Avg, Max, Last.
    // Default sort is Count.
    assert_eq!(current_graph(&m).sort_column, SortColumn::Count);

    // Right cycles through Min, Avg, Max, Last, then wraps to SplitKey(api).
    m.apply(Update::GraphSortColumnRight);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Min);
    m.apply(Update::GraphSortColumnRight);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Avg);
    m.apply(Update::GraphSortColumnRight);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Max);
    m.apply(Update::GraphSortColumnRight);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Last);
    m.apply(Update::GraphSortColumnRight);
    assert_eq!(
        current_graph(&m).sort_column,
        SortColumn::SplitKey("api".into())
    );

    // Left wraps the other way.
    m.apply(Update::GraphSortColumnLeft);
    assert_eq!(current_graph(&m).sort_column, SortColumn::Last);
}

#[test]
fn graph_series_keys_order_follows_sort_column() {
    // Three series with predictable counts so we can verify the
    // resulting order under different sort columns.  series order
    // by count desc: fetch(3), update(2), post(1).
    let mut m = Model::new(32);
    for id in 0..3u64 {
        m.apply(Update::SpanReceived(timed_span_with_field(
            10 + id,
            None,
            "req",
            id * 10,
            id * 10 + 50,
            "api",
            "fetch",
        )));
    }
    for id in 0..2u64 {
        m.apply(Update::SpanReceived(timed_span_with_field(
            20 + id,
            None,
            "req",
            id * 10,
            id * 10 + 60,
            "api",
            "update",
        )));
    }
    m.apply(Update::SpanReceived(timed_span_with_field(
        30, None, "req", 0, 70, "api", "post",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // turn split on → 3 series

    // Count is descending: fetch (3), update (2), post (1).
    let order: Vec<String> = current_graph(&m)
        .series_keys()
        .iter()
        .map(|k| k[0].1.clone())
        .collect();
    assert_eq!(order, vec!["fetch", "update", "post"]);

    // Switch to SplitKey(api) — alphabetical ascending: fetch, post, update.
    m.apply(Update::GraphSortColumnLeft); // Count → SplitKey(api)
    let order: Vec<String> = current_graph(&m)
        .series_keys()
        .iter()
        .map(|k| k[0].1.clone())
        .collect();
    assert_eq!(order, vec!["fetch", "post", "update"]);

    // Switch to Max (descending): fetch=50, update=70, post=70.
    // Update first by alpha within max-tie at 70.
    m.apply(Update::GraphSortColumnRight); // → Count
    m.apply(Update::GraphSortColumnRight); // → Min
    m.apply(Update::GraphSortColumnRight); // → Avg
    m.apply(Update::GraphSortColumnRight); // → Max
    assert_eq!(current_graph(&m).sort_column, SortColumn::Max);
    let order: Vec<String> = current_graph(&m)
        .series_keys()
        .iter()
        .map(|k| k[0].1.clone())
        .collect();
    assert_eq!(order, vec!["post", "update", "fetch"]);
}

#[test]
fn graph_sort_falls_back_when_split_removed() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit); // enable split → SplitKey(api) now selectable
    m.apply(Update::GraphSortColumnLeft); // Count → SplitKey(api)
    assert_eq!(
        current_graph(&m).sort_column,
        SortColumn::SplitKey("api".into())
    );

    // Disable the split — sort column should fall back to Count.
    m.apply(Update::GraphToggleSplit);
    assert!(current_graph(&m).split_keys.is_empty());
    assert_eq!(current_graph(&m).sort_column, SortColumn::Count);
}

#[test]
fn graph_chart_focus_cursor_walks_series_only() {
    // Two series + one candidate key.  In Chart focus the cursor
    // must stop at series[1] (index 1); it must not slip onto
    // the key row (which is only reachable when Details is
    // expanded).
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span_with_field(
        10, None, "req", 0, 100, "api", "fetch",
    )));
    m.apply(Update::SpanReceived(timed_span_with_field(
        11, None, "req", 100, 200, "api", "update",
    )));
    m.apply(Update::ToggleGraph);
    // Enable api split via Details.
    m.apply(Update::GraphSwitchFocus);
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    // Tab back to Chart focus.
    m.apply(Update::GraphSwitchFocus);
    assert_eq!(current_graph(&m).focus, GraphFocus::Chart);
    // Cursor was at the key row (index 2) but got clamped to
    // series_count - 1 = 1 on the focus switch.
    assert_eq!(current_graph(&m).details_selected, 1);
    // Down clamps at series count - 1.
    m.apply(Update::GraphSelectDown);
    assert_eq!(current_graph(&m).details_selected, 1, "no key rows in Chart focus");
    m.apply(Update::GraphSelectUp);
    assert_eq!(current_graph(&m).details_selected, 0);
}

#[test]
fn graph_chart_focus_space_toggles_series_visibility() {
    // From compact (Chart focus) the user should still be able
    // to navigate the series list with j/k and toggle visibility
    // with Space.  Re-uses the merged "(all)" series as the
    // simplest workable bucket.
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(timed_span(10, None, "alpha", 0, 100)));
    m.apply(Update::SpanReceived(timed_span(11, None, "alpha", 100, 200)));
    m.apply(Update::ToggleGraph);
    assert_eq!(current_graph(&m).focus, GraphFocus::Chart);

    let series = current_graph(&m).series_keys();
    assert_eq!(series.len(), 1);
    m.apply(Update::GraphToggleSplit);
    assert!(current_graph(&m).hidden_series.contains(&series[0]));
    m.apply(Update::GraphToggleSplit);
    assert!(!current_graph(&m).hidden_series.contains(&series[0]));
}

#[test]
fn graph_entry_rehydrates_from_existing_ring() {
    let mut m = Model::new(8);
    // Build up some ring history before entering graph mode.
    for i in 0..5u64 {
        m.apply(Update::SpanReceived(timed_span(
            10 + i,
            None,
            "alpha",
            i * 100,
            i * 100 + 50,
        )));
    }
    m.apply(Update::ToggleGraph);
    // Chart should be non-empty immediately — populated from
    // the ring rather than waiting for new arrivals.
    assert!(!current_graph(&m).store.series.is_empty());
}

// ── RateTracker ─────────────────────────────────────────────

#[test]
fn rate_tracker_is_zero_before_any_sample() {
    let rt = RateTracker::default();
    assert_eq!(rt.rate_hz(), 0.0);
}

#[test]
fn rate_tracker_excludes_active_bucket_then_rolls_it_in_after_advance() {
    use std::time::{Duration, Instant};
    let mut rt = RateTracker::default();
    let t0 = Instant::now();
    // 8 events all land in the in-progress bucket.
    for _ in 0..8 {
        rt.record(t0);
    }
    // The current bucket is excluded from rate_hz, so nothing yet.
    assert_eq!(rt.rate_hz(), 0.0);
    // Advance one half-second; the previous bucket is now completed.
    rt.record(t0 + Duration::from_millis(500));
    let expected = 8.0 / RateTracker::WINDOW_SECS;
    assert!(
        (rt.rate_hz() - expected).abs() < 1e-9,
        "rate_hz={} expected={}",
        rt.rate_hz(),
        expected,
    );
}

#[test]
fn rate_tracker_drops_old_samples_after_full_window() {
    use std::time::{Duration, Instant};
    let mut rt = RateTracker::default();
    let t0 = Instant::now();
    for _ in 0..10 {
        rt.record(t0);
    }
    // 5s later — past all BUCKETS half-second slots → every old
    // bucket has been reset; the surviving sample sits in the
    // (excluded) head bucket.
    rt.record(t0 + Duration::from_millis(5_000));
    assert_eq!(rt.rate_hz(), 0.0);
}

// ── ExpandAllSelected / CollapseSelected ────────────────────

#[test]
fn expand_all_selected_recursively_opens_all_descendants() {
    let mut m = Model::new(16);
    // root → mid → leaf chain.
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::SpanReceived(span_with_parent(11, "mid", Some(10))));
    m.apply(Update::SpanReceived(span_with_parent(12, "leaf", Some(11))));
    // Cursor starts on "root" (depth 0).
    assert_eq!(m.visible_rows().len(), 1);
    m.apply(Update::ExpandAllSelected);
    let rows = m.visible_rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].key.stack, vec!["root".to_string()]);
    assert_eq!(rows[1].key.stack, vec!["root".to_string(), "mid".into()]);
    assert_eq!(
        rows[2].key.stack,
        vec!["root".to_string(), "mid".into(), "leaf".into()],
    );
}

#[test]
fn expand_all_selected_only_runs_under_stacks_focus() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::SpanReceived(span_with_parent(11, "mid", Some(10))));
    m.apply(Update::SwitchFocus); // → Details
    m.apply(Update::ExpandAllSelected);
    // No expansion happened — "mid" should still be hidden.
    assert_eq!(m.visible_rows().len(), 1);
}

#[test]
fn collapse_selected_jumps_to_parent_when_cursor_on_leaf() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::SpanReceived(span_with_parent(11, "mid", Some(10))));
    m.apply(Update::SpanReceived(span_with_parent(12, "leaf", Some(11))));
    m.apply(Update::ExpandAllSelected);
    // Walk cursor to the leaf row (index 2).
    m.apply(Update::SelectDown);
    m.apply(Update::SelectDown);
    assert_eq!(m.selected, 2);
    // Collapse on an already-collapsed leaf should jump cursor up
    // to the parent and collapse it.
    m.apply(Update::CollapseSelected);
    let rows = m.visible_rows();
    assert_eq!(rows[m.selected].key.stack, vec!["root".to_string(), "mid".into()]);
    assert!(!rows[m.selected].is_expanded);
}

// ── Chance input commit ─────────────────────────────────────

#[test]
fn chance_input_commit_emits_request_for_valid_value() {
    let mut m = Model::new(8);
    m.apply(Update::BeginChanceInput);
    for c in "42.5".chars() {
        m.apply(Update::ChanceInputChar(c));
    }
    let eff = m.apply(Update::ChanceInputCommit);
    assert_eq!(eff, Effect::RequestSetChance(42.5));
    // Buffer is consumed on commit regardless of validity.
    assert!(m.chance_input.is_none());
}

#[test]
fn chance_input_commit_accepts_boundaries_0_and_100() {
    for (input, expected) in [("0", 0.0), ("100", 100.0)] {
        let mut m = Model::new(8);
        m.apply(Update::BeginChanceInput);
        for c in input.chars() {
            m.apply(Update::ChanceInputChar(c));
        }
        let eff = m.apply(Update::ChanceInputCommit);
        assert_eq!(eff, Effect::RequestSetChance(expected));
    }
}

#[test]
fn chance_input_commit_rejects_out_of_range_and_garbage() {
    // The char filter would normally keep these inputs out of the
    // buffer; set them directly so we exercise the commit-stage
    // defense rather than the input-stage filter.
    for bad in ["", "150", "-3", "nope"] {
        let mut m = Model::new(8);
        m.apply(Update::BeginChanceInput);
        m.chance_input = Some(bad.to_string());
        let eff = m.apply(Update::ChanceInputCommit);
        assert_eq!(eff, Effect::None, "expected None for {:?}", bad);
        assert!(m.chance_input.is_none(), "buffer must drain on commit");
    }
}

#[test]
fn chance_input_char_filter_drops_non_numeric_and_extra_dots() {
    let mut m = Model::new(8);
    m.apply(Update::BeginChanceInput);
    for c in "1a.2b.3".chars() {
        m.apply(Update::ChanceInputChar(c));
    }
    // Letters dropped; second '.' dropped → "1.23".
    assert_eq!(m.chance_input.as_deref(), Some("1.23"));
}

// ── parse_agg_input edge cases ──────────────────────────────

#[test]
fn parse_agg_input_rejects_negative_empty_p_and_nonsense() {
    assert!(parse_agg_input("").is_none());
    assert!(parse_agg_input("p").is_none());
    assert!(parse_agg_input("p-5").is_none());
    assert!(parse_agg_input("p-0.1").is_none());
    assert!(parse_agg_input("pnan").is_none());
    assert!(parse_agg_input("pinf").is_none());
    assert!(parse_agg_input("foo").is_none());
    assert!(parse_agg_input("avg ").is_some()); // trailing whitespace tolerated
}

// ── Bin::aggregate via series_summary.last_ns ───────────────

#[test]
fn graph_percentile_via_series_summary_last() {
    let mut m = Model::new(32);
    m.apply(Update::SpanReceived(timed_span(10, None, "a", 0, 10)));
    m.apply(Update::ToggleGraph);
    // 10 more spans, all closed in the very first 1-second bin
    // (closed_at_ns ranges from 20 to 200ns), with totals
    // {20, 40, …, 200} so percentile arithmetic is deterministic.
    for i in 1u64..=10 {
        m.apply(Update::SpanReceived(timed_span(
            10 + i,
            None,
            "a",
            0,
            20 * i,
        )));
    }
    let gs = current_graph(&m);
    assert_eq!(gs.store.series.len(), 1);
    // Sorted samples include the original (10) plus the new 10 →
    // [10, 20, 40, ..., 200].  p50 of 11 samples is index
    // round(0.5 * 10) = 5 → 100ns.
    let summary = gs.store.series_summary(AggMode::Percentile(50.0));
    assert_eq!(summary[0].last_ns, 100);
    // p100 should map to the max sample (200).
    let summary = gs.store.series_summary(AggMode::Max);
    assert_eq!(summary[0].last_ns, 200);
}

#[test]
fn graph_self_time_metric_subtracts_child_total() {
    let mut m = Model::new(16);
    // Parent total = 1000ns; child total = 300ns → SelfTime parent = 700ns.
    m.apply(Update::SpanReceived(timed_span(10, None, "root", 0, 1000)));
    m.apply(Update::SpanReceived(timed_span(
        11,
        Some(10),
        "leaf",
        100,
        400,
    )));
    m.apply(Update::ToggleGraph);
    // Default Metric::Total — the lone root sample is the full 1000.
    let gs = current_graph(&m);
    assert_eq!(gs.metric, Metric::Total);
    assert_eq!(
        gs.store.series_summary(AggMode::Avg)[0].last_ns,
        1000,
    );
    // Flip to SelfTime; rehydrate re-walks the ring with the
    // up-to-date child_sum, so the root re-records as 1000 - 300.
    m.apply(Update::ToggleGraphMetric);
    let gs = current_graph(&m);
    assert_eq!(gs.metric, Metric::SelfTime);
    assert_eq!(
        gs.store.series_summary(AggMode::Avg)[0].last_ns,
        700,
    );
}

// ── Multi-key splits ────────────────────────────────────────

#[test]
fn graph_multi_key_splits_partition_cartesian_product() {
    let mut m = Model::new(32);
    let combos = [
        ("fetch", "alice"),
        ("fetch", "bob"),
        ("post", "alice"),
        ("post", "bob"),
    ];
    for (i, (api, user)) in combos.iter().enumerate() {
        let mut s = timed_span_with_field(
            10 + i as u64,
            None,
            "req",
            (i as u64) * 100,
            (i as u64) * 100 + 50,
            "api",
            api,
        );
        s.fields.push((
            "user".into(),
            tracing_console_host::WireFieldValue::Str((*user).into()),
        ));
        m.apply(Update::SpanReceived(s));
    }
    m.apply(Update::ToggleGraph);
    m.apply(Update::GraphSwitchFocus);
    // candidate_split_keys is alphabetical: ["api", "user"].
    // Enable "api" at key_idx 0.
    move_cursor_to_first_key(&mut m);
    m.apply(Update::GraphToggleSplit);
    // After re-pin, cursor sits at the same key row; advance one
    // line to land on "user" and toggle it on too.
    m.apply(Update::GraphSelectDown);
    m.apply(Update::GraphToggleSplit);
    let gs = current_graph(&m);
    assert!(gs.split_keys.contains("api"));
    assert!(gs.split_keys.contains("user"));
    // 2×2 cartesian product of {fetch,post} × {alice,bob} = 4 series.
    assert_eq!(gs.store.series.len(), 4);
}

// ── Graph window-input defense-in-depth ─────────────────────

#[test]
fn graph_window_input_commit_rejects_nan_and_inf_at_commit_stage() {
    // The input filter would never let "nan" reach the buffer, but
    // the commit path also guards against non-finite values.  Set
    // the buffer directly to confirm the second check holds.
    for bad in ["nan", "inf", "-inf"] {
        let mut m = Model::new(8);
        m.apply(Update::SpanReceived(span(10, "a")));
        m.apply(Update::ToggleGraph);
        let original = current_graph(&m).window_secs;
        if let ViewMode::Graph(gs) = &mut m.view {
            gs.window_input = Some(bad.to_string());
        }
        m.apply(Update::GraphWindowInputCommit);
        let gs = current_graph(&m);
        assert!(
            (gs.window_secs - original).abs() < 1e-9,
            "window_secs changed on {:?}",
            bad,
        );
        assert!(gs.window_input.is_none(), "buffer must drain on commit");
    }
}

// ── Explore mode ────────────────────────────────────────────

fn current_explore(m: &Model) -> &ExploreState {
    match &m.view {
        ViewMode::Explore(es) => es,
        other => panic!("expected explore view, got {other:?}"),
    }
}

#[test]
fn enter_explore_requires_a_selected_row_and_sets_level_off() {
    let mut m = Model::new(8);
    // No rows yet — pressing `e` is a no-op.
    let eff = m.apply(Update::EnterExplore);
    assert_eq!(eff, Effect::None);
    assert!(matches!(m.view, ViewMode::Table));

    // With a row present, EnterExplore locks onto its stack and
    // emits a `RequestSetLevel(Off)` so producers stop streaming.
    m.apply(Update::SpanReceived(span(10, "root")));
    let eff = m.apply(Update::EnterExplore);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Off));
    let es = current_explore(&m);
    assert_eq!(es.locked_stack, vec!["root".to_string()]);
}

#[test]
fn exit_explore_restores_the_prior_cache_level() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    // Pretend the server confirmed Info before we entered.
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Info));
    m.apply(Update::EnterExplore);
    let eff = m.apply(Update::ExitExplore);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Info));
    assert!(matches!(m.view, ViewMode::Table));
}

#[test]
fn exit_explore_with_unknown_prior_level_defaults_to_trace() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    // No CacheLevelReceived yet → restore_level == None.
    m.apply(Update::EnterExplore);
    let eff = m.apply(Update::ExitExplore);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Trace));
}

#[test]
fn explore_search_filters_by_field_value() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span_with_field(10, "root", None, "api", "fetch")));
    m.apply(Update::SpanReceived(span_with_field(11, "root", None, "api", "post")));
    m.apply(Update::SpanReceived(span_with_field(12, "root", None, "api", "delete")));
    m.apply(Update::EnterExplore);
    // No filter → all 3 visible.
    let es = current_explore(&m);
    assert_eq!(crate::model::explore::matching_spans(&m, es).len(), 3);

    // Live-filter by typing "fe" — only `fetch` matches.
    m.apply(Update::BeginExploreSearch);
    m.apply(Update::ExploreSearchChar('f'));
    m.apply(Update::ExploreSearchChar('e'));
    let es = current_explore(&m);
    assert_eq!(crate::model::explore::matching_spans(&m, es).len(), 1);

    // Commit then cancel resets selected to 0; filter persists.
    m.apply(Update::ExploreSearchCommit);
    let es = current_explore(&m);
    assert!(es.search_input.is_none());
    assert_eq!(es.query, "fe");
    assert_eq!(crate::model::explore::matching_spans(&m, es).len(), 1);
}

#[test]
fn explore_search_cancel_keeps_prior_committed_query() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span_with_field(10, "root", None, "api", "fetch")));
    m.apply(Update::EnterExplore);
    // Commit "fetch" then open a fresh search and Esc — original
    // query is preserved.
    m.apply(Update::BeginExploreSearch);
    for c in "fetch".chars() {
        m.apply(Update::ExploreSearchChar(c));
    }
    m.apply(Update::ExploreSearchCommit);
    assert_eq!(current_explore(&m).query, "fetch");
    m.apply(Update::BeginExploreSearch);
    m.apply(Update::ExploreSearchChar('x'));
    m.apply(Update::ExploreSearchCancel);
    assert_eq!(current_explore(&m).query, "fetch");
}

#[test]
fn explore_open_trace_swaps_to_trace_detail_keeping_explore_state() {
    let mut m = Model::new(16);
    let mut s = span(10, "root");
    s.opened_at_ns = 0;
    s.closed_at_ns = Some(100);
    m.apply(Update::SpanReceived(s));
    m.apply(Update::EnterExplore);
    m.apply(Update::ExploreOpenTrace);
    match &m.view {
        ViewMode::TraceDetail(td) => {
            assert_eq!(td.root_id, 10);
            assert_eq!(td.selected_idx, 0);
            assert!(td.collapsed.is_empty());
            assert_eq!(td.explore.locked_stack, vec!["root".to_string()]);
        }
        other => panic!("expected trace detail, got {other:?}"),
    }
    // Esc goes back to explore with the same locked stack.
    m.apply(Update::ExitTraceDetail);
    assert_eq!(current_explore(&m).locked_stack, vec!["root".to_string()]);
}

#[test]
fn explore_sort_cycle_walks_time_latency_then_field_columns() {
    let mut m = Model::new(16);
    // Two distinct field values → "api" becomes a distinguishing column.
    m.apply(Update::SpanReceived(span_with_field(10, "root", None, "api", "a")));
    m.apply(Update::SpanReceived(span_with_field(11, "root", None, "api", "b")));
    m.apply(Update::EnterExplore);
    assert_eq!(current_explore(&m).sort, ExploreSortColumn::Timestamp);
    m.apply(Update::ExploreSortRight);
    assert_eq!(current_explore(&m).sort, ExploreSortColumn::Latency);
    m.apply(Update::ExploreSortRight);
    assert_eq!(
        current_explore(&m).sort,
        ExploreSortColumn::Field("api".into()),
    );
    // Wraps back to Timestamp.
    m.apply(Update::ExploreSortRight);
    assert_eq!(current_explore(&m).sort, ExploreSortColumn::Timestamp);
    // Left goes the other direction.
    m.apply(Update::ExploreSortLeft);
    assert_eq!(
        current_explore(&m).sort,
        ExploreSortColumn::Field("api".into()),
    );
}

#[test]
fn explore_invert_sort_flips_direction_and_reorders() {
    let mut m = Model::new(16);
    let mut a = span(10, "root");
    a.opened_at_ns = 0;
    a.closed_at_ns = Some(100);
    let mut b = span(11, "root");
    b.opened_at_ns = 500;
    b.closed_at_ns = Some(600);
    m.apply(Update::SpanReceived(a));
    m.apply(Update::SpanReceived(b));
    m.apply(Update::EnterExplore);
    // Time defaults to descending — newest first.
    let ids: Vec<u64> = crate::model::explore::matching_spans(&m, current_explore(&m))
        .iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, vec![11, 10]);
    // `i` flips to ascending — oldest first.
    m.apply(Update::ExploreInvertSort);
    let ids: Vec<u64> = crate::model::explore::matching_spans(&m, current_explore(&m))
        .iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, vec![10, 11]);
}

#[test]
fn explore_cycle_sort_resets_direction_to_new_columns_default() {
    let mut m = Model::new(16);
    m.apply(Update::SpanReceived(span_with_field(10, "root", None, "api", "a")));
    m.apply(Update::SpanReceived(span_with_field(11, "root", None, "api", "b")));
    m.apply(Update::EnterExplore);
    // Time default = descending; flip to ascending.
    m.apply(Update::ExploreInvertSort);
    assert_eq!(
        current_explore(&m).direction,
        crate::model::explore::SortDirection::Asc,
    );
    // Cycling to Latency resets direction to *its* default (Desc).
    m.apply(Update::ExploreSortRight);
    assert_eq!(current_explore(&m).sort, ExploreSortColumn::Latency);
    assert_eq!(
        current_explore(&m).direction,
        crate::model::explore::SortDirection::Desc,
    );
    // Field columns default to ascending.
    m.apply(Update::ExploreSortRight);
    assert_eq!(
        current_explore(&m).sort,
        ExploreSortColumn::Field("api".into()),
    );
    assert_eq!(
        current_explore(&m).direction,
        crate::model::explore::SortDirection::Asc,
    );
}

#[test]
fn explore_default_sort_orders_newest_span_first() {
    // Regression: time sort must be descending (newest at the
    // top) so the user reads down chronologically backward.
    let mut m = Model::new(16);
    let mut a = span(10, "root");
    a.opened_at_ns = 0;
    a.closed_at_ns = Some(100);
    let mut b = span(11, "root");
    b.opened_at_ns = 500;
    b.closed_at_ns = Some(600);
    let mut c = span(12, "root");
    c.opened_at_ns = 1000;
    c.closed_at_ns = Some(1100);
    m.apply(Update::SpanReceived(a));
    m.apply(Update::SpanReceived(b));
    m.apply(Update::SpanReceived(c));
    m.apply(Update::EnterExplore);
    let es = current_explore(&m);
    let ids: Vec<u64> = crate::model::explore::matching_spans(&m, es)
        .iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, vec![12, 11, 10]);
}

#[test]
fn enter_table_from_explore_restores_level() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Debug));
    m.apply(Update::EnterExplore);
    let eff = m.apply(Update::EnterTable);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Debug));
    assert!(matches!(m.view, ViewMode::Table));
}

#[test]
fn enter_table_from_trace_detail_pops_all_the_way_up() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Info));
    m.apply(Update::EnterExplore);
    m.apply(Update::ExploreOpenTrace);
    assert!(matches!(m.view, ViewMode::TraceDetail(_)));
    // `s` from trace-detail must skip explore entirely and land
    // on the stacks table, while still restoring the level.
    let eff = m.apply(Update::EnterTable);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Info));
    assert!(matches!(m.view, ViewMode::Table));
}

#[test]
fn enter_explore_from_graph_carries_locked_stack_and_sets_level_off() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Info));
    m.apply(Update::ToggleGraph);
    assert!(matches!(m.view, ViewMode::Graph(_)));
    // `e` from graph mode opens explore on the graph's locked
    // stack and silences ingest until exit.
    let eff = m.apply(Update::EnterExplore);
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Off));
    let es = current_explore(&m);
    assert_eq!(es.locked_stack, vec!["root".to_string()]);
    assert_eq!(es.restore_level, Some(WireLevelFilter::Info));
}

#[test]
fn enter_graph_from_explore_carries_locked_stack_and_restores_level() {
    let mut m = Model::new(8);
    m.apply(Update::SpanReceived(span(10, "root")));
    m.apply(Update::CacheLevelReceived(WireLevelFilter::Trace));
    m.apply(Update::EnterExplore);
    let eff = m.apply(Update::EnterGraph);
    // Leaving Explore re-enables the previously-confirmed level.
    assert_eq!(eff, Effect::RequestSetLevel(WireLevelFilter::Trace));
    match &m.view {
        ViewMode::Graph(gs) => assert_eq!(gs.locked_stack, vec!["root".to_string()]),
        other => panic!("expected graph, got {other:?}"),
    }
}

#[test]
fn trace_detail_collapse_then_expand_round_trip() {
    let mut m = Model::new(16);
    let mut root = span(10, "root");
    root.opened_at_ns = 0;
    root.closed_at_ns = Some(100);
    let mut child = span_with_parent(11, "child", Some(10));
    child.opened_at_ns = 10;
    child.closed_at_ns = Some(50);
    m.apply(Update::SpanReceived(root));
    m.apply(Update::SpanReceived(child));
    m.apply(Update::EnterExplore);
    m.apply(Update::ExploreOpenTrace);
    // Cursor sits on the root by default.  Collapse hides the
    // child; expand reveals it again.
    let before = match &m.view {
        ViewMode::TraceDetail(td) => crate::model::explore::visible_trace_rows(&m, td).len(),
        _ => panic!("expected trace detail"),
    };
    assert_eq!(before, 2);
    m.apply(Update::TraceDetailCollapse);
    let after_collapse = match &m.view {
        ViewMode::TraceDetail(td) => {
            assert!(td.collapsed.contains(&10));
            crate::model::explore::visible_trace_rows(&m, td).len()
        }
        _ => panic!("expected trace detail"),
    };
    assert_eq!(after_collapse, 1, "child must hide when root collapses");
    m.apply(Update::TraceDetailExpand);
    let after_expand = match &m.view {
        ViewMode::TraceDetail(td) => {
            assert!(td.collapsed.is_empty());
            crate::model::explore::visible_trace_rows(&m, td).len()
        }
        _ => panic!("expected trace detail"),
    };
    assert_eq!(after_expand, 2);
}
