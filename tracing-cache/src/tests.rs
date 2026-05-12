//! Unit tests covering the `SpanCache` subscriber surface — span creation,
//! eviction, parent resolution, fields, events, pagination, and a tokio
//! task overlap scenario.

use std::sync::Arc;

use tracing::metadata::LevelFilter;
use tracing::{Level, Metadata};

use crate::cache::SpanCache;
use crate::config::CacheConfig;
use crate::driver::Driver;
use crate::id_encoding::DISABLED;
use crate::predicate::{EnabledPredicate, Interest, LevelPredicate};
use crate::record::{EventRecord, FieldValue, SpanRecord};

// ── field helpers ─────────────────────────────────────────────────────────

/// Print the named field of a `SpanRecord` to a `String`, or `None` if absent.
fn span_field(record: &SpanRecord, name: &str) -> Option<String> {
    record.field(name).map(|v| v.to_display_string().to_string())
}

/// Same, for events.
fn event_field(event: &EventRecord, name: &str) -> Option<String> {
    event.field(name).map(|v| v.to_display_string().to_string())
}

#[allow(dead_code)]
fn assert_field_value(record: &SpanRecord, name: &str, expected: &str) {
    let got = span_field(record, name);
    assert_eq!(got.as_deref(), Some(expected), "field {name:?}");
}

#[allow(dead_code)]
fn fv_str(value: &FieldValue) -> String {
    value.to_display_string().to_string()
}

// ── helpers ──────────────────────────────────────────────────────────────

fn make_cache(capacity: usize) -> (Arc<SpanCache>, Driver) {
    let (cache, driver) = SpanCache::new(capacity);
    (Arc::new(cache), driver)
}

/// Runs `f` under `cache` as the active subscriber, then flushes and drains
/// so all closed spans are committed to the map before returning.
fn run_with_drain<F, T>(cache: &Arc<SpanCache>, driver: Driver, f: F) -> T
where
    F: FnOnce() -> T,
{
    let result = tracing::subscriber::with_default(Arc::clone(cache), f);
    cache.flush_pending();
    driver.drain_sync();
    result
}

fn span_id(span: &tracing::Span) -> Option<u64> {
    span.id().map(|id| id.into_u64())
}

/// Captures `SpanRecord.id` (the actual_id) of an in-flight span — needed
/// to look it up in the closed-span map after drain (the tracing id and
/// actual id live in disjoint namespaces).
fn actual_id_of(cache: &Arc<SpanCache>, span: &tracing::Span) -> u64 {
    cache.get_active_span(span_id(span).unwrap()).unwrap().id
}

struct DisableByName(pub &'static str);

impl EnabledPredicate for DisableByName {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        None
    }
    fn callsite_enabled(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::Sometimes
    }
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
        span.metadata().name() != self.0
    }
}

// ── tests ─────────────────────────────────────────────────────────────────

#[test]
fn basic_span_creation_and_retrieval() {
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(parent: None, Level::INFO, "root", field = "value");
        let actual_id = actual_id_of(&cache, &span);
        let _g = span.enter();
        actual_id
    });
    let record = cache.get_span(actual_id).unwrap();
    assert_eq!(record.id, actual_id);
    assert_eq!(record.metadata.name(), "root");
    assert_eq!(span_field(&record, "field").as_deref(), Some("value"));
}

#[test]
fn closed_at_set_after_drop() {
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(parent: None, Level::INFO, "root");
        let tracing_id = span_id(&span).unwrap();
        let actual_id = cache.get_active_span(tracing_id).unwrap().id;
        {
            let _g = span.enter();
        }
        // While alive: lookup by tracing id finds the slab entry.
        assert!(
            cache.get_active_span(tracing_id).unwrap().closed_at.is_none(),
            "not closed while span is alive"
        );
        actual_id
        // span drops here → try_close → PENDING
    });
    // After drain: lookup by actual_id finds the BTreeMap entry.
    assert!(
        cache.get_span(actual_id).unwrap().closed_at.is_some(),
        "should be closed after Span drops"
    );
}

#[test]
fn child_of_disabled_is_disabled() {
    let (cache_inner, driver) = SpanCache::with_predicate(10, DisableByName("bad_parent"));
    let cache = Arc::new(cache_inner);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        let parent = tracing::span!(parent: None, Level::INFO, "bad_parent");
        assert_eq!(span_id(&parent), Some(DISABLED), "predicate disables this span");
        let _g = parent.enter(); // pushes DISABLED onto thread-local stack
        let child = tracing::span!(Level::INFO, "child");
        assert_eq!(child.id(), None, "child of DISABLED is a tracing no-op");
    });
    drop(driver);
}

#[test]
fn contextual_span_with_empty_stack_is_disabled() {
    let (cache, driver) = make_cache(10);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        let span = tracing::span!(Level::INFO, "contextual");
        assert_eq!(span_id(&span), Some(DISABLED));
    });
    drop(driver);
}

#[test]
fn root_span_with_active_stack_is_disabled() {
    let (cache, driver) = make_cache(10);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        let root_a = tracing::span!(parent: None, Level::INFO, "root_a");
        let _g = root_a.enter();
        let root_b = tracing::span!(parent: None, Level::INFO, "root_b");
        assert_eq!(span_id(&root_b), Some(DISABLED));
    });
    drop(driver);
}

#[test]
fn eviction_removes_closed_spans() {
    // Single-lane so capacity=2 means "2 in-flight", not "2 lanes × 1
    // each".  With thread-id sharding all spans on this thread go to the
    // same shard, so a multi-lane setup with capacity=2 could only fit 1
    // in-flight per thread — not what this test wants to verify.
    let (cache, driver) = SpanCache::with_config(
        2,
        CacheConfig { lane_count: 1, ..CacheConfig::default() },
    );
    let cache = Arc::new(cache);
    let (a, b, c) = run_with_drain(&cache, driver, || {
        let span_a = tracing::span!(parent: None, Level::INFO, "a");
        let a = actual_id_of(&cache, &span_a);
        let span_b = tracing::span!(parent: None, Level::INFO, "b");
        let b = actual_id_of(&cache, &span_b);
        drop(span_a);
        drop(span_b);
        // in_flight is empty; C is allowed.
        let span_c = tracing::span!(parent: None, Level::INFO, "c");
        assert_ne!(span_id(&span_c), Some(DISABLED), "C should be enabled");
        let c = actual_id_of(&cache, &span_c);
        (a, b, c)
    });
    // Driver inserted A, B, then C: capacity=2, so A was evicted when C was inserted.
    assert!(cache.get_span(a).is_none(), "A should have been evicted");
    assert!(cache.get_span(b).is_some(), "B should still be in cache");
    assert!(cache.get_span(c).is_some(), "C should be in cache");
    let page_ids: Vec<u64> = cache.page(0, 10).iter().map(|s| s.id).collect();
    assert!(!page_ids.contains(&a));
    assert!(page_ids.contains(&b));
    assert!(page_ids.contains(&c));
}

#[test]
fn eviction_full_of_open_spans_returns_disabled() {
    // With thread-id sharding, all spans on this thread land on a single
    // shard.  capacity=2 spread over 16 lanes → per-shard cap = 1, so
    // creating two simultaneously-alive spans on this thread fills the
    // shard's slot and the second is DISABLED.
    let (cache, driver) = make_cache(2);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        let _s1 = tracing::span!(parent: None, Level::INFO, "s1");
        let s2 = tracing::span!(parent: None, Level::INFO, "s2");
        assert_eq!(span_id(&s2), Some(DISABLED));
    });
    drop(driver);
}

#[test]
fn custom_lane_count_is_respected() {
    // 4 lanes, capacity 4 → per-shard cap 1.  With thread-id sharding,
    // this thread always picks one shard, so the 2nd simultaneously-alive
    // span on that shard is DISABLED.
    let (cache, driver) = SpanCache::with_config(
        4,
        CacheConfig { lane_count: 4, ..CacheConfig::default() },
    );
    let cache = Arc::new(cache);
    assert_eq!(cache.lane_count(), 4);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        let _s1 = tracing::span!(parent: None, Level::INFO, "s1");
        let s2 = tracing::span!(parent: None, Level::INFO, "s2");
        assert_eq!(span_id(&s2), Some(DISABLED));
    });
    drop(driver);
}

#[test]
fn separate_threads_get_distinct_keys() {
    // Each thread's first new_span claims a fresh slot from the global
    // NEXT_THREAD_KEY counter, so independent threads don't all collide
    // on a single shard.  The exact mapping is implementation-defined
    // (depends on counter state from prior tests), so we assert the
    // weakest interesting property: when we run a handful of threads
    // against a wide cache, at least two distinct shards are exercised.
    use std::collections::HashSet;
    use std::sync::Mutex;

    // Wide enough that test interleaving doesn't pin everyone on one shard.
    let (cache, driver) = SpanCache::with_config(
        64 * 16,
        CacheConfig { lane_count: 16, ..CacheConfig::default() },
    );
    let cache = Arc::new(cache);
    let observed: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let cache = Arc::clone(&cache);
        let observed = Arc::clone(&observed);
        handles.push(std::thread::spawn(move || {
            tracing::subscriber::with_default(cache, || {
                let s = tracing::span!(parent: None, Level::INFO, "tt");
                let id = span_id(&s).unwrap();
                observed.lock().unwrap().push(id);
                // Hold the span alive briefly so other threads observe a
                // populated slab if they happen to land on the same shard.
                std::thread::sleep(std::time::Duration::from_millis(5));
            });
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let ids = observed.lock().unwrap().clone();
    let shards: HashSet<u64> = ids.iter().map(|id| id >> 60).collect();
    assert!(
        shards.len() >= 2,
        "expected ≥2 distinct shards across 8 threads, got {shards:?}",
    );
    drop(driver);
}

#[test]
fn lane_count_is_clamped_and_rounded_to_power_of_two() {
    // Out-of-range / non-power-of-two values are silently normalised to
    // the next power of two within [1, 256].
    let cases = [
        (0_usize, 1_usize),     // zero → minimum lane count of 1
        (1, 1),
        (3, 4),                 // round up
        (5, 8),
        (16, 16),               // already a power of two
        (200, 256),             // round up to ceiling
        (256, 256),
        (1000, 256),            // capped at 256
    ];
    for (input, expected) in cases {
        let (cache, _driver) = SpanCache::with_config(
            64,
            CacheConfig { lane_count: input, ..CacheConfig::default() },
        );
        assert_eq!(
            cache.lane_count(),
            expected,
            "lane_count({input}) should normalise to {expected}",
        );
    }
}

#[test]
fn pagination() {
    let (cache, driver) = make_cache(10);
    let ids: Vec<u64> = run_with_drain(&cache, driver, || {
        let mut ids = Vec::new();
        for _ in 0..5usize {
            let span = tracing::span!(parent: None, Level::INFO, "s");
            ids.push(actual_id_of(&cache, &span));
            // span drops here → closed via try_close → PENDING
        }
        ids
    });
    assert_eq!(ids.len(), 5);

    let p1 = cache.page(0, 3);
    assert_eq!(p1.len(), 3);
    assert_eq!(p1[0].id, ids[0]);
    assert_eq!(p1[2].id, ids[2]);

    let last = p1.last().unwrap().id;
    let p2 = cache.page(last, 3);
    assert_eq!(p2.len(), 2);
    assert_eq!(p2[0].id, ids[3]);
    assert_eq!(p2[1].id, ids[4]);

    assert!(cache.page(ids[4] + 1000, 3).is_empty());
}

#[test]
fn event_attached_to_current_span() {
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(parent: None, Level::INFO, "root");
        let actual_id = actual_id_of(&cache, &span);
        let _g = span.enter();
        tracing::event!(Level::INFO, "test event happened");
        actual_id
    });
    let record = cache.get_span(actual_id).unwrap();
    assert_eq!(record.events.len(), 1);
    assert!(
        record.events[0].field("message").is_some(),
        "event should have a message field"
    );
}

#[test]
fn event_dropped_with_no_active_span() {
    let (cache, driver) = make_cache(10);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        tracing::event!(Level::INFO, "orphan event");
    });
    drop(driver);
    assert!(cache.page(0, 10).is_empty());
}

#[test]
fn field_capture() {
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(
            parent: None,
            Level::INFO,
            "fields",
            str_field = "hello",
            int_field = 42i64,
            bool_field = true,
        );
        actual_id_of(&cache, &span)
    });
    let record = cache.get_span(actual_id).unwrap();
    assert_eq!(span_field(&record, "str_field").as_deref(), Some("hello"));
    assert_eq!(span_field(&record, "int_field").as_deref(), Some("42"));
    assert_eq!(span_field(&record, "bool_field").as_deref(), Some("true"));
}

// ── API-handler-shape coverage ────────────────────────────────────────────

#[test]
fn record_updates_span_fields_after_creation() {
    // Common API-handler pattern: span!(...) declares a field with no value
    // up front, then span.record() fills it in once the operation finishes.
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(
            parent: None,
            Level::INFO,
            "op",
            initial = "ready",
            status = tracing::field::Empty,
        );
        let actual_id = actual_id_of(&cache, &span);
        span.record("status", "success");
        actual_id
    });
    let record = cache.get_span(actual_id).unwrap();
    assert_eq!(span_field(&record, "initial").as_deref(), Some("ready"));
    assert_eq!(span_field(&record, "status").as_deref(), Some("success"));
}

#[test]
fn multiple_events_recorded_in_order() {
    let (cache, driver) = make_cache(10);
    let actual_id = run_with_drain(&cache, driver, || {
        let span = tracing::span!(parent: None, Level::INFO, "op");
        let actual_id = actual_id_of(&cache, &span);
        let _g = span.enter();
        tracing::event!(Level::INFO, step = "first");
        tracing::event!(Level::INFO, step = "second", note = "middle");
        tracing::event!(Level::INFO, step = "third");
        actual_id
    });
    let record = cache.get_span(actual_id).unwrap();
    assert_eq!(record.events.len(), 3);
    let steps: Vec<String> = record
        .events
        .iter()
        .map(|e| event_field(e, "step").unwrap())
        .collect();
    assert_eq!(steps, vec!["first", "second", "third"]);
    assert_eq!(
        event_field(&record.events[1], "note").as_deref(),
        Some("middle"),
    );
    // Timestamps monotonically non-decreasing.
    assert!(record.events[0].recorded_at <= record.events[1].recorded_at);
    assert!(record.events[1].recorded_at <= record.events[2].recorded_at);
}

#[test]
fn sibling_spans_share_parent_actual_id() {
    // 4 spans alive simultaneously on one thread (root + 3 siblings).
    // With 16 lanes that needs per-shard cap ≥ 4, so capacity ≥ 64.
    let (cache, driver) = make_cache(64);
    let (root_id, sibling_ids) = run_with_drain(&cache, driver, || {
        let root = tracing::span!(parent: None, Level::INFO, "root");
        let root_id = actual_id_of(&cache, &root);
        let _g = root.enter();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let sib = tracing::span!(Level::INFO, "child");
            ids.push(actual_id_of(&cache, &sib));
            // sib drops at end of loop iteration → close
        }
        (root_id, ids)
    });
    for (i, &sid) in sibling_ids.iter().enumerate() {
        let s = cache.get_span(sid).unwrap();
        assert_eq!(s.parent_id, Some(root_id), "sibling #{i} parent_id");
        assert_eq!(s.metadata.name(), "child");
    }
}

#[test]
fn level_predicate_filters_below_threshold() {
    let (inner, driver) = SpanCache::with_predicate(10, LevelPredicate::new(Level::INFO));
    let cache = Arc::new(inner);
    tracing::subscriber::with_default(Arc::clone(&cache), || {
        // INFO is at the threshold — enabled.
        let info_span = tracing::span!(parent: None, Level::INFO, "info_op");
        assert!(info_span.id().is_some(), "INFO at INFO threshold");
        // ERROR is more severe — enabled.
        let error_span = tracing::span!(parent: None, Level::ERROR, "error_op");
        assert!(error_span.id().is_some(), "ERROR at INFO threshold");
        // DEBUG is below the threshold; tracing's macro short-circuits when
        // callsite_enabled returns Never, so the Span has no id.
        let debug_span = tracing::span!(parent: None, Level::DEBUG, "debug_op");
        assert!(debug_span.id().is_none(), "DEBUG filtered at INFO threshold");
    });
    drop(driver);
}

#[test]
fn api_handler_lifecycle() {
    // The whole reason the cache exists, expressed as a test: a request
    // root span with a deferred field, two sibling child spans (one with
    // its own field, one with two events), a deferred record() on the
    // root once everything finishes.
    let (cache, driver) = make_cache(20);
    let request_id = run_with_drain(&cache, driver, || {
        let request = tracing::span!(
            parent: None,
            Level::INFO,
            "request",
            method = "GET",
            path = "/users/42",
            status = tracing::field::Empty,
        );
        let request_id = actual_id_of(&cache, &request);
        let _g = request.enter();

        {
            let validate = tracing::span!(Level::INFO, "validate", ok = true);
            let _v = validate.enter();
            tracing::event!(Level::INFO, message = "validation passed");
        }

        {
            let query = tracing::span!(Level::INFO, "db_query", table = "users");
            let _q = query.enter();
            tracing::event!(Level::INFO, message = "query started");
            tracing::event!(Level::INFO, message = "query finished", rows = 1u64);
        }

        request.record("status", "200");
        request_id
    });

    let pages = cache.page(0, 100);
    assert_eq!(pages.len(), 3, "request, validate, db_query all present");

    let request = cache.get_span(request_id).unwrap();
    assert_eq!(request.metadata.name(), "request");
    assert_eq!(request.parent_id, None);
    assert_eq!(span_field(&request, "method").as_deref(), Some("GET"));
    assert_eq!(span_field(&request, "path").as_deref(), Some("/users/42"));
    assert_eq!(span_field(&request, "status").as_deref(), Some("200"));

    let validate = pages.iter().find(|s| s.metadata.name() == "validate").unwrap();
    assert_eq!(validate.parent_id, Some(request_id));
    assert_eq!(span_field(validate, "ok").as_deref(), Some("true"));
    assert_eq!(validate.events.len(), 1);
    assert_eq!(
        event_field(&validate.events[0], "message").as_deref(),
        Some("validation passed"),
    );

    let query = pages.iter().find(|s| s.metadata.name() == "db_query").unwrap();
    assert_eq!(query.parent_id, Some(request_id));
    assert_eq!(span_field(query, "table").as_deref(), Some("users"));
    assert_eq!(query.events.len(), 2);
    let messages: Vec<String> = query
        .events
        .iter()
        .map(|e| event_field(e, "message").unwrap())
        .collect();
    assert_eq!(messages, vec!["query started", "query finished"]);
    assert_eq!(event_field(&query.events[1], "rows").as_deref(), Some("1"));
}

// ── public re-export surface ──────────────────────────────────────────────

/// Compiles-only test: every public type pulled in via `crate::*` (the
/// re-exports from lib.rs) is reachable and usable.  Catches accidental
/// privacy regressions in the lib.rs `pub use` block — if any of these
/// imports stops resolving, the refactor lost a public surface item.
#[test]
fn public_api_reexports_are_reachable() {
    use crate::{
        CacheConfig as ReexportedConfig, Driver as ReexportedDriver,
        EventRecord as ReexportedEventRecord, Interest as ReexportedInterest,
        LevelPredicate as ReexportedLevelPredicate, SpanCache as ReexportedSpanCache,
        SpanRecord as ReexportedSpanRecord, DEFAULT_LANE_COUNT,
    };
    // EnabledPredicate is re-exported (used as an associated trait object in
    // `with_predicate`); the line below confirms it's reachable as a path.
    let _: fn(crate::CacheConfig) -> crate::CacheConfig = std::convert::identity;
    fn _check_predicate_reexport<P: crate::EnabledPredicate>(_: P) {}

    let _ = DEFAULT_LANE_COUNT;
    let _: ReexportedInterest = ReexportedInterest::Always;
    let _ = ReexportedLevelPredicate::new(Level::INFO);
    let cfg = ReexportedConfig::default();

    let (cache, driver): (ReexportedSpanCache, ReexportedDriver) =
        ReexportedSpanCache::with_config(8, cfg);
    let cache = Arc::new(cache);
    let _id = run_with_drain(&cache, driver, || {
        let s = tracing::span!(parent: None, Level::INFO, "smoke");
        actual_id_of(&cache, &s)
    });
    let pages: Vec<ReexportedSpanRecord> = cache.page(0, 4);
    let _: Option<&ReexportedEventRecord> = pages.first().and_then(|s| s.events.first());
}

// ── async overlap test ────────────────────────────────────────────────────

#[test]
fn async_instrumented_tasks_with_overlapping_spans() {
    use tracing_futures::Instrument;

    // Up to 4 spans (root_a, root_b, acquire, release) are simultaneously
    // alive on the current_thread runtime — all on this thread's shard
    // under thread-id sharding.  capacity=64 / 16 lanes → per-shard cap 4.
    let (cache, driver) = make_cache(64);

    tracing::subscriber::with_default(Arc::clone(&cache), || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let sem = Arc::new(tokio::sync::Semaphore::new(0));
                let sem_a = Arc::clone(&sem);
                let sem_b = Arc::clone(&sem);

                let root_a = tracing::span!(parent: None, Level::INFO, "task_a");
                let root_b = tracing::span!(parent: None, Level::INFO, "task_b");

                let h_a = tokio::spawn(
                    async move {
                        async move {
                            sem_a.acquire().await.unwrap().forget();
                        }
                        .instrument(tracing::span!(Level::INFO, "acquire"))
                        .await;
                    }
                    .instrument(root_a),
                );

                let h_b = tokio::spawn(
                    async move {
                        async move {
                            sem_b.add_permits(1);
                        }
                        .instrument(tracing::span!(Level::INFO, "release"))
                        .await;
                    }
                    .instrument(root_b),
                );

                h_a.await.unwrap();
                h_b.await.unwrap();
            });
    });

    cache.flush_pending();
    driver.drain_sync();

    let all = cache.page(0, 20);
    assert_eq!(all.len(), 4, "task_a, acquire, task_b, release");
    assert!(all.iter().all(|s| s.closed_at.is_some()), "all spans must close");

    let find = |name: &str| all.iter().find(|s| s.metadata.name() == name).unwrap();
    let task_a = find("task_a");
    let task_b = find("task_b");
    let acquire = find("acquire");
    let release = find("release");

    assert_eq!(acquire.parent_id, Some(task_a.id), "acquire is child of task_a");
    assert_eq!(release.parent_id, Some(task_b.id), "release is child of task_b");

    assert!(
        acquire.opened_at < release.closed_at.unwrap(),
        "acquire started before release ended"
    );
    assert!(
        release.opened_at < acquire.closed_at.unwrap(),
        "release started before acquire closed"
    );
}
