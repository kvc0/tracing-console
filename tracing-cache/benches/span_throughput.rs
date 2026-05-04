/// Throughput benchmarks for [`SpanCache`] as a `tracing::Subscriber`.
///
/// Three scenarios in ascending concurrency:
///
/// 1. **single_thread** — one thread, plain synchronous span creation.  Establishes
///    the baseline cost of `new_span + enter + exit + try_close`.
///
/// 2. **four_threads** — four OS threads each running the same workload.
///    Uses `iter_custom` so thread-spawn latency is paid before the clock starts.
///
/// 3. **four_async_tasks** — four Tokio tasks on a four-worker-thread runtime,
///    each workload wrapped with `.instrument()`.  Shows the extra overhead that
///    `tracing_futures::Instrumented` adds on top of the synchronous path.
use std::hint::black_box;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use tracing::Level;
use tracing_cache::SpanCache;
use tracing_futures::Instrument;

// ── Shared subscriber ────────────────────────────────────────────────────────

// A single SpanCache set as the global subscriber so all threads (including
// Tokio worker threads) automatically route through it.  The capacity is large
// enough that eviction is a minor fraction of each iteration's cost.
//
// The Driver runs on a dedicated background thread so the benchmark threads
// never block on a map write lock.
static CACHE: LazyLock<Arc<SpanCache>> = LazyLock::new(|| {
    let (cache, driver) = SpanCache::new(4096);
    let cache = Arc::new(cache);
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(driver.run())
    });
    cache
});

static SUBSCRIBER_INIT: OnceLock<()> = OnceLock::new();

fn init_subscriber() {
    SUBSCRIBER_INIT.get_or_init(|| {
        tracing::subscriber::set_global_default(Arc::clone(&*CACHE))
            .expect("global tracing subscriber already set");
    });
}

// ── Workload ─────────────────────────────────────────────────────────────────

/// Synchronous 2-level span hierarchy: a root span containing one child span.
/// Exercises `new_span`, `enter`, `exit`, and `try_close` for both levels.
#[inline]
fn two_level_spans() {
    let root = tracing::span!(parent: None, Level::INFO, "bench_root");
    let _root = root.enter();
    let child = tracing::span!(Level::INFO, "bench_child");
    let _child = child.enter();
    black_box(());
}

/// Async 2-level hierarchy for use with `.instrument()`.
///
/// The caller must wrap this future with `.instrument(root_span)`.  The child
/// span is created here — inside the future body — so when it first executes
/// the root span is already on the thread-local stack and is recorded as its
/// parent.
async fn two_level_async_spans() {
    black_box(async { black_box(()) }
        .instrument(tracing::span!(Level::INFO, "bench_child"))
        .await);
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_single_thread(c: &mut Criterion) {
    init_subscriber();
    c.bench_function("single_thread/two_level_spans", |b| {
        b.iter(|| two_level_spans());
    });
}

fn bench_four_threads(c: &mut Criterion) {
    use std::sync::Barrier;

    init_subscriber();
    c.bench_function("four_threads/two_level_spans", |b| {
        // iter_custom lets us pay the thread-spawn cost before the clock starts.
        b.iter_custom(|iters| {
            // 4 workers + 1 timing thread all rendezvous at the barrier before
            // any work begins.  The timing thread is the last to arrive, so when
            // barrier.wait() returns on the timing thread all workers are already
            // running — thread-spawn latency is excluded from the measurement.
            let barrier = Arc::new(Barrier::new(5));

            let handles: Vec<_> = (0..4usize)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..iters {
                            black_box(two_level_spans());
                        }
                    })
                })
                .collect();

            barrier.wait(); // release all workers; start measuring immediately after
            let start = Instant::now();
            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        });
    });
}

fn bench_four_async_tasks(c: &mut Criterion) {
    init_subscriber();

    // Build the runtime once; reuse it across all criterion iterations.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();

    c.bench_function("four_async_tasks/instrumented_two_level_spans", |b| {
        // to_async drives the iter_custom future with rt.block_on().
        // Inside that future we use tokio::spawn so the four tasks truly run
        // concurrently on the runtime's worker threads.
        b.to_async(&rt).iter_custom(|iters| async move {
            // Spawn once; each task runs the full iters loop so that task-spawn
            // overhead is amortised the same way thread-spawn is in bench_four_threads.
            let start = Instant::now();
            let handles: Vec<_> = (0..4usize)
                .map(|_| {
                    tokio::spawn(async move {
                        for _ in 0..iters {
                            // The root span is created here, on the worker thread,
                            // and entered by .instrument() before two_level_async_spans
                            // is polled — so the child span sees it on the stack.
                            black_box(
                                two_level_async_spans()
                                    .instrument(tracing::span!(
                                        parent: None,
                                        Level::INFO,
                                        "bench_root"
                                    ))
                                    .await,
                            );
                        }
                    })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
            start.elapsed()
        });
    });
}

criterion_group!(
    span_throughput,
    bench_single_thread,
    bench_four_threads,
    bench_four_async_tasks
);
criterion_main!(span_throughput);
