# tracing-console

An interactive TUI for inspecting `tracing` spans coming off a live server, plus the cache and RPC layer that gets them to you.

The `tracing` tie-in, `tracing-cache`, is made to have the lowest possible overhead while disabled. It's expected that you will
have nothing connected to your tracing port almost all the time. In the rare case you need it, you can connect, enable briefly,
(or for longer with a non-100% sampling rate) and then disable. In this way you can capture detailed traces during events
without needing a bunch of infrastructure.

The downside is that if you wanted to see something that already happened, you can't. This is for looking at problems that are
happening!

## What this is for

This is suited for **short-lived API traces** — request-response work, RPC handlers, background jobs that complete in
milliseconds to seconds. The cache only commits spans to its shared map when they close, and the protocol only streams *closed*
spans. Anything still in flight is invisible to the console.

That makes it a poor fit for:

- **Long-running background spans** (event loops, daemons, things that stay open for hours) — they never reach the cache.
- **Low-value logging** that has no span structure — the console can stream events, but the UI is span-oriented.
- **Distributed observability** — This is a single-host debugging console, not a distributed tracing backend.

What it is good at: attaching to a running server, watching the live shape of recent requests, finding outliers, drilling into
one trace's full event/span tree, and disabling again without leaving any trace behind.

## Getting started — integrating with your server

### 1. Add the dependencies

```toml
[dependencies]
tokio = "1"
tracing = "0"
tracing-cache = "0"
tracing-console-host = "0"
```

### 2. Stand up the cache + serve

```rust
use std::sync::Arc;
use tracing_cache::{ChancePredicate, LevelPredicate, SpanCache};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Predicate stack: LevelPredicate gates by min level; ChancePredicate
    // wraps it with a 0-100 % sampling rate.  Both are dynamic — the
    // console can flip them at runtime via the level switcher and the
    // `C` chance modal. You should probably always start this OFF.
    let level = LevelPredicate::with_filter(tracing::metadata::LevelFilter::OFF);
    let level_handle = level.handle();
    let predicate = ChancePredicate::new(level, 100.0);
    let chance_handle = predicate.handle();

    // 16k closed-span ring buffer. Sized for your worst-case burst rate
    // × the time you'd want to look back at it. If you overflow while
    // recording you'll just drop spans. Oh well.
    let (cache, driver) = SpanCache::with_predicate(16_384, predicate);
    let cache = Arc::new(cache);

    // Install as the global subscriber.  Spans your code emits anywhere
    // in the process from now on land in this cache when they close.
    tracing::subscriber::set_global_default(Arc::clone(&cache))?;
    tokio::spawn(driver.run());

    // Serve the cache to console clients.  Pick a port reachable from
    // wherever you'll be running the TUI.
    let addr: std::net::SocketAddr = "127.0.0.1:7890".parse()?;
    tokio::spawn(async move {
        if let Err(e) = tracing_console_host::serve(cache, level_handle, chance_handle, addr).await {
            eprintln!("tracing-console-host serve: {e}");
        }
    });

    // ... your server's normal entry point ...
    your_server::run().await
}
```

### 3. Instrument with regular `tracing` macros

You put your root spans carefully at the base of your APIs and the units of work you want to trace.
For **async** code, use `#[tracing::instrument]` or `future.instrument(span).await` instead of `let _g = span.enter()`.
An `enter()` guard sits on the thread until it goes out of scope, including across every `.await` in between; when the
executor parks the task and runs something else on the same worker thread, that unrelated work ends up captured as a
child of your span. That's really confusing.

```rust
use tracing::{event, Instrument, Level};

// Outer handler — root of one trace per request.  `parent = None`
// explicitly anchors it so it can't inherit whatever happened to
// be on the caller's stack.  `skip(req)` keeps the request body
// out of the span fields; we pull the bits we want via `fields`.
#[tracing::instrument(
    parent = None,
    skip(req),
    fields(method = %req.method, path = %req.path),
)]
async fn handle_request(req: Request) -> Response {
    event!(Level::INFO, "received");
    let body = parse_body(&req).await;       // each its own #[instrument]
    let result = run_query(&body).await;
    event!(Level::INFO, rows = result.rows, "done");
    build_response(result)
}

#[tracing::instrument(skip(body))]
async fn parse_body(body: &Body) -> Parsed { /* … */ }
```

If you'd rather build the span by hand, use `.instrument`:

```rust
let span = tracing::info_span!("request", method = %req.method);
do_work(&req).instrument(span).await
```

`let _g = span.enter()` is fine in synchronous code where no
`.await` happens inside the guard's scope.

### 4. Connect from the TUI

```sh
cargo run --release -p tracing-console -- 127.0.0.1:7777
```

You'll land in the stacks view with nothing visible (level defaults to `Off`). Press `Shift+I` (or `Shift+D`, `Shift+T`) to ask the server to start recording at Info / Debug / Trace. Spans will start showing up as they close.

### Keyboard cheat sheet
The UI highlights available options contextually. Here are some of the main controls:

| Where | Key | Action |
|---|---|---|
| any | `Shift+O/I/D/T` | request cache level Off/Info/Debug/Trace |
| any | `Shift+C` | open chance % modal (sampling rate) |
| stack / graph / explore | `s` `g` `e` | jump between top-level views |
| stack | `↑/↓`, `→`, `←`, `Enter` | navigate, expand, collapse, expand-all |
| graph | `a` `w` `l` `m` `u` | edit agg / window / lookback, toggle metric / time labels |
| explore | `↑/↓`, `←/→`, `i` | row cursor, cycle sort column, invert direction |
| explore | `/` | search across span/event names + field values |
| explore | `Enter` | open the trace-detail view of the selected row |
| trace detail | `↑/↓`, `←/→` | cursor, collapse / expand subtree |
| any | `Esc` | pop one level up |
| any | `q` | quit |

## Configuration

- **Cache capacity** (`SpanCache::with_predicate(capacity, …)`) — the max number of closed spans in the ring buffer. Older spans evict FIFO. Too small and your client will get gaps. Too large and you'll just be wasting memory for no reason while you're tracing.
- **`CacheConfig::pending_batch`** (default `8`) — per-thread closed-span buffer before flushing. Lower = more responsive at low traffic, more sends at high traffic.
- **`CacheConfig::channel_capacity`** (default `65_536`) — buffer between producer threads and the driver.
- **Sampling rate** (`ChancePredicate`) — live-tunable via `Shift+C` in the TUI, or programmatically via `chance_handle`.
- **Cache level** (`LevelPredicate`) — live-tunable via `Shift+letter` in the TUI, or programmatically via `level_handle`. The server starts at whatever you initialized; starting up with `Off` is recommended.

## Examples

- `tracing-console-host/examples/fs_listing_api.rs` — a self-contained host that traces a directory walk.
- `tracing-console-host/examples/synth_load.rs` — a synthetic-load generator used for bench testing.

```sh
# terminal 1 — start the example "server"
cargo run -p tracing-console-host --example fs_listing_api -- /tmp

# terminal 2 — connect the TUI
cargo run -p tracing-console -- 127.0.0.1:7777
```

## Limitations
Spans only become visible when they close. In-flight long-running spans don't show up until they end. When you enable tracing,
you are doing it to a live system in who-knows what state. You'll get some orphaned spans at the beginning, and some incomplete
spans at the end.

This tool is intended for quick live analysis, not rigorous archival. There is no persistence. All caching is done in bounded ring buffers
in-memory, and lost on restart.
