//! Throughput test host: emits a fixed-shape span tree at a configurable
//! rate and serves the resulting cache to console clients.
//!
//! Each tick produces a deterministic 8-span tree:
//!
//!   api_request                ← root
//!     validate
//!     fetch_user
//!       db_query
//!       cache_lookup
//!     serialize_response
//!       json_encode
//!     audit_log
//!
//! Per-tick total: 8 spans + 8 events.  Total span throughput = 8 × Hz.
//!
//! Run:
//!   cargo run -p tracing-console-host --example synth_load --release -- --hz 10
//! Then, in another terminal:
//!   cargo run -p tracing-console --release -- --stats 1
//!
//! Args:
//!   --hz <N>      tick frequency in Hz (default 10; accepts fractional)
//!   --addr <A>    bind address (default 127.0.0.1:7777)

use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::Level;
use tracing_cache::SpanCache;
use tracing_console_host::serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (hz, per_tick, addr) = parse_args();

    let (cache, driver) = SpanCache::new(16384);
    let cache = Arc::new(cache);
    tracing::subscriber::set_global_default(Arc::clone(&cache))?;
    tokio::spawn(driver.run());

    let serve_cache = Arc::clone(&cache);
    tokio::spawn(async move {
        if let Err(e) = serve(serve_cache, addr).await {
            eprintln!("serve: {e}");
        }
    });

    let period = Duration::from_secs_f64(1.0 / hz);
    eprintln!(
        "synth_load: emitting {per_tick} × 8-span tree every {:.3?} ({:.1} Hz, ~{:.0} spans/s); RPC at {addr}",
        period,
        hz,
        8.0 * hz * per_tick as f64,
    );

    let mut tick = interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for _ in 0..per_tick {
            emit_tree();
        }
    }
}

fn parse_args() -> (f64, usize, std::net::SocketAddr) {
    let mut hz = 10.0_f64;
    let mut per_tick: usize = 1;
    let mut addr_str = "127.0.0.1:7777".to_string();
    let mut args = std::env::args().skip(1).peekable();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--hz" => {
                hz = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .filter(|v: &f64| v.is_finite() && *v > 0.0)
                    .unwrap_or_else(|| {
                        eprintln!("--hz expects a positive number");
                        std::process::exit(2);
                    });
            }
            "--per-tick" => {
                per_tick = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .filter(|v: &usize| *v > 0)
                    .unwrap_or_else(|| {
                        eprintln!("--per-tick expects a positive integer");
                        std::process::exit(2);
                    });
            }
            "--addr" => {
                addr_str = args.next().unwrap_or_else(|| {
                    eprintln!("--addr expects an address");
                    std::process::exit(2);
                });
            }
            other => {
                eprintln!("ignoring unknown arg: {other}");
            }
        }
    }
    let addr = addr_str
        .parse()
        .unwrap_or_else(|e| panic!("invalid address {addr_str:?}: {e}"));
    (hz, per_tick, addr)
}

/// One span tree per tick.  No fields, no events — pure span-shape so the
/// throughput test isolates cache + RPC + client cost from the
/// `FieldVisitor`-side allocator pressure.
fn emit_tree() {
    let root = tracing::span!(parent: None, Level::INFO, "api_request");
    let _r = root.enter();
    {
        let s = tracing::span!(Level::INFO, "validate");
        let _v = s.enter();
    }
    {
        let s = tracing::span!(Level::INFO, "fetch_user");
        let _f = s.enter();
        {
            let q = tracing::span!(Level::INFO, "db_query");
            let _q = q.enter();
        }
        {
            let c = tracing::span!(Level::INFO, "cache_lookup");
            let _c = c.enter();
        }
    }
    {
        let s = tracing::span!(Level::INFO, "serialize_response");
        let _s = s.enter();
        let j = tracing::span!(Level::INFO, "json_encode");
        let _j = j.enter();
    }
    {
        let s = tracing::span!(Level::INFO, "audit_log");
        let _a = s.enter();
    }
}
