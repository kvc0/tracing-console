//! Throughput test host: emits api_request span trees at a configurable
//! rate and serves the resulting cache to console clients.
//!
//! Three API variants share the `api_request` root name so the console
//! sees them all aggregate into one row whose `api` field distinguishes
//! them.  Each variant has its own inner work span and child shape, so
//! expanding the row in the tree view reveals different leaves:
//!
//!   api_request [api=fetch_user, user_id=N]
//!     validate
//!     fetch_user
//!       db_query
//!       cache_lookup
//!     serialize_response
//!       json_encode
//!     audit_log
//!
//!   api_request [api=update_user, user_id=N]
//!     validate
//!     update_user
//!       db_query
//!       db_write
//!     serialize_response
//!       json_encode
//!     audit_log
//!
//!   api_request [api=post_message, channel=X]
//!     validate
//!     post_message
//!       db_write
//!       publish
//!     serialize_response
//!       json_encode
//!     audit_log
//!
//! Frequencies per `per_tick` unit of work:
//!   * fetch_user:   1 (always)
//!   * update_user:  1/10 of fetch_user
//!   * post_message: 1/5 of fetch_user
//!
//! Each call also emits one `completed` event with `status=ok` (or
//! `status=error` every 20th call) so the detail pane has events to
//! summarise.
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
use tracing::metadata::LevelFilter;
use tracing_cache::{ChancePredicate, LevelPredicate, SpanCache};
use tracing_console_host::serve;

/// Channel labels rotated through by `post_message`.
const CHANNELS: &[&str] = &["general", "engineering", "ops"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (hz, per_tick, addr) = parse_args();

    // Default OFF + chance=100% so the host doesn't record anything
    // until a console connects and raises the level via Shift+I /D /T,
    // and root sampling is full when it does.
    let level = LevelPredicate::with_filter(LevelFilter::OFF);
    let level_handle = level.handle();
    let predicate = ChancePredicate::new(level, 100.0);
    let chance_handle = predicate.handle();
    let (cache, driver) = SpanCache::with_predicate(16384, predicate);
    let cache = Arc::new(cache);
    tracing::subscriber::set_global_default(Arc::clone(&cache))?;
    tokio::spawn(driver.run());

    let serve_cache = Arc::clone(&cache);
    let serve_level = level_handle.clone();
    let serve_chance = chance_handle.clone();
    tokio::spawn(async move {
        if let Err(e) = serve(serve_cache, serve_level, serve_chance, addr).await {
            eprintln!("serve: {e}");
        }
    });

    let period = Duration::from_secs_f64(1.0 / hz);
    // Average spans per tick: per_tick × (fetch + update + post) × 8.
    // update_user fires every 10 fetch_user, post_message every 5.
    let spans_per_tick = (per_tick as f64) * (1.0 + 0.1 + 0.2) * 8.0;
    eprintln!(
        "synth_load: per tick = {per_tick} fetch_user + {per_tick}/10 update_user + {per_tick}/5 post_message \
         (≈ {avg:.1} spans/tick, ~{rate:.0} spans/s); RPC at {addr}",
        avg = spans_per_tick,
        rate = spans_per_tick * hz,
    );

    let mut tick = interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut counter: u64 = 0;
    loop {
        tick.tick().await;
        for _ in 0..per_tick {
            counter = counter.wrapping_add(1);
            emit_fetch_user(counter);
            if counter.is_multiple_of(10) {
                emit_update_user(counter);
            }
            if counter.is_multiple_of(5) {
                emit_post_message(counter);
            }
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

fn emit_fetch_user(counter: u64) {
    let user_id = counter % 10;
    let root = tracing::span!(
        parent: None,
        Level::INFO,
        "api_request",
        api = "fetch_user",
        user_id = user_id,
    );
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
    emit_completed(counter);
}

fn emit_update_user(counter: u64) {
    let user_id = counter % 10;
    let root = tracing::span!(
        parent: None,
        Level::INFO,
        "api_request",
        api = "update_user",
        user_id = user_id,
    );
    let _r = root.enter();
    {
        let s = tracing::span!(Level::INFO, "validate");
        let _v = s.enter();
    }
    {
        let s = tracing::span!(Level::INFO, "update_user");
        let _u = s.enter();
        {
            let q = tracing::span!(Level::INFO, "db_query");
            let _q = q.enter();
        }
        {
            let w = tracing::span!(Level::INFO, "db_write");
            let _w = w.enter();
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
    emit_completed(counter);
}

fn emit_post_message(counter: u64) {
    let channel = CHANNELS[(counter as usize) % CHANNELS.len()];
    let root = tracing::span!(
        parent: None,
        Level::INFO,
        "api_request",
        api = "post_message",
        channel = channel,
    );
    let _r = root.enter();
    {
        let s = tracing::span!(Level::INFO, "validate");
        let _v = s.enter();
    }
    {
        let s = tracing::span!(Level::INFO, "post_message");
        let _p = s.enter();
        {
            let w = tracing::span!(Level::INFO, "db_write");
            let _w = w.enter();
        }
        {
            let pb = tracing::span!(Level::INFO, "publish");
            let _pb = pb.enter();
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
    emit_completed(counter);
}

/// One `completed` event per request — `status=error` every 20th call,
/// `ok` otherwise, so the detail pane's event summary shows a
/// non-trivial bucket distribution.
fn emit_completed(counter: u64) {
    let status = if counter.is_multiple_of(20) {
        "error"
    } else {
        "ok"
    };
    tracing::event!(Level::INFO, status = status, "completed");
}
