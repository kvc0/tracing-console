//! Synthetic "API" host that lists a directory once per second under a tree
//! of tracing spans, then serves the resulting span cache to console clients.
//!
//! Span shape per tick:
//!   list_directory(path=…, root=true)               ← root span
//!     file_size(path=…)                             ← per-file leaf span
//!     list_directory(path=…, root=false)            ← per-subdir child span
//!       file_size(path=…)
//!       list_directory(...)                         ← deeper recursion
//!
//! Run:
//!   cargo run -p tracing-console-host --example fs_listing_api -- <dir> [addr]
//! Then in another terminal:
//!   cargo run -p tracing-console -- [addr]                   # interactive TUI
//!   cargo run -p tracing-console -- --states [addr]          # JSON dump

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::Level;
use tracing_cache::SpanCache;
use tracing_console_host::serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: fs_listing_api <directory> [bind-addr]");
            std::process::exit(2);
        }
    };
    let addr: std::net::SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:7777".to_string())
        .parse()?;

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

    eprintln!(
        "walking {} every second; serving console RPC at {addr}",
        dir.display()
    );

    let mut tick = interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        walk_root(&dir);
    }
}

/// Top-level span for one walk pass.  Explicit `parent: None` so it's a root.
fn walk_root(path: &Path) {
    let span = tracing::span!(
        parent: None,
        Level::INFO,
        "list_directory",
        path = %path.display(),
        root = true,
    );
    let _g = span.enter();
    walk_body(path);
}

/// Contextual child span — the parent (the previous list_directory) is on the
/// SPAN_STACK at call time, so this becomes its child.
fn walk_recursive(path: &Path) {
    let span = tracing::span!(
        Level::INFO,
        "list_directory",
        path = %path.display(),
        root = false,
    );
    let _g = span.enter();
    walk_body(path);
}

fn walk_body(path: &Path) {
    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        Err(e) => {
            tracing::event!(Level::WARN, error = %e, "read_dir failed");
            return;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let p = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            walk_recursive(&p);
        } else if file_type.is_file() {
            fetch_size(&p);
        }
    }
}

fn fetch_size(path: &Path) {
    let span = tracing::span!(
        Level::INFO,
        "file_size",
        path = %path.display(),
    );
    let _g = span.enter();
    match std::fs::metadata(path) {
        Ok(m) => tracing::event!(Level::INFO, bytes = m.len(), "size"),
        Err(e) => tracing::event!(Level::WARN, error = %e, "metadata failed"),
    }
}
