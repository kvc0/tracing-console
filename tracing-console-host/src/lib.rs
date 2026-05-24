//! protosocket-rpc host that streams closed spans from a `tracing-cache::SpanCache`
//! to console clients over messagepack.
//!
//! Usage:
//! ```ignore
//! let (cache, driver) = tracing_cache::SpanCache::new(16384);
//! let cache = std::sync::Arc::new(cache);
//! tokio::spawn(driver.run());
//! tracing_console_host::serve(cache, "127.0.0.1:7777".parse()?).await?;
//! ```

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::panic_in_result_fn,)
)]

mod protocol;
mod server;
mod wire;

pub use protocol::{
    Request, RequestBody, Response, ResponseBody, WireEvent, WireFieldValue, WireLevel,
    WireLevelFilter, WireServerInfo, WireSpan,
};
pub use server::{ServeError, serve};
