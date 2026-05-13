//! protosocket-rpc host that streams closed spans from a `tracing-cache::SpanCache`
//! to console clients over messagepack.
//!
//! Usage sketch:
//! ```ignore
//! let (cache, driver) = tracing_cache::SpanCache::new(16384);
//! let cache = std::sync::Arc::new(cache);
//! tokio::spawn(driver.run());
//! tracing_console_host::serve(cache, "127.0.0.1:7777".parse()?).await?;
//! ```

pub mod protocol;
pub mod server;
pub mod wire;

pub use protocol::{
    Request, RequestBody, Response, ResponseBody, WireEvent, WireFieldValue, WireLevel,
    WireLevelFilter, WireSpan,
};
pub use server::serve;
