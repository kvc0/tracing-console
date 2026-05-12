//! In-memory `tracing::Subscriber` that holds spans and events for
//! inspection by a console UI.
//!
//! Open spans live in a sharded slab; closed spans flow through a
//! lock-free spillway channel into a background [`Driver`] that
//! commits them to a shared `BTreeMap`.  Filtering is pluggable via
//! [`EnabledPredicate`]; the default [`LevelPredicate`] enables
//! everything at or below a given level.

mod cache;
mod config;
mod driver;
mod id_encoding;
mod predicate;
mod record;
mod thread_state;

#[cfg(test)]
mod tests;

pub use cache::SpanCache;
pub use config::{CacheConfig, DEFAULT_LANE_COUNT};
pub use driver::Driver;
pub use predicate::{EnabledPredicate, Interest, LevelPredicate};
pub use record::{EventRecord, FieldList, FieldValue, SpanRecord};
