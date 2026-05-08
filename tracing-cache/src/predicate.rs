//! Filtering predicate that decides which callsites the cache observes.
//!
//! `LevelPredicate` is the default and trivial implementation; downstream
//! consumers can plug in their own `EnabledPredicate` to filter by name,
//! target, dynamic state, etc.  The trait mirrors the four points the
//! `tracing::Subscriber` trait checks per callsite.

use tracing::metadata::LevelFilter;
use tracing::{Level, Metadata};

/// Mirror of `tracing::subscriber::Interest` — kept as our own type so the
/// predicate trait isn't bound to tracing's exact type.
pub enum Interest {
    Never,
    Sometimes,
    Always,
}

pub trait EnabledPredicate: Send + Sync + 'static {
    fn max_level_hint(&self) -> Option<LevelFilter>;
    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest;
    fn enabled(&self, metadata: &Metadata<'_>) -> bool;
    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool;
}

/// Default predicate: enables everything at or below `level`.
pub struct LevelPredicate {
    level: Level,
}

impl LevelPredicate {
    pub fn new(level: Level) -> Self {
        Self { level }
    }
}

impl EnabledPredicate for LevelPredicate {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::from_level(self.level))
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.level() <= &self.level {
            Interest::Always
        } else {
            Interest::Never
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= &self.level
    }

    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
        span.metadata().level() <= &self.level
    }
}
