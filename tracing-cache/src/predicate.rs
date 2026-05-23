//! Filtering predicate that decides which callsites the cache observes.
//!
//! `LevelPredicate` is the default and trivial implementation; downstream
//! consumers can plug in their own `EnabledPredicate` to filter by name,
//! target, dynamic state, etc.  The trait mirrors the four points the
//! `tracing::Subscriber` trait checks per callsite.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

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

/// Default predicate: enables everything at or below the current
/// `LevelFilter` setting (including `OFF` for "disable everything").
///
/// The level is dynamic — call [`LevelPredicate::handle`] to grab a
/// cheap-to-clone [`LevelHandle`] that can change it from another
/// thread/task at runtime without rebuilding the subscriber.  Every
/// `callsite_enabled` returns `Sometimes` so tracing-core re-asks
/// `enabled` on each event/span, picking up live changes immediately;
/// `LevelHandle::set` additionally calls
/// `tracing::callsite::rebuild_interest_cache` so any callsites that
/// were registered before the level changed get re-evaluated against
/// the new `max_level_hint`.
pub struct LevelPredicate {
    level: Arc<AtomicU8>,
}

impl LevelPredicate {
    /// Construct from a `tracing::Level` (legacy callers).  See
    /// [`with_filter`](Self::with_filter) for the `LevelFilter` form
    /// that can also express `OFF`.
    pub fn new(level: Level) -> Self {
        Self::with_filter(LevelFilter::from_level(level))
    }

    /// Construct from a `LevelFilter` (the only way to start `OFF`).
    pub fn with_filter(filter: LevelFilter) -> Self {
        Self {
            level: Arc::new(AtomicU8::new(filter_to_u8(filter))),
        }
    }

    /// A cheap-to-clone handle that sets/gets the current level from
    /// other threads/tasks — typically held by an admin RPC.
    pub fn handle(&self) -> LevelHandle {
        LevelHandle {
            level: Arc::clone(&self.level),
        }
    }
}

/// Remote control for a [`LevelPredicate`]'s active level.  Cloning
/// shares the same atomic — multiple owners (e.g. one per
/// administrative connection) all see and mutate the same value.
#[derive(Clone)]
pub struct LevelHandle {
    level: Arc<AtomicU8>,
}

impl LevelHandle {
    pub fn set(&self, filter: LevelFilter) {
        self.level.store(filter_to_u8(filter), Ordering::Release);
        // Invalidate tracing-core's per-callsite Interest cache so
        // callsites that were registered before the level changed get
        // re-evaluated against the new max_level_hint.  Without this,
        // raising the level (e.g. OFF → INFO) would leave already-
        // -registered Never-cached callsites disabled forever.
        tracing::callsite::rebuild_interest_cache();
    }

    pub fn get(&self) -> LevelFilter {
        u8_to_filter(self.level.load(Ordering::Acquire))
    }
}

impl EnabledPredicate for LevelPredicate {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(u8_to_filter(self.level.load(Ordering::Acquire)))
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        // Return Always / Never so tracing-core caches the decision
        // per callsite — disabled callsites get short-circuited at
        // the macro level with no further work.  `LevelHandle::set`
        // calls `rebuild_interest_cache` to invalidate this cache
        // whenever the level changes, so callsites get re-evaluated
        // against the new max_level_hint.
        let filter = u8_to_filter(self.level.load(Ordering::Acquire));
        if filter == LevelFilter::OFF {
            return Interest::Never;
        }
        if LevelFilter::from_level(*metadata.level()) <= filter {
            Interest::Always
        } else {
            Interest::Never
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        // Reached only when `callsite_enabled` returned `Sometimes`,
        // which we never do — but tracing's contract still requires
        // a sane answer for any path that calls it directly.
        let filter = u8_to_filter(self.level.load(Ordering::Relaxed));
        if filter == LevelFilter::OFF {
            return false;
        }
        LevelFilter::from_level(*metadata.level()) <= filter
    }

    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
        self.enabled(span.metadata())
    }
}

fn filter_to_u8(f: LevelFilter) -> u8 {
    if f == LevelFilter::OFF {
        0
    } else if f == LevelFilter::ERROR {
        1
    } else if f == LevelFilter::WARN {
        2
    } else if f == LevelFilter::INFO {
        3
    } else if f == LevelFilter::DEBUG {
        4
    } else {
        5 // TRACE
    }
}

fn u8_to_filter(n: u8) -> LevelFilter {
    match n {
        0 => LevelFilter::OFF,
        1 => LevelFilter::ERROR,
        2 => LevelFilter::WARN,
        3 => LevelFilter::INFO,
        4 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    }
}

// ── Chance-gated root sampling ───────────────────────────────────────────────

/// Predicate wrapper that probabilistically denies root spans before
/// they get to the inner predicate.  Root spans (the ones whose
/// `Attributes` carry [`tracing::span::Attributes::is_root()`] —
/// typically `tracing::span!(parent: None, …)`) are gated by a
/// runtime-tunable percentage; descendants and events pass straight
/// through to the inner predicate.
///
/// Because the chance is read with a `Relaxed` load of an
/// `AtomicU64` holding `f64::to_bits` of the percentage, updates
/// from a [`ChanceHandle`] are picked up by the next root-span
/// roll without needing a wake — the inner subscriber's existing
/// `rebuild_interest_cache` invalidation isn't required either,
/// since the dice are rerolled per span instance via
/// [`EnabledPredicate::new_span_enabled`], not at
/// callsite-registration time.
pub struct ChancePredicate<P: EnabledPredicate> {
    /// Bit-packed `f64` percentage in `[0.0, 100.0]`.
    chance_pct_bits: Arc<AtomicU64>,
    inner: P,
}

impl<P: EnabledPredicate> ChancePredicate<P> {
    /// Construct with an initial chance percentage `[0.0, 100.0]`.
    /// Out-of-range inputs are silently clamped.  Use `100.0` for
    /// "always pass to inner".
    pub fn new(inner: P, chance_pct: f64) -> Self {
        let pct = clamp_pct(chance_pct);
        Self {
            chance_pct_bits: Arc::new(AtomicU64::new(pct.to_bits())),
            inner,
        }
    }

    /// Cheap-to-clone handle for changing the chance percentage at
    /// runtime — typically held by an admin RPC.
    pub fn handle(&self) -> ChanceHandle {
        ChanceHandle {
            bits: Arc::clone(&self.chance_pct_bits),
        }
    }
}

/// Remote control for a [`ChancePredicate`]'s active percentage.
/// Cloning shares the same atomic — multiple owners observe and
/// mutate the same value.  Reads are `Relaxed` since per-span
/// freshness is not required; updates are visible to the next roll.
#[derive(Clone)]
pub struct ChanceHandle {
    bits: Arc<AtomicU64>,
}

impl ChanceHandle {
    pub fn set(&self, pct: f64) {
        let pct = clamp_pct(pct);
        self.bits.store(pct.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

fn clamp_pct(pct: f64) -> f64 {
    if pct.is_nan() {
        0.0
    } else {
        pct.clamp(0.0, 100.0)
    }
}

impl<P: EnabledPredicate> EnabledPredicate for ChancePredicate<P> {
    fn max_level_hint(&self) -> Option<LevelFilter> {
        // Chance doesn't constrain level — defer to inner.
        self.inner.max_level_hint()
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        // The dice roll happens per span instance in
        // `new_span_enabled`, not per callsite — let the inner
        // predicate decide whether the callsite is enabled.
        self.inner.callsite_enabled(metadata)
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        // Events are not gated by chance — only root spans are.
        self.inner.enabled(metadata)
    }

    fn new_span_enabled(&self, span: &tracing::span::Attributes<'_>) -> bool {
        if span.is_root() {
            let pct = f64::from_bits(self.chance_pct_bits.load(Ordering::Relaxed));
            if pct <= 0.0 {
                return false;
            }
            if pct < 100.0 {
                // Roll a fresh u64 / 2^64 fraction and scale to [0, 100).
                // Per-thread fast PRNG — cheap and doesn't touch the
                // OS RNG on the hot path.
                let roll = rand::random::<u64>() as f64 / (u64::MAX as f64 + 1.0) * 100.0;
                if roll >= pct {
                    return false;
                }
            }
        }
        self.inner.new_span_enabled(span)
    }
}
