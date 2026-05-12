//! Per-type buffer pool with try-only locking semantics.
//!
//! [`ObjectPool`] holds N shards (`Vec<Arc<Pool<T>>>`); the hot path picks
//! a shard via the same per-thread key the rest of the cache uses, then
//! attempts to pop a pre-reset `T` from that shard's `Mutex<Vec<T>>`.  If
//! the `try_lock` fails (another thread is currently mid-acquire or
//! mid-return on that shard), we don't wait — we allocate a fresh
//! `T::default()` and mark the resulting [`ReuseRef`] as *unattached*.
//!
//! On `ReuseRef::drop`:
//!   * If the ref is unattached, drop the boxed value.
//!   * Otherwise try-lock the source shard.  On success, if there's
//!     room (`len < capacity`), `reset()` the value and push it back.
//!     If the shard is full or `try_lock` fails, drop the value.
//!
//! Every step is bounded: no thread ever blocks on the pool's lock.

use std::sync::{Arc, Mutex};

use crate::thread_state::ensure_thread_shard_key;

/// State that can be reset to a pool-ready empty form.
pub trait Resettable {
    fn reset(&mut self);
}

/// One shard of the pool.  Holds up to `capacity` ready-to-reuse boxed
/// items.  Storing `Box<T>` (not `T`) means the heap allocation itself
/// is recycled — `acquire` is `Vec::pop` returning the prior Box, and
/// `return` is `Vec::push` putting it back; no `Box::new` on the hot
/// path after warmup.
pub struct Pool<T: Resettable + Default + Send + 'static> {
    items: Mutex<Vec<Box<T>>>,
    capacity: usize,
}

impl<T: Resettable + Default + Send + 'static> Pool<T> {
    fn new(capacity: usize) -> Self {
        Self {
            items: Mutex::new(Vec::with_capacity(capacity.min(64))),
            capacity,
        }
    }

    /// Try to take a ready boxed item.  Returns `(boxed, attached)` —
    /// `attached` is `true` if we successfully interacted with this
    /// shard's lock (so the resulting `ReuseRef` should try to return
    /// on drop).  If `try_lock` failed we allocate a fresh
    /// `Box<T::default()>` and mark the ref as unattached so its drop
    /// doesn't bounce off this shard.
    fn try_take(self: &Arc<Self>) -> (Box<T>, bool) {
        match self.items.try_lock() {
            Ok(mut guard) => match guard.pop() {
                Some(b) => (b, true),
                None => (Box::new(T::default()), true),
            },
            Err(_) => (Box::new(T::default()), false),
        }
    }

    /// Push a (reset) boxed value back into the shard, dropping it if
    /// there's no room or if the lock is contended.  Drop is what frees
    /// the Box's heap allocation.
    fn try_return(&self, value: Box<T>) {
        match self.items.try_lock() {
            Ok(mut guard) => {
                if guard.len() < self.capacity {
                    guard.push(value);
                }
                // else: full — Box drops here, freeing the allocation
            }
            Err(_) => {
                // Contended — Box drops here, freeing the allocation
            }
        }
    }
}

/// A sharded buffer pool for `T`.  Construction allocates the shards;
/// items are created lazily via `T::default()` when no ready item is
/// available.  Sharding keeps `try_lock`-failure rates low under
/// concurrent acquire / release.
pub struct ObjectPool<T: Resettable + Default + Send + 'static> {
    shards: Vec<Arc<Pool<T>>>,
    shard_mask: u64,
}

impl<T: Resettable + Default + Send + 'static> ObjectPool<T> {
    /// Build a pool with `shard_count` shards (rounded to a power of two,
    /// minimum 1), each holding up to `per_shard_capacity` ready items.
    pub fn new(shard_count: usize, per_shard_capacity: usize) -> Arc<Self> {
        let n = shard_count.max(1).next_power_of_two();
        let shards = (0..n).map(|_| Arc::new(Pool::new(per_shard_capacity))).collect();
        Arc::new(Self { shards, shard_mask: (n as u64) - 1 })
    }

    /// Acquire a `ReuseRef<T>` from this thread's shard.  Always
    /// succeeds — falls back to `Box::new(T::default())` if the shard
    /// is contended (in which case the resulting ref is unattached and
    /// will drop directly rather than try the pool again).
    pub fn acquire(&self) -> ReuseRef<T> {
        let key = ensure_thread_shard_key();
        let shard = &self.shards[(key & self.shard_mask) as usize];
        let (boxed, attached) = shard.try_take();
        ReuseRef {
            value: Some(boxed),
            pool: if attached { Some(Arc::clone(shard)) } else { None },
        }
    }
}

/// A pool-backed owned reference.  `Deref` / `DerefMut` give access to
/// the inner `T`; on drop the value is reset and returned to the source
/// shard if possible, otherwise dropped.
///
/// `Clone` allocates a fresh standalone copy (not pool-attached) so that
/// external clones via `cache.get_span()` etc. don't muddy the pool's
/// ownership.  Hot-path insertion and pipeline transit pay no clone.
pub struct ReuseRef<T: Resettable + Default + Send + 'static> {
    value: Option<Box<T>>,
    /// `Some` only if the acquire's `try_lock` succeeded — we'll try to
    /// return to this shard on drop.  `None` means "unattached", drop
    /// the value directly.
    pool: Option<Arc<Pool<T>>>,
}

impl<T: Resettable + Default + Send + 'static> ReuseRef<T> {
    /// Standalone (not pool-attached) wrapper around a default-constructed
    /// `T`.  Useful for tests and for cloning paths that don't have a pool
    /// handle in scope.
    pub fn standalone() -> Self {
        Self { value: Some(Box::new(T::default())), pool: None }
    }
}

impl<T: Resettable + Default + Send + 'static> std::ops::Deref for ReuseRef<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: `value` is only ever taken out in `drop`; while the
        // ReuseRef is alive `value` is `Some`.
        self.value.as_deref().expect("ReuseRef value taken before drop")
    }
}

impl<T: Resettable + Default + Send + 'static> std::ops::DerefMut for ReuseRef<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_deref_mut().expect("ReuseRef value taken before drop")
    }
}

impl<T: Resettable + Default + Send + 'static + Clone> Clone for ReuseRef<T> {
    fn clone(&self) -> Self {
        // External clones don't take from the pool — they hand out a
        // standalone Box that just gets dropped at end of life.  This
        // preserves the invariant that each pool-attached value is
        // owned by exactly one ReuseRef.
        ReuseRef {
            value: Some(Box::new((**self).clone())),
            pool: None,
        }
    }
}

impl<T: Resettable + Default + Send + 'static> Drop for ReuseRef<T> {
    fn drop(&mut self) {
        let Some(mut boxed) = self.value.take() else { return };
        let Some(pool) = self.pool.take() else { return };
        boxed.reset();
        pool.try_return(boxed);
    }
}

impl<T: Resettable + Default + Send + 'static + std::fmt::Debug> std::fmt::Debug for ReuseRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `Buf` that increments a shared atomic on drop.  Each test
    /// builds its own counter so parallel tests don't interfere.
    #[derive(Debug, Default, Clone)]
    struct Buf {
        bytes: Vec<u8>,
        drops: Option<Arc<AtomicUsize>>,
    }

    impl Resettable for Buf {
        fn reset(&mut self) {
            self.bytes.clear();
        }
    }

    impl Drop for Buf {
        fn drop(&mut self) {
            if let Some(d) = &self.drops {
                d.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn install_counter(r: &mut ReuseRef<Buf>, counter: &Arc<AtomicUsize>) {
        r.drops = Some(Arc::clone(counter));
    }

    #[test]
    fn acquire_then_drop_returns_to_shard() {
        let pool = ObjectPool::<Buf>::new(1, 4);
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let mut r = pool.acquire();
            install_counter(&mut r, &counter);
            r.bytes.extend_from_slice(b"hello");
            assert_eq!(r.bytes, b"hello");
        }
        // First drop returned to the shard — no Buf dropped.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        // Re-acquiring should hand back the same allocation (cleared).
        let r = pool.acquire();
        assert_eq!(r.bytes, b"");
    }

    #[test]
    fn full_shard_drops_overflow() {
        let pool = ObjectPool::<Buf>::new(1, 2);
        let counter = Arc::new(AtomicUsize::new(0));
        // Acquire 4 — shard cap is 2, so two should drop on return.
        let mut refs: Vec<_> = (0..4).map(|_| pool.acquire()).collect();
        for r in &mut refs {
            install_counter(r, &counter);
        }
        drop(refs);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn unattached_drops_directly() {
        // Build a Pool, grab its lock with `lock()`, then try_take while
        // the lock is held — should return an unattached value.
        let pool = Arc::new(Pool::<Buf>::new(4));
        let _guard = pool.items.lock().unwrap();
        let (value, attached) = pool.try_take();
        assert!(!attached);
        drop(value);
    }

    #[test]
    fn standalone_does_not_touch_pool() {
        let r: ReuseRef<Buf> = ReuseRef::standalone();
        drop(r);
    }
}
