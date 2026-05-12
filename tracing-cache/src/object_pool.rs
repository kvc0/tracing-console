//! Per-type buffer pool with try-only locking semantics.
//!
//! [`ObjectPool`] holds N shards (`Vec<Arc<Pool<T>>>`); the hot path
//! picks a shard via the same per-thread key the rest of the cache
//! uses, then attempts to pop a pre-reset `Box<T>` from that shard's
//! `Mutex<Vec<Box<T>>>`.
//!
//! The pool starts **empty**.  It grows under load:
//!   * acquire: `try_lock` the shard.  Success → pop the next ready
//!     box; or, if the shard is empty, allocate `Box::new(T::default())`.
//!     `try_lock` fail (contended) → still allocate a fresh box.
//!   * Every `ReuseRef` is attached to its source shard regardless of
//!     whether the acquire try-lock succeeded.  On drop the ref tries
//!     to hand its box back; that's how the pool fills up.
//!
//! On `ReuseRef::drop`:
//!   * `try_lock` the source shard.  Success + room (`len < capacity`)
//!     → `reset()` and push back.  Full or contended → drop the box.
//!
//! No thread ever blocks on the pool's lock.

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
            // Don't pre-allocate.  The pool grows from empty as
            // ReuseRefs drop and hand boxes back.
            items: Mutex::new(Vec::new()),
            capacity,
        }
    }

    /// Try to take a ready boxed item.  Always returns a `Box<T>` —
    /// pops from the shard if its `try_lock` succeeds and an item is
    /// available, otherwise allocates `Box::new(T::default())`.  In
    /// either case the resulting `ReuseRef` is attached and will
    /// attempt to hand the box back on drop, which is how the pool
    /// grows.
    fn try_take(self: &Arc<Self>) -> Box<T> {
        if let Ok(mut guard) = self.items.try_lock() {
            if let Some(b) = guard.pop() {
                return b;
            }
        }
        Box::new(T::default())
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
        let shards = (0..n)
            .map(|_| Arc::new(Pool::new(per_shard_capacity)))
            .collect();
        Arc::new(Self {
            shards,
            shard_mask: (n as u64) - 1,
        })
    }

    /// Acquire a `ReuseRef<T>` from this thread's shard.  Always
    /// succeeds; if the shard is empty or its `try_lock` is contended,
    /// falls back to `Box::new(T::default())`.  Either way the ref is
    /// attached to the shard — on drop it will try to hand the box
    /// back, which is how the pool grows.
    pub fn acquire(&self) -> ReuseRef<T> {
        let key = ensure_thread_shard_key();
        let shard = &self.shards[(key & self.shard_mask) as usize];
        let boxed = shard.try_take();
        ReuseRef {
            value: Some(boxed),
            pool: Arc::clone(shard),
        }
    }
}

/// A pool-backed owned reference.  `Deref` / `DerefMut` give access to
/// the inner `T`; on drop the value is reset and handed back to the
/// source shard if room is available and `try_lock` succeeds —
/// otherwise dropped.
///
/// `Clone` allocates a fresh box backed by the same shard.  External
/// clones (e.g. `cache.get_span()`) pay one heap allocation; the clone
/// hands its box back into the same pool on drop, just like the
/// original.
pub struct ReuseRef<T: Resettable + Default + Send + 'static> {
    value: Option<Box<T>>,
    pool: Arc<Pool<T>>,
}

impl<T: Resettable + Default + Send + 'static> std::ops::Deref for ReuseRef<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: `value` is only ever taken out in `drop`; while the
        // ReuseRef is alive `value` is `Some`.
        self.value
            .as_deref()
            .expect("ReuseRef value taken before drop")
    }
}

impl<T: Resettable + Default + Send + 'static> std::ops::DerefMut for ReuseRef<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.value
            .as_deref_mut()
            .expect("ReuseRef value taken before drop")
    }
}

impl<T: Resettable + Default + Send + 'static + Clone> Clone for ReuseRef<T> {
    fn clone(&self) -> Self {
        // Allocate a fresh box for the clone; share the source shard.
        // On drop the clone tries to hand its box back the same way as
        // any other acquire — that's how clones round-trip through the
        // pool just like fresh acquires under contention.
        ReuseRef {
            value: Some(Box::new((**self).clone())),
            pool: Arc::clone(&self.pool),
        }
    }
}

impl<T: Resettable + Default + Send + 'static> Drop for ReuseRef<T> {
    fn drop(&mut self) {
        let Some(mut boxed) = self.value.take() else {
            return;
        };
        boxed.reset();
        self.pool.try_return(boxed);
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
    fn pool_starts_empty_and_grows() {
        // Fresh pool: items vec is empty until something gets returned.
        let pool = ObjectPool::<Buf>::new(1, 4);
        let shard = &pool.shards[0];
        assert_eq!(shard.items.lock().unwrap().len(), 0);

        {
            let mut r = pool.acquire();
            r.bytes.extend_from_slice(b"x");
        }
        // After one acquire/drop the shard has exactly one entry.
        assert_eq!(shard.items.lock().unwrap().len(), 1);
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
        // Box was returned — no Buf dropped.
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
    fn contended_acquire_still_returns_on_drop() {
        // Hold the shard's lock; an acquire while contended should
        // still produce a working ReuseRef (allocated on the fly),
        // and dropping it (after the lock is released) should add it
        // to the shard, growing the pool.
        let pool = ObjectPool::<Buf>::new(1, 4);
        let shard = Arc::clone(&pool.shards[0]);
        let r;
        {
            let _guard = shard.items.lock().unwrap();
            r = pool.acquire();
            // Lock guard drops here; r is still alive.
        }
        // Drop r now that lock is free — should add to pool.
        drop(r);
        assert_eq!(shard.items.lock().unwrap().len(), 1);
    }
}
