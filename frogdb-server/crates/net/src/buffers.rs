//! The per-core network buffer pool.
//!
//! Connection read and write buffers grow to fit the largest frame a connection
//! has ever carried and then keep that capacity for the connection's life. Ten
//! thousand idle connections that each once carried a one-megabyte reply hold
//! ten gigabytes nobody can see or reclaim
//! ([PRD](../../../../../.scratch/memory-architecture/PRD.md) R11).
//!
//! This module is the answer: buffers are **leased** from a size-classed free
//! list and **returned** when the connection goes quiet, so a burst becomes a
//! transient allocation rather than a permanent high-water mark.
//!
//! # Why this lives in `frogdb-net`, and why it needs no lock
//!
//! A shard is an OS thread with its own current-thread runtime, and the
//! acceptor spawns a connection onto *its shard's* runtime (see
//! [`ShardExecutor::connection_runtime`](crate::ShardExecutor::connection_runtime)).
//! Every connection assigned to shard `n` therefore runs on shard `n`'s thread.
//! The pool is a `thread_local!`, which makes "per-core, no cross-core sharing"
//! true by construction rather than by convention: there is no handle to pass
//! to another core, so no lock and no atomic is needed, and a buffer can never
//! be leased on one core and returned on another.
//!
//! A build or a simulation with no shard runtimes still works — the connection
//! task then runs on whatever thread the general runtime gives it and uses that
//! thread's pool. The pool is a per-thread free list either way; pinning is what
//! makes "per-thread" mean "per-core".
//!
//! # Shrink when idle
//!
//! Two mechanisms, because one is not enough:
//!
//! * **Return above a watermark frees.** A class parks at most
//!   [`CLASS_HIGH_WATER`] buffers. A return past that is dropped, so a burst
//!   that leases hundreds of 1 MiB buffers does not leave hundreds parked.
//! * **A sweep trims toward a low-water target.** [`sweep`] takes each class
//!   down to [`CLASS_LOW_WATER`]. It is called from an existing per-connection
//!   tick rather than from a timer of its own, so an idle core's pools decay
//!   without the server holding a wakeup open to make them.
//!
//! # Refcount handoff
//!
//! [`PooledBuf::split_shared`] hands out a [`Bytes`] that shares the lease's
//! allocation. This is the seam the zero-copy parse path
//! ([issue 19](../../../../../.scratch/memory-architecture/issues/)) is built
//! on: a command's argument slices keep the read buffer's bytes alive for as
//! long as the command runs, and the buffer only becomes poolable again once
//! the last slice is gone. A lease dropped while slices are still outstanding is
//! *not* pooled — its allocation belongs to those slices now and is freed when
//! they drop. That is checked, not assumed: see [`PooledBuf::reclaim`].

use std::cell::RefCell;

use bytes::{Bytes, BytesMut};

/// The smallest pooled size class.
pub const MIN_CLASS_BYTES: usize = 1 << MIN_SHIFT;

/// The largest pooled size class. A lease above this is an unpooled one-off:
/// parking a multi-megabyte buffer to serve a request that may never come again
/// is the never-shrink bug with extra steps.
pub const MAX_CLASS_BYTES: usize = 1 << MAX_SHIFT;

const MIN_SHIFT: u32 = 12; // 4 KiB
const MAX_SHIFT: u32 = 20; // 1 MiB

/// How many power-of-two classes the pool keeps: 4 KiB, 8 KiB, … 1 MiB.
pub const CLASS_COUNT: usize = (MAX_SHIFT - MIN_SHIFT + 1) as usize;

/// How many buffers one class parks. A return past this is freed rather than
/// pooled — the per-class watermark that keeps a burst from becoming a
/// permanent reservation.
pub const CLASS_HIGH_WATER: usize = 8;

/// What [`sweep`] trims each class down to. One buffer per class is enough to
/// serve the next lease without an allocation; the rest is a burst's residue.
pub const CLASS_LOW_WATER: usize = 1;

/// Bytes in a class, by index.
pub const fn class_bytes(class: usize) -> usize {
    1usize << (MIN_SHIFT as usize + class)
}

/// The class that can serve `min_capacity`, or `None` when the request is
/// larger than [`MAX_CLASS_BYTES`] and must be an unpooled one-off.
fn class_for(min_capacity: usize) -> Option<usize> {
    if min_capacity > MAX_CLASS_BYTES {
        return None;
    }
    let want = min_capacity.max(MIN_CLASS_BYTES);
    // ceil(log2(want)) for a non-zero `want`.
    let shift = usize::BITS - (want - 1).leading_zeros();
    Some((shift.max(MIN_SHIFT) - MIN_SHIFT) as usize)
}

/// What the pool has done since the thread started, for tests and for the
/// operator-facing counters that read it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Leases served from a class's free list without allocating.
    pub hits: usize,
    /// Leases that had to allocate because the class was empty.
    pub misses: usize,
    /// Leases too large to pool, allocated and freed as one-offs.
    pub oneoffs: usize,
    /// Buffers returned to a free list.
    pub pooled_returns: usize,
    /// Buffers freed on return — above the class watermark, still shared with
    /// outstanding slices, or a one-off.
    pub freed_returns: usize,
    /// Bytes [`sweep`] has freed.
    pub swept_bytes: usize,
}

/// A size-classed free list of `BytesMut`, owned by one thread.
#[derive(Debug)]
pub struct BufferPool {
    classes: [Vec<BytesMut>; CLASS_COUNT],
    stats: PoolStats,
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool {
    /// An empty pool.
    pub fn new() -> Self {
        Self {
            classes: std::array::from_fn(|_| Vec::new()),
            stats: PoolStats::default(),
        }
    }

    /// What this pool has done since it was created.
    pub fn stats(&self) -> PoolStats {
        self.stats
    }

    /// How many buffers are parked in `class`.
    pub fn parked(&self, class: usize) -> usize {
        self.classes[class].len()
    }

    /// Bytes parked across every class — the pool's own footprint.
    pub fn parked_bytes(&self) -> usize {
        (0..CLASS_COUNT)
            .map(|class| self.classes[class].len() * class_bytes(class))
            .sum()
    }

    /// Take a buffer with at least `min_capacity` capacity, empty and ready to
    /// write into.
    fn take(&mut self, min_capacity: usize) -> (BytesMut, Option<usize>) {
        let Some(class) = class_for(min_capacity) else {
            self.stats.oneoffs += 1;
            return (BytesMut::with_capacity(min_capacity), None);
        };
        match self.classes[class].pop() {
            Some(buf) => {
                self.stats.hits += 1;
                (buf, Some(class))
            }
            None => {
                self.stats.misses += 1;
                (BytesMut::with_capacity(class_bytes(class)), Some(class))
            }
        }
    }

    /// Give a buffer back. It is parked only when its capacity is exactly a
    /// pooled class's size, it is not still shared with outstanding slices, and
    /// that class is below its watermark; otherwise it is freed here.
    ///
    /// The class comes from the buffer's *current* capacity, not from whatever
    /// it was leased as. A caller is free to grow a lease, and parking a grown
    /// buffer under its old class would put a megabyte in the 4 KiB list — where
    /// `parked_bytes` under-reports it and the next caller asking for one page
    /// is handed a megabyte it will hold for the rest of its life.
    fn give(&mut self, mut buf: BytesMut) {
        // Nothing to park or free: an `into_inner` lease, or a `mem::take`d slot.
        if buf.capacity() == 0 {
            return;
        }
        buf.clear();
        // Exactly a class, or not pooled: an odd capacity parked in the class
        // below it would make every figure the pool reports an estimate.
        let class = class_for(buf.capacity()).filter(|&c| class_bytes(c) == buf.capacity());
        let Some(class) = class else {
            self.stats.freed_returns += 1;
            return;
        };
        let want = class_bytes(class);
        // `try_reclaim` is the refcount check: it is false while a slice handed
        // out by `split_shared` is still alive, because the allocation is not
        // ours alone to hand to the next lease.
        if self.classes[class].len() >= CLASS_HIGH_WATER || !buf.try_reclaim(want) {
            self.stats.freed_returns += 1;
            return;
        }
        self.stats.pooled_returns += 1;
        self.classes[class].push(buf);
    }

    /// Trim every class toward [`CLASS_LOW_WATER`], returning the bytes freed.
    pub fn sweep(&mut self) -> usize {
        let mut freed = 0;
        for class in 0..CLASS_COUNT {
            let list = &mut self.classes[class];
            while list.len() > CLASS_LOW_WATER {
                list.pop();
                freed += class_bytes(class);
            }
        }
        self.stats.swept_bytes += freed;
        freed
    }
}

thread_local! {
    /// This thread's — and so, on a pinned shard thread, this core's — pool.
    static POOL: RefCell<BufferPool> = RefCell::new(BufferPool::new());
}

/// Run `f` against this core's pool.
///
/// Re-entrant use is a bug the pool refuses rather than panics on: a
/// [`PooledBuf`] dropped *inside* this closure cannot return itself, so it frees
/// instead. Callers here never hold a lease across the borrow.
pub fn with_pool<R>(f: impl FnOnce(&mut BufferPool) -> R) -> Option<R> {
    POOL.with(|pool| pool.try_borrow_mut().ok().map(|mut pool| f(&mut pool)))
}

/// Lease a buffer with at least `min_capacity` capacity from this core's pool.
///
/// The lease returns itself on drop.
pub fn lease(min_capacity: usize) -> PooledBuf {
    match with_pool(|pool| pool.take(min_capacity)) {
        Some((buf, class)) => PooledBuf { buf, class },
        // Only reachable during thread-local teardown, where there is no pool
        // left to lease from and a plain allocation is the honest answer.
        None => PooledBuf {
            buf: BytesMut::with_capacity(min_capacity),
            class: None,
        },
    }
}

/// Hand `buf`'s allocation back to this core's pool and replace it with one
/// sized for `min_capacity`.
///
/// This is how a buffer the caller does not own the storage slot of — notably
/// [`tokio_util::codec::Framed`]'s internal read and write buffers — gets
/// re-leased smaller after a burst. `buf` is cleared first: whatever it still
/// held is *discarded*, so callers must only recycle a buffer they have
/// established is drained.
///
/// [`tokio_util::codec::Framed`]: https://docs.rs/tokio-util/latest/tokio_util/codec/struct.Framed.html
pub fn recycle(buf: &mut BytesMut, min_capacity: usize) {
    with_pool(|pool| {
        let (fresh, _) = pool.take(min_capacity);
        let old = std::mem::replace(buf, fresh);
        pool.give(old);
    });
}

/// Hand `buf`'s allocation to this core's pool and leave the slot empty.
///
/// [`recycle`] for a buffer that has no next use: a closing connection's read
/// and write buffers, which would otherwise be freed at exactly the moment the
/// next accept on this core wants them. Nothing is leased in return, so the slot
/// is left with a zero-capacity `BytesMut` and any further write allocates.
///
/// `buf` is cleared first, so callers must only release a buffer whose contents
/// are dead.
pub fn release(buf: &mut BytesMut) {
    if buf.capacity() == 0 {
        return;
    }
    with_pool(|pool| pool.give(std::mem::take(buf)));
}

/// Trim this core's pools toward the low-water target, returning bytes freed.
///
/// Called from an existing per-core tick — deliberately not from a timer of its
/// own, which would keep an idle core awake to do nothing.
pub fn sweep() -> usize {
    with_pool(|pool| pool.sweep()).unwrap_or(0)
}

/// This core's pool statistics.
pub fn stats() -> PoolStats {
    with_pool(|pool| pool.stats()).unwrap_or_default()
}

/// A buffer leased from this core's pool, returned when dropped.
///
/// Derefs to the `BytesMut` it wraps, so it is used exactly like one.
#[derive(Debug)]
pub struct PooledBuf {
    buf: BytesMut,
    /// The class to return to, or `None` for an unpooled one-off.
    class: Option<usize>,
}

impl PooledBuf {
    /// The size class this lease returns to, in bytes — `None` for a one-off
    /// too large to pool.
    pub fn class_bytes(&self) -> Option<usize> {
        self.class.map(class_bytes)
    }

    /// Split the first `n` bytes off as a refcounted slice sharing this lease's
    /// allocation.
    ///
    /// The **refcount handoff** seam. The slice keeps those bytes valid
    /// independently of this lease, so a parsed command argument can outlive the
    /// read that produced it without a copy. The cost is stated where it is
    /// paid: while any slice is alive, the lease's allocation cannot go back to
    /// the pool, and a lease dropped in that state frees rather than parks.
    pub fn split_shared(&mut self, n: usize) -> Bytes {
        self.buf.split_to(n).freeze()
    }

    /// Reclaim the space handed out by [`split_shared`](Self::split_shared) once
    /// the slices are gone, returning whether the lease is whole again.
    ///
    /// `false` means slices are still outstanding — not an error, just the
    /// answer to "may this go back to the pool yet".
    pub fn reclaim(&mut self) -> bool {
        let want = self.class.map(class_bytes).unwrap_or(self.buf.capacity());
        self.buf.clear();
        self.buf.try_reclaim(want)
    }

    /// Take the buffer out of the lease, opting out of the pool. The allocation
    /// becomes the caller's to free.
    pub fn into_inner(mut self) -> BytesMut {
        // The empty slot left behind is what opts out: `give` parks by the
        // capacity it is handed, and a taken lease has none.
        std::mem::take(&mut self.buf)
    }
}

impl std::ops::Deref for PooledBuf {
    type Target = BytesMut;

    fn deref(&self) -> &BytesMut {
        &self.buf
    }
}

impl std::ops::DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.buf);
        // A failed borrow means we are being dropped inside `with_pool`; the
        // buffer is freed rather than parked, which is safe and rare.
        let _ = with_pool(|pool| pool.give(buf));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every test runs on its own thread so the thread-local pool is fresh —
    /// which is also a statement of the pool's contract: there is no way to
    /// reach another thread's pool, not even from a test.
    fn on_fresh_core<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
        std::thread::spawn(f).join().expect("pool test thread")
    }

    #[test]
    fn classes_round_up_to_the_next_power_of_two() {
        assert_eq!(class_for(1), Some(0));
        assert_eq!(class_for(MIN_CLASS_BYTES), Some(0));
        assert_eq!(class_for(MIN_CLASS_BYTES + 1), Some(1));
        assert_eq!(class_for(MAX_CLASS_BYTES), Some(CLASS_COUNT - 1));
        assert_eq!(
            class_for(MAX_CLASS_BYTES + 1),
            None,
            "past the largest class a lease is an unpooled one-off"
        );
        assert_eq!(class_bytes(0), MIN_CLASS_BYTES);
        assert_eq!(class_bytes(CLASS_COUNT - 1), MAX_CLASS_BYTES);
    }

    #[test]
    fn a_returned_buffer_serves_the_next_lease_without_allocating() {
        on_fresh_core(|| {
            let first = lease(8_000);
            assert_eq!(first.class_bytes(), Some(8 * 1024));
            drop(first);

            let second = lease(8_000);
            assert_eq!(second.capacity(), 8 * 1024);
            let stats = stats();
            assert_eq!(stats.misses, 1, "only the first lease allocates");
            assert_eq!(stats.hits, 1, "the second is served from the free list");
            assert_eq!(stats.pooled_returns, 1);
        });
    }

    #[test]
    fn an_oversized_lease_is_a_one_off_and_is_never_pooled() {
        on_fresh_core(|| {
            let big = lease(MAX_CLASS_BYTES * 4);
            assert_eq!(big.class_bytes(), None);
            assert!(big.capacity() >= MAX_CLASS_BYTES * 4);
            drop(big);

            with_pool(|pool| {
                assert_eq!(pool.parked_bytes(), 0, "a one-off must not be parked");
                assert_eq!(pool.stats().oneoffs, 1);
                assert_eq!(pool.stats().freed_returns, 1);
            });
        });
    }

    /// The acceptance criterion: a burst leases far more than a class parks, and
    /// after the sweep each class is back at the low-water target.
    #[test]
    fn a_burst_decays_to_the_low_water_target() {
        on_fresh_core(|| {
            const BURST: usize = 64;
            let held: Vec<PooledBuf> = (0..BURST).map(|_| lease(MAX_CLASS_BYTES)).collect();
            let class = CLASS_COUNT - 1;

            drop(held);
            with_pool(|pool| {
                assert_eq!(
                    pool.parked(class),
                    CLASS_HIGH_WATER,
                    "returns past the class watermark are freed, not parked"
                );
            });

            let freed = sweep();
            assert_eq!(
                freed,
                (CLASS_HIGH_WATER - CLASS_LOW_WATER) * MAX_CLASS_BYTES,
                "the sweep frees everything above the low-water target"
            );
            with_pool(|pool| {
                assert_eq!(pool.parked(class), CLASS_LOW_WATER);
                assert_eq!(pool.parked_bytes(), CLASS_LOW_WATER * MAX_CLASS_BYTES);
            });

            assert_eq!(sweep(), 0, "a swept pool has nothing left to trim");
        });
    }

    #[test]
    fn recycle_swaps_a_grown_buffer_for_a_small_lease() {
        on_fresh_core(|| {
            // A buffer that grew to a whole class during a burst.
            let mut grown = BytesMut::with_capacity(MAX_CLASS_BYTES);
            grown.extend_from_slice(&[7u8; 1024]);

            recycle(&mut grown, MIN_CLASS_BYTES);

            assert!(grown.is_empty());
            assert_eq!(grown.capacity(), MIN_CLASS_BYTES, "re-leased smaller");
            with_pool(|pool| {
                assert_eq!(
                    pool.parked(CLASS_COUNT - 1),
                    1,
                    "the grown allocation is parked for reuse, not dropped on the floor"
                );
            });
        });
    }

    /// A lease the caller grew goes back to the class it *became*, not the one
    /// it was taken from — otherwise the next caller asking for a page is handed
    /// the megabyte someone else grew, and holds it for its whole life.
    #[test]
    fn a_grown_lease_returns_to_the_class_it_grew_into() {
        on_fresh_core(|| {
            {
                let mut leased = lease(MIN_CLASS_BYTES);
                assert_eq!(leased.class_bytes(), Some(MIN_CLASS_BYTES));
                // Grow it a whole way up the ladder, as a big reply would.
                leased.reserve(MAX_CLASS_BYTES);
                assert_eq!(leased.capacity(), MAX_CLASS_BYTES);
            }

            with_pool(|pool| {
                assert_eq!(
                    pool.parked(0),
                    0,
                    "a megabyte must not be parked in the 4 KiB class"
                );
                assert_eq!(pool.parked(CLASS_COUNT - 1), 1);
                assert_eq!(
                    pool.parked_bytes(),
                    MAX_CLASS_BYTES,
                    "the pool's own footprint must be what it actually holds"
                );
            });

            let reused = lease(MIN_CLASS_BYTES);
            assert_eq!(
                reused.capacity(),
                MIN_CLASS_BYTES,
                "a small lease gets a small buffer, not the grown one"
            );
        });
    }

    /// A closing connection's buffers go to the pool, not to the allocator: the
    /// next accept on this core is the reuse this pool exists for.
    #[test]
    fn release_parks_a_dead_buffer_without_leasing_a_replacement() {
        on_fresh_core(|| {
            let mut buf = BytesMut::with_capacity(MIN_CLASS_BYTES);
            buf.extend_from_slice(b"a half-read command");

            release(&mut buf);

            assert_eq!(buf.capacity(), 0, "the slot is left empty, not re-leased");
            with_pool(|pool| {
                assert_eq!(pool.parked(0), 1);
                assert_eq!(
                    pool.stats().hits + pool.stats().misses,
                    0,
                    "releasing must not take a buffer back out of the pool"
                );
            });

            // Empty slot, nothing to do, no stats moved.
            release(&mut buf);
            with_pool(|pool| assert_eq!(pool.parked(0), 1));
        });
    }

    #[test]
    fn recycle_frees_an_off_class_buffer_instead_of_mislabelling_it() {
        on_fresh_core(|| {
            // 6 KiB is not a class size: parking it under the 8 KiB class would
            // hand the next 8 KiB lease a 6 KiB buffer.
            let mut odd = BytesMut::with_capacity(6 * 1024);
            odd.extend_from_slice(b"x");
            recycle(&mut odd, MIN_CLASS_BYTES);

            with_pool(|pool| {
                assert_eq!(pool.parked_bytes(), 0);
                assert_eq!(pool.stats().freed_returns, 1);
            });
        });
    }

    #[test]
    fn a_shared_slice_keeps_the_lease_out_of_the_pool_until_it_drops() {
        on_fresh_core(|| {
            let mut leased = lease(MIN_CLASS_BYTES);
            leased.extend_from_slice(b"hello world");
            let arg = leased.split_shared(5);
            assert_eq!(&arg[..], b"hello");

            assert!(
                !leased.reclaim(),
                "a lease with a live slice is not whole and must not be pooled"
            );
            drop(leased);
            with_pool(|pool| {
                assert_eq!(
                    pool.parked_bytes(),
                    0,
                    "the allocation belongs to the outstanding slice now"
                );
                assert_eq!(pool.stats().freed_returns, 1);
            });

            // The slice is still valid after its lease is gone — the point of
            // the handoff.
            assert_eq!(&arg[..], b"hello");
            drop(arg);
        });
    }

    #[test]
    fn a_lease_whose_slices_are_gone_is_poolable_again() {
        on_fresh_core(|| {
            let mut leased = lease(MIN_CLASS_BYTES);
            leased.extend_from_slice(b"hello world");
            let arg = leased.split_shared(5);
            drop(arg);

            assert!(
                leased.reclaim(),
                "the last slice dropped; the lease is whole"
            );
            drop(leased);
            with_pool(|pool| {
                assert_eq!(pool.parked(0), 1);
                assert_eq!(pool.stats().pooled_returns, 1);
            });
        });
    }

    #[test]
    fn into_inner_opts_out_of_the_pool() {
        on_fresh_core(|| {
            let leased = lease(MIN_CLASS_BYTES);
            let raw = leased.into_inner();
            assert_eq!(raw.capacity(), MIN_CLASS_BYTES);
            drop(raw);
            with_pool(|pool| {
                assert_eq!(pool.parked_bytes(), 0);
                assert_eq!(pool.stats().pooled_returns, 0);
            });
        });
    }

    #[test]
    fn pools_do_not_cross_threads() {
        on_fresh_core(|| {
            drop(lease(MIN_CLASS_BYTES));
            with_pool(|pool| assert_eq!(pool.parked(0), 1));
        });
        // A different thread — a different core — starts empty.
        on_fresh_core(|| {
            with_pool(|pool| {
                assert_eq!(pool.parked(0), 0);
                assert_eq!(pool.stats(), PoolStats::default());
            });
        });
    }
}
