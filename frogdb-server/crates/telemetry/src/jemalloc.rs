//! `jemalloc` allocator introspection: `mallctl` stats reads and arena purge.
//!
//! FrogDB links `tikv-jemallocator` as the global allocator everywhere
//! except `msvc` targets (see the `#[global_allocator]` gate at the top of
//! `frogdb-server/crates/server/src/main.rs`). This module reads that same
//! allocator's `mallctl` interface behind the identical
//! `cfg(not(target_env = "msvc"))` gate, and is the one place `INFO memory`,
//! `MEMORY PURGE`, and the `frogdb_allocator_*` Prometheus gauges reach into
//! jemalloc — no other module calls `tikv_jemalloc_ctl`/`tikv_jemalloc_sys`
//! directly.
//!
//! Redis derives its allocator fields the same way: `allocator_frag_ratio =
//! allocator_active / allocator_allocated`, `allocator_rss_ratio =
//! allocator_resident / allocator_active`, and the top-level
//! `mem_fragmentation_ratio = used_memory_rss / used_memory`. FrogDB matches
//! that derivation (see `frogdb-server/crates/server/src/info/sections.rs`);
//! the one deviation is that `used_memory_rss` there is the OS-reported
//! process RSS (`sysinfo`, already wired via `SystemMetricsCollector`)
//! rather than jemalloc's own `stats.resident`, matching Redis's
//! `zmalloc_get_rss()` (an OS read, not an allocator read).
//!
//! # Per-shard arenas
//!
//! On top of the process-wide reads this module owns the per-shard arena
//! mechanism ruled in
//! [`adr/0006`](../../../../adr/0006-memory-architecture-seams.md) §3:
//! [`create_arena`] (`arenas.create`) and [`bind_current_thread_to_arena`]
//! (`thread.arena`) are what a shard thread calls as its first act, and
//! [`read_arena_stats`] is what the sampler in [`crate::shard_arenas`] reads
//! back. `frogdb-net` reaches them through its own `ShardArenaSource` trait
//! rather than by calling `tikv_jemalloc_*` itself, so this module stays the
//! only caller.
//!
//! Two properties of those reads are load-bearing enough to be in the type
//! system rather than in prose, and both come from the phase-1 spike
//! (`.scratch/memory-architecture/spike-report.md` §(a), validated in
//! `spike-report-linux.md`):
//!
//! * **Arena `allocated` is an upper bound on live bytes.** Objects freed into
//!   the owning thread's cache stay charged to their arena until an explicit
//!   `thread.tcache.flush` (E4 row 1: a non-zero residue bounded by the
//!   tcache's per-bin capacity — 25,600 B on macOS, 8,192 B on aarch64 Linux —
//!   and exactly 0 after a flush; only those two statements are portable).
//!   [`ArenaStats`] names every such field `*_upper_bound` so no later caller
//!   can read it as live bytes. The overstatement direction is the safe one
//!   for a refusal.
//! * **Stats are sampled, not live.** They refresh only on an `epoch` advance,
//!   which merges *every* arena at a per-arena cost that is a platform constant
//!   (E5: ~3.4 µs/arena on macOS, ~13.4 µs on aarch64 Linux — budget from a
//!   measured sample, never a hardcoded figure). So [`advance_epoch`] is
//!   separated from [`read_arena_stats`]: one advance per sampler tick, then N
//!   cheap by-name reads (nanoseconds each, noise next to the epoch). Nothing
//!   on a command path advances the epoch.
//!
//! On the pre-resolved-MIB fast path the spike left open: `mallctlnametomib`
//! does resolve for `stats.arenas.<i>.small.allocated` (see
//! `arena_stat_names_resolve_to_mibs`), but it is deliberately not used. A MIB
//! read saves ~100 ns against a by-name read that already runs at most
//! `3 × arenas` times per sampler tick — under a microsecond per tick at eight
//! shards, against the ~22 µs the tick's single epoch advance costs. Buying that
//! with a cached `[usize; 5]` whose third component has to be rewritten per
//! arena, in unsafe code, is not a trade worth making; the finding is recorded
//! here instead.

#[cfg(not(target_env = "msvc"))]
mod imp {
    use tikv_jemalloc_ctl::{epoch, raw, stats};

    /// A snapshot of jemalloc's `stats.*` mallctls, taken after advancing
    /// the epoch (these are cached and only refreshed on an epoch bump —
    /// see [`tikv_jemalloc_ctl::epoch`]).
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct AllocatorStats {
        /// `stats.allocated`: bytes allocated by the application.
        pub allocated: u64,
        /// `stats.active`: bytes in active pages allocated by the
        /// application (a multiple of the page size, >= `allocated`).
        pub active: u64,
        /// `stats.resident`: bytes physically resident (allocator
        /// metadata + active pages + unused dirty pages).
        pub resident: u64,
        /// `stats.mapped`: bytes in active extents mapped by the
        /// allocator.
        pub mapped: u64,
        /// `stats.retained`: bytes retained rather than returned to the
        /// OS via `munmap(2)`.
        pub retained: u64,
    }

    /// Advance the epoch and read the current allocator stats.
    ///
    /// `None` if a `mallctl` call fails — should not happen once jemalloc
    /// is linked, but INFO/metrics reads must never panic on an allocator
    /// hiccup, so this degrades to "no allocator stats" rather than
    /// propagating the error.
    pub fn read_stats() -> Option<AllocatorStats> {
        epoch::advance().ok()?;
        Some(AllocatorStats {
            allocated: stats::allocated::read().ok()? as u64,
            active: stats::active::read().ok()? as u64,
            resident: stats::resident::read().ok()? as u64,
            mapped: stats::mapped::read().ok()? as u64,
            retained: stats::retained::read().ok()? as u64,
        })
    }

    /// `mem_allocator` INFO field: `"jemalloc-<version>"`, matching Redis's
    /// `<name>-<version>` convention (e.g. `jemalloc-5.3.0-0-...`).
    pub fn allocator_name() -> String {
        match tikv_jemalloc_ctl::version::read() {
            Ok(version) => format!("jemalloc-{version}"),
            Err(_) => "jemalloc".to_string(),
        }
    }

    /// A snapshot of one arena's `stats.arenas.<i>.*` mallctls, as of the last
    /// [`advance_epoch`].
    ///
    /// Every byte figure here is an **upper bound** on the arena's live bytes,
    /// not a live-byte count: jemalloc charges a region to its arena from
    /// allocation until it leaves the owning thread's cache, so objects that
    /// have already been freed still count until that thread flushes. The spike
    /// measured 25,600 B still charged to an arena after every object allocated
    /// on it had been freed, dropping to 0 only on an explicit
    /// `thread.tcache.flush` (spike-report §(a) E4 rows 1 and 3). The field
    /// names say so; a caller that wants live bytes cannot get them from here.
    ///
    /// The page-derived fields (`active`, `dirty`, `muzzy`) are the per-arena
    /// depth PRD R13 asks for in place of an active defragmenter: an operator
    /// seeing a shard's resident bytes climb needs to know whether the growth is
    /// live data (`allocated` rising) or reclamation lag (`dirty`/`muzzy` rising
    /// while `allocated` holds), because only the first is a reason to add
    /// capacity. jemalloc reports those three as *page counts*
    /// (`pactive`/`pdirty`/`pmuzzy`), so they are multiplied by
    /// [`page_size`] here — a caller never sees a page count it might mistake
    /// for bytes.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ArenaStats {
        /// The arena these figures describe.
        pub arena: u32,
        /// `stats.arenas.<i>.small.allocated`, an upper bound (see the type
        /// docs).
        pub small_allocated_upper_bound: u64,
        /// `stats.arenas.<i>.large.allocated`, an upper bound (see the type
        /// docs).
        pub large_allocated_upper_bound: u64,
        /// `stats.arenas.<i>.resident`: bytes physically resident for this
        /// arena — active pages plus unused dirty pages plus its share of
        /// allocator metadata. Exact (nothing is parked in a thread cache at
        /// page granularity), and the numerator of the per-shard fragmentation
        /// ratio PRD R13 wants.
        pub resident: u64,
        /// `stats.arenas.<i>.pactive` × [`page_size`]: bytes in pages the arena
        /// is actively serving allocations from. The gap to
        /// [`allocated_upper_bound`](ArenaStats::allocated_upper_bound) is
        /// internal (within-page) fragmentation.
        pub active: u64,
        /// `stats.arenas.<i>.pdirty` × [`page_size`]: bytes in pages the arena
        /// has freed but not yet returned to the OS. These decay away on
        /// `dirty_decay_ms` — see [`ArenaDecay`] — so a large figure here means
        /// reclamation lag, not growth.
        pub dirty: u64,
        /// `stats.arenas.<i>.pmuzzy` × [`page_size`]: bytes in pages already
        /// madvised away but still mapped, awaiting the `muzzy_decay_ms` step
        /// that unmaps them. Zero under FrogDB's `muzzy_decay_ms:0` default,
        /// which skips the muzzy stage entirely.
        pub muzzy: u64,
        /// `stats.arenas.<i>.retained`: bytes of address space the arena keeps
        /// mapped rather than returning via `munmap(2)`. Costs address space,
        /// not physical memory, so it is deliberately outside `resident`.
        pub retained: u64,
    }

    /// One arena's page-reclamation timings (`arena.<i>.{dirty,muzzy}_decay_ms`).
    ///
    /// jemalloc frees pages in two steps: a freed page first becomes *dirty*
    /// (still mapped and still resident), then after `dirty_decay_ms` is
    /// madvised into the *muzzy* state, and after `muzzy_decay_ms` is unmapped.
    /// Both are `ssize_t` milliseconds where `0` means "purge immediately" and
    /// `-1` means "never decay".
    ///
    /// PRD R13's ruling is that FrogDB gets no active defragmenter, so these two
    /// numbers *are* FrogDB's reclamation policy — which is why they are set
    /// explicitly (`frogdb_server::malloc_conf`) rather than inherited from
    /// jemalloc's defaults, and why they are readable and writable per arena at
    /// runtime instead of only at startup.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ArenaDecay {
        /// Milliseconds a freed page stays dirty before being madvised away.
        pub dirty_ms: i64,
        /// Milliseconds a muzzy page stays mapped before being unmapped.
        pub muzzy_ms: i64,
    }

    impl ArenaStats {
        /// Small + large: an upper bound on the arena's live application bytes.
        pub fn allocated_upper_bound(&self) -> u64 {
            self.small_allocated_upper_bound
                .saturating_add(self.large_allocated_upper_bound)
        }

        /// `resident / allocated`, the per-shard fragmentation ratio (the spike
        /// measured 1.15–1.23 across four shard arenas). `None` when the arena
        /// has no allocated bytes to divide by.
        pub fn fragmentation_ratio(&self) -> Option<f64> {
            let allocated = self.allocated_upper_bound();
            (allocated > 0).then(|| self.resident as f64 / allocated as f64)
        }
    }

    /// Create a fresh arena and return its index (`arenas.create`).
    ///
    /// Callable from any thread — creating an arena does not bind it. The shard
    /// executor creates and binds on the shard's own thread anyway, so the
    /// arena's own creation cost lands on the shard that will own it.
    pub fn create_arena() -> std::io::Result<u32> {
        // SAFETY: `arenas.create` reads back an `unsigned` (the new arena
        // index) and takes no input value, which is exactly what a `read::<u32>`
        // issues.
        unsafe { raw::read::<u32>(b"arenas.create\0") }.map_err(std::io::Error::other)
    }

    /// Bind the **calling** thread to `arena` (`thread.arena`).
    ///
    /// Must be the calling thread's first act, before it allocates anything:
    /// `thread.arena` does not flush the thread cache, so a rebind leaves
    /// already-cached regions charged to the old arena — the spike measured
    /// bleed bounded by the tcache's per-bin capacity (1.00 % of subsequent
    /// bytes on macOS, 0.32 % on aarch64 Linux; spike-report §(a) E4 row 2 and
    /// spike-report-linux.md). There is deliberately no rebind helper here. If one is
    /// ever needed (shard migration, teardown) it is this call **plus**
    /// [`flush_current_thread_cache`], or the no-cross-arena-bleed invariant is
    /// quietly false by a percent.
    pub fn bind_current_thread_to_arena(arena: u32) -> std::io::Result<()> {
        // SAFETY: `thread.arena` is a writable `unsigned` mallctl and `arena` is
        // the `u32` it expects.
        unsafe { raw::write::<u32>(b"thread.arena\0", arena) }.map_err(std::io::Error::other)
    }

    /// The arena the calling thread is currently bound to (`thread.arena`), or
    /// `None` if the read fails.
    pub fn current_thread_arena() -> Option<u32> {
        // SAFETY: `thread.arena` reads back the `unsigned` it is written with.
        unsafe { raw::read::<u32>(b"thread.arena\0") }.ok()
    }

    /// Flush the calling thread's allocation cache (`thread.tcache.flush`),
    /// returning every cached region to its owning arena.
    ///
    /// This is what turns an arena's `allocated` upper bound into its exact live
    /// figure, and it is the second half of any future rebind (see
    /// [`bind_current_thread_to_arena`]). It is not on any periodic path: the
    /// thread cache is the allocator's fast path and dropping it costs 2.3–2.9×
    /// on small allocations (spike-report §(a) E3).
    pub fn flush_current_thread_cache() -> std::io::Result<()> {
        void_mallctl("thread.tcache.flush")
    }

    /// Advance jemalloc's stats epoch, refreshing every `stats.*` mallctl.
    ///
    /// Costs about 2.5 µs **per live arena** because the advance merges them all
    /// (spike-report §(a) E5), which is why the process runs with `narenas:1`
    /// and creates exactly one arena per shard. Sampled at 10–100 Hz by
    /// [`crate::shard_arenas::ArenaSampler`]; never called from a command path.
    pub fn advance_epoch() -> bool {
        epoch::advance().is_ok()
    }

    /// Read one arena's figures as of the last [`advance_epoch`].
    ///
    /// Does **not** advance the epoch itself: a caller reading N arenas advances
    /// once and then reads N times, rather than paying the all-arena merge per
    /// arena.
    pub fn read_arena_stats(arena: u32) -> Option<ArenaStats> {
        let page = page_size()?;
        Some(ArenaStats {
            arena,
            small_allocated_upper_bound: arena_stat(arena, "small.allocated")?,
            large_allocated_upper_bound: arena_stat(arena, "large.allocated")?,
            resident: arena_stat(arena, "resident")?,
            active: arena_stat(arena, "pactive")?.saturating_mul(page),
            dirty: arena_stat(arena, "pdirty")?.saturating_mul(page),
            muzzy: arena_stat(arena, "pmuzzy")?.saturating_mul(page),
            retained: arena_stat(arena, "retained")?,
        })
    }

    /// `arenas.page`: the page size jemalloc was built for, in bytes.
    ///
    /// The three `p*` arena stats are page counts, so this is what turns them
    /// into the bytes callers actually want. It is a build-time constant of the
    /// allocator (4 KiB on x86-64 Linux, 16 KiB on aarch64 macOS), so it is read
    /// once and cached rather than re-read on every sampler tick.
    pub fn page_size() -> Option<u64> {
        static PAGE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
        // SAFETY: `arenas.page` is a read-only `size_t` mallctl.
        *PAGE.get_or_init(|| {
            unsafe { raw::read::<usize>(b"arenas.page\0") }
                .ok()
                .map(|v| v as u64)
        })
    }

    /// One `stats.arenas.<i>.<leaf>` read. The leaves used here are all
    /// `size_t`.
    fn arena_stat(arena: u32, leaf: &str) -> Option<u64> {
        let name = format!("stats.arenas.{arena}.{leaf}\0");
        // SAFETY: every leaf passed by this module (`small.allocated`,
        // `large.allocated`, `resident`, `pactive`, `pdirty`, `pmuzzy`,
        // `retained`) is a `size_t`-typed read-only mallctl, and the name is
        // null-terminated by construction above.
        unsafe { raw::read::<usize>(name.as_bytes()) }
            .ok()
            .map(|v| v as u64)
    }

    /// Read one arena's `arena.<i>.{dirty,muzzy}_decay_ms`.
    ///
    /// Unlike the `stats.*` reads these are current the instant they are made —
    /// they are settings, not statistics, so they need no [`advance_epoch`].
    pub fn read_arena_decay(arena: u32) -> Option<ArenaDecay> {
        Some(ArenaDecay {
            dirty_ms: arena_decay_ms(arena, "dirty")?,
            muzzy_ms: arena_decay_ms(arena, "muzzy")?,
        })
    }

    /// One `arena.<i>.<which>_decay_ms` read.
    fn arena_decay_ms(arena: u32, which: &str) -> Option<i64> {
        let name = format!("arena.{arena}.{which}_decay_ms\0");
        // SAFETY: both decay mallctls are `ssize_t`-typed, and the name is
        // null-terminated by construction above.
        unsafe { raw::read::<isize>(name.as_bytes()) }
            .ok()
            .map(|v| v as i64)
    }

    /// Set one arena's decay timings at runtime.
    ///
    /// Writing a decay setting also purges whatever the new, shorter deadline
    /// has already made overdue, so lowering `dirty_ms` returns pages
    /// immediately rather than at the next natural decay tick.
    ///
    /// One real arena at a time: jemalloc's [`ALL_ARENAS`] sentinel is *not*
    /// accepted by the `decay_ms` mallctls (its handler looks the index up as a
    /// real arena and answers `EFAULT`), unlike `purge`. A caller retuning a
    /// whole node loops over the arenas it owns — see `DEBUG ARENA-DECAY`.
    pub fn set_arena_decay(arena: u32, decay: ArenaDecay) -> std::io::Result<()> {
        write_arena_decay_ms(arena, "dirty", decay.dirty_ms)?;
        write_arena_decay_ms(arena, "muzzy", decay.muzzy_ms)
    }

    /// One `arena.<i>.<which>_decay_ms` write.
    fn write_arena_decay_ms(arena: u32, which: &str, ms: i64) -> std::io::Result<()> {
        if ms < -1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{which}_decay_ms must be -1 (never) or a non-negative millisecond count"),
            ));
        }
        let name = format!("arena.{arena}.{which}_decay_ms\0");
        // SAFETY: both decay mallctls are writable `ssize_t`s, the name is
        // null-terminated by construction above, and the value is range-checked
        // against what jemalloc accepts.
        unsafe { raw::write::<isize>(name.as_bytes(), ms as isize) }
            .map_err(|e| std::io::Error::other(format!("arena.{arena}.{which}_decay_ms: {e}")))
    }

    /// Read the process-wide `opt.{dirty,muzzy}_decay_ms` — the decay settings
    /// `malloc_conf` asked for, which every arena inherits when it is created.
    ///
    /// Reading these back is how a build proves its `malloc_conf` decay
    /// settings were picked up, the same way [`configured_narenas`] proves
    /// `narenas:`.
    pub fn configured_decay() -> Option<ArenaDecay> {
        // SAFETY: both `opt.*_decay_ms` are read-only `ssize_t` mallctls.
        let dirty = unsafe { raw::read::<isize>(b"opt.dirty_decay_ms\0") }.ok()?;
        let muzzy = unsafe { raw::read::<isize>(b"opt.muzzy_decay_ms\0") }.ok()?;
        Some(ArenaDecay {
            dirty_ms: dirty as i64,
            muzzy_ms: muzzy as i64,
        })
    }

    /// `arenas.narenas`: how many arenas the process has actually created.
    ///
    /// With `narenas:1` set (see `frogdb_server`'s `malloc_conf`) this settles
    /// at `1 + shard_count` once the shards are up — one automatic arena for
    /// every non-shard thread, plus one per shard.
    pub fn narenas() -> Option<u32> {
        // SAFETY: `arenas.narenas` is a read-only `unsigned` mallctl.
        unsafe { raw::read::<u32>(b"arenas.narenas\0") }.ok()
    }

    /// `opt.narenas`: the arena count jemalloc was *configured* with, i.e. the
    /// `narenas:` setting `malloc_conf` asked for.
    ///
    /// Reading this is how a build proves its `malloc_conf` symbol was actually
    /// picked up — jemalloc's default is `4 × ncpu`, so a value of 1 can only
    /// come from the configuration having taken effect.
    pub fn configured_narenas() -> Option<u32> {
        // SAFETY: `opt.narenas` is a read-only `unsigned` mallctl.
        unsafe { raw::read::<u32>(b"opt.narenas\0") }.ok()
    }

    /// `thread.allocated`: bytes this thread has ever allocated.
    ///
    /// Exact, thread-local and cheap (23 ns, against 146 ns for an arena stat
    /// read and ~2.5 µs per arena for the epoch advance that makes it current),
    /// so per-request accounting rides this counter between samples and
    /// reconciles against the arena figure at each tick (ADR-0006 §3). It is
    /// cumulative, not a live figure: the difference of two reads is the bytes
    /// allocated in between.
    pub fn thread_allocated() -> Option<u64> {
        // SAFETY: `thread.allocated` is a read-only `uint64_t` mallctl.
        unsafe { raw::read::<u64>(b"thread.allocated\0") }.ok()
    }

    /// `thread.deallocated`: bytes this thread has ever freed. The companion of
    /// [`thread_allocated`]; their difference is the thread's live bytes.
    pub fn thread_deallocated() -> Option<u64> {
        // SAFETY: `thread.deallocated` is a read-only `uint64_t` mallctl.
        unsafe { raw::read::<u64>(b"thread.deallocated\0") }.ok()
    }

    /// `jemalloc`'s `MALLCTL_ARENAS_ALL` sentinel arena index: purging this
    /// pseudo-arena operates over every real arena in one call. It's a C
    /// preprocessor `#define` (`4096`), not a symbol bindgen exposes, so it's
    /// pinned here as a plain constant.
    ///
    /// Not every per-arena mallctl accepts it — [`set_arena_decay`] does not.
    pub const ALL_ARENAS: u32 = 4096;

    /// `MEMORY PURGE`: force jemalloc to return unused dirty pages across
    /// every arena to the OS (`arena.<ALL_ARENAS>.purge`).
    pub fn purge_all() -> std::io::Result<()> {
        void_mallctl(&format!("arena.{ALL_ARENAS}.purge"))
    }

    /// Issue a `void`-typed mallctl — one that takes and returns no value.
    ///
    /// jemalloc's `arena.<i>.purge` and `thread.tcache.flush` can neither be
    /// read nor written (see `NEITHER_READ_NOR_WRITE()` guarding
    /// `arena_i_purge_ctl` in jemalloc's `ctl.c`): the call must pass null for
    /// every one of `oldp`/`oldlenp`/`newp`, with `newlen == 0`.
    /// `tikv_jemalloc_ctl`'s typed `read`/`write`/`update` helpers always pass a
    /// non-null pointer to their value (even a zero-sized one), which jemalloc
    /// rejects with `EPERM` — so this issues the raw `mallctl` call directly
    /// through `tikv-jemalloc-sys` instead.
    fn void_mallctl(name: &str) -> std::io::Result<()> {
        let name = format!("{name}\0");
        // SAFETY: `name` is a valid null-terminated C string naming a
        // `void`-typed mallctl (see doc comment above); every in/out
        // pointer is null and `newlen` is 0, matching what
        // `NEITHER_READ_NOR_WRITE()` requires.
        let ret = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr() as *const std::ffi::c_char,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(ret))
        }
    }
}

#[cfg(target_env = "msvc")]
mod imp {
    /// jemalloc isn't linked on `msvc` (see the `#[global_allocator]` gate
    /// in `frogdb-server`'s `main.rs`), so there is no allocator to read.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct AllocatorStats {
        pub allocated: u64,
        pub active: u64,
        pub resident: u64,
        pub mapped: u64,
        pub retained: u64,
    }

    /// See the jemalloc arm for what each field means. Without jemalloc there
    /// are no arenas, so nothing ever constructs one of these.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ArenaStats {
        pub arena: u32,
        pub small_allocated_upper_bound: u64,
        pub large_allocated_upper_bound: u64,
        pub resident: u64,
        pub active: u64,
        pub dirty: u64,
        pub muzzy: u64,
        pub retained: u64,
    }

    /// See the jemalloc arm. Without jemalloc there is no decay to configure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ArenaDecay {
        pub dirty_ms: i64,
        pub muzzy_ms: i64,
    }

    /// See the jemalloc arm.
    pub const ALL_ARENAS: u32 = 4096;

    impl ArenaStats {
        pub fn allocated_upper_bound(&self) -> u64 {
            self.small_allocated_upper_bound
                .saturating_add(self.large_allocated_upper_bound)
        }

        pub fn fragmentation_ratio(&self) -> Option<f64> {
            None
        }
    }

    pub fn read_stats() -> Option<AllocatorStats> {
        None
    }

    pub fn allocator_name() -> String {
        "libc".to_string()
    }

    /// No jemalloc, so no arena to create. The shard executor treats this the
    /// same way it treats a failed `arenas.create`: the shard runs unbound and
    /// reports no arena, rather than reporting a fictional one.
    pub fn create_arena() -> std::io::Result<u32> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "jemalloc is not linked on this target; there are no arenas to create",
        ))
    }

    pub fn bind_current_thread_to_arena(_arena: u32) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "jemalloc is not linked on this target; there is no arena to bind to",
        ))
    }

    pub fn current_thread_arena() -> Option<u32> {
        None
    }

    /// Matches [`purge_all`]: an allocator with no thread cache to flush has
    /// nothing to fail at.
    pub fn flush_current_thread_cache() -> std::io::Result<()> {
        Ok(())
    }

    pub fn advance_epoch() -> bool {
        false
    }

    pub fn read_arena_stats(_arena: u32) -> Option<ArenaStats> {
        None
    }

    pub fn page_size() -> Option<u64> {
        None
    }

    pub fn read_arena_decay(_arena: u32) -> Option<ArenaDecay> {
        None
    }

    pub fn set_arena_decay(_arena: u32, _decay: ArenaDecay) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "jemalloc is not linked on this target; there is no arena decay to configure",
        ))
    }

    pub fn configured_decay() -> Option<ArenaDecay> {
        None
    }

    pub fn narenas() -> Option<u32> {
        None
    }

    pub fn configured_narenas() -> Option<u32> {
        None
    }

    pub fn thread_allocated() -> Option<u64> {
        None
    }

    pub fn thread_deallocated() -> Option<u64> {
        None
    }

    /// Matches real Redis's own non-jemalloc `jemalloc_purge()` fallback
    /// (`zmalloc.c`): unconditionally succeeds as a no-op rather than
    /// erroring, since `MEMORY PURGE` against an allocator with nothing to
    /// purge isn't a failure.
    pub fn purge_all() -> std::io::Result<()> {
        Ok(())
    }
}

pub use imp::{
    ALL_ARENAS, AllocatorStats, ArenaDecay, ArenaStats, advance_epoch, allocator_name,
    bind_current_thread_to_arena, configured_decay, configured_narenas, create_arena,
    current_thread_arena, flush_current_thread_cache, narenas, page_size, purge_all,
    read_arena_decay, read_arena_stats, read_stats, set_arena_decay, thread_allocated,
    thread_deallocated,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn read_stats_reports_nonzero_allocated_and_resident() {
        // Force an allocation so `stats.allocated` can't be a startup
        // fluke, then advance the epoch (inside `read_stats`) and check.
        let _keep_alive: Vec<u8> = vec![0u8; 4 * 1024 * 1024];
        let stats = read_stats().expect("jemalloc stats should be readable when linked");
        assert!(stats.allocated > 0, "allocated should be nonzero");
        assert!(stats.resident > 0, "resident should be nonzero");
        assert!(
            stats.resident >= stats.active,
            "resident ({}) should be >= active ({})",
            stats.resident,
            stats.active
        );
        assert!(
            stats.active >= stats.allocated,
            "active ({}) should be >= allocated ({})",
            stats.active,
            stats.allocated
        );
        drop(_keep_alive);
    }

    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn allocator_name_reports_jemalloc() {
        assert!(allocator_name().starts_with("jemalloc"));
    }

    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn purge_all_succeeds() {
        purge_all().expect("arena purge should succeed under jemalloc");
    }

    // ------------------------------------------------------- per-arena ---
    //
    // These are the executable form of ADR-0006 §3 and of the spike's E1/E4
    // (`.scratch/memory-architecture/spike-report.md` §(a)). They allocate on
    // real threads because that is the only place the properties exist: under
    // one thread every "cross-arena" assertion is vacuously true, which is
    // exactly why `specs/memory.md` forbids forcing them from a simulation.

    /// A 64 KiB chunk is an exact jemalloc large size class, so a thread's
    /// requested volume equals the bytes its arena is charged (spike §(a) E1)
    /// and the assertions can be tight rather than order-of-magnitude.
    #[cfg(not(target_env = "msvc"))]
    const CHUNK: usize = 64 * 1024;

    /// Allocate `chunks × CHUNK` bytes and keep them live.
    #[cfg(not(target_env = "msvc"))]
    fn allocate_chunks(chunks: usize) -> Vec<Vec<u8>> {
        (0..chunks)
            .map(|_| {
                let mut v = vec![0u8; CHUNK];
                // Touch it so the pages are resident, not just reserved.
                v[0] = 1;
                v
            })
            .collect()
    }

    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn create_arena_hands_out_distinct_indices() {
        let a = create_arena().expect("arenas.create");
        let b = create_arena().expect("arenas.create");
        assert_ne!(a, b, "each arenas.create must yield a fresh arena");
        // Arena 0 is jemalloc's automatic arena; a created arena is never it,
        // which is what lets `arena_of(shard) == Some(0)` be impossible.
        assert!(a > 0 && b > 0, "created arenas are never the auto arena 0");
    }

    /// PRD R3's no-cross-arena-bleed rule, generalised from the spike's E1b:
    /// four threads each allocate a *different* known volume, and every arena
    /// reports its own thread's bytes and nobody else's.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn each_bound_thread_is_charged_its_own_bytes_and_no_others() {
        // Distinct volumes, each separated from the others by far more than the
        // per-thread incidental overhead, so "arena i holds thread j's bytes"
        // cannot hide inside the slack.
        const CHUNKS: [usize; 4] = [16, 48, 112, 240];
        // Stack frames, the `Vec<Vec<u8>>` spine and thread-local setup are
        // charged to the same arena (spike §(a): the spines are why raw
        // attribution ran 0.7–3.6 % over requested). 1 MiB covers them with
        // room to spare and is a fifth of the smallest gap between volumes.
        const SLACK: u64 = 1024 * 1024;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(CHUNKS.len() + 1));
        let done = std::sync::Arc::new(std::sync::Barrier::new(CHUNKS.len() + 1));
        let arenas = std::sync::Arc::new(std::sync::Mutex::new(vec![0u32; CHUNKS.len()]));

        let threads: Vec<_> = CHUNKS
            .iter()
            .enumerate()
            .map(|(i, &chunks)| {
                let (barrier, done, arenas) = (barrier.clone(), done.clone(), arenas.clone());
                std::thread::spawn(move || {
                    // Bind first, allocate second — the whole point.
                    let arena = create_arena().expect("arenas.create");
                    bind_current_thread_to_arena(arena).expect("thread.arena");
                    arenas.lock().unwrap()[i] = arena;
                    let held = allocate_chunks(chunks);
                    barrier.wait(); // everyone allocated; main reads
                    done.wait(); // main is done reading
                    drop(held);
                })
            })
            .collect();

        barrier.wait();
        assert!(advance_epoch(), "epoch advance");
        let arenas = arenas.lock().unwrap().clone();
        let observed: Vec<u64> = arenas
            .iter()
            .map(|&a| {
                read_arena_stats(a)
                    .expect("arena stats")
                    .allocated_upper_bound()
            })
            .collect();
        done.wait();
        for t in threads {
            t.join().expect("shard thread");
        }

        for (i, &chunks) in CHUNKS.iter().enumerate() {
            let expected = (chunks * CHUNK) as u64;
            assert!(
                observed[i] >= expected,
                "arena {} (thread {i}) reported {} B, short of the {expected} B that thread \
                 allocated — attribution is losing bytes",
                arenas[i],
                observed[i],
            );
            assert!(
                observed[i] < expected + SLACK,
                "arena {} (thread {i}) reported {} B against {expected} B allocated: {} B of \
                 excess is more than this thread's own overhead, so it is carrying another \
                 thread's bytes (no-cross-arena-bleed, PRD R3)",
                arenas[i],
                observed[i],
                observed[i] - expected,
            );
        }
        // Stated the other way round, so a failure names the pair: no arena can
        // be within slack of a *different* thread's volume.
        for (i, &here) in observed.iter().enumerate() {
            for (j, &chunks) in CHUNKS.iter().enumerate() {
                if i == j {
                    continue;
                }
                let theirs = (chunks * CHUNK) as u64;
                assert!(
                    here < theirs || here >= theirs + SLACK,
                    "arena {} (thread {i}) reports {here} B, indistinguishable from thread {j}'s \
                     {theirs} B",
                    arenas[i],
                );
            }
        }
    }

    /// Bind-order, the executable form of ADR-0006 §3's "before the thread
    /// allocates anything": bytes allocated ahead of the bind are charged to
    /// the automatic arena and never move, so a late bind leaves the shard's
    /// own arena reporting a near-zero figure while the bytes sit elsewhere.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn bytes_allocated_before_the_bind_never_reach_the_arena() {
        const CHUNKS: usize = 96; // 6 MiB
        let volume = (CHUNKS * CHUNK) as u64;

        let early = std::thread::spawn(|| {
            let arena = create_arena().expect("arenas.create");
            let held = allocate_chunks(CHUNKS); // allocated BEFORE the bind
            bind_current_thread_to_arena(arena).expect("thread.arena");
            assert!(advance_epoch(), "epoch advance");
            let stats = read_arena_stats(arena).expect("arena stats");
            drop(held);
            stats.allocated_upper_bound()
        })
        .join()
        .expect("late-binding thread");

        let ordered = std::thread::spawn(|| {
            let arena = create_arena().expect("arenas.create");
            bind_current_thread_to_arena(arena).expect("thread.arena");
            let held = allocate_chunks(CHUNKS); // allocated AFTER the bind
            assert!(advance_epoch(), "epoch advance");
            let stats = read_arena_stats(arena).expect("arena stats");
            drop(held);
            stats.allocated_upper_bound()
        })
        .join()
        .expect("bind-first thread");

        assert!(
            ordered >= volume,
            "a thread that bound before allocating must have its {volume} B on its own arena, \
             got {ordered} B"
        );
        assert!(
            early < volume / 2,
            "a thread that allocated before binding reported {early} B on its arena, but those \
             {volume} B were charged to the automatic arena at allocation time and cannot move — \
             this assertion is what makes the bind an *ordering* requirement rather than a \
             startup step"
        );
    }

    /// Why every byte field is named `*_upper_bound` (spike §(a) E4 rows 1 and
    /// 3): a region freed into the owning thread's cache is still charged to
    /// its arena, and only `thread.tcache.flush` returns it.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn freed_bytes_stay_charged_until_the_thread_cache_is_flushed() {
        // 128 B is an exact small size class; 100 regions is well inside the
        // per-bin cache capacity (the spike saw 200 parked), so every free
        // lands in the cache rather than going back to the arena.
        const N: usize = 100;
        const SZ: usize = 128;
        const VOLUME: u64 = (N * SZ) as u64;

        let (filled, after_free, after_flush) = std::thread::spawn(|| {
            let arena = create_arena().expect("arenas.create");
            bind_current_thread_to_arena(arena).expect("thread.arena");

            let small = |label: &str| {
                assert!(advance_epoch(), "epoch advance ({label})");
                read_arena_stats(arena)
                    .expect("arena stats")
                    .small_allocated_upper_bound
            };

            let base = small("baseline");
            let mut held: Vec<Box<[u8; SZ]>> = (0..N).map(|_| Box::new([0u8; SZ])).collect();
            let filled = small("filled");
            held.clear();
            let after_free = small("freed");
            flush_current_thread_cache().expect("thread.tcache.flush");
            let after_flush = small("flushed");
            drop(held);
            (
                filled - base,
                after_free - base,
                after_flush.saturating_sub(base),
            )
        })
        .join()
        .expect("tcache thread");

        assert!(
            filled >= VOLUME,
            "{N} × {SZ} B should charge at least {VOLUME} B, got {filled} B"
        );
        assert!(
            after_free >= VOLUME,
            "after freeing every object the arena still reported only {after_free} B; the whole \
             reason the accounting fields are named `*_upper_bound` is that this figure does NOT \
             drop on free"
        );
        assert!(
            after_flush < VOLUME,
            "an explicit thread-cache flush must return the cached regions to the arena, but it \
             still reports {after_flush} B"
        );
    }

    /// The knobs the startup report and the broker read.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn arena_counts_and_thread_counters_are_readable() {
        let before = narenas().expect("arenas.narenas");
        let created = create_arena().expect("arenas.create");
        let after = narenas().expect("arenas.narenas");
        assert!(
            after > before && created < after,
            "arenas.narenas ({before} -> {after}) must account for the arena just created \
             ({created})"
        );
        assert!(
            configured_narenas().is_some(),
            "opt.narenas is how a build proves its malloc_conf took effect"
        );

        // `thread.allocated` is the exact per-thread counter accounting rides
        // between samples (23 ns, ADR-0006 §3).
        let start = thread_allocated().expect("thread.allocated");
        let held = allocate_chunks(16);
        let end = thread_allocated().expect("thread.allocated");
        assert!(
            end - start >= (16 * CHUNK) as u64,
            "thread.allocated moved {} B for a {} B allocation",
            end - start,
            16 * CHUNK
        );
        assert!(thread_deallocated().is_some());
        drop(held);
    }

    /// The page-derived depth figures are bytes, not page counts, and they sit
    /// in the order jemalloc's own accounting guarantees: an arena's active
    /// bytes cover everything allocated out of it, and its resident bytes cover
    /// the active and dirty pages both.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn arena_depth_is_reported_in_bytes() {
        let page = page_size().expect("arenas.page");
        assert!(
            page.is_power_of_two() && page >= 4096,
            "arenas.page is a page size, got {page}"
        );

        let arena = create_arena().expect("arenas.create");
        let held = std::thread::spawn(move || {
            bind_current_thread_to_arena(arena).expect("thread.arena");
            let held = allocate_chunks(64);
            flush_current_thread_cache().expect("thread.tcache.flush");
            held
        })
        .join()
        .expect("bound thread");

        assert!(advance_epoch());
        let stats = read_arena_stats(arena).expect("stats.arenas.<i>");
        assert!(
            stats.active >= stats.allocated_upper_bound(),
            "active bytes ({}) must cover the arena's allocations ({})",
            stats.active,
            stats.allocated_upper_bound()
        );
        assert!(
            stats.active % page == 0 && stats.dirty % page == 0 && stats.muzzy % page == 0,
            "the page-derived figures must be whole pages: {stats:?}"
        );
        assert!(
            stats.resident >= stats.active,
            "resident bytes ({}) must cover the active pages ({})",
            stats.resident,
            stats.active
        );
        drop(held);
    }

    /// Decay is per-arena, readable, and settable at runtime — the whole point
    /// of PRD R13 refusing a background defragmenter is that these two numbers
    /// are the reclamation policy, so an operator has to be able to move them
    /// without a restart.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn arena_decay_is_readable_and_settable_at_runtime() {
        let arena = create_arena().expect("arenas.create");
        let before = read_arena_decay(arena).expect("arena.<i>.dirty_decay_ms");

        let wanted = ArenaDecay {
            dirty_ms: before.dirty_ms + 1234,
            muzzy_ms: 0,
        };
        set_arena_decay(arena, wanted).expect("arena.<i> decay write");
        assert_eq!(read_arena_decay(arena), Some(wanted));

        // -1 is jemalloc's "never decay"; anything below it is not a duration.
        set_arena_decay(
            arena,
            ArenaDecay {
                dirty_ms: -1,
                muzzy_ms: -1,
            },
        )
        .expect("never-decay");
        assert_eq!(
            read_arena_decay(arena),
            Some(ArenaDecay {
                dirty_ms: -1,
                muzzy_ms: -1
            })
        );
        assert!(
            set_arena_decay(
                arena,
                ArenaDecay {
                    dirty_ms: -2,
                    muzzy_ms: 0
                }
            )
            .is_err(),
            "-2 is not a decay setting jemalloc accepts"
        );

        set_arena_decay(arena, before).expect("restore");
        assert!(
            configured_decay().is_some(),
            "opt.*_decay_ms is how a build proves its malloc_conf decay settings took effect"
        );
    }

    /// `arena.<i>.purge` takes [`ALL_ARENAS`]; `arena.<i>.*_decay_ms` does not —
    /// its handler resolves the index to a real arena first and answers
    /// `EFAULT`. Callers therefore loop. Pinned because the two mallctls look
    /// interchangeable in the header and are not.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn the_all_arenas_sentinel_is_rejected_by_the_decay_mallctl() {
        assert!(
            set_arena_decay(
                ALL_ARENAS,
                ArenaDecay {
                    dirty_ms: 0,
                    muzzy_ms: 0
                }
            )
            .is_err()
        );
        purge_all().expect("the same sentinel is accepted by arena.<i>.purge");
    }

    /// The spike left open whether `mallctlnametomib` resolves for a
    /// `stats.arenas.<i>.*` name under `tikv-jemalloc-ctl 0.6.1`; it does. This
    /// records the answer rather than adopting it — see the module docs for why
    /// the by-name path stays.
    #[test]
    #[cfg(not(target_env = "msvc"))]
    fn arena_stat_names_resolve_to_mibs() {
        let arena = create_arena().expect("arenas.create");
        let mut mib = [0usize; 5]; // stats . arenas . <i> . small . allocated
        let resolved =
            tikv_jemalloc_ctl::raw::name_to_mib(b"stats.arenas.0.small.allocated\0", &mut mib);
        assert!(
            resolved.is_ok(),
            "mallctlnametomib failed for a stats.arenas.<i> name: {resolved:?}"
        );
        // Component 2 is the arena index, rewritten per arena on a MIB read.
        mib[2] = arena as usize;
        assert!(advance_epoch());
        // SAFETY: the MIB was resolved from a `size_t`-typed name above.
        let by_mib = unsafe { tikv_jemalloc_ctl::raw::read_mib::<usize>(&mib) };
        assert_eq!(
            by_mib.ok().map(|v| v as u64),
            read_arena_stats(arena).map(|s| s.small_allocated_upper_bound),
            "the MIB and by-name paths must agree, or the recorded finding is about the wrong \
             thing"
        );
    }

    #[test]
    #[cfg(target_env = "msvc")]
    fn read_stats_is_none_without_jemalloc() {
        assert_eq!(read_stats(), None);
    }

    #[test]
    #[cfg(target_env = "msvc")]
    fn purge_all_is_a_no_op_success_without_jemalloc() {
        purge_all().expect("purge should no-op successfully, matching Redis's fallback");
    }

    /// Without jemalloc there is no arena to create, and the shard executor must
    /// see that as a failure so it reports `arena_of() == None` rather than a
    /// fictional arena index.
    #[test]
    #[cfg(target_env = "msvc")]
    fn arena_binding_is_unsupported_without_jemalloc() {
        assert!(create_arena().is_err());
        assert!(bind_current_thread_to_arena(1).is_err());
        assert_eq!(current_thread_arena(), None);
        assert_eq!(read_arena_stats(1), None);
        assert_eq!(narenas(), None);
        assert!(!advance_epoch());
    }
}
