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

#[cfg(not(target_env = "msvc"))]
mod imp {
    use tikv_jemalloc_ctl::{epoch, stats};

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

    /// `jemalloc`'s `MALLCTL_ARENAS_ALL` sentinel arena index: purging (or
    /// reading stats for) this pseudo-arena operates over every real arena
    /// in one call. It's a C preprocessor `#define` (`4096`), not a symbol
    /// bindgen exposes, so it's pinned here as a plain constant.
    const MALLCTL_ARENAS_ALL: u32 = 4096;

    /// `MEMORY PURGE`: force jemalloc to return unused dirty pages across
    /// every arena to the OS (`arena.<MALLCTL_ARENAS_ALL>.purge`).
    ///
    /// jemalloc's arena-purge control can neither be read nor written (see
    /// `NEITHER_READ_NOR_WRITE()` guarding `arena_i_purge_ctl` in
    /// jemalloc's `ctl.c`): the call must pass null for every one of
    /// `oldp`/`oldlenp`/`newp`, with `newlen == 0`. `tikv_jemalloc_ctl`'s
    /// typed `read`/`write`/`update` helpers always pass a non-null
    /// pointer to their value (even a zero-sized one), which jemalloc
    /// rejects with `EPERM` — so this issues the raw `mallctl` call
    /// directly through `tikv-jemalloc-sys` instead.
    pub fn purge_all() -> std::io::Result<()> {
        let name = format!("arena.{MALLCTL_ARENAS_ALL}.purge\0");
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

    pub fn read_stats() -> Option<AllocatorStats> {
        None
    }

    pub fn allocator_name() -> String {
        "libc".to_string()
    }

    /// Matches real Redis's own non-jemalloc `jemalloc_purge()` fallback
    /// (`zmalloc.c`): unconditionally succeeds as a no-op rather than
    /// erroring, since `MEMORY PURGE` against an allocator with nothing to
    /// purge isn't a failure.
    pub fn purge_all() -> std::io::Result<()> {
        Ok(())
    }
}

pub use imp::{AllocatorStats, allocator_name, purge_all, read_stats};

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
}
