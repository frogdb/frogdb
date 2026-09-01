//! One block cache and one write-buffer manager for the whole process, sized
//! from the persistence [`Budget`].
//!
//! RocksDB's memtables and block cache are C++ allocations that no Rust figure
//! in FrogDB used to see: not `INFO memory`, not the `maxmemory` arithmetic,
//! not the per-subsystem breakdown. Worse, they were sized as a *product* —
//! `write_buffer_size` × `max_write_buffer_number` × shards × column-family
//! tiers — so the ceiling moved every time an operator changed the shard count.
//!
//! This module is the seam that replaces the product with a budget:
//!
//! * one [`WriteBufferManager`], created with
//!   [`RocksConfig::memtable_budget_bytes`], stalling writers at capacity
//!   (FM-PERSISTENCE-060);
//! * one [`Cache`], created with [`RocksConfig::memory_budget_bytes`] — the sum
//!   of the two operator knobs, because memtable bytes are costed to the same
//!   cache (FM-PERSISTENCE-061);
//! * one [`Budget`] for [`Subsystem::Persistence`], whose [`Charge`] is
//!   reconciled against what the engine *reports* it is holding.
//!
//! # What is bounded by what
//!
//! The memtable half is hard-bounded: `allow_stall = true` makes RocksDB block
//! the writer until a flush frees room, which is the `Backpressure` disposition
//! realized by the engine rather than asserted by us.
//!
//! The cache half is bounded by **eviction**, not by refusal. A strict-capacity
//! cache is not constructible through `rocksdb 0.24`: the C API has
//! `rocksdb_cache_create_lru_with_strict_capacity_limit`, but `rocksdb::Cache`
//! exposes no constructor from a raw `rocksdb_cache_t*`, `LruCacheOptions`
//! carries only capacity and shard bits, and the crate does not re-export
//! `librocksdb_sys`. So the cache stays at capacity by evicting, and overshoots
//! only for entries something has pinned.
//!
//! The refusal that remains observable is the *budget's*: [`RocksMemory::refresh`]
//! reconciles the charge against the engine's reported footprint, and a reading
//! above the budget is a [`Refused`] counted at
//! `frogdb_memory_budget_refusals_total{subsystem="persistence"}`.

use frogdb_memory::{Budget, Charge, Disposition, Refused, Subsystem};
use frogdb_types::metrics::definitions::{
    RocksdbBlockCacheBytes, RocksdbBlockCacheCapacityBytes, RocksdbBlockCachePinnedBytes,
    RocksdbWriteBufferBytes, RocksdbWriteBufferLimitBytes,
};
use rocksdb::{BlockBasedOptions, Cache, Options, WriteBufferManager};

use super::config::RocksConfig;

/// What the engine reports it is holding, plus what the budget thinks.
///
/// Every field is a live read from RocksDB (or from the budget), taken at the
/// same instant, so the gauges derived from it describe one moment rather than
/// five staggered ones.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RocksMemoryUsage {
    /// `WriteBufferManager::get_usage` — arena bytes across every memtable in
    /// the process.
    pub write_buffer_bytes: u64,
    /// `WriteBufferManager::get_buffer_size` — the memtable ceiling.
    pub write_buffer_limit_bytes: u64,
    /// `Cache::get_usage` — block-cache bytes, *including* the memtable bytes
    /// costed to it. Zero when no cache is installed.
    pub block_cache_bytes: u64,
    /// `Cache::get_capacity`, as configured. Zero when no cache is installed
    /// (RocksDB's own per-table-factory default is then in force, and its size
    /// is not reachable from here).
    pub block_cache_capacity_bytes: u64,
    /// `Cache::get_pinned_usage` — the part of the cache that cannot be
    /// evicted, which is what lets usage exceed capacity.
    pub block_cache_pinned_bytes: u64,
}

impl RocksMemoryUsage {
    /// The single figure the persistence budget is charged for.
    ///
    /// When a cache is installed, memtable bytes are already inside
    /// `block_cache_bytes` (that is what "cost to cache" means), so adding the
    /// write-buffer usage would double-count it. Without a cache the two are
    /// disjoint and the memtable figure is all there is.
    pub fn charged_footprint_bytes(&self) -> u64 {
        if self.block_cache_capacity_bytes > 0 {
            self.block_cache_bytes
        } else {
            self.write_buffer_bytes
        }
    }
}

/// The process-wide RocksDB memory seam: the shared cache, the shared
/// write-buffer manager, and the persistence budget they are sized from.
///
/// Held by the `RocksStore` for the lifetime of the DB. That is not
/// bookkeeping: `rocksdb`'s `OptionsMustOutliveDB` rule means dropping the
/// cache or the manager while a column family still points at it is undefined
/// behavior, so ownership sits with the store, not with the options builder
/// that installed them.
///
/// No `Debug`: neither `rocksdb::Cache` nor `rocksdb::WriteBufferManager`
/// implements it, and a hand-rolled one would print handle addresses. Use
/// [`Self::usage`], which is the readable state.
pub struct RocksMemory {
    /// `None` when `block_cache_size == 0` — the operator asked for RocksDB's
    /// own default, and installing a cache of any capacity would override it
    /// (FM-PERSISTENCE-061).
    cache: Option<Cache>,
    /// The capacity `cache` was created with. `rocksdb::Cache` has no capacity
    /// reader, and the DB property that does (`rocksdb.block-cache-capacity`)
    /// needs a live DB and a column family; this is the same number, recorded
    /// where the cache is built.
    cache_capacity_bytes: u64,
    write_buffer_manager: WriteBufferManager,
    budget: Budget,
    /// The live charge against `budget`, grown and shrunk by [`Self::refresh`].
    /// A `Mutex` rather than an atomic because `Charge::grow` is a
    /// read-modify-write against the budget that must not interleave, and
    /// refresh runs on one metrics ticker at a time.
    charge: std::sync::Mutex<Charge>,
}

impl RocksMemory {
    /// Build the process-wide cache, manager, and budget from `config`, and
    /// install them on the options the caller will hand to every column family.
    ///
    /// `block_opts` and `cf_opts` are the *single* pair the open path clones
    /// per CF descriptor, which is what makes "one cache, one manager" true of
    /// the whole process rather than of one column family.
    pub(crate) fn install(
        config: &RocksConfig,
        block_opts: &mut BlockBasedOptions,
        cf_opts: &mut Options,
    ) -> Self {
        let memtable_budget = config.memtable_budget_bytes();
        let total_budget = config.memory_budget_bytes();

        // Two arms, and the difference between them is whether the operator
        // asked for RocksDB's default cache. With a cache, the manager costs
        // memtable bytes to it so one capacity covers both halves; without one,
        // the manager stands alone and still bounds memtables.
        let (cache, write_buffer_manager) = if config.block_cache_size > 0 {
            let cache = Cache::new_lru_cache(total_budget);
            block_opts.set_block_cache(&cache);
            let manager = WriteBufferManager::new_write_buffer_manager_with_cache(
                memtable_budget,
                true,
                cache.clone(),
            );
            (Some(cache), manager)
        } else {
            (
                None,
                WriteBufferManager::new_write_buffer_manager(memtable_budget, true),
            )
        };
        cf_opts.set_write_buffer_manager(&write_buffer_manager);

        // `Backpressure`, and the backpressure is not ours: it is the write
        // stall the manager applies at capacity. The budget's own refusals name
        // an engine footprint that has grown past what was authorized, which is
        // an operator signal rather than a per-write verdict.
        let budget = Budget::new(
            Subsystem::Persistence,
            Disposition::Backpressure,
            total_budget as u64,
        );
        let charge = budget.open_charge();

        Self {
            cache_capacity_bytes: if cache.is_some() {
                total_budget as u64
            } else {
                0
            },
            cache,
            write_buffer_manager,
            budget,
            charge: std::sync::Mutex::new(charge),
        }
    }

    /// A clone of the persistence budget handle, for the broker that adopts it.
    pub fn budget(&self) -> Budget {
        self.budget.clone()
    }

    /// Read the engine's current memory counters.
    pub fn usage(&self) -> RocksMemoryUsage {
        RocksMemoryUsage {
            write_buffer_bytes: self.write_buffer_manager.get_usage() as u64,
            write_buffer_limit_bytes: self.write_buffer_manager.get_buffer_size() as u64,
            block_cache_bytes: self.cache.as_ref().map_or(0, |c| c.get_usage() as u64),
            block_cache_capacity_bytes: self.cache_capacity_bytes,
            block_cache_pinned_bytes: self
                .cache
                .as_ref()
                .map_or(0, |c| c.get_pinned_usage() as u64),
        }
    }

    /// Reconcile the persistence charge with what the engine reports, and
    /// return the reading the reconciliation was based on.
    ///
    /// This is a *tracking* charge, not a gate: the bytes already exist by the
    /// time RocksDB reports them, so the charge follows the engine rather than
    /// authorizing it. What the reconciliation buys is the refusal — a reported
    /// footprint above the budget cannot be charged, and that refusal is the
    /// observable signal that the engine has outgrown its ceiling.
    pub fn refresh(&self) -> RocksMemoryUsage {
        let usage = self.usage();
        // A poisoned lock here means a previous refresh panicked mid-charge.
        // The charge is still valid (it is a plain counter handle), so the
        // reading is taken rather than propagating a panic into a metrics tick.
        let mut charge = self
            .charge
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Refusal is deliberately not swallowed here: `reconcile` leaves the
        // charge where it was and the budget counts the refusal, which is what
        // reaches `frogdb_memory_budget_refusals_total`.
        let _ = reconcile(&mut charge, usage.charged_footprint_bytes());
        usage
    }

    /// Reconcile the charge and publish the engine's counters as gauges.
    ///
    /// The two happen together on purpose: the gauges and the persistence
    /// `Budget` must describe the same instant, or an operator reading
    /// `frogdb_rocksdb_block_cache_bytes` next to
    /// `frogdb_memory_budget_charged_bytes{subsystem="persistence"}` sees a
    /// disagreement that is an artifact of two separate reads.
    pub fn record_metrics(&self, recorder: &dyn frogdb_types::traits::MetricsRecorder) {
        let usage = self.refresh();
        RocksdbWriteBufferBytes::set(recorder, usage.write_buffer_bytes as f64);
        RocksdbWriteBufferLimitBytes::set(recorder, usage.write_buffer_limit_bytes as f64);
        RocksdbBlockCacheBytes::set(recorder, usage.block_cache_bytes as f64);
        RocksdbBlockCacheCapacityBytes::set(recorder, usage.block_cache_capacity_bytes as f64);
        RocksdbBlockCachePinnedBytes::set(recorder, usage.block_cache_pinned_bytes as f64);
    }
}

/// Move `charge` to `reported`, growing or shrinking as needed.
///
/// A free function rather than a method so the refusal arm is reachable in a
/// unit test without a RocksDB instance: the engine's reported footprint stays
/// under the cache capacity by construction (the cache evicts), so the only way
/// to exercise the over-budget path in-crate is to drive the arithmetic
/// directly.
pub(crate) fn reconcile(charge: &mut Charge, reported: u64) -> Result<(), Refused> {
    let held = charge.bytes();
    match reported.cmp(&held) {
        std::cmp::Ordering::Greater => charge.grow(reported - held),
        std::cmp::Ordering::Less => {
            charge.shrink(held - reported);
            Ok(())
        }
        std::cmp::Ordering::Equal => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(limit: u64) -> Budget {
        Budget::new(Subsystem::Persistence, Disposition::Backpressure, limit)
    }

    // FM-PERSISTENCE-061
    #[test]
    fn the_charge_follows_the_engine_footprint_up_and_down() {
        let budget = budget(1024);
        let mut charge = budget.open_charge();

        reconcile(&mut charge, 400).expect("400 of 1024");
        assert_eq!(budget.charged(), 400, "charge follows the engine up");

        reconcile(&mut charge, 900).expect("900 of 1024");
        assert_eq!(budget.charged(), 900);

        // A flush frees memtable arena: the charge must follow the engine back
        // down, or the budget ratchets and every later reading is refused.
        reconcile(&mut charge, 100).expect("shrinking never refuses");
        assert_eq!(budget.charged(), 100);

        reconcile(&mut charge, 100).expect("an unchanged reading is a no-op");
        assert_eq!(budget.charged(), 100);
    }

    // FM-PERSISTENCE-061
    #[test]
    fn an_over_budget_engine_footprint_is_refused_and_counted() {
        let budget = budget(1024);
        let mut charge = budget.open_charge();
        reconcile(&mut charge, 1000).expect("1000 of 1024");

        let refused = reconcile(&mut charge, 2048).expect_err("2048 is over the 1024 budget");
        assert_eq!(refused.disposition, Disposition::Backpressure);
        assert_eq!(refused.subsystem, Subsystem::Persistence);
        assert_eq!(
            budget.charged(),
            1000,
            "a refused reconcile leaves the charge where it was"
        );
        assert_eq!(budget.refusals(), 1, "the refusal is what an operator sees");

        // And the seam recovers: once the engine is back inside the budget the
        // charge tracks it again rather than staying latched at the refusal.
        reconcile(&mut charge, 500).expect("500 of 1024");
        assert_eq!(budget.charged(), 500);
    }

    #[test]
    fn the_charged_footprint_is_the_cache_when_one_is_installed() {
        // With a cache, memtable bytes are already inside the cache figure —
        // adding them would double-count the same arena.
        let with_cache = RocksMemoryUsage {
            write_buffer_bytes: 300,
            write_buffer_limit_bytes: 1000,
            block_cache_bytes: 800,
            block_cache_capacity_bytes: 2000,
            block_cache_pinned_bytes: 10,
        };
        assert_eq!(with_cache.charged_footprint_bytes(), 800);

        let no_cache = RocksMemoryUsage {
            write_buffer_bytes: 300,
            write_buffer_limit_bytes: 1000,
            ..Default::default()
        };
        assert_eq!(no_cache.charged_footprint_bytes(), 300);
    }
}
