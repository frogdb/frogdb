//! Eager space reclamation after a full-shard clear (FLUSHDB / FLUSHALL).
//!
//! Proposal 43 lands a FLUSHDB clear as a single RocksDB range tombstone
//! (`DeleteRange`). The tombstone hides the data immediately and correctly, but
//! the underlying SST bytes are only reclaimed when compaction happens to cover
//! the range — so a large keyspace flushed on a quiet instance can hold its disk
//! footprint indefinitely (proposal 48).
//!
//! This module reclaims that space eagerly, following the RocksDB-recommended
//! bulk-deletion combo (also what Kvrocks does after its FLUSHDB `DeleteRange`):
//!
//! 1. [`DB::delete_file_in_range_cf`] — near-free, drops SSTs whose entire key
//!    range is covered, without rewriting anything. L0 files are always left
//!    alone by RocksDB, so writes that arrive after the clear (which land in the
//!    memtable / L0) are never dropped.
//! 2. [`DB::compact_range_cf_opt`] with `bottommost_level_compaction = Force` —
//!    rewrites the remainder and drops the tombstone itself. Compaction never
//!    discards live keys, so post-clear writes survive it.
//!
//! Reclamation runs on a dedicated background thread, never on the WAL flush
//! thread (a `CompactRange` can take seconds to minutes and must not stall the
//! flush pipeline). Repeated FLUSHDBs are coalesced by [`ReclaimGuard`]: while a
//! pass is in flight for a (tier, shard) the next request is dropped rather than
//! queued, because a pass started even slightly later already sees the newer
//! tombstone.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use frogdb_types::metrics::definitions::{FlushCompactCompleted, FlushCompactStarted};
use rocksdb::{BottommostLevelCompaction, CompactOptions};
use tracing::{debug, info, warn};

use super::RocksStore;
use super::columns::CfTier;

/// Stable discriminant for a [`CfTier`], used as a coalescing-map key alongside
/// the shard id. (`CfTier` is `Copy` but not `Hash`, and deriving `Hash` on a
/// public enum in another module is more coupling than a one-line mapping.)
fn tier_key(tier: CfTier) -> u8 {
    match tier {
        CfTier::Main => 0,
        CfTier::Warm => 1,
        CfTier::SearchMeta => 2,
    }
}

/// Coalescing guard for post-clear reclamation.
///
/// At most one reclamation pass runs per `(tier, shard)` at a time. A request
/// that arrives while a pass is already in flight is *coalesced* — dropped
/// rather than queued — because the running pass (or one started immediately
/// after it releases) already covers the newer tombstone. This is the guard the
/// double-flush unit test exercises.
#[derive(Default)]
pub(crate) struct ReclaimGuard {
    in_flight: Mutex<HashSet<(u8, usize)>>,
}

impl ReclaimGuard {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Try to claim the reclamation slot for `(tier, shard)`.
    ///
    /// Returns `true` if the caller now owns the slot and must run a pass (and
    /// later release it via [`finish`](Self::finish)); `false` if a pass is
    /// already in flight and this request should be coalesced away.
    pub(crate) fn try_begin(&self, tier: CfTier, shard: usize) -> bool {
        self.in_flight
            .lock()
            .unwrap()
            .insert((tier_key(tier), shard))
    }

    /// Release the slot claimed by a successful [`try_begin`](Self::try_begin).
    pub(crate) fn finish(&self, tier: CfTier, shard: usize) {
        self.in_flight
            .lock()
            .unwrap()
            .remove(&(tier_key(tier), shard));
    }

    /// Number of reclamation passes currently in flight (test observability).
    #[cfg(test)]
    pub(crate) fn in_flight_count(&self) -> usize {
        self.in_flight.lock().unwrap().len()
    }
}

/// Spawn an asynchronous post-clear reclamation pass for one column family.
///
/// No-op when `flush_compact_range` is disabled or when a pass is already in
/// flight for this `(tier, shard)` (coalesced). Otherwise spawns a background
/// thread that runs the DeleteFilesInRange + CompactRange combo and releases the
/// coalescing slot on completion.
///
/// `upper_bound` is the exclusive upper bound of the cleared keyspace captured
/// when the range tombstone was staged (`max_key ++ [0x00]`), scoping the cheap
/// `delete_file_in_range` pass. Callers only invoke this when a tombstone was
/// actually committed — an empty CF stages no tombstone and has nothing to
/// reclaim.
pub(crate) fn spawn_clear_reclamation(
    rocks: Arc<RocksStore>,
    tier: CfTier,
    shard_id: usize,
    upper_bound: Vec<u8>,
) {
    if !rocks.flush_compact_range {
        return;
    }
    if !rocks.reclaim_guard.try_begin(tier, shard_id) {
        debug!(
            shard_id,
            tier = ?tier,
            "post-clear reclamation already in flight; coalescing"
        );
        return;
    }

    let rocks_for_thread = Arc::clone(&rocks);
    let spawn = std::thread::Builder::new()
        .name(format!("clear-reclaim-{shard_id}"))
        .spawn(move || {
            run_reclamation(&rocks_for_thread, tier, shard_id, &upper_bound);
            rocks_for_thread.reclaim_guard.finish(tier, shard_id);
        });

    // If the OS refuses the thread, the closure never ran, so release the slot
    // we just claimed — otherwise a later FLUSHDB would be coalesced against a
    // pass that never happened.
    if let Err(e) = spawn {
        warn!(shard_id, tier = ?tier, error = %e, "failed to spawn post-clear reclamation thread");
        rocks.reclaim_guard.finish(tier, shard_id);
    }
}

/// The reclamation body: DeleteFilesInRange (best-effort, cheap) then a forced
/// bottommost CompactRange over the whole CF. Shared by the production spawn
/// path and by tests that want to run it synchronously. Emits the
/// `frogdb_flush_compact_{started,completed}_total` counters through the
/// store's metrics recorder.
pub(crate) fn run_reclamation(
    rocks: &RocksStore,
    tier: CfTier,
    shard_id: usize,
    upper_bound: &[u8],
) {
    let shard_label = shard_id.to_string();
    let cf = match rocks.tier_cf_handle(tier, shard_id) {
        Ok(cf) => cf,
        Err(e) => {
            warn!(shard_id, tier = ?tier, error = %e, "post-clear reclamation: CF handle unavailable");
            return;
        }
    };

    let metrics = rocks.metrics_recorder();
    FlushCompactStarted::inc(&*metrics, &shard_label);
    let start = Instant::now();
    info!(shard_id, tier = ?tier, "post-clear reclamation started");

    // (1) Cheap first pass: drop SSTs entirely within the cleared keyspace.
    // Bounded by the pre-clear max key so it never targets files that begin
    // above the cleared range; L0 files (where fresh post-clear writes live) are
    // exempt by RocksDB regardless.
    if let Err(e) = rocks
        .db
        .delete_file_in_range_cf(&cf, [].as_slice(), upper_bound)
    {
        // Non-fatal: the compaction below still reclaims the space.
        warn!(shard_id, tier = ?tier, error = %e, "post-clear delete_file_in_range failed; falling through to compaction");
    }

    // (2) Force a bottommost compaction over the whole CF to rewrite the
    // remainder and drop the range tombstone. Compaction never discards live
    // keys, so any writes that landed after the clear survive. `compact_range`
    // has no failure channel in the Rust binding (returns unit).
    let mut opts = CompactOptions::default();
    opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    rocks
        .db
        .compact_range_cf_opt(&cf, None::<&[u8]>, None::<&[u8]>, &opts);

    FlushCompactCompleted::inc(&*metrics, &shard_label);
    info!(
        shard_id,
        tier = ?tier,
        duration_ms = start.elapsed().as_millis() as u64,
        "post-clear reclamation finished"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_coalesces_while_in_flight() {
        let guard = ReclaimGuard::new();

        // First request for (Main, 0) claims the slot.
        assert!(guard.try_begin(CfTier::Main, 0));
        assert_eq!(guard.in_flight_count(), 1);

        // A second request for the same (tier, shard) while in flight is
        // coalesced away — no duplicate pass is queued.
        assert!(!guard.try_begin(CfTier::Main, 0));
        assert!(!guard.try_begin(CfTier::Main, 0));
        assert_eq!(guard.in_flight_count(), 1);

        // After the running pass finishes, the slot is free again.
        guard.finish(CfTier::Main, 0);
        assert_eq!(guard.in_flight_count(), 0);
        assert!(guard.try_begin(CfTier::Main, 0));
        guard.finish(CfTier::Main, 0);
    }

    #[test]
    fn guard_is_independent_per_tier_and_shard() {
        let guard = ReclaimGuard::new();

        // Distinct (tier, shard) slots never coalesce against each other.
        assert!(guard.try_begin(CfTier::Main, 0));
        assert!(guard.try_begin(CfTier::Warm, 0)); // different tier
        assert!(guard.try_begin(CfTier::Main, 1)); // different shard
        assert_eq!(guard.in_flight_count(), 3);

        // But each remains individually coalesced while in flight.
        assert!(!guard.try_begin(CfTier::Main, 0));
        assert!(!guard.try_begin(CfTier::Warm, 0));
        assert!(!guard.try_begin(CfTier::Main, 1));

        guard.finish(CfTier::Main, 0);
        guard.finish(CfTier::Warm, 0);
        guard.finish(CfTier::Main, 1);
        assert_eq!(guard.in_flight_count(), 0);
    }
}
