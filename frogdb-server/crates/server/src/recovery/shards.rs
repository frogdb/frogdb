//! Phases 2-3: open RocksDB and restore per-shard stores.
//!
//! Phase 2 opens RocksDB with the configured shard count; the open call itself
//! validates the persisted shard count against the configured one and fails
//! loudly on a mismatch (rather than silently misrouting or dropping data).
//! Phase 3 rebuilds the per-shard hash tables, expiry indexes, and warm-tier
//! entries from the opened store.

use anyhow::Result;
use frogdb_core::persistence::{RecoveryStats, RocksConfig, RocksStore, recover_all_shards};
use frogdb_core::sync::Arc;
use frogdb_core::{ExpiryIndex, HashMapStore};
use tracing::info;

use super::RecoveryInputs;

/// Phase 2: open RocksDB (with optional warm-tier column families).
pub(super) fn open_rocks(inputs: &RecoveryInputs<'_>) -> Result<Arc<RocksStore>> {
    let config = inputs.persistence;

    // The operator-vs-invariant knob partition lives in `RocksConfig::from_persistence`
    // (next to `RocksConfig::default()`); this site only supplies the two arguments
    // that come from `RecoveryInputs`, not `PersistenceConfig`: `num_shards` and
    // `warm_enabled`.
    let rocks_config = RocksConfig::from_persistence(config);

    let rocks = Arc::new(RocksStore::open_with_warm_metrics(
        &config.data_dir,
        inputs.num_shards,
        &rocks_config,
        inputs.warm_enabled,
        inputs.metrics_recorder.clone(),
    )?);

    Ok(rocks)
}

/// Phase 3: restore per-shard hash tables, expiry indexes, and warm-tier entries.
///
/// When the database has no existing data this returns `num_shards` fresh empty
/// stores; otherwise it replays every shard's persisted state.
pub(super) fn restore(
    inputs: &RecoveryInputs<'_>,
    rocks: &Arc<RocksStore>,
) -> Result<(Vec<(HashMapStore, ExpiryIndex)>, RecoveryStats)> {
    if rocks.has_data() {
        info!("Recovering data from RocksDB...");
        let (stores, stats) = recover_all_shards(rocks)?;
        info!(
            keys_loaded = stats.keys_loaded,
            keys_expired = stats.keys_expired_skipped,
            bytes = stats.bytes_loaded,
            duration_ms = stats.duration_ms,
            "Recovery complete"
        );
        Ok((stores, stats))
    } else {
        info!("No existing data found, starting fresh");
        Ok((
            (0..inputs.num_shards).map(|_| Default::default()).collect(),
            RecoveryStats::default(),
        ))
    }
}
