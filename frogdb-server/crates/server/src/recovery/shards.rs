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
use crate::server::util::{num_cpus, parse_compression};

/// Phase 2: open RocksDB (with optional warm-tier column families).
pub(super) fn open_rocks(inputs: &RecoveryInputs<'_>) -> Result<Arc<RocksStore>> {
    let config = inputs.persistence;

    let rocks_config = RocksConfig {
        write_buffer_size: config.write_buffer_size_mb * 1024 * 1024,
        compression: parse_compression(&config.compression),
        max_background_jobs: num_cpus::get() as i32,
        create_if_missing: true,
        block_cache_size: config.block_cache_size_mb * 1024 * 1024,
        bloom_filter_bits: config.bloom_filter_bits,
        max_write_buffer_number: config.max_write_buffer_number,
        level0_file_num_compaction_trigger: 8,
        target_file_size_base: 128 * 1024 * 1024,
        max_bytes_for_level_base: 512 * 1024 * 1024,
        compaction_rate_limit_mb: if config.compaction_rate_limit_mb > 0 {
            Some(config.compaction_rate_limit_mb)
        } else {
            None
        },
    };

    let rocks = Arc::new(RocksStore::open_with_warm(
        &config.data_dir,
        inputs.num_shards,
        &rocks_config,
        inputs.warm_enabled,
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
