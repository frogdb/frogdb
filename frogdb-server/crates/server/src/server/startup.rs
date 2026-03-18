//! Server component initialization helpers.

use anyhow::Result;
use frogdb_core::persistence::{
    RocksConfig, RocksStore, SnapshotCoordinator, recover_all_shards, spawn_periodic_sync,
};
use frogdb_core::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::config::PersistenceConfig;
use crate::net::{JoinHandle, spawn};

use super::util::{PersistenceInitResult, num_cpus, parse_compression};

/// Spawn periodic snapshot task.
pub fn spawn_periodic_snapshot_task(
    coordinator: Arc<dyn SnapshotCoordinator>,
    interval_secs: u64,
) -> JoinHandle<()> {
    info!(interval_secs, "Starting periodic snapshot task");

    spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            if coordinator.in_progress() {
                tracing::debug!("Skipping periodic snapshot - already in progress");
                continue;
            }

            match coordinator.start_snapshot() {
                Ok(handle) => {
                    tracing::info!(epoch = handle.epoch(), "Periodic snapshot started");
                    // Handle completes when background task finishes
                }
                Err(e) => {
                    tracing::error!(error = %e, "Periodic snapshot failed to start");
                }
            }
        }
    })
}

/// Initialize persistence layer.
pub fn init_persistence(
    config: &PersistenceConfig,
    num_shards: usize,
    wal_sync_monitor: Option<tokio_metrics::TaskMonitor>,
    warm_enabled: bool,
) -> Result<PersistenceInitResult> {
    use std::fs;

    info!(
        data_dir = %config.data_dir.display(),
        durability_mode = %config.durability_mode,
        "Initializing persistence"
    );

    // Ensure data directory exists
    fs::create_dir_all(&config.data_dir)?;

    // Check for and load staged checkpoint from replica full sync
    match RocksStore::load_staged_checkpoint(&config.data_dir) {
        Ok(true) => {
            info!("Loaded staged checkpoint from replica full sync");
        }
        Ok(false) => {
            // No checkpoint to load, continue normally
        }
        Err(e) => {
            error!(error = %e, "Failed to load staged checkpoint");
            return Err(anyhow::anyhow!("Failed to load staged checkpoint: {}", e));
        }
    }

    // Build RocksDB config
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

    // Open RocksDB (with optional warm tier column families)
    let rocks = Arc::new(RocksStore::open_with_warm(
        &config.data_dir,
        num_shards,
        &rocks_config,
        warm_enabled,
    )?);

    // Recover data if database has existing data
    let recovered = if rocks.has_data() {
        info!("Recovering data from RocksDB...");
        let (stores, stats) = recover_all_shards(&rocks)?;
        info!(
            keys_loaded = stats.keys_loaded,
            keys_expired = stats.keys_expired_skipped,
            bytes = stats.bytes_loaded,
            duration_ms = stats.duration_ms,
            "Recovery complete"
        );
        stores
    } else {
        info!("No existing data found, starting fresh");
        (0..num_shards).map(|_| Default::default()).collect()
    };

    // Start periodic sync if using periodic durability mode
    let sync_handle = if config.durability_mode.to_lowercase() == "periodic" {
        info!(
            interval_ms = config.sync_interval_ms,
            "Starting periodic WAL sync"
        );
        Some(spawn_periodic_sync(
            rocks.clone(),
            config.sync_interval_ms,
            wal_sync_monitor,
        ))
    } else {
        None
    };

    Ok((rocks, recovered, sync_handle))
}
