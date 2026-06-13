//! Server component initialization helpers.

use frogdb_core::persistence::{RocksStore, SnapshotCoordinator, spawn_periodic_sync};
use frogdb_core::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::config::PersistenceConfig;
use crate::net::{JoinHandle, spawn};

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

/// Spawn the periodic WAL sync task when persistence is enabled and the
/// durability mode is `periodic`.
///
/// This is a runtime concern, deliberately kept out of the synchronous recovery
/// seam (`crate::recovery`): recovery returns opened handles and plain data, and
/// the caller decides what background tasks to spawn afterwards. When `rocks` is
/// `None` (persistence disabled) or the mode is not `periodic`, no task is
/// spawned.
pub fn spawn_wal_sync_if_periodic(
    config: &PersistenceConfig,
    rocks: &Option<Arc<RocksStore>>,
    wal_sync_monitor: Option<tokio_metrics::TaskMonitor>,
) -> Option<JoinHandle<()>> {
    let rocks = rocks.as_ref()?;

    if config.durability_mode.to_lowercase() == "periodic" {
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
    }
}
