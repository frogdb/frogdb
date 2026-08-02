//! Server component initialization helpers.

use frogdb_core::persistence::{RocksStore, SnapshotCoordinator, spawn_periodic_sync};
use frogdb_core::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::config::PersistenceConfig;
use crate::net::{JoinHandle, spawn};

/// How long the periodic-snapshot loop waits between re-reads of the live
/// cadence. Bounding each sleep at this slice is what makes
/// `snapshot-interval-secs` live: a `CONFIG SET` that shortens the interval (or
/// re-arms it from 0) takes effect within one slice instead of waiting out the
/// old — possibly hour-long — period.
const SNAPSHOT_INTERVAL_POLL: Duration = Duration::from_secs(1);

/// Spawn the periodic snapshot task.
///
/// The cadence is **not** captured at spawn time: the loop re-reads
/// [`SnapshotCoordinator::periodic_interval_secs`] before every scheduling
/// decision, so `CONFIG SET snapshot-interval-secs` retunes the task in place.
/// A cadence of 0 means "periodic saves disabled" — the loop idles (still
/// polling) rather than exiting, so a later non-zero value re-arms it without a
/// restart. Because of that the task is spawned unconditionally whenever a real
/// snapshot coordinator exists.
///
/// Timing semantics: the first save of an armed cadence fires immediately (the
/// historical `tokio::time::interval` first-tick behaviour), and subsequent
/// saves fire `interval_secs` after the previous save *started*. Shortening the
/// interval is evaluated against that same previous-start instant, so a save
/// already overdue under the new value fires on the next poll.
pub fn spawn_periodic_snapshot_task(coordinator: Arc<dyn SnapshotCoordinator>) -> JoinHandle<()> {
    info!(
        interval_secs = coordinator.periodic_interval_secs(),
        "Starting periodic snapshot task"
    );

    spawn(async move {
        // `None` = due immediately (boot, or re-arming from a disabled cadence).
        let mut last_start: Option<tokio::time::Instant> = None;

        loop {
            let interval_secs = coordinator.periodic_interval_secs();
            if interval_secs == 0 {
                // Disabled. Idle at the poll cadence so a later CONFIG SET
                // re-arms this task instead of requiring a restart.
                last_start = None;
                tokio::time::sleep(SNAPSHOT_INTERVAL_POLL).await;
                continue;
            }
            if let Some(prev) = last_start {
                let due = prev + Duration::from_secs(interval_secs);
                let now = tokio::time::Instant::now();
                if now < due {
                    tokio::time::sleep(SNAPSHOT_INTERVAL_POLL.min(due - now)).await;
                    continue;
                }
            }
            last_start = Some(tokio::time::Instant::now());

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
/// seam (`frogdb_recovery`): recovery returns opened handles and plain data, and
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

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::persistence::{
        SnapshotError, SnapshotHandle, SnapshotMode, SnapshotRequest, SnapshotScheduler,
        SnapshotStats,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Minimal coordinator that records how many periodic saves were started and
    /// carries the live cadence on a real [`SnapshotScheduler`] (the same seam the
    /// production coordinators delegate to).
    struct CountingCoordinator {
        scheduler: SnapshotScheduler,
        starts: AtomicU64,
    }

    impl CountingCoordinator {
        fn new(interval_secs: u64) -> Self {
            let scheduler = SnapshotScheduler::with_epoch(0);
            scheduler.set_periodic_interval_secs(interval_secs);
            Self {
                scheduler,
                starts: AtomicU64::new(0),
            }
        }
        fn starts(&self) -> u64 {
            self.starts.load(Ordering::SeqCst)
        }
    }

    impl SnapshotCoordinator for CountingCoordinator {
        fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
            Ok(SnapshotHandle::new(
                self.starts.fetch_add(1, Ordering::SeqCst) + 1,
            ))
        }
        fn stats(&self) -> SnapshotStats {
            SnapshotStats::default()
        }
        /// This double never fails a save (it never runs one), so it matches its
        /// own default `stats()`, whose `last_error` is `None`.
        fn last_save_failed(&self) -> bool {
            false
        }
        fn in_progress(&self) -> bool {
            false
        }
        fn request_snapshot(&self, _mode: SnapshotMode) -> SnapshotRequest {
            SnapshotRequest::Coalesced
        }
        fn periodic_interval_secs(&self) -> u64 {
            self.scheduler.periodic_interval_secs()
        }
        fn set_periodic_interval_secs(&self, secs: u64) {
            self.scheduler.set_periodic_interval_secs(secs);
        }
    }

    /// Step simulated time forward in poll-sized slices (yielding between them so
    /// the periodic task actually runs) until `cond` holds or `max_slices` are
    /// spent. Returns whether it held. Slicing keeps the test independent of how
    /// far tokio's auto-advance jumps: every step is bounded by one poll.
    async fn advance_until(max_slices: u32, cond: impl Fn() -> bool) -> bool {
        for _ in 0..max_slices {
            if cond() {
                return true;
            }
            tokio::time::advance(SNAPSHOT_INTERVAL_POLL).await;
            tokio::task::yield_now().await;
        }
        cond()
    }

    /// Propagation truth: storing a new cadence on the live coordinator retunes
    /// the *already running* periodic task. Booted disabled (0), the task never
    /// saves; `set_periodic_interval_secs(60)` arms it without a restart, and a
    /// later store back to 0 disarms it again — all observed by one task that is
    /// never respawned.
    #[tokio::test(start_paused = true)]
    async fn periodic_snapshot_task_rearms_on_live_interval_change() {
        let coord = Arc::new(CountingCoordinator::new(0));
        let task = spawn_periodic_snapshot_task(coord.clone() as Arc<dyn SnapshotCoordinator>);

        // Disabled cadence: minutes of simulated time produce no saves.
        let c = coord.clone();
        assert!(
            !advance_until(120, move || c.starts() > 0).await,
            "a 0 cadence must not fire saves"
        );

        // Live arm — no restart, no respawn.
        coord.set_periodic_interval_secs(60);
        let c = coord.clone();
        assert!(
            advance_until(5, move || c.starts() >= 1).await,
            "arming the cadence must start a save on the running task"
        );

        // And it keeps firing at the new cadence.
        let c = coord.clone();
        assert!(
            advance_until(120, move || c.starts() >= 2).await,
            "the armed cadence must keep firing"
        );

        // Live disarm: no further saves.
        coord.set_periodic_interval_secs(0);
        let before = coord.starts();
        let c = coord.clone();
        assert!(
            !advance_until(300, move || c.starts() > before).await,
            "a 0 cadence must disarm the running task"
        );

        task.abort();
    }

    /// Shortening the interval is evaluated against the *previous* save, so a
    /// save already overdue under the new cadence fires promptly instead of
    /// waiting out the old (here: hour-long) period.
    #[tokio::test(start_paused = true)]
    async fn periodic_snapshot_task_shortened_interval_fires_early() {
        let coord = Arc::new(CountingCoordinator::new(3600));
        let task = spawn_periodic_snapshot_task(coord.clone() as Arc<dyn SnapshotCoordinator>);

        // Armed at boot: the first save fires immediately, the second is an hour out.
        let c = coord.clone();
        assert!(advance_until(5, move || c.starts() >= 1).await);
        let c = coord.clone();
        assert!(
            !advance_until(300, move || c.starts() >= 2).await,
            "still inside the 3600s period"
        );

        // CONFIG SET snapshot-interval-secs 60 — well over 60s have already
        // elapsed since the last save, so the next one is immediately overdue.
        coord.set_periodic_interval_secs(60);
        let c = coord.clone();
        assert!(
            advance_until(5, move || c.starts() >= 2).await,
            "a shortened interval must not wait out the old period"
        );

        task.abort();
    }
}
