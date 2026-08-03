//! RocksDB-backed snapshot coordinator using the Checkpoint API.
use super::handle::SnapshotHandle;
use super::metadata::{SnapshotConfig, SnapshotMetadataFile};
use super::scheduler::SnapshotScheduler;
use super::stager::SnapshotStager;
use super::{
    SaveHistory, SnapshotCoordinator, SnapshotError, SnapshotMode, SnapshotRequest, SnapshotStats,
};
use crate::rocks::RocksStore;
use frogdb_types::metrics::definitions::{
    PersistenceErrors, SnapshotDuration, SnapshotEpoch, SnapshotInProgress, SnapshotLastTimestamp,
    SnapshotSizeBytes,
};
use frogdb_types::metrics::labels::PersistenceErrorType;
use frogdb_types::traits::MetricsRecorder;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinError;
use tracing::Instrument;
/// Work that must complete before a checkpoint is cut, injected by the owner of
/// the shards (see `checkpoint_quiesce` in the server crate).
///
/// It returns a result because the work it does — draining every shard's WAL
/// flush engine — is what makes the artifact contain every acknowledged write.
/// A hook that cannot finish means the checkpoint would be silently incomplete,
/// so the save is failed instead of cut: the run reports `err` through
/// [`SnapshotStats`](super::SnapshotStats), leaves `LASTSAVE` alone, and leaves
/// the previous snapshot as the newest one on disk (issue 05).
pub type PreSnapshotHook =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), SnapshotError>> + Send>> + Send + Sync>;
pub struct RocksSnapshotCoordinator {
    rocks_store: Arc<RocksStore>,
    snapshot_dir: PathBuf,
    num_shards: usize,
    scheduler: Arc<SnapshotScheduler>,
    stats: Arc<SaveHistory>,
    max_snapshots: usize,
    metrics_recorder: Arc<dyn MetricsRecorder>,
    pre_snapshot_hook: Arc<RwLock<Option<PreSnapshotHook>>>,
    data_dir: PathBuf,
}
impl RocksSnapshotCoordinator {
    pub fn new(
        rs: Arc<RocksStore>,
        config: SnapshotConfig,
        mr: Arc<dyn MetricsRecorder>,
        data_dir: PathBuf,
    ) -> Result<Self, SnapshotError> {
        std::fs::create_dir_all(&config.snapshot_dir)?;
        let ns = rs.num_shards();
        // An unreadable `latest`/`metadata.json` — absent, zero-length, garbage,
        // whatever a power loss left behind — must not collapse the epoch to 0
        // Epoch 0 makes the next `BGSAVE` reuse
        // `snapshot_00001`, whose `final_dir` already exists, and it makes the
        // retention pass reason about a snapshot set it has mis-numbered. So the
        // failure is logged, not swallowed, and the seed falls back to the
        // highest epoch actually on disk.
        let (mut ie, lm) = match Self::load_latest_metadata(&config.snapshot_dir) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    snapshot_dir = %config.snapshot_dir.display(),
                    "Failed to read the latest snapshot metadata; \
                     seeding the epoch from the snapshot directories on disk"
                );
                (0, None)
            }
        };
        ie = ie.max(Self::highest_snapshot_epoch(&config.snapshot_dir));
        // Seed the last-save time from the newest complete snapshot's *own*
        // recorded completion time, not from "now": the artifact on disk may be
        // days old, and a boot is not a save. A complete snapshot with no
        // `completed_at_ms` (a hand-edited or pre-versioned metadata file) seeds
        // `None` — "unknown" is reported as "never saved", never as "just now".
        let stats = SnapshotStats {
            last_save_time: lm.as_ref().and_then(|m| m.completed_at()),
            ..SnapshotStats::default()
        };
        let scheduler = Arc::new(SnapshotScheduler::with_epoch(ie));
        // Seed the live periodic cadence from config. The periodic task reads it
        // back off the scheduler each cycle instead of capturing it at spawn.
        scheduler.set_periodic_interval_secs(config.snapshot_interval_secs);
        Ok(Self {
            rocks_store: rs,
            snapshot_dir: config.snapshot_dir,
            num_shards: ns,
            scheduler,
            stats: Arc::new(SaveHistory::new(stats)),
            max_snapshots: config.max_snapshots,
            metrics_recorder: mr,
            pre_snapshot_hook: Arc::new(RwLock::new(None)),
            data_dir,
        })
    }
    /// The epoch the next save will advance from — seeded at boot from the
    /// newest snapshot on disk, then incremented per save.
    pub fn current_epoch(&self) -> u64 {
        self.scheduler.current_epoch()
    }
    pub fn set_pre_snapshot_hook(&self, hook: PreSnapshotHook) {
        *self.pre_snapshot_hook.write().unwrap() = Some(hook);
    }
    /// Highest `snapshot_NNNNN` epoch present under `sd`, or 0 if there are
    /// none. The floor for the boot epoch seed: a snapshot directory on disk is
    /// proof that epoch ran, whatever `latest` and `metadata.json` say. Ignores
    /// unreadable directories and unparsable names rather than failing — this is
    /// the fallback path, and it may only ever raise the seed.
    pub(crate) fn highest_snapshot_epoch(sd: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(sd) else {
            return 0;
        };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .strip_prefix("snapshot_")
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0)
    }

    fn load_latest_metadata(
        sd: &std::path::Path,
    ) -> Result<(u64, Option<SnapshotMetadataFile>), SnapshotError> {
        let ll = sd.join("latest");
        if !ll.exists() {
            return Ok((0, None));
        }
        let target = std::fs::read_link(&ll)?;
        let mp = if target.is_absolute() {
            target.join("metadata.json")
        } else {
            sd.join(target).join("metadata.json")
        };
        if !mp.exists() {
            return Ok((0, None));
        }
        let c = std::fs::read_to_string(&mp)?;
        let m: SnapshotMetadataFile = serde_json::from_str(&c)
            .map_err(|e| SnapshotError::Internal(format!("Failed to parse metadata: {}", e)))?;
        if !m.is_complete() {
            return Ok((m.epoch, None));
        }
        Ok((m.epoch, Some(m)))
    }
}
impl RocksSnapshotCoordinator {
    /// Emit the "started" metrics/log for `epoch` and spawn the background
    /// [`run_loop`]. Called once the scheduler has already claimed the slot for
    /// `epoch` (via `try_begin` / `request`).
    fn spawn_run(&self, epoch: u64) {
        // Stamp the start *here*, not inside the spawned task: the slot is
        // already claimed, so `in_progress()` is true from this point on and
        // `rdb_current_bgsave_time_sec` must not read `-1` in the window before
        // the runtime polls the task.
        let started = self.stats.record_start();
        SnapshotInProgress::set(&*self.metrics_recorder, 1.0);
        SnapshotEpoch::set(&*self.metrics_recorder, epoch as f64);
        tracing::info!(epoch, "Snapshot started");
        let run = SnapshotRun {
            scheduler: self.scheduler.clone(),
            rocks_store: self.rocks_store.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
            data_dir: self.data_dir.clone(),
            stats: self.stats.clone(),
            metrics: self.metrics_recorder.clone(),
            pre_snapshot_hook: self.pre_snapshot_hook.clone(),
            num_shards: self.num_shards,
            max_snapshots: self.max_snapshots,
        };
        tokio::spawn(
            run_loop(run, epoch, started).instrument(tracing::info_span!("snapshot_create")),
        );
    }
}
impl SnapshotCoordinator for RocksSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        let epoch = self
            .scheduler
            .try_begin()
            .ok_or(SnapshotError::AlreadyInProgress)?;
        self.spawn_run(epoch);
        Ok(SnapshotHandle::new(epoch))
    }
    fn stats(&self) -> SnapshotStats {
        self.stats.snapshot()
    }
    fn last_save_failed(&self) -> bool {
        self.stats.last_save_failed()
    }
    fn in_progress(&self) -> bool {
        self.scheduler.in_progress()
    }
    fn request_snapshot(&self, mode: SnapshotMode) -> SnapshotRequest {
        match self.scheduler.request_mode(mode) {
            SnapshotRequest::Started(epoch) => {
                self.spawn_run(epoch);
                SnapshotRequest::Started(epoch)
            }
            other => other,
        }
    }
    fn periodic_interval_secs(&self) -> u64 {
        self.scheduler.periodic_interval_secs()
    }
    fn set_periodic_interval_secs(&self, secs: u64) {
        self.scheduler.set_periodic_interval_secs(secs);
    }
}

/// Everything one background save needs, with real field names (replaces the
/// twelve two-letter move-captures of the old inline `tokio::spawn` closure).
struct SnapshotRun {
    scheduler: Arc<SnapshotScheduler>,
    rocks_store: Arc<RocksStore>,
    snapshot_dir: PathBuf,
    data_dir: PathBuf,
    stats: Arc<SaveHistory>,
    metrics: Arc<dyn MetricsRecorder>,
    pre_snapshot_hook: Arc<RwLock<Option<PreSnapshotHook>>>,
    num_shards: usize,
    max_snapshots: usize,
}
impl SnapshotRun {
    /// Run the pre-snapshot hook (if any), then stage + install one checkpoint on
    /// a blocking thread. Returns the joined stager result.
    async fn execute(
        &self,
        epoch: u64,
    ) -> Result<Result<SnapshotMetadataFile, SnapshotError>, JoinError> {
        // Clone out of the lock into a local first, so the read guard is dropped
        // before the `.await` (guards are not `Send`).
        let hook = self.pre_snapshot_hook.read().unwrap().clone();
        if let Some(hook) = hook
            && let Err(e) = hook().await
        {
            // The hook is what makes the cut contain every acknowledged write;
            // without it the artifact is a silently incomplete one. Report the
            // save as failed and cut nothing, so the newest snapshot on disk
            // stays the last known-good one.
            return Ok(Err(e));
        }
        let snapshot_dir = self.snapshot_dir.clone();
        let data_dir = self.data_dir.clone();
        let rocks_store = self.rocks_store.clone();
        let num_shards = self.num_shards;
        let max_snapshots = self.max_snapshots;
        tokio::task::spawn_blocking(move || {
            SnapshotStager {
                tmp: snapshot_dir.join(format!(".snapshot_{epoch:05}.tmp")),
                final_dir: snapshot_dir.join(format!("snapshot_{epoch:05}")),
                name: format!("snapshot_{epoch:05}"),
                snapshot_dir,
                data_dir,
                epoch,
                num_shards,
                max_snapshots,
                fs: Arc::new(crate::fs_seam::RealFs),
            }
            .run(&rocks_store)
        })
        .await
    }

    /// Record the outcome of one save: metrics, `last_*` state, and the log line.
    fn record(
        &self,
        epoch: u64,
        started: Instant,
        result: Result<Result<SnapshotMetadataFile, SnapshotError>, JoinError>,
    ) {
        match result {
            Ok(Ok(md)) => {
                let elapsed = started.elapsed();
                let sequence = md.sequence_number;
                let path = self.snapshot_dir.join(format!("snapshot_{epoch:05}"));
                // The artifact's own recorded completion time, so the value
                // reported now is the same one a later boot reads back out of
                // `metadata.json` (a metadata file with no completion time is
                // not produced by `mark_complete`; fall back to now rather than
                // dropping the save's timestamp).
                let completed_at = md.completed_at().unwrap_or_else(SystemTime::now);
                self.stats.record_success(completed_at, elapsed);
                SnapshotDuration::observe(&*self.metrics, elapsed.as_secs_f64());
                SnapshotSizeBytes::set(&*self.metrics, md.size_bytes as f64);
                SnapshotLastTimestamp::set(
                    &*self.metrics,
                    completed_at
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64(),
                );
                tracing::info!(
                    epoch,
                    sequence,
                    path = %path.display(),
                    size_bytes = md.size_bytes,
                    duration_ms = elapsed.as_millis(),
                    "Snapshot completed"
                );
            }
            Ok(Err(e)) => {
                PersistenceErrors::inc(&*self.metrics, PersistenceErrorType::Snapshot);
                self.stats.record_failure(e.to_string());
                tracing::error!(epoch, error = %e, "Snapshot failed");
            }
            Err(e) => {
                PersistenceErrors::inc(&*self.metrics, PersistenceErrorType::Snapshot);
                self.stats
                    .record_failure(format!("snapshot task panicked: {e}"));
                tracing::error!(epoch, error = %e, "Snapshot task panicked");
            }
        }
    }
}

/// One background save, then coalesced re-runs, until the scheduler reports idle.
/// The reschedule handshake lives entirely in
/// [`SnapshotScheduler::finish_and_maybe_rebegin`].
/// `started` is the instant [`RocksSnapshotCoordinator::spawn_run`] published as
/// this run's start; each coalesced follow-up re-stamps it, so the duration this
/// loop measures and the one `INFO` reports as in-flight are the same window.
async fn run_loop(run: SnapshotRun, mut epoch: u64, mut started: Instant) {
    loop {
        let result = run.execute(epoch).await;
        run.record(epoch, started, result);

        match run.scheduler.finish_and_maybe_rebegin() {
            None => {
                SnapshotInProgress::set(&*run.metrics, 0.0);
                break;
            }
            Some(next) => {
                epoch = next;
                started = run.stats.record_start();
                SnapshotEpoch::set(&*run.metrics, epoch as f64);
                tracing::info!(epoch, "Starting scheduled snapshot");
            }
        }
    }
}
