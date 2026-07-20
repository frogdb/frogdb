//! RocksDB-backed snapshot coordinator using the Checkpoint API.
use super::handle::SnapshotHandle;
use super::metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
use super::scheduler::SnapshotScheduler;
use super::stager::SnapshotStager;
use super::{SnapshotCoordinator, SnapshotError, SnapshotRequest};
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
pub type PreSnapshotHook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
pub struct RocksSnapshotCoordinator {
    rocks_store: Arc<RocksStore>,
    snapshot_dir: PathBuf,
    num_shards: usize,
    scheduler: Arc<SnapshotScheduler>,
    last_save_time: Arc<RwLock<Option<Instant>>>,
    last_metadata: Arc<RwLock<Option<SnapshotMetadataFile>>>,
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
        let (ie, lm) = Self::load_latest_metadata(&config.snapshot_dir).unwrap_or((0, None));
        let lst = if lm.is_some() {
            Some(Instant::now())
        } else {
            None
        };
        Ok(Self {
            rocks_store: rs,
            snapshot_dir: config.snapshot_dir,
            num_shards: ns,
            scheduler: Arc::new(SnapshotScheduler::with_epoch(ie)),
            last_save_time: Arc::new(RwLock::new(lst)),
            last_metadata: Arc::new(RwLock::new(lm)),
            max_snapshots: config.max_snapshots,
            metrics_recorder: mr,
            pre_snapshot_hook: Arc::new(RwLock::new(None)),
            data_dir,
        })
    }
    pub fn set_pre_snapshot_hook(&self, hook: PreSnapshotHook) {
        *self.pre_snapshot_hook.write().unwrap() = Some(hook);
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
        SnapshotInProgress::set(&*self.metrics_recorder, 1.0);
        SnapshotEpoch::set(&*self.metrics_recorder, epoch as f64);
        tracing::info!(epoch, "Snapshot started");
        let run = SnapshotRun {
            scheduler: self.scheduler.clone(),
            rocks_store: self.rocks_store.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
            data_dir: self.data_dir.clone(),
            last_save_time: self.last_save_time.clone(),
            last_metadata: self.last_metadata.clone(),
            metrics: self.metrics_recorder.clone(),
            pre_snapshot_hook: self.pre_snapshot_hook.clone(),
            num_shards: self.num_shards,
            max_snapshots: self.max_snapshots,
        };
        tokio::spawn(run_loop(run, epoch).instrument(tracing::info_span!("snapshot_create")));
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
    fn last_save_time(&self) -> Option<Instant> {
        *self.last_save_time.read().unwrap()
    }
    fn in_progress(&self) -> bool {
        self.scheduler.in_progress()
    }
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_metadata
            .read()
            .unwrap()
            .as_ref()
            .map(|m| m.to_metadata())
    }
    fn schedule_snapshot(&self) -> bool {
        self.scheduler.schedule()
    }
    fn is_scheduled(&self) -> bool {
        self.scheduler.is_scheduled()
    }
    fn request_snapshot(&self) -> SnapshotRequest {
        match self.scheduler.request() {
            SnapshotRequest::Started(epoch) => {
                self.spawn_run(epoch);
                SnapshotRequest::Started(epoch)
            }
            SnapshotRequest::Coalesced => SnapshotRequest::Coalesced,
        }
    }
}

/// Everything one background save needs, with real field names (replaces the
/// twelve two-letter move-captures of the old inline `tokio::spawn` closure).
struct SnapshotRun {
    scheduler: Arc<SnapshotScheduler>,
    rocks_store: Arc<RocksStore>,
    snapshot_dir: PathBuf,
    data_dir: PathBuf,
    last_save_time: Arc<RwLock<Option<Instant>>>,
    last_metadata: Arc<RwLock<Option<SnapshotMetadataFile>>>,
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
        if let Some(hook) = hook {
            hook().await;
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
                *self.last_save_time.write().unwrap() = Some(Instant::now());
                *self.last_metadata.write().unwrap() = Some(md.clone());
                SnapshotDuration::observe(&*self.metrics, elapsed.as_secs_f64());
                SnapshotSizeBytes::set(&*self.metrics, md.size_bytes as f64);
                SnapshotLastTimestamp::set(
                    &*self.metrics,
                    SystemTime::now()
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
                tracing::error!(epoch, error = %e, "Snapshot failed");
            }
            Err(e) => {
                PersistenceErrors::inc(&*self.metrics, PersistenceErrorType::Snapshot);
                tracing::error!(epoch, error = %e, "Snapshot task panicked");
            }
        }
    }
}

/// One background save, then coalesced re-runs, until the scheduler reports idle.
/// The reschedule handshake lives entirely in
/// [`SnapshotScheduler::finish_and_maybe_rebegin`].
async fn run_loop(run: SnapshotRun, mut epoch: u64) {
    loop {
        let started = Instant::now();
        let result = run.execute(epoch).await;
        run.record(epoch, started, result);

        match run.scheduler.finish_and_maybe_rebegin() {
            None => {
                SnapshotInProgress::set(&*run.metrics, 0.0);
                break;
            }
            Some(next) => {
                epoch = next;
                SnapshotEpoch::set(&*run.metrics, epoch as f64);
                tracing::info!(epoch, "Starting scheduled snapshot");
            }
        }
    }
}
