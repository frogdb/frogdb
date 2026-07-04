//! RocksDB-backed snapshot coordinator using the Checkpoint API.
use super::handle::SnapshotHandle;
use super::metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
use super::stager::SnapshotStager;
use super::{SnapshotCoordinator, SnapshotError};
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::Instrument;
pub type PreSnapshotHook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
pub struct RocksSnapshotCoordinator {
    rocks_store: Arc<RocksStore>,
    snapshot_dir: PathBuf,
    num_shards: usize,
    epoch: Arc<AtomicU64>,
    in_progress: Arc<AtomicBool>,
    scheduled: Arc<AtomicBool>,
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
            epoch: Arc::new(AtomicU64::new(ie)),
            in_progress: Arc::new(AtomicBool::new(false)),
            scheduled: Arc::new(AtomicBool::new(false)),
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
impl SnapshotCoordinator for RocksSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SnapshotError::AlreadyInProgress);
        }
        let ie = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        SnapshotInProgress::set(&*self.metrics_recorder, 1.0);
        SnapshotEpoch::set(&*self.metrics_recorder, ie as f64);
        tracing::info!(epoch = ie, "Snapshot started");
        let rs = self.rocks_store.clone();
        let sd = self.snapshot_dir.clone();
        let ip = self.in_progress.clone();
        let sc = self.scheduled.clone();
        let ec = self.epoch.clone();
        let lst = self.last_save_time.clone();
        let lm = self.last_metadata.clone();
        let mt = self.metrics_recorder.clone();
        let ms = self.max_snapshots;
        let ns = self.num_shards;
        let psh = self.pre_snapshot_hook.clone();
        let dd = self.data_dir.clone();
        tokio::spawn(async move { let mut ce = ie; let mut start = Instant::now(); loop {
            { let h = psh.read().unwrap().clone(); if let Some(h) = h { h().await; } }
            let rs2 = rs.clone(); let sd2 = sd.clone(); let dd2 = dd.clone(); let se = ce;
            let result = tokio::task::spawn_blocking(move || {
                SnapshotStager {
                    tmp: sd2.join(format!(".snapshot_{:05}.tmp", se)),
                    final_dir: sd2.join(format!("snapshot_{:05}", se)),
                    name: format!("snapshot_{:05}", se),
                    snapshot_dir: sd2,
                    data_dir: dd2,
                    epoch: se,
                    num_shards: ns,
                    max_snapshots: ms,
                }
                .run(&rs2)
            }).await;
            match result { Ok(Ok(md)) => { let el = start.elapsed(); let seq = md.sequence_number; let sp = sd.join(format!("snapshot_{:05}", ce)); *lst.write().unwrap() = Some(Instant::now()); *lm.write().unwrap() = Some(md.clone()); SnapshotDuration::observe(&*mt, el.as_secs_f64()); SnapshotSizeBytes::set(&*mt, md.size_bytes as f64); SnapshotLastTimestamp::set(&*mt, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64()); tracing::info!(epoch = ce, sequence = seq, path = %sp.display(), size_bytes = md.size_bytes, duration_ms = el.as_millis(), "Snapshot completed"); } Ok(Err(e)) => { PersistenceErrors::inc(&*mt, PersistenceErrorType::Snapshot); tracing::error!(epoch = ce, error = %e, "Snapshot failed"); } Err(e) => { PersistenceErrors::inc(&*mt, PersistenceErrorType::Snapshot); tracing::error!(epoch = ce, error = %e, "Snapshot task panicked"); } }
            ip.store(false, Ordering::SeqCst);
            if !sc.swap(false, Ordering::SeqCst) { SnapshotInProgress::set(&*mt, 0.0); break; }
            if ip.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() { SnapshotInProgress::set(&*mt, 0.0); break; }
            ce = ec.fetch_add(1, Ordering::SeqCst) + 1; start = Instant::now(); SnapshotEpoch::set(&*mt, ce as f64); tracing::info!(epoch = ce, "Starting scheduled snapshot");
        } }.instrument(tracing::info_span!("snapshot_create")));
        Ok(SnapshotHandle::new(ie, || {}))
    }
    fn last_save_time(&self) -> Option<Instant> {
        *self.last_save_time.read().unwrap()
    }
    fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_metadata
            .read()
            .unwrap()
            .as_ref()
            .map(|m| m.to_metadata())
    }
    fn schedule_snapshot(&self) -> bool {
        if !self.in_progress() {
            return false;
        }
        self.scheduled.store(true, Ordering::SeqCst);
        true
    }
    fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::SeqCst)
    }
}
