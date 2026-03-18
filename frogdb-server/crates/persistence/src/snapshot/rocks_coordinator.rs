//! RocksDB-backed snapshot coordinator using the Checkpoint API.
use super::handle::SnapshotHandle;
use super::metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
use super::{SnapshotCoordinator, SnapshotError};
use crate::rocks::RocksStore;
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
    fn calculate_dir_size(p: &std::path::Path) -> std::io::Result<u64> {
        let mut s = 0;
        if p.is_dir() {
            for e in std::fs::read_dir(p)? {
                let e = e?;
                let m = e.metadata()?;
                if m.is_dir() {
                    s += Self::calculate_dir_size(&e.path())?;
                } else {
                    s += m.len();
                }
            }
        }
        Ok(s)
    }
    fn copy_search_indexes(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
        for ie in std::fs::read_dir(src)? {
            let ie = ie?;
            if !ie.file_type()?.is_dir() {
                continue;
            }
            let in_ = ie.file_name();
            for se in std::fs::read_dir(ie.path())? {
                let se = se?;
                if !se.file_type()?.is_dir() {
                    continue;
                }
                let sd = dst.join(&in_).join(se.file_name());
                std::fs::create_dir_all(&sd)?;
                for fe in std::fs::read_dir(se.path())? {
                    let fe = fe?;
                    if !fe.file_type()?.is_file() {
                        continue;
                    }
                    let fn_ = fe.file_name();
                    let sp = fe.path();
                    let dp = sd.join(&fn_);
                    let ns = fn_.to_string_lossy();
                    if ns == "meta.json"
                        || ns.starts_with('.')
                        || std::fs::hard_link(&sp, &dp).is_err()
                    {
                        std::fs::copy(&sp, &dp)?;
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn cleanup_old_snapshots(
        sd: &std::path::Path,
        ms: usize,
    ) -> Result<(), SnapshotError> {
        if ms == 0 {
            return Ok(());
        }
        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        for e in std::fs::read_dir(sd)? {
            let e = e?;
            let n = e.file_name();
            let ns = n.to_string_lossy();
            if ns.starts_with("snapshot_")
                && e.file_type()?.is_dir()
                && let Some(es) = ns.strip_prefix("snapshot_")
                && let Ok(ep) = es.parse::<u64>()
            {
                entries.push((ep, e.path()));
            }
        }
        if entries.len() <= ms {
            return Ok(());
        }
        entries.sort_by_key(|(ep, _)| *ep);
        let dc = entries.len() - ms;
        for (ep, p) in entries.into_iter().take(dc) {
            tracing::info!(epoch = ep, path = %p.display(), "Deleting old snapshot");
            if let Err(e) = std::fs::remove_dir_all(&p) {
                tracing::warn!(epoch = ep, error = %e, "Failed to delete old snapshot");
            }
        }
        Ok(())
    }
    #[cfg(unix)]
    fn update_latest_symlink(sd: &std::path::Path, sn: &str) -> Result<(), SnapshotError> {
        let ll = sd.join("latest");
        let tl = sd.join(".latest.tmp");
        let _ = std::fs::remove_file(&tl);
        std::os::unix::fs::symlink(sn, &tl)?;
        std::fs::rename(&tl, &ll)?;
        Ok(())
    }
    #[cfg(not(unix))]
    fn update_latest_symlink(sd: &std::path::Path, sn: &str) -> Result<(), SnapshotError> {
        std::fs::write(sd.join("latest"), sn)?;
        Ok(())
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
        self.metrics_recorder
            .record_gauge("frogdb_snapshot_in_progress", 1.0, &[]);
        self.metrics_recorder
            .record_gauge("frogdb_snapshot_epoch", ie as f64, &[]);
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
                let sn = format!("snapshot_{:05}", se); let td = sd2.join(format!(".snapshot_{:05}.tmp", se)); let fd = sd2.join(&sn); let cp = td.join("checkpoint");
                if let Err(e) = std::fs::create_dir_all(&cp) { return Err(SnapshotError::Io(e)); }
                let seq = rs2.latest_sequence_number();
                if let Err(e) = rs2.create_checkpoint(&cp) { let _ = std::fs::remove_dir_all(&td); return Err(SnapshotError::Internal(format!("Failed to create checkpoint: {}", e))); }
                let ss = dd2.join("search"); if ss.exists() && let Err(e) = Self::copy_search_indexes(&ss, &td.join("search")) { tracing::warn!(error = %e, "Failed to copy search indexes to snapshot"); }
                let mut md = SnapshotMetadataFile::new(se, seq, ns); let mut sb = Self::calculate_dir_size(&cp).unwrap_or(0); let ssp = td.join("search"); if ssp.exists() { sb += Self::calculate_dir_size(&ssp).unwrap_or(0); } md.mark_complete(0, sb);
                let mj = serde_json::to_string_pretty(&md).map_err(|e| SnapshotError::Internal(format!("Failed to serialize metadata: {}", e)))?;
                let mtp = td.join("metadata.json.tmp"); std::fs::write(&mtp, &mj)?; std::fs::rename(&mtp, td.join("metadata.json"))?; std::fs::rename(&td, &fd)?;
                if let Err(e) = Self::update_latest_symlink(&sd2, &sn) { tracing::warn!(error = %e, "Failed to update latest symlink"); }
                if let Err(e) = Self::cleanup_old_snapshots(&sd2, ms) { tracing::warn!(error = %e, "Failed to cleanup old snapshots"); }
                Ok((md, seq, fd))
            }).await;
            match result { Ok(Ok((md, seq, sp))) => { let el = start.elapsed(); *lst.write().unwrap() = Some(Instant::now()); *lm.write().unwrap() = Some(md.clone()); mt.record_histogram("frogdb_snapshot_duration_seconds", el.as_secs_f64(), &[]); mt.record_gauge("frogdb_snapshot_size_bytes", md.size_bytes as f64, &[]); mt.record_gauge("frogdb_snapshot_last_timestamp", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64(), &[]); tracing::info!(epoch = ce, sequence = seq, path = %sp.display(), size_bytes = md.size_bytes, duration_ms = el.as_millis(), "Snapshot completed"); } Ok(Err(e)) => { mt.increment_counter("frogdb_persistence_errors_total", 1, &[("type", "snapshot")]); tracing::error!(epoch = ce, error = %e, "Snapshot failed"); } Err(e) => { mt.increment_counter("frogdb_persistence_errors_total", 1, &[("type", "snapshot")]); tracing::error!(epoch = ce, error = %e, "Snapshot task panicked"); } }
            ip.store(false, Ordering::SeqCst);
            if !sc.swap(false, Ordering::SeqCst) { mt.record_gauge("frogdb_snapshot_in_progress", 0.0, &[]); break; }
            if ip.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() { mt.record_gauge("frogdb_snapshot_in_progress", 0.0, &[]); break; }
            ce = ec.fetch_add(1, Ordering::SeqCst) + 1; start = Instant::now(); mt.record_gauge("frogdb_snapshot_epoch", ce as f64, &[]); tracing::info!(epoch = ce, "Starting scheduled snapshot");
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
