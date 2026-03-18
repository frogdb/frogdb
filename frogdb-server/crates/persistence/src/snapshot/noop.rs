//! No-op snapshot coordinator.
use super::handle::SnapshotHandle;
use super::metadata::SnapshotMetadata;
use super::{SnapshotCoordinator, SnapshotError};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

pub struct NoopSnapshotCoordinator {
    last_save: RwLock<Option<Instant>>,
    in_progress: AtomicBool,
    epoch: AtomicU64,
    scheduled: AtomicBool,
}
impl Default for NoopSnapshotCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
impl NoopSnapshotCoordinator {
    pub fn new() -> Self {
        Self {
            last_save: RwLock::new(None),
            in_progress: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            scheduled: AtomicBool::new(false),
        }
    }
}
impl SnapshotCoordinator for NoopSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SnapshotError::AlreadyInProgress);
        }
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_save.write().unwrap() = Some(Instant::now());
        let in_progress = &self.in_progress as *const AtomicBool as usize;
        let handle = SnapshotHandle::new(epoch, move || {
            let ip = unsafe { &*(in_progress as *const AtomicBool) };
            ip.store(false, Ordering::SeqCst);
        });
        tracing::info!(
            epoch = epoch,
            "Noop snapshot started (no actual data saved)"
        );
        Ok(handle)
    }
    fn last_save_time(&self) -> Option<Instant> {
        *self.last_save.read().unwrap()
    }
    fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        let ls = self.last_save_time()?;
        let now = Instant::now();
        let sn = SystemTime::now();
        let sa = if ls <= now { sn - (now - ls) } else { sn };
        Some(SnapshotMetadata {
            epoch: self.epoch.load(Ordering::SeqCst),
            started_at: sa,
            completed_at: Some(sa),
            num_keys: 0,
            size_bytes: 0,
        })
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
