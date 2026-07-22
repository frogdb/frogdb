//! No-op snapshot coordinator.
use super::handle::SnapshotHandle;
use super::{SnapshotCoordinator, SnapshotError, SnapshotMode, SnapshotRequest};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

pub struct NoopSnapshotCoordinator {
    last_save: RwLock<Option<Instant>>,
    in_progress: Arc<AtomicBool>,
    epoch: AtomicU64,
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
            in_progress: Arc::new(AtomicBool::new(false)),
            epoch: AtomicU64::new(0),
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
        // The handle releases `in_progress` on drop/complete — modelling an
        // instant, forkless completion without a background loop.
        let handle = SnapshotHandle::completing(epoch, self.in_progress.clone());
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
    fn request_snapshot(&self, mode: SnapshotMode) -> SnapshotRequest {
        // A save already runs: `Schedule` coalesces, `Immediate` refuses without
        // queuing. The no-op has no run loop to drain a follow-up, so `Coalesced`
        // is purely the observable "scheduled" contract.
        if self.in_progress() {
            return match mode {
                SnapshotMode::Schedule => SnapshotRequest::Coalesced,
                SnapshotMode::Immediate => SnapshotRequest::AlreadyRunning,
            };
        }
        match self.start_snapshot() {
            // The no-op save completes instantly: drop the handle to release
            // `in_progress` immediately, mirroring the Rocks run loop's release.
            Ok(handle) => {
                let epoch = handle.epoch();
                drop(handle);
                SnapshotRequest::Started(epoch)
            }
            // Lost the start race — a save claimed the slot after our probe.
            Err(_) => match mode {
                SnapshotMode::Schedule => SnapshotRequest::Coalesced,
                SnapshotMode::Immediate => SnapshotRequest::AlreadyRunning,
            },
        }
    }
}
