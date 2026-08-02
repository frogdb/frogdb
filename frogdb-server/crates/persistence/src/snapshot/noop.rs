//! No-op snapshot coordinator.
//!
//! Backed by the real [`SnapshotScheduler`] (the same Module the Rocks
//! coordinator delegates to), rather than a hand-rolled copy of its atomics and
//! coalesce handshake. The one no-op-specific move is *instant* completion: a
//! no-op save has no disk work, so instead of spawning a background run loop it
//! releases the slot and drains any coalesced follow-up inline
//! ([`complete_instantly`](NoopSnapshotCoordinator::complete_instantly)). The
//! coalesce / lost-wakeup guarantee is inherited from the scheduler for free.
use super::handle::SnapshotHandle;
use super::scheduler::SnapshotScheduler;
use super::{SnapshotCoordinator, SnapshotError, SnapshotMode, SnapshotRequest, SnapshotStats};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

pub struct NoopSnapshotCoordinator {
    stats: RwLock<SnapshotStats>,
    scheduler: Arc<SnapshotScheduler>,
}
impl Default for NoopSnapshotCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
impl NoopSnapshotCoordinator {
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(SnapshotStats::default()),
            scheduler: Arc::new(SnapshotScheduler::with_epoch(0)),
        }
    }

    /// A no-op save writes nothing, so it always succeeds *now*: there is no
    /// artifact whose completion time could differ from the wall clock, and no
    /// failure path (`failures` stays 0 for the life of the process). Its
    /// duration is genuinely zero — there is no work between claiming the slot
    /// and releasing it — so `rdb_last_bgsave_time_sec` reports `0`, not a
    /// borrowed number from a real save that never happened.
    fn record_instant_save(&self) {
        self.stats
            .write()
            .unwrap()
            .record_success(SystemTime::now(), Duration::ZERO);
    }

    /// A no-op save has no disk work, so it completes instantly. Release the slot
    /// and drain any coalesced follow-up inline — the synchronous analogue of the
    /// Rocks `run_loop`'s `finish_and_maybe_rebegin` drain (see
    /// `rocks_coordinator.rs`). Each drained follow-up "runs" instantly too, so it
    /// stamps `last_save`. Returns the last epoch that ran.
    fn complete_instantly(&self, started_epoch: u64) -> u64 {
        let mut epoch = started_epoch;
        while let Some(next) = self.scheduler.finish_and_maybe_rebegin() {
            epoch = next;
            self.record_instant_save();
        }
        epoch
    }
}
impl SnapshotCoordinator for NoopSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        let epoch = self
            .scheduler
            .try_begin()
            .ok_or(SnapshotError::AlreadyInProgress)?;
        self.record_instant_save();
        tracing::info!(epoch, "Noop snapshot started (no actual data saved)");
        // A bare epoch carrier, identical to the Rocks handle: completion is owned
        // by the scheduler, drained inline above — the handle holds no state.
        Ok(SnapshotHandle::new(self.complete_instantly(epoch)))
    }
    fn stats(&self) -> SnapshotStats {
        self.stats.read().unwrap().clone()
    }
    fn in_progress(&self) -> bool {
        self.scheduler.in_progress()
    }
    fn request_snapshot(&self, mode: SnapshotMode) -> SnapshotRequest {
        // The FIXED coalesce handshake, shared with the Rocks path. On `Started`
        // the save runs instantly: stamp `last_save` and drain any follow-up
        // (closing the lost-wakeup window on the no-op too). `Coalesced` /
        // `AlreadyRunning` pass through unchanged.
        match self.scheduler.request_mode(mode) {
            SnapshotRequest::Started(epoch) => {
                self.record_instant_save();
                SnapshotRequest::Started(self.complete_instantly(epoch))
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
