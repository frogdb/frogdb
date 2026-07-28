//! Snapshot handle.
//!
//! A `SnapshotHandle` is the value a caller receives after starting a save. It is
//! a bare epoch carrier: no completion state lives on the handle at all.
//! Completion is tracked by the coordinator's
//! [`SnapshotScheduler`](super::scheduler::SnapshotScheduler) `in_progress`
//! atomic — the Rocks path clears it from its background run loop, and the no-op
//! path drains it inline (instant completion). Either way the handle just names
//! the epoch that was started, so its `Drop` is trivial.

pub struct SnapshotHandle {
    epoch: u64,
}
impl SnapshotHandle {
    /// Create a handle naming the epoch that was started.
    pub fn new(epoch: u64) -> Self {
        Self { epoch }
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}
