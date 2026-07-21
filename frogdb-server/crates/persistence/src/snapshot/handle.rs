//! Snapshot handle.
//!
//! A `SnapshotHandle` is the value a caller receives after starting a save. In
//! the production (Rocks) path it is a bare epoch carrier: completion is tracked
//! by the coordinator's [`SnapshotScheduler`](super::scheduler::SnapshotScheduler)
//! `in_progress` atomic, which the background run loop clears — the handle has
//! nothing to do, so its `Drop` is a no-op and costs a single `Option` check.
//!
//! The no-op coordinator, which has no background loop, instead models *instant*
//! completion by handing out a handle that releases an `in_progress` flag when it
//! is dropped. That flag is a plain `Arc<AtomicBool>` (no `unsafe` raw-pointer
//! laundering).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SnapshotHandle {
    epoch: u64,
    /// When set, dropping the handle releases this `in_progress` flag (stores
    /// `false`). `None` on the production path, where the scheduler owns
    /// completion.
    on_complete: Option<Arc<AtomicBool>>,
}
impl SnapshotHandle {
    /// A production handle: a bare epoch carrier whose `Drop` does nothing.
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            on_complete: None,
        }
    }
    /// A handle whose `Drop` releases `in_progress` — used by the no-op
    /// coordinator to model instant completion.
    pub fn completing(epoch: u64, in_progress: Arc<AtomicBool>) -> Self {
        Self {
            epoch,
            on_complete: Some(in_progress),
        }
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}
impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if let Some(flag) = self.on_complete.take() {
            flag.store(false, Ordering::SeqCst);
        }
    }
}
