//! Pure coalesce/reschedule state machine for the snapshot lifecycle.
//!
//! Owns the three scheduling atomics (`in_progress`, `scheduled`, `epoch`) and
//! their handshake — nothing else. No Tokio, no filesystem, no `RocksStore`. The
//! coordinator drives disk work and metrics *around* this type; the correctness-
//! critical sequencing (the double-CAS reschedule handshake) lives here and is
//! exhaustively unit-testable in isolation.
use super::SnapshotRequest;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};

/// The three scheduling atomics and their coalesce/reschedule handshake.
pub struct SnapshotScheduler {
    in_progress: AtomicBool,
    scheduled: AtomicBool,
    epoch: AtomicU64,
}

impl SnapshotScheduler {
    /// Create a scheduler whose epoch counter resumes from `initial_epoch`
    /// (the epoch of the newest snapshot recovered on startup, or 0).
    pub fn with_epoch(initial_epoch: u64) -> Self {
        Self {
            in_progress: AtomicBool::new(false),
            scheduled: AtomicBool::new(false),
            epoch: AtomicU64::new(initial_epoch),
        }
    }

    /// Try to claim the in-progress slot. Returns the new epoch on success, or
    /// `None` if a save is already running (the `AlreadyInProgress` case).
    pub fn try_begin(&self) -> Option<u64> {
        self.in_progress
            .compare_exchange(false, true, SeqCst, SeqCst)
            .ok()?;
        Some(self.epoch.fetch_add(1, SeqCst) + 1)
    }

    /// End the current run; if a follow-up was scheduled *and* we can re-acquire
    /// the slot, return the next epoch to run. Otherwise `None` (fully idle).
    ///
    /// This is the double-CAS handshake in one named place. Its two race windows:
    /// between the release `store` and the `swap` (another caller may set
    /// `scheduled`), and between the `swap` and the re-`compare_exchange` (another
    /// caller may win the slot and start a fresh save). If the re-acquire fails,
    /// we deliberately return `None` and let the winner own the run — we must not
    /// double-run an epoch.
    pub fn finish_and_maybe_rebegin(&self) -> Option<u64> {
        self.in_progress.store(false, SeqCst);
        if !self.scheduled.swap(false, SeqCst) {
            return None;
        }
        self.in_progress
            .compare_exchange(false, true, SeqCst, SeqCst)
            .ok()?;
        Some(self.epoch.fetch_add(1, SeqCst) + 1)
    }

    /// Caller-facing coalesce decision (replaces the check-then-act at BGSAVE).
    /// If a save is running, mark a follow-up and report `Coalesced`; otherwise
    /// begin immediately and report `Started(epoch)`.
    ///
    /// The lost-race branch (we observed idle, then `try_begin` failed because
    /// another caller just claimed the slot) reports `Coalesced` *without*
    /// setting `scheduled`: that fresh save began after our decision point, so it
    /// already reflects our request and no redundant follow-up is needed.
    pub fn request(&self) -> SnapshotRequest {
        if self.in_progress.load(SeqCst) {
            self.scheduled.store(true, SeqCst);
            SnapshotRequest::Coalesced
        } else {
            match self.try_begin() {
                Some(epoch) => SnapshotRequest::Started(epoch),
                None => SnapshotRequest::Coalesced,
            }
        }
    }

    /// Back the legacy `schedule_snapshot` protocol: mark a follow-up only while a
    /// save is actually running. Returns whether the flag was set.
    pub fn schedule(&self) -> bool {
        if !self.in_progress.load(SeqCst) {
            return false;
        }
        self.scheduled.store(true, SeqCst);
        true
    }

    pub fn in_progress(&self) -> bool {
        self.in_progress.load(SeqCst)
    }

    pub fn is_scheduled(&self) -> bool {
        self.scheduled.load(SeqCst)
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(SeqCst)
    }
}
