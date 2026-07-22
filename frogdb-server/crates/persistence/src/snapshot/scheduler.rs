//! Pure coalesce/reschedule state machine for the snapshot lifecycle.
//!
//! Owns the three scheduling atomics (`in_progress`, `scheduled`, `epoch`) and
//! their handshake — nothing else. No Tokio, no filesystem, no `RocksStore`. The
//! coordinator drives disk work and metrics *around* this type; the correctness-
//! critical sequencing (the double-CAS reschedule handshake) lives here and is
//! exhaustively unit-testable in isolation.
use super::{SnapshotMode, SnapshotRequest};
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
    /// # Contract
    ///
    /// After `request()` returns, a snapshot whose run *begins at or after* this
    /// call is guaranteed — either the one we `Started`, or a rescheduled /
    /// concurrent run that another caller owns. No request is ever silently
    /// dropped (no lost wakeup).
    ///
    /// # Lost-wakeup window and how it is closed
    ///
    /// The naive body (`if in_progress { scheduled.store(true) }`) has a race:
    /// between our `in_progress.load() == true` and our `scheduled.store(true)`,
    /// the runner we observed can reach [`finish_and_maybe_rebegin`] and run its
    /// `scheduled.swap(false)` *before* our store lands — consuming nothing. It
    /// then exits with `in_progress == false`, and our `scheduled == true` arms
    /// no follow-up: a lost wakeup.
    ///
    /// We close it with a double-check: after arming `scheduled`, re-load
    /// `in_progress`. Because `finish_and_maybe_rebegin` stores `in_progress =
    /// false` *before* it swaps `scheduled`, and both types are `SeqCst`, the
    /// single total order gives us:
    ///
    /// * If the re-load still sees `in_progress == true`, the runner active at
    ///   that load has *not* yet run its finish-store; therefore its later
    ///   finish-swap is ordered after our `scheduled.store` and is guaranteed to
    ///   observe it (or a peer already consumed our flag and owns the run). We
    ///   fold in — a post-request run is guaranteed.
    /// * If the re-load sees `in_progress == false`, the runner has exited and
    ///   may have missed our flag. We take responsibility: `scheduled.swap(false)`
    ///   claims the flag. If we win it we must run, so we `try_begin`; if that
    ///   fails, a fresh save already claimed the slot *after* our request (so a
    ///   post-request run again exists) and we fold in. If we lose the swap, some
    ///   peer already claimed our flag and owns the post-request run.
    ///
    /// The idle branch mirrors the old lost-race comment: observing idle then
    /// losing `try_begin` means a fresh save began after our decision point, so
    /// it already reflects our request — `Coalesced` without arming a follow-up.
    pub fn request(&self) -> SnapshotRequest {
        if !self.in_progress.load(SeqCst) {
            // Observed idle: claim the slot, or fold into the winner's fresh run.
            return match self.try_begin() {
                Some(epoch) => SnapshotRequest::Started(epoch),
                None => SnapshotRequest::Coalesced,
            };
        }
        // Observed a save running: arm a follow-up and close the wakeup window.
        self.arm_follow_up()
    }

    /// Mode-aware coalesce decision — the single seam behind both `BGSAVE` and
    /// `BGSAVE SCHEDULE`. Claims the slot if idle (`Started`); otherwise the
    /// behaviour splits on `mode`: `Schedule` arms a coalesced follow-up
    /// (`Coalesced`, or `Started` on a finish race — see [`arm_follow_up`]), while
    /// `Immediate` reports `AlreadyRunning` WITHOUT queuing, preserving plain
    /// BGSAVE's no-queue semantics.
    ///
    /// [`arm_follow_up`]: SnapshotScheduler::arm_follow_up
    pub fn request_mode(&self, mode: SnapshotMode) -> SnapshotRequest {
        match self.try_begin() {
            Some(epoch) => SnapshotRequest::Started(epoch),
            None => match mode {
                SnapshotMode::Schedule => self.arm_follow_up(),
                SnapshotMode::Immediate => SnapshotRequest::AlreadyRunning,
            },
        }
    }

    /// Arm a coalesced follow-up after the caller observed `in_progress == true`,
    /// then re-verify to close the lost-wakeup window (see [`request`] docs).
    ///
    /// Precondition: the caller loaded `in_progress == true` immediately before
    /// this call. Split out (rather than inlined into `request`) so a
    /// deterministic unit test can drive the exact former race — enter with the
    /// runner *already exited* — which the black-box `request` re-load would
    /// otherwise hide.
    ///
    /// [`request`]: SnapshotScheduler::request
    pub(super) fn arm_follow_up(&self) -> SnapshotRequest {
        self.scheduled.store(true, SeqCst);

        if self.in_progress.load(SeqCst) {
            // Runner still active — its finish-swap is ordered after our store
            // (finish stores `in_progress = false` before swapping `scheduled`),
            // so the follow-up is guaranteed. Fold in.
            return SnapshotRequest::Coalesced;
        }

        // The runner exited in the arm window and may have missed our flag.
        // Claim the flag ourselves rather than lose the wakeup.
        if self.scheduled.swap(false, SeqCst) {
            // We own the follow-up: we must begin the post-request run.
            match self.try_begin() {
                Some(epoch) => SnapshotRequest::Started(epoch),
                // A concurrent save already claimed the slot after our request,
                // so a post-request run exists without us. Fold in.
                None => SnapshotRequest::Coalesced,
            }
        } else {
            // A peer (the exiting runner's finish-swap, or a concurrent request)
            // already consumed our flag and owns the post-request run. Fold in.
            SnapshotRequest::Coalesced
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
