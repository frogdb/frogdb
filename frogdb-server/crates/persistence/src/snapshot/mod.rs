//! Snapshot abstractions for point-in-time backups.
mod handle;
pub mod metadata;
mod noop;
mod rocks_coordinator;
mod scheduler;
mod stager;
#[cfg(test)]
mod tests;
use frogdb_types::clock;
pub use handle::SnapshotHandle;
pub use metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
pub use noop::NoopSnapshotCoordinator;
pub use rocks_coordinator::{PreSnapshotHook, RocksSnapshotCoordinator};
pub use scheduler::SnapshotScheduler;
use std::time::{Duration, Instant, SystemTime};
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("Snapshot already in progress")]
    AlreadyInProgress,
    #[error("No snapshot in progress")]
    NotInProgress,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Internal error: {0}")]
    Internal(String),
    /// The pre-snapshot hook ([`PreSnapshotHook`]) could not complete, so
    /// cutting now would produce an artifact missing acknowledged writes. The
    /// save is failed rather than cut; see the hook's docs.
    #[error("Pre-snapshot quiesce failed: {0}")]
    PreSnapshot(String),
}
/// How a background-save request should behave when a save is already running.
///
/// This is the one seam that distinguishes plain `BGSAVE` from `BGSAVE SCHEDULE`:
/// both start a save when idle, but only `Schedule` queues a coalesced follow-up
/// when a save is in flight — `Immediate` refuses without queuing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Plain `BGSAVE`: start if idle, else report already-running WITHOUT queuing.
    Immediate,
    /// `BGSAVE SCHEDULE`: start if idle, else coalesce a single follow-up.
    Schedule,
}
/// Outcome of a coalescing snapshot request ([`SnapshotCoordinator::request_snapshot`]).
///
/// Folds the check-then-act BGSAVE decision (is a save running? if so schedule a
/// follow-up, else start one) into a single atomic step so callers no longer
/// hand-sequence the raw scheduling booleans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRequest {
    /// No save was running; this call claimed the slot and started `epoch`.
    Started(u64),
    /// A save was already running (or another caller won the start race); this
    /// call folded into a single pending follow-up ([`SnapshotMode::Schedule`]).
    Coalesced,
    /// A save was already running; nothing was queued ([`SnapshotMode::Immediate`]).
    AlreadyRunning,
}
/// Outcome state of the save history: everything `LASTSAVE` and `INFO
/// persistence` report about background saves, in one value read under a single
/// lock acquisition.
///
/// Each clock is chosen for what its field has to survive. `last_save_time` is
/// wall-clock ([`SystemTime`]) because the last save may have happened *before
/// this process started* — the coordinator seeds it from the newest complete
/// snapshot's `metadata.json` at boot — which a process-relative monotonic clock
/// cannot represent. Durations use [`Instant`]: they only ever describe a window
/// inside this process, and a monotonic clock keeps a wall-clock step (NTP, a
/// manual `date`) from inventing a negative or hour-long save.
///
/// The counters and `last_duration` are per-process, matching Redis' `rdb_saves`
/// ("since startup"); `last_save_time` is not, because it describes an artifact
/// on disk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotStats {
    /// Completion time of the last **successful** save — the newest complete
    /// snapshot's `completed_at_ms`, whichever process wrote it. `None` means
    /// nothing has ever been saved (Redis' `LASTSAVE` 0).
    pub last_save_time: Option<SystemTime>,
    /// How long the last **successful** save took (Redis
    /// `rdb_last_bgsave_time_sec`, which reports `-1` until one completes).
    /// `None` after a boot even when `last_save_time` is set: the duration is
    /// not recorded in the artifact, so it is per-process where the completion
    /// time is not. A failed attempt does not overwrite it — the field answers
    /// "how long does a save take here", and a save that died on `mkdir` is no
    /// evidence about that.
    pub last_duration: Option<Duration>,
    /// When the currently-running save started, monotonic so the elapsed time
    /// cannot be distorted by a clock step (Redis
    /// `rdb_current_bgsave_time_sec`). `Some` exactly while a run owns the
    /// scheduler slot; the reader turns it into an elapsed count, so a save
    /// that hangs shows a *growing* number rather than a frozen one.
    pub current_started_at: Option<Instant>,
    /// Successful saves completed by this process.
    pub saves: u64,
    /// Save attempts that failed in this process. Cumulative: a later success
    /// does not reset it, so an operator sampling `INFO` after a recovery still
    /// sees that saves were failing.
    pub failures: u64,
    /// Cause of the most recent *failed* attempt, cleared by the next success.
    /// `Some` is exactly Redis' `rdb_last_bgsave_status:err` condition; `None`
    /// covers both "the last attempt succeeded" and "nothing attempted yet".
    pub last_error: Option<String>,
}

impl SnapshotStats {
    /// Mark a save as running, from the moment it claims the scheduler slot.
    /// Returns the stamped start instant so the caller measures the same window
    /// it publishes — one clock read, one meaning.
    pub(crate) fn record_start(&mut self) -> Instant {
        let started = clock::now();
        self.current_started_at = Some(started);
        started
    }

    /// Record a completed save: stamp the artifact's own completion time and how
    /// long it took, count it, and clear any previous failure.
    pub(crate) fn record_success(&mut self, completed_at: SystemTime, elapsed: Duration) {
        self.last_save_time = Some(completed_at);
        self.last_duration = Some(elapsed);
        self.current_started_at = None;
        self.saves += 1;
        self.last_error = None;
    }

    /// Record a failed attempt. `last_save_time` and `last_duration`
    /// deliberately do not move — a failed save is not a save, which is what
    /// makes `LASTSAVE` honest.
    pub(crate) fn record_failure(&mut self, error: String) {
        self.current_started_at = None;
        self.failures += 1;
        self.last_error = Some(error);
    }

    /// How long the in-flight save has been running, or `None` when none is.
    /// Reported as Redis' `rdb_current_bgsave_time_sec` (`-1` when idle).
    pub fn current_save_elapsed(&self) -> Option<Duration> {
        self.current_started_at.map(clock::elapsed)
    }
}

/// The save history plus the one bit of it the *write path* needs cheaply.
///
/// [`SnapshotStats`] answers every `INFO`/`LASTSAVE` question, but it lives
/// behind an `RwLock` and cloning it to ask one boolean question would put a
/// lock acquisition (and, once a cause is retained, a `String` allocation) on
/// every write a server accepts. The `-MISCONF` refusal
/// (`snapshot.stop-writes-on-save-error`) asks exactly that one question, so the
/// answer is mirrored into an `AtomicBool`.
///
/// A mirror is a second copy of the truth and can drift; this type is what makes
/// drift impossible rather than merely unlikely. It owns both fields and is the
/// only way to write either, so the only two methods that can move the bool are
/// the same two that write `last_error` — `last_save_failed()` and
/// `stats().last_error.is_some()` are the same fact by construction.
#[derive(Debug, Default)]
pub struct SaveHistory {
    stats: std::sync::RwLock<SnapshotStats>,
    /// Mirrors `stats.last_error.is_some()`. Relaxed throughout: it guards no
    /// other memory, and a write that races a save's completion by one
    /// instruction is a write that raced the save itself.
    failed: std::sync::atomic::AtomicBool,
}

impl SaveHistory {
    /// Seed the history (from the newest complete snapshot on disk, at boot).
    ///
    /// The latch is derived from the seed rather than assumed clear, so the
    /// invariant holds from the first instant this type exists. In practice a
    /// boot seed carries no cause — it is built from the newest *complete*
    /// snapshot, so a fresh process never refuses writes for a failure nobody
    /// observed — but deriving costs nothing and does not depend on that
    /// staying true.
    pub(crate) fn new(stats: SnapshotStats) -> Self {
        let failed = stats.last_error.is_some();
        Self {
            stats: std::sync::RwLock::new(stats),
            failed: std::sync::atomic::AtomicBool::new(failed),
        }
    }

    /// See [`SnapshotStats::record_start`].
    pub(crate) fn record_start(&self) -> Instant {
        self.stats.write().unwrap().record_start()
    }

    /// See [`SnapshotStats::record_success`]. Clears the refusal latch: a save
    /// succeeded, so whatever was broken is not broken now.
    pub(crate) fn record_success(&self, completed_at: SystemTime, elapsed: Duration) {
        let mut stats = self.stats.write().unwrap();
        stats.record_success(completed_at, elapsed);
        self.failed
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// See [`SnapshotStats::record_failure`]. Sets the refusal latch.
    pub(crate) fn record_failure(&self, error: String) {
        let mut stats = self.stats.write().unwrap();
        stats.record_failure(error);
        self.failed
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// The whole history, published in one lock acquisition so no two `INFO`
    /// fields can describe different moments.
    pub(crate) fn snapshot(&self) -> SnapshotStats {
        self.stats.read().unwrap().clone()
    }

    /// Whether the most recent save attempt failed, without touching the lock.
    pub(crate) fn last_save_failed(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub trait SnapshotCoordinator: Send + Sync {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;
    /// Save history: outcome, counters, and the last successful save's time.
    fn stats(&self) -> SnapshotStats;
    /// Wall-clock time of the last successful save (`LASTSAVE`,
    /// `rdb_last_save_time`). Derived from [`stats`](Self::stats) so the two
    /// surfaces cannot disagree.
    fn last_save_time(&self) -> Option<SystemTime> {
        self.stats().last_save_time
    }
    /// Whether the most recent save attempt failed with no success since —
    /// exactly Redis' `rdb_last_bgsave_status:err`, and exactly the condition
    /// `snapshot.stop-writes-on-save-error` refuses client writes on.
    ///
    /// Deliberately not defaulted to `self.stats().last_error.is_some()`: this
    /// is read on the write path, and a default would silently put an `RwLock`
    /// acquisition and a `String` clone there for every implementor that forgot
    /// to override it. Implementors back it with [`SaveHistory`], which keeps
    /// the two answers identical by construction.
    fn last_save_failed(&self) -> bool;
    fn in_progress(&self) -> bool;
    /// Atomically request a background save, coalescing with any in-flight run.
    /// `mode` selects the no-queue (`Immediate`) vs coalesce (`Schedule`)
    /// behaviour when a save is already running.
    fn request_snapshot(&self, mode: SnapshotMode) -> SnapshotRequest;
    /// Live periodic-save cadence in seconds (0 = periodic saves disabled).
    ///
    /// The background periodic-snapshot task re-reads this before every
    /// scheduling decision rather than capturing it once at spawn time.
    fn periodic_interval_secs(&self) -> u64;
    /// Retune the periodic-save cadence without a restart. Reachable from
    /// `ConfigManager` for `CONFIG SET snapshot-interval-secs`.
    fn set_periodic_interval_secs(&self, secs: u64);
}
