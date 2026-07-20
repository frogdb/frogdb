//! Snapshot abstractions for point-in-time backups.
mod handle;
pub mod metadata;
mod noop;
mod rocks_coordinator;
mod scheduler;
mod stager;
#[cfg(test)]
mod tests;
pub use handle::SnapshotHandle;
pub use metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
pub use noop::NoopSnapshotCoordinator;
pub use rocks_coordinator::{PreSnapshotHook, RocksSnapshotCoordinator};
pub use scheduler::SnapshotScheduler;
use std::time::Instant;
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
    /// call folded into a single pending follow-up.
    Coalesced,
}
pub trait SnapshotCoordinator: Send + Sync {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;
    fn last_save_time(&self) -> Option<Instant>;
    fn in_progress(&self) -> bool;
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata>;
    fn schedule_snapshot(&self) -> bool;
    fn is_scheduled(&self) -> bool;
    /// Atomically request a background save, coalescing with any in-flight run.
    /// Replaces the caller-side check-then-act over `in_progress` +
    /// `schedule_snapshot` / `start_snapshot`.
    fn request_snapshot(&self) -> SnapshotRequest;
}
