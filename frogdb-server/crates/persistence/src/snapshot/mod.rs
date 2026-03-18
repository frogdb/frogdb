//! Snapshot abstractions for point-in-time backups.
mod handle;
pub mod metadata;
mod noop;
mod rocks_coordinator;
#[cfg(test)]
mod tests;
pub use handle::{NoopOnWriteHook, OnWriteHook, SnapshotHandle};
pub use metadata::{SnapshotConfig, SnapshotMetadata, SnapshotMetadataFile};
pub use noop::NoopSnapshotCoordinator;
pub use rocks_coordinator::{PreSnapshotHook, RocksSnapshotCoordinator};
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
pub trait SnapshotCoordinator: Send + Sync {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;
    fn last_save_time(&self) -> Option<Instant>;
    fn in_progress(&self) -> bool;
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata>;
    fn schedule_snapshot(&self) -> bool;
    fn is_scheduled(&self) -> bool;
}
