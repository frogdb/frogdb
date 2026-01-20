//! Snapshot abstractions for point-in-time backups.
//!
//! This module provides traits and types for snapshot coordination.
//! The actual snapshot implementation is deferred to a future phase;
//! this phase only provides the NoopSnapshotCoordinator.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Instant, SystemTime};

use crate::types::Value;

/// Metadata about a snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Monotonically increasing snapshot epoch.
    pub epoch: u64,

    /// When the snapshot was started.
    pub started_at: SystemTime,

    /// When the snapshot completed (None if in progress).
    pub completed_at: Option<SystemTime>,

    /// Number of keys in the snapshot.
    pub num_keys: u64,

    /// Total size of the snapshot in bytes.
    pub size_bytes: u64,
}

/// Handle to an in-progress snapshot.
///
/// The snapshot is considered complete when this handle is dropped
/// or when `complete()` is called.
pub struct SnapshotHandle {
    epoch: u64,
    is_noop: bool,
    complete_fn: Option<Box<dyn FnOnce() + Send>>,
}

impl SnapshotHandle {
    /// Create a noop snapshot handle.
    pub fn noop() -> Self {
        Self {
            epoch: 0,
            is_noop: true,
            complete_fn: None,
        }
    }

    /// Create a real snapshot handle with a completion callback.
    pub fn new(epoch: u64, complete_fn: impl FnOnce() + Send + 'static) -> Self {
        Self {
            epoch,
            is_noop: false,
            complete_fn: Some(Box::new(complete_fn)),
        }
    }

    /// Get the snapshot epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Check if this is a noop snapshot.
    pub fn is_noop(&self) -> bool {
        self.is_noop
    }

    /// Mark the snapshot as complete.
    pub fn complete(mut self) {
        if let Some(f) = self.complete_fn.take() {
            f();
        }
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if let Some(f) = self.complete_fn.take() {
            f();
        }
    }
}

/// Error during snapshot operations.
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

/// Trait for coordinating snapshots across shards.
pub trait SnapshotCoordinator: Send + Sync {
    /// Start a new snapshot.
    ///
    /// Returns a handle that must be held until the snapshot is complete.
    /// Returns an error if a snapshot is already in progress.
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;

    /// Get the time of the last completed snapshot.
    fn last_save_time(&self) -> Option<Instant>;

    /// Check if a snapshot is currently in progress.
    fn in_progress(&self) -> bool;

    /// Get metadata about the last completed snapshot.
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata>;
}

/// Hook called on write operations for COW (Copy-on-Write) support.
///
/// This is used during snapshots to capture the old value before
/// a write modifies it, enabling consistent snapshots without blocking.
pub trait OnWriteHook: Send + Sync {
    /// Called before a write operation modifies or deletes a key.
    ///
    /// - `key`: The key being modified
    /// - `old_value`: The current value (None if key doesn't exist)
    fn on_write(&self, key: &[u8], old_value: Option<&Value>);
}

/// Noop implementation that does nothing.
#[allow(dead_code)]
pub struct NoopOnWriteHook;

impl OnWriteHook for NoopOnWriteHook {
    fn on_write(&self, _key: &[u8], _old_value: Option<&Value>) {
        // Do nothing
    }
}

/// Noop snapshot coordinator that doesn't actually take snapshots.
///
/// This is used when snapshots are disabled or not yet implemented.
/// BGSAVE will return success immediately without doing anything.
pub struct NoopSnapshotCoordinator {
    last_save: RwLock<Option<Instant>>,
    in_progress: AtomicBool,
    epoch: AtomicU64,
}

impl Default for NoopSnapshotCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl NoopSnapshotCoordinator {
    /// Create a new noop snapshot coordinator.
    pub fn new() -> Self {
        Self {
            last_save: RwLock::new(None),
            in_progress: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
        }
    }
}

impl SnapshotCoordinator for NoopSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        // Check if already in progress (for correctness, even though we're noop)
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SnapshotError::AlreadyInProgress);
        }

        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;

        // Update last save time immediately for noop
        *self.last_save.write().unwrap() = Some(Instant::now());

        // Create handle that will clear in_progress when dropped
        let in_progress = &self.in_progress as *const AtomicBool as usize;

        let handle = SnapshotHandle::new(epoch, move || {
            // SAFETY: The coordinator outlives the handle
            let in_progress = unsafe { &*(in_progress as *const AtomicBool) };
            in_progress.store(false, Ordering::SeqCst);
        });

        tracing::info!(epoch = epoch, "Noop snapshot started (no actual data saved)");

        Ok(handle)
    }

    fn last_save_time(&self) -> Option<Instant> {
        *self.last_save.read().unwrap()
    }

    fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        let last_save = self.last_save_time()?;
        let now = Instant::now();
        let system_now = SystemTime::now();

        // Calculate SystemTime from Instant
        let started_at = if last_save <= now {
            let elapsed = now - last_save;
            system_now - elapsed
        } else {
            system_now
        };

        Some(SnapshotMetadata {
            epoch: self.epoch.load(Ordering::SeqCst),
            started_at,
            completed_at: Some(started_at), // Noop completes immediately
            num_keys: 0,
            size_bytes: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_coordinator() {
        let coord = NoopSnapshotCoordinator::new();

        assert!(coord.last_save_time().is_none());
        assert!(!coord.in_progress());

        let handle = coord.start_snapshot().unwrap();
        assert!(coord.in_progress());
        assert!(coord.last_save_time().is_some());
        assert_eq!(handle.epoch(), 1);

        drop(handle);
        assert!(!coord.in_progress());
    }

    #[test]
    fn test_noop_coordinator_rejects_concurrent() {
        let coord = NoopSnapshotCoordinator::new();

        let _handle = coord.start_snapshot().unwrap();

        // Second snapshot should fail
        let result = coord.start_snapshot();
        assert!(matches!(result, Err(SnapshotError::AlreadyInProgress)));
    }

    #[test]
    fn test_snapshot_handle_complete() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let completed = Arc::new(AtomicBool::new(false));
        let completed_clone = completed.clone();

        let handle = SnapshotHandle::new(1, move || {
            completed_clone.store(true, Ordering::SeqCst);
        });

        assert!(!completed.load(Ordering::SeqCst));

        handle.complete();

        assert!(completed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_snapshot_handle_noop() {
        let handle = SnapshotHandle::noop();
        assert!(handle.is_noop());
        assert_eq!(handle.epoch(), 0);
        drop(handle); // Should not panic
    }

    #[test]
    fn test_noop_metadata() {
        let coord = NoopSnapshotCoordinator::new();

        assert!(coord.last_snapshot_metadata().is_none());

        let handle = coord.start_snapshot().unwrap();
        drop(handle);

        let metadata = coord.last_snapshot_metadata().unwrap();
        assert_eq!(metadata.epoch, 1);
        assert!(metadata.completed_at.is_some());
        assert_eq!(metadata.num_keys, 0);
    }
}
