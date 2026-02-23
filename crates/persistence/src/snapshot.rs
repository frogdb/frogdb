//! Snapshot abstractions for point-in-time backups.
//!
//! This module provides traits and types for snapshot coordination using RocksDB's
//! Checkpoint API. The Checkpoint API creates hard links to immutable SST files,
//! providing efficient point-in-time snapshots without requiring application-level
//! Copy-on-Write semantics.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::rocks::RocksStore;
use frogdb_types::traits::MetricsRecorder;
use frogdb_types::types::Value;

/// Completion marker written to metadata files.
const COMPLETION_MARKER: &str = "FROGDB_SNAPSHOT_COMPLETE_v1";

/// Metadata about a snapshot (in-memory representation).
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

/// Metadata file stored with each snapshot (persisted to disk).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadataFile {
    /// Format version (currently 1).
    pub version: u8,
    /// Snapshot epoch number.
    pub epoch: u64,
    /// RocksDB sequence number at snapshot start.
    pub sequence_number: u64,
    /// Unix timestamp in milliseconds when snapshot started.
    pub started_at_ms: u64,
    /// Unix timestamp in milliseconds when snapshot finished (None if incomplete).
    pub completed_at_ms: Option<u64>,
    /// Number of shards in the database.
    pub num_shards: usize,
    /// Estimated total number of keys (may be approximate).
    pub num_keys: u64,
    /// Total size of snapshot files in bytes.
    pub size_bytes: u64,
    /// Completion marker - only set if snapshot is complete.
    pub completion_marker: Option<String>,
}

impl SnapshotMetadataFile {
    /// Create a new metadata file for an in-progress snapshot.
    pub fn new(epoch: u64, sequence_number: u64, num_shards: usize) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);

        Self {
            version: 1,
            epoch,
            sequence_number,
            started_at_ms: now.as_millis() as u64,
            completed_at_ms: None,
            num_shards,
            num_keys: 0,
            size_bytes: 0,
            completion_marker: None,
        }
    }

    /// Mark the snapshot as complete with final statistics.
    pub fn mark_complete(&mut self, num_keys: u64, size_bytes: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);

        self.completed_at_ms = Some(now.as_millis() as u64);
        self.num_keys = num_keys;
        self.size_bytes = size_bytes;
        self.completion_marker = Some(COMPLETION_MARKER.to_string());
    }

    /// Check if this snapshot is complete.
    pub fn is_complete(&self) -> bool {
        self.completion_marker.as_deref() == Some(COMPLETION_MARKER)
    }

    /// Convert to in-memory SnapshotMetadata.
    pub fn to_metadata(&self) -> SnapshotMetadata {
        let started_at = UNIX_EPOCH + Duration::from_millis(self.started_at_ms);
        let completed_at = self
            .completed_at_ms
            .map(|ms| UNIX_EPOCH + Duration::from_millis(ms));

        SnapshotMetadata {
            epoch: self.epoch,
            started_at,
            completed_at,
            num_keys: self.num_keys,
            size_bytes: self.size_bytes,
        }
    }
}

/// Configuration for the snapshot coordinator.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Directory for storing snapshots.
    pub snapshot_dir: PathBuf,
    /// Interval between automatic snapshots (0 = disabled).
    pub snapshot_interval_secs: u64,
    /// Maximum number of snapshots to retain (0 = unlimited).
    pub max_snapshots: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: PathBuf::from("./snapshots"),
            snapshot_interval_secs: 3600, // 1 hour
            max_snapshots: 5,
        }
    }
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

    /// Schedule a snapshot to run after the current one completes.
    /// Returns true if scheduled, false if no snapshot was in progress.
    fn schedule_snapshot(&self) -> bool;

    /// Check if a snapshot is scheduled to run.
    fn is_scheduled(&self) -> bool;
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
    scheduled: AtomicBool,
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
            scheduled: AtomicBool::new(false),
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

    fn schedule_snapshot(&self) -> bool {
        // Only schedule if a snapshot is in progress
        if !self.in_progress() {
            return false;
        }
        self.scheduled.store(true, Ordering::SeqCst);
        true
    }

    fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::SeqCst)
    }
}

// =============================================================================
// RocksSnapshotCoordinator - Real snapshot implementation using RocksDB Checkpoint
// =============================================================================

/// Real snapshot coordinator that uses RocksDB's Checkpoint API.
///
/// This coordinator creates point-in-time snapshots by leveraging RocksDB's
/// native checkpoint functionality. Checkpoints use hard links to immutable
/// SST files, making them fast and space-efficient.
pub struct RocksSnapshotCoordinator {
    /// RocksDB store reference.
    rocks_store: Arc<RocksStore>,
    /// Directory where snapshots are stored.
    snapshot_dir: PathBuf,
    /// Number of shards in the database.
    num_shards: usize,
    /// Current snapshot epoch (monotonically increasing).
    epoch: Arc<AtomicU64>,
    /// Whether a snapshot is currently in progress.
    in_progress: Arc<AtomicBool>,
    /// Whether a snapshot is scheduled to run after the current one completes.
    scheduled: Arc<AtomicBool>,
    /// Time of the last completed snapshot.
    last_save_time: Arc<RwLock<Option<Instant>>>,
    /// Metadata of the last completed snapshot.
    last_metadata: Arc<RwLock<Option<SnapshotMetadataFile>>>,
    /// Maximum number of snapshots to retain.
    max_snapshots: usize,
    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,
}

impl RocksSnapshotCoordinator {
    /// Create a new RocksDB snapshot coordinator.
    pub fn new(
        rocks_store: Arc<RocksStore>,
        config: SnapshotConfig,
        metrics_recorder: Arc<dyn MetricsRecorder>,
    ) -> Result<Self, SnapshotError> {
        // Ensure snapshot directory exists
        std::fs::create_dir_all(&config.snapshot_dir)?;

        let num_shards = rocks_store.num_shards();

        // Try to load the latest snapshot metadata to initialize epoch
        let (initial_epoch, last_metadata) =
            Self::load_latest_metadata(&config.snapshot_dir).unwrap_or((0, None));

        let last_save_time = if last_metadata.is_some() {
            Some(Instant::now()) // Approximate - we don't know the exact time
        } else {
            None
        };

        Ok(Self {
            rocks_store,
            snapshot_dir: config.snapshot_dir,
            num_shards,
            epoch: Arc::new(AtomicU64::new(initial_epoch)),
            in_progress: Arc::new(AtomicBool::new(false)),
            scheduled: Arc::new(AtomicBool::new(false)),
            last_save_time: Arc::new(RwLock::new(last_save_time)),
            last_metadata: Arc::new(RwLock::new(last_metadata)),
            max_snapshots: config.max_snapshots,
            metrics_recorder,
        })
    }

    /// Load the latest snapshot metadata from disk.
    fn load_latest_metadata(
        snapshot_dir: &std::path::Path,
    ) -> Result<(u64, Option<SnapshotMetadataFile>), SnapshotError> {
        let latest_link = snapshot_dir.join("latest");

        if !latest_link.exists() {
            return Ok((0, None));
        }

        // Read the symlink to find the latest snapshot directory
        let target = std::fs::read_link(&latest_link)?;
        let metadata_path = if target.is_absolute() {
            target.join("metadata.json")
        } else {
            snapshot_dir.join(target).join("metadata.json")
        };

        if !metadata_path.exists() {
            return Ok((0, None));
        }

        let content = std::fs::read_to_string(&metadata_path)?;
        let metadata: SnapshotMetadataFile = serde_json::from_str(&content)
            .map_err(|e| SnapshotError::Internal(format!("Failed to parse metadata: {}", e)))?;

        if !metadata.is_complete() {
            // Incomplete snapshot - don't use it
            return Ok((metadata.epoch, None));
        }

        Ok((metadata.epoch, Some(metadata)))
    }

    /// Calculate directory size recursively.
    fn calculate_dir_size(path: &std::path::Path) -> std::io::Result<u64> {
        let mut size = 0;
        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_dir() {
                    size += Self::calculate_dir_size(&entry.path())?;
                } else {
                    size += metadata.len();
                }
            }
        }
        Ok(size)
    }

    /// Clean up old snapshots, keeping only the most recent ones.
    fn cleanup_old_snapshots(
        snapshot_dir: &std::path::Path,
        max_snapshots: usize,
    ) -> Result<(), SnapshotError> {
        if max_snapshots == 0 {
            return Ok(()); // Unlimited retention
        }

        let mut entries: Vec<(u64, PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Only consider snapshot directories (snapshot_XXXXX format)
            if name_str.starts_with("snapshot_")
                && entry.file_type()?.is_dir()
                && let Some(epoch_str) = name_str.strip_prefix("snapshot_")
                && let Ok(epoch) = epoch_str.parse::<u64>()
            {
                entries.push((epoch, entry.path()));
            }
        }

        if entries.len() <= max_snapshots {
            return Ok(());
        }

        // Sort by epoch (ascending) so oldest are first
        entries.sort_by_key(|(epoch, _)| *epoch);

        // Delete oldest snapshots
        let delete_count = entries.len() - max_snapshots;
        for (epoch, path) in entries.into_iter().take(delete_count) {
            tracing::info!(epoch, path = %path.display(), "Deleting old snapshot");
            if let Err(e) = std::fs::remove_dir_all(&path) {
                tracing::warn!(epoch, error = %e, "Failed to delete old snapshot");
            }
        }

        Ok(())
    }

    /// Update the 'latest' symlink atomically.
    #[cfg(unix)]
    fn update_latest_symlink(
        snapshot_dir: &std::path::Path,
        snapshot_name: &str,
    ) -> Result<(), SnapshotError> {
        let latest_link = snapshot_dir.join("latest");
        let temp_link = snapshot_dir.join(".latest.tmp");

        // Remove old temp link if it exists
        let _ = std::fs::remove_file(&temp_link);

        // Create new symlink pointing to snapshot directory name (relative)
        std::os::unix::fs::symlink(snapshot_name, &temp_link)?;

        // Atomically rename to 'latest'
        std::fs::rename(&temp_link, &latest_link)?;

        Ok(())
    }

    #[cfg(not(unix))]
    fn update_latest_symlink(
        snapshot_dir: &std::path::Path,
        snapshot_name: &str,
    ) -> Result<(), SnapshotError> {
        // On non-Unix systems, write the snapshot name to a file instead
        let latest_file = snapshot_dir.join("latest");
        std::fs::write(&latest_file, snapshot_name)?;
        Ok(())
    }
}

impl SnapshotCoordinator for RocksSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
        // CAS to acquire the in_progress lock
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SnapshotError::AlreadyInProgress);
        }

        // Increment epoch
        let initial_epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;

        // Record that a snapshot is starting
        self.metrics_recorder
            .record_gauge("frogdb_snapshot_in_progress", 1.0, &[]);
        self.metrics_recorder
            .record_gauge("frogdb_snapshot_epoch", initial_epoch as f64, &[]);

        tracing::info!(epoch = initial_epoch, "Snapshot started");

        // Clone Arc handles for the spawned task
        let rocks_store = self.rocks_store.clone();
        let snapshot_dir = self.snapshot_dir.clone();
        let in_progress = self.in_progress.clone();
        let scheduled = self.scheduled.clone();
        let epoch_counter = self.epoch.clone();
        let last_save_time = self.last_save_time.clone();
        let last_metadata = self.last_metadata.clone();
        let metrics = self.metrics_recorder.clone();
        let max_snapshots = self.max_snapshots;
        let num_shards = self.num_shards;

        // Spawn background task (returns immediately)
        tokio::spawn(async move {
            let mut current_epoch = initial_epoch;
            let mut start = Instant::now();

            loop {
                // Clone values needed for the blocking task
                let rocks_store_inner = rocks_store.clone();
                let snapshot_dir_inner = snapshot_dir.clone();
                let snapshot_epoch = current_epoch;

                // Use spawn_blocking for RocksDB Checkpoint (it's !Send)
                let result = tokio::task::spawn_blocking(move || {
                    let snapshot_name = format!("snapshot_{:05}", snapshot_epoch);
                    let temp_dir =
                        snapshot_dir_inner.join(format!(".snapshot_{:05}.tmp", snapshot_epoch));
                    let final_dir = snapshot_dir_inner.join(&snapshot_name);
                    let checkpoint_path = temp_dir.join("checkpoint");

                    // Create temp directory structure
                    if let Err(e) = std::fs::create_dir_all(&checkpoint_path) {
                        return Err(SnapshotError::Io(e));
                    }

                    // Capture sequence number atomically with checkpoint
                    let sequence = rocks_store_inner.latest_sequence_number();

                    // Create the RocksDB checkpoint
                    if let Err(e) = rocks_store_inner.create_checkpoint(&checkpoint_path) {
                        // Clean up temp directory on error
                        let _ = std::fs::remove_dir_all(&temp_dir);
                        return Err(SnapshotError::Internal(format!(
                            "Failed to create checkpoint: {}",
                            e
                        )));
                    }

                    // Create metadata
                    let mut metadata =
                        SnapshotMetadataFile::new(snapshot_epoch, sequence, num_shards);

                    // Calculate snapshot size
                    let size_bytes = Self::calculate_dir_size(&checkpoint_path).unwrap_or(0);

                    // Mark as complete (we don't have an accurate key count without scanning)
                    metadata.mark_complete(0, size_bytes);

                    // Write metadata to temp directory
                    let metadata_path = temp_dir.join("metadata.json");
                    let metadata_json = serde_json::to_string_pretty(&metadata).map_err(|e| {
                        SnapshotError::Internal(format!("Failed to serialize metadata: {}", e))
                    })?;

                    // Write metadata atomically using temp file + rename
                    let metadata_tmp = temp_dir.join("metadata.json.tmp");
                    std::fs::write(&metadata_tmp, &metadata_json)?;
                    std::fs::rename(&metadata_tmp, &metadata_path)?;

                    // Atomic rename of entire snapshot directory
                    std::fs::rename(&temp_dir, &final_dir)?;

                    // Update 'latest' symlink
                    if let Err(e) = Self::update_latest_symlink(&snapshot_dir_inner, &snapshot_name)
                    {
                        tracing::warn!(error = %e, "Failed to update latest symlink");
                    }

                    // Clean up old snapshots
                    if let Err(e) = Self::cleanup_old_snapshots(&snapshot_dir_inner, max_snapshots)
                    {
                        tracing::warn!(error = %e, "Failed to cleanup old snapshots");
                    }

                    Ok((metadata, sequence, final_dir))
                })
                .await;

                match result {
                    Ok(Ok((metadata, sequence, snapshot_path))) => {
                        let elapsed = start.elapsed();

                        // Update state
                        *last_save_time.write().unwrap() = Some(Instant::now());
                        *last_metadata.write().unwrap() = Some(metadata.clone());

                        // Record metrics
                        metrics.record_histogram(
                            "frogdb_snapshot_duration_seconds",
                            elapsed.as_secs_f64(),
                            &[],
                        );
                        metrics.record_gauge(
                            "frogdb_snapshot_size_bytes",
                            metadata.size_bytes as f64,
                            &[],
                        );

                        tracing::info!(
                            epoch = current_epoch,
                            sequence,
                            path = %snapshot_path.display(),
                            size_bytes = metadata.size_bytes,
                            duration_ms = elapsed.as_millis(),
                            "Snapshot completed"
                        );
                    }
                    Ok(Err(e)) => {
                        metrics.increment_counter(
                            "frogdb_persistence_errors_total",
                            1,
                            &[("type", "snapshot")],
                        );
                        tracing::error!(epoch = current_epoch, error = %e, "Snapshot failed");
                    }
                    Err(e) => {
                        metrics.increment_counter(
                            "frogdb_persistence_errors_total",
                            1,
                            &[("type", "snapshot")],
                        );
                        tracing::error!(epoch = current_epoch, error = %e, "Snapshot task panicked");
                    }
                }

                // Clear in_progress
                in_progress.store(false, Ordering::SeqCst);

                // Check if another snapshot was scheduled
                if !scheduled.swap(false, Ordering::SeqCst) {
                    // No scheduled snapshot, we're done
                    metrics.record_gauge("frogdb_snapshot_in_progress", 0.0, &[]);
                    break;
                }

                // Try to acquire in_progress for the scheduled snapshot
                if in_progress
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    // Someone else started a snapshot, we're done
                    metrics.record_gauge("frogdb_snapshot_in_progress", 0.0, &[]);
                    break;
                }

                // Increment epoch for the scheduled snapshot
                current_epoch = epoch_counter.fetch_add(1, Ordering::SeqCst) + 1;
                start = Instant::now();

                // Update metrics for the new snapshot
                metrics.record_gauge("frogdb_snapshot_epoch", current_epoch as f64, &[]);

                tracing::info!(epoch = current_epoch, "Starting scheduled snapshot");
            }
        });

        // Return handle immediately (background task completes asynchronously)
        // The completion callback is empty since the background task manages state
        Ok(SnapshotHandle::new(initial_epoch, || {}))
    }

    fn last_save_time(&self) -> Option<Instant> {
        *self.last_save_time.read().unwrap()
    }

    fn in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata> {
        self.last_metadata
            .read()
            .unwrap()
            .as_ref()
            .map(|m| m.to_metadata())
    }

    fn schedule_snapshot(&self) -> bool {
        // Only schedule if a snapshot is in progress
        if !self.in_progress() {
            return false;
        }
        self.scheduled.store(true, Ordering::SeqCst);
        true
    }

    fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::SeqCst)
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
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

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

    // =========================================================================
    // Snapshot scheduling tests
    // =========================================================================

    #[test]
    fn test_noop_schedule_returns_false_when_no_save_in_progress() {
        let coord = NoopSnapshotCoordinator::new();

        // No save in progress, scheduling should return false
        assert!(!coord.schedule_snapshot());
        assert!(!coord.is_scheduled());
    }

    #[test]
    fn test_noop_schedule_returns_true_when_save_in_progress() {
        let coord = NoopSnapshotCoordinator::new();

        // Start a snapshot to make one "in progress"
        let _handle = coord.start_snapshot().unwrap();
        assert!(coord.in_progress());

        // Now scheduling should succeed
        assert!(coord.schedule_snapshot());
        assert!(coord.is_scheduled());
    }

    #[test]
    fn test_noop_is_scheduled_returns_correct_state() {
        let coord = NoopSnapshotCoordinator::new();

        // Initially not scheduled
        assert!(!coord.is_scheduled());

        // Start a snapshot and schedule another
        let _handle = coord.start_snapshot().unwrap();
        coord.schedule_snapshot();

        // Should be scheduled
        assert!(coord.is_scheduled());
    }

    #[test]
    fn test_noop_schedule_multiple_times() {
        let coord = NoopSnapshotCoordinator::new();

        let _handle = coord.start_snapshot().unwrap();

        // Schedule multiple times - all should succeed
        assert!(coord.schedule_snapshot());
        assert!(coord.schedule_snapshot());
        assert!(coord.schedule_snapshot());

        // Should still be scheduled
        assert!(coord.is_scheduled());
    }

    // =========================================================================
    // SnapshotMetadataFile tests
    // =========================================================================

    #[test]
    fn test_metadata_file_new() {
        let metadata = SnapshotMetadataFile::new(1, 12345, 4);

        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.epoch, 1);
        assert_eq!(metadata.sequence_number, 12345);
        assert_eq!(metadata.num_shards, 4);
        assert!(metadata.started_at_ms > 0);
        assert!(metadata.completed_at_ms.is_none());
        assert!(metadata.completion_marker.is_none());
        assert!(!metadata.is_complete());
    }

    #[test]
    fn test_metadata_file_mark_complete() {
        let mut metadata = SnapshotMetadataFile::new(1, 12345, 4);
        metadata.mark_complete(1000, 5_000_000);

        assert!(metadata.is_complete());
        assert!(metadata.completed_at_ms.is_some());
        assert_eq!(metadata.num_keys, 1000);
        assert_eq!(metadata.size_bytes, 5_000_000);
        assert_eq!(
            metadata.completion_marker.as_deref(),
            Some("FROGDB_SNAPSHOT_COMPLETE_v1")
        );
    }

    #[test]
    fn test_metadata_file_to_metadata() {
        let mut metadata_file = SnapshotMetadataFile::new(5, 99999, 8);
        metadata_file.mark_complete(5000, 10_000_000);

        let metadata = metadata_file.to_metadata();

        assert_eq!(metadata.epoch, 5);
        assert_eq!(metadata.num_keys, 5000);
        assert_eq!(metadata.size_bytes, 10_000_000);
        assert!(metadata.started_at.elapsed().unwrap() < Duration::from_secs(1));
        assert!(metadata.completed_at.is_some());
    }

    #[test]
    fn test_metadata_file_serialization() {
        let mut metadata = SnapshotMetadataFile::new(3, 54321, 2);
        metadata.mark_complete(100, 1_000_000);

        // Serialize to JSON
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(json.contains("\"epoch\":3"));
        assert!(json.contains("\"sequence_number\":54321"));
        assert!(json.contains("FROGDB_SNAPSHOT_COMPLETE_v1"));

        // Deserialize back
        let deserialized: SnapshotMetadataFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.epoch, 3);
        assert_eq!(deserialized.sequence_number, 54321);
        assert!(deserialized.is_complete());
        assert_eq!(deserialized.num_keys, 100);
        assert_eq!(deserialized.size_bytes, 1_000_000);
    }

    // =========================================================================
    // SnapshotConfig tests
    // =========================================================================

    #[test]
    fn test_snapshot_config_default() {
        let config = SnapshotConfig::default();

        assert_eq!(config.snapshot_dir, PathBuf::from("./snapshots"));
        assert_eq!(config.snapshot_interval_secs, 3600);
        assert_eq!(config.max_snapshots, 5);
    }

    // =========================================================================
    // RocksSnapshotCoordinator tests (require temp directory)
    // =========================================================================

    #[test]
    fn test_cleanup_old_snapshots_logic() {
        // Test the cleanup function directly with a temp directory
        let temp_dir = std::env::temp_dir().join(format!(
            "frogdb_snapshot_test_cleanup_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create some fake snapshot directories
        for i in 1..=7 {
            let snapshot_dir = temp_dir.join(format!("snapshot_{:05}", i));
            std::fs::create_dir_all(&snapshot_dir).unwrap();
        }

        // Should have 7 snapshots
        let count_snapshots = || {
            std::fs::read_dir(&temp_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
                .count()
        };
        assert_eq!(count_snapshots(), 7);

        // Cleanup to keep only 3
        RocksSnapshotCoordinator::cleanup_old_snapshots(&temp_dir, 3).unwrap();

        // Should have 3 snapshots left
        assert_eq!(count_snapshots(), 3);

        // The remaining should be the most recent ones (5, 6, 7)
        assert!(!temp_dir.join("snapshot_00001").exists());
        assert!(!temp_dir.join("snapshot_00002").exists());
        assert!(!temp_dir.join("snapshot_00003").exists());
        assert!(!temp_dir.join("snapshot_00004").exists());
        assert!(temp_dir.join("snapshot_00005").exists());
        assert!(temp_dir.join("snapshot_00006").exists());
        assert!(temp_dir.join("snapshot_00007").exists());

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_cleanup_unlimited_retention() {
        let temp_dir = std::env::temp_dir().join(format!(
            "frogdb_snapshot_test_unlimited_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create some fake snapshot directories
        for i in 1..=5 {
            let snapshot_dir = temp_dir.join(format!("snapshot_{:05}", i));
            std::fs::create_dir_all(&snapshot_dir).unwrap();
        }

        // Cleanup with max_snapshots = 0 (unlimited)
        RocksSnapshotCoordinator::cleanup_old_snapshots(&temp_dir, 0).unwrap();

        // All 5 should remain
        let count = std::fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
            .count();
        assert_eq!(count, 5);

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
