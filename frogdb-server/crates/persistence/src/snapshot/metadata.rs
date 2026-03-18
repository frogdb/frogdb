//! Snapshot metadata types.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const COMPLETION_MARKER: &str = "FROGDB_SNAPSHOT_COMPLETE_v1";

#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    pub epoch: u64,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub num_keys: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadataFile {
    pub version: u8,
    pub epoch: u64,
    pub sequence_number: u64,
    pub started_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub num_shards: usize,
    pub num_keys: u64,
    pub size_bytes: u64,
    pub completion_marker: Option<String>,
}

impl SnapshotMetadataFile {
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
    pub fn mark_complete(&mut self, num_keys: u64, size_bytes: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        self.completed_at_ms = Some(now.as_millis() as u64);
        self.num_keys = num_keys;
        self.size_bytes = size_bytes;
        self.completion_marker = Some(COMPLETION_MARKER.to_string());
    }
    pub fn is_complete(&self) -> bool {
        self.completion_marker.as_deref() == Some(COMPLETION_MARKER)
    }
    pub fn to_metadata(&self) -> SnapshotMetadata {
        SnapshotMetadata {
            epoch: self.epoch,
            started_at: UNIX_EPOCH + Duration::from_millis(self.started_at_ms),
            completed_at: self
                .completed_at_ms
                .map(|ms| UNIX_EPOCH + Duration::from_millis(ms)),
            num_keys: self.num_keys,
            size_bytes: self.size_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub snapshot_dir: PathBuf,
    pub snapshot_interval_secs: u64,
    pub max_snapshots: usize,
}
impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: PathBuf::from("./snapshots"),
            snapshot_interval_secs: 3600,
            max_snapshots: 5,
        }
    }
}
