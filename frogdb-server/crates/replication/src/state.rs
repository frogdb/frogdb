//! Replication state management.
//!
//! This module handles the persistent state required for replication:
//! - Replication ID (40-char hex string)
//! - Secondary replication ID (for PSYNC continuity after failover)
//! - Current replication offset

use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Length of the replication ID in characters (40 hex chars = 20 bytes)
pub const REPLICATION_ID_LEN: usize = 40;

/// File name of the staged full-sync replication metadata.
///
/// A replica writes this file into `checkpoint_ready/` when it receives a
/// checkpoint full sync. When the staged checkpoint is installed on the next
/// boot, the file is carried into the data directory and describes the
/// replication identity + offset that matches the freshly installed snapshot.
pub const STAGED_METADATA_FILE: &str = "replication_metadata.json";

/// Replication metadata staged alongside a full-sync checkpoint.
///
/// Mirrors the JSON written by the replica connection state machine
/// (`receive_checkpoint`). The offset describes the snapshot's position in the
/// replication stream — the standard model that couples offset durability to
/// snapshot durability (Redis stores repl-id + offset in the RDB aux fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedReplicationMetadata {
    /// Primary replication ID the snapshot was taken under.
    pub replication_id: String,
    /// Replication offset matching the snapshot's data.
    pub replication_offset: u64,
    /// Hex-encoded checkpoint checksum (informational; not validated here).
    #[serde(default)]
    pub checksum: Option<String>,
}

/// Read staged full-sync replication metadata from a data directory, if present.
///
/// Returns `Ok(None)` when nothing is staged or when the file is corrupt or
/// carries an invalid replication id. A corrupt/invalid file is deliberately
/// treated as absent so recovery falls back to a full resync (offset 0 →
/// `PSYNC ? -1`) rather than crashing or trusting garbage — matching Redis,
/// where a mismatched replid forces a full sync.
pub fn read_staged_replication_metadata(
    data_dir: &Path,
) -> io::Result<Option<StagedReplicationMetadata>> {
    let path = ReplicationState::staged_metadata_path(data_dir);
    match fs::read_to_string(&path) {
        Ok(contents) => match serde_json::from_str::<StagedReplicationMetadata>(&contents) {
            Ok(meta) if is_valid_replication_id(&meta.replication_id) => Ok(Some(meta)),
            Ok(_) => {
                tracing::warn!(
                    path = %path.display(),
                    "Staged replication metadata has an invalid replication id; ignoring"
                );
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to parse staged replication metadata; ignoring"
                );
                Ok(None)
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Remove the staged replication metadata file once it has been consumed.
///
/// Idempotent: a missing file is not an error.
pub fn consume_staged_replication_metadata(data_dir: &Path) {
    let path = ReplicationState::staged_metadata_path(data_dir);
    if let Err(e) = fs::remove_file(&path)
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(
            path = %path.display(),
            error = %e,
            "Failed to remove staged replication metadata"
        );
    }
}

/// Replication state that is persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationState {
    /// Primary replication ID (40-char hex string).
    /// Generated when a node becomes a primary.
    pub replication_id: String,

    /// Secondary replication ID.
    /// Stores the previous primary's ID for PSYNC continuity after failover.
    /// This allows replicas that were connected to the old primary to
    /// partial-sync with the new primary.
    #[serde(default)]
    pub secondary_id: Option<String>,

    /// Current replication offset.
    /// This is the byte offset in the replication stream.
    pub replication_offset: u64,

    /// The offset at which secondary_id is valid.
    /// Replicas can use secondary_id for PSYNC if their offset is <= this value.
    #[serde(default)]
    pub secondary_offset: i64,

    /// The finalized active version. `None` means pre-versioning (original install,
    /// no finalization has ever occurred). Gates check this to decide behavior.
    #[serde(default)]
    pub active_version: Option<String>,

    /// Primary host (runtime-only, not persisted). Set when running as a replica.
    #[serde(skip)]
    pub master_host: Option<String>,

    /// Primary port (runtime-only, not persisted). Set when running as a replica.
    #[serde(skip)]
    pub master_port: Option<u16>,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationState {
    /// Create a new replication state with a fresh replication ID.
    pub fn new() -> Self {
        Self {
            replication_id: generate_replication_id(),
            secondary_id: None,
            replication_offset: 0,
            secondary_offset: -1,
            active_version: None,
            master_host: None,
            master_port: None,
        }
    }

    /// Load replication state from a file, or create new if file doesn't exist or is corrupted.
    pub fn load_or_create(path: &Path) -> io::Result<Self> {
        match fs::read_to_string(path) {
            Ok(contents) => {
                match serde_json::from_str::<ReplicationState>(&contents) {
                    Ok(state) => {
                        // Validate the loaded state
                        if state.validate() {
                            tracing::info!(
                                replication_id = %state.replication_id,
                                offset = state.replication_offset,
                                "Loaded replication state from disk"
                            );
                            Ok(state)
                        } else {
                            tracing::warn!(
                                "Replication state file is corrupted, generating new state"
                            );
                            let new_state = Self::new();
                            new_state.save(path)?;
                            Ok(new_state)
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to parse replication state file, generating new state"
                        );
                        let new_state = Self::new();
                        new_state.save(path)?;
                        Ok(new_state)
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tracing::info!("Replication state file not found, generating new state");
                let new_state = Self::new();
                new_state.save(path)?;
                Ok(new_state)
            }
            Err(e) => Err(e),
        }
    }

    /// Save replication state to a file.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write atomically using a temp file
        let temp_path = path.with_extension("tmp");
        let contents = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&temp_path, contents)?;
        fs::rename(temp_path, path)?;

        tracing::debug!(
            replication_id = %self.replication_id,
            offset = self.replication_offset,
            "Saved replication state to disk"
        );
        Ok(())
    }

    /// Validate the replication state.
    pub fn validate(&self) -> bool {
        // Check replication ID format
        if !is_valid_replication_id(&self.replication_id) {
            return false;
        }

        // Check secondary ID if present
        if let Some(ref secondary) = self.secondary_id
            && !is_valid_replication_id(secondary)
        {
            return false;
        }

        true
    }

    /// Generate a new replication ID (for when becoming primary).
    pub fn new_replication_id(&mut self) {
        // Store current ID as secondary for failover continuity
        self.secondary_id = Some(self.replication_id.clone());
        self.secondary_offset = self.replication_offset as i64;

        // Generate new primary ID
        self.replication_id = generate_replication_id();

        tracing::info!(
            new_id = %self.replication_id,
            secondary_id = ?self.secondary_id,
            "Generated new replication ID"
        );
    }

    /// Check whether a PSYNC request's offset window can be continued from this
    /// node's replication stream.
    ///
    /// `current_offset` is the primary's **live** replication offset — the live
    /// write position advanced by `broadcast_command`. It is supplied by the
    /// [`crate::offset_coordinator::OffsetCoordinator`], the sole caller, which
    /// owns the live offset; this method never reads `self.replication_offset`
    /// (the persisted field lags the live stream head, so checking against it
    /// made every reconnect fall outside the window and forced a full resync).
    /// The secondary-ID branch keeps using `self.secondary_offset`, which is a
    /// frozen failover boundary, not a live position.
    ///
    /// Returns `true` if the requested replication ID and offset fall within the
    /// continuable window. Note this only validates the *offset window*; the
    /// caller is responsible for confirming it can actually deliver the backlog
    /// range `(requested_offset, current_offset]` before granting `+CONTINUE`.
    pub fn window_contains(
        &self,
        requested_id: &str,
        requested_offset: u64,
        current_offset: u64,
    ) -> bool {
        // Check primary ID against the live write position.
        if requested_id == self.replication_id && requested_offset <= current_offset {
            return true;
        }

        // Check secondary ID (for failover continuity)
        if let Some(ref secondary) = self.secondary_id
            && requested_id == secondary
            && self.secondary_offset >= 0
            && requested_offset <= self.secondary_offset as u64
        {
            return true;
        }

        false
    }

    /// Increment the replication offset.
    #[inline]
    pub fn increment_offset(&mut self, bytes: u64) {
        self.replication_offset = self.replication_offset.saturating_add(bytes);
    }

    /// Get the default path for the replication state file.
    pub fn default_path(data_dir: &Path) -> PathBuf {
        data_dir.join("replication_state.json")
    }

    /// Path of the staged full-sync replication metadata inside a data directory.
    pub fn staged_metadata_path(data_dir: &Path) -> PathBuf {
        data_dir.join(STAGED_METADATA_FILE)
    }

    /// Adopt the replication identity + offset from staged full-sync metadata.
    ///
    /// Called after a staged checkpoint is installed: the metadata describes the
    /// offset that matches the recovered snapshot, so it overrides whatever the
    /// (now stale or freshly generated) state file held. Runtime-only fields
    /// (`master_host`/`master_port`) are preserved.
    pub fn apply_staged_metadata(&mut self, meta: &StagedReplicationMetadata) {
        self.replication_id = meta.replication_id.clone();
        self.replication_offset = meta.replication_offset;
    }
}

/// Generate a new random replication ID.
///
/// The ID is 40 hexadecimal characters (representing 20 random bytes),
/// matching the Redis replication ID format.
pub fn generate_replication_id() -> String {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 20];
    rng.fill(&mut bytes);

    // Convert to hex string
    bytes.iter().fold(String::with_capacity(40), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
        s
    })
}

/// Check if a string is a valid replication ID.
pub fn is_valid_replication_id(id: &str) -> bool {
    id.len() == REPLICATION_ID_LEN && id.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_generate_replication_id() {
        let id = generate_replication_id();
        assert_eq!(id.len(), REPLICATION_ID_LEN);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));

        // Generate another and ensure they're different
        let id2 = generate_replication_id();
        assert_ne!(id, id2);
    }

    #[test]
    fn test_is_valid_replication_id() {
        // Valid ID
        assert!(is_valid_replication_id(
            "0123456789abcdef0123456789abcdef01234567"
        ));

        // Wrong length
        assert!(!is_valid_replication_id("0123456789abcdef"));

        // Invalid characters
        assert!(!is_valid_replication_id(
            "0123456789abcdef0123456789abcdef0123456g"
        ));

        // Empty
        assert!(!is_valid_replication_id(""));
    }

    #[test]
    fn test_replication_state_new() {
        let state = ReplicationState::new();
        assert!(is_valid_replication_id(&state.replication_id));
        assert!(state.secondary_id.is_none());
        assert_eq!(state.replication_offset, 0);
        assert!(state.validate());
    }

    #[test]
    fn test_replication_state_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("replication_state.json");

        // Create and save
        let state = ReplicationState::new();
        let original_id = state.replication_id.clone();
        state.save(&path).unwrap();

        // Load and verify
        let loaded = ReplicationState::load_or_create(&path).unwrap();
        assert_eq!(loaded.replication_id, original_id);
        assert_eq!(loaded.replication_offset, 0);
    }

    #[test]
    fn test_replication_state_load_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        // Load from nonexistent file should create new
        let state = ReplicationState::load_or_create(&path).unwrap();
        assert!(state.validate());

        // File should now exist
        assert!(path.exists());
    }

    #[test]
    fn test_replication_state_load_corrupted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupted.json");

        // Write corrupted data
        fs::write(&path, "not valid json").unwrap();

        // Load should create new state
        let state = ReplicationState::load_or_create(&path).unwrap();
        assert!(state.validate());
    }

    #[test]
    fn test_replication_state_new_replication_id() {
        let mut state = ReplicationState::new();
        let original_id = state.replication_id.clone();
        state.replication_offset = 1000;

        state.new_replication_id();

        // New ID should be different
        assert_ne!(state.replication_id, original_id);

        // Old ID should be saved as secondary
        assert_eq!(state.secondary_id, Some(original_id));
        assert_eq!(state.secondary_offset, 1000);
    }

    #[test]
    fn test_window_contains() {
        let mut state = ReplicationState::new();
        // The live offset is supplied by the caller (the coordinator),
        // independent of the persisted `replication_offset` field. Leave the
        // field at its default to prove the window check no longer reads it for
        // the primary branch.
        let live_offset = 1000;

        // Can sync with current ID and valid offset
        assert!(state.window_contains(&state.replication_id.clone(), 500, live_offset));
        assert!(state.window_contains(&state.replication_id.clone(), 1000, live_offset));

        // Cannot sync with future offset
        assert!(!state.window_contains(&state.replication_id.clone(), 1001, live_offset));

        // Cannot sync with unknown ID
        assert!(!state.window_contains("unknown_id", 500, live_offset));

        // Test secondary ID after failover. `new_replication_id` freezes
        // `secondary_offset` from the persisted offset, so set it explicitly.
        state.replication_offset = 1000;
        let old_id = state.replication_id.clone();
        state.new_replication_id();

        // Can still sync with old ID up to secondary_offset (the frozen failover
        // boundary), regardless of the current live offset.
        assert!(state.window_contains(&old_id, 500, live_offset));
        assert!(state.window_contains(&old_id, 1000, live_offset));
        assert!(!state.window_contains(&old_id, 1001, live_offset));
    }

    #[test]
    fn test_read_staged_replication_metadata_missing() {
        let dir = tempdir().unwrap();
        // No file staged -> None, not an error.
        let result = read_staged_replication_metadata(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_staged_replication_metadata_valid() {
        let dir = tempdir().unwrap();
        let id = generate_replication_id();
        let json = serde_json::json!({
            "replication_id": id,
            "replication_offset": 4242u64,
            "checksum": "deadbeef",
        });
        fs::write(
            ReplicationState::staged_metadata_path(dir.path()),
            json.to_string(),
        )
        .unwrap();

        let meta = read_staged_replication_metadata(dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(meta.replication_id, id);
        assert_eq!(meta.replication_offset, 4242);
        assert_eq!(meta.checksum.as_deref(), Some("deadbeef"));

        // Applying it overrides the state's id + offset.
        let mut state = ReplicationState::new();
        state.apply_staged_metadata(&meta);
        assert_eq!(state.replication_id, id);
        assert_eq!(state.replication_offset, 4242);
    }

    #[test]
    fn test_read_staged_replication_metadata_corrupt_is_ignored() {
        let dir = tempdir().unwrap();
        fs::write(
            ReplicationState::staged_metadata_path(dir.path()),
            "not valid json",
        )
        .unwrap();
        // Corrupt metadata is treated as absent (forces full resync), not a crash.
        assert!(
            read_staged_replication_metadata(dir.path())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_read_staged_replication_metadata_invalid_id_is_ignored() {
        let dir = tempdir().unwrap();
        let json = serde_json::json!({
            "replication_id": "tooshort",
            "replication_offset": 10u64,
        });
        fs::write(
            ReplicationState::staged_metadata_path(dir.path()),
            json.to_string(),
        )
        .unwrap();
        assert!(
            read_staged_replication_metadata(dir.path())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_consume_staged_replication_metadata() {
        let dir = tempdir().unwrap();
        let path = ReplicationState::staged_metadata_path(dir.path());
        fs::write(&path, "{}").unwrap();
        assert!(path.exists());
        consume_staged_replication_metadata(dir.path());
        assert!(!path.exists());
        // Idempotent: removing again is a no-op.
        consume_staged_replication_metadata(dir.path());
    }

    #[test]
    fn test_increment_offset() {
        let mut state = ReplicationState::new();
        assert_eq!(state.replication_offset, 0);

        state.increment_offset(100);
        assert_eq!(state.replication_offset, 100);

        state.increment_offset(50);
        assert_eq!(state.replication_offset, 150);

        // Test saturation (no overflow)
        state.replication_offset = u64::MAX - 10;
        state.increment_offset(100);
        assert_eq!(state.replication_offset, u64::MAX);
    }
}
