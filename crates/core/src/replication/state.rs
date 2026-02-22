//! Replication state management.
//!
//! This module handles the persistent state required for replication:
//! - Replication ID (40-char hex string)
//! - Secondary replication ID (for PSYNC continuity after failover)
//! - Current replication offset

use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Length of the replication ID in characters (40 hex chars = 20 bytes)
pub const REPLICATION_ID_LEN: usize = 40;

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
<<<<<<< HEAD
        let contents = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
||||||| parent of 670778b (more fixing stuff?)
        let contents = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
=======
        let contents = serde_json::to_string_pretty(self)
            .map_err(io::Error::other)?;
>>>>>>> 670778b (more fixing stuff?)
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

    /// Check if a PSYNC request can be satisfied with partial sync.
    ///
    /// Returns `true` if the requested replication ID and offset can be
    /// continued from this node's replication stream.
    pub fn can_partial_sync(&self, requested_id: &str, requested_offset: u64) -> bool {
        // Check primary ID
        if requested_id == self.replication_id && requested_offset <= self.replication_offset {
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
}

/// Generate a new random replication ID.
///
/// The ID is 40 hexadecimal characters (representing 20 random bytes),
/// matching the Redis replication ID format.
pub fn generate_replication_id() -> String {
    let mut rng = rand::thread_rng();
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
    fn test_can_partial_sync() {
        let mut state = ReplicationState::new();
        state.replication_offset = 1000;

        // Can sync with current ID and valid offset
        assert!(state.can_partial_sync(&state.replication_id.clone(), 500));
        assert!(state.can_partial_sync(&state.replication_id.clone(), 1000));

        // Cannot sync with future offset
        assert!(!state.can_partial_sync(&state.replication_id.clone(), 1001));

        // Cannot sync with unknown ID
        assert!(!state.can_partial_sync("unknown_id", 500));

        // Test secondary ID after failover
        let old_id = state.replication_id.clone();
        state.new_replication_id();

        // Can still sync with old ID up to secondary_offset
        assert!(state.can_partial_sync(&old_id, 500));
        assert!(state.can_partial_sync(&old_id, 1000));
        assert!(!state.can_partial_sync(&old_id, 1001));
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
