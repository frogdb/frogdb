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
/// The name is owned by the staged-checkpoint contract in `frogdb-persistence`
/// (`rocks::staged`); this is a re-export, not a second definition.
pub const STAGED_METADATA_FILE: &str =
    frogdb_persistence::rocks::staged::STAGED_REPLICATION_METADATA_FILE;

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

/// Throw away every artifact of an inherited full sync: the staged checkpoint
/// directory and the replication metadata that rides with it (both the copy
/// inside the staged dir and any copy a previous install already carried into
/// `data_dir`).
///
/// A staged checkpoint is normally left on disk after a runtime install so a
/// crash mid-install re-installs the same snapshot on the next boot
/// (`replica/connection.rs`, `recovery/checkpoint.rs`). That replay is only
/// harmless while the node is still following the history the snapshot came
/// from. Once the node is **promoted** it heads its own history: reinstalling
/// the inherited snapshot would move its live database aside and resurrect the
/// old primary's replication id, silently discarding every write it took as a
/// primary. So promotion consumes the staging area instead of leaving it armed.
///
/// Disarmed atomically: the staged directory is **renamed aside first**, which
/// is the single step that makes the boot installer stop seeing it, and only
/// then deleted. A recursive delete is not atomic — interrupted halfway it
/// leaves a partial directory that the installer may still accept, so a crash
/// during promotion could resurrect a mangled snapshot. After the rename the
/// leftover `*.discarded` directory is inert (the installer only looks at the
/// exact staged name), so its removal is best-effort.
///
/// Idempotent: missing files are not an error. A rename failure **is** an error
/// and is propagated — the staging area is still armed, so the caller must not
/// complete a promotion on top of it.
pub fn discard_staged_full_sync(data_dir: &Path) -> io::Result<()> {
    if let Some(staged) = frogdb_persistence::rocks::staged::StagedCheckpoint::for_db_dir(data_dir)
        && staged.exists()
    {
        let disarmed = staged.dir().with_extension("discarded");
        // A leftover from an earlier discard whose delete did not finish: a
        // rename onto a non-empty directory fails, so clear the target first.
        if disarmed.exists()
            && let Err(e) = fs::remove_dir_all(&disarmed)
        {
            tracing::warn!(
                path = %disarmed.display(),
                error = %e,
                "Failed to clear a leftover discarded checkpoint directory"
            );
        }
        fs::rename(staged.dir(), &disarmed).map_err(|e| {
            tracing::error!(
                path = %staged.dir().display(),
                error = %e,
                "Failed to disarm the staged full-sync checkpoint"
            );
            e
        })?;
        tracing::info!(
            path = %staged.dir().display(),
            "Discarded the staged full-sync checkpoint inherited from the previous primary"
        );
        if let Err(e) = fs::remove_dir_all(&disarmed) {
            tracing::warn!(
                path = %disarmed.display(),
                error = %e,
                "Discarded checkpoint is disarmed but could not be deleted"
            );
        }
    }
    consume_staged_replication_metadata(data_dir);
    Ok(())
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

    /// Replication offset reconciled AT THE LAST SAVE POINT — NOT a live stream
    /// position on either role. On a Primary the live head lives in
    /// [`crate::offset_coordinator::OffsetCoordinator`]; on a Replica it lives in
    /// [`crate::replica::offset::ReplicaOffset`]. This field is reconciled up to
    /// the live head only at save points, and it lags between them. Renamed from
    /// `replication_offset` (kept as a serde alias for one release so existing
    /// on-disk state still loads) so no reader mistakes it for the live head.
    #[serde(alias = "replication_offset")]
    pub offset_at_save: u64,

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
            offset_at_save: 0,
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
                                offset_at_save = state.offset_at_save,
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
            offset_at_save = self.offset_at_save,
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
    ///
    /// `live_offset` is the current *live* replication offset — the failover
    /// continuity boundary is frozen from it, not from [`Self::offset_at_save`].
    /// Because the persisted field lags the live head between save points, taking
    /// the live value as an argument keeps `secondary_offset` from ever being
    /// frozen behind where the stream has actually reached.
    pub fn new_replication_id(&mut self, live_offset: u64) {
        let minted = generate_replication_id();
        self.shift_replication_id(minted, live_offset);
        tracing::info!(
            new_id = %self.replication_id,
            secondary_id = ?self.secondary_id,
            "Generated new replication ID"
        );
    }

    /// Adopt `new_id` as the primary history, demoting the current id into the
    /// failover window frozen at `live_offset`.
    ///
    /// Redis's `shiftReplicationId()`. Two callers: minting on promotion
    /// ([`Self::new_replication_id`]) and a replica adopting the id carried by a
    /// `+CONTINUE` — in both cases everything up to `live_offset` is byte-identical
    /// under the old id, so downstream replicas following it can still be resumed.
    ///
    /// `live_offset` is **inclusive** here: FrogDB's window check is
    /// `requested_offset <= secondary_offset`, so it does not carry Redis's `+1`
    /// (see [`Self::window_contains`]).
    pub fn shift_replication_id(&mut self, new_id: String, live_offset: u64) {
        self.secondary_id = Some(std::mem::replace(&mut self.replication_id, new_id));
        self.secondary_offset = live_offset as i64;
    }

    /// Drop the failover continuity window (`secondary_id` / `secondary_offset`).
    ///
    /// The analogue of Redis's `clearReplicationId2()`. The window says "I used
    /// to be `secondary_id` up to `secondary_offset`, so I can still serve a
    /// partial resync to anyone who was following that history". The moment this
    /// node adopts a *different* node's history — every `+FULLRESYNC` and every
    /// checkpoint install — that claim becomes a lie: the node no longer holds
    /// the old stream's data, and honoring a `+CONTINUE` against it would ship a
    /// tail from the new history under the old id. Redis clears the window at
    /// exactly these points (`replication.c` `readSyncBulkPayload`).
    pub fn clear_secondary_window(&mut self) {
        self.secondary_id = None;
        self.secondary_offset = -1;
    }

    /// Adopt `replication_id` as this node's history, dropping any stale failover
    /// window in the same step.
    ///
    /// Every replica-side adoption point (`+FULLRESYNC`, `+CONTINUE` id refresh,
    /// checkpoint metadata) goes through here so the window can never outlive the
    /// history it described. See [`Self::clear_secondary_window`].
    pub fn adopt_replication_history(&mut self, replication_id: String) {
        self.replication_id = replication_id;
        self.clear_secondary_window();
    }

    /// Check whether a PSYNC request's offset window can be continued from this
    /// node's replication stream.
    ///
    /// `current_offset` is the primary's **live** replication offset — the live
    /// write position advanced by `broadcast_command`. It is supplied by the
    /// [`crate::offset_coordinator::OffsetCoordinator`], the sole caller, which
    /// owns the live offset; this method never reads `self.offset_at_save`
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
        // Adopting a checkpoint means adopting the primary's history wholesale —
        // any failover window this node carried described a stream it no longer
        // holds, so it goes with it.
        self.adopt_replication_history(meta.replication_id.clone());
        // A staged checkpoint offset *is* a save-point offset.
        self.offset_at_save = meta.replication_offset;
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
        assert_eq!(state.offset_at_save, 0);
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
        assert_eq!(loaded.offset_at_save, 0);
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

        // The failover boundary is frozen from the LIVE offset passed in, not
        // from the persisted `offset_at_save`.
        state.new_replication_id(1000);

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
        // `secondary_offset` from the live offset passed in.
        let old_id = state.replication_id.clone();
        state.new_replication_id(1000);

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
        assert_eq!(state.offset_at_save, 4242);
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
    fn discard_staged_full_sync_disarms_the_staging_area() {
        let parent = tempdir().unwrap();
        let data_dir = parent.path().join("db");
        fs::create_dir_all(&data_dir).unwrap();
        let staged =
            frogdb_persistence::rocks::staged::StagedCheckpoint::for_db_dir(&data_dir).unwrap();
        fs::create_dir_all(staged.dir()).unwrap();
        fs::write(staged.replication_metadata_path(), "{}").unwrap();
        // A previous install may already have carried a copy into the data dir.
        let carried = ReplicationState::staged_metadata_path(&data_dir);
        fs::write(&carried, "{}").unwrap();

        discard_staged_full_sync(&data_dir).unwrap();

        assert!(
            !staged.exists(),
            "a promoted node must not leave an inherited checkpoint armed for the next boot"
        );
        assert!(!carried.exists());
        // Idempotent: a second promotion (or a node that never full-synced)
        // finds nothing to discard and does not fail.
        discard_staged_full_sync(&data_dir).unwrap();
    }

    #[test]
    fn discard_staged_full_sync_disarms_before_deleting() {
        let parent = tempdir().unwrap();
        let data_dir = parent.path().join("db");
        fs::create_dir_all(&data_dir).unwrap();
        let staged =
            frogdb_persistence::rocks::staged::StagedCheckpoint::for_db_dir(&data_dir).unwrap();
        fs::create_dir_all(staged.dir()).unwrap();
        // A leftover from an earlier discard whose delete never finished. The
        // rename target must be cleared, not collided with, or the staging area
        // would stay armed.
        let disarmed = staged.dir().with_extension("discarded");
        fs::create_dir_all(&disarmed).unwrap();
        fs::write(disarmed.join("stale.sst"), b"junk").unwrap();

        discard_staged_full_sync(&data_dir).unwrap();

        assert!(!staged.exists(), "staging area must be disarmed");
        assert!(!disarmed.exists(), "the renamed copy is deleted afterwards");
    }

    #[test]
    fn discard_staged_full_sync_keeps_the_metadata_when_disarming_fails() {
        let parent = tempdir().unwrap();
        let data_dir = parent.path().join("db");
        fs::create_dir_all(&data_dir).unwrap();
        let staged =
            frogdb_persistence::rocks::staged::StagedCheckpoint::for_db_dir(&data_dir).unwrap();
        fs::create_dir_all(staged.dir()).unwrap();
        let carried = ReplicationState::staged_metadata_path(&data_dir);
        fs::write(&carried, "{}").unwrap();

        // Make both the pre-clear and the rename fail: the rename target is a
        // non-empty directory (rename onto it is ENOTEMPTY) whose entries cannot
        // be unlinked (no write permission on the directory).
        let disarmed = staged.dir().with_extension("discarded");
        fs::create_dir_all(disarmed.join("child")).unwrap();
        let mut perms = fs::metadata(&disarmed).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&disarmed, perms).unwrap();
        let restore = |dir: &Path| {
            let mut perms = fs::metadata(dir).unwrap().permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            fs::set_permissions(dir, perms).unwrap();
        };
        if fs::write(disarmed.join("probe"), b"x").is_ok() {
            // Permission checks are bypassed (running as root); this failure
            // cannot be provoked here.
            restore(&disarmed);
            return;
        }

        let result = discard_staged_full_sync(&data_dir);
        restore(&disarmed);

        assert!(
            result.is_err(),
            "a still-armed staging area must fail the promotion, not be ignored"
        );
        assert!(
            staged.exists(),
            "the staging area is still armed — the caller must see that"
        );
        assert!(
            carried.exists(),
            "metadata must survive a failed disarm so the aborted promotion stays consistent"
        );
    }

    #[test]
    fn offset_at_save_loads_from_legacy_replication_offset_key() {
        // Existing on-disk state files were written with the old
        // `replication_offset` key; the serde alias keeps them loadable so a
        // boot-time field rename never rewinds the persisted offset.
        let id = generate_replication_id();
        let json = serde_json::json!({
            "replication_id": id,
            "replication_offset": 9876u64,
            "secondary_offset": -1,
        });
        let state: ReplicationState = serde_json::from_str(&json.to_string()).unwrap();
        assert_eq!(state.offset_at_save, 9876);

        // The new key also loads (round-trip through the current field name).
        let json = serde_json::json!({
            "replication_id": id,
            "offset_at_save": 1234u64,
            "secondary_offset": -1,
        });
        let state: ReplicationState = serde_json::from_str(&json.to_string()).unwrap();
        assert_eq!(state.offset_at_save, 1234);
    }

    #[test]
    fn shift_replication_id_freezes_window_inclusively() {
        let mut state = ReplicationState::new();
        let old = state.replication_id.clone();
        state.new_replication_id(100);

        assert_eq!(state.secondary_id.as_deref(), Some(old.as_str()));
        // Inclusive boundary: no Redis-style `+1`, because `window_contains`
        // compares with `<=`.
        assert_eq!(state.secondary_offset, 100);
        assert_ne!(state.replication_id, old);
        assert!(state.window_contains(&old, 100, 100));
        assert!(!state.window_contains(&old, 101, 200));
    }

    #[test]
    fn clear_secondary_window_closes_the_old_history() {
        let mut state = ReplicationState::new();
        let old = state.replication_id.clone();
        state.new_replication_id(100);
        assert!(state.window_contains(&old, 50, 100));

        state.clear_secondary_window();

        assert!(state.secondary_id.is_none());
        assert_eq!(
            state.secondary_offset, -1,
            "-1 is the INFO 'no window' sentinel"
        );
        assert!(!state.window_contains(&old, 50, 100));
        // The primary branch is untouched.
        let current = state.replication_id.clone();
        assert!(state.window_contains(&current, 50, 100));
    }

    #[test]
    fn adopt_replication_history_drops_a_stale_window() {
        let mut state = ReplicationState::new();
        let old = state.replication_id.clone();
        state.new_replication_id(100);

        let adopted = generate_replication_id();
        state.adopt_replication_history(adopted.clone());

        assert_eq!(state.replication_id, adopted);
        assert!(state.secondary_id.is_none());
        assert_eq!(state.secondary_offset, -1);
        assert!(
            !state.window_contains(&old, 50, 100),
            "a node that adopted someone else's history no longer holds the old stream"
        );
    }

    #[test]
    fn apply_staged_metadata_drops_a_stale_window() {
        let mut state = ReplicationState::new();
        let old = state.replication_id.clone();
        state.new_replication_id(100);

        let meta = StagedReplicationMetadata {
            replication_id: generate_replication_id(),
            replication_offset: 4242,
            checksum: None,
        };
        state.apply_staged_metadata(&meta);

        assert_eq!(state.replication_id, meta.replication_id);
        assert_eq!(state.offset_at_save, 4242);
        assert!(state.secondary_id.is_none());
        assert!(!state.window_contains(&old, 50, 4242));
    }
}
