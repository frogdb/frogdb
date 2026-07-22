//! The checkpoint stage/verify/commit seam.
//!
//! [`CheckpointStager`] owns the persistence-side steps of a full-sync
//! checkpoint — verify the combined checksum, remove any stale staged dir,
//! rename the received scratch dir onto the staged dir (the *writer's commit
//! point* in the [`StagedCheckpoint`] contract), and stamp
//! `replication_metadata.json`. It is pure `tokio::fs` + [`StagedCheckpoint`]
//! path math: it touches no socket and no `RocksStore`. The actual database
//! swap is the boot-time **Installer** (`RocksStore::load_staged_checkpoint`),
//! a different party entirely.
//!
//! **Contract: `commit` *stages* the checkpoint — it does NOT install it.** The
//! returned [`StagedOutcome`] is the caller's licence to adopt the offset and
//! then proceed to live streaming; the on-disk DB is unchanged until the next
//! boot. This is the streaming-before-install invariant, expressed as a type
//! rather than the prose comment it used to be.
//!
//! The symmetric encoder on the primary side is
//! `ReplicaSession::stream_checkpoint` (`replica_session.rs`); it lives on the
//! opposite side of the wire and is deliberately not folded together with this.

use crate::fullsync::FullSyncMetadata;
use crate::state::StagedReplicationMetadata;
use frogdb_persistence::rocks::staged::StagedCheckpoint;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;

/// Name of the scratch directory received checkpoint files land in before the
/// commit rename.
const INCOMING_DIR_NAME: &str = "checkpoint_incoming";

/// What a committed stage yields the caller: the replication identity + offset
/// the just-staged snapshot corresponds to.
///
/// Adopting these is the caller's job, deliberately kept *out* of the stager so
/// the mutation of live `ReplicationState` / the replica offset atomic stays on
/// the connection that owns it.
///
/// NOTE: no `checksum` field. The only caller (the connection driver) adopts
/// only `replication_id` + `replication_offset`, so a checksum field would be
/// dead on arrival. The hex encoding still happens inside [`CheckpointStager::commit`]
/// for the on-disk metadata; add a field back only when a real consumer appears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedOutcome {
    pub replication_id: String,
    pub replication_offset: u64,
}

/// Owns verify -> commit -> metadata for one full-sync checkpoint, against the
/// parent of the live db dir. See the module docs for the streaming-before-install
/// contract.
pub struct CheckpointStager {
    parent_dir: PathBuf,
}

impl CheckpointStager {
    /// A stager landing checkpoints into `parent_dir` (the directory that holds
    /// the live db dir).
    pub fn new(parent_dir: &Path) -> Self {
        Self {
            parent_dir: parent_dir.to_path_buf(),
        }
    }

    /// Scratch dir (`checkpoint_incoming`) that received files land in before
    /// the commit rename. The [`receive_checkpoint_files`] loop writes here.
    ///
    /// [`receive_checkpoint_files`]: crate::fullsync::receive_checkpoint_files
    pub fn incoming_dir(&self) -> PathBuf {
        self.parent_dir.join(INCOMING_DIR_NAME)
    }

    /// Verify the combined checksum against `meta`, remove any stale staged dir,
    /// rename `incoming` onto the staged dir (the writer's commit point), and
    /// stamp `replication_metadata.json`.
    ///
    /// The failure asymmetry is load-bearing and reproduced exactly from the
    /// original `receive_checkpoint`:
    /// - **Fatal (return `Err`):** checksum mismatch → remove `incoming`, return
    ///   [`io::ErrorKind::InvalidData`]; rename failure → remove `incoming`,
    ///   return [`io::Error::other`].
    /// - **Non-fatal (`warn!` and continue, still returning `Ok`):** failure to
    ///   remove a stale staged dir before the rename, and any serialize/write
    ///   failure stamping `replication_metadata.json`. These MUST NOT be
    ///   promoted to `Err` — the offset is still adopted and the stage is still
    ///   considered successful.
    pub async fn commit(
        &self,
        incoming: PathBuf,
        computed: [u8; 32],
        meta: &FullSyncMetadata,
    ) -> io::Result<StagedOutcome> {
        // (b) Verify the combined checksum. Mismatch is fatal: scrub the scratch
        // dir and refuse to stage.
        if computed != meta.checksum {
            tracing::error!(expected = %hex::encode(meta.checksum), actual = %hex::encode(computed), "Checkpoint checksum mismatch");
            let _ = fs::remove_dir_all(&incoming).await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint checksum mismatch: received data does not match primary's checksum",
            ));
        }
        tracing::info!(checksum = %hex::encode(computed), "Checkpoint checksum verified");

        // (c) Commit through the typed staging contract shared with the boot-time
        // installer (`frogdb_persistence::rocks::staged`): the rename onto the
        // staged dir is the writer's commit point.
        let staged = StagedCheckpoint::in_parent(&self.parent_dir);
        // Non-fatal: removing a stale staged dir is best-effort.
        if staged.exists()
            && let Err(e) = fs::remove_dir_all(staged.dir()).await
        {
            tracing::warn!(error = %e, "Failed to remove old staged checkpoint");
        }
        // Fatal: a failed rename leaves nothing staged; scrub and error.
        if let Err(e) = fs::rename(&incoming, staged.dir()).await {
            tracing::error!(error = %e, "Failed to stage checkpoint for loading");
            let _ = fs::remove_dir_all(&incoming).await;
            return Err(io::Error::other(format!(
                "Failed to stage checkpoint: {}",
                e
            )));
        }

        // (d) Stamp the staged replication metadata. The wire checksum
        // (`[u8;32]`) is hex-encoded here so the on-disk `Option<String>` type
        // and the wire type stay decoupled. Serialize/write failures are
        // non-fatal — the offset is adopted regardless.
        let staged_meta = StagedReplicationMetadata {
            replication_id: meta.replication_id.clone(),
            replication_offset: meta.replication_offset,
            checksum: Some(hex::encode(meta.checksum)),
        };
        match serde_json::to_string(&staged_meta) {
            Ok(json) => {
                if let Err(e) = fs::write(staged.replication_metadata_path(), json).await {
                    tracing::warn!(error = %e, "Failed to write replication metadata");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to serialize replication metadata");
            }
        }
        tracing::info!(checkpoint_dir = %staged.dir().display(), replication_id = %meta.replication_id, offset = meta.replication_offset, "Checkpoint staged for loading - server restart required to apply");

        Ok(StagedOutcome {
            replication_id: meta.replication_id.clone(),
            replication_offset: meta.replication_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_persistence::rocks::staged::STAGED_REPLICATION_METADATA_FILE;
    use tempfile::tempdir;

    fn meta_with(checksum: [u8; 32], offset: u64) -> FullSyncMetadata {
        FullSyncMetadata {
            rdb_size: 0,
            checksum,
            replication_id: "repl-stager".to_string(),
            replication_offset: offset,
        }
    }

    /// Populate a scratch `incoming` dir with a `CURRENT` marker + one SST so it
    /// looks like a real received checkpoint. Returns the incoming path.
    async fn populate_incoming(stager: &CheckpointStager) -> PathBuf {
        let incoming = stager.incoming_dir();
        fs::create_dir_all(&incoming).await.unwrap();
        fs::write(incoming.join("CURRENT"), b"MANIFEST-000005\n")
            .await
            .unwrap();
        fs::write(incoming.join("000042.sst"), b"payload")
            .await
            .unwrap();
        incoming
    }

    #[tokio::test]
    async fn stager_commit_stages_and_stamps_metadata() {
        let dir = tempdir().unwrap();
        let stager = CheckpointStager::new(dir.path());
        let incoming = populate_incoming(&stager).await;
        let checksum = [0x11; 32];

        let outcome = stager
            .commit(incoming.clone(), checksum, &meta_with(checksum, 777))
            .await
            .unwrap();

        assert_eq!(
            outcome,
            StagedOutcome {
                replication_id: "repl-stager".to_string(),
                replication_offset: 777,
            }
        );

        // The staged dir now holds the moved content and the scratch dir is gone.
        let staged = StagedCheckpoint::in_parent(dir.path());
        assert!(staged.exists());
        assert!(!incoming.exists(), "scratch dir consumed by the rename");
        let current = fs::read(staged.dir().join("CURRENT")).await.unwrap();
        assert_eq!(current, b"MANIFEST-000005\n");

        // replication_metadata.json carries the right replid/offset + hex checksum.
        let json = fs::read_to_string(staged.dir().join(STAGED_REPLICATION_METADATA_FILE))
            .await
            .unwrap();
        let parsed: StagedReplicationMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.replication_id, "repl-stager");
        assert_eq!(parsed.replication_offset, 777);
        assert_eq!(parsed.checksum, Some(hex::encode(checksum)));
    }

    #[tokio::test]
    async fn stager_commit_checksum_mismatch_cleans_up() {
        let dir = tempdir().unwrap();
        let stager = CheckpointStager::new(dir.path());
        let incoming = populate_incoming(&stager).await;

        // Computed differs from the metadata's checksum.
        let err = stager
            .commit(incoming.clone(), [0xAA; 32], &meta_with([0xBB; 32], 5))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // The scratch dir was scrubbed and nothing was staged.
        assert!(!incoming.exists(), "scratch dir removed on mismatch");
        assert!(
            !StagedCheckpoint::in_parent(dir.path()).exists(),
            "no staged dir created on mismatch"
        );
    }

    #[tokio::test]
    async fn stager_commit_removes_stale_staged() {
        let dir = tempdir().unwrap();
        let stager = CheckpointStager::new(dir.path());

        // A pre-existing staged dir with old content.
        let staged = StagedCheckpoint::in_parent(dir.path());
        fs::create_dir_all(staged.dir()).await.unwrap();
        fs::write(staged.dir().join("STALE"), b"old").await.unwrap();
        fs::write(staged.dir().join("CURRENT"), b"old-manifest")
            .await
            .unwrap();

        let incoming = populate_incoming(&stager).await;
        let checksum = [0x22; 32];
        stager
            .commit(incoming, checksum, &meta_with(checksum, 9))
            .await
            .unwrap();

        // The stale content is gone; the new content won.
        assert!(!staged.dir().join("STALE").exists(), "stale file removed");
        let current = fs::read(staged.dir().join("CURRENT")).await.unwrap();
        assert_eq!(current, b"MANIFEST-000005\n", "new checkpoint content wins");
    }

    #[tokio::test]
    async fn stager_commit_metadata_write_failure_is_non_fatal() {
        // Pre-create the metadata path *as a directory* inside the incoming dir
        // so that after the rename, `fs::write` to that path errors (cannot
        // write a file over a directory). This pins the deliberate non-fatal
        // asymmetry: a metadata-write failure still returns Ok(StagedOutcome).
        let dir = tempdir().unwrap();
        let stager = CheckpointStager::new(dir.path());
        let incoming = populate_incoming(&stager).await;
        // The metadata file name, but as a directory — survives the rename.
        fs::create_dir_all(incoming.join(STAGED_REPLICATION_METADATA_FILE))
            .await
            .unwrap();
        let checksum = [0x33; 32];

        let outcome = stager
            .commit(incoming, checksum, &meta_with(checksum, 42))
            .await
            .expect("metadata-write failure must be non-fatal");

        assert_eq!(outcome.replication_offset, 42);
        assert_eq!(outcome.replication_id, "repl-stager");
        // The staged dir is intact (rename succeeded, only the stamp failed).
        let staged = StagedCheckpoint::in_parent(dir.path());
        assert!(staged.exists());
        assert!(
            staged.dir().join(STAGED_REPLICATION_METADATA_FILE).is_dir(),
            "the blocking directory is still there, proving the write was attempted and failed non-fatally"
        );
    }
}
