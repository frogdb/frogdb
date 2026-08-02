//! Staged creation of a single snapshot.
//!
//! [`SnapshotStager`] owns the durability-critical write half of the checkpoint
//! machinery: cut a RocksDB checkpoint at a sequence number, write
//! `metadata.json`, and atomically promote a staged directory
//! (`.snapshot_NNNNN.tmp` → `snapshot_NNNNN`) into the live snapshot set, then
//! repoint the `latest` pointer.
//!
//! A snapshot deliberately does **not** include the search-index sidecar
//! (`<data_dir>/search`). An earlier version copied it in; that copy was removed
//! (proposal 23) because it had no restore-path reader — see the decision note on
//! [`SnapshotStager::run`].
//!
//! The contract is *all-or-nothing*: every stage builds under `tmp`, and only a
//! fully-formed snapshot is atomically promoted to `final_dir`. This mirrors the
//! install side ([`crate::rocks::RocksStore::load_staged_checkpoint`]), which
//! treats a half-written database on disk as a first-class hazard.
use super::SnapshotError;
use super::metadata::SnapshotMetadataFile;
use crate::fs_seam::SnapshotFs;
use crate::rocks::RocksStore;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// RAII cleanup for the staging dir. Removes `path` on `Drop` unless `commit()`
/// has run. This is the single owner of "remove the temp dir on failure": it
/// replaces the lone hand-placed `remove_dir_all` that previously fired on
/// exactly one of five failure paths, so the all-or-nothing invariant can no
/// longer be partially applied. Every early return from [`SnapshotStager::run`]
/// drops the guard and reclaims the (potentially checkpoint-sized) temp dir.
struct TmpDirGuard {
    path: PathBuf,
    committed: bool,
}
impl TmpDirGuard {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            committed: false,
        }
    }
    /// Mark the staging dir as kept (it has been atomically promoted), so `Drop`
    /// leaves it in place.
    fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for TmpDirGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Owns the staged creation of one snapshot. All-or-nothing: every stage builds
/// under `tmp`; only a fully-formed snapshot is atomically promoted to
/// `final_dir`. The async spawn/loop, the `pre_snapshot_hook` await, the
/// scheduled-retry handshake, and metrics stay in the coordinator; the stager is
/// the blocking core that `spawn_blocking` runs.
pub(crate) struct SnapshotStager {
    /// `<snapshot_dir>` — the root holding all `snapshot_NNNNN` dirs and `latest`.
    pub(crate) snapshot_dir: PathBuf,
    /// `<snapshot_dir>/.snapshot_NNNNN.tmp` — the staging dir built under, then promoted.
    pub(crate) tmp: PathBuf,
    /// `<snapshot_dir>/snapshot_NNNNN` — the promotion target.
    pub(crate) final_dir: PathBuf,
    /// `snapshot_NNNNN` — the `latest` symlink target (relative).
    pub(crate) name: String,
    /// The live data dir. Retained on the stager (the coordinator constructs it)
    /// as the re-add anchor for a future search-sidecar copy; unread today since
    /// the copy path was removed (proposal 23) — see [`SnapshotStager::run`].
    #[allow(dead_code)]
    pub(crate) data_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) num_shards: usize,
    pub(crate) max_snapshots: usize,
    /// The filesystem the publication protocol writes through
    /// ([`crate::fs_seam::RealFs`] in production). Every rename on the path to a
    /// published snapshot is bracketed by syncs through this seam, which is also
    /// what makes the ordering assertable from a unit test
    /// See [`crate::fs_seam`] for the rule and why it is written through a seam.
    pub(crate) fs: Arc<dyn SnapshotFs>,
}

impl SnapshotStager {
    /// The whole blocking pipeline. Each `?` aborts the snapshot *and* removes
    /// the temp dir via the guard. Returns the completed metadata on success.
    pub(crate) fn run(self, rocks: &RocksStore) -> Result<SnapshotMetadataFile, SnapshotError> {
        // Reclaim any stale tmp dir left by a crashed run at this epoch, then
        // claim a clean one. This keeps a pre-fix orphan (or a crash between
        // guard-construction paths) from wedging the epoch with `Directory not
        // empty` on the next attempt.
        let _ = std::fs::remove_dir_all(&self.tmp);
        let guard = TmpDirGuard::new(&self.tmp);

        let seq = self.stage_checkpoint(rocks)?;
        // NOTE (proposal 23 — search-sidecar layout, DELETE branch): a snapshot no
        // longer copies the search-index sidecar (`<data_dir>/search`) into the
        // checkpoint. The former `copy_indexes` step produced an *unconsumed
        // artifact*: nothing on the restore path ever reads a Checkpoint's
        // `search/` subtree back. Startup reads only `metadata.json`
        // (`RocksSnapshotCoordinator::load_latest_metadata`); the staged-checkpoint
        // installer installs only the RocksDB dir; and search indexes are rebuilt
        // from the `search_meta` column family by
        // `frogdb_core::IndexLifecycleManager::recover`, which opens tantivy/usearch
        // files from the *live* `<data_dir>/search` — never from a snapshot copy.
        // Replication full sync ships its own flat RocksDB checkpoint and never
        // touches `search/` either.
        //
        // Proposal 23's decision rule: extract a shared sidecar-layout type only if
        // a restore-from-sidecar consumer exists or is on the roadmap; otherwise
        // prefer deleting the reader-less copy over abstracting it. Verified against
        // the current tree (2026-07): no such consumer and no roadmap promise
        // (checked server recovery, the operator, website docs, and CHANGELOG), so
        // the copy is deleted, removing one of the layout's two authors and closing
        // the cross-crate silent-drift seam. `stager_excludes_search_sidecar` pins
        // this exclusion so it is enforced, not accidental.
        //
        // Re-add path: if a warm-open / restore-from-sidecar consumer is added,
        // reintroduce the copy as proposal 23's `SearchSidecar::copy_into` extraction
        // (a `persistence`-owned layout type shared by this copier and core's
        // `IndexLifecycleManager::index_dir` writer), not as the previous
        // hand-rolled `copy_search_indexes` walk. `data_dir` is retained on the
        // stager as that anchor.
        let md = self.finalize_metadata(seq)?;
        self.install()?;
        guard.commit();

        // Post-install, best-effort: the snapshot is already durable in
        // `final_dir`. These touch only the *pointer* and *retention*, not the
        // snapshot's contents, so a failure must not fail the snapshot.
        if let Err(e) = Self::update_latest_symlink(&*self.fs, &self.snapshot_dir, &self.name) {
            tracing::warn!(error = %e, "Failed to update latest symlink after snapshot install");
        }
        if let Err(e) = Self::cleanup_old_snapshots(&self.snapshot_dir, self.max_snapshots) {
            tracing::warn!(error = %e, "Failed to clean up old snapshots after install");
        }
        Ok(md)
    }

    /// Create the RocksDB checkpoint under `tmp/checkpoint` at the current
    /// sequence. On failure the guard in [`run`](Self::run) removes the temp dir.
    ///
    /// Only the staging parent is created here — RocksDB's `CreateCheckpoint`
    /// rejects a pre-existing checkpoint dir with `Invalid argument: Directory
    /// exists` and creates `cp` itself (via an internal `<cp>.tmp` + rename).
    fn stage_checkpoint(&self, rocks: &RocksStore) -> Result<u64, SnapshotError> {
        std::fs::create_dir_all(&self.tmp)?;
        let cp = self.tmp.join("checkpoint");
        let seq = rocks.latest_sequence_number();
        rocks
            .create_checkpoint(&cp)
            .map_err(|e| SnapshotError::Internal(format!("Failed to create checkpoint: {e}")))?;
        Ok(seq)
    }

    /// Compute the size, then write `metadata.json` atomically (`.tmp` + rename).
    ///
    /// Durability ([`crate::fs_seam`]): the metadata's *contents* are fsynced
    /// before the rename that publishes them, and the staging directory is
    /// fsynced after it. The second sync is what the promotion in
    /// [`install`](Self::install) rides on — without it the commit rename could
    /// become durable while the `metadata.json` entry it publishes has not,
    /// producing a `snapshot_NNNNN` that reads as incomplete after a reboot.
    /// The checkpoint subtree needs no sync from us: RocksDB's
    /// `create_checkpoint` fsyncs the files it writes and the directory holding
    /// them, so `checkpoint`'s entry in the staging dir is covered by the same
    /// `sync_dir` call.
    fn finalize_metadata(&self, seq: u64) -> Result<SnapshotMetadataFile, SnapshotError> {
        let cp = self.tmp.join("checkpoint");
        let mut md = SnapshotMetadataFile::new(self.epoch, seq, self.num_shards);
        // The snapshot contains only the RocksDB checkpoint (no search sidecar —
        // see the decision note in `run`), so its size is the checkpoint's alone.
        let size = Self::calculate_dir_size(&cp).unwrap_or(0);
        md.mark_complete(size);
        let json = serde_json::to_string_pretty(&md)
            .map_err(|e| SnapshotError::Internal(format!("Failed to serialize metadata: {e}")))?;
        let tmp_meta = self.tmp.join("metadata.json.tmp");
        self.fs.write(&tmp_meta, json.as_bytes())?;
        self.fs.sync_file(&tmp_meta)?;
        self.fs.rename(&tmp_meta, &self.tmp.join("metadata.json"))?;
        self.fs.sync_dir(&self.tmp)?;
        Ok(md)
    }

    /// Atomic promotion: rename `tmp` → `final_dir`, then fsync the directory
    /// that gained the name. Both paths live in `snapshot_dir`, so the one sync
    /// makes the disappearance of `.snapshot_NNNNN.tmp` and the appearance of
    /// `snapshot_NNNNN` durable together — the rename is the commit point, and
    /// this is what commits it against a power loss rather than only against a
    /// process crash.
    fn install(&self) -> Result<(), SnapshotError> {
        self.fs.rename(&self.tmp, &self.final_dir)?;
        self.fs.sync_dir(&self.snapshot_dir)?;
        Ok(())
    }

    fn calculate_dir_size(p: &std::path::Path) -> std::io::Result<u64> {
        let mut s = 0;
        if p.is_dir() {
            for e in std::fs::read_dir(p)? {
                let e = e?;
                let m = e.metadata()?;
                if m.is_dir() {
                    s += Self::calculate_dir_size(&e.path())?;
                } else {
                    s += m.len();
                }
            }
        }
        Ok(s)
    }

    /// Keep the newest `ms` `snapshot_NNNNN` dirs, delete the rest.
    pub(crate) fn cleanup_old_snapshots(
        sd: &std::path::Path,
        ms: usize,
    ) -> Result<(), SnapshotError> {
        if ms == 0 {
            return Ok(());
        }
        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        for e in std::fs::read_dir(sd)? {
            let e = e?;
            let n = e.file_name();
            let ns = n.to_string_lossy();
            if ns.starts_with("snapshot_")
                && e.file_type()?.is_dir()
                && let Some(es) = ns.strip_prefix("snapshot_")
                && let Ok(ep) = es.parse::<u64>()
            {
                entries.push((ep, e.path()));
            }
        }
        if entries.len() <= ms {
            return Ok(());
        }
        entries.sort_by_key(|(ep, _)| *ep);
        let dc = entries.len() - ms;
        for (ep, p) in entries.into_iter().take(dc) {
            tracing::info!(epoch = ep, path = %p.display(), "Deleting old snapshot");
            if let Err(e) = std::fs::remove_dir_all(&p) {
                tracing::warn!(epoch = ep, error = %e, "Failed to delete old snapshot");
            }
        }
        Ok(())
    }

    /// Atomically repoint `latest` at `sn` via `.latest.tmp` → rename, then
    /// fsync the directory so the repoint survives a power loss
    /// ([`crate::fs_seam`]). A symlink's target is stored in the inode, so
    /// there is nothing to `sync_file` here — the directory entry is the whole
    /// payload.
    #[cfg(unix)]
    fn update_latest_symlink(
        fs: &dyn SnapshotFs,
        sd: &std::path::Path,
        sn: &str,
    ) -> Result<(), SnapshotError> {
        let ll = sd.join("latest");
        let tl = sd.join(".latest.tmp");
        let _ = std::fs::remove_file(&tl);
        fs.symlink(Path::new(sn), &tl)?;
        fs.rename(&tl, &ll)?;
        fs.sync_dir(sd)?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn update_latest_symlink(
        fs: &dyn SnapshotFs,
        sd: &std::path::Path,
        sn: &str,
    ) -> Result<(), SnapshotError> {
        let ll = sd.join("latest");
        fs.write(&ll, sn.as_bytes())?;
        fs.sync_file(&ll)?;
        fs.sync_dir(sd)?;
        Ok(())
    }
}
