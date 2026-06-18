//! Staged creation of a single snapshot.
//!
//! [`SnapshotStager`] owns the durability-critical write half of the checkpoint
//! machinery: cut a RocksDB checkpoint at a sequence number, copy the
//! search-index sidecar, write `metadata.json`, and atomically promote a staged
//! directory (`.snapshot_NNNNN.tmp` → `snapshot_NNNNN`) into the live snapshot
//! set, then repoint the `latest` pointer.
//!
//! The contract is *all-or-nothing*: every stage builds under `tmp`, and only a
//! fully-formed snapshot is atomically promoted to `final_dir`. This mirrors the
//! install side ([`crate::rocks::RocksStore::load_staged_checkpoint`]), which
//! treats a half-written database on disk as a first-class hazard.
use super::SnapshotError;
use super::metadata::SnapshotMetadataFile;
use crate::rocks::RocksStore;
use std::path::PathBuf;

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
    /// Source of the `search/` sidecar (the live data dir).
    pub(crate) data_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) num_shards: usize,
    pub(crate) max_snapshots: usize,
}

impl SnapshotStager {
    /// The whole blocking pipeline. Returns the completed metadata on success.
    pub(crate) fn run(self, rocks: &RocksStore) -> Result<SnapshotMetadataFile, SnapshotError> {
        let seq = self.stage_checkpoint(rocks)?;
        self.copy_indexes()?;
        let md = self.finalize_metadata(seq)?;
        self.install()?;

        // Post-install, best-effort: the snapshot is already durable in
        // `final_dir`. These touch only the *pointer* and *retention*, not the
        // snapshot's contents, so a failure must not fail the snapshot.
        if let Err(e) = Self::update_latest_symlink(&self.snapshot_dir, &self.name) {
            tracing::warn!(error = %e, "Failed to update latest symlink after snapshot install");
        }
        if let Err(e) = Self::cleanup_old_snapshots(&self.snapshot_dir, self.max_snapshots) {
            tracing::warn!(error = %e, "Failed to clean up old snapshots after install");
        }
        Ok(md)
    }

    /// Create the RocksDB checkpoint under `tmp/checkpoint` at the current sequence.
    fn stage_checkpoint(&self, rocks: &RocksStore) -> Result<u64, SnapshotError> {
        let cp = self.tmp.join("checkpoint");
        std::fs::create_dir_all(&cp)?;
        let seq = rocks.latest_sequence_number();
        if let Err(e) = rocks.create_checkpoint(&cp) {
            let _ = std::fs::remove_dir_all(&self.tmp);
            return Err(SnapshotError::Internal(format!(
                "Failed to create checkpoint: {e}"
            )));
        }
        Ok(seq)
    }

    /// Copy the search-index sidecar into the snapshot.
    fn copy_indexes(&self) -> Result<(), SnapshotError> {
        let src = self.data_dir.join("search");
        if src.exists()
            && let Err(e) = Self::copy_search_indexes(&src, &self.tmp.join("search"))
        {
            tracing::warn!(error = %e, "Failed to copy search indexes to snapshot");
        }
        Ok(())
    }

    /// Compute the size, then write `metadata.json` atomically (`.tmp` + rename).
    fn finalize_metadata(&self, seq: u64) -> Result<SnapshotMetadataFile, SnapshotError> {
        let cp = self.tmp.join("checkpoint");
        let mut md = SnapshotMetadataFile::new(self.epoch, seq, self.num_shards);
        let mut size = Self::calculate_dir_size(&cp).unwrap_or(0);
        let search = self.tmp.join("search");
        if search.exists() {
            size += Self::calculate_dir_size(&search).unwrap_or(0);
        }
        md.mark_complete(0, size);
        let json = serde_json::to_string_pretty(&md)
            .map_err(|e| SnapshotError::Internal(format!("Failed to serialize metadata: {e}")))?;
        let tmp_meta = self.tmp.join("metadata.json.tmp");
        std::fs::write(&tmp_meta, &json)?;
        std::fs::rename(&tmp_meta, self.tmp.join("metadata.json"))?;
        Ok(md)
    }

    /// Atomic promotion: rename `tmp` → `final_dir`.
    fn install(&self) -> Result<(), SnapshotError> {
        std::fs::rename(&self.tmp, &self.final_dir)?;
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

    fn copy_search_indexes(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
        for ie in std::fs::read_dir(src)? {
            let ie = ie?;
            if !ie.file_type()?.is_dir() {
                continue;
            }
            let in_ = ie.file_name();
            for se in std::fs::read_dir(ie.path())? {
                let se = se?;
                if !se.file_type()?.is_dir() {
                    continue;
                }
                let sd = dst.join(&in_).join(se.file_name());
                std::fs::create_dir_all(&sd)?;
                for fe in std::fs::read_dir(se.path())? {
                    let fe = fe?;
                    if !fe.file_type()?.is_file() {
                        continue;
                    }
                    let fn_ = fe.file_name();
                    let sp = fe.path();
                    let dp = sd.join(&fn_);
                    let ns = fn_.to_string_lossy();
                    if ns == "meta.json"
                        || ns.starts_with('.')
                        || std::fs::hard_link(&sp, &dp).is_err()
                    {
                        std::fs::copy(&sp, &dp)?;
                    }
                }
            }
        }
        Ok(())
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

    /// Atomically repoint `latest` at `sn` via `.latest.tmp` → rename.
    #[cfg(unix)]
    fn update_latest_symlink(sd: &std::path::Path, sn: &str) -> Result<(), SnapshotError> {
        let ll = sd.join("latest");
        let tl = sd.join(".latest.tmp");
        let _ = std::fs::remove_file(&tl);
        std::os::unix::fs::symlink(sn, &tl)?;
        std::fs::rename(&tl, &ll)?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn update_latest_symlink(sd: &std::path::Path, sn: &str) -> Result<(), SnapshotError> {
        std::fs::write(sd.join("latest"), sn)?;
        Ok(())
    }
}
