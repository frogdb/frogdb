//! The staged-checkpoint contract: one typed owner for the on-disk protocol a
//! replica full sync and boot-time recovery share.
//!
//! Three parties collaborate through the data directory
//! ([`DataDirLayout`](crate::data_dir::DataDirLayout)), whose live RocksDB is
//! `<data-dir>/db`:
//!
//! 1. **Writer** — the replica full-sync state machine
//!    (`frogdb-replication`, `replica/connection.rs`) downloads the primary's
//!    checkpoint into `<data-dir>/staging.incoming`, then renames it to
//!    `<data-dir>/staging` and stamps
//!    [`STAGED_REPLICATION_METADATA_FILE`] inside it.
//! 2. **Installer** — `RocksStore::load_staged_checkpoint`
//!    (`rocks/checkpoint.rs`) swaps the staged dir in for the live database on
//!    the next boot.
//! 3. **Orchestrator** — startup recovery (`frogdb-recovery`) runs the
//!    installer before opening the DB and afterwards consumes the replication
//!    metadata that the install carried into `<data-dir>/db`.
//!
//! Before this module the dir/file names were string literals duplicated
//! across the three crates; `StagedCheckpoint` is the single owner.

use super::payload::{PayloadCheck, PayloadError, PayloadReport, verify_payload};
use crate::data_dir::DataDirLayout;
use std::io;
use std::path::{Path, PathBuf};

/// Replication metadata file the writer stamps inside the staged checkpoint.
/// Installing the checkpoint carries it into the data dir, coupling offset
/// durability to snapshot durability (compare Redis' RDB aux fields).
pub const STAGED_REPLICATION_METADATA_FILE: &str = "replication_metadata.json";

/// How many `<data-dir>/backup/db_backup_<ts>` directories survive a successful
/// install.
///
/// Retention decision: **keep the newest 1**. The backup exists so an operator
/// can recover the immediately-previous database if a full sync installed bad
/// data; one generation covers that story, while every additional generation
/// is a full database copy of disk with no recovery story attached. (Before
/// this existed, backups were never cleaned: every replica full sync leaked a
/// complete copy of the database, forever.)
pub const BACKUP_RETENTION: usize = 1;

/// Typed handle on the staged-checkpoint location for one database directory.
#[derive(Debug, Clone)]
pub struct StagedCheckpoint {
    dir: PathBuf,
}

impl StagedCheckpoint {
    /// The staging location inside the data directory (`<data-dir>/staging`).
    ///
    /// Infallible, and inside the mount by construction: there is no
    /// `data_dir.parent()` step that could land on a filesystem the operator
    /// never provisioned, and no rename operand that could be the mount point
    /// itself (FM-PERSISTENCE-057).
    pub fn in_data_dir(data_dir: &Path) -> Self {
        Self {
            dir: DataDirLayout::new(data_dir).staging_dir(),
        }
    }

    /// The staged checkpoint directory itself.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Is a staged checkpoint present?
    pub fn exists(&self) -> bool {
        self.dir.exists()
    }

    /// Prove the staged dir holds a complete RocksDB database, naming the
    /// defect if it does not. The installer refuses anything but `Ok` — see
    /// `load_staged_checkpoint`.
    ///
    /// Structure and payload manifest only ([`verify_payload`]); the install
    /// pairs this with a trial open through RocksDB itself, which is the party
    /// that can resolve the MANIFEST to a live SST set.
    pub fn verify(&self) -> Result<PayloadReport, PayloadError> {
        verify_payload(&self.dir, PayloadCheck::Sizes)
    }

    /// [`verify`](Self::verify) as a predicate, for callers that only branch on
    /// it. Prefer `verify` where the reason can be surfaced: "this staged
    /// checkpoint is not a database" is the least useful half of what the check
    /// knows.
    pub fn is_complete_db(&self) -> bool {
        self.verify().is_ok()
    }

    /// Where the writer stamps the replication metadata inside the staged dir.
    pub fn replication_metadata_path(&self) -> PathBuf {
        self.dir.join(STAGED_REPLICATION_METADATA_FILE)
    }
}

/// Backup-name plumbing: `<db>_backup_<unix_secs>`, inside
/// `<data-dir>/backup`.
pub(crate) fn backup_dir_name(db_name: &str, unix_secs: u64) -> String {
    format!("{db_name}_backup_{unix_secs}")
}

/// Delete all but the newest `keep` `<db>_backup_*` directories under
/// `backup_root`.
///
/// "Newest" is decided by the numeric `<unix_secs>` suffix (numeric compare —
/// string order would rank `_2` above `_10`); unparsable suffixes sort oldest.
/// Returns how many directories were removed. Callers treat failure as
/// non-fatal: retention is hygiene, never worth failing an install over.
pub(crate) fn prune_backups(backup_root: &Path, db_name: &str, keep: usize) -> io::Result<usize> {
    let prefix = format!("{db_name}_backup_");
    let mut backups: Vec<(u64, PathBuf)> = match std::fs::read_dir(backup_root) {
        Ok(entries) => entries,
        // Nothing has ever been backed up here: no backup root, nothing to prune.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    }
    .filter_map(|e| e.ok())
    .map(|e| e.path())
    .filter(|p| p.is_dir())
    .filter_map(|p| {
        let ts = p
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(&prefix))
            .map(|suffix| suffix.parse::<u64>().unwrap_or(0))?;
        Some((ts, p))
    })
    .collect();
    if backups.len() <= keep {
        return Ok(0);
    }
    // Newest (largest timestamp) first; delete the tail.
    backups.sort_by(|a, b| b.0.cmp(&a.0));
    let mut removed = 0;
    for (_, path) in backups.into_iter().skip(keep) {
        std::fs::remove_dir_all(&path)?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// The staged-checkpoint *names* are a cross-crate protocol: the replica
    /// writer (in `frogdb-replication`) and the installer here only meet
    /// through these paths, so the paths are pinned in the crate that owns
    /// them — and both of them resolve inside the data directory.
    // FM-PERSISTENCE-057
    #[test]
    fn staged_paths_are_the_cross_crate_contract() {
        let data_dir = Path::new("/mnt/frogdb-data");
        let staged = StagedCheckpoint::in_data_dir(data_dir);
        assert_eq!(
            staged.dir(),
            DataDirLayout::new(data_dir).staging_dir(),
            "the staging location is the layout's, not a name invented here"
        );
        assert_eq!(
            staged.replication_metadata_path(),
            data_dir
                .join("staging")
                .join(STAGED_REPLICATION_METADATA_FILE),
            "the metadata file lives inside the staged dir, so the commit rename carries it"
        );
        assert!(
            staged.dir().starts_with(data_dir),
            "staging must never leave the data directory"
        );
        assert_ne!(
            staged.dir(),
            data_dir,
            "the mount point itself is never a rename operand"
        );
    }

    // FM-PERSISTENCE-024
    /// `exists` and `verify` are two different questions, and the installer
    /// asks the second: a staged directory that is merely *there* is not a
    /// database. Nor is one holding a `CURRENT` that resolves to nothing —
    /// which is exactly what a truncated operator copy leaves behind, and what
    /// the old `CURRENT.exists()` check accepted.
    #[test]
    fn verify_requires_a_current_pointer_that_resolves_to_a_manifest() {
        let tmp = TempDir::new().unwrap();
        let staged = StagedCheckpoint::in_data_dir(tmp.path());
        assert!(!staged.exists(), "nothing staged yet");
        assert!(!staged.is_complete_db());

        std::fs::create_dir(staged.dir()).unwrap();
        std::fs::write(staged.dir().join("000123.sst"), b"partial download").unwrap();
        assert!(staged.exists(), "the directory is there");
        assert!(
            !staged.is_complete_db(),
            "but a dir of stray SSTs with no CURRENT is not a database"
        );

        std::fs::write(staged.dir().join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        assert!(
            !staged.is_complete_db(),
            "a CURRENT naming a MANIFEST that never arrived is still not a database"
        );

        std::fs::write(staged.dir().join("MANIFEST-000001"), b"version edits").unwrap();
        assert!(
            staged.is_complete_db(),
            "CURRENT resolving to a real MANIFEST is what completes it"
        );
    }

    /// Backup directory names carry a numeric timestamp suffix — the ordering
    /// key `prune_backups` sorts on.
    #[test]
    fn backup_dir_name_carries_the_numeric_timestamp() {
        assert_eq!(
            backup_dir_name("frogdb", 1_700_000_000),
            "frogdb_backup_1700000000"
        );
        assert_eq!(backup_dir_name("frogdb", 0), "frogdb_backup_0");
    }

    /// A data directory that never installed anything has no `backup/`, and
    /// asking to prune it is not a failure — the install treats prune errors as
    /// warnings, so a spurious one would log noise on every first full sync.
    #[test]
    fn prune_backups_is_a_noop_when_no_backup_root_exists() {
        let tmp = TempDir::new().unwrap();
        let root = DataDirLayout::new(tmp.path()).backup_root();
        assert!(!root.exists());
        assert_eq!(prune_backups(&root, "db", BACKUP_RETENTION).unwrap(), 0);
    }
}
