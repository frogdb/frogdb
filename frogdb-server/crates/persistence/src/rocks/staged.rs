//! The staged-checkpoint contract: one typed owner for the on-disk protocol a
//! replica full sync and boot-time recovery share.
//!
//! Three parties collaborate through the *parent* directory of the live
//! RocksDB dir (`<parent>/<db>`):
//!
//! 1. **Writer** — the replica full-sync state machine
//!    (`frogdb-replication`, `replica/connection.rs`) downloads the primary's
//!    checkpoint into a scratch dir, then renames it to
//!    [`STAGED_CHECKPOINT_DIR`] and stamps
//!    [`STAGED_REPLICATION_METADATA_FILE`] inside it.
//! 2. **Installer** — `RocksStore::load_staged_checkpoint`
//!    (`rocks/checkpoint.rs`) swaps the staged dir in for the live database on
//!    the next boot.
//! 3. **Orchestrator** — server recovery (`server/src/recovery/`) runs the
//!    installer before opening the DB and afterwards consumes the replication
//!    metadata that the install carried into the data dir.
//!
//! Before this module the dir/file names were string literals duplicated
//! across the three crates; `StagedCheckpoint` is the single owner.

use std::io;
use std::path::{Path, PathBuf};

/// Directory (sibling of the live db dir) holding a fully-received checkpoint
/// that is ready to be installed on the next boot. The rename *to* this name
/// is the writer's commit point; the rename *from* it is the installer's.
pub const STAGED_CHECKPOINT_DIR: &str = "checkpoint_ready";

/// Replication metadata file the writer stamps inside the staged checkpoint.
/// Installing the checkpoint carries it into the data dir, coupling offset
/// durability to snapshot durability (compare Redis' RDB aux fields).
pub const STAGED_REPLICATION_METADATA_FILE: &str = "replication_metadata.json";

/// RocksDB's manifest pointer; its presence distinguishes a complete database
/// directory from a partial copy. Same marker `RocksStore::open` trusts.
const ROCKSDB_CURRENT_MANIFEST: &str = "CURRENT";

/// How many `<db>_backup_<ts>` directories survive a successful install.
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
    /// The staging location used with a live db at `<parent>/<db>`.
    /// `None` if `db_dir` has no parent (there is nowhere to stage).
    pub fn for_db_dir(db_dir: &Path) -> Option<Self> {
        db_dir.parent().map(Self::in_parent)
    }

    /// The staging location inside `parent` (the directory that holds the db
    /// dir). This is what the full-sync writer uses, which works from the
    /// parent directly.
    pub fn in_parent(parent: &Path) -> Self {
        Self {
            dir: parent.join(STAGED_CHECKPOINT_DIR),
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

    /// Does the staged dir hold a complete RocksDB database (has `CURRENT`)?
    /// The installer refuses anything else — see `load_staged_checkpoint`.
    pub fn is_complete_db(&self) -> bool {
        self.dir.join(ROCKSDB_CURRENT_MANIFEST).exists()
    }

    /// Where the writer stamps the replication metadata inside the staged dir.
    pub fn replication_metadata_path(&self) -> PathBuf {
        self.dir.join(STAGED_REPLICATION_METADATA_FILE)
    }
}

/// Backup-name plumbing: `<db>_backup_<unix_secs>`.
pub(crate) fn backup_dir_name(db_name: &str, unix_secs: u64) -> String {
    format!("{db_name}_backup_{unix_secs}")
}

/// Delete all but the newest `keep` `<db>_backup_*` sibling directories.
///
/// "Newest" is decided by the numeric `<unix_secs>` suffix (numeric compare —
/// string order would rank `_2` above `_10`); unparsable suffixes sort oldest.
/// Returns how many directories were removed. Callers treat failure as
/// non-fatal: retention is hygiene, never worth failing an install over.
pub(crate) fn prune_backups(parent: &Path, db_name: &str, keep: usize) -> io::Result<usize> {
    let prefix = format!("{db_name}_backup_");
    let mut backups: Vec<(u64, PathBuf)> = std::fs::read_dir(parent)?
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
