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

/// The file that carries the backup sequence across restarts, inside
/// `<data-dir>/backup`.
///
/// Not named with the backup prefix on purpose: [`prune_backups`] only ever
/// considers directories, and a counter that could be mistaken for a backup is
/// a counter that can be pruned.
pub const BACKUP_COUNTER_FILE: &str = "counter";

/// Scratch name the counter is written under before its publishing rename.
const BACKUP_COUNTER_TEMP_FILE: &str = "counter.tmp";

/// Backup-name plumbing: `<db>_backup_<seq>`, inside `<data-dir>/backup`.
///
/// `seq` is a [monotone counter](BackupCounter), never a clock reading. A
/// wall-clock name has two failure modes the campaign rules against elsewhere
/// (no wall clock in state): a clock stepped backwards between two installs
/// inverts the retention order and deletes the generation that should be kept,
/// and two installs inside one second collide on the name — which fails the
/// second install's rename with `ENOTEMPTY`, i.e. right when a resync is
/// already in trouble.
pub(crate) fn backup_dir_name(db_name: &str, seq: u64) -> String {
    format!("{db_name}_backup_{seq}")
}

/// Parse the sequence out of a backup directory name, if it is one.
///
/// `None` covers both "not a backup name at all" and "carries the prefix but
/// the suffix is not a number". The second case used to be `unwrap_or(0)`,
/// which sorted the directory oldest and deleted it: silent removal of a
/// directory nobody could identify.
fn backup_seq(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?.parse::<u64>().ok()
}

/// The monotone counter that names backups, persisted inside the backup root.
///
/// Reserving a sequence is durable *before* the name is used: the counter is
/// written to a scratch file, fsynced, renamed into place, and the directory
/// entry fsynced (the same publish rule as every other durable name here — see
/// [`crate::fs_seam`]). A crash between the reservation and the rename that
/// uses it therefore burns a number rather than reusing one, which is the safe
/// direction: reuse is a name collision on the next install.
pub(crate) struct BackupCounter;

impl BackupCounter {
    /// Where the counter lives for a backup root.
    pub(crate) fn path(backup_root: &Path) -> PathBuf {
        backup_root.join(BACKUP_COUNTER_FILE)
    }

    /// Reserve and durably record the next sequence for `backup_root`.
    ///
    /// The reserved value is one past the highest of *both* sources of truth:
    /// the counter file and the backup directories actually present. Reading
    /// the directories is not belt-and-braces — a counter file that is missing
    /// (an operator copied the backups but not the file, a filesystem lost the
    /// small file and kept the big directories) must not restart naming at 0
    /// and collide with a backup that is sitting right there. Unparseable
    /// backup names contribute nothing to the maximum, exactly as they
    /// contribute nothing to retention.
    pub(crate) fn reserve_next(
        backup_root: &Path,
        db_name: &str,
        fs: &dyn crate::fs_seam::SnapshotFs,
    ) -> io::Result<u64> {
        let recorded = Self::read(backup_root)?;
        let observed = Self::highest_existing(backup_root, db_name)?;
        let next = match (recorded, observed) {
            (Some(r), Some(o)) => r.max(o) + 1,
            (Some(r), None) => r + 1,
            (None, Some(o)) => {
                tracing::warn!(
                    backup_root = %backup_root.display(),
                    highest_existing = o,
                    "Backup counter missing; recovering the sequence from the backups on disk"
                );
                o + 1
            }
            // Nothing recorded and nothing on disk: this is the first backup.
            (None, None) => 0,
        };
        Self::record(backup_root, next, fs)?;
        Ok(next)
    }

    /// The highest sequence the counter file records, or `None` when it is
    /// absent or unreadable as a number.
    ///
    /// A malformed counter reads as absent rather than as an error: the
    /// recovery path (the highest backup on disk) is strictly better than
    /// failing an install over a hygiene file, and it cannot collide.
    fn read(backup_root: &Path) -> io::Result<Option<u64>> {
        let path = Self::path(backup_root);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        match raw.trim().parse::<u64>() {
            Ok(seq) => Ok(Some(seq)),
            Err(_) => {
                tracing::warn!(
                    path = %path.display(),
                    contents = %raw.trim(),
                    "Backup counter is not a number; recovering the sequence from the backups on disk"
                );
                Ok(None)
            }
        }
    }

    /// The highest sequence any backup directory under `backup_root` carries.
    fn highest_existing(backup_root: &Path, db_name: &str) -> io::Result<Option<u64>> {
        let prefix = format!("{db_name}_backup_");
        let entries = match std::fs::read_dir(backup_root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut highest = None;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(seq) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| backup_seq(n, &prefix))
            else {
                continue;
            };
            highest = Some(highest.map_or(seq, |h: u64| h.max(seq)));
        }
        Ok(highest)
    }

    /// Publish `seq` durably: scratch write, fsync, rename, fsync the directory
    /// that gained the name.
    fn record(backup_root: &Path, seq: u64, fs: &dyn crate::fs_seam::SnapshotFs) -> io::Result<()> {
        let tmp = backup_root.join(BACKUP_COUNTER_TEMP_FILE);
        let path = Self::path(backup_root);
        let published = (|| -> io::Result<()> {
            fs.write(&tmp, seq.to_string().as_bytes())?;
            fs.sync_file(&tmp)?;
            fs.rename(&tmp, &path)?;
            fs.sync_dir(backup_root)
        })();
        if published.is_err() {
            // The scratch name is not a backup and must not be left behind for
            // an operator to puzzle over; the publish failure is the error
            // worth reporting.
            let _ = std::fs::remove_file(&tmp);
        }
        published
    }
}

/// Does `data_dir` hold an install that a boot would finish — or one that a
/// previous boot began and did not?
///
/// Two shapes answer yes:
///
/// - `<data-dir>/staging` holds a complete database
///   ([`StagedCheckpoint::is_complete_db`]): the next boot installs it. This is
///   both the replica full-sync handoff and the documented operator restore
///   (copy a checkpoint into `staging`, start the server).
/// - a `db_backup_*` directory exists under `<data-dir>/backup`: rename 1 of an
///   install ran, so a previous database is sitting there.
///
/// The question exists because [`crate::data_dir::foreign_files`]
/// answers a *different* one — "did somebody else write here?" — and skips
/// exactly these names. A directory holding only an unfinished install is
/// therefore "empty" to that probe while holding a whole database, which is the
/// wrong answer for `persistence.require-existing-data`: it would refuse the
/// boot that finishes the install, and the only way past it discards the data
/// the install exists to deliver.
///
/// An incomplete `staging` deliberately does not count. A download that died
/// mid-flight is not data, and calling it data would turn the honest
/// "your volume is not mounted" refusal into a boot that fails one phase later.
pub fn pending_install(data_dir: &Path) -> io::Result<bool> {
    if StagedCheckpoint::in_data_dir(data_dir).is_complete_db() {
        return Ok(true);
    }
    let backup_root = DataDirLayout::new(data_dir).backup_root();
    let prefix = format!("{}_backup_", crate::data_dir::DB_DIR_NAME);
    let entries = match std::fs::read_dir(&backup_root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        // Prefix match only: a backup whose sequence will not parse is still a
        // displaced database, and prune refuses rather than deletes it
        // (FM-PERSISTENCE-026). Reporting it as absent here would be the same
        // mistake in the other direction.
        if entry.file_type()?.is_dir() && entry.file_name().to_string_lossy().starts_with(&prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// What one [`prune_backups`] pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PruneOutcome {
    /// Backup directories deleted.
    pub removed: usize,
    /// Directories carrying the backup prefix whose sequence would not parse.
    /// Left on disk, and reported so the count is observable rather than only
    /// loggable.
    pub refused: usize,
}

/// Delete all but the newest `keep` `<db>_backup_<seq>` directories under
/// `backup_root`.
///
/// "Newest" is decided by the numeric sequence suffix ([`BackupCounter`]) —
/// numeric compare, because string order ranks `_2` above `_10`. A directory
/// that carries the backup prefix but whose suffix does not parse is
/// **refused**: it is neither a retention candidate nor a deletion candidate,
/// and it is logged and counted. Deleting a directory whose name this code
/// cannot explain is the one outcome worth ruling out — it used to parse as
/// sequence 0, sort oldest, and be removed.
///
/// Callers treat failure as non-fatal: retention is hygiene, never worth
/// failing an install over.
pub(crate) fn prune_backups(
    backup_root: &Path,
    db_name: &str,
    keep: usize,
) -> io::Result<PruneOutcome> {
    let prefix = format!("{db_name}_backup_");
    let entries = match std::fs::read_dir(backup_root) {
        Ok(entries) => entries,
        // Nothing has ever been backed up here: no backup root, nothing to prune.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(PruneOutcome::default()),
        Err(e) => return Err(e),
    };
    let mut backups: Vec<(u64, PathBuf)> = Vec::new();
    let mut outcome = PruneOutcome::default();
    for path in entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
    {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix) {
            // Not ours at all: never a candidate, never touched.
            continue;
        }
        match backup_seq(name, &prefix) {
            Some(seq) => backups.push((seq, path)),
            None => {
                tracing::warn!(
                    path = %path.display(),
                    "Backup directory name carries no parseable sequence; refusing to prune it"
                );
                outcome.refused += 1;
            }
        }
    }
    if backups.len() <= keep {
        return Ok(outcome);
    }
    // Newest (largest sequence) first; delete the tail.
    backups.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, path) in backups.into_iter().skip(keep) {
        std::fs::remove_dir_all(&path)?;
        outcome.removed += 1;
    }
    Ok(outcome)
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

    /// The install's own artifacts are invisible to the "did somebody else
    /// write here?" probe, so something has to answer the other question: is
    /// there an install here that a boot would finish? Both halves of an
    /// interrupted install answer yes — the staged payload and the backup rename
    /// 1 left behind — and a half-arrived download answers no, because a
    /// download that died mid-flight is not data.
    // FM-PERSISTENCE-059
    #[test]
    fn an_unfinished_install_is_not_an_empty_data_directory() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let layout = DataDirLayout::new(root);

        assert!(
            !pending_install(root).unwrap(),
            "an empty data directory holds no install"
        );

        // A download that stopped partway: a directory, some bytes, no database.
        std::fs::create_dir_all(layout.staging_dir()).unwrap();
        std::fs::write(layout.staging_dir().join("000123.sst"), b"partial").unwrap();
        assert!(
            !pending_install(root).unwrap(),
            "an incomplete staged payload is not an install waiting to finish"
        );

        // Completed: `CURRENT` resolves to a MANIFEST that is really there.
        std::fs::write(layout.staging_dir().join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        std::fs::write(layout.staging_dir().join("MANIFEST-000001"), b"edits").unwrap();
        assert!(
            pending_install(root).unwrap(),
            "a complete staged payload is an install the next boot finishes"
        );

        // The other half of the crash window: rename 1 ran, rename 2 did not.
        std::fs::remove_dir_all(layout.staging_dir()).unwrap();
        assert!(!pending_install(root).unwrap());
        let backup_root = layout.backup_root();
        std::fs::create_dir_all(backup_root.join(backup_dir_name(crate::data_dir::DB_DIR_NAME, 7)))
            .unwrap();
        assert!(
            pending_install(root).unwrap(),
            "a displaced database under backup/ is a database, wherever the install stopped"
        );

        // A name whose sequence will not parse is still a displaced database:
        // prune refuses to delete it, and this must not report it as absent.
        std::fs::remove_dir_all(&backup_root).unwrap();
        std::fs::create_dir_all(backup_root.join("db_backup_not-a-number")).unwrap();
        assert!(
            pending_install(root).unwrap(),
            "an unparseable backup name is not an absent backup"
        );

        // Files that merely share the neighbourhood are not installs.
        std::fs::remove_dir_all(&backup_root).unwrap();
        std::fs::create_dir_all(&backup_root).unwrap();
        std::fs::write(backup_root.join("counter"), b"7").unwrap();
        assert!(
            !pending_install(root).unwrap(),
            "the counter file is bookkeeping, not a backup"
        );
    }

    /// Backup directory names carry a numeric sequence suffix — the ordering
    /// key `prune_backups` sorts on.
    #[test]
    fn backup_dir_name_carries_the_numeric_sequence() {
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
        assert_eq!(
            prune_backups(&root, "db", BACKUP_RETENTION).unwrap(),
            PruneOutcome::default()
        );
    }
}
