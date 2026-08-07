//! The data directory's identity marker.
//!
//! `has_data()` cannot tell "this database is empty" from "I am not looking
//! where you think I am": a mistyped `persistence.data-dir`, a volume that
//! failed to mount, and a container that lost its bind mount all present as a
//! directory with no keys in it, which boots as a fresh database and then
//! starts persisting over whatever the operator believed was there.
//!
//! The marker is the missing bit of evidence. Recovery stamps
//! [`MARKER_FILE_NAME`] into the data directory the first time it initializes
//! one, and refuses to boot against a directory that has content but no marker
//! — see the decision tree in `frogdb-recovery`'s `data_dir` phase. This module
//! owns only the on-disk artifact: its format, its atomic publication, and the
//! emptiness question the decision tree asks.
//!
//! The marker records a generated database id and a creation timestamp
//! alongside the layout version. Only the version is enforced today (a marker
//! from a future layout refuses rather than being read under the wrong rules);
//! the id exists so that a future cross-check — "this node came up on a data
//! directory that is not the one it had last time" — has something stable to
//! compare against. It is stable across restarts *and* across a full resync
//! that replaces the database wholesale, because it names the directory, not
//! its current contents.
//!
//! Specced as FM-PERSISTENCE-048..052 in
//! `.scratch/hardening/specs/persistence-failure-modes.md`.

use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use crate::fs_seam::{RealFs, SnapshotFs};

/// The marker file's name inside the data directory.
pub const MARKER_FILE_NAME: &str = "frogdb_data_dir";

/// Scratch name the marker is written under before being renamed into place.
///
/// It shares the marker's prefix so an operator who finds one after a crash can
/// tell what left it behind.
const MARKER_TEMP_FILE_NAME: &str = "frogdb_data_dir.tmp";

/// Layout version stamped into new markers.
///
/// Bump this when the *directory layout* changes in a way a older binary must
/// not open — the column-family naming scheme, the staged-checkpoint protocol,
/// the marker's own required fields. A marker carrying a higher version than
/// this refuses to boot ([`DataDirMarkerError::FutureLayout`]); a lower one is
/// read normally, because reading an older layout is the migration path.
pub const DATA_DIR_LAYOUT_VERSION: u32 = 1;

/// The identity FrogDB stamps into a data directory it initialized.
///
/// Unknown fields are deliberately *not* denied: forward compatibility is the
/// [`layout_version`](Self::layout_version)'s job, and a future version that
/// adds a purely informational field should not be unreadable by every binary
/// that predates it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirMarker {
    /// Random, stable identifier for this data directory.
    pub database_id: String,
    /// When the directory was first initialized, in milliseconds since the unix
    /// epoch.
    pub created_at_unix_ms: u64,
    /// The directory layout this marker was written under.
    pub layout_version: u32,
}

/// Why a marker could not be read or written.
///
/// Every variant is a *refusal* cause: none of them may be collapsed into "no
/// marker", because "no marker" is the branch that initializes a fresh database.
#[derive(Debug, thiserror::Error)]
pub enum DataDirMarkerError {
    /// The marker file exists but could not be read (permissions, IO error).
    #[error("data directory marker {path} could not be read: {source}")]
    Unreadable {
        /// The marker's path.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: io::Error,
    },
    /// The marker file was read but is not a marker.
    #[error("data directory marker {path} is not valid FrogDB marker JSON: {source}")]
    Malformed {
        /// The marker's path.
        path: PathBuf,
        /// The underlying parse error.
        #[source]
        source: serde_json::Error,
    },
    /// The marker was written by a FrogDB that uses a newer directory layout.
    #[error(
        "data directory marker {path} was written under layout version {found}, but this build \
         understands at most {supported}: refusing to open a directory whose layout it may not \
         read correctly (run the newer FrogDB, or restore this directory from a compatible backup)"
    )]
    FutureLayout {
        /// The marker's path.
        path: PathBuf,
        /// The version the marker carries.
        found: u32,
        /// The newest version this build understands.
        supported: u32,
    },
    /// The marker could not be published.
    #[error("data directory marker {path} could not be written: {source}")]
    Write {
        /// The marker's path.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: io::Error,
    },
}

impl DataDirMarker {
    /// Mint a marker for a directory being initialized now.
    ///
    /// Deliberately not a `Default`: every call produces a *different* value,
    /// which is the opposite of what a default is for.
    pub fn mint() -> Self {
        Self {
            database_id: new_database_id(),
            created_at_unix_ms: now_unix_ms(),
            layout_version: DATA_DIR_LAYOUT_VERSION,
        }
    }

    /// Where the marker lives for a given data directory.
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(MARKER_FILE_NAME)
    }

    /// Read the marker out of `dir`.
    ///
    /// `Ok(None)` means, and only means, that there is no marker file — an
    /// absent directory included. Anything else (an IO error, a file that will
    /// not parse, a layout from the future) is an error, because the caller's
    /// `None` branch initializes a database.
    pub fn read(dir: &Path) -> Result<Option<Self>, DataDirMarkerError> {
        let path = Self::path(dir);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(DataDirMarkerError::Unreadable { path, source }),
        };
        let marker: Self =
            serde_json::from_slice(&bytes).map_err(|source| DataDirMarkerError::Malformed {
                path: path.clone(),
                source,
            })?;
        if marker.layout_version > DATA_DIR_LAYOUT_VERSION {
            return Err(DataDirMarkerError::FutureLayout {
                path,
                found: marker.layout_version,
                supported: DATA_DIR_LAYOUT_VERSION,
            });
        }
        Ok(Some(marker))
    }

    /// Publish this marker into `dir`, which must already exist.
    ///
    /// Atomic and durable to the same rule the checkpoint publishers follow
    /// (see [`crate::fs_seam`]): write the scratch file, fsync it, rename it
    /// into place, fsync the directory that gained the name. A marker that
    /// reached disk half-written would be [`DataDirMarkerError::Malformed`] on
    /// the next boot — a refusal, not a fresh start, but still a refusal nobody
    /// earned.
    pub fn stamp(&self, dir: &Path) -> Result<(), DataDirMarkerError> {
        self.stamp_with(dir, &RealFs)
    }

    /// [`stamp`](Self::stamp) against an injectable filesystem, so the
    /// sync/rename ordering is assertable from a unit test.
    pub(crate) fn stamp_with(
        &self,
        dir: &Path,
        fs: &dyn SnapshotFs,
    ) -> Result<(), DataDirMarkerError> {
        let json = serde_json::to_vec_pretty(self)
            .expect("a DataDirMarker is strings and integers, which always serialize");
        let tmp = dir.join(MARKER_TEMP_FILE_NAME);
        let path = Self::path(dir);

        let published = (|| -> io::Result<()> {
            fs.write(&tmp, &json)?;
            fs.sync_file(&tmp)?;
            fs.rename(&tmp, &path)?;
            fs.sync_dir(dir)
        })();

        published.map_err(|source| {
            // A leftover scratch file is not cosmetic: it is *content* in a
            // directory that has no marker, which is exactly the shape the
            // guard refuses to boot against. Best-effort — the publish already
            // failed, and the reason it failed is the one worth reporting.
            let _ = std::fs::remove_file(&tmp);
            DataDirMarkerError::Write { path, source }
        })
    }
}

/// Does the tree under `dir` contain anything that is not an empty directory?
///
/// This is the question "is this a first boot?" reduces to, and it counts
/// *files*, not entries. Directories are excluded on purpose:
///
/// - a freshly formatted ext4/xfs volume mounts with a `lost+found` directory,
///   so counting entries would refuse the single most common production first
///   boot (a dedicated volume mounted at the data dir);
/// - orchestration and deployment tooling routinely pre-creates subdirectories
///   under the data dir (the cluster storage path, for instance) before FrogDB
///   ever runs.
///
/// Neither shape is evidence of a wrong directory, and initializing a database
/// alongside empty directories loses nothing. A file is different: something
/// wrote it, and this process did not.
///
/// A directory that is not there contains no files. Symlinks count as files —
/// they are content, and following them to decide otherwise would let a link
/// farm hide a populated tree.
pub fn contains_files(dir: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if contains_files(&entry.path())? {
                return Ok(true);
            }
        } else {
            return Ok(true);
        }
    }
    Ok(false)
}

/// 128 random bits, rendered as 32 lowercase hex characters (the shape Redis'
/// `replid` uses, for the same reason: greppable in a log line).
fn new_database_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Milliseconds since the unix epoch, or 0 on a clock before it — a marker is
/// worth stamping even on a machine whose clock is nonsense, and the timestamp
/// is informational.
fn now_unix_ms() -> u64 {
    frogdb_types::clock::system_now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_seam::RecordingFs;
    use tempfile::TempDir;

    /// The publish protocol, in order: scratch write, fsync of the scratch
    /// file, rename onto the final name, fsync of the directory that gained it.
    /// A marker that skipped either sync could be absent after a power loss on
    /// a directory that already holds a database — and "no marker" is the
    /// branch that initializes a fresh one.
    // FM-PERSISTENCE-049
    #[test]
    fn marker_round_trips_through_an_atomic_publish() {
        let tmp = TempDir::new().unwrap();
        let fs = RecordingFs::new();
        let marker = DataDirMarker::mint();

        marker.stamp_with(tmp.path(), &fs).unwrap();

        assert_eq!(
            fs.trace(tmp.path()),
            vec![
                "write frogdb_data_dir.tmp".to_string(),
                "sync_file frogdb_data_dir.tmp".to_string(),
                "rename frogdb_data_dir.tmp -> frogdb_data_dir".to_string(),
                "sync_dir .".to_string(),
            ],
            "the marker must be published like every other durable rename"
        );
        assert_eq!(
            DataDirMarker::read(tmp.path()).unwrap(),
            Some(marker),
            "and read back byte-identical"
        );
        assert!(
            !tmp.path().join(MARKER_TEMP_FILE_NAME).exists(),
            "the scratch name must not survive a successful publish"
        );
    }

    /// Two directories initialized by the same build must not share an id, or
    /// the id can never answer "is this the directory this node had last time".
    // FM-PERSISTENCE-049
    #[test]
    fn minted_markers_are_unique_and_carry_the_current_layout() {
        let a = DataDirMarker::mint();
        let b = DataDirMarker::mint();
        assert_ne!(a.database_id, b.database_id, "ids must not be a constant");
        assert_eq!(a.database_id.len(), 32, "128 bits of hex");
        assert_eq!(a.layout_version, DATA_DIR_LAYOUT_VERSION);
        assert!(
            a.created_at_unix_ms > 1_700_000_000_000,
            "the creation timestamp must be a real wall-clock reading, got {}",
            a.created_at_unix_ms
        );
    }

    /// The two shapes that must read as "no marker", and nothing else may.
    // FM-PERSISTENCE-049
    #[test]
    fn a_missing_marker_is_absent_not_an_error() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(
            DataDirMarker::read(tmp.path()).unwrap(),
            None,
            "an empty directory has no marker"
        );
        assert_eq!(
            DataDirMarker::read(&tmp.path().join("not-there")).unwrap(),
            None,
            "a directory that does not exist has no marker either"
        );
    }

    /// `NotFound` is the *only* IO error that means "no marker". A permissions
    /// failure, a directory sitting where the marker belongs, an IO error off a
    /// failing disk — each of those reports a directory whose identity is
    /// unknown, and treating unknown as absent is what initializes a database
    /// over one that is already there.
    // FM-PERSISTENCE-050
    #[test]
    fn an_io_error_other_than_not_found_is_an_error_not_an_absent_marker() {
        let tmp = TempDir::new().unwrap();
        // A directory at the marker's path: the read fails with something that
        // is emphatically not `NotFound`, on every platform.
        std::fs::create_dir(DataDirMarker::path(tmp.path())).unwrap();

        let err = DataDirMarker::read(tmp.path())
            .expect_err("a marker path that cannot be read must not read as absent");
        assert!(
            matches!(err, DataDirMarkerError::Unreadable { .. }),
            "expected Unreadable, got {err:?}"
        );
    }

    /// A file that will not parse must not be indistinguishable from no file at
    /// all — that collapse is what would let a corrupt marker initialize a
    /// fresh database over a real one.
    // FM-PERSISTENCE-050
    #[test]
    fn a_malformed_marker_is_an_error_not_an_absent_marker() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(DataDirMarker::path(tmp.path()), b"{ this is not json").unwrap();

        let err = DataDirMarker::read(tmp.path()).expect_err("a malformed marker must not read");
        assert!(
            matches!(err, DataDirMarkerError::Malformed { .. }),
            "expected Malformed, got {err:?}"
        );

        // The same directory with a valid marker reads fine, so the assertion
        // above is about the contents and not about the path.
        DataDirMarker::mint().stamp(tmp.path()).unwrap();
        assert!(DataDirMarker::read(tmp.path()).unwrap().is_some());
    }

    /// A marker from a layout this build does not understand is a refusal, not
    /// a downgrade-and-hope.
    // FM-PERSISTENCE-050
    #[test]
    fn a_marker_from_a_future_layout_is_refused() {
        let tmp = TempDir::new().unwrap();
        let mut marker = DataDirMarker::mint();
        marker.layout_version = DATA_DIR_LAYOUT_VERSION + 1;
        marker.stamp(tmp.path()).unwrap();

        let err = DataDirMarker::read(tmp.path()).expect_err("a future layout must not read");
        assert!(
            matches!(
                err,
                DataDirMarkerError::FutureLayout { found, supported, .. }
                    if found == DATA_DIR_LAYOUT_VERSION + 1 && supported == DATA_DIR_LAYOUT_VERSION
            ),
            "expected FutureLayout, got {err:?}"
        );

        // The current version, and anything older, still reads.
        marker.layout_version = DATA_DIR_LAYOUT_VERSION;
        marker.stamp(tmp.path()).unwrap();
        assert_eq!(DataDirMarker::read(tmp.path()).unwrap(), Some(marker));
    }

    /// A failed publish must not leave the scratch file behind: it would be a
    /// *file* in a directory with no marker, which is precisely the state the
    /// boot guard refuses.
    // FM-PERSISTENCE-049
    #[test]
    fn a_failed_stamp_leaves_no_temporary_file() {
        let tmp = TempDir::new().unwrap();
        // A directory occupying the marker's name makes the rename fail after
        // the scratch file has already been written.
        std::fs::create_dir(DataDirMarker::path(tmp.path())).unwrap();

        let err = DataDirMarker::mint()
            .stamp(tmp.path())
            .expect_err("renaming a file onto a directory must fail");
        assert!(
            matches!(err, DataDirMarkerError::Write { .. }),
            "expected Write, got {err:?}"
        );
        assert!(
            !tmp.path().join(MARKER_TEMP_FILE_NAME).exists(),
            "the scratch file must be cleaned up when the publish fails"
        );
    }

    /// Emptiness is about files, at any depth — and a first file is enough of
    /// an answer to stop looking.
    // FM-PERSISTENCE-048
    #[test]
    fn contains_files_counts_files_at_any_depth_and_ignores_empty_directories() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("data");

        assert!(
            !contains_files(&root).unwrap(),
            "a directory that does not exist holds no files"
        );

        std::fs::create_dir_all(root.join("cluster")).unwrap();
        std::fs::create_dir(root.join("lost+found")).unwrap();
        assert!(
            !contains_files(&root).unwrap(),
            "pre-created empty subdirectories are not evidence of a wrong directory"
        );

        std::fs::create_dir_all(root.join("nested/deeper")).unwrap();
        std::fs::write(root.join("nested/deeper/somebody-elses.db"), b"x").unwrap();
        assert!(
            contains_files(&root).unwrap(),
            "a file at any depth is content"
        );
    }

    /// "Not there" is the only listing failure that means "no files". A path
    /// that is a file rather than a directory, a permissions failure, an IO
    /// error — each of those is a directory whose contents are *unknown*, and
    /// reporting unknown as empty is what licenses initializing a database on
    /// top of it.
    // FM-PERSISTENCE-048
    #[test]
    fn contains_files_propagates_a_listing_failure_that_is_not_absence() {
        let tmp = TempDir::new().unwrap();
        let not_a_dir = tmp.path().join("data");
        std::fs::write(&not_a_dir, b"somebody else's file").unwrap();

        assert!(
            contains_files(&not_a_dir).is_err(),
            "a data-dir that is a file must be an error, not an empty directory"
        );
    }

    /// A symlink is content even when it points at a directory: resolving it
    /// would let `data-dir/link -> /somebody/elses/tree` read as empty.
    // FM-PERSISTENCE-048
    #[cfg(unix)]
    #[test]
    fn contains_files_treats_a_symlink_as_content() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("data");
        let target = tmp.path().join("elsewhere");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&target).unwrap();

        std::os::unix::fs::symlink(&target, root.join("link")).unwrap();

        assert!(
            contains_files(&root).unwrap(),
            "a symlink to an empty directory is still something this process did not create"
        );
    }
}
