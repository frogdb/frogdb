//! The filesystem seam the checkpoint-publication paths write through.
//!
//! Publishing a checkpoint — a snapshot, or a staged full-sync database — is a
//! chain of renames. `rename(2)` is atomic with respect to other *processes*,
//! which is what makes a partial snapshot invisible, but it
//! says nothing about *durability*: after a power loss the filesystem may present
//! any prefix of the chain, including states the code never produces in memory
//! (`snapshot_NNNNN` with no `metadata.json`, a `latest` pointing at a directory
//! that is not there, a backup rename that reached disk paired with an install
//! rename that did not).
//!
//! The rule, applied by every publisher: fsync what a rename
//! publishes *before* the rename, and fsync the directory that gains the new name
//! *after* it.
//!
//! Routing those calls through [`SnapshotFs`] rather than calling `std::fs`
//! directly is what makes the rule testable. Whether an fsync actually reached
//! the platter is not observable from a unit test without syscall interposition;
//! whether the publisher *issued* the sync, and in the right order relative to the
//! rename, is — with a recording fake in place of [`RealFs`].
//!
//! Specced as the checkpoint-publication failure mode in
//! `.scratch/hardening/specs/persistence-failure-modes.md`.
use std::io;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

/// The subset of `std::fs` the checkpoint publishers use. Deliberately narrow:
/// every method here is one the durability ordering depends on, so a fake that
/// records the call sequence sees the whole protocol.
pub(crate) trait SnapshotFs: Send + Sync {
    /// Write `contents` to `path`, truncating an existing file.
    fn write(&self, path: &Path, contents: &[u8]) -> io::Result<()>;
    /// Rename `from` to `to`. Atomic within a filesystem, but *not* durable on
    /// its own — the caller fsyncs the containing directory afterwards.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Create a symlink at `link` pointing at `target`.
    fn symlink(&self, target: &Path, link: &Path) -> io::Result<()>;
    /// Flush a file's contents and metadata to the device.
    fn sync_file(&self, path: &Path) -> io::Result<()>;
    /// Flush a *directory's entries* to the device, so the names created or
    /// removed inside it survive a power loss.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
}

/// The real filesystem.
pub(crate) struct RealFs;

impl SnapshotFs for RealFs {
    fn write(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        std::fs::write(path, contents)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    /// One function with the platform split *inside* it, rather than two
    /// `#[cfg]`-gated definitions: only the arm for the host platform is ever
    /// compiled, so a second definition would be code no test on this host can
    /// reach or refute.
    fn symlink(&self, target: &Path, link: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link)
        }
        // No symlink without elevated privileges on Windows; the caller's
        // non-unix path writes the target name as a plain file instead.
        #[cfg(not(unix))]
        {
            self.write(link, target.to_string_lossy().as_bytes())
        }
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }

    /// `File::open(dir)?.sync_all()` is the portable directory-entry barrier on
    /// unix. Windows cannot open a directory as a file handle through
    /// `std::fs`, so there it degrades to a no-op: a save must not fail on a
    /// platform that offers no way to perform this sync.
    #[cfg(unix)]
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }

    #[cfg(not(unix))]
    fn sync_dir(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// A pass-through [`SnapshotFs`] that performs every operation for real while
/// recording the ordered call sequence.
///
/// Pass-through rather than in-memory on purpose: the publishers under test are
/// judged on their on-disk result *and* on their sync ordering, and a fake
/// filesystem would only be able to check the second. The recorded trace is
/// rendered relative to a root by [`RecordingFs::trace`], which keeps the
/// assertions readable as a protocol.
#[cfg(test)]
pub(crate) struct RecordingFs {
    ops: std::sync::Mutex<Vec<FsOp>>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FsOp {
    Write(PathBuf),
    Rename(PathBuf, PathBuf),
    Symlink(PathBuf),
    SyncFile(PathBuf),
    SyncDir(PathBuf),
}

#[cfg(test)]
impl RecordingFs {
    pub(crate) fn new() -> Self {
        Self {
            ops: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn record(&self, op: FsOp) {
        self.ops.lock().unwrap().push(op);
    }

    /// The recorded sequence, with every path rendered relative to `root`
    /// (`root` itself renders as `.`). Paths outside `root` keep their full
    /// form, which makes an unexpected write outside the tree obvious.
    pub(crate) fn trace(&self, root: &Path) -> Vec<String> {
        fn rel(p: &Path, root: &Path) -> String {
            match p.strip_prefix(root) {
                Ok(r) if r.as_os_str().is_empty() => ".".to_string(),
                Ok(r) => r.display().to_string(),
                Err(_) => p.display().to_string(),
            }
        }
        self.ops
            .lock()
            .unwrap()
            .iter()
            .map(|op| match op {
                FsOp::Write(p) => format!("write {}", rel(p, root)),
                FsOp::Rename(a, b) => format!("rename {} -> {}", rel(a, root), rel(b, root)),
                FsOp::Symlink(p) => format!("symlink {}", rel(p, root)),
                FsOp::SyncFile(p) => format!("sync_file {}", rel(p, root)),
                FsOp::SyncDir(p) => format!("sync_dir {}", rel(p, root)),
            })
            .collect()
    }
}

#[cfg(test)]
impl SnapshotFs for RecordingFs {
    fn write(&self, path: &Path, contents: &[u8]) -> io::Result<()> {
        RealFs.write(path, contents)?;
        self.record(FsOp::Write(path.to_path_buf()));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.rename(from, to)?;
        self.record(FsOp::Rename(from.to_path_buf(), to.to_path_buf()));
        Ok(())
    }

    fn symlink(&self, target: &Path, link: &Path) -> io::Result<()> {
        RealFs.symlink(target, link)?;
        self.record(FsOp::Symlink(link.to_path_buf()));
        Ok(())
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        RealFs.sync_file(path)?;
        self.record(FsOp::SyncFile(path.to_path_buf()));
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        RealFs.sync_dir(path)?;
        self.record(FsOp::SyncDir(path.to_path_buf()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Whether an fsync reached the platter is not observable here, but whether
    /// it was *attempted against a real object* is: both sync methods open the
    /// path first, so a path that is not there comes back as an error. A
    /// version that answered `Ok(())` unconditionally would let a publisher
    /// report a durable checkpoint it never touched.
    #[test]
    fn real_fs_syncs_fail_on_a_path_that_is_not_there() {
        let tmp = TempDir::new().unwrap();
        assert!(
            RealFs.sync_file(&tmp.path().join("no-such-file")).is_err(),
            "fsync of a missing file must not report success"
        );
        assert!(
            RealFs.sync_dir(&tmp.path().join("no-such-dir")).is_err(),
            "fsync of a missing directory must not report success"
        );

        // The same calls against real objects succeed, so the assertions above
        // are about the missing path and not about the methods never working.
        let file = tmp.path().join("f");
        RealFs.write(&file, b"x").unwrap();
        RealFs.sync_file(&file).unwrap();
        RealFs.sync_dir(tmp.path()).unwrap();
    }

    /// `symlink` has to leave behind a name that resolves to the target — the
    /// `latest` pointer a snapshot reader follows.
    #[test]
    fn real_fs_symlink_creates_a_name_that_resolves_to_the_target() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("snapshot_00001");
        std::fs::create_dir(&target).unwrap();
        RealFs.write(&target.join("metadata.json"), b"{}").unwrap();

        let link = tmp.path().join("latest");
        RealFs.symlink(&target, &link).unwrap();

        assert!(link.exists(), "the link must exist");
        assert_eq!(
            std::fs::read(link.join("metadata.json")).unwrap(),
            b"{}",
            "and it must resolve to the target directory's contents"
        );
    }
}
