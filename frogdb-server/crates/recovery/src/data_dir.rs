//! Phase 0: decide whether this directory is FrogDB's before anything writes to
//! it.
//!
//! `has_data()` is a first-key-exists probe, so "this database is empty" and "I
//! am not looking where you think I am" used to be the same observation: a
//! mistyped `persistence.data-dir`, a volume that failed to mount, or a
//! container that lost its bind mount came up as a healthy empty keyspace and
//! then began persisting over the real state (FM-PERSISTENCE-029). The
//! column-family guards do not help — they compare a *persisted layout* against
//! the configured one, and a directory that was never FrogDB's has no layout to
//! disagree with.
//!
//! So recovery stamps a marker (`frogdb_persistence::data_dir`) into every
//! directory it initializes, and this phase reads it:
//!
//! | Directory | Decision |
//! |---|---|
//! | marker present and readable | boot; the restore path and its existing guards are unchanged |
//! | no marker, no files | genuine first boot: initialize and stamp |
//! | no marker, but files present | **refuse** — name the resolved path and the override |
//! | marker present but unreadable | **refuse** — an unreadable marker is not an absent one |
//!
//! Two orderings are load-bearing.
//!
//! *Before the install.* [`verify`] runs ahead of the staged-checkpoint install
//! (and therefore ahead of the RocksDB open, and far ahead of any replication
//! dial), because a replica that refuses only *after* a full resync has already
//! repopulated the directory has not refused at all — it has quietly replaced
//! the operator's data with the primary's, which is a different failure than
//! starting fresh but the same lost bytes.
//!
//! *After the install.* The marker lives at `<data-dir>/frogdb_data_dir`, a
//! sibling of the `db/` the install renames aside (FM-PERSISTENCE-057), so an
//! install can no longer carry the directory's identity off with the database
//! it replaces. [`stamp`] still runs after the install rather than before, for
//! a simpler reason: on a first boot the data directory may not exist until the
//! install phase has created it, and a marker cannot be published into a
//! directory that is not there.
//!
//! Specced as FM-PERSISTENCE-048..052 and FM-PERSISTENCE-057 in
//! `specs/persistence.md`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use frogdb_core::persistence::data_dir::{DataDirMarker, MARKER_FILE_NAME, contains_foreign_files};
use tracing::{info, warn};

use crate::RecoveryInputs;

/// Decide which marker this data directory must end up carrying, refusing the
/// boot if the directory is not one FrogDB may initialize.
///
/// Writes nothing: the directory does not necessarily exist yet, and the staged
/// checkpoint install that runs next would throw away anything written here
/// anyway. [`stamp`] does the writing once the directory is settled.
pub(crate) fn verify(inputs: &RecoveryInputs<'_>) -> Result<DataDirMarker> {
    let dir = inputs.data_dir;
    let force = inputs.persistence.force_fresh_data_dir;

    let existing = match DataDirMarker::read(dir) {
        Ok(found) => found,
        Err(err) if force => {
            warn!(
                data_dir = %resolved(dir).display(),
                error = %err,
                "Data directory marker is unusable, but --force-fresh-data-dir was given: \
                 re-stamping it and continuing"
            );
            None
        }
        Err(err) => {
            bail!(
                "the FrogDB marker in data directory {} could not be read ({err}): refusing to \
                 start, because treating an unreadable marker as a missing one would initialize a \
                 fresh, empty database over whatever this directory holds. Restore the directory \
                 from a backup, or restart once with --force-fresh-data-dir to adopt it as it is \
                 and re-stamp the marker.",
                resolved(dir).display(),
            );
        }
    };

    if let Some(marker) = existing {
        info!(
            data_dir = %resolved(dir).display(),
            database_id = %marker.database_id,
            layout_version = marker.layout_version,
            "Data directory verified"
        );
        return Ok(marker);
    }

    // "Somebody else wrote here", not "there are bytes here": staging and
    // backups live inside the data directory now (FM-PERSISTENCE-057), and a
    // checkpoint an operator placed in `<data-dir>/staging` to restore from is
    // exactly the boot this gate must let through, not refuse. A `db/` with no
    // marker still counts, which is the case the gate is for.
    let has_files = contains_foreign_files(dir).with_context(|| {
        format!(
            "failed to inspect data directory {}",
            resolved(dir).display()
        )
    })?;

    if has_files && !force {
        bail!(
            "data directory {} holds files but no FrogDB marker ({MARKER_FILE_NAME}): refusing to \
             start, because initializing a fresh database here would begin overwriting whatever \
             is already in it. This is what a mistyped persistence.data-dir, a container that \
             lost its bind mount, or a volume mounted somewhere else look like from the inside. \
             Point data-dir at the right directory, or — if this directory really is FrogDB's \
             (written before markers existed, or restored by hand) — restart once with \
             --force-fresh-data-dir to adopt it. That flag never deletes anything.",
            resolved(dir).display(),
        );
    }

    if !has_files && inputs.persistence.require_existing_data && !force {
        bail!(
            "data directory {} is empty and persistence.require-existing-data is set: refusing to \
             start. An empty directory is what an unmounted volume and a genuine first boot both \
             look like, and this deployment has declared that it is past its first boot. Check \
             that the volume is mounted at that path; if this really is the first boot, restart \
             once with --force-fresh-data-dir.",
            resolved(dir).display(),
        );
    }

    if force {
        warn!(
            data_dir = %resolved(dir).display(),
            has_files,
            "--force-fresh-data-dir: adopting this data directory without a marker check and \
             stamping it. Nothing is deleted; existing data is recovered normally."
        );
    }

    let marker = DataDirMarker::mint();
    info!(
        data_dir = %resolved(dir).display(),
        database_id = %marker.database_id,
        "Initializing a new FrogDB data directory"
    );
    Ok(marker)
}

/// Write the marker [`verify`] settled on into the (now existing) data
/// directory.
///
/// Unconditional rather than write-if-absent: distinguishing "already stamped"
/// from "being initialized" means asking the same question twice, and rewriting
/// a marker that is already correct costs one rename per process start. What it
/// buys is an invariant with no exceptions: after this call the data directory
/// carries the marker recovery decided on.
pub(crate) fn stamp(dir: &Path, marker: &DataDirMarker) -> Result<()> {
    marker.stamp(dir).map_err(anyhow::Error::from)
}

/// The data directory as an absolute path, for the operator reading the
/// refusal. A relative `data-dir` (the default is `./frogdb-data`) is resolved
/// against a working directory the operator may not know — which is exactly the
/// confusion these refusals exist to end — so the message must not repeat the
/// configured spelling back at them.
///
/// `std::path::absolute` rather than `canonicalize`: the directory may not
/// exist yet, and a path that cannot be made absolute is still better named by
/// its configured form than by nothing.
fn resolved(dir: &Path) -> PathBuf {
    std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf())
}
