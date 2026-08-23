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
//! | no marker, no foreign entries | genuine first boot: initialize and stamp |
//! | no marker, but foreign entries present | **refuse** — name the resolved path and the entries |
//! | marker present but unreadable | **refuse** — an unreadable marker is not an absent one |
//!
//! `--force-fresh-data-dir` is a *fresh-start tool, not an override*
//! (FM-PERSISTENCE-051). It lets a boot past the unreadable-marker refusal and
//! past `persistence.require-existing-data`, and both of those are about a
//! directory that holds nothing but FrogDB's own artifacts. It has no power
//! over the foreign-entries refusal: "this really is my first boot" and "these
//! bytes are not mine" are indistinguishable to a flag, so the flag must not
//! resolve both — CockroachDB refuses to init into a directory that is not its
//! own for the same reason. The operator moves the bytes out and retries.
//!
//! Both halves of the phase run before the install, for two different reasons.
//!
//! *The verdict.* [`verify`] runs ahead of the staged-checkpoint install
//! (and therefore ahead of the RocksDB open, and far ahead of any replication
//! dial), because a replica that refuses only *after* a full resync has already
//! repopulated the directory has not refused at all — it has quietly replaced
//! the operator's data with the primary's, which is a different failure than
//! starting fresh but the same lost bytes.
//!
//! *The stamp.* The marker lives at `<data-dir>/frogdb_data_dir`,
//! a sibling of the `db/` the install renames aside (FM-PERSISTENCE-057), and
//! [`stamp`] publishes it *before* the install runs — creating the directory
//! first if this is a first boot. Identity is the one thing a directory must
//! never have to re-derive: stamping afterwards left a window (crash between
//! the install's renames, or between the install and the stamp) in which a
//! directory held a whole database and no name for it, and the next boot either
//! refused it as somebody else's or, once adopted, minted a *different*
//! `database_id` for the same data (FM-PERSISTENCE-059). CockroachDB writes its
//! `StoreIdent` first for the same reason.
//!
//! Specced as FM-PERSISTENCE-048..052, FM-PERSISTENCE-057 and
//! FM-PERSISTENCE-059 in `specs/persistence.md`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use frogdb_core::persistence::data_dir::{DataDirMarker, MARKER_FILE_NAME, foreign_files};
use frogdb_core::persistence::rocks::staged::pending_install;
use tracing::{info, warn};

use crate::RecoveryInputs;

/// Decide which marker this data directory must end up carrying, refusing the
/// boot if the directory is not one FrogDB may initialize.
///
/// Writes nothing, and does not create the directory: this is the decision, and
/// a decision that refuses must leave the directory exactly as it found it.
/// [`stamp`] does the writing once the verdict is in.
pub(crate) fn verify(inputs: &RecoveryInputs<'_>) -> Result<DataDirMarker> {
    let dir = inputs.data_dir;
    let force = inputs.persistence.force_fresh_data_dir;

    let existing = match DataDirMarker::read(dir) {
        Ok(found) => found,
        Err(err) if force => {
            // Fresh-start tool, not an override: re-stamping is a rewrite of
            // FrogDB's own identity, which is only defensible on a directory
            // that holds nothing but FrogDB's own artifacts. Beside somebody
            // else's bytes it would mint an identity over them, which is the
            // adoption R6 removed.
            let foreign = foreign_entries(dir)?;
            if !foreign.is_empty() {
                bail!(
                    "the FrogDB marker in data directory {} could not be read ({err}), and the \
                     directory also holds entries FrogDB did not write: {}. Refusing to start. \
                     --force-fresh-data-dir re-stamps a directory that is FrogDB's own; it is \
                     not a way to claim one that is not. Restore the directory from a backup, or \
                     move those entries out and restart.",
                    resolved(dir).display(),
                    describe(&foreign),
                );
            }
            warn!(
                data_dir = %resolved(dir).display(),
                error = %err,
                "Data directory marker is unusable and the directory holds nothing but FrogDB's \
                 own artifacts, and --force-fresh-data-dir was given: re-stamping it and \
                 continuing"
            );
            None
        }
        Err(err) => {
            bail!(
                "the FrogDB marker in data directory {} could not be read ({err}): refusing to \
                 start, because treating an unreadable marker as a missing one would initialize a \
                 fresh, empty database over whatever this directory holds. Restore the directory \
                 from a backup, or — if the directory holds nothing but FrogDB's own artifacts — \
                 restart once with --force-fresh-data-dir to re-stamp the marker.",
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
    let foreign = foreign_entries(dir)?;

    if !foreign.is_empty() {
        bail!(
            "data directory {} holds entries FrogDB did not write and no FrogDB marker \
             ({MARKER_FILE_NAME}): {}. Refusing to start, because initializing a fresh database \
             here would begin overwriting whatever is already in it. This is what a mistyped \
             persistence.data-dir, a container that lost its bind mount, or a volume mounted \
             somewhere else look like from the inside. Point data-dir at the right directory, or \
             move those entries somewhere else and restart. --force-fresh-data-dir does not \
             override this: it starts a directory FrogDB owns, it never adopts one it does not.",
            resolved(dir).display(),
            describe(&foreign),
        );
    }

    if inputs.persistence.require_existing_data && !force {
        // An install this boot would finish is *data*, even though the probe
        // above cannot see it: `staging` and `backup` are skipped there because
        // they are FrogDB's own artifacts, so a directory holding nothing but an
        // interrupted install — or a checkpoint an operator staged to restore
        // from — reads as empty. Refusing it would refuse the boot that
        // completes the install, and the only documented way past that refusal
        // (`--force-fresh-data-dir`) mints a fresh identity for a directory
        // whose database is already sitting there: the re-mint
        // FM-PERSISTENCE-059 rules out.
        let pending = pending_install(dir).with_context(|| {
            format!(
                "failed to inspect data directory {} for an unfinished install",
                resolved(dir).display()
            )
        })?;
        if pending {
            info!(
                data_dir = %resolved(dir).display(),
                "Data directory holds an install to finish; persistence.require-existing-data is \
                 satisfied by it"
            );
        } else {
            bail!(
                "data directory {} is empty and persistence.require-existing-data is set: \
                 refusing to start. An empty directory is what an unmounted volume and a genuine \
                 first boot both look like, and this deployment has declared that it is past its \
                 first boot. Check that the volume is mounted at that path; if this really is the \
                 first boot, restart once with --force-fresh-data-dir.",
                resolved(dir).display(),
            );
        }
    }

    if force {
        warn!(
            data_dir = %resolved(dir).display(),
            "--force-fresh-data-dir: this data directory holds nothing FrogDB did not write, so \
             it is being stamped and initialized as a fresh one. Nothing is deleted; an install \
             waiting in staging is finished normally."
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

/// Write the marker [`verify`] settled on into the data directory, creating the
/// directory if this is a first boot.
///
/// Unconditional rather than write-if-absent: distinguishing "already stamped"
/// from "being initialized" means asking the same question twice, and rewriting
/// a marker that is already correct costs one rename per process start. What it
/// buys is an invariant with no exceptions: after this call the data directory
/// carries the marker recovery decided on — and it runs before the install, so
/// there is no crash point at which a database exists here without one.
///
/// Creating the directory is this phase's job for the same reason. It used to
/// be the install's, which is what forced the stamp to run *after* the install
/// and opened the window (FM-PERSISTENCE-059).
pub(crate) fn stamp(dir: &Path, marker: &DataDirMarker) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| {
        format!(
            "failed to create data directory {}",
            resolved(dir).display()
        )
    })?;
    marker.stamp(dir).map_err(anyhow::Error::from)
}

/// How many foreign entries a refusal names before it says "and more".
///
/// A refusal exists to tell the operator *which* directory they are looking at;
/// a handful of names does that, and a full listing of somebody else's tree in
/// a startup error does not. The probe walks only far enough to fill it.
const NAMED_FOREIGN_ENTRIES: usize = 8;

/// The entries in `dir` that FrogDB did not write, at most
/// [`NAMED_FOREIGN_ENTRIES`] of them plus one — the extra is how [`describe`]
/// knows to say the list is truncated.
///
/// The probe's failures propagate rather than answering "empty": a directory
/// whose contents are unknown must not read as one FrogDB may initialize.
fn foreign_entries(dir: &Path) -> Result<Vec<PathBuf>> {
    foreign_files(dir, NAMED_FOREIGN_ENTRIES + 1).with_context(|| {
        format!(
            "failed to inspect data directory {}",
            resolved(dir).display()
        )
    })
}

/// Render [`foreign_entries`]' answer for the operator: paths relative to the
/// data directory, comma-separated, truncated with a count of what is left.
fn describe(entries: &[PathBuf]) -> String {
    let named = entries
        .iter()
        .take(NAMED_FOREIGN_ENTRIES)
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if entries.len() > NAMED_FOREIGN_ENTRIES {
        format!("{named}, and more")
    } else {
        named
    }
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
