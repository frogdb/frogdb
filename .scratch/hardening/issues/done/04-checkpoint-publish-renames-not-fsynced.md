# Checkpoint publication relies on rename atomicity without fsyncing anything

Status: done
Type: bug (durability)
Severity: likelihood 1/3 (power loss / kernel panic only — a process crash is unaffected),
consequence 3/3 (a snapshot reported complete is absent or truncated after reboot) — score 3
Area: persistence / snapshot stager, staged-checkpoint install

## Problem

The snapshot stager's publication protocol is a chain of renames with no `fsync` anywhere on it
(`frogdb-server/crates/persistence/src/snapshot/stager.rs`):

- `metadata.json.tmp` → `metadata.json` inside the staging dir,
- `.snapshot_NNNNN.tmp` → `snapshot_NNNNN` (the commit point),
- `.latest.tmp` → `latest` (the symlink repoint).

`rename(2)` is atomic with respect to *other processes*, which is what makes a partial snapshot
invisible (FM-PERSISTENCE-017) and makes a process crash safe. It says nothing about durability:
neither the renamed entries nor the containing directory are fsynced, and neither are the
checkpoint's own files (RocksDB's `create_checkpoint` syncs its SST/manifest writes, but the
directory entries linking them into the snapshot dir are not synced by FrogDB).

After a power loss the filesystem may therefore present any prefix of that chain, including
combinations the code never produces in memory:

- `snapshot_NNNNN` exists with no `metadata.json` — reads as an incomplete snapshot at boot
  (`load_latest_metadata` returns `(0, None)`), so `LASTSAVE` and the epoch silently reset.
- `latest` points at a directory that is not there.
- The whole snapshot is gone even though `BGSAVE` reported success and `LASTSAVE` advanced.

The same applies to the staged-checkpoint install path
(`frogdb-server/crates/persistence/src/rocks/staged.rs` +
`rocks/checkpoint.rs`): the two renames that swap a downloaded full-sync checkpoint in for the live
database are unsynced, so a power loss mid-install can leave the data dir in the "backup exists,
live dir missing" window — which the install *does* handle idempotently on the next boot
(FM-PERSISTENCE-025), but only because the rename it needs happened to reach disk.

Note this is a deliberate asymmetry with the WAL watermark side-file, which is *intentionally* not
fsynced (FM-PERSISTENCE-035) because an unsynced watermark can only under-report. A missing
snapshot is not the benign direction.

Current behavior pinned by FM-PERSISTENCE-023 in
`specs/persistence.md`.

## Candidate fixes

1. **fsync the directory after each publishing rename.** `File::open(dir)?.sync_all()` on the
   snapshot dir after the commit rename and after the symlink repoint; same for the data dir's
   parent on the staged install. Cheap (a handful of syncs per snapshot, not per write) and makes
   the existing crash-window reasoning true under power loss, not just process crash.
2. **Also sync the staged tree before the commit rename.** Walk the staging dir and `sync_all()`
   its files, so the commit rename cannot be durable ahead of the data it points at. More
   expensive; arguably RocksDB's checkpoint already syncs the file contents, so this may reduce to
   syncing the staging directory itself.
3. **Do nothing and document it.** Defensible only if snapshots are positioned purely as a
   best-effort backup artifact — but `LASTSAVE` advancing for a snapshot that a reboot can erase is
   still a false claim.

## Forcing test

Not forcible with today's machinery: this needs power-loss (not process-crash) injection — a
filesystem seam that can drop unsynced directory entries. FM-PERSISTENCE-023 carries
`Forced by | MISSING ([gap: ...])` pointing here. The cheapest real forcing option is a
`std::fs`-shaped trait in the stager plus a fake that records sync calls, which would at least pin
*that the syncs are issued* once fix 1 lands.

## Resolution

Fixed with candidate **1 + the cheap half of 2**, and the "cheapest real forcing option" this
issue proposed (a `std::fs`-shaped trait plus a recording fake) is what pins it.

This is the same defect as
`.scratch/testing-improvements-round2/issues/done/74-snapshot-stager-fsyncs-nothing-before-promoting-latest.md`,
filed independently by the round-2 testing audit. Fixed once, both closed together. Issue 74
carries the full resolution; the parts specific to this filing are below.

### Fix

New `frogdb-server/crates/persistence/src/fs_seam.rs` — a narrow `SnapshotFs` trait
(`write`, `rename`, `symlink`, `sync_file`, `sync_dir`) with a `RealFs` implementation and a
pass-through `RecordingFs` for tests. `sync_dir` is `File::open(dir)?.sync_all()` on unix,
degrading to a no-op on Windows.

Snapshot side (`snapshot/stager.rs`): `sync_file(metadata.json.tmp)` before the metadata rename,
`sync_dir(staging dir)` after it and therefore before the promotion rename, `sync_dir(snapshot_dir)`
after the promotion, and `sync_dir(snapshot_dir)` again after the `latest` repoint.

Install side (`rocks/checkpoint.rs`): `load_staged_checkpoint` now delegates to
`load_staged_checkpoint_with(dir, &dyn SnapshotFs)` and fsyncs the data dir's *parent* after both
renames. The first sync matters as much as the second — the crash-window reasoning in that
function ("crash between rename 1 and rename 2 is recoverable") is only true if rename 1 is
durable when rename 2 runs. Without it a power loss could surface rename 2 alone: the live
directory replaced while the backup that was supposed to hold the previous database never
appeared.

Candidate 2's expensive half (walking the staging tree and syncing every file) was **not**
taken, for the reason the issue itself anticipates: RocksDB's `create_checkpoint` already fsyncs
the SST/manifest files and the directory holding them, so this reduces to syncing the staging
directory, which the metadata step already does.

Candidate 3 (document it) was rejected: `LASTSAVE` advancing for a snapshot a reboot can erase is
a false claim, exactly as the issue argues.

### Forcing test

The issue's assessment was right — the *outcome* is not forcible without power-loss injection.
What is forcible, and what now runs, is that the publisher **issues** the syncs in the right order
relative to the renames. `RecordingFs` performs every operation for real (so the on-disk result is
still asserted) while recording the ordered call sequence, which the tests compare as an exact
protocol:

- `stager_fsyncs_metadata_and_staging_dir_before_install`
- `stager_fsyncs_snapshot_dir_after_install_and_after_latest_repoint`
- `staged_checkpoint_install_fsyncs_the_data_dir_parent`

All three fail pre-fix. FM-PERSISTENCE-023's `Forced by | MISSING ([gap: ...])` pointing at this
file has been replaced by those three names, and the row now describes real behavior instead of a
gap.

### Related

The boot-side consequence this issue lists first — "`snapshot_NNNNN` exists with no
`metadata.json` → `load_latest_metadata` returns `(0, None)`, so the epoch silently resets" —
was fixed alongside it: the boot seed now takes `max(metadata epoch, highest snapshot_NNNNN on
disk)` and logs the read failure instead of swallowing it. Pinned by
`coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable` under FM-PERSISTENCE-017.

The deliberate asymmetry with the WAL watermark side-file (FM-PERSISTENCE-035) stands: it stays
unsynced because it can only under-report.
