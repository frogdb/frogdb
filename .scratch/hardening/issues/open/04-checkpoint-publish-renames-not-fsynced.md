# Checkpoint publication relies on rename atomicity without fsyncing anything

Status: needs-triage
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
`.scratch/hardening/specs/persistence-failure-modes.md`.

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
