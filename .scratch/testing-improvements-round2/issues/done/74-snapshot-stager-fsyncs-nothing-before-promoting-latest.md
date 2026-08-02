# The snapshot stager fsyncs neither `metadata.json` nor any directory before promoting `latest`

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/13 F20 · MASTER.md §3 (durability)
Score: severity 4 · likelihood 3 · effort 4 · priority 14
Area: frogdb-persistence / snapshot stager

## Context

The stager writes a snapshot, renames it into place and repoints `latest` without a single
fsync — not on `metadata.json`, not on any directory. A power loss inside that install window
can leave `latest` pointing at a snapshot whose `metadata.json` is zero-length or absent, which
on the next startup resets the epoch to 0 and triggers the reap loop. Corruption caught at
restart. The window is small, but it is exactly when a crash is most likely, because it is the
heaviest I/O the process does.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. `needs-triage`: the proposal carried an `OPTIONS:` block on how to observe
the fsync, and the choice determines whether new infrastructure is needed.

## Evidence

`persistence/src/snapshot/stager.rs` — `run()` is `remove_dir_all(tmp)` → `TmpDirGuard` →
`stage_checkpoint` → `finalize_metadata` → `install()` (rename tmp → final) → `guard.commit()` →
best-effort `update_latest_symlink` → best-effort `cleanup_old_snapshots`. There is no
`File::sync_all`, no `sync_data`, and no directory fsync anywhere in the file.
`persistence/src/rocks/wal_watermark.rs` has the same shape: `write` is temp+rename and never
fsyncs (the module documents that it can only under-report, which bounds the damage there but
not here).

## What to fix

1. `fsync` `metadata.json` before `install()`.
2. `fsync` the snapshot directory before the `latest` symlink is repointed, and the parent
   directory after the rename, so the rename itself is durable.
3. Make `load_latest_metadata` refuse a zero-length/absent `metadata.json` rather than silently
   yielding epoch 0.
4. Apply the same review to `persistence/src/rocks/wal_watermark.rs::write`.

## Options

Reproduced verbatim from proposals/13 F20:

- *(a)* Introduce a narrow `SnapshotFs` trait the stager writes through, faked in tests to record
  fsyncs (level 2). Cheap, deterministic, no platform dependency. **Recommended.**
- *(b)* Rely on F10's subprocess-SIGKILL harness and assert only the *outcome* (post-crash
  `latest` always resolves to a complete snapshot). No new abstraction, but probabilistic — it
  will not reliably land inside the install window.

## Acceptance criteria

- [x] A test routes the stager's writes through a filesystem seam that records `sync_all` calls
      and asserts `metadata.json` is fsynced before `install()`. Fails today.
- [x] The same test asserts the snapshot directory is fsynced before the symlink is repointed.
      Fails today.
- [x] After truncating `metadata.json` to zero length, `load_latest_metadata` reports an error
      rather than silently yielding epoch 0.
- [n/a] Option (a) was chosen, so this criterion does not apply. If option (b) is chosen instead, the post-crash invariant "`latest` always resolves to a
      complete snapshot" is asserted, and the probabilistic coverage is stated in the test.

## Test boundary

Level 2 with the `SnapshotFs` seam — the stager's own API plus a recording fake is the whole
behaviour. Level 5 without one, because asserting that an fsync happened otherwise needs syscall
interposition; the seam is the cheaper path and is worth introducing.

## Depends on

issue 02 (I2 — subprocess-SIGKILL crash primitive) **only if option (b) is chosen**,
`.scratch/testing-improvements-round2/issues/`. Option (a) depends on nothing.

## Resolution

Fixed with **option (a)** — the `SnapshotFs` seam.

This is the same hole as `.scratch/hardening/issues/done/04-checkpoint-publish-renames-not-fsynced.md`,
filed twice from two different audits. Fixed once, both closed together; issue 04 carries the
same resolution and additionally covers the staged-checkpoint install path.

### Root cause

The publication protocol was a chain of renames with no fsync anywhere on it. Confirmed against
the tree — the evidence in this issue was accurate:

- `frogdb-server/crates/persistence/src/snapshot/stager.rs:161-162` (pre-fix) —
  `std::fs::write(&tmp_meta, &json)` then `std::fs::rename(...)`, no sync on either side.
- `frogdb-server/crates/persistence/src/snapshot/stager.rs:168` (pre-fix) — `install()` is a bare
  `std::fs::rename(&self.tmp, &self.final_dir)`.
- `frogdb-server/crates/persistence/src/snapshot/stager.rs:229-230` (pre-fix) —
  `update_latest_symlink` symlinks and renames, no dir sync.

`rename(2)` is atomic with respect to other processes — which is what FM-PERSISTENCE-017 rests on
and what makes a *process* crash safe — but it is not durable, so a power loss can present any
prefix of the chain.

### Fix

New `frogdb-server/crates/persistence/src/fs_seam.rs`: a narrow `SnapshotFs` trait
(`write`, `rename`, `symlink`, `sync_file`, `sync_dir`) with a `RealFs` implementation, plus a
pass-through `RecordingFs` for tests. `sync_dir` is `File::open(dir)?.sync_all()` on unix and a
no-op on Windows, which cannot open a directory as a file handle through `std::fs` — a save must
not fail on a platform that offers no way to do this.

The stager now holds `fs: Arc<dyn SnapshotFs>` and the rule is "fsync what a rename publishes
before it, fsync the directory that gains the name after it":

- `finalize_metadata` — `sync_file(metadata.json.tmp)` → rename → `sync_dir(tmp)`. The second sync
  also covers the `checkpoint` entry, so the promotion rename cannot become durable ahead of the
  staging directory's contents. RocksDB's `create_checkpoint` already fsyncs the checkpoint's own
  files, so the stager syncs only what it wrote itself.
- `install` — rename → `sync_dir(snapshot_dir)`.
- `update_latest_symlink` — symlink → rename → `sync_dir(snapshot_dir)`.

### Item 3 (`load_latest_metadata` must not silently yield epoch 0)

Fixed differently from the literal wording, deliberately. Refusing to boot on an unreadable
snapshot is wrong — a snapshot is a backup artifact (FM-PERSISTENCE-021) and the live database is
in the WAL, so a damaged backup pointer must not stop the server. What must not happen is the
epoch *collapsing to 0*, which makes the next `BGSAVE` target an existing `snapshot_00001` and
makes retention mis-order the set.

`RocksSnapshotCoordinator::new` now logs the parse failure at `error` instead of swallowing it
with `unwrap_or((0, None))`, and seeds the epoch as
`max(metadata epoch, highest snapshot_NNNNN directory on disk)` via the new
`highest_snapshot_epoch`. This covers the *silent* zero too: `latest` pointing at a directory
whose `metadata.json` is absent returns `Ok((0, None))`, which never surfaced as an error at all.

### Item 4 (`wal_watermark::write`)

Reviewed, deliberately left unsynced. The module already documents why, and
FM-PERSISTENCE-035 pins it: an unsynced watermark can only under-report, which costs a future
corruption detection, never correctness. A missing snapshot is not the benign direction; a stale
watermark is.

### Tests

`frogdb-server/crates/persistence/src/snapshot/tests.rs`, tagged `// FM-PERSISTENCE-023`:

- `stager_fsyncs_metadata_and_staging_dir_before_install`
- `stager_fsyncs_snapshot_dir_after_install_and_after_latest_repoint`

Both assert the recorded op sequence exactly; between them they pin the complete protocol.
`frogdb-server/crates/persistence/src/rocks/tests.rs` adds
`staged_checkpoint_install_fsyncs_the_data_dir_parent` for the install side, and
`coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable` (tagged
`// FM-PERSISTENCE-017`) covers item 3 for both the garbage-JSON and absent-`metadata.json`
shapes. All four fail pre-fix.

### Spec

FM-PERSISTENCE-023 was a `MISSING ([gap: ...])` row; it is now real behavior with the three
forcing tests above. FM-PERSISTENCE-017's invariant gained the epoch-floor rule.
