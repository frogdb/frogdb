# The snapshot stager fsyncs neither `metadata.json` nor any directory before promoting `latest`

Status: needs-triage
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

- [ ] A test routes the stager's writes through a filesystem seam that records `sync_all` calls
      and asserts `metadata.json` is fsynced before `install()`. Fails today.
- [ ] The same test asserts the snapshot directory is fsynced before the symlink is repointed.
      Fails today.
- [ ] After truncating `metadata.json` to zero length, `load_latest_metadata` reports an error
      rather than silently yielding epoch 0.
- [ ] If option (b) is chosen instead, the post-crash invariant "`latest` always resolves to a
      complete snapshot" is asserted, and the probabilistic coverage is stated in the test.

## Test boundary

Level 2 with the `SnapshotFs` seam — the stager's own API plus a recording fake is the whole
behaviour. Level 5 without one, because asserting that an fsync happened otherwise needs syscall
interposition; the seam is the cheaper path and is worth introducing.

## Depends on

issue 02 (I2 — subprocess-SIGKILL crash primitive) **only if option (b) is chosen**,
`.scratch/testing-improvements-round2/issues/`. Option (a) depends on nothing.
