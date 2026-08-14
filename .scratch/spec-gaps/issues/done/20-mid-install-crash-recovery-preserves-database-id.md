# 20: Mid-install crash recovery — boot finishes the install, `database_id` never re-mints

Status: done

## Origin

Distsys-review MAJ-17 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`install_staged` (`frogdb-server/crates/persistence/src/rocks/checkpoint.rs`) installs
a staged checkpoint with two renames: `<db>` → `<db>_backup_<ts>`, then
`checkpoint_ready` → `<db>`. Rename 1 moves the `frogdb_data_dir` marker aside with
the old DB. Power loss before rename 2 leaves **no marker and no files at `<db>`**.

The next boot's Phase 0 (`data_dir.rs:25-30`) runs *before* the install, so it:

- (a) mints a **new** `database_id` (`:133`) — contradicting FM-PERSISTENCE-049; even
  a later successful boot presents as a *different database*, defeating the identity
  that guards against cross-cluster data mixing; or
- (b) under `require-existing-data = true` (`:113-122`) **bails** with "data directory
  is empty" while a complete `checkpoint_ready` sits in the parent. The install code
  never runs; the node is unbootable except via `--force-fresh-data-dir` — which
  discards the very data the staged checkpoint was there to install.

Both contradict FM-PERSISTENCE-025's claim that "the next boot finishes the install
cleanly".

CRDB comparison: `StoreIdent` is written first and read before anything else; a store
with a missing ident but non-empty directory is a hard error, never a re-mint.
Rule: identity establishment is the last thing you re-derive, not the first.

## What to build (spec-first; persistence locked, gate 0.85)

1. Spec rows first:
   - New FM row: "crash between the two install renames" — Observable: next boot
     completes the install (backup present + staged present + `<db>` absent →
     finish rename 2 path), same `database_id` as before the crash; NOT observable:
     a fresh `database_id`, or a `require-existing-data` bail while a completable
     install exists.
   - Amend FM-PERSISTENCE-025/049 to cite the mid-install recovery path.
2. Code:
   - Phase 0 becomes mid-install-aware: before concluding "empty", probe
     `StagedCheckpoint::for_db_dir` and `<db>_backup_*`. If a completable install
     exists, run/finish the install **before** the `require-existing-data` gate and
     before any identity minting.
   - `database_id` carried forward from the backup (or the staged checkpoint's own
     identity) — never re-minted when either exists.
3. Forcing tests (crash-point injection):
   - Kill between rename 1 and rename 2 → next boot finishes the install, data
     present, `database_id` unchanged (fails pre-fix: bail or re-mint).
   - Same crash + `require-existing-data = true` → boot succeeds without
     `--force-fresh-data-dir`.
4. Backup-dir naming: handled by
   [issue 27](27-backup-naming-monotonic-counter-prune-refuses-unparseable.md)
   (persisted monotonic counter) — the probe here matches counter-named entries.

## Cross-references

- [Cluster issue 35](../../../cluster-correctness/issues/open/35-node-identity-outlives-the-process.md):
  same identity-outlives-process family (node id / replica run id / database id) —
  keep persistence surfaces consistent.
- MAJ-18 (staging lives in the data dir's *parent* — deployment constraint): the
  probe locations change if that finding moves staging inside the data dir;
  coordinate if both land.

## Acceptance criteria

- [ ] FM row + FM-025/049 amendments landed; `just lint-spec` green
- [ ] Phase 0 probes staged/backup before "empty"; install precedes the
      `require-existing-data` gate and identity minting
- [ ] `database_id` survives the mid-install crash; forcing tests fail pre-fix
- [ ] `just mutants-diff` on frogdb-persistence (locked, 0.85) triaged

## Blocked by

None — coordinate with MAJ-18's ruling if it relocates the staging directory.
