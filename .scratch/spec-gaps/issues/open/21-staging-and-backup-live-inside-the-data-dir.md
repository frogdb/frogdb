# 21: Staging and backup live inside the data dir — the mount point is never renamed

Status: ready-for-agent

## Origin

Distsys-review MAJ-18 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **restage inside the data dir** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The staged-install mechanism puts its working directories in the **parent** of the
configured data dir (`frogdb-server/crates/persistence/src/rocks/staged.rs:58-67`:
`for_db_dir` = `db_dir.parent()`; `checkpoint_ready` at `:28`, `backup_dir_name` at
`:94`), and `install_staged` renames the data dir itself aside. The spec never states
the resulting precondition (parent writable, rename atomicity across it).

In the standard Kubernetes layout the PVC is mounted *at* `persistence.data-dir`:

- `rename("/data", "/data_backup_T")` fails `EBUSY` — a mount point cannot be
  renamed — so **full resync is permanently impossible on that node**.
- The staged download writes a complete copy of the database into the container's
  ephemeral root filesystem, where it can fill the node's disk.

Both failures appear only during a resync — exactly when they must not.

etcd and CRDB confine all snapshot staging *inside* the data directory so the only
filesystem requirement is the one the operator already provisioned.

## Ruled layout

The mount point is never renamed. Everything lives inside the data dir, one
filesystem:

```
<data-dir>/            <- the PVC mount; never renamed, never concluded "empty"
                          without checking its subdirs
<data-dir>/db/         <- the live RocksDB directory (today: <data-dir> itself)
<data-dir>/staging/    <- staged checkpoint download (today: ../checkpoint_ready)
<data-dir>/backup/     <- install-time backup (today: ../<db>_backup_<ts>)
```

Install = `rename(db, backup)` then `rename(staging, db)`, both inside the mount.
Pre-alpha: no layout compat shim, no migration of old layouts (explicitly ruled
campaign-wide).

## What to build (spec-first; persistence locked, gate 0.85)

1. Spec rows first:
   - State-space row for the data-dir layout: the four paths above, with the
     precondition stated honestly — the only filesystem requirement is the data dir
     itself (single filesystem, writable). Remove/replace any row implying the
     parent is written.
   - Amend the staged-checkpoint rows (and FM-PERSISTENCE-023/025 references) to the
     new paths.
2. Code: `StagedCheckpoint` paths move inside the data dir; `install_staged` renames
   only subdirectories; `data_dir.rs` Phase 0 (marker, `require-existing-data`,
   identity) reads `<data-dir>/db` and treats staging/backup presence per issue 20's
   mid-install probe.
3. Forcing tests:
   - Install completes with the data dir as a filesystem boundary (bind-mount or
     rename-refusing harness seam for the mount-point case; at minimum a test
     asserting no path outside `<data-dir>` is ever created/renamed — a recording-fs
     trace over the full staging+install path fails pre-fix).
   - Resync download lands under `<data-dir>/staging`, never in the parent.
4. Operator/helm/docs: data-dir layout documented; any operator-side path
   assumptions updated.

## Cross-references

- [Issue 20](20-mid-install-crash-recovery-preserves-database-id.md): same rename
  machinery; its Phase 0 probe locations become the new in-dir paths. Coordinate —
  ideally one implementer takes both, this issue's layout first.
- MAJ-19 (fsync-before-publish on the full-sync stager, ruling pending): the shared
  bracketed-rename helper it proposes should be built against this layout.

## Acceptance criteria

- [ ] Layout rows landed; parent-dir precondition gone; `just lint-spec` green
- [ ] No write/rename outside `<data-dir>` on the staging/install path (trace test)
- [ ] Install + resync work with `<data-dir>` as a filesystem boundary
- [ ] Operator/helm/docs updated
- [ ] `just mutants-diff` on frogdb-persistence (locked, 0.85) triaged

## Blocked by

None — land before or with [issue 20](20-mid-install-crash-recovery-preserves-database-id.md).
