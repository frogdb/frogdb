# 27: Backup naming uses a persisted monotonic counter; prune refuses anything unparseable

Status: done

## Origin

Distsys-review MIN-7 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **full counter redesign** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)) —
graduated from the [minors sweep (issue 26)](26-distsys-review-minors-sweep.md) to
its own issue by ruling.

## What is wrong

Backup directories are wall-clock-named: `backup_dir_name(db_name, unix_secs)` at
`frogdb-server/crates/persistence/src/rocks/staged.rs:94`, `BACKUP_RETENTION = 1`.
Three failure modes:

1. **NTP step-back** → the newest backup sorts oldest → `prune_backups` deletes the
   only rollback copy.
2. **Same-second reinstall** → identical dir name → rename fails `ENOTEMPTY`.
3. **Malformed suffix** parses via `unwrap_or(0)`, sorts first, and is deleted —
   silent deletion of an unrecognized directory.

Same class the campaign ruled against elsewhere (no wall-clock in state; issue 17's
log-ordered fence): the last-resort rollback copy must not depend on wall-clock
ordering, and prune must never delete what it cannot identify.

## What to build (spec-first; persistence locked, gate 0.85)

1. Spec rows first:
   - Backup-naming row: names carry a **monotonically increasing counter persisted
     alongside the data dir** (crash-safe: counter durably advanced before the name
     is used; reuse the atomic-write seam). Wall-clock never appears in the name.
   - Prune row: retention prunes by counter order; **any entry whose name does not
     parse is refused** — logged + surfaced (metric/warning), never deleted
     (fail-stop over silent deletion).
2. Code: `backup_dir_name` takes the counter; counter persistence + recovery
   (missing counter file with existing backups → recover as max(parsed)+1, never
   reset to 0); `prune_backups` drops `unwrap_or(0)`, partitions parseable vs not,
   refuses the latter.
3. Forcing tests:
   - Clock step-back between two installs → older backup pruned, newest kept
     (fails pre-fix).
   - Two installs same wall-clock second → both succeed, distinct names
     (fails pre-fix with `ENOTEMPTY`).
   - Garbage-named dir planted in the backup location → survives prune, warning
     emitted, install still completes (fails pre-fix: deleted).
   - Counter file lost, backups present → next name > all existing (no collision,
     no reset).

## Cross-references

- [Issue 20](20-mid-install-crash-recovery-preserves-database-id.md): its step 4
  ("rename backup dir to a sequence-based name") is **superseded by this issue** —
  the counter here is that sequence. Its mid-install probe matches counter names.
- [Issue 21](21-staging-and-backup-live-inside-the-data-dir.md): counter file and
  backup dirs live inside `<data-dir>` per its ruled layout (`<data-dir>/backup/`
  as the parent for counter-named entries). Build against that layout.
- Ideally one implementer takes 20 + 21 + 27 together; layout (21) first.

## Acceptance criteria

- [ ] Naming + prune rows landed; `just lint-spec` green
- [ ] Counter persisted crash-safe; recovery from missing counter proven
- [ ] All four forcing tests landed, fail pre-fix
- [ ] `just mutants-diff` on frogdb-persistence (locked, 0.85) triaged

## Blocked by

None — coordinate with issues 20/21 (shared rename machinery and layout).
