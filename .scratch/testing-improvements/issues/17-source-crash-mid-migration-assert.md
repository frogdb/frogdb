# test_source_dies_during_migration has zero assertions

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: cluster

## Context

`test_source_dies_during_migration` (`frogdb-server/crates/server/tests/integration_cluster.rs:3599-`) sets up a 3-node cluster, starts a slot migration (`CLUSTER SETSLOT ... MIGRATING`/`IMPORTING` on slot 950), kills the source node mid-migration, then only `eprintln!`s cluster info and issues `SETSLOT ... STABLE` — there are no assertions anywhere in the test body. The doc comment claims the test verifies "cluster can handle" the crash, but as written it verifies nothing: no check that the slot resolves to exactly one owner, no check that the target node has no lingering half-completed migration state, and no check that keys in the slot remain readable from exactly one node. Today only a panic (e.g. a connection error) could fail this test — a regression that leaves the slot orphaned, dual-owned, or the migration record stuck would pass silently.

Expected behavior: after a source-crash mid-migration and recovery, the cluster should converge to a single, well-defined owner for the slot (either fully migrated to target or rolled back to a recovered source), the target must not retain a stale "importing" migration entry once resolved, and keys in the slot must be readable from exactly one node — never zero (data loss) or ambiguously from both (dual ownership).

## What to build

- Add real assertions to `test_source_dies_during_migration`:
  - (a) after recovery/STABLE, the slot resolves to exactly one owner (via `CLUSTER SLOTS`/`CLUSTER NODES` on all surviving nodes, checked for convergence within a bound).
  - (b) the target node has no lingering migration entry once resolved (no stale "importing" state for the slot).
  - (c) keys previously in the slot are readable from exactly one node post-recovery (not from two, not from zero).
- Consider parametrizing to cover both "source recovers" and "source stays dead" (fails over) sub-cases if only one is currently exercised.

## Acceptance criteria

- [ ] Test asserts single-owner convergence for the migrated slot after source crash + recovery, within a bounded wait.
- [ ] Test asserts no orphaned/lingering migration record on the target once the slot is resolved.
- [ ] Test asserts a representative key from the migrating slot is readable from exactly one node post-resolution.
- [ ] Test fails if a deliberately introduced regression (e.g. skip clearing migration state on target) is injected — confirm by hand before merging, then revert the injected bug.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/tests/integration_cluster.rs:3599` (`test_source_dies_during_migration`, no assertions in body)
