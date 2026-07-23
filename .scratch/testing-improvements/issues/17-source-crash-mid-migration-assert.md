# test_source_dies_during_migration has zero assertions

Status: done
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

## Resolution

Strengthened the zero-assertion test and split it into three phase-differentiated
variants in `frogdb-server/crates/server/tests/integration_cluster.rs`. All three
assert against each surviving node's Raft-replicated `ClusterState` (via the
harness `node().cluster_state()` seam — `get_slot_owner` / `is_slot_migrating`),
so they check *cluster-wide* convergence, not one node looking healthy.

### Shared assertion helpers (new)

- `assert_slot_converges_to_single_owner(harness, surviving_ids, slot, timeout)`:
  polls within a bound until every surviving node agrees on exactly one owner for
  the slot **and** no surviving node retains a migration record; panics with a
  diagnostic (distinct owners, lingering-record flag, unassigned flag) otherwise.
  Covers acceptance (a) single-owner convergence + (b) no lingering migration.
- `assert_key_served_by_exactly_one(harness, running_ids, key)`: asserts exactly
  one running node serves the key directly (no MOVED/error) and all others MOVED
  to that same address — guards both zero-owner (data loss) and dual-owner.
  Covers acceptance (c).
- `assert_key_served_by_no_running_node(...)`: for the sole-owner-dead-no-replica
  case, asserts no survivor silently claims the orphaned slot (split-brain guard).

### Phase coverage (parametrized as three tests)

- `test_source_dies_during_migration` (strengthened): crash **after
  BeginSlotMigration, before handoff**, source **stays dead**, operator cancels
  (SETSLOT STABLE). Asserts slot rolls back to the (dead) source as the single
  owner, migration record cleared everywhere, and no survivor serves the key.
- `test_source_dies_during_migration_source_recovers` (new, persistence on):
  same crash phase, source **restarts and rejoins**, then cancel. Asserts the
  slot converges rolled-back to the recovered source across all three nodes, no
  lingering record, and the key is served by exactly one node (the source).
  Value durability across a hard crash is checked best-effort (logged), since an
  unsynced write may be dropped; the routing invariant is asserted strictly.
- `test_source_dies_after_migration_complete` (new): crash **post-finalization**
  (SETSLOT NODE committed, keys already on target via `run_full_slot_migration`),
  then kill the old source. Asserts the slot stays owned solely by the target, no
  lingering record, and the migrated key/value is readable from exactly one node
  (the target) — no data loss, no dual ownership.

### Test evidence

- `just test frogdb-server '...'` → 3 passed (≈3.4–4.6s each).
- Flake check: 9 consecutive runs (1 initial + 8-iteration loop) → 9/9 passed,
  **0% flake rate**.
- Regression injection: commenting out `inner.migrations.remove(&slot)` in
  `CancelSlotMigration` (`frogdb-server/crates/cluster/src/commands.rs`) made
  `test_source_dies_during_migration` and `..._source_recovers` fail
  deterministically (all nextest retries) on `lingering migration record = true`;
  `..._after_migration_complete` correctly still passed (uses the Complete path,
  not Cancel). Injection reverted after confirmation.

Note on architecture: with no replicas, a crashed sole slot-owner means its data
is genuinely unrecoverable (acceptance (c)'s "never zero" ideal is unreachable
without a replica). The strengthened tests therefore assert the metadata-level
convergence strictly in that case, and assert full key-readability in the
recovered-source and completed-migration variants where a live node holds the slot.
