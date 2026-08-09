# 01 — RemoveNode prunes what it orphans; CompleteSlotMigration guards its insert

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W1 (the dirty states INV-REF-1/2/3 would flag), ruled in §8 D2:
fix the behavior, do not register exceptions.

## What to build

Two spec-first behavior changes to `ClusterState::apply_command`:

1. `RemoveNode` prunes every migration whose source or target is the removed node and
   re-parents or detaches replicas whose `primary_id` names it — exactly the pruning
   `Failover { force: true }` already performs. The asymmetry between the two removal
   paths is the defect: retiring a node via FORGET leaves dangling references that the
   forced-failover path cleans up.
2. `CompleteSlotMigration` refuses to insert a slot owner that does not exist in `nodes`
   (today FM-CLUSTER-033 blesses the unguarded insert).

Spec-first: amend the affected FM rows (033 and the RemoveNode non-guarantee row) in
`.scratch/hardening/specs/cluster-failure-modes.md`, write the failing forcing tests, then
fix. The round-2 documented non-guarantee pinning tests flip polarity rather than being
deleted.

## Acceptance criteria

- [ ] `RemoveNode` leaves no migration or replica reference to the removed node; forcing
      tests construct both dirty shapes and assert the pruning
- [ ] `CompleteSlotMigration` targeting an absent owner is rejected, not applied
- [ ] Both FM rows amended; `just lint-failure-modes` green
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None - can start immediately.
