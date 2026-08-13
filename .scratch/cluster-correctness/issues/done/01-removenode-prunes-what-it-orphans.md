# 01 — RemoveNode prunes what it orphans; CompleteSlotMigration guards its insert

Status: done

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
`specs/cluster.md`, write the failing forcing tests, then
fix. The round-2 documented non-guarantee pinning tests flip polarity rather than being
deleted.

## Acceptance criteria

- [x] `RemoveNode` leaves no migration or replica reference to the removed node; forcing
      tests construct both dirty shapes and assert the pruning
- [x] `CompleteSlotMigration` targeting an absent owner is rejected, not applied
- [x] Both FM rows amended; `just lint-failure-modes` green
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

None - can start immediately.

## Resolution

Shipped 2026-08-08. Spec-first: **FM-CLUSTER-002** and **FM-CLUSTER-033** amended before the
code, with **FM-CLUSTER-035** and **FM-CLUSTER-036** re-worded where they cited the retired
non-guarantee.

`ClusterState::apply_command`:

- `RemoveNode` now prunes through the same two helpers the `Failover { force: true }` arm
  uses — `prune_migrations_naming` (cancels every migration naming the node as source or
  target, emitting the `SlotHandoffReleased` a pruned prepared handoff owes) and
  `reparent_children` (called with `Some(successor)` by the failover, with `None` here).
  Both were extracted from the failover arm, so the two removal paths cannot drift apart
  again — the asymmetry was the defect.
- Replicas of a forgotten primary are **detached**, not re-parented and not promoted:
  `primary_id` clears, the role is untouched, `CLUSTER NODES` renders `-`. `FORGET` names no
  successor, and a role transition belongs to `SetRole`/`Failover`. This is what Redis'
  `freeClusterNode` does to a removed master's slaves.
- `CompleteSlotMigration` refuses a `target_node` absent from `nodes` with
  `NodeNotFound(target)` — the same idiom `BeginSlotMigration` uses for the same check at
  begin time. Only the target is guarded: it is the id the arm writes into `slot_assignment`,
  and a ghost owner is the INV-REF-1 state the coverage readers would count as healthy
  (FM-CLUSTER-073).

Forcing tests (in `frogdb-cluster`, all three failed against the old behavior first):
`remove_node_prunes_migrations_and_detaches_replicas` (the round-2 pinning test
`remove_node_leaves_migrations_and_replicas_dangling`, flipped and renamed),
`remove_node_prunes_a_migration_that_only_targets_it_and_releases_its_barrier`,
`complete_migration_refuses_a_target_that_is_no_longer_a_member`, plus the negative
`remove_node_keeps_migrations_and_parents_that_do_not_name_it`.

Verified: `just test frogdb-cluster` 234/234, `just check frogdb-server`,
`just lint-failure-modes` OK (278 modes, 1382 references), `just mutants-diff frogdb-cluster`
13 mutants — 10 caught, 3 unviable, 0 missed.

Leaves round-2 issue 62's remaining acceptance criteria (a post-state
`slot_assignment.values() ⊆ nodes.keys()` check and its proptest generalisation) to issues 02
and 03 of this campaign, which is where the catalog and the generator live.
