# 14 — AddNode/SetRole admit malformed parent pointers (dangling and chained)

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W1 — found while seeding the invariant catalog (issue 02). INV-REF-3B
is the catalog's only DOCUMENTED-EXCEPTION and it cites this file; closing this issue retires
the exception.

## What is wrong

Neither transition validates the parent pointer it writes:

- `ClusterCommand::AddNode` writes the incoming `NodeInfo` verbatim for a node that is not
  already a member — a `NodeInfo::new_replica(9, .., primary_id: 404)` registers a replica of
  a node that does not exist (INV-REF-3, HARD). Forced today by
  `apply_command_catches_a_transition_that_malforms_the_topology` in `frogdb-cluster`, which
  asserts the *hook fires*, not that the transition refuses.
- `ClusterCommand::SetRole { role: Replica, primary_id: Some(p) }` checks that `p` exists but
  not that it is a Primary, so it will chain a replica onto a replica, cycle included
  (`SetRole { node_id: 1, primary_id: Some(2) }` where 2 is already a replica of 1).
  `AddNode` admits the same chain for a brand-new node.

Redis refuses the chain: `CLUSTER REPLICATE` answers "I can only replicate a master, not a
replica", and `clusterSetMaster` re-points a slave at the *master* rather than building a
chain. A chained replica in FrogDB has no replication identity — nothing feeds it, and the
failover candidate search treats it as a candidate for a node that cannot fail over.

## What to build

Spec-first (amend the `SetRole`/`AddNode` FM rows in
`specs/cluster.md` first, then the failing forcing tests,
then the fix):

1. `SetRole` rejects a `primary_id` naming a node that is not a Primary.
2. `AddNode` rejects a new node whose `primary_id` names a non-member, and (same rule as
   above) one that names a replica. Re-registration is unaffected: that path already keeps
   the recorded role and parent and ignores the claimed ones.
3. Retire the INV-REF-3B exception: fold the check back into INV-REF-3 as HARD, or flip
   INV-REF-3B's tier and turn
   `inv_ref_3b_reports_a_replica_parented_onto_a_replica_without_asserting` into a plain
   `assert_reports`.
4. Re-point `apply_command_catches_a_transition_that_malforms_the_topology` (frogdb-cluster,
   `state.rs`) at whatever malformed transition is still reachable — it is the only forcing
   test for the post-apply hook and must not silently stop panicking.

## Affected tests

`frogdb-cluster-runtime`'s `failure_detector::tests::auto_failover_ignores_a_failed_replica`
builds `[primary 1, primary 2, replica 3 of 2, replica 4 of 3]` on purpose: node 4 exists so
that a detector skipping the is-a-primary check would find a candidate and promote it. Once
the chain is illegal that fixture cannot be built through `apply_command`, and the test needs
another way to stay non-vacuous (a test-only unchecked constructor, or an assertion that the
detector rejects the failed node before it ever looks for candidates).

## Acceptance criteria

- [ ] `SetRole` and `AddNode` refuse a parent that is absent or is itself a replica, with
      forcing tests in `frogdb-cluster`
- [ ] FM rows amended; `just lint-spec` green
- [ ] INV-REF-3B is HARD (or folded into INV-REF-3); the catalog has no DOCUMENTED-EXCEPTION
- [ ] `auto_failover_ignores_a_failed_replica` still distinguishes "not a primary" from "no
      candidates"
- [ ] `just mutants-diff frogdb-cluster` triaged

## Blocked by

None.

## Ruling (2026-08-13)

**Confirmed as proposed.** Reject dangling/chained parent pointers at `AddNode`/`SetRole` apply time. Fold INV-REF-3B into INV-REF-3 as a HARD invariant, retiring the catalog's only DOCUMENTED-EXCEPTION. Re-point the forcing test `apply_command_catches_a_transition_that_malforms_the_topology`; the illegal-chain fixture in `auto_failover_ignores_a_failed_replica` gets a test-only unchecked constructor.
