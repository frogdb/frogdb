# 18 — A missed failover is never retried, so slots come to rest on a node the cluster has flagged FAIL

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W3 — found by the stateright failover model (issue 11). Not covered by
issue 14 (chained replicas), 15 (graceful failover and migrations), 16 (AssignSlots vs. open
migrations) or 17 (stale source vs. write barrier), so it is filed on its own.

## What is wrong

Automatic failover is **edge-triggered on the `MarkNodeFailed` write**, while the loop that
produces that write is **level-triggered on something else**.

`FailureDetector::reconcile_topology` (cluster-runtime, `failure_detector.rs`) walks the peers
and writes only where the local verdict disagrees with the replicated flag:

* `LocalVerdict::Failed` and not yet `flags.fail` → `MarkNodeFailed`
* `LocalVerdict::Healthy` and `flags.fail` → `MarkNodeRecovered`
* otherwise → nothing

`mark_node_failed` then calls `trigger_auto_failover` **on the success path of that one write**.
`trigger_auto_failover` reads the proposer's *own* snapshot and returns early when

* the failed node is not a primary there, or
* it has no replicas there, or
* `select_failover_target` scores nobody, or
* the `Failover` proposal fails `MAX_ATTEMPTS = 3` times.

Every one of those early returns discards the failover permanently. The next reconciliation pass
sees the verdict and the flag agreeing — the node *is* flagged, that is why the failover was
attempted — so it writes nothing, forever. Nothing else in the system re-examines the topology and
asks "is a flagged primary still holding slots?".

The state the cluster settles into is: a slot owned by a primary the cluster itself has flagged
FAIL, with a healthy replica of that primary sitting beside it, and no further activity. That is
an availability outage with the fix one command away and nobody issuing it.

The cheapest way in needs no operator error and no exotic timing: the proposer's snapshot is
allowed to lag. A node that proposes a failover, then reconciles a *second* verdict before it has
applied the first failover, sees the new primary as a replica and returns early.

## Evidence

Model: `frogdb-server/crates/cluster/src/model/failover/mod.rs`. The exposure is reachable inside
the checked configurations, not only in a scope built to provoke it, so it is pinned there as the
characterization property `a_slot_strands_on_a_failed_primary` — a `sometimes` property that goes
unwitnessed, and therefore red, the day the exposure is closed.

`model::failover::tests::a_slot_strands_on_a_primary_the_cluster_has_failed` finds it in 1075
states with a single detector:

```
[Flip(0, 0), Reconcile(0, 0), Select(0, 1), Propose(0), Flip(0, 1), Reconcile(0, 1),
 Abandon(0), Apply(2), ...]
```

Replayed against the real state machine with no model in the loop by
`model::failover::replay::a_missed_failover_leaves_the_slot_on_a_failed_primary`: node 3 flags
node 1 and fails it over onto node 2; before node 3 applies that entry it flags node 2 as well;
`trigger_auto_failover` for node 2 reads node 3's stale snapshot, sees a replica rather than a
primary, and returns. Node 3 then catches up, every verdict matches every flag, and slot 0 is
stranded on failed node 2 with healthy node 3 replicating it.

Both tests assert the exposure is **still present** — they are characterization tests, so a fix
turns them red and they get flipped rather than silently passing.

## What to build

Spec-first. Add an FM row for "a flagged primary still owning slots is re-examined until it is
not", add the forcing test, then fix.

Candidate rulings, to be decided and recorded in the row:

1. **Make the failover trigger level-triggered too.** `reconcile_topology` already runs on a
   timer as the leader. Give it a second pass: for every primary with `flags.fail` that still owns
   slots and has an eligible replica, run the failover selection — independently of whether this
   pass wrote the flag. Idempotent by construction (once the slots have moved, the primary owns
   nothing and the pass is a no-op), and it subsumes every early-return case above rather than
   patching them one at a time. Costs a scan of the topology per tick.
2. **Retry from the failed write only.** Keep the trigger where it is and re-arm it on the
   `MAX_ATTEMPTS`-exhausted path. Smaller, but it closes only one of the four early returns —
   the stale-snapshot path in the evidence above survives it.
3. **Require the proposer to be caught up before scoring.** Have `trigger_auto_failover` wait for
   its own applied index to reach the entry it just wrote before reading the snapshot. Closes the
   stale-snapshot path and nothing else, and adds a wait to the detector's hot path.

Option 1 is the only one that closes the class. It also removes the need for the trigger to hang
off the write at all, which is what makes the current control flow fragile.

## Acceptance criteria

- [ ] FM row added for the un-retried failover; `just lint-spec` green
- [ ] Forcing test in `frogdb-cluster-runtime` fails first
- [ ] `model::failover::tests::a_slot_strands_on_a_primary_the_cluster_has_failed` and
      `model::failover::replay::a_missed_failover_leaves_the_slot_on_a_failed_primary` flipped to
      assert convergence, and `a_slot_strands_on_a_failed_primary` flipped from `sometimes` to
      `always(!at_rest || unrescued_slot().is_none())`
- [ ] `just mutants-diff` triaged on every touched locked crate

## Blocked by

None.

## Ruling (2026-08-13)

**Option: level-triggered.** `reconcile_topology` gains a pass that runs failover selection for EVERY failed primary still owning slots that has an eligible replica, every tick, independent of which pass wrote the FAIL flag. Idempotent by construction. Flip the characterization test `a_slot_strands_on_a_primary_the_cluster_has_failed` to an always-property (no at-rest stranded slot with an eligible replica).
