# 25 — no layer sees what a full-resync payload *contains*

Status: needs-triage

## Parent

Found by [issue 15](15-retro-validation-gate.md)'s retro-validation gate, revert (a). [PRD](../PRD.md)
§6 exit criterion 8, §3 W4.

## What the gate found

Revert (a) reinstates the pre-`ebdf7d9e` defect: the full-resync checkpoint is cut before the shard
WALs have drained, so a write the primary already acked can be missing from the payload the replica
loads. The inverse patch is one line in `ReplicaSession::view`
(`frogdb-server/crates/replication/src/replica_session.rs`), which stops reporting
`pre_checkpoint_drain` and so suppresses the `Effect::DrainBeforeCheckpoint` step ahead of
`Effect::CutCheckpoint`.

Three tests failed, and issue 15's rules exclude all three. All three shipped *with* `ebdf7d9e`
(`git log -S` on each name), so all three are the fix's own regression tests; the last is
additionally a named forcing test of FM-REPLICATION-036, which the rules exclude outright:

- `replica_session::tests::fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook`
- `replica_session::tests::fullresync_fails_when_the_pre_checkpoint_drain_fails`
- `integration_replication::test_full_resync_checkpoint_carries_writes_still_pending_in_the_wal`
  (FM-REPLICATION-036)

The spec's own drain row is not reached by this inverse patch at all: `a_full_resync_drains_before_it_cuts`
and its siblings live in `session_machine.rs` and drive the machine with `pre_checkpoint_drain: true`
directly, while the patch changes what `ReplicaSession::view` *reports*. That is itself the shape of
the gap — the machine is proven correct on an input the production seam is free to get wrong.

Every non-forcing layer stayed green:

| layer | run | result |
| --- | --- | --- |
| L1 catalog + `DEBUG REPLICATION CHECK` | `just test frogdb-server replication` | green (only the one forcing test red) |
| L2 R1–R6 | `just test frogdb-replication`, `just test frogdb-replication-runtime` | green |
| L3 stateright smokes | default suite (promotion, feed_gate) | green |
| L4 seeded schedules | `just concurrency-turmoil replication_scheduler` | 32/32 green |
| L4 escalated | `just replication-seeds 500` | 1 passed, 237.9s, rc=0 |

**Verdict: MISS.** This issue is the gap it opens.

## Why each layer is blind

1. **L1/L2/L3 are structurally out of reach.** `ReplicationView` is a projection of *replication
   bookkeeping* — offsets, ids, phases, gate state, registry entries. It carries no keyspace and no
   payload bytes, deliberately (it is the thing the catalog, the proptest model and the stateright
   models all share). "Which writes are inside the checkpoint file" is not expressible in it, so no
   catalog invariant, property or model can ever state the violated fact. This is the same
   "n/a means structurally out of reach" line the cluster campaign's §6.1 draws.
2. **L4 can express it and still does not force it.** The sweep runs real servers and already checks
   XREPL-1 (no acked write lost on promotion) and XREPL-2 (replica history is a prefix of the
   primary's), so the *check* exists. What is missing is the *state*: the defect only manifests when
   an acked-but-not-yet-flushed write is sitting in a shard's batch window at the instant the
   checkpoint is cut. `simulation::replication_scheduler` builds its nodes with
   `PersistenceConfig { enabled, data_dir, ..Default::default() }` — the default batch-flush window,
   which under turmoil's fast virtual clock closes essentially immediately. The window the defect
   needs is never open, at any seed, which is why 500 seeds cost 238s and found nothing.
3. **There is also no cross-node check for the payload itself.** XREPL-1 is about promotion;
   XREPL-2 compares *applied history*, which a replica that loaded a short checkpoint reports
   consistently with its own (short) state. A replica can be internally coherent and still be
   missing an acked write.

## What closing it takes

Two parts, both in W4:

1. **Force the state.** Give the `StagedCheckpoint` payload shape a persistence config with a batch
   window wide enough that an acked write is reliably still un-flushed when the full resync cuts —
   i.e. make "acked but not yet on disk" a steady state the scheduler passes through, not a
   microsecond race. Without this no amount of seeds helps.
2. **Add the check.** A cross-node `XREPL-4`: every write the primary acked before a replica's full
   resync completed is readable on that replica once it reaches `Streaming`. This is the check that
   turns (1) into a red run, and it is worth having independently of this defect — it is the only
   assertion in the sweep that would look at data rather than at offsets.

The fix belongs in
`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs`; the acceptance test is
the revert itself: with (1) and (2) in place, the one-line inverse patch above must turn the
seven-seed smoke red, with the forcing tests disabled.

## Note on scope

This is a *layer* gap, not a product defect — `ebdf7d9e` is in the tree and the behaviour is
correct today. Nothing here needs a new `FM-REPLICATION` row; the existing row for the
checkpoint-drain ordering already names its forcing tests. What is owed is a second, independent
witness for a fact that currently has exactly one.
