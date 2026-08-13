# 20 — A forced auto-failover evicts the old primary from Raft, so it never learns it lost its slots and keeps serving them

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W4 — found by the seeded fault scheduler ([issue 09](../done/09-seeded-fault-scheduler.md))
on its first six-seed sweep, at **seed 3** (`replica-partition` family). Distinct from
[issue 14](14-role-transitions-admit-malformed-parents.md) (role-transition validation),
[issue 15](15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md) (the *graceful*
branch's migration pruning) and [issue 16](16-assign-slots-ignores-open-migrations.md)
(`AssignSlots` vs. open migrations): this is the `force: true` branch's effect on Raft
**membership**, and the defect is what the removed node does afterwards.

## What is wrong

Auto-failover proposes `ClusterCommand::Failover { force: true }`
(`cluster-runtime/src/failure_detector.rs:681`). `voter_change` maps that to
`VoterChange::Remove { node_id: old_primary_id }` (`cluster/src/network.rs:719-725`) — the old
primary is dropped from the Raft **voter set**, not merely demoted in the node table
(FM-CLUSTER-040 / FM-CLUSTER-041 draw exactly this distinction, and the removal is intended).

The consequence nothing covers: a node evicted from Raft has no channel left through which to
learn it was evicted. Its state machine freezes at the last entry it applied — the *pre*-failover
topology, in which it is a healthy primary owning its original slot range. It keeps answering
`GET`/`SET` for those slots. It does not self-fence, because the fence is on quorum loss
(FM-CLUSTER-059) and its stale 3-node view still counts a reachable peer as quorum. When the
partition that triggered the failover heals, nothing reconciles it: the surviving nodes no longer
have it in their node tables, and Raft no longer replicates to it.

Two nodes then serve the same slot, indefinitely, and a client reaches whichever one it asks.

Redis does not have this hole: a returning old master learns a higher `configEpoch` for its slots
through gossip and reconfigures itself as a replica of the winner
(`clusterUpdateSlotsConfigWith`). FrogDB's equivalent channel is Raft, and the force branch is
precisely what closes it.

## Reproduction

```
CLUSTER_SEEDS_START=3 CLUSTER_SEEDS_JOBS=1 just cluster-seeds 1
```

Not rare: **10 of the first 100 seeds** reproduce it — 3, 13, 17, 21, 24, 25, 39, 50, 72 and 99,
all with the same signature, and they are the *only* failures in that range. Seed 72 is the
worst flavor: both halves serve the same key with different values (`n0=v42`, `n2=v8`), so this is
a divergence, not only a routing split. All ten are muzzled in the regression list against this
issue, and the triage rule is written at the top of that file.

At the ruled nightly budget it is the **only** thing the sweep reports:
`CLUSTER_SEEDS_JOBS=6 just cluster-seeds 500` (592s) fails **36 of the 490 unmuzzled seeds**,
every one of them an `XNODE-SLOT-1` with this signature —

```
113 125 126 138 143 157 159 162 170 176 179 183 214 228 234 265 329 349 363 364
387 398 401 406 424 427 430 438 450 452 460 470 478 485 491 493
```

Those 36 are deliberately **not** added to the regression list: 46 replays of one defect would add
minutes to the per-PR suite and buy nothing over the canonical seeds 3 and 72. The consequence is
that the nightly sweep stays red until this lands — which is the honest state, and the triage rule
tells a reader which failures are already accounted for.

or, in the default suite, the regression-seed replay
(`frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt`, seed 3), currently
muzzled as EXPECTED-FAILURE against this issue.

Seed 3's derived schedule:

```
family replica-partition   auto_failover true   attach_replica true
timers[0] election=291ms   timers[1] election=349ms   timers[2] election=407ms
fault[0] hold-edge 0-2 arm=726ms heal=4368ms
```

Node 2 is both the Raft leader (lowest node id) and a PSYNC replica of node 0. Holding the 0-2
edge cuts node 0 off from the leader; the failure detector latches node 0 FAIL and promotes its
replica (node 2) with `force: true`. The edge is released at 4368ms; quiesce then polls for a
single agreed owner for 30 simulated seconds and never gets one:

```
XNODE-SLOT-1: slot 14438 (echo) never converged on a single agreed owner after every fault healed
  n0=value(v33)  view[n2 slave of n0 ... 0-5460 / n1 master 5461-10921 / n0 myself,master 10922-16383]
  n1=err(MOVED 14438 -> n2)  view[n2 master epoch 2 ... 0-5460 10922-16383 / n1 myself,master 5461-10921]
  n2=missing  view[n2 myself,master epoch 2 ... 0-5460 10922-16383 / n1 master 5461-10921]
```

Node 0 is absent from both survivors' node tables and still holds the *entire* pre-failover view
at config epoch 0. It answers the read with the value it accepted before the fault; node 2, the
new owner, answers `missing` for the same key, because ownership moved without the data (there is
no `MIGRATE`, so a promotion carries only what PSYNC already shipped).

Note the data-loss half is a *separate* consequence of the same event and is not what this issue
asks to fix — the single-owner violation is.

## What to build

Spec-first: this needs a ruling before a fix, because all three candidates change observable
failover behavior.

Candidate rulings:

1. **Demote instead of remove.** Auto-failover proposes `force: false`, so the old primary stays
   a voter, keeps receiving entries, and applies its own demotion the moment the link heals.
   Smallest change and it makes the reconciliation channel structural rather than added. Costs:
   the removal semantics FM-CLUSTER-040 documents (and its migration pruning, which
   [issue 15](15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md) is separately
   fixing on the graceful leg) no longer apply to the automatic path, and a genuinely dead node
   lingers as a voter, which is a liveness cost on the next membership change.
2. **Fence on eviction, detected locally.** The old primary notices it is no longer receiving
   Raft entries / is no longer a member and fences its own writes and reads until it re-joins.
   Keeps the removal, closes the client-visible split, but needs a new local signal ("I am not in
   the membership I last applied") and a defined re-join path.
3. **Re-admit on heal.** The surviving leader re-`MEET`s an evicted node it can reach again,
   which re-seeds it from the current state. Closest to Redis's behavior in effect; largest new
   mechanism, and it needs a rule for what happens to the evicted node's slots and data.

Ruling 1 is the smallest claim. Ruling 2 is the one that holds even if the node is evicted for
reasons other than a partition.

## Acceptance criteria

- [ ] Ruling recorded; FM row added or amended in
      `specs/cluster.md` (FM-CLUSTER-040/041 neighborhood), with
      its forcing test in `frogdb-cluster` / `frogdb-cluster-runtime` (fails first)
- [ ] No client-visible window in which two nodes serve the same slot after an automatic
      failover heals
- [ ] The EXPECTED-FAILURE muzzles for seeds 3, 13, 17, 21, 24, 25, 39, 50, 72 and 99 are deleted
      from `frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt` (keeping 3
      and 72 as plain regression seeds), and all of them pass in the default suite
- [ ] `just cluster-seeds 500` is clean
- [ ] `just lint-spec` green
- [ ] `just mutants-diff frogdb-cluster` and `just mutants-diff frogdb-cluster-runtime` triaged

## Blocked by

None.

## Ruling (2026-08-13)

**Option: demote, don't remove.** Automatic failover proposes `force: false`; the demoted primary remains a Raft voter, keeps receiving entries, and applies its own demotion when the partition heals (structural reconciliation — Raft is FrogDB's gossip-equivalent channel). Failover changes roles, never membership; node removal stays an explicit operator action. Accepted liveness cost: a dead node lingers as a voter until the operator removes it. Option 2 (local eviction-fence signal) is a candidate follow-up issue for administrative eviction, not part of this fix. Delete the 9 EXPECTED-FAILURE seed muzzles; acceptance is a clean 500-seed run.

## Amendment (2026-08-13)

**Admin path closed.** Demote-don't-remove covered the failover path only; `CLUSTER FORGET` of a live node and a permanent `add_learner` failure still reproduced this issue's exact shape on the admin path, and demoted-never-removed voters accumulated invisibly. Three additions, all accepted:

1. `CLUSTER FORGET` of a Raft-reachable voter is **refused unless the node is demoted first**; a FORCE override remains as a documented-unsafe escape hatch.
2. The eviction fence is promoted from candidate follow-up to **required** (covers removal after permanent `add_learner` failure).
3. **Dead-voter observability:** count surfaced in CLUSTER INFO + metric.
