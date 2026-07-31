# Leader-only failure detector's false-positive (asymmetric partition) path is untested

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: cluster

## Context

FrogDB's cluster failure detection is deliberately leader-only: only the Raft leader runs TCP-connect health probes against peers and can issue `MarkNodeFailed` (`frogdb-server/crates/server/src/failure_detector.rs:1-11`, documented design rationale citing CockroachDB/FoundationDB-style leader-only detection instead of peer gossip). This is a documented blind spot in `clustering.md:123,230-234`: if a primary is partitioned from the Raft leader's TCP probe but is still reachable by clients, the leader will mark it failed and trigger auto-failover against a node that is, from the client's perspective, still alive and serving.

The only existing coverage, `test_asymmetric_node_failure` (`frogdb-server/crates/server/tests/integration_cluster.rs:1197-`), kills the leader itself and checks re-election — it does not exercise the inverse case: a follower/primary is cut off from the leader's probe specifically while remaining client-reachable. That inverse case is exactly the false-positive-failover scenario the design doc flags as unverified.

## What to build

- New integration test: construct an asymmetric partition that isolates one primary from the Raft leader's TCP probe (network-level, not a process kill) while leaving that primary reachable to a normal client connection.
- Assert what the failure detector does: does it mark the primary failed and trigger auto-failover (expected per the documented design), and if so, what is the fate of writes the "failed" primary continues to accept from clients during the false-positive window (are they later reconciled, lost, or does the primary reject them once it detects it's been demoted)?
- Pin the actual behavior explicitly as a documented contract — this is a known trade-off of leader-only detection, so the goal is a regression guard on the current behavior plus visibility into write-loss bound, not necessarily a fix.

## Acceptance criteria

- [ ] New test constructs a partition isolating exactly the leader-to-primary probe path (not client connectivity) and confirms the primary remains client-reachable during the test.
- [ ] Test asserts whether `MarkNodeFailed`/auto-failover fires under this asymmetric partition.
- [ ] Test asserts the fate of writes accepted by the falsely-marked-failed primary during the partition window (documents the write-loss/split-brain bound, does not just assert "no crash").
- [ ] Findings referenced back into `clustering.md:123,230-234` if behavior differs from what's documented there.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/src/failure_detector.rs:1-11` (leader-only TCP-probe design rationale)
- `frogdb-server/crates/server/tests/integration_cluster.rs:1197` (`test_asymmetric_node_failure`, kills leader — not the inverse case)
- `clustering.md:123,230-234` (documented blind spot)

## Resolution

Status `done`. Built a deterministic turmoil simulation that pins the designed behavior of the
leader-only failure detector under an asymmetric partition, and documented the contract in the
architecture doc.

### Test

`test_cluster_asymmetric_partition_false_failover`
(`frogdb-server/crates/server/tests/simulation.rs`), seeds `[1, 2, 3, 7, 42]`, built on the issue-11
3-node turmoil cluster harness (`spawn_cluster_hosts`, `hold`/`release` isolation). It runs with
`auto_failover` **on** (a new `auto_failover` param was threaded through
`real_frogdb_cluster_node`/`spawn_cluster_hosts`).

Scenario: 3 primaries, no replicas. A non-leader primary (`victim`) owning a known key's slot is
isolated from the leader's probe path by holding **only** the `leader ↔ victim` edge. Both the
leader and the victim keep their link to the third node, so both retain Raft quorum: the leader
stays leader and the victim stays client-reachable. `victim` is thus cut from the leader's TCP probe
while a client on a separate host reaches it normally — the exact inverse of the existing
leader-kill test. The single held edge (vs. a full partition) also keeps the scenario deterministic:
the victim campaigns fruitlessly (openraft leader-stickiness — the third node keeps the leader's
lease and rejects its votes), so leadership never churns across all 5 seeds.

### Pinned behavior (all asserted, matches the documented design — not a bug)

1. **The false positive fires.** The leader marks the alive, client-reachable `victim` `FAIL`
   (`MarkNodeFailed` committed by the `{leader, third}` majority; observed as the `fail` token in
   `CLUSTER NODES` from the third node).
2. **No spurious slot movement.** Even with `auto_failover = true`, `trigger_auto_failover` is a
   no-op because `victim` has no replica to promote (`get_replicas` empty). `victim` stays the sole
   owner of its slot cluster-wide; every peer still redirects to it.
3. **Fate of client writes = no loss.** `victim` keeps quorum via the third node, so the self-fence
   never arms and it accepts a client `SET` during the FAIL window. Because no failover/slot
   transfer occurred, that write survives the heal (read back post-heal). The write-loss/split-brain
   bound in this (replica-less) topology is **zero**.
4. **Automatic recovery.** On heal the leader re-probes `victim` and commits `MarkNodeRecovered`;
   the `FAIL` flag clears.

### Key code findings

- `MarkNodeFailed` (`cluster/src/state.rs`) sets only `flags.fail` + bumps the Config Epoch; it does
  **not** move slots. Slot movement is exclusively the `auto_failover` → `Failover` path, which
  early-returns when the failed primary has no replicas
  (`failure_detector.rs::trigger_auto_failover`).
- The safety net against split-brain is the **self-fence** (`connection/guards.rs:290`,
  `self-fence-on-quorum-loss`, default on), which is independent of the FAIL flag: a primary rejects
  writes with `CLUSTERDOWN` once *it* loses quorum. In this scenario the victim keeps quorum, so it
  correctly keeps serving; a fully-isolated victim would instead fence itself. Either branch is safe.

### Residual / boundary (not covered by this test)

The genuinely dangerous path — a promoted replica and the wrongly-failed old primary both serving
the same slot (split-brain) — needs a **replica topology + auto-failover racing the old primary's
self-fence**. The current turmoil cluster harness bootstraps a flat all-primary cluster with no
replica-role wiring, so that path is out of scope here and remains a separate follow-up.

### Docs

`website/src/content/docs/architecture/clustering.md` (Failover section) gained an "Asymmetric
partitions and false positives" subsection stating the trade-off, the three ways it is bounded
benign, and the replica-topology caveat, cross-referencing the test.
