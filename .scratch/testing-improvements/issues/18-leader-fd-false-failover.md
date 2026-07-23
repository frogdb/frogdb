# Leader-only failure detector's false-positive (asymmetric partition) path is untested

Status: needs-triage
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
