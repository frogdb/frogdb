# Self-fence + min-replicas-to-write have no e2e coverage; min-replicas-to-write is inert as a write gate

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4) — min-replicas-to-write sub-issue arguably C3 (inert write-safety gate)
Area: replication (area E)

## Context

`self_fence_on_replica_loss` wires a `ReplicationQuorumChecker`
(`server/src/server/replication_init.rs:186-195`) that rejects writes with
`"CLUSTERDOWN The cluster is down (quorum lost, writes rejected)"` — and does so even on a
standalone node (`guards.rs:286-296`), which is a questionable error string to surface outside
cluster mode. Coverage today is unit-only: `replication_quorum.rs:85-157` and
`MockQuorumChecker` in `guards.rs:794-825`. There is no end-to-end test that actually flags a
replica as lost, confirms writes get rejected, and confirms recovery once quorum is restored.

Separately, `min_replicas_to_write` is only consulted inside `wait_for_acks`
(`replication/src/tracker.rs:254-262`) — it never gates writes at all. A Redis user configuring
`min-replicas-to-write 1` with zero connected replicas expects writes to be refused with
`NOREPLICAS`; in FrogDB the config is silently inert and writes pass through unconditionally.
This is a real behavioral gap, not just a test gap, and the adversarial verification pass flagged
it as arguably C3 (a broken write-safety guarantee) rather than C2 — filing it here as part of
the same e2e task since fixing/testing one naturally exercises the other, but the min-replicas
sub-issue should be treated as higher severity when triaged.

Verdict (adversarial pass): CONFIRMED L2/C2 overall; min-replicas-to-write sub-issue "arguably C3."

## What to build

1. An e2e test that engages the replica-loss fence (kill/disconnect the replica quorum), asserts
   writes are rejected with the documented error, then restores quorum and asserts writes recover.
2. A decision (pin via test either way) on whether `min-replicas-to-write` should actually gate
   writes like Redis's `NOREPLICAS`, or is deliberately out of scope — if it should gate writes,
   wire it into the write path and test `min-replicas-to-write 1` + zero replicas → write
   rejected. If deliberately inert, an explicit test + doc note saying so (current silent
   pass-through is the "certain gap").
3. A turmoil test partitioning a primary from its replicas within the fence timeout window,
   confirming fencing engages within bound.
4. Fix (or explicitly document/test) the standalone `CLUSTERDOWN` error string, since it's
   misleading outside cluster mode.

## Acceptance criteria

- [ ] e2e test: fence engages on replica loss, write rejected with documented error, write
      succeeds again after quorum recovery.
- [ ] `min-replicas-to-write` behavior is pinned by a test (either gating writes with
      `NOREPLICAS`-equivalent, or explicitly documented+tested as inert) — no more silent
      pass-through with zero coverage.
- [ ] Turmoil/deterministic test: partition primary from replicas, assert fencing engages within
      the configured timeout.
- [ ] Standalone `CLUSTERDOWN` error string reviewed — either changed to a standalone-appropriate
      message or explicitly justified and pinned by test.

## Blocked by

None - can start immediately

## References

- `server/src/server/replication_init.rs:186-195`
- `server/src/connection/guards.rs:286-296,794-825`
- `replication/src/tracker.rs:254-262`
- `replication/src/primary/tests.rs` / `replication_quorum.rs:85-157`
- `.scratch/testing-improvements/audit/E-replication.md` (`self-fence-and-min-replicas-write-rejection-untested-e2e`, E#4)
- `.scratch/testing-improvements/audit/verdicts-E.md`
