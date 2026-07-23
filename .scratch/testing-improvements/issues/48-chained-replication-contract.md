# Pin or reject chained replication (replica-of-replica) contract

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Replication

## Context

A replica is constructed with a `NoopBroadcaster` (`server/src/replication/replication_init.rs:177-181`),
and frames it applies are attributed to `REPLICA_INTERNAL_CONN_ID`, which suppresses further
broadcast. Practically, this means if a client issues `REPLICAOF` pointing a second replica at an
existing replica (chained replication / replica-of-replica), the write stream silently does not
propagate to the sub-replica — `REPLICAOF`/`PSYNC` targeting a replica is not rejected at the
command layer, so the operation appears to succeed but data never flows.

There is no test and no documented contract for this scenario today (verdict CONFIRMED L2/C1). This
is a silent-failure footgun: an operator building a fan-out replication topology (a common Redis
pattern) gets no error and no data, with the only symptom being an empty/stale sub-replica.

## What to build

Establish and pin the intended contract. Two valid outcomes exist and the choice affects
implementation scope:

- **(a) Support it**: implement actual chained replication so writes flow transitively through a
  replica to its own replicas, and surface chain depth/topology in `INFO`/`CLUSTER NODES`.
- **(b) Reject it explicitly**: validate at `REPLICAOF`/`PSYNC` command-handling time that the target
  is a primary, not a replica, and return a clear, documented error instead of silently accepting a
  connection that never receives data.

Regardless of which is chosen, first add a test that pins the *current* behavior (data does not flow,
per the `NoopBroadcaster`/`REPLICA_INTERNAL_CONN_ID` suppression) so the gap is at least visible and
guarded before the design decision lands.

## Acceptance criteria

- [ ] Test pinning current behavior: `REPLICAOF` from node B pointing at existing replica A results
      in B accepting the connection but never receiving primary-origin writes (documents the
      as-is silent-failure state).
- [ ] Design decision made and recorded: support chained replication (a) or reject it explicitly (b).
- [ ] Integration test asserting the decided contract end-to-end.
- [ ] Docs (operations/replication or clustering docs) updated to state the contract explicitly.
- [ ] If (a) is chosen, chain depth/topology is visible via INFO/CLUSTER NODES.

## Blocked by

Design decision on target contract (support vs. reject) — flag for human input before implementing
the fix; the regression-pin-only test for current behavior can start immediately.

## References

- .scratch/testing-improvements/audit/E-replication.md #9 (`chained-replication-behavior-undefined`)
- .scratch/testing-improvements/audit/verdicts-E.md #9 (CONFIRMED L2/C1)
- server/src/replication/replication_init.rs:177-181
