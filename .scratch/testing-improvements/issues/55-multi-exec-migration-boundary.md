# Pin MULTI/EXEC-vs-slot-migration boundary semantics deliberately

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Transactions / Cluster

## Context

Cluster-mode MULTI/EXEC test coverage is limited to the READONLY-replica case
(`integration_cluster.rs:6655-6776`, `cluster_sharded_pubsub_tcl.rs:40-181`). No test commits a slot
migration between queue-time and EXEC-time for a transaction. Slot ownership is validated at queue
time (`server/src/connection/guards.rs:567`); the EXEC path
(`server/src/connection/transaction.rs:234-241`) resolves and executes queued commands without
re-running `MOVED`/`ASK`/`TRYAGAIN` checks. This leaves a narrow race window: a transaction that
validated against slot ownership at queue time could execute at EXEC time against a slot that has
since transitioned to `MIGRATING` or moved away entirely, with no re-validation.

Verdict CONFIRMED L1/C2 — deliberately low consequence given the narrow race window and cluster-only
scope, but the current behavior (no re-validation at EXEC) is unpinned and undocumented, so it's
worth locking down deliberately rather than leaving as an accidental property of the code.

## What to build

A test that deliberately queues a keyed write in `MULTI`, transitions the owning slot to `MIGRATING`
(or completes a migration away) from a second connection before `EXEC` runs, then documents and
asserts whatever the actual current behavior is.

## Acceptance criteria

- [ ] Integration test: `MULTI`; queue a keyed write for a slot owned by the current node; from a
      second connection, `SETSLOT MIGRATING` (or complete a migration away) for that slot; `EXEC`
      on the first connection.
- [ ] Test asserts and documents the actual current behavior (no re-validation at EXEC per
      `transaction.rs:234-241`) as the deliberate, accepted semantics — with an explicit code
      comment explaining the race window and why it's accepted.
- [ ] If, during triage, the team decides EXEC *should* re-run `MOVED`/`ASK`/`TRYAGAIN` validation,
      escalate that as a separate design/implementation follow-up rather than silently changing
      behavior as part of this test-only task.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #8 (`multi-exec-across-migration-boundary`)
- .scratch/testing-improvements/audit/verdicts-F.md #8 (CONFIRMED L1/C2)
- server/src/connection/guards.rs:567
- server/src/connection/transaction.rs:234-241
- frogdb-server/crates/server/tests/integration_cluster.rs:6655-6776
