# Tighten cross-slot MULTI/EXEC assertion to pin exact queue-time + EXECABORT contract

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Transactions / Cluster

## Context

The only cluster cross-slot MULTI test,
`test_multi_exec_cross_slot_returns_error`
(`frogdb-server/crates/server/tests/integration_cluster.rs:7846-7884`), accepts an error on *any* of
MULTI, either queued SET, or EXEC — its own in-test comment admits "the exact behavior depends on
implementation." This means the assertion is too weak to catch a regression.

The actual, current behavior is well-defined: FrogDB aborts at **queue time**, via
`try_queue_in_transaction` → `validate_cluster_slots` → `abort_transaction`
(`frogdb-server/crates/server/src/connection/guards.rs:454-461`). Specifically: a same-slot command
queues normally (`+QUEUED`), the different-slot command is rejected immediately with a `CROSSSLOT`
error at queue time (not deferred to EXEC), and the transaction is marked aborted so `EXEC` returns
`EXECABORT`. None of this specificity is pinned by the existing test — a regression that silently
moved slot-crossing detection from queue-time to EXEC-time, or that let EXEC continue with a partial
transaction, would still pass the current test (verdict CONFIRMED L2/C1: correct today, but the test
can't catch a break).

## What to build

Tighten `test_multi_exec_cross_slot_returns_error` (or replace it with a stricter test) to assert the
exact, specific contract instead of "an error occurs somewhere."

## Acceptance criteria

- [ ] Test asserts the first (same-slot) queued command returns `+QUEUED` (proves the failure is
      specifically about slot-crossing detection, not blanket rejection of the whole transaction).
- [ ] Test asserts the *second* (different-slot) queued command is the one that errors, at queue
      time, not deferred to EXEC.
- [ ] Test asserts the exact `CROSSSLOT` error prefix/text (lock the real FrogDB string).
- [ ] Test asserts `EXEC` subsequently returns `EXECABORT` (not a partial execution, not success).

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #3 (`cross-slot-multi-weak-assertion`)
- .scratch/testing-improvements/audit/verdicts-B.md #3 (CONFIRMED L2/C1)
- frogdb-server/crates/server/tests/integration_cluster.rs:7846-7884
- frogdb-server/crates/server/src/connection/guards.rs:454-461
