# Regression guard: WAIT inside MULTI must stay non-blocking

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Transactions / Replication

## Context

`WaitCommand::execute` (`frogdb-server/crates/server/src/commands/replication.rs:560-576`) has a
"deny-blocking context (MULTI/EXEC)" branch that returns `count_acked(current_offset)` immediately,
without blocking, when reached. `wait_tcl.rs` only tests standalone arg-validation and no-replica
cases outside of MULTI, and no test anywhere in the suite exercises a queued `WAIT` inside
`MULTI`/`EXEC`.

The original audit treated this as a live-bug risk (does the queued WAIT actually reach the
non-blocking branch, or could it fall through to the blocking `WaitCoordinator`?). Verified against
the code: it's **correct by design and not a bug**. `WAIT` is dispatched via
`ExecutionStrategy::Standard` → `execute_transaction`
(`server/src/connection/transaction.rs:277-298`) → the shard-level non-blocking branch
(`replication.rs:568-575`); the blocking `WaitIntercept` path is documented and enforced to be
reachable only *outside* `MULTI` (`dispatch.rs:491-494`). Verdict: ADJUSTED L1/C2 — this task exists
purely to add the missing regression guard, not to fix a bug.

## What to build

Add integration tests that pin the non-blocking behavior with a tight timeout, so a future refactor
that accidentally routes queued `WAIT` through the blocking coordinator is caught immediately (as a
hang) rather than silently shipping.

## Acceptance criteria

- [ ] Integration test: `MULTI; WAIT 0 0; EXEC` completes within a tight timeout (e.g. <1s) and
      returns `Integer(0)`.
- [ ] Integration test: `MULTI; WAIT 3 100; EXEC` on standalone with 0 replicas connected completes
      within a tight timeout, does not block past the requested 100ms window, and returns
      immediately via the non-blocking shard branch (not the blocking coordinator).
- [ ] Test comments document that this is a regression guard, not a bug fix, citing:
      `ExecutionStrategy::Standard` → `transaction.rs:277-298` → non-blocking branch
      `replication.rs:568-575`; blocking `WaitIntercept` only reachable outside MULTI
      (`dispatch.rs:491-494`).
- [ ] With ≥1 replica connected and acknowledged, a queued `WAIT` at EXEC returns the correct
      acked-replica count.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #4 (`wait-in-multi-untested`)
- .scratch/testing-improvements/audit/verdicts-B.md #4 (ADJUSTED L1/C2 — correct-by-design)
- server/src/commands/replication.rs:560-576
- server/src/connection/transaction.rs:277-298
- server/src/connection/dispatch.rs:491-494
