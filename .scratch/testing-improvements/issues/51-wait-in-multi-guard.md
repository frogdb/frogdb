# Regression guard: WAIT inside MULTI must stay non-blocking

Status: done
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

- [x] Integration test: `MULTI; WAIT 0 0; EXEC` completes within a tight timeout (e.g. <1s) and
      returns `Integer(0)`.
- [x] Integration test: `MULTI; WAIT 3 100; EXEC` on standalone with 0 replicas connected completes
      within a tight timeout, does not block past the requested 100ms window, and returns
      immediately via the non-blocking shard branch (not the blocking coordinator).
- [x] Test comments document that this is a regression guard, not a bug fix, citing:
      `ExecutionStrategy::Standard` → `transaction.rs:277-298` → non-blocking branch
      `replication.rs:568-575`; blocking `WaitIntercept` only reachable outside MULTI
      (`dispatch.rs:491-494`).
- [x] With ≥1 replica connected and acknowledged, a queued `WAIT` at EXEC returns the correct
      acked-replica count.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #4 (`wait-in-multi-untested`)
- .scratch/testing-improvements/audit/verdicts-B.md #4 (ADJUSTED L1/C2 — correct-by-design)
- server/src/commands/replication.rs:560-576
- server/src/connection/transaction.rs:277-298
- server/src/connection/dispatch.rs:491-494

## Resolution

No live bug, as the verdict predicted (ADJUSTED L1/C2): confirmed the routing chain still holds —
`WAIT` keeps `ExecutionStrategy::Standard`, so a queued `WAIT` executes on the shard at EXEC time
via `execute_transaction` (`transaction.rs:277-298`), landing in `WaitCommand::execute`'s
deny-blocking branch (`replication.rs:568-575`); `dispatch.rs`'s `WaitIntercept` stage
(`dispatch.rs:491-494`) — the only path into the blocking connection-level `WaitCoordinator` — is
documented as unreachable once a command is inside MULTI/EXEC. Also verified this matches
documented Redis 8.6 behavior (redis.io `WAIT` docs): "If the command is sent as part of a MULTI
transaction (since Redis 7.0, any context that does not allow blocking, such as inside scripts),
the command does not block but instead just return ASAP the number of replicas that acknowledged
the previous write commands."

While reading the existing suite, found `test_wait_inside_multi_returns_count_immediately`
already living in `frogdb-server/crates/server/tests/integration_replication.rs` (added in an
earlier WAIT/WaitCoordinator refactor) — so the audit's "no test anywhere exercises a queued WAIT
inside MULTI" claim was stale. That test only covered `WAIT 1 0` (implicit "block forever if
regressed" case) with no real replica involved, and didn't cite the routing chain in its comment.
Extended it in place (added citations to the routing chain and to the Redis docs quote) and added
three new tests alongside it in the same file, closing the remaining acceptance-criteria gaps:

- `test_wait_inside_multi_zero_replicas_zero_timeout` — the literal `MULTI; WAIT 0 0; EXEC` case
  (AC1), asserting elapsed < 1s and `Integer(0)`.
- `test_wait_inside_multi_nonzero_timeout_does_not_block` — `MULTI; WAIT 3 100; EXEC` on standalone
  (AC2), asserting elapsed < 50ms (well under even the 100ms window itself, since a bound merely
  under 1s wouldn't distinguish "returned immediately" from "blocked for the full 100ms timeout and
  then returned the same 0").
- `test_wait_inside_multi_returns_correct_acked_count_with_replica` — real primary+replica pair via
  `start_primary_replica_pair` (AC4, the one genuinely new piece of coverage): a plain, blocking
  WAIT first confirms the replica has acked the current offset (sync point), then a queued `WAIT 1
  5000` inside MULTI is asserted to return `Integer(1)` in well under 1s, proving the non-blocking
  shard branch reports the *correct* acked count, not just a fast 0.

Chose `frogdb-server/crates/server/tests/integration_replication.rs` over
`frogdb-redis-regression/tests/multi_tcl.rs` for all four tests: `multi_tcl.rs`'s own file header
explicitly excludes replication-dependent MULTI tests ("needs primary+replica test infrastructure
in regression suite"), the pre-existing correct-by-design test already lived in
`integration_replication.rs`, and that file already has the `start_primary_replica_pair` /
`wait_for_replication` helpers AC4 needs — keeping all four together avoids splitting one
regression-guard concern across two suites and two different replica-setup mechanisms.

Verified: `just check frogdb-server` clean; `cargo clippy -p frogdb-server --tests -- -D warnings`
and `cargo clippy -p frogdb-server -- -D warnings` both clean (via `just lint frogdb-server`);
`just fmt frogdb-server` made no changes. `just test frogdb-server test_wait_inside_multi` (the 4
tests) run green 3x in a row (~0.05-1.1s each, no flakes). Broader `just test frogdb-server
test_wait` (21 tests) run 3x: all passed every time; nextest's "leaky test" marker appeared once on
an unrelated pre-existing test (`test_wait_with_disconnected_replica`) and once on my own
`test_wait_inside_multi_nonzero_timeout_does_not_block`, in different runs — a known sporadic
nextest artifact under parallel load (server shutdown/socket teardown racing the harness's leak
scan), not a functional failure; the flagged tests still reported PASS with correct assertions
each time, and a rerun without the marker confirmed it's noise rather than a leak my new code
introduced.
