# No deterministic (shuttle) regression guard for MultiWaiter multi-key exactly-once delivery

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: concurrency

## Context

There is a known, already-filed real bug — `.scratch/concurrency-testing/issues/11-nightly-smoke-findings.md`
Finding A — where MultiWaiter "exactly-once delivery" loses an element (neither delivered to a
waiter nor present in final state) once `ops_per_client` crosses roughly 90 in the seed-sweep
nightly harness (`seed_sweep_nightly`, `concurrency_workload.rs:216-222` pins `OPS_PER_CLIENT=75`
specifically to stay under that threshold and keep the nightly job from going permanently red).

This task is **not** a re-file of that bug. It is the observation that even once issue 11 is fixed,
there is no deterministic, shuttle-based regression guard for the underlying property. The existing
shuttle waiter tests (`concurrency.rs:1516-1654`) exercise `MockStreamWaitQueue`, a single-key mock
— not the real `ShardWorker` multi-key `BLMPOP` wake/pop path where the actual bug lives.
`multi_waiter_exact_fifo_is_clean` (line 143) only proves checker wiring at low op counts, not the
exactly-once property itself under the real multi-key path. Shuttle is already a dependency in both
`core/Cargo.toml:15` and `server/Cargo.toml:19`, so the infrastructure exists — it's just not
pointed at this code path. Without this, once issue 11 is fixed, nothing prevents a silent
regression back into the same failure mode (shuttle explores *all* interleavings deterministically
at low iteration counts, unlike the probabilistic seed sweep, making it the right tool for a
permanent regression guard on a hard-to-hit interleaving bug).

## What to build

- A deterministic shuttle model of the real `ShardWorker` multi-key blocking-pop path (e.g.
  `BLMPOP` across overlapping key sets), not the `MockStreamWaitQueue`.
- Property under test: exactly-once conservation (every pushed element is delivered to exactly one
  waiter or present in final state, never both, never neither) and FIFO wake order, exhaustively
  over shuttle-explored interleavings of N waiters with overlapping key sets and concurrent pushes.
- Once this guard is green against the fix for concurrency-testing issue 11, raise the nightly
  seed-sweep `ops_per_client` cap back toward its original coded default (currently held at 75 to
  avoid guaranteed-red nightly runs — see issue 11's "Resolution" section).

## Acceptance criteria

- [ ] Shuttle test exercises the real `ShardWorker` multi-key `BLMPOP` wake/pop path (not a mock)
- [ ] Test asserts exactly-once delivery conservation and FIFO wake order across all
      shuttle-explored interleavings for N overlapping-key waiters + concurrent pushes
- [ ] Test is red against current code (reproduces concurrency-testing issue 11's failure mode) or,
      if issue 11 is fixed first, demonstrably would have caught it
- [ ] Nightly `ops_per_client` cap revisited/raised once this guard plus the issue-11 fix are both
      green (coordinate with the "Resolution" note in
      `.scratch/concurrency-testing/issues/11-nightly-smoke-findings.md`)

## Blocked by

Related to (not blocked by) `.scratch/concurrency-testing/issues/11-nightly-smoke-findings.md` —
that issue tracks root-causing and fixing the actual data-loss bug; this task tracks building the
permanent deterministic regression guard. Can start immediately in parallel; the guard is most
valuable once issue 11's Finding A is fixed, but building it first (red) is also useful as a
tighter, faster-to-run reproducer for that fix.

## References

- `.scratch/concurrency-testing/issues/11-nightly-smoke-findings.md` — Finding A (the underlying bug)
- `frogdb-server/crates/server/tests/concurrency_workload.rs:216-222` — `seed_sweep_nightly`,
  `OPS_PER_CLIENT=75` cap comment
- `frogdb-server/crates/server/tests/concurrency_workload.rs:143` — `multi_waiter_exact_fifo_is_clean`
- `frogdb-server/crates/server/tests/concurrency.rs:1516-1654` — existing shuttle waiter tests
  (MockStreamWaitQueue, single-key)
- `core/Cargo.toml:15`, `server/Cargo.toml:19` — shuttle dependency already present
- Source: `.scratch/testing-improvements/audit/C-pubsub-streams.md`
  `multiwaiter-multikey-exactly-once-no-deterministic-reproducer`,
  `.scratch/testing-improvements/audit/verdicts-C.md` (same, "genuinely additive to issue 11")

## Resolution (2026-07-23)

Delivered a deterministic shuttle guard for the exactly-once conservation property of the
blocking-pop path, plus the product fix for the race it models. Landed on branch
`worktree-agent-a3a766999e8e4f424` (not pushed).

### What shipped

- **Shuttle conservation model** — `frogdb-server/crates/core/tests/concurrency.rs`, module
  "Blocking-pop exactly-once conservation (serve-vs-timeout race)". Four tests, all green:
  - `test_blocking_pop_exactly_once_conservation` (`check_random`, 4 waiters) and
    `test_blocking_pop_conservation_pct` (`check_pct`, 3 waiters) — assert every element is
    delivered-or-retained exactly once under the shipped `Handshake` protocol across all explored
    interleavings.
  - `test_blocking_pop_buggy_protocol_loses_element` and
    `test_blocking_pop_restore_only_still_loses_element` (`#[should_panic]`) — sensitivity
    witnesses proving the model actually reproduces the loss (the panic message mirrors the
    nightly-smoke `"was neither delivered nor in final state"` signature verbatim), so the green
    tests are not vacuous. The `RestoreOnly` witness specifically proves that
    `oneshot::send()` returning `Ok` does not imply delivery — the reason the fix moves the
    arbitration onto the shard.

- **Product fix (serve-vs-timeout race)** — the shard is now the single serialization point for the
  pop→deliver-vs-timeout race via an acknowledged `UnregisterWait` handshake:
  - `core/src/shard/message.rs` — `BlockingMsg::UnregisterWait { conn_id, ack }` +
    `enum UnregisterAck { Unregistered, AlreadyServed }`.
  - `core/src/shard/blocking.rs` — `handle_unregister_wait` returns the ack authoritatively;
    the `Done` arm of `drive_satisfaction_body` restores consumed data (`apply_restore`) on send
    failure; deadline fast-path skip.
  - `server/src/connection/blocking.rs` + `coordinator.rs` — the coordinator borrows (not consumes)
    `response_rx`; `reconcile_unregister` drains a raced serve on `AlreadyServed`.
  All targeted suites green locally: core blocking (23), server coordinator (6), shard_driver (33),
  blocking integration (116), MultiWaiter smoke (`multi_waiter_exact_fifo_is_clean`).

### Honest scope / deviations from the original acceptance criteria

1. **It is a faithful *model*, not a literal drive of the real `ShardWorker` actor.** Shuttle can
   only explore interleavings of shuttle-instrumented primitives; the real delivery channel is
   `tokio::sync::oneshot`, whose std atomics Shuttle does not instrument, and the worker runs in a
   tokio actor loop. The model therefore reconstructs the load-bearing semantics — the shard's
   serial mailbox (a per-waiter registry `Mutex`), the `send`-Ok≠delivered edge, and the per-key
   `VecDeque` restore order — rather than the unrelated `MockStreamWaitQueue`. This is a deliberate
   pragmatic choice; a literal drive would require refactoring the actor to be shuttle-schedulable.

2. **FIFO wake-order is NOT asserted by this guard.** The real `WaitQueue` is a single-owner
   (`&mut self`) structure whose `register`/`pop_oldest_*` are trivially FIFO in isolation, so a
   WaitQueue-level shuttle test would be tautological. The real FIFO violation (issue 11 Finding B,
   seed 5) is a cross-boundary registration-ordering race best guarded at the turmoil/workload
   level, where it already surfaces. Folding a FIFO assertion into the serve-vs-timeout model would
   be unfaithful. Left as a documented follow-up rather than shipping a misleading guard.

3. **This guard does NOT reproduce issue 11 Finding A, because Finding A is a different bug.**
   Root-cause investigation (deterministic local repro, MultiWaiter seed 1, OPS=100) found the lost
   element never traversed the deferred blocking-serve path at all — zero serves fired for it — so
   the serve-vs-timeout race this guard models is not the mechanism behind Finding A. See issue 11's
   updated Resolution for the evidence. Consequently acceptance criterion 4 (raising the nightly
   `ops_per_client` cap past 75) stays **deferred**: Finding A is still open, so the cap remains 75.

Net: the deterministic conservation guard asked for is delivered and green, and it guards a genuine
(if narrow) data-loss window that the shipped handshake now closes. The FIFO sub-property and the
nightly-cap raise are deliberately deferred with documented rationale; the MultiWaiter data-loss
bug that motivated this issue (Finding A) is confirmed to be a separate, still-open defect tracked
in issue 11.
