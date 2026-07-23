# No deterministic (shuttle) regression guard for MultiWaiter multi-key exactly-once delivery

Status: needs-triage
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
