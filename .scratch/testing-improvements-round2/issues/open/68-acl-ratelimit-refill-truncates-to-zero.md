# ACL rate-limit refill truncates to zero and clamps non-atomically — legitimate traffic can be rejected forever

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/15 F7 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 3 · effort 1 · priority 17
Area: frogdb-acl / rate limiting

## Context

`refill()` CASes `last_refill_us` to `now` *before* computing the credit. If the elapsed window
is short enough that `cps * SCALE * elapsed_us / 1_000_000` truncates to 0, the elapsed time is
consumed and **zero tokens are credited**. With `cps = 100` that happens for every call spaced
under 10 µs — trivially reached by a pipelining client or several concurrent connections. Once
the bucket drains it never refills, and enforcement is real, so the user is locked out
permanently. The clamp and spend paths are also non-atomic, so a concurrent caller can observe
an over-cap bucket, have its own spend subtracted twice, or see a transiently negative bucket
and reject spuriously.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`acl/src/ratelimit.rs:188-232` — CAS-then-credit ordering, integer-division
truncation, and a clamp implemented as `fetch_add(add)` followed by
`fetch_sub(new_val - cap)` with no atomicity between them, so a concurrent `try_acquire`
can observe an over-cap bucket or have its own spend subtracted twice.
`acl/src/ratelimit.rs:107-142` `try_acquire` likewise spends via `fetch_sub` and refunds
via `fetch_add`, so a concurrent caller can see a transiently negative bucket and reject
spuriously. `acl/src/ratelimit.rs`: 15 untested + 20 single-test functions;
`acl/src/parser.rs:446` `parse_ratelimit` is untested. Enforcement is real:
`server/src/connection/guards.rs:123` and `server/src/connection/transaction.rs:170`.

## What to fix

1. Credit before consuming the elapsed window, or carry the truncated remainder forward, so a
   sequence of sub-resolution calls credits the same total as one long call.
2. Make the clamp a single atomic operation (CAS loop or `fetch_update`) so no caller can
   observe an over-cap or negative bucket.
3. Make `now_us` (`acl/src/ratelimit.rs:23`) injectable — this is the smallest useful slice of
   the injectable-clock seam and is a single production-code seam.
4. Add `acl` to the shuttle feature matrix for the interleaving test.

## Acceptance criteria

- [ ] A unit test with an injectable clock drains the bucket, advances the clock in 1 µs steps
      1000 times, and asserts the credited total equals the credit for 1 ms of elapsed time.
      Today the credited total is 0 — fails today.
- [ ] A unit test asserts steady-state throughput over a simulated second is within ±5% of `cps`
      for `cps ∈ {1, 10, 100, 1000, 100_000}`.
- [ ] A shuttle test with two threads acquiring concurrently asserts total granted ≤ cap and
      that no acquire is rejected while tokens remain.
- [ ] `acl/src/parser.rs:446 parse_ratelimit` gains direct coverage.

## Test boundary

Level 1 for the refill/throughput assertions — a pure arithmetic property over an injectable
clock. The clamp race needs shuttle (level-5 flavoured) because it is an interleaving bug and
nothing below shuttle finds it deterministically; a level-1 test cannot schedule the two threads.

## Depends on

issue 03 (I3 — injectable clock seam; its "smallest useful slice" is exactly
`acl/src/ratelimit.rs:23 now_us`), `.scratch/testing-improvements-round2/issues/`
