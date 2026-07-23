# Blocking multi-key commands don't have CROSSSLOT negative coverage

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: cluster / blocking commands (pubsub-streams area C)

## Context

`blocking_nil_shape_regression.rs:103` deliberately hash-tags every key onto the same slot
"so the command validates" for its blocking-nil-shape assertions — the cross-slot rejection
path itself is never exercised for blocking multi-key commands. The only CROSSSLOT test in the
suite is for `MSET` (`integration_cluster.rs:7736`); there is no equivalent for `BLPOP`,
`BLMPOP`, `BZMPOP`, or `BLMOVE`.

Single-key CROSSSLOT validation is known to work and is guarded at queue/dispatch time
elsewhere in the cluster path (see `guards.rs:567` and related), but nothing pins that a
blocking multi-key command presented with keys in different slots is rejected immediately with
`CROSSSLOT` and never enters the blocking/wait path. A regression here would manifest as a
client blocking forever (or against a wrong node) instead of getting a fast, correct error —
worse than a silent behavior change because it hangs.

Verdict (adversarial pass): CONFIRMED L2/C2 — no reframing needed.

## What to build

Integration tests, in cluster mode, that pick keys landing in different slots (no hash-tag
trick) for each of `BLPOP`, `BLMPOP`, `BZMPOP`, and `BLMOVE`, and assert an immediate
`CROSSSLOT` error with no blocking/timeout behavior observed.

## Acceptance criteria

- [ ] `BLPOP key1{slotA} key2{slotB} 0` (or equivalent non-tagged keys) returns `CROSSSLOT`
      immediately (bounded, non-blocking) in a cluster-mode integration test.
- [ ] Same coverage added for `BLMPOP`, `BZMPOP`, and `BLMOVE` (source/dest in different slots).
- [ ] Tests assert the command does not enter the blocking wait path (e.g., returns well under
      any configured timeout, or asserts before a timeout would matter).
- [ ] Existing hash-tagged blocking tests (`blocking_nil_shape_regression.rs:103`) are left
      intact — this adds the negative path, not a replacement.

## Blocked by

None - can start immediately

## References

- `server/tests/blocking_nil_shape_regression.rs:103`
- `server/tests/integration_cluster.rs:7736` (existing MSET CROSSSLOT test, pattern to follow)
- `.scratch/testing-improvements/audit/C-pubsub-streams.md` (`blocking-multikey-crossslot-rejection-untested`)
- `.scratch/testing-improvements/audit/verdicts-C.md`
