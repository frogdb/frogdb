# BGSAVE AlreadyRunning + LASTSAVE reply paths untested against a real coordinator

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: persistence (area D) — merges D#6 (BGSAVE AlreadyRunning) + D#7 (LASTSAVE)

## Context

Two related persistence reply-path gaps, merged into one task since both stem from tests only
ever exercising a `NoopSnapshotCoordinator` instead of the real one:

**BGSAVE AlreadyRunning** (D#6): `handle_bgsave`'s `AlreadyRunning` branch returns
`Response::Simple("Background save already in progress")`
(`server/src/connection/persistence_conn_command.rs:125-128`). All existing unit tests use
`NoopSnapshotCoordinator`, which completes instantly, so this branch never actually fires in
tests (the tests at `:242-258` only document back-to-back `Started` responses). There is no
integration test that overlaps two real `BGSAVE`s against the real coordinator to observe
`AlreadyRunning`. Separately, the reply *type* itself diverges from Redis: Redis 8.6 returns this
condition as a `-ERR` error reply; FrogDB returns a `+` simple string, which most clients treat as
success rather than a rejected request — this divergence should be pinned by a test either way
(fix to match Redis, or explicitly document and test the deliberate deviation).

**LASTSAVE** (D#7): `handle_lastsave` computes
`now.as_secs().saturating_sub(elapsed.as_secs())` (`persistence_conn_command.rs:133-152`), which
involves double truncation (`elapsed` truncated to whole seconds, then subtracted from `now`
truncated to whole seconds) — a potential ±1s error. Unit tests only exercise the `Noop`
coordinator (`:277-294`); there is no integration test verifying `LASTSAVE` starts at `0` and
advances correctly after a real `BGSAVE` completes.

Verdict (adversarial pass): both CONFIRMED L2/C1.

## What to build

1. Integration test overlapping two real `BGSAVE` calls against the real (non-Noop) snapshot
   coordinator, asserting the second observes `AlreadyRunning`. Pin the reply type (currently a
   simple string) — either change it to an error reply to match Redis, or explicitly document and
   test the deviation as intentional.
2. Integration test: `LASTSAVE` returns `0` (or documented sentinel) before any save, then
   advances to a value consistent with real elapsed time after a real `BGSAVE` completes — with
   the ±1s truncation behavior either tightened or explicitly tolerated/documented in the test.

## Acceptance criteria

- [ ] Integration test overlaps two `BGSAVE` calls against a real (non-Noop) coordinator; second
      call's response is asserted to reflect `AlreadyRunning`.
- [ ] `AlreadyRunning` reply type reviewed and pinned by test — either changed to an error reply
      matching Redis 8.6 semantics, or the simple-string deviation explicitly documented and
      tested as intentional.
- [ ] Integration test: `LASTSAVE` before any save vs. after a real `BGSAVE` completes, asserting
      correct advancement (accounting for/documenting the ±1s truncation).
- [ ] Both tests use the real snapshot coordinator, not `NoopSnapshotCoordinator`.

## Blocked by

None - can start immediately

## References

- `server/src/connection/persistence_conn_command.rs:125-128,133-152,242-258,277-294`
- `.scratch/testing-improvements/audit/D-persistence.md` (`bgsave-already-running-path-untested-and-reply-diverges` D#6, `lastsave-only-unit-tested-with-noop-and-lossy-conversion` D#7)
- `.scratch/testing-improvements/audit/verdicts-D.md`
