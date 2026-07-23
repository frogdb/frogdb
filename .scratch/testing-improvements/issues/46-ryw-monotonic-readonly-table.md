# Table-driven READONLY enforcement + monotonic replica-read regression coverage

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Replication

## Context

`consistency.md` carries [Design intent] rows for read-your-writes (line 27) and monotonic reads
(line 38) that are never converted to executable tests. READONLY enforcement on replicas exists and
is centrally wired on the WRITE command flag, but the only tests exercising it cover a hand-picked
subset of commands: `integration_replication.rs:3940-3975` checks SET/DEL/ZADD only. Nothing proves
the enforcement generalizes to every WRITE-flagged command in the registry (GETEX, GETDEL, SETRANGE,
EXPIRE, COPY, PFMERGE, etc.) — a hand-picked list can pass while a newly added WRITE command slips
through un-enforced.

Verdict on this gap is ADJUSTED to L2/C1: READONLY enforcement is centrally implemented on the WRITE
flag (not per-command special-casing), so this is not a suspected live bug — the missing coverage
only pins design intent and guards against a future regression in the central enforcement path or in
flag assignment for a new command. Replica lazy-expiry-on-read behavior is also untested but is a
distinct sub-topic (see task 32, `replica-expiry-ttl-drift`, E#3) and should not be duplicated here.

No monotonic-read test exists either: nothing writes to the primary, waits for replica ack, then
loops reads on the replica asserting the value never regresses to a pre-write state.

## What to build

1. A registry-driven (not hand-listed) integration test that enumerates every command carrying the
   WRITE flag from the command registry and, for each, executes it against a replica connection,
   asserting a READONLY rejection. The test must fail if a new WRITE command is registered without
   READONLY enforcement — i.e., it walks the registry rather than a fixed table maintained by hand.
2. A monotonic-read regression test: write a value to the primary, `WAIT` for replica
   acknowledgment, then perform a tight loop of reads on the replica asserting the value is never
   observed reverting to its pre-write state.

## Acceptance criteria

- [ ] Registry-driven test enumerates every WRITE-flagged command (not a hand-picked subset) and
      asserts READONLY error against a replica connection for each.
- [ ] Test fails automatically if a future WRITE command is added to the registry without READONLY
      enforcement (proves registry-driven, not list-driven).
- [ ] Monotonic-read test: primary write, WAIT ack, tight replica-read loop asserts the value never
      reverts to a prior state across repeated reads.
- [ ] Replica lazy-expiry-on-read behavior explicitly out of scope here; cross-reference task 32
      (`replica-expiry-ttl-drift`, E#3) rather than duplicating.
- [ ] `consistency.md` RYW (line 27) and monotonic-reads (line 38) rows relabeled from
      [Design intent] to [Tested] once these land.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/E-replication.md #7 (`read-your-writes-monotonic-staleness-untested`)
- .scratch/testing-improvements/audit/verdicts-E.md #7 (ADJUSTED L2/C1)
- frogdb-server/crates/server/tests/integration_replication.rs:3940-3975
- consistency.md lines 27 (RYW), 38 (monotonic reads)
- Task 32 (`replica-expiry-ttl-drift`, E#3) — related but distinct
