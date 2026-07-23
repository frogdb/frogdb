# Replication broadcast-lag disconnect/resync path is under-tested and has a stale doc-comment

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: replication (area E)

## Context

A lagged replica is supposed to be disconnected to force a resync
(`replication/src/replica_session.rs:734-738`); the broadcast channel capacity is 10000
(`replication/src/primary/mod.rs:120`). `test_replica_lag_behavior`
(`server/tests/integration_replication.rs:1798-1848`) under-fills this: it only writes 1000
frames, never triggering the `Lagged` condition it's nominally testing, and asserts nothing
beyond an `eprintln!` of 5 keys. The test also carries a stale doc-comment claiming "Issue #2:
only warns — no resync," which no longer matches the actual disconnect-then-resync behavior in
`replica_session.rs:734-738`.

There is no test that actually drives a replica past the channel capacity, confirms the primary
proceeds without blocking, confirms the lagged replica disconnects, confirms it reconnects and
performs a resync, and confirms the resulting dataset matches the primary's (via `SCAN` or a
checksum). This leaves the resync-after-lag path — a core availability/correctness guarantee for
replication — effectively unverified.

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

An integration test that stalls a replica's socket (or otherwise prevents it from draining), then
writes more than 10000 frames from the primary so the broadcast channel actually lags and drops
the replica. Assert: primary continues processing without blocking on the stalled replica; the
lagged replica disconnects; on reconnect it performs a resync; post-resync, primary and replica
datasets are checksum/`SCAN`-equal; `WAIT` reflects the replica dropping out of the ack count
during the stall and recovering once resynced. Fix the stale "Issue #2: only warns — no resync"
doc-comment.

## Acceptance criteria

- [ ] Test genuinely exceeds the 10000-frame broadcast channel capacity for a stalled replica
      (not the current 1000-write under-fill).
- [ ] Test asserts the lagged replica is disconnected (not just implied via log output).
- [ ] Test asserts reconnect triggers a resync and post-resync data equality (checksum or full
      `SCAN` comparison), not just "some keys present."
- [ ] Test asserts `WAIT` ack-count behavior during the stall and after recovery.
- [ ] Stale doc-comment ("Issue #2: only warns — no resync") in
      `integration_replication.rs:1798-1848` corrected to match actual disconnect/resync
      behavior.

## Blocked by

None - can start immediately

## References

- `replication/src/replica_session.rs:734-738`
- `replication/src/primary/mod.rs:120`
- `server/tests/integration_replication.rs:1798-1848`
- `.scratch/testing-improvements/audit/E-replication.md` (`broadcast-lag-disconnect-resync-untested`, E#8)
- `.scratch/testing-improvements/audit/verdicts-E.md`
