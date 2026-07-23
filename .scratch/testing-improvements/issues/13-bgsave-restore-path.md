# No test ever restores a server from an actual BGSAVE snapshot artifact

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: persistence

## Context

No test in the suite ever recovers a server from a BGSAVE checkpoint via the operator-documented
restore path. `test_bgsave_snapshot_survives_restart`
(`server/tests/integration_persistence.rs:176-222`) performs `BGSAVE`, writes an additional key,
does a graceful shutdown, and then reopens the *same live directory* — this exercises ordinary WAL
replay, not snapshot loading. The `snapshot_NNNNN` checkpoint directory and its `latest` symlink
(`snapshot/stager.rs:60-66`) are never consulted by any automated path in the test suite: server
boot only ever installs a replica's `checkpoint_ready` staged directory or opens the live DB
directly (`operations/persistence.md:95-99`). The operator-facing "restore from a BGSAVE snapshot"
procedure has zero end-to-end automated coverage confirming it actually works.

Existing coverage is all at the mechanism level, not the end-to-end restore path:
`snapshot/tests.rs:251-486` tests staging/rename/retention mechanics; `rocks/tests.rs:419-654` tests
`load_staged_checkpoint` against hand-written, artificially small 1-key test databases, not a
snapshot produced by an actual `BGSAVE` run. The ops doc itself independently confirms the gap: "no
snapshot-load step at startup ... primary recovery never reads them."

A BGSAVE checkpoint is the operator's disaster-recovery artifact — Redis verifies RDB loadability on
restart; FrogDB has no equivalent verification that a BGSAVE snapshot is actually restorable.

## What to build

- Integration test: run `BGSAVE` against a populated server, then install the resulting
  `snapshot_NNNNN` checkpoint directory into a *fresh* data directory (following the documented
  operator restore procedure exactly), start a server against that fresh directory, and assert all
  keys and types survive intact.
- This test should cover the actual documented operator restore procedure end-to-end, not a
  synthetic hand-built checkpoint.

## Acceptance criteria

- [ ] Integration test performs `BGSAVE`, installs the real resulting snapshot artifact into a
      fresh directory (per the documented restore procedure), starts a server against it, and
      asserts data integrity across multiple key types
- [ ] Test fails if the documented restore procedure is broken (i.e. it exercises the real
      operator-facing steps, not a shortcut)

## Blocked by

None - can start immediately.

## References

- `server/tests/integration_persistence.rs:176-222` — `test_bgsave_snapshot_survives_restart`
  (WAL-replay path, not snapshot load)
- `snapshot/stager.rs:60-66` — checkpoint directory + `latest` symlink, never consulted by any
  automated path
- `operations/persistence.md:95-99` — documented boot behavior (no snapshot-load step)
- `snapshot/tests.rs:251-486` — staging/rename/retention mechanics only
- `rocks/tests.rs:419-654` — `load_staged_checkpoint` against hand-written 1-key DBs
- Source: `.scratch/testing-improvements/audit/D-persistence.md` gap #2, `.scratch/testing-improvements/audit/verdicts-D.md`
  #2 ("Ops doc independently confirms 'no snapshot-load step at startup ... primary recovery never
  reads them'")
