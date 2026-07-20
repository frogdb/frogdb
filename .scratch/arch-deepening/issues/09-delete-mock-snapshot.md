# 09 — Delete `core/tests/common/mock_snapshot.rs` (deletion test)

Status: needs-triage

## What to build

`core/tests/common/mock_snapshot.rs` is a hand-rolled CAS mock that existed only because the
snapshot begin/finish/reschedule state machine was untestable inside
`RocksSnapshotCoordinator`. Round-8 extracted that machine as the public pure
`SnapshotScheduler`. The mock is now a deletion-test candidate: its consumers should exercise
the real scheduler (or `NoopSnapshotCoordinator`) instead.

## Acceptance criteria

- [ ] `core/tests/common/mock_snapshot.rs` deleted
- [ ] Its consumer tests use `SnapshotScheduler`/`NoopSnapshotCoordinator` with equivalent coverage
- [ ] `just test frogdb-core` green

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report.
