# 09 — Delete `core/tests/common/mock_snapshot.rs` (deletion test)

Status: ready-for-human

## What to build

`core/tests/common/mock_snapshot.rs` is a hand-rolled CAS mock that existed only because the
snapshot begin/finish/reschedule state machine was untestable inside
`RocksSnapshotCoordinator`. Round-8 extracted that machine as the public pure
`SnapshotScheduler`. The mock is now a deletion-test candidate: its consumers should exercise
the real scheduler (or `NoopSnapshotCoordinator`) instead.

## Acceptance criteria

- [x] `core/tests/common/mock_snapshot.rs` deleted
- [x] Its consumer tests use `SnapshotScheduler`/`NoopSnapshotCoordinator` with equivalent coverage
- [x] `just test frogdb-core` green

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report.

## Comments

### 2026-07-20 — implemented

Deleted `crates/core/tests/common/mock_snapshot.rs` and its `pub mod mock_snapshot;` line in
`crates/core/tests/common/mod.rs`. Migrated its four consumer tests in
`crates/core/tests/concurrency.rs` to drive the real pure `SnapshotScheduler`
(`frogdb_persistence::SnapshotScheduler`) instead of the hand-rolled CAS mock:

- `MockSnapshotCoordinator::new()` → `SnapshotScheduler::with_epoch(0)`
- `start_snapshot() -> Result<u64, "AlreadyInProgress">` → `try_begin() -> Option<u64>` (`None` is
  the former `AlreadyInProgress`)
- `complete_snapshot(epoch)` → `finish_and_maybe_rebegin()` (no follow-up is ever armed in these
  tests, so it always releases and returns `None`)
- mock-internal `completed_epochs()` → a test-local `Mutex<Vec<u64>>` populated on each successful
  begin, preserving the uniqueness assertions.

Coverage kept equivalent (and slightly strengthened): `test_snapshot_atomicity_shuttle` and
`test_snapshot_atomicity_pct` still assert ≥1 success + all completed epochs unique (plus a new
"completed count == success count" check); `test_snapshot_already_in_progress_shuttle` asserts the
second begin is rejected, the epoch does not advance on rejection, and the slot re-acquires cleanly
after finish; `test_snapshot_concurrent_writes_shuttle` still verifies writes proceed unblocked
during a held snapshot.

`SnapshotScheduler` uses `std` atomics (the `frogdb-persistence` crate has no shuttle feature), so
Shuttle does not model-check the atomic orderings themselves — but the tests use only non-blocking
CAS/store ops (no spin-wait), so there is no deadlock risk, and Shuttle still explores the
thread-scheduling interleavings at its `thread::spawn`/`yield_now`/`join` boundaries, which is what
these tests target.

**Test evidence:**
- `cargo nextest run -p frogdb-core --features shuttle -E 'test(/test_snapshot/)'` — 4 migrated
  tests pass.
- `just test frogdb-core` — 759 tests run, 759 passed, 0 skipped (default features; the migrated
  concurrency tests compile via the `shuttle` dev-dependency and pass here too).
