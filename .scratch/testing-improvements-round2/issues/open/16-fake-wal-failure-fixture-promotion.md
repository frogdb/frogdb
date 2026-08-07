# Promote the fake-WAL failure fixture into `harness.rs` — and give `FakeFailure::Predicate` a user

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I16
LOE: ~0.5 day (estimated)
Tier: A
Area: frogdb-core / `core/tests/shard_driver/` fake-WAL fixture
Asked by: 01

## Context

A working WAL-failure fixture already exists, but it is private to one scenario file, so every
other WAL-failure finding would have to copy it. Promoting it into the shared harness is
mechanical. The same pass should expose the per-key failure primitive that already exists in
production-side code and has never been used by a test.

## Evidence

- `scenario_s6.rs:32-59` has a working fixture (`WalMode::Fake` + `FakeFailure::AtWriteIndex`
  + `set_wal_failure_policy_flag`), private to s6.
- `FakeFailure::Predicate(fn(write_index, key) -> bool)` **already exists and has no users** —
  it is the right primitive for per-key WAL failure injection.

## What to build

1. Move the `scenario_s6.rs:32-59` fixture into `core/tests/shard_driver/harness.rs` as a
   shared constructor, and have s6 call it instead of its private copy.
2. Expose `FakeFailure::Predicate(fn(write_index, key) -> bool)` through the same shared
   fixture, so a test can fail the WAL for a specific key.
3. Add one test using the predicate form, so the primitive stops being unused.

## Acceptance criteria

- [ ] The fixture lives in `core/tests/shard_driver/harness.rs`; `scenario_s6.rs` uses it and
      no longer defines its own.
- [ ] The shared fixture accepts both `FakeFailure::AtWriteIndex` and
      `FakeFailure::Predicate`.
- [ ] At least one test fails the WAL for a named key via `FakeFailure::Predicate` and asserts
      the resulting behaviour; `FakeFailure::Predicate` has at least one caller.
- [ ] `set_wal_failure_policy_flag` handling is inside the fixture, not duplicated at call
      sites.

## Test boundary

Level 3 — WAL failure injection is a shard-worker seam and the fixture belongs in the
`shard_driver` harness; a socket adds nothing to a WAL write failure and would only make the
assertion indirect.

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/` — naturally lands with it.

## Re-triage 2026-08-06

**Verdict: partially-fixed**

Criterion 3 is discharged: `FakeFailure::Predicate` has a caller since the Phase-2 persistence
lock (`1d0df774`, 2026-08-02) — `predicate_failure_selects_by_key` at
`frogdb-server/crates/persistence/src/wal/fake.rs:358-376` fails the WAL for the key `poison` and
asserts only the surrounding writes land. It exercises `FakeWalSink` directly, though, not the
shard-driver path, so the *fixture* half is untouched. Path corrections: the harness moved from
`core/tests/shard_driver/` into its own crate, so `scenario_s6.rs:32-59` →
`frogdb-server/crates/shard-harness/tests/scenario_s6.rs:31-58` (`build_rollback_worker`, still
private to s6, still calling `set_wal_failure_policy_flag` at its own call site, `:57`), and
`core/tests/shard_driver/harness.rs` → `frogdb-server/crates/shard-harness/src/harness.rs`, which
has no WAL-failure constructor at all (`ShardDriver`, `:48-360`). The predicted copy-paste has
already happened once: `shard-harness/tests/shard_driver.rs:99-133`
(`fake_wal_failure_is_injected_at_index`) hand-rolls a second `WalMode::Fake` +
`FakeFailure::AtWriteIndex(0)` builder chain. Criteria 1, 2 and 4 remain.
