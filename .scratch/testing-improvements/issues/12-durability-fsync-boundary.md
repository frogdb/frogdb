# Fsync durability boundary (sync/periodic/async mode) never actually exercised by a real crash

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: persistence

## Context

Every "crash" in the current test suite preserves the OS page cache, so the different durability
modes (sync / periodic / async fsync) are indistinguishable in test outcomes. The unit-level "crash"
is literally a graceful drop: `CrashTestHarness::crash()` is `self.rocks = None`
(`core/src/persistence/test_harness.rs:183-186`). The Jepsen "kill" is `pkill -KILL`
(`testing/jepsen/frogdb/src/jepsen/frogdb/db.clj:102-105`), but the single-node compose config sets
no durability mode env var (`testing/jepsen/docker-compose.yml:16-19`), so it defaults to
`Periodic{1000}` — and a `SIGKILL` still leaves acked-but-unfsynced WAL bytes sitting in the kernel
page cache, which the kernel then flushes on its own after the process dies. This means neither
crash path can actually distinguish sync from async durability behavior.

As a direct result, the per-mode assertions in the test suite are vacuous: `test_periodic_mode_within_window`
and `test_async_mode_explicit_flush` (`crash_recovery_tests.rs:156-159,190-193`) only assert
`keys_loaded >= 1`; `test_sync_mode_crash_recovery` (line 45) would pass identically even if the
server were actually running in async mode. The turmoil fake WAL is described as "synchronously
durable" (`wal/fake.rs:221-223`), which models append-failure, not unsynced-data loss on a real
power/process-loss event.

`consistency.md:59-69` currently labels Durability as `[Tested]` and states "Per-mode crash windows
are verified" — this claim is unproven and should be corrected. Verification did rule out one
possible root cause: the sync `WriteOptions` wiring itself is confirmed correct
(`flush.rs:358` `is_sync` -> `:175` `set_sync`) — this is a test-vacuity and documentation-accuracy
problem, not a broken sync code path.

## What to build

- A harness that actually severs the OS page cache boundary — either a Jepsen nemesis doing a VM
  hard reset / `sysrq-b` / `dm-flakey` loopback-device fault injection, or an integration-level
  fsync-seam `WriteObserver` that discards writes not yet fsynced when `crash()` is called,
  parameterized by durability mode.
- Per-mode tests asserting the *exact* crash window each mode should tolerate (sync: zero acked-write
  loss; periodic: bounded by the periodic-flush interval; async: unbounded within the async window),
  not just `keys_loaded >= 1`.
- At minimum (if the full fault-injection harness isn't feasible immediately): downgrade
  `consistency.md:59-69`'s Durability label from `[Tested]` to `[Design intent]` to stop overclaiming.

## Acceptance criteria

- [ ] A crash mechanism exists (Jepsen nemesis or fsync-seam injection) that can actually drop
      unfsynced-but-acked writes, distinguishing sync/periodic/async behavior
- [ ] Sync-mode test: zero acked-write loss across the new crash mechanism
- [ ] Periodic-mode test: loss window bounded by the configured flush interval, not unbounded
- [ ] Async-mode test: documents/asserts its (wider) loss window explicitly
- [ ] `consistency.md:59-69` label matches actual test coverage (either genuinely `[Tested]` against
      the new harness, or corrected to `[Design intent]`)

## Blocked by

None - can start immediately.

## References

- `core/src/persistence/test_harness.rs:183-186` — `CrashTestHarness::crash()` is a graceful drop
- `testing/jepsen/frogdb/src/jepsen/frogdb/db.clj:102-105` — `pkill -KILL`
- `testing/jepsen/docker-compose.yml:16-19` — no durability-mode env var, defaults to `Periodic{1000}`
- `server/tests/crash_recovery_tests.rs:45,156-159,190-193` — vacuous per-mode assertions
- `core/src/persistence/wal/fake.rs:221-223` — turmoil fake WAL "synchronously durable" doc comment
- `consistency.md:59-69` — Durability `[Tested]` label to correct
- `flush.rs:358,175` — sync `WriteOptions` wiring, verified correct (not the bug)
- Source: `.scratch/testing-improvements/audit/D-persistence.md` gap #1, `.scratch/testing-improvements/audit/verdicts-D.md`
  #1 ("Sub-hypothesis REFUTED: sync WriteOptions ARE wired... gap = test vacuity + false doc label,
  not broken sync path")
