# Fsync durability boundary (sync/periodic/async mode) never actually exercised by a real crash

Status: done
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

## Resolution

**Approach chosen: fsync-seam fault injection at the `WriteSink::commit(sync)` boundary.**

The durability modes differ only in *whether and when they fsync*. The seam where that decision
lands is `WriteSink::commit(sync)` (`persistence/src/wal/flush.rs`): `FlushEngine::is_sync`
(derived from `DurabilityMode`) is passed straight to `WriteOptions::set_sync`. Injecting a fault
there is the tractable, platform-independent route — it works identically on macOS and aarch64
Linux and is fully deterministic, unlike a real page-cache sever (VM `sysrq-b` / `dm-flakey`),
which is Linux-only, slow, and non-deterministic. It also drives the *real* production flush thread
and mode-selection code; only the storage backend is substituted.

Added to `persistence/src/wal/tests.rs` a `PageCacheSink` (a `WriteSink`) backed by a shared
`PageCacheState` with two tiers: a **durable** log (survives crash) and a volatile **page cache**
log (dropped on crash). `commit(sync=true)` promotes the whole page cache to durable (one fsync of
the shared WAL file makes all preceding writes durable); `commit(sync=false)` only appends to the
page cache. An external `fsync()` models the periodic-sync task's `rocks.flush()` / a RocksDB
memtable flush. `crash()` clears the page cache. The real `flush_thread_loop` + `FlushEngine` are
driven over this sink, parameterized by `DurabilityMode`.

**Test evidence** (`just test frogdb-persistence`, 5 new tests, all pass locally + on aarch64
testbox):
- `test_fsync_seam_sync_flag_matches_mode` — behaviourally pins `is_sync` -> `set_sync`: `Sync`
  commits with `sync=true`, `Periodic`/`Async` with `sync=false` (previously only static-verified).
- `test_sync_mode_zero_acked_write_loss` — 200 writes each acked via a syncing flush; crash drops
  nothing (page cache always empty). **Zero acked-write loss.**
- `test_periodic_mode_loss_bounded_by_flush_interval` — writes before a periodic fsync all survive;
  only the writes since that fsync (the current interval) are lost. **Loss bounded to one interval**,
  never an unbounded suffix.
- `test_async_mode_unbounded_loss_without_fsync` — with no out-of-band fsync, *every* acked-but-
  unsynced write is lost.
- `test_async_mode_window_bounded_only_by_external_fsync` — only the externally-fsynced prefix
  survives; the tail after it is lost with no interval bound. **Documents the wide async window.**

**Doc-label outcome:** `consistency.md` Durability kept as `[Tested]`, but the evidence sentence was
rewritten to (a) point at the new fsync-boundary tests instead of the vacuous `crash_recovery_tests`,
(b) state plainly that they inject the fault at the WAL commit/fsync seam with a page-cache model
rather than real storage, and (c) note that a full end-to-end power-loss test against real storage
(VM reset / block-device fault injection) remains design intent. The two vacuous
`crash_recovery_tests.rs` tests (`test_periodic_mode_within_window`, `test_async_mode_explicit_flush`)
got doc-comments explaining they only exercise recovery plumbing (same-process reopen preserves the
page cache) and pointing at the real seam tests.

**Acceptance criteria:** #1 crash mechanism that drops unfsynced-but-acked writes ✓; #2 sync zero
loss ✓; #3 periodic bounded by interval ✓; #4 async window asserted ✓; #5 doc label matches
coverage ✓.

**Caveats / follow-ups:**
- The harness models the page-cache/fsync boundary; it does not prove RocksDB's own WAL fsync drops
  exactly the unsynced tail on real hardware. A Jepsen nemesis doing a real page-cache sever
  (`dm-flakey` / VM hard reset) is the remaining end-to-end step (left as design intent in the doc).
- Observability nuance noticed while wiring this: `FlushOutcomes::durable_sequence` advances on any
  successful `commit`, including `sync=false` commits — so in `periodic`/`async` modes the reported
  "durable sequence" is really "handed to RocksDB" (may still be in the page cache), not
  fsync-durable. Not in scope here; worth a separate observability-accuracy issue.
