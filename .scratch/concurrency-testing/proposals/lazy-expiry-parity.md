# Proposal: Lazy-expiry effect parity (per-key expired-watch tracking)

Status: done
Origin: phase-4b final review + task-9/task-12 reviews (2026-07-21, merge `252b39af`)

## Problem

The store-layer lazy purge (`check_and_delete_expired`, `frogdb-server/crates/core/src/store/hashmap.rs`; reached via `get_with_expiry_check` and `purge_if_expired`) is **version- and wait-queue-ignorant**. Active expiry (`apply_expiry_effects`, `event_loop.rs`) bumps the shard version and — since F1 (`63eff9a2`) — drains blocked XREADGROUP waiters to NOGROUP. The lazy path does neither. Phase 4b closed the EXEC-time and WATCH-time validation seams (F3, `673a52c9`) and the active-sweep seam (F1), but four gaps share this one root cause:

1. **Lazy path doesn't drain XREADGROUP waiters.** A blocked XREADGROUP waiter on a TTL-expired stream stays parked until BLOCK timeout when the key is lazily purged by any read (`TYPE`/`XLEN`/`EXISTS`/another XREADGROUP) instead of the active sweep.
2. **Racing lazy read nullifies the F1 drain.** Lazy purge removes the key from the store, so the later active sweep's `deleted_keys` never contains it and the F1 drain never fires — a read racing ahead of the sweep silently disables the fix.
3. **Read-path lazy purge doesn't bump the WATCH version.** A watched key lazily purged by a third party's read leaves the per-shard version untouched → WATCH false negative (under-abort), same class as F3 but on the read path.
4. **Concurrent lazy-purge under-abort for a second watcher.** Watcher B watches `k` live; `k` expires; watcher A's WATCH-time purge (dispatch_core.rs, deliberately no-bump per F3 design) removes `k`; B's later EXEC finds nothing to purge, doesn't bump, commits over a live→gone transition.

## Direction

Redis solves this with per-key state: `signalModifiedKey`/`keyModified` → `touchWatchedKey` fires on expiry-at-lookup (redis/redis PR #7920, issue #7918), plus `wk->expired` recorded at WATCH time. Point-fixing the four gaps separately at the coarse per-shard version is the wrong seam (gap 4 is unfixable that way). Proposed shape:

- Per-key expired-watch tracking (`wk->expired`-style) in the watch registry, replacing reliance on the coarse `shard_version` for expiry-driven invalidation.
- A lazy-purge effects seam: when the store reports a lazy removal, the worker (not the store — store stays version-ignorant, F3 encapsulation constraint) applies the same effects as active expiry: version/watch invalidation + stream-waiter drain.
- Discipline: each gap needs a **real-path (turmoil) repro before its fix lands** (D8 rule). Gap 2 and 3 repros likely mirror `watch_lazy_expiry_false_negative_realpath` (note the turmoil clock trap: TTL is real-clock `Instant`; use `std::thread::sleep`).

## Stage-1 verdicts (D8 verification, 2026-07-21, commit `29454617` merged `1644ea6e`)

**All 4 gaps CONFIRMED.** Repros (all `#[ignore]`d, run with `--run-ignored all`):
- Gap 1: `s5_gap1_lazy_read_does_not_drain_xreadgroup` (shard-driver; turmoil argued unreachable — observing non-drain needs the BLOCK deadline, a real-`Instant` sleep of undefined resolution under turmoil).
- Gap 2: `s5_gap2_lazy_read_nullifies_active_drain` (shard-driver).
- Gap 3: `watch_read_lazy_purge_no_bump_realpath` (turmoil; EXEC commits `*1\r\n+OK\r\n`, flip target `$-1\r\n`).
- Gap 4: `watch_second_watcher_under_abort_realpath` (turmoil; Notify-handshake-forced ordering).

**Design constraints discovered (bind the fix stage):**
1. Only value reads via `get_with_expiry_check` → `check_and_delete_expired` → `uninstall` physically purge. `TYPE` (`key_type`), `EXISTS`/`TOUCH` (`exists_unexpired`), and the FirstKey-hit seam read expiry non-destructively. Anchor the effects seam at the **physical-removal site**, not a per-command is-a-read predicate.
2. The seam must not rely solely on the sweep's `deleted_keys` (gap 2 shows lazy purge starves it).
3. Gap 4 requires per-key `wk->expired` state — a coarse per-shard bump cannot distinguish the stale watcher (must not abort) from the live one (must abort).

## Acceptance criteria

- [x] Turmoil real-path repro (or reasoned unreachability note) for each of gaps 1-4 — all CONFIRMED, see Stage-1 verdicts
- [x] Lazy purge produces the same externally observable effects as active expiry (WATCH abort, XREADGROUP NOGROUP drain) on all paths — `ShardWorker::apply_lazy_purge_effects` (`frogdb-server/crates/core/src/shard/worker.rs`) drains `Store::take_lazily_purged` after every command (`execute_command_inner` wrapper + scatter-part wrapper) and applies one shard-version bump + a per-key `drain_stream_waiters_with_error` (XREADGROUP → NOGROUP) for the batch — task `8ffc47d7`. Gap 4's second-watcher case (unfixable by a shard-wide bump) is closed separately by the per-key `WatchEntry.live_at_watch` clause in `check_watches` — task `0eeea7ef`. Full effect parity (keyspace notifications / tracking / search-index deletion) is explicitly out of scope here — see Status line below.
- [x] Store remains version- and wait-queue-ignorant (worker-seam only) — `Store::take_lazily_purged` (`frogdb-server/crates/core/src/store/mod.rs`, default-empty trait method) only *reports* keys `HashMapStore::check_and_delete_expired` physically removed (`hashmap.rs`, `lazily_purged: Vec<Bytes>` buffer, pushed only on the actual-removal branch, never on `expiry_suppressed`); `HashMapStore` gained no `shard_version`, `wait_queue`, or `check_watches` field. All bump/drain effects live in the worker (`apply_lazy_purge_effects`), confirmed in Task 1's review (seam introduced behavior-neutral, drain-and-discard) and unchanged by Tasks 2-4.
- [x] Shard-driver scenarios extended to cover the closed gaps (S2 lazy arm, S5 lazy arm) — S5 gained the gap-1/gap-2 lazy-read arms (`regression_gap1_lazy_read_drains_xreadgroup`, `regression_gap2_lazy_read_drains_before_sweep`, `frogdb-server/crates/core/tests/shard_driver/scenario_s5.rs`); S2 gained the gap-3 third-party lazy-read arm (`regression_gap3_third_party_lazy_read_aborts_watch`) and the gap-4 full two-watcher interleave arm (`regression_gap4_second_watcher_aborts`, using the `watch_keys` harness helper per design ruling 4), both in `scenario_s2.rs`.
- [x] `regression_`-pinned tests for each confirmed gap — gap 1: `regression_gap1_lazy_read_drains_xreadgroup` (shard-driver); gap 2: `regression_gap2_lazy_read_drains_before_sweep` (shard-driver); gap 3: `regression_gap3_third_party_lazy_read_aborts_watch` (shard-driver) + `regression_watch_read_lazy_purge_aborts_realpath` (turmoil, `frogdb-server/crates/server/tests/simulation.rs`); gap 4: `regression_gap4_second_watcher_aborts` (shard-driver) + `regression_watch_second_watcher_aborts_realpath` (turmoil). None `#[ignore]`d.

## Fix stage (2026-07-21)

All four gaps closed behind the two-mechanism design (per-key `WatchEntry.live_at_watch` + the `Store::take_lazily_purged` / `apply_lazy_purge_effects` worker seam). Task-by-task record: `.superpowers/sdd/lazy-expiry/progress.md`.

- Gap 1 (lazy read doesn't drain XREADGROUP waiters) — fixed `8ffc47d7`, pinned `regression_gap1_lazy_read_drains_xreadgroup`.
- Gap 2 (racing lazy read nullifies the F1 drain) — fixed `8ffc47d7`, pinned `regression_gap2_lazy_read_drains_before_sweep`.
- Gap 3 (read-path lazy purge doesn't bump the WATCH version) — fixed `8ffc47d7`, pinned `regression_gap3_third_party_lazy_read_aborts_watch` + `regression_watch_read_lazy_purge_aborts_realpath`.
- Gap 4 (concurrent lazy-purge under-abort for a second watcher) — fixed `0eeea7ef` (built on the `WatchEntry` plumbing landed behavior-neutral in `0609aa85`), pinned `regression_gap4_second_watcher_aborts` + `regression_watch_second_watcher_aborts_realpath`.

**Status: done.** All five acceptance boxes above are checked; F3 base pins (`s2_f3_lazy_expiry_watched_key_aborts`, `watch_lazy_expiry_false_negative_realpath`, `s2_zero_false_negatives_and_over_abort_characterized`) stay green throughout (reconfirmed at every task in `.superpowers/sdd/lazy-expiry/progress.md`). This proposal covers the WATCH-abort + XREADGROUP-drain effect pair only, per design ruling 3 (deferred effect scope). Keyspace-notification / client-tracking / search-index parity for lazy purge is tracked separately in `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md`.

## References

- Ledger: `.superpowers/sdd/phase4b/progress.md` (tasks 9, 12; follow-up header); fix-stage ledger: `.superpowers/sdd/lazy-expiry/progress.md`
- Fix-stage plan: `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- Effect-scope follow-up: `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md`
- F3 two-seam fix: `673a52c9`; F1 drain: `63eff9a2`
- Spec: `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`
