# SDD ledger — lazy-expiry parity fix stage

Plan: `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md` (incl. Design rulings 2026-07-21)
Branch: `testing/lazy-expiry-parity` (worktree `frogdb-worktrees/lazy-expiry-parity`, base `6ea87e02`)
Proposal: `.scratch/concurrency-testing/proposals/lazy-expiry-parity.md` (stage-1 verdicts: all 4 gaps CONFIRMED)

Rulings in force: (1) read-triggered bumps accepted; (2) named `WatchEntry` struct end-to-end;
(3) effect scope deferred → `proposals/lazy-expiry-effect-scope.md`; (4) gap-4 full two-watcher
interleave required, `watch_keys` harness helper in Task 3, fallback only on unforeseen blocker
with weak-test guard, turmoil pin never weakened.

Process: fresh implementer subagent per task (Opus tasks 1-4, Sonnet task 5) + review gate per
task + final whole-branch review. Task 5 full-suite verification routes through Blacksmith
testbox (`just tb-warmup` / `just tb-run`); `just concurrency` + `just lint` run locally.

## Task status

- [x] Task 1 — Store reports lazy removals (behavior-neutral seam) — `64e8b3d0` APPROVED
- [x] Task 2 — Apply lazy-purge effects (gaps 1-3) + flip repros + S2 lazy arm — `8ffc47d7` APPROVED
- [x] Task 3 — Per-key `live_at_watch` plumbing via `WatchEntry` (behavior-neutral) + `watch_keys` helper — `0609aa85` APPROVED
- [x] Task 4 — `check_watches` honors `live_at_watch` (gap 4) + flip repro + two-watcher arm — `0eeea7ef` APPROVED
- [ ] Task 5 — Proposal acceptance update + full verification (testbox + local concurrency/lint)

## Per-task record

### Task 1 — `64e8b3d0` (amended once)
Implementer (Opus): seam per plan; deviations endorsed (test seeding via `set`+`set_expiry`, suppression-test ordering, wrapper doc comment). Review (Sonnet) round 1: REVISE — BLOCKER: `ScatterRequest`→`execute_scatter_part` path (MGET/DEL/Touch/Copy/Dump) reached lazy-purge sites with no drain; stale reports would misapply under Task 2. Fix: `execute_scatter_part` split into wrapper + `execute_scatter_part_body`, wrapper drains after every return path; two-part worker test `scatter_mget_drains_lazy_purge_report` (purge fired AND buffer drained). Round 2: APPROVE — body private, single caller, byte-identical elsewhere; 46/46 + F3 pin + lint green. Carried nit for Task 2: delete stale one-line doc comment at execution.rs:602-603 above `execute_scatter_part`.

### Task 2 — `8ffc47d7`
Implementer (Opus): effects filled in (per-key XREADGROUP NOGROUP drain + one bump per non-empty batch, early-return on empty); `purge_expired_watches` refactored to route through the seam (dropped explicit bump + discard); gap-1/2 shard-driver + gap-3 turmoil repros flipped (`regression_gap1_lazy_read_drains_xreadgroup`, `regression_gap2_lazy_read_drains_before_sweep`, `regression_watch_read_lazy_purge_aborts_realpath`); new S2 lazy arm `regression_gap3_third_party_lazy_read_aborts_watch`; carried nit fixed. Review (Opus): APPROVE — refactor sound (bump exactly on real removal; EXEC-time drain parity-correct, no reentrancy; exactly-once drain per purge incl. txn loop); parity exact (no deferred effects added; `expired_keys` not double-counted); pins genuinely inverted. 19/19 + 2/2 turmoil + lint green.
**Carried observation (LOW) for Task 4 review:** `purge_expired_watches` no longer bumps for a pause-suppressed (CLIENT PAUSE ALL) logically-expired watched key (suppressed branch reports nothing). Narrow under-abort window; Task 4's `live_at_watch && !exists_unexpired` clause covers it (present-but-expired reads as not-live). Task-4 reviewer: confirm coverage.

### Task 3 — `0609aa85`
Implementer (Opus): `WatchEntry` end-to-end; `GetVersion` reply `(u64, Vec<bool>)` with `exists_unexpired` probe before no-bump purge; `watch_keys` harness helper; 15 files. Deviations endorsed: re-export files added (lib.rs, shard/mod.rs, conn_command.rs trait); connection map stays `HashMap<Bytes,(usize,u64,bool)>` (shard_id connection-local, `WatchEntry` at boundary via `take_transaction`); replication executor was already `watches: Vec::new()` pre-commit (verified neutral); test sites hardcode `live_at_watch: true` (correct + ignored this task). Review (Opus): APPROVE — check_watches pure version compare preserved; handle_watch zip provably aligned (WATCH is single-shard by SlotValidator, one GetVersion covers all keys; re-WATCH overwrites with fresh liveness = Redis semantics); EXEC single-shard by construction (multi-shard watch set → CROSSSLOT). 9/9 + 43/43 transaction + 17/17 watch + 2/2 turmoil + lint green. Useful Task-4 fact: WATCH/EXEC are single-shard; `watch_keys` returns real computed liveness.

### Task 4 — `0eeea7ef`
Implementer (Opus): clause = version unchanged AND NOT (`live_at_watch && !exists_unexpired`); turmoil pin flipped (`regression_watch_second_watcher_aborts_realpath`, `$-1\r\n`); full-interleave S2 arm `regression_gap4_second_watcher_aborts` (no fallback needed — real liveness both sides asserted `[true]`/`[false]`, physical-absence assert, A-side non-abort sibling); pause-suppressed pin ADDED (`suppressed_expired_watched_key_still_aborts_via_clause`, execution.rs scatter_effect_tests, real worker path, version-unchanged asserted → abort provably clause-only; Task-2 LOW closed). Review (Opus): APPROVE — over-abort analysis: only no-bump removal path in FrogDB is the WATCH-time GetVersion purge = exactly the case Redis aborts (touchWatchedKey/isWatchedKeyExpired); every other removal bumps → clause redundant-never-wrong; no ABA (u64 wrapping). 11/11 + 3/3 turmoil + 43/43 + 17/17 + lints green. Nit (non-blocking): pause pin lives in module named `scatter_effect_tests`.
All 4 gaps CLOSED with pins: gap1 `regression_gap1_lazy_read_drains_xreadgroup`, gap2 `regression_gap2_lazy_read_drains_before_sweep`, gap3 `regression_gap3_third_party_lazy_read_aborts_watch` + `regression_watch_read_lazy_purge_aborts_realpath`, gap4 `regression_gap4_second_watcher_aborts` + `regression_watch_second_watcher_aborts_realpath`.
