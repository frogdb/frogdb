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

- [ ] Task 1 — Store reports lazy removals (behavior-neutral seam)
- [ ] Task 2 — Apply lazy-purge effects (gaps 1-3) + flip repros + S2 lazy arm
- [ ] Task 3 — Per-key `live_at_watch` plumbing via `WatchEntry` (behavior-neutral) + `watch_keys` helper
- [ ] Task 4 — `check_watches` honors `live_at_watch` (gap 4) + flip repro + two-watcher arm
- [ ] Task 5 — Proposal acceptance update + full verification (testbox + local concurrency/lint)

## Per-task record

(appended as tasks complete)
