# 01 — S2 over_aborts floor too weak to catch coarseness regressions

Status: done
Type: AFK
Origin: phase-4b task-7 review (design-level, brief-inherited)

## What to build

Strengthen the S2 WATCH-characterization assertion in `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs`. The current `over_aborts >= 1` floor only proves the coarse per-shard version aborts *at least once*; it cannot catch a partial coarseness regression, and a spurious abort from an unrelated arm folds into the count instead of failing loudly.

## Acceptance criteria

- [x] Assertion distinguishes expected over-abort causes from spurious ones (attribute each abort to the write that caused it, or pin an exact expected count per fixed schedule)
  - `s2_zero_false_negatives_and_over_abort_characterized` in
    `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs` now pins a
    fixed schedule → `Outcome` table (`Commit` / `TrueAbort` / `OverAbort`) and
    attributes every arm. `run_case` returns a `CaseObservation { aborted,
    watched_changed, version_bumped }`; `version_bumped` is read
    non-destructively (empty-key `GetVersion`, no purge) just before EXEC — the
    exact coarse-bump signal `check_watches` compares on. Assertions: (1) abort
    bit == pinned expectation per arm; (2) cause matches the pin — a `TrueAbort`
    must have `watched_changed`, an `OverAbort` must have `!watched_changed &&
    version_bumped`; (3) attribution completeness — any abort must be explained
    by a real change OR a coarse bump (a spurious abort with neither fails);
    (4) exact partition pinned: `true_aborts == 2`, `over_aborts == 2` (replaces
    the `over_aborts >= 1` floor). Added a `ReadWatchedKey` arm (GET of the live
    watched key → `Commit`) so a bump-version-on-read regression is also caught.
- [x] A deliberately-introduced spurious abort (mutation test by hand) fails the test
  - Mutation: added `if *live_at_watch { return false; }` before the final
    `true` in `check_watches` (`frogdb-server/crates/core/src/shard/worker.rs`),
    a spurious abort of every live watch. The OLD `over_aborts >= 1` floor would
    have tolerated it (the abort folds into the count); the strengthened test
    failed loudly at the first Commit arm:
    ```
    assertion `left == right` failed: None: pinned Commit (abort=false), got
    aborted=true (changed=false, version_bumped=false)
      left: true
     right: false
    ```
    That signature (`aborted=true, changed=false, version_bumped=false`) is
    exactly an unattributable spurious abort. Mutation reverted (worker.rs clean,
    no diff); full suite re-run green afterward.
- [x] Existing S2 arms (incl. F3 arm) stay green
  - `just test frogdb-core scenario_s2` → 4/4 pass (`s2_f3_lazy_expiry_...`,
    `regression_gap3_...`, `regression_gap4_...`, and the strengthened
    characterization). Full `just test frogdb-core` → 796/796 pass;
    `just lint frogdb-core` + `cargo clippy -p frogdb-core --tests --features
    shard-driver,fake-wal -- -D warnings` clean.

## Blocked by

None - can start immediately. (If the lazy-expiry-parity proposal replaces the coarse version with per-key tracking, revisit — the characterization itself changes.)
