# 01 — S2 over_aborts floor too weak to catch coarseness regressions

Status: needs-triage
Type: AFK
Origin: phase-4b task-7 review (design-level, brief-inherited)

## What to build

Strengthen the S2 WATCH-characterization assertion in `frogdb-server/crates/core/tests/shard_driver/scenario_s2.rs`. The current `over_aborts >= 1` floor only proves the coarse per-shard version aborts *at least once*; it cannot catch a partial coarseness regression, and a spurious abort from an unrelated arm folds into the count instead of failing loudly.

## Acceptance criteria

- [ ] Assertion distinguishes expected over-abort causes from spurious ones (attribute each abort to the write that caused it, or pin an exact expected count per fixed schedule)
- [ ] A deliberately-introduced spurious abort (mutation test by hand) fails the test
- [ ] Existing S2 arms (incl. F3 arm) stay green

## Blocked by

None - can start immediately. (If the lazy-expiry-parity proposal replaces the coarse version with per-key tracking, revisit — the characterization itself changes.)
