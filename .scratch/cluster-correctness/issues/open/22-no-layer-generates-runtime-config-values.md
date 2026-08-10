# 22 — No validation layer generates runtime config values, so FM-CLUSTER-102 survives every generated check

Status: needs-triage

## Parent

[PRD](../../PRD.md) §6.1 — filed by the retro-validation gate (issue 13). FM-CLUSTER-102
(`FailureDetectorConfig` trusted verbatim; `check_interval_ms = 0` panics the detector task) is
the second of the two audit defects that **no** layer catches when the fix is reverted. Nearest
owner is §3 W2 (property harness), whose scope stops at the `frogdb-cluster` state machine.

## What the experiment showed

Revert (inverse of `e183dfaa`, `cluster-runtime/src/failure_detector.rs::FailureDetector::new`):
drop `let config = config.clamped();`.

Result with the three spec forcing tests
(`degenerate_zero_config_is_clamped_to_safe_minimums`,
`huge_config_values_are_clamped_and_do_not_overflow_the_staleness_multiply`,
`a_zero_check_interval_does_not_panic_the_detector_task`) excluded from the judgment:

| layer | result |
|---|---|
| L1 invariant catalog + hooks (`just test frogdb-cluster-runtime`) | **miss** — 75/78, and the only three failures are the forcing tests |
| L2 properties P1–P4 | structurally out of reach — they live in `frogdb-cluster` and drive `apply_command`; the detector is a different crate with no state-machine surface |
| L3 stateright | structurally out of reach — model 2 generates detector *verdicts*, never a detector *config* |
| L4 seeded schedules | structurally out of reach — the scheduler derives partitions, crashes, delays and an `auto_failover` flag from the seed, but never varies `FailureDetectorConfig`; every sim runs the harness default |

The defect class is "a value that arrives from `frogdb.conf` reaches a constructor that assumes
it is sane". `frogdb-cluster-runtime` has no `proptest` dependency at all, and the seed-derived
schedule space deliberately excludes timing knobs so runs stay comparable across seeds — so the
degenerate value is never constructed by anything except a hand-written test.

## What to build

A **generated config-admission layer** covering the runtime's config structs, sized as a small
in-crate proptest rather than new machinery:

1. `proptest` dev-dependency in `frogdb-cluster-runtime`; `arb_failure_detector_config()` over
   the full `u64`/`u32` ranges with the degenerate values (0, 1, `MAX`, `MAX - 1`) weighted in.
2. Property C1 — **admission is total**: `FailureDetector::new(cfg)` never panics, and
   `config()` reports values inside `[MIN_*, MAX_*]` for every generated input. This is the
   universal form of the three point witnesses and it fails on the first case with the clamp
   removed.
3. Property C2 — **every derived duration is finite and non-zero**: `raft_write_timeout`, the
   task's interval/timeout construction and `HealthTable::stale_threshold`'s
   `check_interval * (fail_threshold + 2)` are computed for each generated config, so an
   overflow in a *future* derivation is caught by the same property rather than needing its own
   row.
4. Sweep the sibling constructors in the same crate for the same shape (anything taking a
   millisecond knob straight from config) and put them behind the same property, so this lands
   as a rule and not a one-off.

Complement, cheap and worth doing in the same change: extend the seeded scheduler (§3 W4) to
derive `FailureDetectorConfig` from the seed within the *clamped* range, so timing skew joins
the fault space. That does not catch this defect (the clamp is what defines the range) but it
covers the adjacent class — a legal-but-extreme detector config interacting with a partition
schedule.

## Acceptance criteria

- Reverting the FM-CLUSTER-102 clamp makes at least one **non-forcing** test fail.
- The properties live in `frogdb-cluster-runtime` so `cargo mutants -p frogdb-cluster-runtime`
  scores them.
- Default case count keeps `just test frogdb-cluster-runtime` under its current runtime budget.
