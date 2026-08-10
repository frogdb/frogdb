# 22 — No validation layer generates runtime config values, so FM-CLUSTER-102 survives every generated check

Status: done

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

## Resolution

Built a config-admission proptest layer in `frogdb-cluster-runtime`
(`crates/cluster-runtime/src/failure_detector.rs`, module `tests::config_admission`),
gated behind a new `proptest.workspace = true` dev-dependency
(`crates/cluster-runtime/Cargo.toml`).

- `arb_failure_detector_config()` draws each of the three
  `FailureDetectorConfig` fields independently: `arb_ms_u64()` /
  `arb_fail_threshold()` weight `{0, 1, MAX-1, MAX}` at 1-in-10 each against
  `any::<u64>()`/`any::<u32>()` for the remaining 6-in-10, so the exact
  degenerate literals the three hand-written forcing tests pin are drawn
  often, not left to a 1-in-2^64 chance.
- **C1** `c1_admission_is_total` — builds a detector from any generated
  config and asserts `config()` reports every field inside its documented
  `[MIN_*, MAX_*]` bound, then calls `has_quorum()` to exercise
  `HealthTable::stale_threshold`'s multiply end to end without panicking.
  Universal form of the three point-witness forcing tests.
- **C2** `c2_every_derived_duration_is_finite_and_nonzero` — for the same
  admitted config, asserts `raft_write_timeout() > Duration::ZERO`, mirrors
  `spawn_failure_detector_task`'s interval/timeout construction (checking
  both `> Duration::ZERO`, since a zero duration there is exactly what
  panics `tokio::time::interval`), and asserts a freshly built
  `HealthTable::stale_threshold() > Duration::ZERO`.

Both tagged `// FM-CLUSTER-102` and added to the FM row's `Forced by` list
in `.scratch/hardening/specs/cluster-failure-modes.md`; `just
lint-failure-modes` stays green.

**Sibling-constructor sweep (item 4):** searched the crate for every
constructor taking a millisecond/duration knob straight from a config
struct (`grep` over `pub fn new`, `_ms\b`, `Duration::from_millis`,
`struct.*Config` across `flags.rs`, `handoff_barrier.rs`, `bus.rs`,
`migration_events.rs`, `pubsub.rs`). `FailureDetectorConfig` /
`FailureDetector::new` is the only instance of this shape in the crate
today. `handoff_barrier.rs`'s `barrier_ms: u64` field is relayed
Raft-replicated event data (computed/clamped upstream in `frogdb-cluster`,
not admitted by a constructor here), so it's out of scope — noted directly
in the `config_admission` module's doc comment so the sweep is traceable
and a second config-shaped constructor added later slots into C1/C2 rather
than needing its own property.

**Complement (seed-derived detector config in the scheduler, §3 W4):
skipped**, per task scope — it belongs with the W4 scheduler work and
doesn't catch the FM-CLUSTER-102 defect class (the clamp is what defines
the legal range the scheduler would draw from), only the adjacent
legal-but-extreme-timing class.

**Revert experiment (acceptance criterion 1), proving the new properties
are non-redundant with the three existing forcing tests:**

Reverted `FailureDetector::new` to drop `let config = config.clamped();`
(inverse of `e183dfaa`) and ran `just test frogdb-cluster-runtime`:
**5/80 tests failed**, all `config_admission`-adjacent or the pre-existing
point witnesses — no other test regressed:

- `degenerate_zero_config_is_clamped_to_safe_minimums` (existing forcing test)
- `huge_config_values_are_clamped_and_do_not_overflow_the_staleness_multiply` (existing forcing test)
- `a_zero_check_interval_does_not_panic_the_detector_task` (existing forcing test) —
  panicked with `'period' must be non-zero` inside `tokio::time::interval`
- `c1_admission_is_total` — failed on the first shrunk case,
  `FailureDetectorConfig { check_interval_ms: 0, .. }`, `admitted.check_interval_ms`
  reported back as `0`, outside `[MIN_CHECK_INTERVAL_MS, MAX_CHECK_INTERVAL_MS]`
- `c2_every_derived_duration_is_finite_and_nonzero` — failed on a shrunk
  `check_interval_ms: 0` case with `task_interval > Duration::ZERO` false

Both new properties fail independently of the three existing forcing
tests, satisfying "at least one non-forcing test fails." Restored the
clamp and deleted the `proptest-regressions/failure_detector.txt` file the
failing run generated (it did not exist before the experiment and was
never committed); confirmed `git status --short` / `git diff --stat` both
empty afterward. Re-ran `just test frogdb-cluster-runtime`: 80/80 passed.

**Budget (acceptance criterion 3):** suite was 78 tests / ~0.46s before
this change; with C1/C2 at proptest's default 256 cases each it is 80
tests / ~0.4s — no measurable regression, well under the ~10s ceiling.

**Gates:** `just test frogdb-cluster-runtime` green (80/80); `just
lint-failure-modes` green (278 failure modes, 1397 tags); `just
scratch-check` green; `just mutants-diff frogdb-cluster-runtime` run and
triaged (test-only change).
