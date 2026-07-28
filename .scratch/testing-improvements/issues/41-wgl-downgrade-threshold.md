# WGL linearizability-checker downgrade rate is unmonitored in real sweeps

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Jepsen harness / turmoil checker infra (area G)

## Context

`checker.rs` handles the "inconclusive" case explicitly (`:258-266`) and never silently passes on
it. However, `invariants.rs:133-154` downgrades keys that exceed a state budget to
conservation-only checking (a materially weaker check than full linearizability), logging only
via `eprintln!`. The `downgraded_keys` count is never thresholded anywhere in real sweep runs —
it's only asserted in a unit test (`invariants.rs:335`, confirmed at `:310` in the verdicts pass).
The practical consequence: a sweep run where every key ends up downgraded to conservation-only
checking still reports as a clean pass, having performed zero actual linearizability checking —
and nothing in the sweep summary or CI signal would reveal that this happened.

Verdict (adversarial pass): CONFIRMED L2/C2 (`downgraded_keys` only asserted in unit test,
never in a real sweep-level check).

## What to build

Surface `downgraded_keys`/downgrade ratio in the sweep summary output, add a warn threshold and a
nightly-fail threshold, and raise `max_states` for nightly runs so downgrades are rarer under the
larger budget nightly can afford.

## Acceptance criteria

- [x] Sweep summary output reports the downgrade ratio (downgraded keys / total keys checked) for
      each run, not just internally to `invariants.rs`.
- [x] A configurable warn threshold logs/flags when the downgrade ratio exceeds it.
- [x] Nightly CI sweep run fails when the downgrade ratio exceeds a (higher) hard threshold,
      catching the "100% downgraded, technically passed" scenario.
- [x] `max_states` raised for nightly sweep runs specifically (vs. per-PR budget) to reduce
      downgrade frequency where compute budget allows.
- [x] Existing unit test (`invariants.rs:335`) retained; new coverage added at the sweep-summary
      level, not just internal unit level.

## Blocked by

None - can start immediately

## References

- `server/tests/common/invariants.rs:133-154,310,335`
- `crates/testing/src/checker.rs:258-266`
- `.scratch/testing-improvements/audit/G-jepsen-harness.md` (`wgl-downgrade-rate-unmonitored`)
- `.scratch/testing-improvements/audit/verdicts-G.md`

## Resolution

Added `InvariantReport::keys_checked` (total WGL-eligible keys evaluated in stage 2, whether fully
checked or downgraded) and `InvariantReport::downgrade_ratio()` (`downgraded_keys.len() as f64 /
keys_checked as f64`, `0.0` — never NaN — when no keys were eligible) in
`server/tests/common/invariants.rs`. The existing `inconclusive_key_downgrades_not_fails` unit
test (`:335`) is untouched; new unit tests (`downgrade_ratio_is_zero_with_no_wgl_eligible_keys`,
`downgrade_ratio_reflects_partial_downgrade`) pin the new fields.

New module `server/tests/common/sweep_summary.rs` (`SweepSummary`) aggregates `InvariantReport`s
across an entire sweep run (`record`), computes the sweep-wide ratio, and exposes:
- `report_line()` — the sweep summary's own observable output (previously only an `eprintln!`
  buried per-key in `invariants.rs`).
- `warn_if_over(label, warn_ratio)` — loud stderr warning past a soft threshold
  (`FROGDB_WGL_DOWNGRADE_WARN_RATIO`, default 0.05); never fails the run.
- `check_threshold(label, fail_ratio)` — `Err` describing the breach past a hard threshold
  (`FROGDB_WGL_DOWNGRADE_FAIL_RATIO`, default 0.25).

Wired into `server/tests/concurrency_workload.rs`: `run_and_check` now takes an explicit
`max_states` and calls `check_all_with` (not the old `check_all`, which hardcoded
`MAX_WGL_STATES`). `seed_sweep_short_workloads` and `seed_sweep_txheavy` (per-PR tier) accumulate a
`SweepSummary`, print its report line, and call `warn_if_over` at the end. `seed_sweep_nightly`
does the same plus `check_threshold` — a nightly run whose downgrade ratio exceeds the fail
threshold now panics with a message citing the ratio and threshold, independent of whether any
seed tripped a violation (the "100% downgraded, technically passed" scenario the issue called
out). `seed_sweep_nightly` also raises its default WGL state budget from `MAX_WGL_STATES`
(200,000, per-PR) to the new `MAX_WGL_STATES_NIGHTLY` (2,000,000), overridable via
`FROGDB_CONCURRENCY_MAX_STATES`.

Verified end-to-end (not just unit-level): ran `seed_sweep_nightly` with tiny seed counts and
`FROGDB_WGL_DOWNGRADE_FAIL_RATIO=-1` (always-exceeded) — the sweep panicked citing the ratio/
threshold as expected; with `FROGDB_WGL_DOWNGRADE_WARN_RATIO=-1` it printed the warning and still
passed. Both per-PR sweeps and the nightly sweep's summary line were confirmed printing correctly
on real turmoil runs (ratio 0.0000 on clean seeds, since no key hit the op-cap/state-bound
downgrade path in these small workloads).

`Justfile`'s `concurrency-nightly` recipe doc comment updated to mention the new
`FROGDB_CONCURRENCY_MAX_STATES` / `FROGDB_WGL_DOWNGRADE_WARN_RATIO` /
`FROGDB_WGL_DOWNGRADE_FAIL_RATIO` env overrides. No CI workflow YAML changes were needed — the
thresholds default sensibly and are already overridable via env var if a future PR wants to set
them explicitly in `.github/workflows/concurrency-nightly.yml`.
