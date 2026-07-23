# WGL linearizability-checker downgrade rate is unmonitored in real sweeps

Status: needs-triage
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

- [ ] Sweep summary output reports the downgrade ratio (downgraded keys / total keys checked) for
      each run, not just internally to `invariants.rs`.
- [ ] A configurable warn threshold logs/flags when the downgrade ratio exceeds it.
- [ ] Nightly CI sweep run fails when the downgrade ratio exceeds a (higher) hard threshold,
      catching the "100% downgraded, technically passed" scenario.
- [ ] `max_states` raised for nightly sweep runs specifically (vs. per-PR budget) to reduce
      downgrade frequency where compute budget allows.
- [ ] Existing unit test (`invariants.rs:335`) retained; new coverage added at the sweep-summary
      level, not just internal unit level.

## Blocked by

None - can start immediately

## References

- `server/tests/common/invariants.rs:133-154,310,335`
- `crates/testing/src/checker.rs:258-266`
- `.scratch/testing-improvements/audit/G-jepsen-harness.md` (`wgl-downgrade-rate-unmonitored`)
- `.scratch/testing-improvements/audit/verdicts-G.md`
