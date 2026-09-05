# 06 — CI: 13 unit tests hit the 15 s nextest hard kill on the ubuntu runner

Status: needs-triage
Type: AFK
Size: S
Origin: Test run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779)

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

`Run unit tests` (`cargo nextest run --all`, 9049 tests, 519 s wall) ended with
`7 failed, 13 timed out`. The 13 TIMEOUTs are all the default profile's `3 × 5 s` hard kill
(`.config/nextest.toml:7`), on tests that are heavy rather than hung:

| test | note |
|---|---|
| `frogdb-replication::quint_conformance` × 7 (`ack_above_live_is_ignored`, `ack_watermark_never_retreats`, `barrier_floor_is_the_live_offset_at_arm_time`, `barrier_while_held`, `channel_overrun_refuses_intake`, `close_and_lag_guards_refuse_a_healthy_session`, `close_inside_window_drains_then_ends_graceful`, `sampled_traces`) | each shells out to `quint run`; the ones that passed took 12.5–13.7 s on the runner |
| `frogdb-redis-regression::main hash_tcl::tcl_hash_fuzzing_1_512_fields`, `..._2_512_fields` | fuzz ports, socket round-trip heavy |
| `frogdb-redis-regression::main scan_tcl::tcl_scan_guarantees_under_write_load` | also timed out on the Sept 3 `main` run |
| `frogdb-telemetry::metrics_usage every_metric_is_emitted_through_its_typed_handle` | also timed out on the Sept 3 `main` run |
| `frogdb-core store::hashmap::tests::scan_stress::scan_present_throughout_is_subset_of_returned` | |

Pre-existing on `main`: the Sept 3 run (76b2a6dae, 450 s) timed out on 2 of these. Not caused by
anything on `build-toolchain/impl` (that branch differs from its base only in workflows and
`.scratch/`).

The 7 test FAILs in the same run are tracked elsewhere: `cluster_handoff_barrier` × 3 →
memory-architecture/26; `frogdb-config` golden counts × 2 → build-toolchain/07; jemalloc × 2 →
memory-architecture/27.

## Options (decision needed)

1. **Recommended:** nextest overrides in the existing style (`.config/nextest.toml` already has a
   dozen "legitimately heavy, not flaky" entries): `binary(quint_conformance)` at
   `30s × 3`, plus the five named tests at `30s × 3`. Cheapest; matches how `tcl_sdiff_fuzzing`,
   HLL and `scan_full_iteration_survives_resizes_mid_scan` were handled.
2. Reduce the work in the tests (fewer quint samples / fuzz iterations) — changes what the tests
   prove.
3. Lower `test-threads` on CI only — slows the whole job for everyone.

## Acceptance criteria (for option 1)

- [ ] overrides added with a comment naming the run and the observed durations, same shape as the neighbours
- [ ] `cargo nextest run -p frogdb-replication --test quint_conformance` green locally
- [ ] a `workflow_dispatch` run of `test.yml` on the integration branch shows 0 timed out

## Files likely touched

- `.config/nextest.toml`

## See also

`test-unit-tests-testbox.yml` (hand-maintained) still lists `install_args: just
cargo:cargo-nextest` — no `node`/quint — so an on-box `cargo nextest run --all` hits the
`quint_conformance` gap 04 fixed in `test.yml`. The workflow itself only prebuilds
(`--no-run`), so it is not red; fix alongside this issue or when the testbox flow next changes.

## Blocked by

None — 04 landed 2026-09-04 (`b6cd9276a` and earlier); the `quint_conformance` durations in
the table predate it and should be re-read from the first post-04 run.

## Decisions

Pending.
