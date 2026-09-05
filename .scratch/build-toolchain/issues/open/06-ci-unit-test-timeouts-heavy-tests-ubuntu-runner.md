# 06 — CI: unit tests killed at the 15 s nextest limit on the ubuntu runner — a quint download race plus a few heavy tests

Status: needs-triage
Type: AFK
Size: S
Origin: Test run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779); re-read against run 33941010778 (b6cd9276a, after 04 landed) on 2026-09-04

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

`Run unit tests` (`cargo nextest run --all`, 9049 tests) ends with TIMEOUTs and FAILs that are
not product defects. Run 33941010778 (first run with quint installing, per 04): `10 failed,
15 timed out`. Two distinct causes:

**A. `frogdb-replication::quint_conformance` — a download race, not heaviness.** The first
`quint run` on a machine fetches quint's Rust evaluator into `~/.quint/rust-evaluator-v0.6.0/`
("Fetching Rust evaluator v0.6.0... Downloading ... from api.github.com/.../releases/assets").
nextest starts the binary's tests in parallel, so every test's own `quint run` races that
fetch: three fail outright with

```
Error: EEXIST: file already exists, open '/home/runner/.quint/rust-evaluator-v0.6.0/quint_evaluator-x86_64-unknown-linux-gnu.tar.gz'
```

(`ack_above_live_is_ignored`, `barrier_floor_is_the_live_offset_at_arm_time`,
`close_inside_window_drains_then_ends_graceful`, each ~13.5–14 s) and the other seven block
behind the download until the `3 × 5 s` hard kill (`sampled_traces`,
`mid_send_session_is_not_classified_out_from_under_its_frame`, `lag_breach_ends_at_the_frame`,
`handoff_dedup`, `channel_overrun_refuses_intake`, `barrier_while_held`,
`ack_watermark_never_retreats`). Locally the cache is warm (`~/.quint/rust-evaluator-v0.6.0`
exists), which is why the binary is green on a dev machine. The `quint` job's `just quint-run`
warms the cache on *its* runner, not the unit-tests runner. Raising the timeout would not fix
the EEXIST failures. Fix direction: warm the evaluator once, serially, in the `unit-tests` job
before `cargo nextest run` (a step running one tiny `quint run` against
`specs/quint/replication_feed_gate.qnt`, or caching `~/.quint` on the sticky disk), in
`workflow_gen/workflows/test.py`.

**B. Genuinely heavy tests at the default `3 × 5 s`** (`.config/nextest.toml:7`):

| test | note |
|---|---|
| `frogdb-redis-regression::main hash_tcl::tcl_hash_fuzzing_1_512_fields`, `..._2_512_fields` | fuzz ports, socket round-trip heavy |
| `frogdb-redis-regression::main scan_tcl::tcl_scan_guarantees_under_write_load` | also timed out on the Sept 3 `main` run |
| `frogdb-redis-regression::main bloom_regression::bf_false_positive_rate` | new in run 33941010778 |
| `frogdb-telemetry::metrics_usage every_metric_is_emitted_through_its_typed_handle` | also timed out on the Sept 3 `main` run |
| `frogdb-core store::hashmap::tests::scan_stress::scan_present_throughout_is_subset_of_returned` | |

**C. Unexplained — needs its own look before any override:**
`frogdb-server::main integration_replication::test_broadcast_lag_disconnect_and_resync`
(`case_1_in_memory` and `case_2_with_persistence`) hit the 45 s kill in run 33941010778 with
the `integration_replication::` override already at `15s × 3` and no output beyond
"(test timed out)". Not in run 33937564779. A test that needs >45 s on the runner after already
having 3× headroom is more likely waiting on something that never arrives (lag disconnect →
resync) than merely slow; do not paper over it with a longer timeout without a local repro.

Pre-existing on `main`: the Sept 3 run (76b2a6dae) timed out on 2 of the B rows. Not caused by
anything on `build-toolchain/impl` (no Rust changes vs its base at the time of either run).

The remaining FAILs in run 33941010778 are tracked elsewhere: `cluster_handoff_barrier` × 2 and
`cluster_migration::test_blocking_command_during_migration_gets_moved` (TRYAGAIN, "source did
not drain in 50ms") → memory-architecture/26; `frogdb-config` golden counts × 2 →
build-toolchain/07; jemalloc × 2 → memory-architecture/27.
`cluster_slots::test_cluster_redirect_outranks_the_link_down_stale_gate` failed twice and passed
on retry (counted flaky, not failed).

## Options (decision needed)

For A (the quint race):
1. **Recommended:** a `Warm quint evaluator` step in the `unit-tests` job between the mise step
   and `cargo nextest run`, running one serial `quint run` (e.g. `--max-steps 0 --max-samples 1`
   against `specs/quint/replication_feed_gate.qnt`). Same generator, no test change, no cache key.
2. Cache `~/.quint` on the sticky disk keyed on the quint version — saves the download but
   the first run after a bump still races.

For B (the heavy tests):
1. **Recommended:** nextest overrides in the existing style (`.config/nextest.toml` already has a
   dozen "legitimately heavy, not flaky" entries): the six named tests at `30s × 3`. Cheapest;
   matches how `tcl_sdiff_fuzzing`, HLL and `scan_full_iteration_survives_resizes_mid_scan` were
   handled.
2. Reduce the work in the tests (fewer fuzz iterations / samples) — changes what the tests prove.
3. Lower `test-threads` on CI only — slows the whole job for everyone.

For C: reproduce locally under load (`cargo nextest run -p frogdb-server -E
'test(test_broadcast_lag_disconnect_and_resync)' --test-threads 1` vs the default), read the
test's waits, and file the finding — a separate issue if it is a hang.

## Acceptance criteria (for A1 + B1)

- [ ] the `unit-tests` job warms the quint evaluator once before `cargo nextest run`; the step
      is generated (`just workflow-gen --check` green) with a comment naming this issue
- [ ] overrides added with a comment naming the run and the observed durations, same shape as the neighbours
- [ ] `cargo nextest run -p frogdb-replication --test quint_conformance` green locally with a
      cold `~/.quint` (move it aside first)
- [ ] a `workflow_dispatch` run of `test.yml` on the integration branch shows 0 timed out in the
      A and B rows; C is reported, not necessarily fixed

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`
- `.github/workflows/test.yml` (regenerated)
- `.config/nextest.toml`

## See also

`test-unit-tests-testbox.yml` (hand-maintained) still lists `install_args: just
cargo:cargo-nextest` — no `node`/quint — so an on-box `cargo nextest run --all` hits the
`quint_conformance` gap 04 fixed in `test.yml`. The workflow itself only prebuilds
(`--no-run`), so it is not red; fix alongside this issue or when the testbox flow next changes.

## Blocked by

None — 04 landed 2026-09-04; the A rows above are read from the first post-04 run.

## Decisions

Pending: A1/A2, B1/B2/B3, whether C becomes its own issue.
