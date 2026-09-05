# 10 — CI: nextest `30s × 3` overrides for the six heavy tests killed at the default `3 × 5 s`

Status: done
Type: AFK
Size: S
Origin: carved from issue 06 part B (CI runs 33941010778, 33942554391; main run 33936207419)

## Parent

`.scratch/build-toolchain/PRD.md`

## What to build

`.config/nextest.toml` gets `[[profile.default.overrides]]` entries giving these six tests
`slow-timeout = { period = "30s", terminate-after = 3 }`, in the same shape as the existing
"legitimately heavy, not flaky" neighbours (`tcl_sdiff_fuzzing`, the HLL pair,
`scan_full_iteration_survives_resizes_mid_scan`): a comment above each entry saying what the test
does that makes it heavy, that it passes run alone, and which CI run killed it.

| test (as nextest prints it) | filter to use |
|---|---|
| `frogdb-redis-regression::main hash_tcl::tcl_hash_fuzzing_1_512_fields` and `..._2_512_fields` | `test(hash_tcl::tcl_hash_fuzzing_1_512_fields) \| test(hash_tcl::tcl_hash_fuzzing_2_512_fields)` |
| `frogdb-redis-regression::main scan_tcl::tcl_scan_guarantees_under_write_load` | `test(scan_tcl::tcl_scan_guarantees_under_write_load)` |
| `frogdb-redis-regression::main bloom_regression::bf_false_positive_rate` | `test(bloom_regression::bf_false_positive_rate)` |
| `frogdb-telemetry::metrics_usage every_metric_is_emitted_through_its_typed_handle` | `package(frogdb-telemetry) & test(every_metric_is_emitted_through_its_typed_handle)` |
| `frogdb-core store::hashmap::tests::scan_stress::scan_present_throughout_is_subset_of_returned` | `package(frogdb-core) & test(scan_stress::scan_present_throughout_is_subset_of_returned)` |

Group entries however reads best (one entry per crate is fine; one per test is fine); the filters
above are the required matching, and a filter must match exactly the named test(s), nothing more.
Place the entries next to the other single-test heavy overrides at the end of the file, after
`tcl_sdiff_fuzzing`. First match wins in nextest, so check none of the six is already caught by an
earlier override (none should be — the six all ran at the default `3 × 5 s` in the CI logs).

Comments name the CI runs (33941010778 and 33942554391 on `build-toolchain/impl`; `main` run
33936207419 also killed `scan_tcl::tcl_scan_guarantees_under_write_load` and the telemetry test)
and build-toolchain issue 06 / 10. Where you can measure it, record the local solo duration in the
comment (as the sdiff and scan_full_iteration comments do).

Nothing else changes: no test code, no `test-threads`, no global `slow-timeout`, no CI workflow
edits.

## Acceptance criteria

- [ ] `.config/nextest.toml` has the overrides, `30s × 3`, comments in the neighbours' shape naming
      the runs and issue 06/10
- [ ] `cargo nextest list -E '<combined filter>'` (the six filters OR'd) lists exactly the six
      tests — paste the output in the report
- [ ] `cargo nextest show-config test-groups` still parses (any config typo fails here), and the
      full gate is green
- [ ] the six tests pass locally under the new config: `cargo nextest run -E '<combined filter>'`
      (or the equivalent `just test` invocations — check the `Justfile` `test` recipe first), with
      the per-test durations from that run in the report

Controller verification after landing (not the implementer's): a `workflow_dispatch` run of
`test.yml` on `build-toolchain/impl` shows 0 timed out in these six rows.

## Files likely touched

- `.config/nextest.toml`

## Blocked by

None.

## Decisions

D7

## Resolution

Landed on `build-toolchain/impl` at merge `109736bb9` (2026-09-05). One commit, `b92304470`: five
`[[profile.default.overrides]]` entries at `30s × 3` covering the six tests, appended after
`tcl_sdiff_fuzzing` with a shared intro comment naming runs 33941010778 / 33942554391 /
33936207419 and issue 06/10, plus a per-entry heaviness rationale checked against each test body.
`cargo nextest list -E` matched exactly the six; solo durations 6.8–11.9 s; no earlier override
shadows them. Reviewer (sonnet) approved, 1 Minor (comment consolidation) accepted. Closes issue 06
part B; the `workflow_dispatch` run of `test.yml` that follows this landing is the D8 probe for 06 C.
