# INFO errorstats fully implemented but never driven end-to-end; info_tcl.rs doc is stale

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: basic commands / introspection (area A)

## Context

`INFO errorstats`/`rejected_calls`/`failed_calls`/`total_error_replies` is fully implemented:
counters and the 128-entry cap live in `core/src/client_registry/mod.rs:37-88`, the
`errorstat_<PREFIX>` prefix formatting at `:96-99`, dispatch wiring via `record_error_response`
in `dispatch.rs:614-622` and `connection.rs:375-379`, and rendering in `info/sections.rs:532-542`.
But coverage is unit-only with fabricated counts (`sections.rs:1006-1012`,
`client_registry/mod.rs:1449,1472+`) — no test drives a *real* error through the server and
asserts the resulting `errorstat_<PREFIX>:count=N` / rejected-vs-failed split via `INFO`.
`integration_info.rs:82-83` only asserts section headers are present, not their contents.

`redis-regression/tests/info_tcl.rs:42-56` is stale: it claims the errorstats feature is
unimplemented and excludes all 10 corresponding upstream errorstats tests — those tests should be
re-enabled/re-ported now that the feature is real.

The adversarial pass found a nuance worth preserving in the acceptance criteria:
`introspection2_tcl.rs:630-655` (`tcl_errors_stats_for_geoadd`) *does* already assert
per-command `failed_calls=1` end-to-end for one command — so this is not a complete blank slate.
What remains genuinely unasserted is the `errorstat_<PREFIX>` section itself, the
rejected-vs-failed split (arity/unknown-command errors should count as `rejected_calls`, type
errors like `WRONGTYPE` should count as `failed_calls`), and `total_error_replies`.

Verdict (adversarial pass): CONFIRMED L2/C1, with the `introspection2_tcl.rs:630` partial-coverage
nuance noted above.

## What to build

Integration tests driving real errors through the server (e.g. `WRONGTYPE`, unknown command,
wrong arity) and asserting the resulting `INFO errorstats` section content — specific
`errorstat_<PREFIX>:count` deltas, the rejected-vs-failed split, and `total_error_replies`. Fix
the stale `info_tcl.rs:42-56` doc/exclusion and re-port the excluded upstream errorstats tests
where they're now reachable.

## Acceptance criteria

- [x] Integration test: trigger a `WRONGTYPE` error, assert `errorstat_WRONGTYPE:count=1` and
      `failed_calls` incremented accordingly via `INFO`.
- [x] Integration test: trigger an arity/unknown-command error, assert it counts toward
      `rejected_calls` (not `failed_calls`), pinning the rejected-vs-failed contract explicitly.
      (The arity case does; empirically, the unknown-command case does not — see Resolution.)
- [x] Integration test: assert `total_error_replies` reflects the sum across error types.
- [x] `redis-regression/tests/info_tcl.rs:42-56` stale doc/exclusion comment corrected; the 10
      excluded upstream errorstats tests re-evaluated and re-ported where applicable.
- [x] Existing `introspection2_tcl.rs:630-655` (`tcl_errors_stats_for_geoadd`) left intact and
      referenced as prior art, not duplicated.

## Blocked by

None - can start immediately

## References

- `core/src/client_registry/mod.rs:37-88,96-99,1449,1472`
- `server/src/dispatch.rs:614-622`
- `server/src/connection.rs:375-379`
- `crates/commands/src/info/sections.rs:532-542,1006-1012`
- `server/tests/integration_info.rs:82-83`
- `redis-regression/tests/info_tcl.rs:42-56`
- `redis-regression/tests/introspection2_tcl.rs:630-655`
- `.scratch/testing-improvements/audit/A-basic-commands.md` (`errorstats-info-untested-end-to-end`, A#1)
- `.scratch/testing-improvements/audit/verdicts-A.md`

## Resolution

Added real, end-to-end integration coverage for `INFO errorstats`/`commandstats`, replacing the
stale `info_tcl.rs:42-56` doc/exclusion comment, and filed a follow-up
(`.scratch/testing-improvements/issues/63`) for the
dispatch-stage coverage gaps and a cardinality-growth vector found while writing these tests.

**`frogdb-server/crates/redis-regression/tests/info_tcl.rs`**: rewrote the stale
`errorstats`/`commandstats` doc section and re-ported all 10 excluded upstream `errorstats: *`
scenarios as 10 new integration tests driving real errors over RESP and asserting `INFO
errorstats`/`commandstats` content (not just section presence):

- `errorstats_wrongtype_is_failed_call`, `errorstats_wrong_arity_is_rejected_call`,
  `errorstats_total_error_replies_sums_across_types` — the three core acceptance-criteria pins
  (WRONGTYPE → failed, arity → rejected, `total_error_replies` sums across types).
- `errorstats_nogroup_is_failed_call`, `errorstats_nopermission_is_rejected_call` — re-ported
  scenarios confirmed to match Redis's classification.
- `errorstats_unknown_command_counts_as_err_but_not_rejected`,
  `errorstats_oom_is_failed_not_rejected_call_divergence` — re-ported scenarios that **diverge**
  from Redis (both land as `failed_calls` where Redis would reject pre-dispatch); pinned as
  documented divergences, not silently made to pass.
- `errorstats_auth_failure_is_untracked_gap`, `errorstats_multi_exec_errors_are_untracked_gap`,
  `errorstats_evalsha_noscript_is_untracked_gap` — pin a genuine observability gap: AUTH,
  MULTI/EXEC, and EVALSHA/NOSCRIPT errors reach the client correctly but are never recorded in
  errorstats/commandstats at all (root-caused to `record_error_response` only being wired into 3
  of the `DispatchStage` gauntlet's stages — see issue 63).

The pre-existing `introspection2_tcl.rs:630-655` (`tcl_errors_stats_for_geoadd`) was left untouched
and is referenced in the new doc comment as prior art, not duplicated.

**`frogdb-server/crates/server/tests/integration_info.rs`**: added 3 plain e2e integration tests
(`info_errorstats_wrongtype_increments_failed_calls`,
`info_errorstats_arity_error_increments_rejected_calls`,
`info_errorstats_total_error_replies_sums_across_types`) asserting the same core contract against
a combined `INFO all` render, per this crate's existing integration-test style.

Nuance on acceptance criterion 2 ("arity/unknown-command error ... counts toward `rejected_calls`
not `failed_calls`"): the arity case does, confirmed by
`errorstats_wrong_arity_is_rejected_call`/`info_errorstats_arity_error_increments_rejected_calls`.
The unknown-command case, once actually driven end-to-end, turned out **not** to match that
assumption — FrogDB records it as `failed_calls`, a real divergence from Redis rather than a gap in
test coverage (see `errorstats_unknown_command_counts_as_err_but_not_rejected` and issue 63). The
criterion is checked off on the basis that the behavior is now correctly asserted and documented,
not that it matches the original assumption.

Test evidence (Blacksmith testbox, aarch64 Linux, 2026-07-23, clean rebuild from a wiped `target/`):

```
cargo nextest run -p frogdb-redis-regression -E 'test(/errorstats/)'
 Summary [0.144s] 10 tests run: 10 passed, 2275 skipped
   PASS info_tcl::errorstats_nopermission_is_rejected_call
   PASS info_tcl::errorstats_nogroup_is_failed_call
   PASS info_tcl::errorstats_evalsha_noscript_is_untracked_gap
   PASS info_tcl::errorstats_multi_exec_errors_are_untracked_gap
   PASS info_tcl::errorstats_unknown_command_counts_as_err_but_not_rejected
   PASS info_tcl::errorstats_total_error_replies_sums_across_types
   PASS info_tcl::errorstats_auth_failure_is_untracked_gap
   PASS info_tcl::errorstats_oom_is_failed_not_rejected_call_divergence
   PASS info_tcl::errorstats_wrong_arity_is_rejected_call
   PASS info_tcl::errorstats_wrongtype_is_failed_call

cargo nextest run -p frogdb-server -E 'test(/errorstats/)'
 Summary [0.067s] 4 tests run: 4 passed, 1810 skipped
   PASS info::sections::tests::errorstats_renders_prefix_counts   (pre-existing unit test)
   PASS integration_info::info_errorstats_arity_error_increments_rejected_calls
   PASS integration_info::info_errorstats_wrongtype_increments_failed_calls
   PASS integration_info::info_errorstats_total_error_replies_sums_across_types
```

Clippy (`just lint frogdb-redis-regression`, `just lint frogdb-server`, full `-D warnings` +
seam-gate checks) is clean on both crates after these changes.
