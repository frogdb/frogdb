# INFO errorstats fully implemented but never driven end-to-end; info_tcl.rs doc is stale

Status: needs-triage
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

- [ ] Integration test: trigger a `WRONGTYPE` error, assert `errorstat_WRONGTYPE:count=1` and
      `failed_calls` incremented accordingly via `INFO`.
- [ ] Integration test: trigger an arity/unknown-command error, assert it counts toward
      `rejected_calls` (not `failed_calls`), pinning the rejected-vs-failed contract explicitly.
- [ ] Integration test: assert `total_error_replies` reflects the sum across error types.
- [ ] `redis-regression/tests/info_tcl.rs:42-56` stale doc/exclusion comment corrected; the 10
      excluded upstream errorstats tests re-evaluated and re-ported where applicable.
- [ ] Existing `introspection2_tcl.rs:630-655` (`tcl_errors_stats_for_geoadd`) left intact and
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
