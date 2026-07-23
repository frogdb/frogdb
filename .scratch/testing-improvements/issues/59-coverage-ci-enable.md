# Enable nightly, non-gating code coverage tracking in CI

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Jepsen / Distributed testing infra / CI

## Context

`test.py:28` sets `COVERAGE_ENABLED = False`, which gates the coverage job off entirely
(`test.py:189`). No CI coverage signal exists today — coverage regressions or newly-introduced
coverage-blind areas are invisible until an ad hoc audit run like this one. Verdict CONFIRMED L1/C1.

This testing-improvements audit itself produced a coverage baseline that should be preserved as the
starting reference point rather than discarded (see `.scratch/testing-improvements/audit/coverage-summary.md`):
`cargo llvm-cov nextest --all` on the aarch64 Blacksmith testbox, 2026-07-22 — 6824 tests total, 6823
pass, 1 flaky (`integration_cluster::test_frogdb_version_reports_cluster_info`, tracked separately as
task 60). Total line coverage 84.0% (105531/125629). Per-crate range from `frogdb-macros` at 0.0%
(217 lines) up to `acl` at 94.5%. Worst server-relevant files under 65% coverage include
`server/src/commands/info.rs` (0.8%, 3/397 — possibly legacy/dead vs. `server/src/info/sections.rs`),
`server/src/connection/builder.rs` (0.0%, 0/175), `server/src/config/loader.rs` (29.8%, 98/329), and
`core/src/store/mod.rs` (34.7%, 111/320).

## What to build

Enable `COVERAGE_ENABLED` for a scheduled nightly (non-PR-gating) CI job that runs `cargo llvm-cov`
and publishes the report as an artifact, with this audit's numbers wired in as the documented
starting baseline so future drift is visible.

## Acceptance criteria

- [ ] `test.py` `COVERAGE_ENABLED` flipped to `True` for a scheduled nightly workflow only — not
      per-PR gating.
- [ ] Nightly job publishes the coverage report as a CI artifact (and/or posts a summary comment or
      dashboard entry), non-blocking to merges.
- [ ] This audit's baseline (84.0% total; per-crate table) recorded as the documented starting
      reference point in the job config or accompanying docs, so future runs show drift, not just an
      absolute number.
- [ ] Worst-file list (`server/src/commands/info.rs`, `connection/builder.rs`, `config/loader.rs`,
      `core/src/store/mod.rs`, etc.) noted as follow-up candidates for future coverage work — not
      required to be fixed as part of this task.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/G-jepsen-harness.md (`coverage-tracking-disabled`)
- .scratch/testing-improvements/audit/verdicts-G.md (CONFIRMED L1/C1)
- test.py:28,189
- .scratch/testing-improvements/audit/coverage-summary.md (baseline: 6824 tests, 84.0% total line coverage)
