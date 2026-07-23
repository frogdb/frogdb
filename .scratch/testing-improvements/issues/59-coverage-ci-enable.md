# Enable nightly, non-gating code coverage tracking in CI

Status: done
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

- [x] `test.py` `COVERAGE_ENABLED` flipped to `True` for a scheduled nightly workflow only — not
      per-PR gating. (Superseded by a cleaner equivalent — see Resolution: the flag and its dead
      `test.yml`-embedded job were removed outright in favor of a dedicated nightly workflow file,
      the same pattern already used for the other three nightly tiers.)
- [x] Nightly job publishes the coverage report as a CI artifact (and/or posts a summary comment or
      dashboard entry), non-blocking to merges.
- [x] This audit's baseline (84.0% total; per-crate table) recorded as the documented starting
      reference point in the job config or accompanying docs, so future runs show drift, not just an
      absolute number.
- [x] Worst-file list (`server/src/commands/info.rs`, `connection/builder.rs`, `config/loader.rs`,
      `core/src/store/mod.rs`, etc.) noted as follow-up candidates for future coverage work — not
      required to be fixed as part of this task.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/G-jepsen-harness.md (`coverage-tracking-disabled`)
- .scratch/testing-improvements/audit/verdicts-G.md (CONFIRMED L1/C1)
- test.py:28,189
- .scratch/testing-improvements/audit/coverage-summary.md (baseline: 6824 tests, 84.0% total line coverage)

## Resolution

Coverage tracking is now a scheduled, non-PR-gating nightly CI job.

### What changed

Rather than literally flipping the dead `COVERAGE_ENABLED` flag in place (which only gated a job
embedded in `test.yml`, triggered on every push/PR — the wrong trigger shape for "nightly,
non-gating"), the job was moved to its own generated workflow, matching the repo's existing pattern
for the other three long-running nightly tiers (`concurrency_nightly.py` / `jepsen_nightly.py` /
`fuzz.py` — each a dedicated cron-triggered file, never embedded in `test.yml`):

- **New DSL module**: `.github/workflows/workflow_gen/src/workflow_gen/workflows/coverage_nightly.py`
  → generates `.github/workflows/coverage-nightly.yml`. Cron `50 4 * * *` (distinct from the other
  three nightly crons: jepsen `37 5`/`37 6`, concurrency `14 3`, fuzz `41 2`) plus
  `workflow_dispatch` for manual runs.
- **`test.py`**: removed the `COVERAGE_ENABLED` flag and the dead `if COVERAGE_ENABLED:` job block
  entirely (and its now-unused `CODECOV`/`INSTALL_ACTION` imports) — coverage no longer has any
  footprint in the per-PR/push `Test` workflow, so it can never gate a merge or show up as a
  required/optional check on a PR.
- **`render.py`**: registered `coverage_nightly_workflow` → `coverage-nightly.yml` in the `WORKFLOWS`
  dict that `workflow-gen`/`workflow-gen --check` iterate.
- **`constants.py`**: updated the `INSTALL_ACTION`/`CODECOV` comments to reflect the new home (and
  that Codecov upload isn't wired in yet — see Caveats).

### Job design

Single `coverage` job on `blacksmith-4vcpu-ubuntu-2404` (same runner class as the other three
nightlies), `timeout-minutes: 180`:

1. mise (`just`, `cargo-nextest`) + pinned Rust with `llvm-tools-preview` + libclang + install
   `cargo-llvm-cov` (`taiki-e/install-action`) + `Swatinem/rust-cache` (`shared-key: coverage`).
2. `just coverage-lcov` — the same recipe a developer runs locally
   (`cargo llvm-cov nextest --all --lcov --output-path target/llvm-cov/lcov.info`).
3. **Coverage summary** step (`if: always()`): sums the `LH:`/`LF:` totals out of every
   `end_of_record` section in the lcov file with `awk`, computes the percentage, and writes it to
   `$GITHUB_STEP_SUMMARY` alongside this issue's 84.0% baseline (dated 2026-07-22, sourced from
   `.scratch/testing-improvements/audit/coverage-summary.md`) and a pointer to that file's per-crate
   table and worst-file list — so a future run shows drift at a glance, not just an absolute number,
   satisfying the "documented starting reference point" criterion without needing to reproduce the
   full per-crate breakdown live in CI (`cargo llvm-cov`'s own stdout table, captured in the raw job
   log, remains the authoritative per-file view for deeper digging).
4. **Upload artifact** step (`if: always()`, `if-no-files-found: error`): publishes
   `target/llvm-cov/lcov.info` as the `coverage-lcov` CI artifact (7-day retention, matching the
   repo's other artifact steps).

Non-gating by construction: the job lives in its own workflow file, is never a dependency of
`test.yml`'s `ci-pass` job, and only runs on its own schedule/dispatch — a red or slow run cannot
block a PR and generates no page/alert (GitHub only surfaces a failed *scheduled* workflow run in
the Actions tab, not as a PR status).

### Baseline (recorded here as the reference point, per acceptance criteria)

From `.scratch/testing-improvements/audit/coverage-summary.md` (`cargo llvm-cov nextest --all`,
aarch64 Blacksmith testbox, 2026-07-22): 6824 tests, 6823 pass, 1 flaky
(`integration_cluster::test_frogdb_version_reports_cluster_info`, tracked as issue 60). **Total line
coverage 84.0% (105531/125629)**.

Per-crate: `frogdb-macros` 0.0% (217L) · `frogdb-server` bin 5.7% · `frogctl` 46.6% · `debug` 56.8% ·
`tokio-coz` 73.6% · `config-derive` 73.9% · `server` 82.5% · `scripting` 82.9% · `commands` 84.7% ·
`protocol` 85.2% · `search` 85.9% · `core` 86.6% · `cluster` 88.3% · `vll` 88.9% · `telemetry` 89.7% ·
`persistence` 90.4% · `config` 91.5% · `types` 91.7% · `replication` 92.4% · `testing` 92.7% ·
`acl` 94.5%.

Worst server-relevant files (≥100 lines, <65%) — follow-up candidates, not fixed here:
`server/src/commands/info.rs` (0.8%, 3/397 — possibly legacy/dead vs. `server/src/info/sections.rs`),
`server/src/connection/builder.rs` (0.0%, 0/175), `server/src/config/loader.rs` (29.8%, 98/329),
`core/src/store/mod.rs` (34.7%, 111/320), `server/src/admin/handlers.rs` (36.9%, 83/225),
`debug/src/web_ui/handlers.rs` (37.5%, 414/1104), `telemetry/src/otlp.rs` (38.2%, 63/165),
`server/src/connection/persistence_handler.rs` (40.9%, 83/203),
`server/src/connection/routing.rs` (52.1%, 98/188), `server/src/commands/search.rs` (53.4%,
260/487), `server/src/connection/scripting/eval.rs` (55.6%, 109/196),
`commands/src/hyperloglog.rs` (56.2%, 100/178), `config/src/cluster.rs` (56.9%, 58/102),
`commands/src/vectorset/vsim.rs` (60.5%, 121/200),
`server/src/connection/search/hybrid.rs` (61.0%, 61/100), `core/src/conn_command.rs` (61.4%,
162/264), `core/src/shard/search/query.rs` (62.1%, 435/701),
`server/src/commands/cluster/admin.rs` (64.1%, 329/513).

### Validation

- `just workflow-gen` — regenerated all 10 workflow files; `coverage-nightly.yml` created,
  `test.yml` diff is the coverage-job removal only.
- `just workflow-gen --check` — clean (all 10 files, including the new one, match generation).
- `actionlint` on `coverage-nightly.yml` and `test.yml` — clean, no findings.
- `just lint-py` (`ruff check`) and `just fmt-py-check` (`ruff format --check`) — clean.
- Confirmed `just coverage-lcov` exists in `Justfile` and its invocation
  (`cargo llvm-cov nextest --all --lcov --output-path target/llvm-cov/lcov.info`) matches what the
  new job runs verbatim via `just coverage-lcov` (not reimplemented inline).
- Did **not** run the full coverage suite locally — it's heavy (whole-workspace instrumented test
  run) and this audit already proved the recipe works end-to-end on aarch64 (source of the 84.0%
  baseline above). `cargo-llvm-cov` is not installed in this dev environment
  (`cargo llvm-cov --help` → "no such command"), consistent with it being a CI-only tool installed
  via `taiki-e/install-action`.

### Caveats / follow-ups

- No Codecov upload/dashboard step. `CODECOV_TOKEN` isn't a configured repo secret today (the old
  disabled job referenced it but was never exercised), and the acceptance criteria's "artifact
  and/or summary/dashboard" is satisfied by the artifact + job-summary combination alone. Wiring
  Codecov later is a small addition (`Step(uses=CODECOV, ...)`, constant already defined) once a
  token exists — not blocking this task.
- The job-summary percentage is a total-line-coverage number computed by summing `LH:`/`LF:` across
  the lcov file; it does not reproduce the audit's full per-crate breakdown live (that table was
  hand-curated from `cargo llvm-cov`'s own richer stdout output). The per-crate table stays the
  static, dated baseline recorded here and in `audit/coverage-summary.md`; per-file drill-down for
  a given run is available from the raw job log or by downloading the `coverage-lcov` artifact.
- Worst-file list is carried forward as documentation only, per the acceptance criteria — no fixes
  attempted in this task.
