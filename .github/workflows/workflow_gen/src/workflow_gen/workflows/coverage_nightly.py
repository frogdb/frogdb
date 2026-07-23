"""Nightly code-coverage tracking workflow definition.

Before this workflow there was *no* CI coverage signal at all: `test.py` defined a
`coverage` job (`cargo llvm-cov` + Codecov upload) but gated it behind a
`COVERAGE_ENABLED = False` module flag, so it never ran — a coverage regression or a
newly introduced coverage-blind file was only ever visible via an ad hoc audit run
(issue 59, `.scratch/testing-improvements/issues/`, CONFIRMED L1/C1 in
`audit/verdicts-G.md`). That audit itself produced the first real baseline
(`cargo llvm-cov nextest --all` on the aarch64 Blacksmith testbox, 2026-07-22): 6824
tests (6823 pass, 1 known-flaky — tracked separately as issue 60), **84.0% total line
coverage (105531/125629)**. Per-crate range from `frogdb-macros` at 0.0% up to `acl` at
94.5%; see `.scratch/testing-improvements/audit/coverage-summary.md` for the full
per-crate table and the worst-file list (follow-up candidates for future coverage
work, not fixed here) — `server/src/commands/info.rs` (0.8%),
`server/src/connection/builder.rs` (0.0%), `server/src/config/loader.rs` (29.8%),
`core/src/store/mod.rs` (34.7%), among others.

Design, mirroring the repo's other single-job nightly tiers (`concurrency_nightly.py`
/ `jepsen_nightly.py` / `fuzz.py`): a dedicated cron-triggered (+ `workflow_dispatch`)
workflow rather than reviving the `test.yml`-embedded job, so a red/slow coverage run
can never gate a PR merge (it isn't a dependency of `test.yml`'s `ci-pass`, and it
lives in an entirely separate workflow file) and never pages anyone. `just
coverage-lcov` (the same recipe a developer runs locally) generates the lcov report;
the job then sums the `LF`/`LH` totals straight out of the lcov file for a total
line-coverage percentage, prints it against this doc's 84.0% baseline in the job
summary (so future drift is visible at a glance without opening the artifact), and
uploads the lcov file itself as a CI artifact for deeper per-file inspection.

Runner: same `blacksmith-4vcpu-ubuntu-2404` (x86) class as the other three nightly
workflows. The audit ran coverage on aarch64 only because that happened to be the
testbox on hand that day; unlike Jepsen/fuzz/concurrency (where architecture affects
which races and timing bugs reproduce), coverage is a build-instrumentation/line-count
signal that doesn't depend on runner architecture, so it stays consistent with the
other nightly jobs rather than requiring its own arm runner class.
"""

from workflow_gen.constants import INSTALL_ACTION
from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    libclang_step,
    mise_setup_step,
    omap,
    run_step,
    rust_toolchain_step,
    script,
    self_hosted_env_step,
    upload_artifact_step,
)
from workflow_gen.schema import Job, ScheduleTrigger, Step, Trigger, Workflow

MISE_TOOLS = "just cargo:cargo-nextest"

# Same runner class as the other long-running single-job nightlies (fuzz/concurrency/
# jepsen) — see module docstring for why aarch64 isn't required here.
RUNS_ON = "blacksmith-4vcpu-ubuntu-2404"

# Off-the-hour, and distinct from the other nightly crons (jepsen 37 5 / 37 6,
# concurrency 14 3, fuzz 41 2) so the campaigns don't contend for Blacksmith minutes.
NIGHTLY_CRON = "50 4 * * *"

# This audit's baseline (2026-07-22, aarch64 Blacksmith testbox) — the documented
# starting reference point so the job summary shows drift, not just an absolute
# number. Keep in sync with `.scratch/testing-improvements/audit/coverage-summary.md`
# if that baseline is ever formally superseded (e.g. a future audit re-measures it).
BASELINE_TOTAL_PCT = "84.0"
BASELINE_DATE = "2026-07-22"

LCOV_PATH = "target/llvm-cov/lcov.info"


def _coverage_summary_step() -> Step:
    """Sum LF/LH across every lcov `end_of_record` section and write the job summary.

    `cargo llvm-cov` already produces the authoritative per-file/per-crate breakdown
    in its own stdout table (captured in the raw job log for anyone who needs it);
    this step only extracts the single total-line-coverage number the acceptance
    criteria asks be surfaced in the summary, alongside this workflow's documented
    baseline so drift is visible without downloading the artifact.
    """
    return Step(
        name="Coverage summary",
        if_="always()",
        run=script(f"""\
            set -uo pipefail
            if [ ! -f "{LCOV_PATH}" ]; then
              echo "### Coverage summary" >> "$GITHUB_STEP_SUMMARY"
              echo "lcov report not found at {LCOV_PATH} - coverage generation failed." \\
                >> "$GITHUB_STEP_SUMMARY"
              exit 0
            fi
            read -r lh lf <<< "$(awk -F: '
              /^LH:/ {{ lh += $2 }}
              /^LF:/ {{ lf += $2 }}
              END {{ print lh, lf }}
            ' {LCOV_PATH})"
            pct=$(awk -v lh="$lh" -v lf="$lf" 'BEGIN {{ printf "%.1f", (lf > 0) ? (lh / lf * 100) : 0 }}')
            cat >> "$GITHUB_STEP_SUMMARY" <<SUMMARY
            ### Coverage summary

            Total line coverage: **${{pct}}%** (${{lh}}/${{lf}} lines)

            Baseline ({BASELINE_DATE} audit, .scratch/testing-improvements/audit/coverage-summary.md): **{BASELINE_TOTAL_PCT}%**

            Per-crate baseline table and worst-file follow-up candidates (server/src/commands/info.rs,
            connection/builder.rs, config/loader.rs, core/src/store/mod.rs, etc.) are tracked in that
            same file - not required to be fixed as part of this job.
            SUMMARY
            """),
    )


def coverage_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Coverage Nightly",
        on=Trigger(schedule=ScheduleTrigger(cron=[NIGHTLY_CRON])),
    )

    w.job(
        "coverage",
        Job(
            name="Nightly Code Coverage",
            runs_on=RUNS_ON,
            # Instrumented builds + test runs cost noticeably more than a plain
            # `cargo nextest run --all` (per-line counters on every codegen unit);
            # generous margin over the plain unit-tests job for CI variance.
            timeout_minutes=180,
            steps=[
                checkout_step(),
                self_hosted_env_step(),
                mise_setup_step(install_args=MISE_TOOLS),
                rust_toolchain_step(components="llvm-tools-preview"),
                libclang_step(),
                Step(
                    name="Install cargo-llvm-cov",
                    uses=INSTALL_ACTION,
                    with_=omap(tool="cargo-llvm-cov"),
                ),
                cargo_cache_step(shared_key="coverage"),
                run_step(name="Generate coverage report", run="just coverage-lcov"),
                _coverage_summary_step(),
                upload_artifact_step(
                    name="coverage-lcov",
                    path=LCOV_PATH,
                    if_="always()",
                    # A missing lcov file means coverage generation itself failed
                    # (not merely "no lines instrumented") — fail loudly rather than
                    # the default "warn" (green, empty artifact) so that failure mode
                    # doesn't hide silently in a job nobody watches.
                    if_no_files_found="error",
                ),
            ],
        ),
    )

    return w
