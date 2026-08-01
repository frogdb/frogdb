"""Nightly frozen Redis-compat regression workflow definition.

The 63K-LOC `frogdb-redis-regression` compat suite (`frogdb-server/crates/redis-regression`)
is frozen for the duration of the foundation-hardening campaign (see
`docs/agents/hardening-campaign.md`): it no longer builds as part of `just check`/`just
test` (gated behind the `regression` Cargo feature via `required-features`), so a compat
regression during the campaign is invisible to the normal PR-gating `test.yml` run. This
workflow is the nightly signal that fills that gap, mirroring the repo's other on-demand
tiers (`concurrency_nightly.py` / `jepsen_nightly.py` / `coverage_nightly.py`):

* `regression-check`: compiles the frozen suite (`just regression-check`, plain `cargo
  check --features regression --all-targets`) without running it — the cheap anti-rot
  guard that catches the suite silently bit-rotting against API changes elsewhere in the
  workspace, without paying the cost of actually running 63K lines of compat tests.
* `regression-run`: runs the full suite (`just regression`) against the real server. Per
  `hardening-campaign.md`, a red nightly here gets filed as an issue under
  `.scratch/hardening/issues/` rather than fixed inline — the suite is explicitly frozen,
  not actively maintained, during the campaign.

Unlike the campaign's other nightly tiers (concurrency/coverage/jepsen), whose crons were
removed because they are expensive, unwatched, and gate nothing, this suite's whole purpose
is to keep a working (if frozen) regression signal alive while it's out of the PR path — so
it originally kept a real nightly cron in addition to `workflow_dispatch`, unlike every
other workflow in the repo. CI is manual-dispatch-only during the hardening campaign,
though, so that cron is off too for now; re-add `ScheduleTrigger` here once the campaign
ends if a standing nightly signal is still wanted.
"""

from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    libclang_step,
    mise_setup_step,
    run_step,
    rust_toolchain_step,
)
from workflow_gen.schema import Job, Trigger, Workflow

MISE_JUST_NEXTEST = "just cargo:cargo-nextest"

# GitHub-hosted standard runner: free and unmetered on public repos, matching the repo's
# other on-demand nightly tiers. Blacksmith is reserved for the testbox workflow.
RUNS_ON = "ubuntu-latest"

# Was "0 3 * * *" (nightly at 03:00 UTC) before the hardening campaign turned every
# schedule trigger off in favor of workflow_dispatch-only. Re-add a `ScheduleTrigger(
# cron=[...])` to the Trigger below to bring the cron back once the campaign ends.


def regression_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Regression Nightly",
        # CI is manual-dispatch-only during the hardening campaign.
        on=Trigger(),
    )

    w.job(
        "regression-check",
        Job(
            name="Regression Suite Compile Check",
            runs_on=RUNS_ON,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="regression"),
                run_step(name="Type-check frozen regression suite", run="just regression-check"),
            ],
        ),
    )

    w.job(
        "regression-run",
        Job(
            name="Run Frozen Redis Compat Suite",
            runs_on=RUNS_ON,
            # The full 63K-LOC compat suite spins up real servers per test group and runs
            # a large number of Redis-command-compat cases; generous headroom matching the
            # repo's other nightly tiers (concurrency-nightly / jepsen-nightly both use 360).
            timeout_minutes=360,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="regression"),
                run_step(name="Run frozen regression suite", run="just regression"),
            ],
        ),
    )

    return w
