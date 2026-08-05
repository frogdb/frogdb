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
it keeps a real nightly cron in addition to `workflow_dispatch`, unlike most other workflows
in the repo. Both jobs sit behind the shared `change_gate_job` (see `helpers.py`): a
scheduled run with no new commits since the last successful run skips rather than re-running
63K lines of compat tests for nothing. The suite itself remains frozen for the duration of
the hardening campaign (`docs/agents/hardening-campaign.md`) — a red `regression-run` gets
filed as an issue, not fixed inline; that policy is orthogonal to whether the workflow runs.
"""

from workflow_gen.helpers import (
    cargo_cache_step,
    change_gate_job,
    checkout_step,
    libclang_step,
    mise_setup_step,
    run_step,
    rust_toolchain_step,
)
from workflow_gen.schema import Job, ScheduleTrigger, Trigger, Workflow

MISE_JUST_NEXTEST = "just cargo:cargo-nextest"

# GitHub-hosted standard runner: free and unmetered on public repos, matching the repo's
# other on-demand nightly tiers. Blacksmith is reserved for the testbox workflow.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "regression-nightly.yml"

# Nightly at 03:00 UTC. No other workflow in this repo currently has an active cron (the
# other nightly tiers deliberately removed theirs — see their module docstrings), so there
# is nothing to stagger against; this is just an off-peak hour.
NIGHTLY_CRON = "0 3 * * *"


def regression_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Regression Nightly",
        on=Trigger(schedule=ScheduleTrigger(cron=[NIGHTLY_CRON])),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "regression-check",
        Job(
            name="Regression Suite Compile Check",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
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
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
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
