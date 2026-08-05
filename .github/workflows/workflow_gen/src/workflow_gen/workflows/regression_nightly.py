"""Nightly Redis-compat regression workflow definition.

The 63K-LOC `frogdb-redis-regression` compat suite (`frogdb-server/crates/redis-regression`)
was frozen for the duration of the foundation-hardening campaign — gated behind a
`regression` required-feature and out of `just check`/`just test` — which left compat
breakage invisible to the PR-gating `test.yml` run. This workflow was the nightly signal
that filled that gap. The freeze ended at campaign exit (see
`docs/agents/hardening-campaign.md`): the suite is back in the default dev loop, so this
tier is now a scheduled backstop rather than the only signal, mirroring the repo's other
on-demand tiers (`concurrency_nightly.py` / `jepsen_nightly.py` / `coverage_nightly.py`):

* `regression-check`: compiles the suite (`just regression-check`) without running it — the
  cheap anti-rot guard that catches the suite silently bit-rotting against API changes
  elsewhere in the workspace, without paying the cost of actually running 63K lines of
  compat tests.
* `regression-run`: runs the full suite (`just regression`) against the real server. Now
  that the suite is unfrozen, a red run here is a normal failure to fix, not an issue to
  file.

Unlike the campaign's other nightly tiers (concurrency/coverage/jepsen), whose crons were
removed because they are expensive, unwatched, and gate nothing, this one keeps a real
nightly cron in addition to `workflow_dispatch` so the full compat run has a scheduled home
even when the PR tier is scoped down. Both jobs sit behind the shared `change_gate_job` (see
`helpers.py`): a scheduled run with no new commits since the last successful run skips
rather than re-running 63K lines of compat tests for nothing.
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
                run_step(name="Type-check regression suite", run="just regression-check"),
            ],
        ),
    )

    w.job(
        "regression-run",
        Job(
            name="Run Redis Compat Suite",
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
                run_step(name="Run regression suite", run="just regression"),
            ],
        ),
    )

    return w
