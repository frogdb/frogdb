"""Nightly replication-correctness fault-scheduler sweep.

The replication arm of the seeded turmoil scheduler
(`.scratch/replication-correctness/`, issue 12): a primary and two replicas over
the same seed derivation the cluster arm uses, where one `u64` derives the whole
run — fault family, held and slowed links, backlog geometry, full-sync payload
shape, and the client workload. Per-PR coverage is a seven-seed smoke sweep (one
per fault family) plus a determinism double-run in the default suite; this tier
sweeps the full seed budget.

Driven through `just replication-seeds` so the budget lives in exactly one place
(PRD §8 D1/D8) rather than being duplicated here. Its cluster sibling is
`cluster_nightly.py`; the two are separate workflows rather than two jobs of one
because they belong to different campaigns and a change gate for one should not
drag the other's build along.
"""

from ruamel.yaml.comments import CommentedMap

from workflow_gen.helpers import (
    cargo_cache_step,
    change_gate_job,
    checkout_step,
    libclang_step,
    mise_setup_step,
    run_step,
    rust_toolchain_step,
    script,
)
from workflow_gen.schema import Job, ScheduleTrigger, Trigger, Workflow

MISE_JUST_NEXTEST = "just cargo:cargo-nextest"

# GitHub-hosted standard runner: free and unmetered on public repos. The sweep
# shards seeds across its own worker threads, so a bigger (paid) box would only
# shorten a run that already fits well inside the timeout.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "replication-seeds-nightly.yml"

# 04:11 UTC: off the hour (avoids the GitHub Actions cron traffic spike at :00)
# and clear of the cluster nightly's 03:47 slot, so the two sweeps do not
# contend for runners.
NIGHTLY_CRON = "11 4 * * *"


def _seeds_input() -> CommentedMap:
    inp = CommentedMap()
    # No default: left empty, the step passes no argument and the
    # `replication-seeds` recipe's own `SEEDS` default applies. The budget lives
    # in that one Justfile variable, and a default echoed here would be a second
    # copy of it that nothing keeps in step.
    inp["description"] = (
        "fault-scheduler seeds to sweep (blank = the replication-seeds recipe's "
        "own default; the per-PR suite runs a seven-seed smoke sweep)"
    )
    inp["required"] = False
    inp["type"] = "string"
    return inp


def _common_steps(*, cache_key: str) -> list:
    return [
        checkout_step(),
        mise_setup_step(install_args=MISE_JUST_NEXTEST),
        rust_toolchain_step(),
        libclang_step(),
        cargo_cache_step(shared_key=cache_key),
    ]


def replication_seeds_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Replication Seeded Sweep Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(seeds=_seeds_input()),
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "replication-seeds",
        Job(
            name="Nightly Replication Fault-Scheduler Seed Sweep",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Each seed brings up three real servers on the simulated network,
            # drives a workload through faults to quiescence, and runs the
            # invariant catalog on every survivor — a second or two apiece in a
            # debug build, sharded across the runner's cores by the sweep's own
            # worker threads. The default budget lands well inside this; the
            # headroom is for a dispatch that raises `seeds`.
            timeout_minutes=120,
            steps=[
                # Its own cache key, disjoint from the cluster nightly's: both
                # build `frogdb-server`'s turmoil-featured test binary, but they
                # run on different schedules and sharing one key would have them
                # evict each other's entries.
                *_common_steps(cache_key="replication-nightly-seeds"),
                run_step(
                    name="Run the replication fault-scheduler sweep at the nightly seed budget",
                    # `${seeds:+"$seeds"}` passes the argument only when the
                    # dispatch input is non-empty, so a scheduled run (no
                    # inputs) falls through to the recipe's default instead of
                    # naming the budget a second time here.
                    run=script("""\
                        seeds="${{ github.event.inputs.seeds }}"
                        just replication-seeds ${seeds:+"$seeds"}
                    """),
                ),
            ],
        ),
    )

    return w
