"""Nightly stateright model-checking workflow definition (replication).

The replication-correctness campaign's W3 wave
(`.scratch/replication-correctness/PRD.md` §3 W3, issues 08 and 09) added
explicit-state models in `frogdb-server/crates/replication/src/model/` that
drive *production* decision functions: the slot-handoff replica-feed hold
(FM-CLUSTER-097, via `decide_feed_hold_until` / `decide_publish` /
`decide_hold`) and the promotion / resume composite (via `plan_primary_stint`,
`PartialSyncReplay::handle_partial_sync_request`, `select_psync_arm`).

Tiering follows the cluster models' ruling: per-commit runs get the bounded
smoke configs (in the default suite), and the real exploration budgets run
nightly. This workflow is that nightly tier, driving the `#[ignore]`d full-scope
tests via `just replication-model-check` — the same budget a laptop can run.

Separate from `cluster-model-nightly.yml` rather than folded into it: the two
crates' models are budgeted and floored independently, and one job running both
would make either crate's scope widening extend the other's failure surface.
"""

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

# Same reasoning as the cluster model nightly: BFS model checking here is a
# single crate's unit test, cheap enough for the free unmetered runner.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "replication-model-nightly.yml"

# 04:47 UTC: off the hour, and twenty minutes behind `cluster-model-nightly` so
# the two model tiers do not contend for runner capacity.
NIGHTLY_CRON = "47 4 * * *"


def replication_model_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Replication Model Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch=True,
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "model-check",
        Job(
            name="Stateright Full Exploration Budget",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Every full config enumerates its whole reachable space in well
            # under a minute on a laptop (the numbers are recorded in each
            # model's file header and floored by the tests themselves). The
            # ceiling is a safety net for a scope widened later, not the
            # expected runtime.
            timeout_minutes=90,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="replication-model-nightly"),
                run_step(
                    name="Model-check the replication composites",
                    run=script("""\
                        just replication-model-check
                    """),
                ),
            ],
        ),
    )

    return w
