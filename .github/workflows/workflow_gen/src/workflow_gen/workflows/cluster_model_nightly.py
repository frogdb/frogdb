"""Nightly stateright model-checking workflow definition.

The cluster-correctness campaign's W3 wave (`.scratch/cluster-correctness/PRD.md`
§3 W3, §8 D1) added explicit-state models that drive the *production* cluster
state machine (`frogdb-cluster`'s `apply_command`) through every interleaving of
the two-phase slot handoff (model 1) and of the failover composite (model 2)
inside a small scope. D1 ruled: per-commit runs get bounded-depth smoke configs
(< 10 s, in the default suite), and the real exploration budgets run nightly.

This workflow is that nightly tier. It drives the `#[ignore]`d full-scope model
tests in `frogdb-server/crates/cluster/src/model/` via `just model-check`, whose
default pattern covers every full config of both models, and which also exists
so the same budget is runnable on a laptop.
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

# Same reasoning as the concurrency nightly: BFS model checking here is a single
# crate's unit test, cheap enough for the free unmetered runner.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "cluster-model-nightly.yml"

# 04:27 UTC: off the hour (avoids the Actions cron spike at :00) and staggered
# away from the other nightlies so they do not contend for runner capacity.
NIGHTLY_CRON = "27 4 * * *"


def cluster_model_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Cluster Model Nightly",
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
            # The full scope explores its whole reachable space in minutes on a
            # laptop (the number is recorded in the model's file header and
            # asserted by the test itself). The ceiling is a safety net for a
            # scope widened later, not the expected runtime.
            timeout_minutes=90,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="cluster-model-nightly"),
                run_step(
                    name="Model-check the slot handoff and the failover composite",
                    run=script("""\
                        just model-check
                    """),
                ),
            ],
        ),
    )

    return w
