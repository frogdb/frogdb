"""Nightly cluster state-machine property sweep workflow definition.

Per-PR coverage of the property harness runs inline in `test.yml` as part of
the ordinary `frogdb-cluster` unit tests, at a case budget sized for the dev
loop (`DEFAULT_CASES` in
`frogdb-server/crates/cluster/src/properties.rs`). This workflow is the nightly
tier of the cluster-correctness campaign (`.scratch/cluster-correctness/`,
issue 03): the same properties, three orders of magnitude more generated
command sequences, driven through `just cluster-proptest` so the case budget
lives in exactly one place (PRD §8 D4) rather than being duplicated here.
"""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

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

# GitHub-hosted standard runner: free and unmetered on public repos. proptest
# runs one case at a time, so a bigger (paid) box buys nothing here.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "cluster-nightly.yml"

# 03:47 UTC: off the hour (avoids the GitHub Actions cron traffic spike at :00)
# and clear of the other nightlies' slots.
NIGHTLY_CRON = "47 3 * * *"

# proptest cases per property. Must match the `cluster-proptest` recipe's own
# default; it is repeated here only so the workflow_dispatch input can show it.
DEFAULT_CASES = "200000"


def _cases_input() -> CommentedMap:
    inp = CommentedMap()
    inp["description"] = (
        f"proptest cases per property (default {DEFAULT_CASES}; the dev-loop budget is ~96)"
    )
    inp["required"] = False
    inp["default"] = SQ(DEFAULT_CASES)
    inp["type"] = "string"
    return inp


def cluster_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Cluster Properties Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(cases=_cases_input()),
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "cluster-proptest",
        Job(
            name="Nightly Cluster State-Machine Property Sweep",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # ~7k cases/s per property in a debug build locally, so the default
            # budget is well under a minute of test time; the ceiling is
            # dominated by the cold build and leaves room for a dispatch that
            # raises `cases` substantially.
            timeout_minutes=60,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="cluster-nightly"),
                run_step(
                    name="Run cluster property sweep at the nightly case budget",
                    run=script(f"""\
                        just cluster-proptest "${{{{ github.event.inputs.cases || '{DEFAULT_CASES}' }}}}"
                    """),
                ),
            ],
        ),
    )

    return w
