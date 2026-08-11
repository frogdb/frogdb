"""Nightly replication correctness property sweep.

The replication-correctness campaign's W2 wave
(`.scratch/replication-correctness/PRD.md` §3 W2, issue 04) added a stateful
proptest generator in `frogdb-server/crates/replication/src/properties.rs` that
folds link actions -- writes, acks, attach/detach, PSYNC grants, promotions,
feed-gate barriers, restarts -- through a *real* replication node and asserts
the `frogdb-replication` invariant catalog after every one of them (property
R1).

Tiering follows the cluster property harness's ruling: per-PR coverage of the
same properties runs inline in `test.yml` as part of the ordinary
`frogdb-replication` unit tests, at a case budget sized for the dev loop
(`DEFAULT_CASES` in that module); this tier raises it three orders of magnitude.
The budget is passed to `just replication-proptest` rather than restated here,
so it lives in exactly one place.

Separate from `replication-model-nightly.yml` rather than folded into it: the
model checks and the property harness are budgeted independently and fail for
different reasons, and one job running both would let a model scope widening
push the property sweep into a shared timeout.
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

WORKFLOW_FILE = "replication-nightly.yml"

# 05:17 UTC: off the hour, and clear of every other nightly's slot (the closest
# neighbours are `replication-model-nightly` at 04:47 and `jepsen-nightly` at
# 05:37).
NIGHTLY_CRON = "17 5 * * *"

# proptest cases for the property sweep. Must match the `replication-proptest`
# recipe's own default; it is repeated here only so the workflow_dispatch input
# can show it.
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


def replication_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Replication Correctness Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(cases=_cases_input()),
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "replication-proptest",
        Job(
            name="Nightly Replication Link Property Sweep",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Every case stands up a real node on a real temp directory, so this
            # is slower per case than a pure state-machine harness: a 30k-case
            # run measured ~290 cases/s in a debug build on an M-series laptop,
            # putting the default budget near a quarter of an hour. The ceiling
            # covers a much colder runner and leaves room for a dispatch that
            # raises `cases`.
            timeout_minutes=90,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="replication-nightly"),
                run_step(
                    name="Run the replication property sweep at the nightly case budget",
                    run=script(f"""\
                        just replication-proptest "${{{{ github.event.inputs.cases || '{DEFAULT_CASES}' }}}}"
                    """),
                ),
            ],
        ),
    )

    return w
