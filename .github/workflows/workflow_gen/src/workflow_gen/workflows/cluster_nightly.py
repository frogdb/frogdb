"""Nightly cluster correctness sweeps (property harness + fault scheduler).

Two nightly tiers of the cluster-correctness campaign
(`.scratch/cluster-correctness/`), both driven through their `just` recipe so the
budget lives in exactly one place (PRD §8 D4) rather than being duplicated here:

- `cluster-proptest` (issue 03) — the `frogdb-cluster` state-machine property
  harness. Per-PR coverage of the same properties runs inline in `test.yml` as
  part of the ordinary `frogdb-cluster` unit tests, at a case budget sized for
  the dev loop (`DEFAULT_CASES` in `frogdb-server/crates/cluster/src/properties.rs`);
  this tier raises it three orders of magnitude.
- `cluster-seeds` (issue 09) — the seed-driven turmoil fault scheduler, which
  generalizes the scripted multi-node sims: one `u64` derives the whole run
  (faults, timings, Raft timer skew, workload). Per-PR coverage is a six-seed
  smoke sweep plus the regression-seed list in the default suite; this tier
  sweeps the full seed budget.

The two run as independent jobs behind one change gate: they share nothing but
the schedule, and a failure in one should not hide the other's result.

A third, report-only job (`cluster-quint-conformance-quarantine`) replays the
quint-connect conformance harness's `#[ignore]`d traces
(`frogdb-server/crates/cluster/tests/quint_conformance.rs`) every night. Most
are expected to keep failing until issues 15/17/19/20/26 and the ghost-field
issue land — that is normal, `just`-triaged-by-hand nightly red, the same as
`cluster-seeds`' known-failing-seed convention. The job exists so a quarantined
test that starts conforming shows up as a status flip instead of staying
silently un-noticed (see issues 15/17/19/20/26's acceptance criteria).
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


def _seeds_input() -> CommentedMap:
    inp = CommentedMap()
    # No default: left empty, the step passes no argument and the `cluster-seeds`
    # recipe's own `SEEDS` default applies. PRD §8 D4 puts the budget in one
    # Justfile variable, and a default echoed here would be a second copy of it
    # that nothing keeps in step.
    inp["description"] = (
        "fault-scheduler seeds to sweep (blank = the cluster-seeds recipe's own "
        "default; the per-PR suite runs a six-seed smoke sweep)"
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


def cluster_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Cluster Correctness Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(
                cases=_cases_input(),
                seeds=_seeds_input(),
            ),
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
                *_common_steps(cache_key="cluster-nightly"),
                run_step(
                    name="Run cluster property sweep at the nightly case budget",
                    run=script(f"""\
                        just cluster-proptest "${{{{ github.event.inputs.cases || '{DEFAULT_CASES}' }}}}"
                    """),
                ),
            ],
        ),
    )

    w.job(
        "cluster-seeds",
        Job(
            name="Nightly Cluster Fault-Scheduler Seed Sweep",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Each seed brings up a real 3-node Raft cluster on the simulated
            # network and drives it to quiescence; a few seconds apiece in a
            # debug build, sharded across the runner's cores by the sweep's own
            # worker threads. The default budget lands well inside this, and the
            # headroom is for a dispatch that raises `seeds`.
            timeout_minutes=120,
            steps=[
                # A separate cache key from the proptest job: this one builds
                # `frogdb-server`'s turmoil-featured test binary, a disjoint
                # (and much larger) artifact set from `frogdb-cluster`'s unit
                # tests, and sharing one key would have the two jobs evict each
                # other's entries every night.
                *_common_steps(cache_key="cluster-nightly-seeds"),
                run_step(
                    name="Run the fault-scheduler sweep at the nightly seed budget",
                    # `${seeds:+"$seeds"}` passes the argument only when the
                    # dispatch input is non-empty, so a scheduled run (no
                    # inputs) falls through to the recipe's default instead of
                    # naming the budget a second time here.
                    run=script("""\
                        seeds="${{ github.event.inputs.seeds }}"
                        just cluster-seeds ${seeds:+"$seeds"}
                    """),
                ),
            ],
        ),
    )

    w.job(
        "cluster-quint-conformance-quarantine",
        Job(
            name="Nightly Quint Conformance Quarantine Report",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Report-only: a red run here means the quarantine list is still
            # accurate, not that the workflow is broken. See the module
            # docstring.
            timeout_minutes=30,
            steps=[
                *_common_steps(cache_key="cluster-nightly"),
                run_step(
                    name="Replay the quint-connect conformance harness's quarantined traces",
                    run=script("""\
                        just quint-conformance-quarantine
                    """),
                ),
            ],
        ),
    )

    return w
