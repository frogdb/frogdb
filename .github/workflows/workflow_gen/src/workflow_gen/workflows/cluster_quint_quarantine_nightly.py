"""Nightly quint-connect conformance quarantine report.

Replays the quint-connect conformance harness's `#[ignore]`d traces
(`frogdb-server/crates/cluster/tests/quint_conformance.rs`) every night. Most
are expected to keep failing until issues 15/17/19/20/26 and the ghost-field
issue land — that is normal, hand-triaged nightly red, the same as
`cluster-seeds`' known-failing-seed convention (cluster_nightly.py). The job
exists so a quarantined test that starts conforming shows up as a status flip
instead of staying silently un-noticed (see issues 15/17/19/20/26's
acceptance criteria).

This used to be a third job inside `cluster-nightly.yml`, behind that
workflow's change gate. Final-review finding I3: `change_gate_job` keys off
`gh run list --workflow cluster-nightly.yml --status success`, and a
report-only job that is *expected* to stay red means that workflow's run
never concludes success — so `last_sha` stayed empty, `skip` was always
`false`, and `cluster-proptest`/`cluster-seeds` (the two jobs the gate exists
to protect) ran unconditionally every night regardless of whether anything
had changed, defeating the whole point of gating them. Splitting this job
into its own ungated workflow — no `change_gate_job` at all, since "run
whether or not anything changed" is this job's actual intent, not a bug to
work around — fixes that without touching the `Job` schema (the smaller of
the two fixes I3 considered).
"""

from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    libclang_step,
    mise_setup_step,
    run_step,
    rust_toolchain_step,
    script,
)
from workflow_gen.schema import Job, ScheduleTrigger, Trigger, Workflow

# quint-connect shells out to the `quint` CLI at test runtime
# (quint-connect-0.1.2/src/trace/generator/utils.rs), so this job needs both
# cargo-nextest (to run the test binary) and quint itself — the same gap C1
# found and fixed in test.yml's `unit-tests` job.
#
# `node` first: the `npm:` backend depends on it and mise >= 2026.8.11 enforces that
# (jdx/mise#12234) — see .scratch/build-toolchain/issues/done/04-ci-mise-npm-backend-node-dependency.md.
MISE_JUST_NEXTEST_QUINT = "just node cargo:cargo-nextest npm:@informalsystems/quint"

# GitHub-hosted standard runner: free and unmetered on public repos, same as
# the rest of the cluster-correctness nightlies.
RUNS_ON = "ubuntu-latest"

# 03:22 UTC: off the hour, and clear of every other nightly's slot (see each
# module's own NIGHTLY_CRON comment for the full set already in use).
NIGHTLY_CRON = "22 3 * * *"


def cluster_quint_quarantine_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Cluster Quint Conformance Quarantine Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch=True,
        ),
    )

    w.job(
        "cluster-quint-conformance-quarantine",
        Job(
            name="Nightly Quint Conformance Quarantine Report",
            runs_on=RUNS_ON,
            # No change gate (see module docstring): this job is meant to run
            # every night regardless of whether anything changed, so a status
            # flip (a quarantined test starting to pass) is never missed.
            timeout_minutes=30,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST_QUINT),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="cluster-nightly"),
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
