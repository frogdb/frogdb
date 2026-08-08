"""Nightly concurrency-invariant sweep workflow definition.

Per-PR concurrency coverage (~20 seeds x short workloads) runs inline in
`test.yml`'s `shuttle-tests`/`turmoil-tests` jobs plus the
`seed_sweep_short_workloads`/`seed_sweep_txheavy` tests pulled in by
`cargo nextest ... -E 'test(/simulation|seed_sweep/)'`-style filters (see
`just concurrency`). This workflow is the nightly tier of the
concurrency-invariant-testing design (`.scratch/concurrency-testing/`):
many more seeds, longer per-client histories, and every workload profile
(including `MultiWaiter`, which the per-PR tier skips in favor of its own
dedicated smoke test). It drives `seed_sweep_nightly` in
`frogdb-server/crates/server/tests/concurrency_workload.rs`, a `#[ignore]`d
test invisible to `just test`/`just concurrency`, via `just concurrency-nightly`.
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
    upload_artifact_step,
)
from workflow_gen.schema import Job, ScheduleTrigger, Trigger, Workflow

MISE_JUST_NEXTEST = "just cargo:cargo-nextest"

# GitHub-hosted standard runner: free and unmetered on public repos. The sweep is
# one sequential test loop, not parallelized across cores, so a bigger (paid) box
# buys nothing here. Blacksmith is reserved for the testbox workflow.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "concurrency-nightly.yml"

# 03:14 UTC has no significance beyond "not on the hour" (avoids the
# GitHub Actions cron traffic spike at :00) and lands overnight for US time
# zones, where this repo's activity concentrates.
NIGHTLY_CRON = "14 3 * * *"

# Unique seeds swept, each replayed once per profile (overridable per-dispatch); NOT the total
# run count. Default 250 unique seeds x 4 profiles (Mixed, BlockingHeavy, TxHeavy, MultiWaiter)
# = 1000 (seed, profile) generated-workload runs. This is our chosen reading of the spec's
# "1000+ seeds" nightly bar as "1000+ generated-workload runs," since a literal 1000 *unique*
# seeds x 4 profiles would be 4000 runs.
DEFAULT_SEEDS_PER_PROFILE = "250"

REPRO_DIR = "target/concurrency-repros"


def _seeds_input() -> CommentedMap:
    inp = CommentedMap()
    inp["description"] = (
        "Unique seeds to sweep, each run under all 4 profiles (default 250 seeds x 4 profiles "
        "= 1000 total runs)"
    )
    inp["required"] = False
    inp["default"] = SQ(DEFAULT_SEEDS_PER_PROFILE)
    inp["type"] = "string"
    return inp


def concurrency_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Concurrency Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(seeds=_seeds_input()),
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "seed-sweep-nightly",
        Job(
            name="Nightly Generated-Workload Seed Sweep",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Testbox timing at the harness's actual defaults (ops_per_client=75,
            # see Justfile `concurrency-nightly` / concurrency_workload.rs
            # `seed_sweep_nightly`): ~3.5-4.6s per seed x profile combo across the
            # ops_per_client range bisected in
            # .scratch/concurrency-testing/issues/11.
            # At the default 250 seeds x 4 profiles = 1000 combos, that's roughly
            # an hour end to end; this ceiling leaves ~6x margin for CI-runner
            # variance (and remains a safety net rather than the expected runtime
            # if `seeds` is later parameterized much higher via workflow_dispatch).
            timeout_minutes=360,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="concurrency-nightly"),
                run_step(
                    name="Run nightly generated-workload seed sweep",
                    run=script(f"""\
                        just concurrency-nightly "${{{{ github.event.inputs.seeds || '{DEFAULT_SEEDS_PER_PROFILE}' }}}}"
                    """),
                ),
                upload_artifact_step(
                    name="concurrency-nightly-repros",
                    path=REPRO_DIR,
                    if_="failure()",
                    # This step only runs when the sweep already failed, so an empty upload
                    # means the repro path is wrong (e.g. a CWD/workspace-root mismatch) — fail
                    # loudly instead of the default "warn" (green, empty artifact).
                    if_no_files_found="error",
                ),
            ],
        ),
    )

    return w
