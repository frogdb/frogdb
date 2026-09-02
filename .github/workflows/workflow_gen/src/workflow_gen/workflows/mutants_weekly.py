"""Weekly full-crate mutation re-measurement for every locked area.

Each locked area's mutation score was measured once, by hand, on the day it
locked. The per-PR ratchet (`mutants-diff` in `test.py`) holds the line for *new*
code only: deleting or weakening a test produces no mutant in the diff, so
survivors that test used to kill come back silently, and a refactor that moves
code within a crate re-mutates only the moved lines. Nothing re-measures the
area itself. This workflow does, on a schedule.

Weekly rather than nightly: a full run is hours of compute per locked crate, the
score drifts slowly, and the diff ratchet already covers day-to-day change.
Behind the shared `change_gate_job` (see `helpers.py`), so a scheduled run with
no new commits since the last successful one skips instead of re-mutating an
unchanged tree.

Shape: one `mutate` leg per (crate, shard), each uploading its
`mutants.out/outcomes.json`; one `score` leg per crate, which downloads that
crate's shards, refuses to score an incomplete set, and runs
`scripts/mutants-gate.py` over all of them at once — the crate's contract is one
score for the crate, not one per shard. The threshold is never typed here: the
gate script reads the crate's `Gate:` from its spec header, the same manifest
this workflow's matrix is generated from (`just locked-areas`).

A red run is read and acted on by hand, like the other scheduled tiers; nothing
auto-files an issue.
"""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import DOWNLOAD_ARTIFACT
from workflow_gen.helpers import (
    cargo_cache_step,
    change_gate_job,
    checkout_step,
    libclang_step,
    locked_areas,
    mise_setup_step,
    omap,
    rust_toolchain_step,
    script,
    upload_artifact_step,
)
from workflow_gen.schema import (
    Job,
    MatrixInclude,
    ScheduleTrigger,
    Step,
    Strategy,
    Trigger,
    Workflow,
)

# cargo-mutants shells out to the test tool named in .cargo/mutants.toml, which
# is nextest — so a mutation job needs both binaries. Same set `test.py`'s
# `mutants-diff` job installs, for the same reasons: python/uv are for the
# `uv run --script` shebang on scripts/locked_areas.py, which `just mutants`'
# siblings call to resolve the perimeter.
MISE_JUST_MUTANTS = "python uv just cargo:cargo-mutants cargo:cargo-nextest"
# The scoring job compiles nothing: it downloads outcomes files and runs
# scripts/mutants-gate.py, a `uv run --script` shebang over stdlib.
MISE_PYTHON = "python uv"

# GitHub-hosted standard runner: free and unmetered on public repos, matching the
# repo's other scheduled tiers. Blacksmith is reserved for the testbox workflow.
# Promote a crate's legs to a bigger runner if they outgrow the leg timeout.
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "mutants-weekly.yml"

# Mondays at 04:17 UTC: off the hour (the GitHub Actions cron traffic spike) and
# off the nightlies' 03:00/03:14 slots, so a weekly run never queues behind them.
WEEKLY_CRON = "17 4 * * 1"

# `cargo mutants --shard k/n` legs per crate, sized by how long a full run takes.
# The three big crates are hours each; everything else fits in one leg. A crate
# absent here runs unsharded.
SHARDS = {"frogdb-persistence": 4, "frogdb-replication": 4, "frogdb-cluster": 4}
DEFAULT_SHARDS = 1

# Per-leg ceiling. A full run is the long pole of this workflow; four hours is
# the sharded-leg budget from the issue's design, not the expected runtime.
LEG_TIMEOUT_MINUTES = 240

# cargo-mutants builds a mutated tree per mutant; two parallel jobs is what a
# 2-core standard runner sustains without the builds thrashing each other.
MUTANTS_JOBS = "2"

# `workflow_dispatch` reads its input through `github.event.inputs`, not the
# `inputs` context: the same expression has to evaluate on `schedule`, where
# `inputs` does not exist (see concurrency_nightly.py). On a scheduled run this
# is null, which compares equal to the empty string — i.e. "all crates".
CRATE_INPUT = "github.event.inputs.crate"
SELECTED = f"({CRATE_INPUT} == '' || {CRATE_INPUT} == matrix.crate)"


def only_selected(steps: list[Step]) -> list[Step]:
    """Bind every step to the dispatch's one-crate filter.

    The filter has to sit on the steps rather than on the job: `matrix` is not
    among the contexts a job-level `if:` may read (GitHub's context-availability
    table — `actionlint` rejects it), and the matrix is generated at
    `just workflow-gen` time from the manifest, so there is no earlier job whose
    output could narrow it. A leg for a crate nobody asked for therefore runs no
    steps and finishes green in seconds instead of mutating that crate anyway.
    """
    for step in steps:
        step.if_ = SELECTED if step.if_ is None else f"({step.if_}) && {SELECTED}"
    return steps


OUTPUT_DIR = "target/mutants/${{ matrix.crate }}"

# One artifact per shard, `mutants-<crate>-shard-<k>`; the score job downloads a
# crate's shards by prefix.
ARTIFACT_PREFIX = "mutants-"


def _crate_input() -> CommentedMap:
    inp = CommentedMap()
    inp["description"] = "one locked crate to run; empty = all"
    inp["required"] = False
    inp["default"] = SQ("")
    inp["type"] = "string"
    return inp


def _shards(crate: str) -> int:
    return SHARDS.get(crate, DEFAULT_SHARDS)


def mutate_matrix() -> MatrixInclude:
    """One leg per (locked crate, shard), generated from the manifest.

    `gate` rides along per leg: GitHub renders a matrix leg's values in its
    display name, so the run page says which contract each leg is feeding
    without anyone opening the spec. It is not a threshold anyone typed and no
    step reads it — the gate script resolves `Gate:` from the header itself.
    """
    legs = []
    for spec in locked_areas():
        for crate in spec.crates:
            n = _shards(crate)
            for k in range(n):
                legs.append(omap(crate=crate, shard=SQ(f"{k}/{n}"), gate=SQ(f"{spec.gate:.2f}")))
    return MatrixInclude(includes=legs)


def score_matrix() -> MatrixInclude:
    """One leg per locked crate, carrying the shard count it must find."""
    return MatrixInclude(
        includes=[
            omap(crate=crate, shards=SQ(str(_shards(crate))))
            for spec in locked_areas()
            for crate in spec.crates
        ]
    )


def mutate_job() -> Job:
    """Mutate one shard of one locked crate and publish its outcomes file."""
    return Job(
        name="Mutate ${{ matrix.crate }} (shard ${{ matrix.shard }})",
        runs_on=RUNS_ON,
        needs="gate",
        if_="needs.gate.outputs.skip != 'true'",
        timeout_minutes=LEG_TIMEOUT_MINUTES,
        strategy=Strategy(matrix=mutate_matrix(), fail_fast=False),
        steps=only_selected(
            [
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_MUTANTS),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="mutants"),
                Step(
                    name="Mutate this shard",
                    env=omap(CRATE="${{ matrix.crate }}", SHARD="${{ matrix.shard }}"),
                    run=script(f"""\
                    set -uo pipefail
                    status=0
                    just mutants "${{CRATE}}" --shard "${{SHARD}}" --jobs {MUTANTS_JOBS} || status=$?
                    # cargo-mutants exits 2 for missed mutants and 3 for timed-out
                    # ones. Neither is this leg's verdict: the score job sums every
                    # shard's outcomes.json and gates the crate's ratio against its
                    # spec header, where a survivor costs score and a timeout is
                    # excluded but reported. Any other non-zero (usage error, a
                    # failing baseline build) means there is no run to score, so it
                    # fails the leg — and the score job then reports the crate
                    # incomplete rather than scoring a fraction of it.
                    case "${{status}}" in
                      0|2|3) exit 0 ;;
                      *) exit "${{status}}" ;;
                    esac
                    """),
                ),
                Step(
                    id="artifact",
                    name="Name this shard's artifact",
                    env=omap(CRATE="${{ matrix.crate }}", SHARD="${{ matrix.shard }}"),
                    run=script(f"""\
                    set -euo pipefail
                    # `k/n` is cargo-mutants' shard spelling; an artifact name
                    # cannot contain `/`, so the name carries k alone. The
                    # `-shard-` separator is what keeps the score job's
                    # `{ARTIFACT_PREFIX}<crate>-shard-*` pattern from matching a
                    # longer crate name: `frogdb-cluster-*` would otherwise
                    # swallow `frogdb-cluster-runtime`'s shard too.
                    echo "name={ARTIFACT_PREFIX}${{CRATE}}-shard-${{SHARD%%/*}}" >> "$GITHUB_OUTPUT"
                    """),
                ),
                upload_artifact_step(
                    name="${{ steps.artifact.outputs.name }}",
                    # missed.txt is the human-readable survivor list the score job's
                    # summary merges; previously_caught.txt only exists after a local
                    # --iterate run, but the gate script honours it beside its own
                    # outcomes.json, so it travels with it.
                    path=script(f"""\
                    {OUTPUT_DIR}/mutants.out/outcomes.json
                    {OUTPUT_DIR}/mutants.out/missed.txt
                    {OUTPUT_DIR}/mutants.out/previously_caught.txt
                    """),
                    if_no_files_found="error",
                ),
            ]
        ),
    )


def score_job() -> Job:
    """Score one crate's shards as a single run against its spec-header gate.

    `always()`: a leg that fails or is cancelled must still reach this job, which
    reports the crate incomplete rather than letting a partial upload silently
    become a score. The dispatch filter matches the mutate job's — without it a
    run targeting one crate would fail the seven crates it never mutated.
    """
    return Job(
        name="Score ${{ matrix.crate }}",
        runs_on=RUNS_ON,
        needs=["gate", "mutate"],
        if_="always() && needs.gate.outputs.skip != 'true'",
        strategy=Strategy(matrix=score_matrix(), fail_fast=False),
        steps=only_selected(
            [
                # The gate script resolves the crate's threshold from its spec header,
                # so the checkout is what makes the run's contract available.
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON),
                Step(
                    name="Download this crate's shards",
                    uses=DOWNLOAD_ARTIFACT,
                    with_=omap(
                        pattern=f"{ARTIFACT_PREFIX}${{{{ matrix.crate }}}}-shard-*",
                        path=OUTPUT_DIR,
                    ),
                ),
                Step(
                    name="Score the crate against its spec gate",
                    # `always()`: when every leg of a crate failed there is no
                    # artifact to download and that step fails, which would
                    # otherwise skip the one step that says *why* the crate has
                    # no score. The job still fails — on the download and on
                    # this step's own exit 1 — but the summary explains it.
                    if_="always()",
                    env=omap(
                        CRATE="${{ matrix.crate }}",
                        SHARDS="${{ matrix.shards }}",
                        OUT=OUTPUT_DIR,
                    ),
                    run=script("""\
                    set -uo pipefail
                    shopt -s nullglob
                    outcomes=("${OUT}"/*/outcomes.json)
                    if [ "${#outcomes[@]}" -ne "${SHARDS}" ]; then
                      echo "incomplete run: expected ${SHARDS} shards, got ${#outcomes[@]}"
                      echo "### mutants-weekly: ${CRATE}" >> "$GITHUB_STEP_SUMMARY"
                      echo "incomplete run: expected ${SHARDS} shards, got ${#outcomes[@]} — not a score." >> "$GITHUB_STEP_SUMMARY"
                      exit 1
                    fi
                    # Streams are captured apart and replayed in order: the gate
                    # script's verdict goes to stderr and its counts to stdout,
                    # which one merged redirect interleaves by buffer, not by
                    # line.
                    status=0
                    ./scripts/mutants-gate.py "${outcomes[@]}" --crate "${CRATE}" \\
                      > gate.out 2> gate.err || status=$?
                    cat gate.out
                    cat gate.err >&2
                    # Every shard's survivors, merged. A shard that caught
                    # everything writes an empty missed.txt, and an older run
                    # may not have uploaded one at all — neither is a failure,
                    # so the glob is read through nullglob rather than `cat`.
                    missed=("${OUT}"/*/missed.txt)
                    survivors=""
                    if [ "${#missed[@]}" -gt 0 ]; then survivors=$(cat "${missed[@]}"); fi
                    {
                      echo "### mutants-weekly: ${CRATE}"
                      echo
                      sed 's/^/    /' gate.out gate.err
                      if [ -n "${survivors}" ]; then
                        echo
                        echo "Surviving mutants — each needs a forcing test or a documented-equivalent exclusion:"
                        echo
                        sed 's/^/- /' "${missed[@]}"
                      fi
                    } >> "$GITHUB_STEP_SUMMARY"
                    exit "${status}"
                    """),
                ),
            ]
        ),
    )


def mutants_weekly_workflow() -> Workflow:
    w = Workflow(
        name="Mutants Weekly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[WEEKLY_CRON]),
            workflow_dispatch_inputs=CommentedMap(crate=_crate_input()),
        ),
    )

    w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))
    w.job("mutate", mutate_job())
    w.job("score", score_job())

    return w
