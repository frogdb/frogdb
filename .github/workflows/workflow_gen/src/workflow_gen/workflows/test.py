"""Test workflow definition."""

from textwrap import dedent

from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    ACTIONLINT,
    CACHE,
    PATHS_FILTER,
    SETUP_GO,
)
from workflow_gen.helpers import (
    MISE_JUST_MUTANTS,
    cargo_cache_step,
    checkout_step,
    ensure_path,
    libclang_step,
    locked_areas,
    locked_crate_paths,
    mise_setup_step,
    omap,
    run_step,
    rust_toolchain_step,
    script,
)
from workflow_gen.schema import (
    Concurrency,
    Job,
    MatrixExpr,
    PullRequestTrigger,
    PushTrigger,
    Step,
    Strategy,
    Trigger,
    Workflow,
)

# Runner label — GitHub-hosted standard runners, which are free and unmetered on
# public repositories. This previously routed trusted actors to a `self-hosted`
# ARM64 box; that runner is retired, and jobs pinned to it queued until the 48h
# GitHub timeout instead of running. Blacksmith runners are reserved for the
# testbox workflow (test-unit-tests-testbox.yml).
RUNS_ON = "ubuntu-latest"

# mise install_args per job — only install tools each job actually needs.
# Rust is installed via dtolnay/rust-toolchain (see helpers.RUST_TOOLCHAIN);
# mise handles everything else. Scoping prevents cargo-backend tools from
# compiling in jobs that don't use them.
MISE_JUST = "just"
MISE_JUST_DENY = "just cargo:cargo-deny"
MISE_JUST_NEXTEST = "just cargo:cargo-nextest"
MISE_JUST_QUINT = "just node npm:@informalsystems/quint"
# `unit-tests` runs `cargo nextest run --all`, which picks up
# frogdb-cluster's quint_conformance test binary (see that job's comment) —
# it needs both cargo-nextest and the quint CLI quint-connect shells out to.
# python/uv are for the `uv run --script` shebang on scripts/spec-lint.py and
# scripts/tests/test_spec_lint.py, which the job's `just lint-spec` step runs.
MISE_UNIT_TESTS = "python uv just cargo:cargo-nextest node npm:@informalsystems/quint"
MISE_PYTHON_WORKFLOW_GEN = "python uv just"
MISE_PYTHON_LINT = "python uv ruff"
MISE_HELM = "helm"


def _touched_env(crate: str) -> str:
    """The env var one crate's `paths-filter` verdict is bound to."""
    return "LOCKED_" + crate.upper().replace("-", "_")


def locked_crate_filters(crate_paths: dict[str, str]) -> str:
    """One `paths-filter` entry per locked crate, generated from the manifest.

    A `locked-<crate>` filter over the crate's own directory is what keeps the
    mutation job's matrix honest: an unrelated hunk elsewhere in the workspace
    spawns no leg, and a crate entering or leaving the perimeter rewrites this
    block on the next `just workflow-gen`.
    """
    return "".join(f"locked-{crate}:\n  - '{path}/**'\n" for crate, path in crate_paths.items())


def locked_crates_touched_step(crate_paths: dict[str, str]) -> Step:
    """Reduce the `locked-<crate>` filter verdicts to the `mutants-diff` matrix.

    Emits a JSON array of the locked crates this change touched (`[]` when it
    touched none), generated from the same manifest list as the filters it
    reads so the two cannot drift apart.
    """
    checks = [
        f'if [ "${_touched_env(crate)}" = "true" ]; then crates+=({crate}); fi'
        for crate in crate_paths
    ]
    return Step(
        id="mutants-matrix",
        name="Reduce locked-crate filters to a matrix",
        env=omap(
            **{
                _touched_env(crate): f"${{{{ steps.filter.outputs['locked-{crate}'] }}}}"
                for crate in crate_paths
            }
        ),
        run=script(
            "\n".join(
                [
                    "set -euo pipefail",
                    "crates=()",
                    *checks,
                    # `${a[@]+"${a[@]}"}` keeps an empty array from tripping `set -u`;
                    # jq then renders zero crates as `[]`, which the job's `if:` reads.
                    "matrix=$(jq -cn '$ARGS.positional' --args ${crates[@]+\"${crates[@]}\"})",
                    'echo "crates=$matrix" >> "$GITHUB_OUTPUT"',
                    'echo "locked crates touched: $matrix"',
                    "",
                ]
            )
        ),
    )


def mutants_diff_job() -> Job:
    """Mutate one locked crate's share of the diff; fail on a missed mutant.

    The post-lock ratchet (`just mutants-diff`) used to run only when an agent
    remembered to. One matrix leg per touched locked crate runs it here, so a
    change inside the perimeter that adds a branch without a forcing test turns
    the run red — on a PR before it merges, on a push to main after it lands.

    The area's mutation gate is deliberately *not* applied to a diff: on a
    denominator of a handful of mutants a ratio is arbitrary, and lenient
    exactly when the diff is large. Zero missed is the criterion; the full-area
    score is re-measured by its own scheduled run.
    """
    return Job(
        name="Mutation Ratchet (locked crates)",
        runs_on=RUNS_ON,
        needs="changes",
        if_="needs.changes.outputs.mutants_matrix != '[]'",
        timeout_minutes=90,
        strategy=Strategy(
            matrix=MatrixExpr(
                dimension="crate",
                expression="${{ fromJSON(needs.changes.outputs.mutants_matrix) }}",
            ),
            fail_fast=False,
        ),
        # Per crate. On a PR the group is the ref, so a rapid re-push cancels
        # only the legs it supersedes. On a push to main it is the sha: every
        # push shares `refs/heads/main`, and a ref-keyed group would let push
        # N+1 cancel push N's leg while N+1's base (`event.before`) starts at
        # N's head — N's diff would never get a verdict (D4).
        concurrency=Concurrency(
            group=(
                "mutants-diff-"
                "${{ github.event_name == 'pull_request' && github.ref || github.sha }}"
                "-${{ matrix.crate }}"
            ),
            cancel_in_progress=True,
        ),
        steps=[
            # Full history: the PR base is a merge-base, and the push base is a
            # commit a shallow clone would not contain.
            checkout_step(fetch_depth="0"),
            mise_setup_step(install_args=MISE_JUST_MUTANTS),
            rust_toolchain_step(),
            libclang_step(),
            # Not the `stable` key the compiling jobs share: cargo-mutants copies
            # the tree to a temp dir and builds *there*, so this job's `./target`
            # is empty at save time. rust-cache saves after every run and a
            # `shared-key` omits the job id, so a short `mutants-diff` leg would
            # otherwise beat `unit-tests` to the save after a Cargo.lock or
            # toolchain change and leave every later `stable` job restoring
            # nothing. Own key, same one `mutants_weekly.py` uses.
            cargo_cache_step(shared_key="mutants"),
            Step(
                id="base",
                name="Resolve the diff base",
                env=omap(
                    EVENT_NAME="${{ github.event_name }}",
                    BEFORE="${{ github.event.before }}",
                ),
                # `-e` matters here: an unresolvable base must fail the job, not
                # write an empty `sha=` that the next step reads as its own
                # first argument (`just mutants-diff <crate> --jobs 2`).
                run=script("""\
                    set -euo pipefail
                    if [ "${EVENT_NAME}" = "pull_request" ]; then
                      sha=$(git merge-base origin/main HEAD)
                    else
                      if [ -z "${BEFORE}" ] || [ "${BEFORE}" = "0000000000000000000000000000000000000000" ]; then
                        echo "skip=true" >> "$GITHUB_OUTPUT"
                        echo "This push created the branch (no before-SHA); nothing to diff against."
                        exit 0
                      fi
                      if ! git cat-file -e "${BEFORE}^{commit}" 2>/dev/null; then
                        echo "skip=true" >> "$GITHUB_OUTPUT"
                        echo "before-SHA ${BEFORE} is unreachable (force push); nothing to diff against."
                        exit 0
                      fi
                      sha="${BEFORE}"
                    fi
                    [ -n "${sha}" ] || { echo "mutants-diff: could not resolve a base commit"; exit 1; }
                    echo "sha=${sha}" >> "$GITHUB_OUTPUT"
                    """),
            ),
            Step(
                name="Mutate the diff",
                if_="steps.base.outputs.skip != 'true'",
                env=omap(
                    CRATE="${{ matrix.crate }}",
                    BASE="${{ steps.base.outputs.sha }}",
                ),
                run='just mutants-diff "${CRATE}" "${BASE}" --jobs 2',
            ),
            Step(
                name="Summarize the run",
                # `steps.base.outcome`: without it a setup step failing leaves
                # `skip` empty and this reports "no mutants" for a job that
                # never mutated anything.
                if_="always() && steps.base.outcome == 'success' && steps.base.outputs.skip != 'true'",
                env=omap(
                    CRATE="${{ matrix.crate }}",
                    BASE="${{ steps.base.outputs.sha }}",
                ),
                run=script("""\
                    set -uo pipefail
                    out="target/mutants/${CRATE}-diff/mutants.out"
                    count() { if [ -f "${out}/$1.txt" ]; then grep -c '' "${out}/$1.txt"; else echo 0; fi; }
                    caught=$(count caught)
                    missed=$(count missed)
                    unviable=$(count unviable)
                    timeout=$(count timeout)
                    total=$((caught + missed + unviable + timeout))
                    {
                      echo "### mutants-diff: ${CRATE}"
                      echo
                      if [ ! -d "${out}" ]; then
                        # The recipe exits 0 before mutating when the crate-scoped
                        # patch is empty, so no mutants.out exists — a different
                        # outcome from "mutated, found nothing to mutate".
                        echo "No changes under \\`${CRATE}\\` since \\`${BASE}\\`; nothing to mutate."
                      elif [ "${total}" -eq 0 ]; then
                        echo "No mutants in this crate's share of the diff."
                      else
                        echo "${total} total, ${caught} caught, ${missed} missed, ${unviable} unviable, ${timeout} timeout"
                        if [ "${missed}" -gt 0 ]; then
                          echo
                          echo "Missed mutants — each needs a forcing test or a documented-equivalent exclusion:"
                          echo
                          sed 's/^/- /' "${out}/missed.txt"
                        fi
                        if [ $((timeout * 100)) -gt $((total * 5)) ]; then
                          echo
                          echo "> Warning: ${timeout} of ${total} mutants timed out (over 5%) — the counts above understate the crate's exposure; raise \\`timeout_multiplier\\` in .cargo/mutants.toml."
                        fi
                      fi
                    } >> "$GITHUB_STEP_SUMMARY"
                    """),
            ),
        ],
    )


def test_workflow() -> Workflow:
    w = Workflow(
        name="Test",
        on=Trigger(
            push=PushTrigger(branches=["main"]),
            pull_request=PullRequestTrigger(branches=["main"]),
        ),
        env=omap(CARGO_TERM_COLOR="always"),
    )

    # The locked-areas manifest, read once and passed down: `locked_areas()`
    # re-reads and re-validates `specs/*.md` on every call.
    specs = locked_areas()
    crate_paths = locked_crate_paths(specs)

    w.job(
        "changes",
        Job(
            name="Detect Changes",
            runs_on=RUNS_ON,
            outputs=omap(
                rust="${{ steps.filter.outputs.rust }}",
                operator="${{ steps.filter.outputs.operator }}",
                operator_config="${{ steps.filter.outputs.operator_config }}",
                workflows="${{ steps.filter.outputs.workflows }}",
                grafana="${{ steps.filter.outputs.grafana }}",
                helm="${{ steps.filter.outputs.helm }}",
                python="${{ steps.filter.outputs.python }}",
                workflow_gen="${{ steps.filter.outputs.workflow_gen }}",
                website="${{ steps.filter.outputs.website }}",
                specs="${{ steps.filter.outputs.specs }}",
                quint="${{ steps.filter.outputs.quint }}",
                testing="${{ steps.filter.outputs.testing }}",
                mutants_matrix="${{ steps.mutants-matrix.outputs.crates }}",
            ),
            steps=[
                checkout_step(),
                Step(
                    id="filter",
                    name="Check changed paths",
                    uses=PATHS_FILTER,
                    with_=omap(
                        # dorny/paths-filter infers a base automatically for push/
                        # pull_request events, but workflow_dispatch has no event-implied
                        # base commit — without this it errors out. `main` matches what
                        # push/pull_request compared against anyway, since this workflow
                        # only ever ran on that branch.
                        base="main",
                        filters=script(
                            dedent("""\
                            rust:
                              - 'frogdb-server/**'
                              - 'frogctl/**'
                              - 'Cargo.toml'
                              - 'Cargo.lock'
                              - 'rust-toolchain.toml'
                              - '.mise.toml'
                              - '.cargo/**'
                              - '.config/nextest.toml'
                            operator:
                              - 'frogdb-operator/**'
                              - 'rust-toolchain.toml'
                              - '.mise.toml'
                            operator_config:
                              - 'frogdb-server/crates/config/**'
                              - 'frogdb-server/crates/config-derive/**'
                              - 'frogdb-operator/Cargo.lock'
                            workflows:
                              - '.github/**'
                            grafana:
                              - 'frogdb-server/ops/grafana/**'
                            helm:
                              - 'frogdb-server/ops/deploy/helm/**'
                            python:
                              - '**/*.py'
                            workflow_gen:
                              - '.github/workflows/workflow_gen/**'
                              - 'Justfile'
                              - '.mise.toml'
                              - 'rust-toolchain.toml'
                            website:
                              - 'website/**'
                            specs:
                              - 'specs/**'
                              - 'website/scripts/spec-gen.py'
                            quint:
                              - 'specs/**'
                              - 'Justfile'
                              - '.mise.toml'
                              - 'scripts/quint-*.sh'
                            testing:
                              - 'testing/**'
                            """)
                            + locked_crate_filters(crate_paths)
                        ),
                    ),
                ),
                locked_crates_touched_step(crate_paths),
            ],
        ),
    )

    actionlint = w.job(
        "actionlint",
        Job(
            name="Actionlint",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.workflows == 'true'",
            steps=[
                checkout_step(),
                Step(name="Run actionlint", uses=ACTIONLINT),
            ],
        ),
    )

    lint = w.job(
        "lint",
        Job(
            name="Lint",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_DENY),
                rust_toolchain_step(components="rustfmt, clippy"),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check toolchain pins are consistent",
                    run="just sync-toolchain-check",
                ),
                run_step(name="Check formatting", run="cargo fmt --all -- --check"),
                run_step(
                    name="Run clippy",
                    run="cargo clippy --all-targets -- -D warnings",
                ),
                run_step(
                    name="Check licenses and advisories",
                    run=f"cargo deny check --config {ensure_path('frogdb-server/deny.toml')}",
                ),
            ],
        ),
    )

    # The compile-free seam-lint family (agents/seam-lints.md):
    # `just lint-gates` runs every `lint-*` gate except `lint-spec`
    # (builds test binaries) and the turmoil lints — grep/regex checks with no
    # compile step, so this job needs no Rust toolchain, just `just` (for the
    # recipe) and `uv` (the clock-seam gate is a PEP-723 script). Kept as its
    # own job, separate from `lint`, so a seam violation is visible without
    # waiting on clippy to compile the whole workspace.
    #
    # `just test-spec-lint` rides along here rather than getting its own job:
    # it is the fixture suite for `scripts/spec-lint.py` (a `uv run --script`,
    # no compile step either) and was previously never run in CI at all. Gated
    # on `python` and `specs` in addition to `rust`, since either a spec-lint
    # change or a spec content change can flip the fixtures without touching
    # anything the `rust` filter would catch.
    seam_gates = w.job(
        "seam-gates",
        Job(
            name="Seam Lint Gates",
            runs_on=RUNS_ON,
            needs="changes",
            if_=(
                "needs.changes.outputs.rust == 'true' || "
                "needs.changes.outputs.python == 'true' || "
                "needs.changes.outputs.specs == 'true'"
            ),
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Run compile-free seam-lint gates",
                    run="just lint-gates",
                ),
                run_step(
                    name="Run spec-lint fixture suite",
                    run="just test-spec-lint",
                ),
            ],
        ),
    )

    # PR-lane tier of the Quint design models' verification (design doc
    # .scratch/formal-spec/2026-08-12-formal-state-spec-design.md §3 cadence):
    # typecheck every model plus a cheap sampled `quint run` that actually
    # checks each model's invariants (see the Justfile's `quint-run` recipe
    # docstring — `quint run` defaults `--invariant` to `"true"`, i.e. no
    # check, unless told otherwise). The exhaustive/bounded Apalache tier
    # (`quint verify`) is too slow for per-PR and runs nightly instead
    # (quint_verify.py). `quint-run` also carries the witness-floor gate — the
    # only lane that sees an action unwired from `step` (see specs/quint/README.md).
    # Gated on the dedicated `quint` filter (specs/**, plus Justfile /
    # .mise.toml / scripts/quint-*.sh — a change to any of those can break
    # `just quint-check`/`quint-run` without touching a single .qnt file, and
    # previously never triggered this job at all).
    quint = w.job(
        "quint",
        Job(
            name="Quint Typecheck & Smoke",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.quint == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_QUINT),
                run_step(name="Typecheck Quint models", run="just quint-check"),
                run_step(
                    name="Quint smoke: named tests + bounded invariant-checked run",
                    run="just quint-run",
                ),
            ],
        ),
    )

    # `just lint-spec` (the spec <-> test agreement lint, `lint-gates` excludes it —
    # see the comment above `seam_gates`) rides along here rather than getting its
    # own job: it needs the compiled test binaries `cargo nextest run --all` just
    # built, so it piggybacks on this job's warm `rust-cache` instead of paying for
    # a second compile. Gated on `specs` in addition to `rust` since a spec content
    # change (an `FM-<AREA>-NNN` row added, renamed, or dropped) can break the
    # agreement without touching any Rust source the `rust` filter would catch.
    # `if: '!cancelled()'` on the lint step: `scripts/spec-lint.py` runs its own
    # `cargo nextest list` (compile only, no test execution), so its verdict does
    # not depend on whether `Run unit tests` passed — a flaky test must not mask
    # spec↔test drift. A cancelled run still skips it.
    unit_tests = w.job(
        "unit-tests",
        Job(
            name="Unit Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.specs == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_UNIT_TESTS),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(name="Run unit tests", run="cargo nextest run --all"),
                Step(
                    name="Spec ↔ test agreement",
                    if_="!cancelled()",
                    run="just lint-spec",
                ),
            ],
        ),
    )

    # Compile-only guard for the command-family cargo features (see
    # frogdb-server/crates/commands/Cargo.toml). `unit-tests` runs
    # `cargo nextest run --all`, whose workspace-wide feature unification pulls
    # `cmd-full` in through docs-gen/redis-regression/shard-harness — so neither
    # the reduced core profile nor an individually-selected family is ever built
    # by the rest of CI. Both directions are checked here so a gated family (or
    # a core-profile-only build) cannot rot unnoticed.
    cmd_full_build = w.job(
        "cmd-full-build",
        Job(
            name="Command Feature Profiles Build",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check commands crate (core profile only)",
                    run="cargo check -p frogdb-commands --no-default-features"
                    " --features core-profile --all-targets",
                ),
                run_step(
                    name="Check commands crate (full command surface)",
                    run="cargo check -p frogdb-commands --features full --all-targets",
                ),
                run_step(
                    name="Check server (core profile only)",
                    run="cargo check -p frogdb-server --all-targets",
                ),
                run_step(
                    name="Check server (full command surface)",
                    run="cargo check -p frogdb-server --features cmd-full --all-targets",
                ),
            ],
        ),
    )

    mutants_diff = w.job("mutants-diff", mutants_diff_job())

    # Coverage tracking lives entirely in the dedicated nightly workflow
    # (coverage_nightly.py -> coverage-nightly.yml, issue 59): a scheduled,
    # non-PR-gating job so a red/slow coverage run never blocks a merge. See that
    # module's docstring for the design and the audit's 84.0% baseline.

    shuttle_tests = w.job(
        "shuttle-tests",
        Job(
            name="Shuttle Concurrency Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="shuttle"),
                run_step(
                    name="Run Shuttle concurrency tests",
                    run="cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'",
                ),
            ],
        ),
    )

    # Mirrors the turmoil-featured lines of `just concurrency` (the shuttle line
    # is covered separately by `shuttle-tests` above): simulation tests plus the
    # ~20-seed-per-profile generated-workload sweeps (short workloads +
    # TxHeavy) that are the per-PR tier of the concurrency-invariant-testing
    # design (`.scratch/concurrency-testing/`).
    # The nightly tier (1000+ seeds, all profiles, longer histories) lives in
    # concurrency-nightly.yml instead — too slow for a per-PR budget.
    turmoil_tests = w.job(
        "turmoil-tests",
        Job(
            name="Turmoil Simulation + Generated-Workload Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="turmoil"),
                run_step(
                    name="Run Turmoil simulation tests",
                    run="cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'",
                ),
                # Filter the whole `concurrency_workload` module rather than its
                # `seed_sweep_*` entry points individually: `mod regressions`'s
                # pinned reproducers live in the same file and were silently
                # never executed under the narrower filters.
                run_step(
                    name="Run generated-workload tests (seed sweeps + pinned regressions)",
                    run="cargo nextest run -p frogdb-server --features turmoil"
                    " -E 'test(/concurrency_workload/)'",
                ),
            ],
        ),
    )

    # The operator is a separate cargo workspace with its own lockfile and no
    # coverage under `cargo nextest run --all`, so ordinary server changes
    # (rust filter) don't need to re-run it. The one real coupling (ADR-0001)
    # is that frogdb-operator imports the frogdb-config crate — only changes
    # to that config schema (or the operator's own lockfile, which pins it)
    # can cause config-generation drift, hence the narrower
    # `operator || operator_config` gate instead of the broad `rust` one.
    operator_tests = w.job(
        "operator-tests",
        Job(
            name="Operator Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_=(
                "needs.changes.outputs.operator == 'true' || "
                "needs.changes.outputs.operator_config == 'true'"
            ),
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="operator"),
                run_step(name="Run operator tests", run="just operator-test"),
            ],
        ),
    )

    helm_gen_check = w.job(
        "helm-gen-check",
        Job(
            name="Helm Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check Helm files are up to date",
                    run=f"cargo run -p helm-gen -- -o {ensure_path('frogdb-server/ops/deploy/helm/frogdb')} --check",
                ),
            ],
        ),
    )

    dashboard_gen_check = w.job(
        "dashboard-gen-check",
        Job(
            name="Dashboard Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check Grafana dashboard is up to date",
                    run=f"cargo run -p dashboard-gen -- -o {ensure_path('frogdb-server/ops/grafana/frogdb-overview.json')} --check",
                ),
            ],
        ),
    )

    dashboard_lint = w.job(
        "dashboard-lint",
        Job(
            name="Grafana Dashboard Lint",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.grafana == 'true'",
            steps=[
                checkout_step(),
                Step(
                    name="Set up Go",
                    uses=SETUP_GO,
                    with_=omap(**{"go-version": "stable"}, cache=SQ("false")),
                ),
                Step(
                    name="Cache dashboard-linter",
                    uses=CACHE,
                    with_=omap(
                        path="~/go/bin/dashboard-linter", key="dashboard-linter-${{ runner.os }}"
                    ),
                ),
                run_step(
                    name="Install dashboard-linter",
                    # `go install .../dashboard-linter@latest` fails as of v0.1.0-v0.2.0:
                    # its go.mod carries a `replace` directive (a memberlist fork pin), and
                    # `go install` refuses to build a module outside its own tree when the
                    # target module's go.mod has replace directives. Cloning the tag and
                    # building it as the main module sidesteps that restriction — the
                    # replace directive is honored as intended in that context. Pinned to
                    # v0.2.0 (latest tag) for reproducibility.
                    run=script(
                        """\
                        if [ -x ~/go/bin/dashboard-linter ]; then
                          exit 0
                        fi
                        git clone --depth 1 -b v0.2.0 https://github.com/grafana/dashboard-linter.git /tmp/dashboard-linter-src
                        mkdir -p ~/go/bin
                        go build -C /tmp/dashboard-linter-src -o ~/go/bin/dashboard-linter .
                        """
                    ),
                ),
                run_step(
                    name="Lint Grafana dashboard",
                    run=f"dashboard-linter lint --strict {ensure_path('frogdb-server/ops/grafana/frogdb-overview.json')}",
                ),
            ],
        ),
    )

    docs_gen_check = w.job(
        "docs-gen-check",
        Job(
            name="Docs Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            # Also gated on website changes: docs-gen's output (commands.json,
            # config-reference.json, ...) lives under website/src/data, so a
            # hand-edit there without regenerating must fail CI too.
            if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.website == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check docs config reference is up to date",
                    run="cargo run -p docs-gen -- --check",
                ),
            ],
        ),
    )

    compat_gen_check = w.job(
        "compat-gen-check",
        Job(
            name="Compat Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            # Also gated on website changes: compat-gen's output
            # (compat-exclusions.json) lives under website/src/data, so a
            # hand-edit there without regenerating must fail CI too.
            if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.website == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Check compatibility data is up to date",
                    run="just compat-gen-check",
                ),
            ],
        ),
    )

    spec_gen_check = w.job(
        "spec-gen-check",
        Job(
            name="Spec Docs Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            # The Specifications section under website/src/content/docs is
            # generated from specs/*.md, so both directions must fail CI: a
            # spec edited without `just spec-gen`, and a hand-edit of the
            # generated pages.
            if_="needs.changes.outputs.specs == 'true' || needs.changes.outputs.website == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Check generated spec pages are up to date",
                    run="just spec-gen-check",
                ),
            ],
        ),
    )

    matrix_gen_check = w.job(
        "matrix-gen-check",
        Job(
            name="Command Matrix Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            # `just matrix-gen-check` runs docs-gen-check and compat-gen-check
            # first (Justfile dependency), then joins their output with the
            # vendored Redis command list — needs Rust (docs-gen) and
            # Python/uv (compat-gen.py, matrix-gen.py) in the same job.
            if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.website == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(
                    name="Check command matrix is up to date",
                    run="just matrix-gen-check",
                ),
            ],
        ),
    )

    command_metadata_gen_check = w.job(
        "command-metadata-gen-check",
        Job(
            name="Command Metadata Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            # Pure Python — reads the vendored JSON under website/src/data and
            # compares against the checked-in Rust module under frogdb-server,
            # so a hand-edit of either side must fail CI.
            if_=(
                "needs.changes.outputs.rust == 'true' || "
                "needs.changes.outputs.website == 'true' || "
                "needs.changes.outputs.python == 'true'"
            ),
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Check generated upstream metadata is up to date",
                    run="just command-metadata-gen-check",
                ),
            ],
        ),
    )

    docs_path_check = w.job(
        "docs-path-check",
        Job(
            name="Docs Path Check",
            runs_on=RUNS_ON,
            needs="changes",
            # Pure Python — no Rust build needed. Triggers on any change to a
            # tree the docs reference by path (frogdb-server/frogctl via
            # `rust`, frogdb-operator, testing/, and the docs themselves).
            if_=(
                "needs.changes.outputs.rust == 'true' || "
                "needs.changes.outputs.operator == 'true' || "
                "needs.changes.outputs.testing == 'true' || "
                "needs.changes.outputs.website == 'true'"
            ),
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Check documentation code paths exist",
                    run="just docs-path-check",
                ),
            ],
        ),
    )

    workflow_gen_check = w.job(
        "workflow-gen-check",
        Job(
            name="Workflow Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.workflow_gen == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_WORKFLOW_GEN),
                run_step(
                    name="Check workflow files are up to date",
                    run="just workflow-gen --check",
                ),
            ],
        ),
    )

    python_lint = w.job(
        "python-lint",
        Job(
            name="Python Lint & Format",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.python == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_PYTHON_LINT),
                run_step(name="Run ruff linter", run="ruff check"),
                run_step(name="Check ruff formatting", run="ruff format --check"),
            ],
        ),
    )

    helm_lint = w.job(
        "helm-lint",
        Job(
            name="Helm Lint",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.helm == 'true'",
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_HELM),
                run_step(
                    name="Lint Helm chart",
                    run=f"helm lint {ensure_path('frogdb-server/ops/deploy/helm/frogdb')}",
                ),
                run_step(
                    name="Template Helm chart",
                    run=f"helm template frogdb {ensure_path('frogdb-server/ops/deploy/helm/frogdb')} --debug",
                ),
                run_step(
                    name="Template cluster preset",
                    run=f"helm template frogdb {ensure_path('frogdb-server/ops/deploy/helm/frogdb')}"
                    f" -f {ensure_path('frogdb-server/ops/deploy/helm/frogdb/values-cluster.yaml')} --debug",
                ),
            ],
        ),
    )

    w.job(
        "ci-pass",
        Job(
            name="CI Pass",
            runs_on=RUNS_ON,
            needs=[
                actionlint,
                lint,
                seam_gates,
                quint,
                unit_tests,
                cmd_full_build,
                mutants_diff,
                shuttle_tests,
                turmoil_tests,
                operator_tests,
                helm_gen_check,
                dashboard_gen_check,
                dashboard_lint,
                docs_gen_check,
                compat_gen_check,
                spec_gen_check,
                matrix_gen_check,
                command_metadata_gen_check,
                docs_path_check,
                workflow_gen_check,
                python_lint,
                helm_lint,
            ],
            if_="always()",
            steps=[
                Step(
                    name="Check results",
                    run="exit 1",
                    if_="contains(needs.*.result, 'failure') || contains(needs.*.result, 'cancelled')",
                ),
            ],
        ),
    )

    return w
