"""Test workflow definition."""

from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    ACTIONLINT,
    CACHE,
    PATHS_FILTER,
    SETUP_GO,
)
from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    ensure_path,
    libclang_step,
    mise_setup_step,
    omap,
    run_step,
    rust_toolchain_step,
    script,
    self_hosted_env_step,
)
from workflow_gen.schema import Job, PullRequestTrigger, PushTrigger, Step, Trigger, Workflow

# Runner label — route to self-hosted only when triggered by a trusted actor.
# For PRs, key on the immutable PR author (survives re-runs); for push/dispatch,
# key on github.actor. Untrusted actors fall back to GitHub-hosted ubuntu-latest.
TRUSTED_ACTOR = "nathanjordan"
RUNS_ON = (
    "${{ (("
    f"github.event_name != 'pull_request' && github.actor == '{TRUSTED_ACTOR}'"
    ") || ("
    f"github.event_name == 'pull_request' && github.event.pull_request.user.login == '{TRUSTED_ACTOR}'"
    ")) && 'self-hosted' || 'ubuntu-latest' }}"
)

# mise install_args per job — only install tools each job actually needs.
# Rust is installed via dtolnay/rust-toolchain (see helpers.RUST_TOOLCHAIN);
# mise handles everything else. Scoping prevents cargo-backend tools from
# compiling in jobs that don't use them.
MISE_JUST = "just"
MISE_JUST_DENY = "just cargo:cargo-deny"
MISE_JUST_NEXTEST = "just cargo:cargo-nextest"
MISE_PYTHON_WORKFLOW_GEN = "python uv just"
MISE_PYTHON_LINT = "python uv ruff"
MISE_HELM = "helm"


def test_workflow() -> Workflow:
    w = Workflow(
        name="Test",
        on=Trigger(
            push=PushTrigger(branches=["main"]),
            pull_request=PullRequestTrigger(branches=["main"]),
        ),
        env=omap(CARGO_TERM_COLOR="always"),
    )

    w.job(
        "changes",
        Job(
            name="Detect Changes",
            runs_on=RUNS_ON,
            outputs=omap(
                rust="${{ steps.filter.outputs.rust }}",
                operator="${{ steps.filter.outputs.operator }}",
                workflows="${{ steps.filter.outputs.workflows }}",
                grafana="${{ steps.filter.outputs.grafana }}",
                helm="${{ steps.filter.outputs.helm }}",
                python="${{ steps.filter.outputs.python }}",
                workflow_gen="${{ steps.filter.outputs.workflow_gen }}",
                website="${{ steps.filter.outputs.website }}",
                testing="${{ steps.filter.outputs.testing }}",
            ),
            steps=[
                checkout_step(),
                Step(
                    id="filter",
                    name="Check changed paths",
                    uses=PATHS_FILTER,
                    with_=omap(
                        filters=script("""\
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
                            testing:
                              - 'testing/**'
                        """),
                    ),
                ),
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
                self_hosted_env_step(),
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

    unit_tests = w.job(
        "unit-tests",
        Job(
            name="Unit Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                self_hosted_env_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(name="Run unit tests", run="cargo nextest run --all"),
            ],
        ),
    )

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
                self_hosted_env_step(),
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
    # design (see the "CI" section of
    # docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md).
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
                self_hosted_env_step(),
                mise_setup_step(install_args=MISE_JUST_NEXTEST),
                rust_toolchain_step(),
                libclang_step(),
                cargo_cache_step(shared_key="turmoil"),
                run_step(
                    name="Run Turmoil simulation tests",
                    run="cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'",
                ),
                run_step(
                    name="Run generated-workload seed sweep (short workloads)",
                    run="cargo nextest run -p frogdb-server --features turmoil"
                    " -E 'test(/seed_sweep_short_workloads/)'",
                ),
                run_step(
                    name="Run generated-workload seed sweep (TxHeavy)",
                    run="cargo nextest run -p frogdb-server --features turmoil"
                    " -E 'test(/seed_sweep_txheavy/)'",
                ),
            ],
        ),
    )

    # The operator is a separate cargo workspace with its own lockfile and no
    # coverage under `cargo nextest run --all`. It depends on frogdb-config, so
    # a server-side schema change (rust filter) must also re-run these tests to
    # catch config-generation drift — hence the `rust || operator` gate.
    operator_tests = w.job(
        "operator-tests",
        Job(
            name="Operator Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.operator == 'true'",
            steps=[
                checkout_step(),
                self_hosted_env_step(),
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
                self_hosted_env_step(),
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
                self_hosted_env_step(),
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
                    run="test -x ~/go/bin/dashboard-linter || go install github.com/grafana/dashboard-linter@latest",
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
                self_hosted_env_step(),
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
                self_hosted_env_step(),
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
                unit_tests,
                shuttle_tests,
                turmoil_tests,
                operator_tests,
                helm_gen_check,
                dashboard_gen_check,
                dashboard_lint,
                docs_gen_check,
                compat_gen_check,
                matrix_gen_check,
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
