"""Test workflow definition."""

from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    ACTIONLINT,
    CACHE,
    CODECOV,
    INSTALL_CARGO_DENY,
    INSTALL_LLVM_COV,
    INSTALL_NEXTEST,
    PATHS_FILTER,
    SETUP_GO,
    SETUP_JUST,
    SETUP_UV,
)
from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    ensure_path,
    libclang_step,
    omap,
    run_step,
    rust_toolchain_step,
    script,
    self_hosted_env_step,
    setup_helm_step,
)
from workflow_gen.schema import Job, PullRequestTrigger, PushTrigger, Step, Trigger, Workflow

# Feature flags — set to True to include the job in the generated workflow
COVERAGE_ENABLED = False

# Runner label — all test jobs run on this runner type
RUNS_ON = "self-hosted"


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
                workflows="${{ steps.filter.outputs.workflows }}",
                grafana="${{ steps.filter.outputs.grafana }}",
                helm="${{ steps.filter.outputs.helm }}",
                python="${{ steps.filter.outputs.python }}",
                workflow_gen="${{ steps.filter.outputs.workflow_gen }}",
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
                              - '.cargo/**'
                              - '.config/nextest.toml'
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
                rust_toolchain_step(components="rustfmt, clippy"),
                libclang_step(),
                cargo_cache_step(shared_key="stable"),
                run_step(name="Check formatting", run="cargo fmt --all -- --check"),
                run_step(
                    name="Run clippy",
                    run="cargo clippy --all-targets -- -D warnings",
                ),
                Step(name="Install cargo-deny", uses=INSTALL_CARGO_DENY),
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
                rust_toolchain_step(),
                libclang_step(),
                Step(name="Install nextest", uses=INSTALL_NEXTEST),
                cargo_cache_step(shared_key="stable"),
                run_step(name="Run unit tests", run="cargo nextest run --all"),
            ],
        ),
    )

    if COVERAGE_ENABLED:
        w.job(
            "coverage",
            Job(
                name="Code Coverage",
                runs_on=RUNS_ON,
                steps=[
                    checkout_step(),
                    self_hosted_env_step(),
                    rust_toolchain_step(components="llvm-tools-preview"),
                    libclang_step(),
                    Step(name="Install nextest", uses=INSTALL_NEXTEST),
                    Step(name="Install cargo-llvm-cov", uses=INSTALL_LLVM_COV),
                    cargo_cache_step(shared_key="coverage"),
                    run_step(
                        name="Generate coverage data",
                        run="cargo llvm-cov nextest --all --lcov --output-path lcov.info",
                    ),
                    Step(
                        name="Upload to Codecov",
                        uses=CODECOV,
                        with_=omap(files="lcov.info", token="${{ secrets.CODECOV_TOKEN }}"),
                    ),
                ],
            ),
        )

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
                rust_toolchain_step(),
                libclang_step(),
                Step(name="Install nextest", uses=INSTALL_NEXTEST),
                cargo_cache_step(shared_key="shuttle"),
                run_step(
                    name="Run Shuttle concurrency tests",
                    run="cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'",
                ),
            ],
        ),
    )

    turmoil_tests = w.job(
        "turmoil-tests",
        Job(
            name="Turmoil Simulation Tests",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                self_hosted_env_step(),
                rust_toolchain_step(),
                libclang_step(),
                Step(name="Install nextest", uses=INSTALL_NEXTEST),
                cargo_cache_step(shared_key="turmoil"),
                run_step(
                    name="Run Turmoil simulation tests",
                    run="cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'",
                ),
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
            if_="needs.changes.outputs.rust == 'true'",
            steps=[
                checkout_step(),
                self_hosted_env_step(),
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

    workflow_gen_check = w.job(
        "workflow-gen-check",
        Job(
            name="Workflow Generation Check",
            runs_on=RUNS_ON,
            needs="changes",
            if_="needs.changes.outputs.workflow_gen == 'true'",
            steps=[
                checkout_step(),
                Step(name="Install uv", uses=SETUP_UV),
                Step(name="Install just", uses=SETUP_JUST),
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
                Step(name="Install uv", uses=SETUP_UV),
                run_step(name="Run ruff linter", run="uvx ruff check"),
                run_step(name="Check ruff formatting", run="uvx ruff format --check"),
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
                setup_helm_step(),
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
                helm_gen_check,
                dashboard_gen_check,
                dashboard_lint,
                docs_gen_check,
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
