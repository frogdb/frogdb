"""Test workflow definition."""

from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    ACTIONLINT,
    CACHE,
    CODECOV,
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
    setup_helm_step,
)
from workflow_gen.schema import Job, PullRequestTrigger, PushTrigger, Step, Trigger, Workflow

# Feature flags — set to True to include the job in the generated workflow
COVERAGE_ENABLED = False


def test_workflow() -> Workflow:
    w = Workflow(
        name="Test",
        on=Trigger(
            push=PushTrigger(branches=["main"]),
            pull_request=PullRequestTrigger(branches=["main"]),
        ),
        env=omap(CARGO_TERM_COLOR="always"),
    )

    w.jobs["changes"] = Job(
        name="Detect Changes",
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
    )

    w.jobs["actionlint"] = Job(
        name="Actionlint",
        needs="changes",
        if_="needs.changes.outputs.workflows == 'true'",
        steps=[
            checkout_step(),
            Step(name="Run actionlint", uses=ACTIONLINT),
        ],
    )

    w.jobs["lint"] = Job(
        name="Lint",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(components="rustfmt, clippy"),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(name="Check formatting", run="cargo fmt --all -- --check"),
            run_step(
                name="Run clippy",
                run="cargo clippy --all-targets -- -D warnings",
            ),
        ],
    )

    w.jobs["unit-tests"] = Job(
        name="Unit Tests",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(shared_key="stable"),
            run_step(name="Run unit tests", run="cargo nextest run --all"),
        ],
    )

    if COVERAGE_ENABLED:
        w.jobs["coverage"] = Job(
            name="Code Coverage",
            steps=[
                checkout_step(),
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
        )

    w.jobs["shuttle-tests"] = Job(
        name="Shuttle Concurrency Tests",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(shared_key="shuttle"),
            run_step(
                name="Run Shuttle concurrency tests",
                run="cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'",
            ),
        ],
    )

    w.jobs["turmoil-tests"] = Job(
        name="Turmoil Simulation Tests",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(shared_key="turmoil"),
            run_step(
                name="Run Turmoil simulation tests",
                run="cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'",
            ),
        ],
    )

    w.jobs["helm-gen-check"] = Job(
        name="Helm Generation Check",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(
                name="Check Helm files are up to date",
                run=f"cargo run -p helm-gen -- -o {ensure_path('frogdb-server/ops/deploy/helm/frogdb')} --check",
            ),
        ],
    )

    w.jobs["dashboard-gen-check"] = Job(
        name="Dashboard Generation Check",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(
                name="Check Grafana dashboard is up to date",
                run=f"cargo run -p dashboard-gen -- -o {ensure_path('frogdb-server/ops/grafana/frogdb-overview.json')} --check",
            ),
        ],
    )

    w.jobs["dashboard-lint"] = Job(
        name="Grafana Dashboard Lint",
        needs="changes",
        if_="needs.changes.outputs.grafana == 'true'",
        steps=[
            checkout_step(),
            Step(name="Set up Go", uses=SETUP_GO, with_=omap(cache=SQ("false"))),
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
    )

    w.jobs["docs-gen-check"] = Job(
        name="Docs Generation Check",
        needs="changes",
        if_="needs.changes.outputs.rust == 'true'",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(
                name="Check docs config reference is up to date",
                run="cargo run -p docs-gen -- --check",
            ),
        ],
    )

    w.jobs["workflow-gen-check"] = Job(
        name="Workflow Generation Check",
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
    )

    w.jobs["python-lint"] = Job(
        name="Python Lint & Format",
        needs="changes",
        if_="needs.changes.outputs.python == 'true'",
        steps=[
            checkout_step(),
            Step(name="Install uv", uses=SETUP_UV),
            run_step(name="Run ruff linter", run="uvx ruff check"),
            run_step(name="Check ruff formatting", run="uvx ruff format --check"),
        ],
    )

    w.jobs["helm-lint"] = Job(
        name="Helm Lint",
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
    )

    return w
