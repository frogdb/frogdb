"""Test workflow definition."""

from workflow_gen.constants import (
    ACTIONLINT,
    CODECOV,
    INSTALL_LLVM_COV,
    INSTALL_NEXTEST,
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
    rust_nightly_toolchain_step,
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

    w.jobs["actionlint"] = Job(
        name="Actionlint",
        steps=[
            checkout_step(),
            Step(name="Run actionlint", uses=ACTIONLINT),
        ],
    )

    w.jobs["lint"] = Job(
        name="Lint",
        steps=[
            checkout_step(),
            rust_toolchain_step(components="rustfmt, clippy"),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(name="Check formatting", run="cargo fmt --all -- --check"),
            run_step(
                name="Run clippy",
                run="cargo clippy --all-targets --all-features -- -D warnings",
            ),
        ],
    )

    w.jobs["unit-tests"] = Job(
        name="Unit Tests",
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
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="codegen"),
            run_step(
                name="Check Helm files are up to date",
                run=f"cargo run -p helm-gen -- -o {ensure_path('frogdb-server/ops/deploy/helm/frogdb')} --check",
            ),
        ],
    )

    w.jobs["dashboard-gen-check"] = Job(
        name="Dashboard Generation Check",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="codegen"),
            run_step(
                name="Check Grafana dashboard is up to date",
                run=f"cargo run -p dashboard-gen -- -o {ensure_path('frogdb-server/ops/grafana/frogdb-overview.json')} --check",
            ),
        ],
    )

    w.jobs["dashboard-lint"] = Job(
        name="Grafana Dashboard Lint",
        steps=[
            checkout_step(),
            Step(name="Set up Go", uses=SETUP_GO),
            run_step(
                name="Install dashboard-linter",
                run="go install github.com/grafana/dashboard-linter@latest",
            ),
            run_step(
                name="Lint Grafana dashboard",
                run=f"dashboard-linter lint --strict {ensure_path('frogdb-server/ops/grafana/frogdb-overview.json')}",
            ),
        ],
    )

    w.jobs["docs-gen-check"] = Job(
        name="Docs Generation Check",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="codegen"),
            run_step(
                name="Check docs config reference is up to date",
                run="cargo run -p docs-gen -- --check",
            ),
        ],
    )

    w.jobs["workflow-gen-check"] = Job(
        name="Workflow Generation Check",
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
        steps=[
            checkout_step(),
            Step(name="Install uv", uses=SETUP_UV),
            run_step(name="Run ruff linter", run="uvx ruff check"),
            run_step(name="Check ruff formatting", run="uvx ruff format --check"),
        ],
    )

    w.jobs["fuzz-regression"] = Job(
        name="Fuzz Regression",
        steps=[
            checkout_step(),
            rust_nightly_toolchain_step(),
            libclang_step(),
            run_step(name="Install cargo-fuzz", run="cargo install cargo-fuzz"),
            cargo_cache_step(shared_key="fuzz"),
            run_step(
                name="Run fuzz targets",
                run=script("""\
                    targets=$(cargo +nightly fuzz list --fuzz-dir testing/fuzz 2>/dev/null)
                    for target in $targets; do
                      echo "=== Fuzzing $target for 10s ==="
                      cargo +nightly fuzz run "$target" --fuzz-dir testing/fuzz -- -max_total_time=10
                    done
                """),
            ),
        ],
    )

    w.jobs["helm-lint"] = Job(
        name="Helm Lint",
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
