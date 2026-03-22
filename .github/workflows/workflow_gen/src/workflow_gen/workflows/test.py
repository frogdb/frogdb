"""Test workflow definition."""

from workflow_gen.constants import ACTIONLINT, INSTALL_NEXTEST, SETUP_JUST, SETUP_UV
from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    omap,
    run_step,
    rust_toolchain_step,
    setup_helm_step,
)
from workflow_gen.schema import Job, Step, Trigger, Workflow


def test_workflow() -> Workflow:
    w = Workflow(
        name="Test",
        on=Trigger(),
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
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(key_suffix="test"),
            run_step(name="Run unit tests", run="cargo nextest run --all"),
        ],
    )

    w.jobs["shuttle-tests"] = Job(
        name="Shuttle Concurrency Tests",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(key_suffix="shuttle"),
            run_step(
                name="Run Shuttle concurrency tests",
                run="cargo nextest run -p frogdb-core --features shuttle --test concurrency",
            ),
        ],
    )

    w.jobs["turmoil-tests"] = Job(
        name="Turmoil Simulation Tests",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            Step(name="Install nextest", uses=INSTALL_NEXTEST),
            cargo_cache_step(key_suffix="turmoil"),
            run_step(
                name="Run Turmoil simulation tests",
                run="cargo nextest run -p frogdb-server --features turmoil --test simulation",
            ),
        ],
    )

    w.jobs["helm-gen-check"] = Job(
        name="Helm Generation Check",
        steps=[
            checkout_step(),
            rust_toolchain_step(),
            cargo_cache_step(key_suffix="helm"),
            run_step(
                name="Check Helm files are up to date",
                run="cargo run -p helm-gen -- --check",
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

    w.jobs["helm-lint"] = Job(
        name="Helm Lint",
        steps=[
            checkout_step(),
            setup_helm_step(),
            run_step(name="Lint Helm chart", run="helm lint frogdb-server/ops/deploy/helm/frogdb"),
            run_step(
                name="Template Helm chart",
                run="helm template frogdb frogdb-server/ops/deploy/helm/frogdb --debug",
            ),
            run_step(
                name="Template cluster preset",
                run="helm template frogdb frogdb-server/ops/deploy/helm/frogdb"
                " -f frogdb-server/ops/deploy/helm/frogdb/values-cluster.yaml --debug",
            ),
        ],
    )

    return w
