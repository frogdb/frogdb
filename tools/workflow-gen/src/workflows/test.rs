//! Test workflow definition (test.yml).

use gh_workflow::{Event, Job, PullRequest, Push, Step, Workflow};

use crate::helpers::{
    cargo_cache, checkout, rust_toolchain, rust_toolchain_with_components, setup_helm,
};

/// Creates the test workflow.
pub fn test_workflow() -> Workflow {
    Workflow::new("Test")
        .on(
            Event::default()
                .push(Push::default().add_branch("main"))
                .pull_request(PullRequest::default().add_branch("main")),
        )
        .add_env(("CARGO_TERM_COLOR", "always"))
        .add_job("lint", lint_job())
        .add_job("unit-tests", unit_tests_job())
        .add_job("shuttle-tests", shuttle_tests_job())
        .add_job("turmoil-tests", turmoil_tests_job())
        .add_job("helm-gen-check", helm_gen_check_job())
        .add_job("workflow-gen-check", workflow_gen_check_job())
        .add_job("helm-lint", helm_lint_job())
}

/// Lint job - formatting and clippy.
fn lint_job() -> Job {
    Job::new("Lint")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain_with_components(&["rustfmt", "clippy"]))
        .add_step(
            Step::new("Check formatting")
                .run("cargo fmt --all -- --check"),
        )
        .add_step(
            Step::new("Run clippy")
                .run("cargo clippy --all-targets --all-features -- -D warnings"),
        )
}

/// Unit tests job.
fn unit_tests_job() -> Job {
    Job::new("Unit Tests")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain())
        .add_step(cargo_cache("test"))
        .add_step(Step::new("Run unit tests").run("cargo test --all"))
}

/// Shuttle concurrency tests job.
fn shuttle_tests_job() -> Job {
    Job::new("Shuttle Concurrency Tests")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain())
        .add_step(cargo_cache("shuttle"))
        .add_step(
            Step::new("Run Shuttle concurrency tests")
                .run("cargo test -p frogdb-core --features shuttle --test concurrency"),
        )
}

/// Turmoil simulation tests job.
fn turmoil_tests_job() -> Job {
    Job::new("Turmoil Simulation Tests")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain())
        .add_step(cargo_cache("turmoil"))
        .add_step(
            Step::new("Run Turmoil simulation tests")
                .run("cargo test -p frogdb-server --features turmoil --test simulation"),
        )
}

/// Helm generation check job.
fn helm_gen_check_job() -> Job {
    Job::new("Helm Generation Check")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain())
        .add_step(cargo_cache("helm"))
        .add_step(
            Step::new("Check Helm files are up to date")
                .run("cargo run -p helm-gen -- --check"),
        )
}

/// Workflow generation check job.
fn workflow_gen_check_job() -> Job {
    Job::new("Workflow Generation Check")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(rust_toolchain())
        .add_step(cargo_cache("workflow"))
        .add_step(
            Step::new("Check workflow files are up to date")
                .run("cargo run -p workflow-gen -- --check"),
        )
}

/// Helm lint job.
fn helm_lint_job() -> Job {
    Job::new("Helm Lint")
        .runs_on("ubuntu-latest")
        .add_step(checkout())
        .add_step(setup_helm())
        .add_step(Step::new("Lint Helm chart").run("helm lint deploy/helm/frogdb"))
        .add_step(
            Step::new("Template Helm chart")
                .run("helm template frogdb deploy/helm/frogdb --debug"),
        )
        .add_step(
            Step::new("Template cluster preset")
                .run("helm template frogdb deploy/helm/frogdb -f deploy/helm/frogdb/values-cluster.yaml --debug"),
        )
}
