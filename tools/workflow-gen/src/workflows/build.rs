//! Build workflow definition (build.yml).

use gh_workflow::{Event, Expression, Job, Level, Permissions, PullRequest, Push, Step, Workflow};

use crate::helpers::{
    cargo_cache_matrix, checkout, docker_build_push_with_cache, docker_login_ghcr, docker_metadata,
    download_artifact, install_cargo_zigbuild, linux_build_matrix, rust_toolchain_with_target,
    setup_buildx, setup_qemu, setup_zig, upload_artifact,
};

/// Creates the build workflow.
pub fn build_workflow() -> Workflow {
    Workflow::new("Build")
        .on(
            Event::default()
                .push(Push::default().add_branch("main"))
                .pull_request(PullRequest::default().add_branch("main")),
        )
        .add_env(("CARGO_TERM_COLOR", "always"))
        .add_env(("REGISTRY", "ghcr.io"))
        .add_env(("IMAGE_NAME", "${{ github.repository }}"))
        .add_job("build", build_job())
        .add_job("docker", docker_job())
}

/// Binary build job with matrix strategy.
fn build_job() -> Job {
    Job::new("Build (${{ matrix.target }})")
        .runs_on("${{ matrix.os }}")
        .strategy(linux_build_matrix())
        .add_step(checkout())
        .add_step(rust_toolchain_with_target("${{ matrix.target }}"))
        .add_step(install_cargo_zigbuild())
        .add_step(setup_zig())
        .add_step(cargo_cache_matrix())
        .add_step(
            Step::new("Build").run(
                "cargo zigbuild --release --target ${{ matrix.target }} --bin frogdb-server",
            ),
        )
        .add_step(upload_artifact(
            "frogdb-server-${{ matrix.arch }}",
            "target/${{ matrix.target }}/release/frogdb-server",
            7,
        ))
}

/// Docker build job.
fn docker_job() -> Job {
    Job::new("Docker Build")
        .add_needs("build")
        .runs_on("ubuntu-latest")
        .cond(Expression::new(
            "github.event_name == 'push' && github.ref == 'refs/heads/main'",
        ))
        .permissions(
            Permissions::default()
                .contents(Level::Read)
                .packages(Level::Write),
        )
        .add_step(checkout())
        .add_step(download_artifact("frogdb-server-amd64", "binaries/amd64"))
        .add_step(download_artifact("frogdb-server-arm64", "binaries/arm64"))
        .add_step(setup_qemu())
        .add_step(setup_buildx())
        .add_step(docker_login_ghcr())
        .add_step(docker_metadata(
            "type=ref,event=branch\ntype=sha,prefix=\ntype=raw,value=latest,enable={{is_default_branch}}",
        ))
        .add_step(docker_build_push_with_cache(
            "./Dockerfile.multiarch",
            "linux/amd64,linux/arm64",
            "BINARY_AMD64=binaries/amd64/frogdb-server\nBINARY_ARM64=binaries/arm64/frogdb-server",
        ))
}
