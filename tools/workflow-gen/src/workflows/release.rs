//! Release workflow definition (release.yml).

use gh_workflow::{Event, Expression, Job, Level, Permissions, Step, Workflow, WorkflowDispatch};

use crate::helpers::{
    checkout, checkout_with_depth, docker_build_push, docker_login_ghcr, docker_metadata,
    download_all_artifacts, download_artifact, gh_release, release_build_matrix,
    rust_toolchain_with_target, setup_buildx, setup_helm, setup_qemu, upload_artifact,
};

/// Creates the release workflow.
pub fn release_workflow() -> Workflow {
    Workflow::new("Release")
        .on(Event::default().workflow_dispatch(WorkflowDispatch::default()))
        .add_env(("CARGO_TERM_COLOR", "always"))
        .add_env(("REGISTRY", "ghcr.io"))
        .add_env(("IMAGE_NAME", "${{ github.repository }}"))
        .add_job("build-binaries", build_binaries_job())
        .add_job("docker", docker_job())
        .add_job("helm", helm_job())
        .add_job("github-release", github_release_job())
}

/// Build release binaries job with matrix strategy.
fn build_binaries_job() -> Job {
    Job::new("Build Release Binaries (${{ matrix.target }})")
        .runs_on("${{ matrix.os }}")
        .strategy(release_build_matrix())
        .add_step(checkout())
        .add_step(rust_toolchain_with_target("${{ matrix.target }}"))
        .add_step(
            Step::new("Install cargo-zigbuild (Linux)")
                .run("cargo install cargo-zigbuild")
                .if_condition(Expression::new("runner.os == 'Linux'")),
        )
        .add_step(
            Step::new("Install Zig (Linux)")
                .uses("goto-bus-stop", "setup-zig", "v2")
                .if_condition(Expression::new("runner.os == 'Linux'"))
                .add_with(("version", "0.11.0")),
        )
        .add_step(
            Step::new("Build (Linux)")
                .run("cargo zigbuild --release --target ${{ matrix.target }} --bin frogdb-server")
                .if_condition(Expression::new("runner.os == 'Linux'")),
        )
        .add_step(
            Step::new("Build (macOS)")
                .run("cargo build --release --target ${{ matrix.target }} --bin frogdb-server")
                .if_condition(Expression::new("runner.os == 'macOS'")),
        )
        .add_step(
            Step::new("Create release archive").run(
                "cd target/${{ matrix.target }}/release\ntar -czvf ../../../frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz frogdb-server${{ matrix.ext }}",
            ),
        )
        .add_step(upload_artifact(
            "frogdb-${{ matrix.target }}",
            "frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz",
            7,
        ))
}

/// Docker release job.
fn docker_job() -> Job {
    Job::new("Docker Release")
        .add_needs("build-binaries")
        .runs_on("ubuntu-latest")
        .permissions(
            Permissions::default()
                .contents(Level::Read)
                .packages(Level::Write),
        )
        .add_step(checkout())
        .add_step(download_artifact(
            "frogdb-x86_64-unknown-linux-gnu",
            "binaries/amd64",
        ))
        .add_step(download_artifact(
            "frogdb-aarch64-unknown-linux-gnu",
            "binaries/arm64",
        ))
        .add_step(
            Step::new("Extract binaries").run(
                "cd binaries/amd64 && tar -xzf *.tar.gz && mv frogdb-server ../amd64-binary\ncd ../arm64 && tar -xzf *.tar.gz && mv frogdb-server ../arm64-binary\nmv binaries/amd64-binary binaries/amd64/frogdb-server\nmv binaries/arm64-binary binaries/arm64/frogdb-server",
            ),
        )
        .add_step(setup_qemu())
        .add_step(setup_buildx())
        .add_step(docker_login_ghcr())
        .add_step(docker_metadata(
            "type=semver,pattern={{version}}\ntype=semver,pattern={{major}}.{{minor}}\ntype=semver,pattern={{major}}",
        ))
        .add_step(docker_build_push(
            "./Dockerfile.multiarch",
            "linux/amd64,linux/arm64",
            "BINARY_AMD64=binaries/amd64/frogdb-server\nBINARY_ARM64=binaries/arm64/frogdb-server",
        ))
}

/// Helm chart publish job.
fn helm_job() -> Job {
    Job::new("Publish Helm Chart")
        .add_needs("docker")
        .runs_on("ubuntu-latest")
        .permissions(
            Permissions::default()
                .contents(Level::Write)
                .pages(Level::Write),
        )
        .add_step(checkout_with_depth(0))
        .add_step(setup_helm())
        .add_step(
            Step::new("Package Helm chart").run(
                "helm package deploy/helm/frogdb --destination .helm-packages\nhelm repo index .helm-packages --url https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/helm",
            ),
        )
        .add_step(
            Step::new("Checkout gh-pages branch")
                .uses("actions", "checkout", "v4")
                .add_with(("ref", "gh-pages"))
                .add_with(("path", "gh-pages")),
        )
        .add_step(
            Step::new("Update Helm repository").run(
                r#"mkdir -p gh-pages/helm
cp .helm-packages/*.tgz gh-pages/helm/
# Merge index files
if [ -f gh-pages/helm/index.yaml ]; then
  helm repo index gh-pages/helm --merge gh-pages/helm/index.yaml --url https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/helm
else
  helm repo index gh-pages/helm --url https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/helm
fi"#,
            ),
        )
        .add_step(
            Step::new("Commit and push").run(
                r#"cd gh-pages
git config user.name "GitHub Actions"
git config user.email "actions@github.com"
git add helm/
git commit -m "Release Helm chart ${{ github.ref_name }}" || echo "No changes"
git push"#,
            ),
        )
}

/// GitHub release job.
fn github_release_job() -> Job {
    Job::new("GitHub Release")
        .add_needs("build-binaries")
        .add_needs("docker")
        .runs_on("ubuntu-latest")
        .permissions(Permissions::default().contents(Level::Write))
        .add_step(checkout())
        .add_step(download_all_artifacts("artifacts"))
        .add_step(gh_release("artifacts/frogdb-*/*.tar.gz"))
}
