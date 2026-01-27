//! Common step builders for GitHub Actions workflows.

use gh_workflow::{Expression, Run, Step, Use};

use super::actions::*;

/// Creates a checkout step.
pub fn checkout() -> Step<Use> {
    Step::new("Checkout").uses("actions", "checkout", CHECKOUT_VERSION)
}

/// Creates a checkout step with fetch-depth.
pub fn checkout_with_depth(depth: u32) -> Step<Use> {
    Step::new("Checkout")
        .uses("actions", "checkout", CHECKOUT_VERSION)
        .add_with(("fetch-depth", depth.to_string()))
}

/// Creates a Rust toolchain step.
pub fn rust_toolchain() -> Step<Use> {
    Step::new("Install Rust toolchain").uses("dtolnay", "rust-action", RUST_TOOLCHAIN_VERSION)
}

/// Creates a Rust toolchain step with components.
pub fn rust_toolchain_with_components(components: &[&str]) -> Step<Use> {
    Step::new("Install Rust toolchain")
        .uses("dtolnay", "rust-action", RUST_TOOLCHAIN_VERSION)
        .add_with(("components", components.join(", ")))
}

/// Creates a Rust toolchain step with target.
pub fn rust_toolchain_with_target(target: &str) -> Step<Use> {
    Step::new("Install Rust toolchain")
        .uses("dtolnay", "rust-action", RUST_TOOLCHAIN_VERSION)
        .add_with(("targets", target.to_string()))
}

/// Creates a cargo cache step.
pub fn cargo_cache(key_suffix: &str) -> Step<Use> {
    Step::new("Cache cargo registry")
        .uses("actions", "cache", CACHE_VERSION)
        .add_with(("path", "~/.cargo/registry\n~/.cargo/git\ntarget"))
        .add_with((
            "key",
            format!(
                "${{{{ runner.os }}}}-cargo-{}-${{{{ hashFiles('**/Cargo.lock') }}}}",
                key_suffix
            ),
        ))
        .add_with((
            "restore-keys",
            format!("${{{{ runner.os }}}}-cargo-{}-\n", key_suffix),
        ))
}

/// Creates a cargo cache step with matrix target.
pub fn cargo_cache_matrix() -> Step<Use> {
    Step::new("Cache cargo registry")
        .uses("actions", "cache", CACHE_VERSION)
        .add_with(("path", "~/.cargo/registry\n~/.cargo/git\ntarget"))
        .add_with((
            "key",
            "${{ runner.os }}-cargo-${{ matrix.target }}-${{ hashFiles('**/Cargo.lock') }}",
        ))
        .add_with((
            "restore-keys",
            "${{ runner.os }}-cargo-${{ matrix.target }}-\n",
        ))
}

/// Creates a setup Helm step.
pub fn setup_helm() -> Step<Use> {
    Step::new("Install Helm")
        .uses("azure", "setup-helm", SETUP_HELM_VERSION)
        .add_with(("version", HELM_VERSION))
}

/// Creates a setup kubectl step.
pub fn setup_kubectl() -> Step<Use> {
    Step::new("Install kubectl").uses("azure", "setup-kubectl", SETUP_KUBECTL_VERSION)
}

/// Creates a setup Zig step.
pub fn setup_zig() -> Step<Use> {
    Step::new("Install Zig")
        .uses("goto-bus-stop", "setup-zig", SETUP_ZIG_VERSION)
        .add_with(("version", ZIG_VERSION))
}

/// Creates a setup Terraform step.
pub fn setup_terraform() -> Step<Use> {
    Step::new("Setup Terraform")
        .uses("hashicorp", "setup-terraform", SETUP_TERRAFORM_VERSION)
        .add_with(("terraform_version", "${{ env.TERRAFORM_VERSION }}"))
}

/// Creates a cargo zigbuild install step.
pub fn install_cargo_zigbuild() -> Step<Run> {
    Step::new("Install cargo-zigbuild").run("cargo install cargo-zigbuild")
}

/// Creates a Docker QEMU setup step.
pub fn setup_qemu() -> Step<Use> {
    Step::new("Set up QEMU").uses("docker", "setup-qemu-action", QEMU_VERSION)
}

/// Creates a Docker Buildx setup step.
pub fn setup_buildx() -> Step<Use> {
    Step::new("Set up Docker Buildx").uses("docker", "setup-buildx-action", BUILDX_VERSION)
}

/// Creates a GHCR login step.
pub fn docker_login_ghcr() -> Step<Use> {
    Step::new("Log in to GitHub Container Registry")
        .uses("docker", "login-action", DOCKER_LOGIN_VERSION)
        .add_with(("registry", "${{ env.REGISTRY }}"))
        .add_with(("username", "${{ github.actor }}"))
        .add_with(("password", "${{ secrets.GITHUB_TOKEN }}"))
}

/// Creates a Docker metadata step.
pub fn docker_metadata(tags: &str) -> Step<Use> {
    Step::new("Extract metadata")
        .uses("docker", "metadata-action", DOCKER_METADATA_VERSION)
        .id("meta")
        .add_with(("images", "${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}"))
        .add_with(("tags", tags))
}

/// Creates a Docker build and push step.
pub fn docker_build_push(dockerfile: &str, platforms: &str, build_args: &str) -> Step<Use> {
    let mut step = Step::new("Build and push multi-arch image")
        .uses("docker", "build-push-action", DOCKER_BUILD_PUSH_VERSION)
        .add_with(("context", "."))
        .add_with(("file", dockerfile))
        .add_with(("platforms", platforms))
        .add_with(("push", "true"))
        .add_with(("tags", "${{ steps.meta.outputs.tags }}"))
        .add_with(("labels", "${{ steps.meta.outputs.labels }}"));

    if !build_args.is_empty() {
        step = step.add_with(("build-args", build_args));
    }

    step
}

/// Creates a Docker build and push step with cache.
pub fn docker_build_push_with_cache(
    dockerfile: &str,
    platforms: &str,
    build_args: &str,
) -> Step<Use> {
    let mut step = docker_build_push(dockerfile, platforms, build_args);
    step = step
        .add_with(("cache-from", "type=gha"))
        .add_with(("cache-to", "type=gha,mode=max"));
    step
}

/// Creates an upload artifact step.
pub fn upload_artifact(name: &str, path: &str, retention_days: u32) -> Step<Use> {
    Step::new(format!("Upload {}", name))
        .uses("actions", "upload-artifact", UPLOAD_ARTIFACT_VERSION)
        .add_with(("name", name))
        .add_with(("path", path))
        .add_with(("retention-days", retention_days.to_string()))
}

/// Creates a download artifact step.
pub fn download_artifact(name: &str, path: &str) -> Step<Use> {
    Step::new(format!("Download {}", name))
        .uses("actions", "download-artifact", DOWNLOAD_ARTIFACT_VERSION)
        .add_with(("name", name))
        .add_with(("path", path))
}

/// Creates a download all artifacts step.
pub fn download_all_artifacts(path: &str) -> Step<Use> {
    Step::new("Download all artifacts")
        .uses("actions", "download-artifact", DOWNLOAD_ARTIFACT_VERSION)
        .add_with(("path", path))
}

/// Creates an AWS credentials configuration step.
pub fn configure_aws_credentials() -> Step<Use> {
    Step::new("Configure AWS credentials")
        .uses("aws-actions", "configure-aws-credentials", AWS_CREDENTIALS_VERSION)
        .if_condition(Expression::new("inputs.cloud == 'aws'"))
        .add_with(("role-to-assume", "${{ secrets.AWS_ROLE_ARN }}"))
        .add_with(("aws-region", "${{ vars.AWS_REGION }}"))
}

/// Creates a GCP authentication step.
pub fn authenticate_gcp() -> Step<Use> {
    Step::new("Authenticate to Google Cloud")
        .uses("google-github-actions", "auth", GCP_AUTH_VERSION)
        .if_condition(Expression::new("inputs.cloud == 'gcp'"))
        .add_with((
            "workload_identity_provider",
            "${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}",
        ))
        .add_with(("service_account", "${{ secrets.GCP_SERVICE_ACCOUNT }}"))
}

/// Creates an Azure login step.
pub fn azure_login() -> Step<Use> {
    Step::new("Azure Login")
        .uses("azure", "login", AZURE_LOGIN_VERSION)
        .if_condition(Expression::new("inputs.cloud == 'azure'"))
        .add_with(("client-id", "${{ secrets.AZURE_CLIENT_ID }}"))
        .add_with(("tenant-id", "${{ secrets.AZURE_TENANT_ID }}"))
        .add_with(("subscription-id", "${{ secrets.AZURE_SUBSCRIPTION_ID }}"))
}

/// Creates a GitHub release step.
pub fn gh_release(files: &str) -> Step<Use> {
    Step::new("Create GitHub Release")
        .uses("softprops", "action-gh-release", GH_RELEASE_VERSION)
        .add_with(("draft", "false"))
        .add_with((
            "prerelease",
            "${{ contains(github.ref, 'alpha') || contains(github.ref, 'beta') || contains(github.ref, 'rc') }}",
        ))
        .add_with(("generate_release_notes", "true"))
        .add_with(("files", files))
}
