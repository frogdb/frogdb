//! GitHub Actions version constants.

/// actions/checkout version
pub const CHECKOUT_VERSION: &str = "v4";

/// actions/cache version
pub const CACHE_VERSION: &str = "v4";

/// actions/upload-artifact version
pub const UPLOAD_ARTIFACT_VERSION: &str = "v4";

/// actions/download-artifact version
pub const DOWNLOAD_ARTIFACT_VERSION: &str = "v4";

/// dtolnay/rust-action version (rust toolchain)
pub const RUST_TOOLCHAIN_VERSION: &str = "stable";

/// docker/setup-qemu-action version
pub const QEMU_VERSION: &str = "v3";

/// docker/setup-buildx-action version
pub const BUILDX_VERSION: &str = "v3";

/// docker/login-action version
pub const DOCKER_LOGIN_VERSION: &str = "v3";

/// docker/metadata-action version
pub const DOCKER_METADATA_VERSION: &str = "v5";

/// docker/build-push-action version
pub const DOCKER_BUILD_PUSH_VERSION: &str = "v5";

/// azure/setup-helm version
pub const SETUP_HELM_VERSION: &str = "v3";

/// azure/setup-kubectl version
#[allow(dead_code)]
pub const SETUP_KUBECTL_VERSION: &str = "v3";

/// goto-bus-stop/setup-zig version
pub const SETUP_ZIG_VERSION: &str = "v2";

/// hashicorp/setup-terraform version
#[allow(dead_code)]
pub const SETUP_TERRAFORM_VERSION: &str = "v3";

/// aws-actions/configure-aws-credentials version
#[allow(dead_code)]
pub const AWS_CREDENTIALS_VERSION: &str = "v4";

/// google-github-actions/auth version
#[allow(dead_code)]
pub const GCP_AUTH_VERSION: &str = "v2";

/// azure/login version
#[allow(dead_code)]
pub const AZURE_LOGIN_VERSION: &str = "v1";

/// softprops/action-gh-release version
pub const GH_RELEASE_VERSION: &str = "v1";

/// Helm version to install
pub const HELM_VERSION: &str = "v3.13.0";

/// Zig version for cargo-zigbuild
pub const ZIG_VERSION: &str = "0.11.0";

/// Terraform version
#[allow(dead_code)]
pub const TERRAFORM_VERSION: &str = "1.6.0";
