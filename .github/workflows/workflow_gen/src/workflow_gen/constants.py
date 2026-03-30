"""Action versions and other CI constants."""

# Action versions
CHECKOUT = "actions/checkout@v6"
RUST_CACHE = "Swatinem/rust-cache@v2"
UPLOAD_ARTIFACT = "actions/upload-artifact@v7"
DOWNLOAD_ARTIFACT = "actions/download-artifact@v8"
RUST_TOOLCHAIN = "dtolnay/rust-toolchain@stable"
SETUP_HELM = "azure/setup-helm@v5"
SETUP_QEMU = "docker/setup-qemu-action@v4"
SETUP_BUILDX = "docker/setup-buildx-action@v4"
DOCKER_LOGIN = "docker/login-action@v4"
DOCKER_METADATA = "docker/metadata-action@v6"
DOCKER_BUILD_PUSH = "docker/build-push-action@v7"
GH_RELEASE = "softprops/action-gh-release@v2"
COSIGN_INSTALLER = "sigstore/cosign-installer@v3"
RELEASE_PLEASE = "googleapis/release-please-action@v4"
SETUP_UV = "astral-sh/setup-uv@v7"
SETUP_JUST = "extractions/setup-just@v3"
INSTALL_NEXTEST = "taiki-e/install-action@nextest"
SETUP_NODE = "actions/setup-node@v6"
SETUP_BUN = "oven-sh/setup-bun@v2"
UPLOAD_PAGES_ARTIFACT = "actions/upload-pages-artifact@v4"
DEPLOY_PAGES = "actions/deploy-pages@v5"
SETUP_GO = "actions/setup-go@v5"
ACTIONLINT = "raven-actions/actionlint@v2"
LYCHEE = "lycheeverse/lychee-action@v2"

HELM_VERSION = "v3.13.0"

HELM_REPO_URL = (
    "https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/helm"
)

APT_REPO_URL = (
    "https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/apt"
)

SETUP_GO = "actions/setup-go@v5"
IMPORT_GPG = "crazy-max/ghaction-import-gpg@v6"
