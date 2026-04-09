"""Action versions and other CI constants.

Actions are pinned to commit SHAs for supply-chain security.
Dependabot (github-actions ecosystem) keeps these updated.
"""

# Action versions — SHA-pinned
CHECKOUT = "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"  # v6
RUST_CACHE = "Swatinem/rust-cache@42dc69e1aa15d09112580998cf2ef0119e2e91ae"  # v2
UPLOAD_ARTIFACT = "actions/upload-artifact@bbbca2ddaa5d8feaa63e36b76fdaad77386f024f"  # v7
DOWNLOAD_ARTIFACT = "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"  # v8
# dtolnay/rust-toolchain: unpinned — @stable/@nightly are branches that track
# the latest Rust release. Pinning would freeze the toolchain version.
RUST_TOOLCHAIN = "dtolnay/rust-toolchain@stable"
RUST_TOOLCHAIN_NIGHTLY = "dtolnay/rust-toolchain@nightly"
SETUP_HELM = "azure/setup-helm@f0accbfd55e3332a28f721b8202b1016cecf90d5"  # v5
SETUP_QEMU = "docker/setup-qemu-action@ce360397dd3f832beb865e1373c09c0e9f86d70a"  # v4
SETUP_BUILDX = "docker/setup-buildx-action@4d04d5d9486b7bd6fa91e7baf45bbb4f8b9deedd"  # v4
DOCKER_LOGIN = "docker/login-action@4907a6ddec9925e35a0a9e82d7399ccc52663121"  # v4
DOCKER_METADATA = "docker/metadata-action@030e881283bb7a6894de51c315a6bfe6a94e05cf"  # v6
DOCKER_BUILD_PUSH = "docker/build-push-action@d08e5c354a6adb9ed34480a06d141179aa583294"  # v7
GH_RELEASE = "softprops/action-gh-release@153bb8e04406b158c6c84fc1615b65b24149a1fe"  # v2
COSIGN_INSTALLER = "sigstore/cosign-installer@f713795cb21599bc4e5c4b58cbad1da852d7eeb9"  # v3
ATTEST_BUILD_PROVENANCE = (
    "actions/attest-build-provenance@96b4a1ef7235a096b17240c259729fdd70c83d45"  # v2
)
RELEASE_PLEASE = "googleapis/release-please-action@c3fc4de07084f75a2b61a5b933069bda6edf3d5c"  # v4
SETUP_UV = "astral-sh/setup-uv@94527f2e458b27549849d47d273a16bec83a01e9"  # v7
SETUP_JUST = "extractions/setup-just@f8a3cce218d9f83db3a2ecd90e41ac3de6cdfd9b"  # v3
# taiki-e/install-action: pinned to v2 SHA; pass tool= via Step.with_
INSTALL_ACTION = "taiki-e/install-action@80e6af7a2ec7f280fffe2d0a9d3a12a9d11d86e9"  # v2
CODECOV = "codecov/codecov-action@75cd11691c0faa626561e295848008c8a7dddffe"  # v5
SETUP_NODE = "actions/setup-node@53b83947a5a98c8d113130e565377fae1a50d02f"  # v6
SETUP_BUN = "oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6"  # v2
UPLOAD_PAGES_ARTIFACT = (
    "actions/upload-pages-artifact@7b1f4a764d45c48632c6b24a0339c27f5614fb0b"  # v4
)
DEPLOY_PAGES = "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"  # v5
SETUP_GO = "actions/setup-go@4a3601121dd01d1626a1e23e37211e3254c1c06c"  # v6
ACTIONLINT = "raven-actions/actionlint@205b530c5d9fa8f44ae9ed59f341a0db994aa6f8"  # v2
LYCHEE = "lycheeverse/lychee-action@8646ba30535128ac92d33dfc9133794bfdd9b411"  # v2

HELM_VERSION = "v3.13.0"

HELM_REPO_URL = (
    "https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/helm"
)

APT_REPO_URL = (
    "https://${{ github.repository_owner }}.github.io/${{ github.event.repository.name }}/apt"
)

CACHE = "actions/cache@668228422ae6a00e4ad889ee87cd7109ec5666a7"  # v5
PATHS_FILTER = "dorny/paths-filter@fbd0ab8f3e69293af611ebaee6363fc25e6d187d"  # v4
IMPORT_GPG = "crazy-max/ghaction-import-gpg@2dc316deee8e90f13e1a351ab510b4d5bc0c82cd"  # v7
