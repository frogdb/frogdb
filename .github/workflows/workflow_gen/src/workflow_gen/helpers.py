"""Reusable step builders and matrix helpers."""

import tomllib
from pathlib import Path
from textwrap import dedent

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import LiteralScalarString
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    CHECKOUT,
    DOCKER_BUILD_PUSH,
    DOCKER_LOGIN,
    DOCKER_METADATA,
    DOWNLOAD_ARTIFACT,
    MISE_ACTION,
    RUST_CACHE,
    RUST_TOOLCHAIN_NIGHTLY,
    SETUP_BUILDX,
    SETUP_QEMU,
    UPLOAD_ARTIFACT,
)
from workflow_gen.schema import Step

DOCKERHUB_IMAGE = "frogdb/frogdb"


def _read_rust_version() -> str:
    """Read the pinned Rust version from rust-toolchain.toml at generation time.

    Single source of truth for the Rust version across rust-toolchain.toml
    (authoritative for rustup) and the generated workflows (dtolnay/rust-toolchain).
    `just sync-toolchain-check` additionally verifies .mise.toml agrees.
    """
    # Walk up from this file to find the repo root (where rust-toolchain.toml lives).
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "rust-toolchain.toml"
        if candidate.exists():
            data = tomllib.loads(candidate.read_text())
            return data["toolchain"]["channel"]
    raise FileNotFoundError("rust-toolchain.toml not found in any parent directory")


RUST_VERSION = _read_rust_version()
# Use dtolnay/rust-toolchain with the exact pinned version instead of mise's
# rust plugin. mise's rust plugin interacts badly with Swatinem/rust-cache on
# self-hosted runners: the mise cache captures its own state but not the rustup
# proxies in /root/.cargo/bin, which rust-cache scrubs. On cache restore, mise
# believes rust is installed (symlink marker) and skips the rustup install, but
# `cargo` is actually missing from PATH.
RUST_TOOLCHAIN = f"dtolnay/rust-toolchain@{RUST_VERSION}"


def ensure_path(path: str) -> str:
    """Validate that a repo-root-relative path exists, then return it."""
    if not Path(path).exists():
        raise FileNotFoundError(f"Referenced path does not exist: {path}")
    return path


def script(text: str) -> LiteralScalarString:
    """Dedent a multiline string and wrap it as a YAML literal block scalar."""
    return LiteralScalarString(dedent(text))


def omap(**kwargs: object) -> CommentedMap:
    """Create a CommentedMap preserving insertion order."""
    m = CommentedMap()
    for k, v in kwargs.items():
        m[k] = v
    return m


# --- Reusable steps ---


def libclang_step() -> Step:
    """Install libclang on GitHub-hosted runners (self-hosted has it baked into the Docker image)."""
    return Step(
        name="Install libclang",
        run="sudo apt-get install -y libclang-dev",
        if_="runner.environment != 'self-hosted'",
    )


def self_hosted_env_step() -> Step:
    """Set compiler and library paths for self-hosted ARM64 runners.

    - CC/CXX: clang-18 provides ARM SVE/BF16 support needed by usearch/simsimd
    - LIBCLANG_PATH: points bindgen to the LLVM 18 libclang (has clang_Type_getValueType)
    """
    return Step(
        name="Configure self-hosted build environment",
        if_="runner.environment == 'self-hosted'",
        run=script("""\
            {
              echo "CC=clang-18"
              echo "CXX=clang++-18"
              echo "LIBCLANG_PATH=/usr/lib/llvm-18/lib"
              echo "CARGO_BUILD_JOBS=4"
            } >> "$GITHUB_ENV"
        """),
    )


def checkout_step(
    name: str = "Checkout",
    fetch_depth: str | None = None,
    ref: str | None = None,
    path: str | None = None,
) -> Step:
    w = CommentedMap()
    if fetch_depth is not None:
        w["fetch-depth"] = fetch_depth
    if ref is not None:
        w["ref"] = ref
    if path is not None:
        w["path"] = path
    return Step(name=name, uses=CHECKOUT, with_=w if w else None)


def mise_setup_step(install_args: str | None = None) -> Step:
    """Install tools from .mise.toml via jdx/mise-action.

    Replaces dtolnay/rust-toolchain, taiki-e/install-action (nextest, cargo-deny),
    astral-sh/setup-uv, extractions/setup-just, actions/setup-node, oven-sh/setup-bun,
    and azure/setup-helm. A single step owns tool installation so versions are the
    same in CI, local dev, the self-hosted runner image, and the release Docker build.

    install_args:
        Space-separated tool names to pass to `mise install`. When provided, mise
        only installs those tools instead of the full .mise.toml manifest. Use this
        to keep jobs fast and skip cargo-backend compilation in jobs that don't
        need Rust (e.g. docs jobs that only need Node + bun).
    """
    w = CommentedMap()
    w["install"] = SQ("true")
    if install_args:
        w["install_args"] = install_args
    w["cache"] = SQ("true")
    w["experimental"] = SQ("true")  # enables cargo: and ubi: backends
    return Step(name="Set up mise toolchain", uses=MISE_ACTION, with_=w)


def rust_toolchain_step(components: str | None = None, targets: str | None = None) -> Step:
    """Install the exact Rust version pinned in rust-toolchain.toml.

    Used in CI instead of mise's rust plugin — see RUST_TOOLCHAIN definition above
    for the reasoning. Components and targets are optional; rust-toolchain.toml's
    own `components`/`targets` fields are respected by the installed toolchain.
    """
    w = CommentedMap()
    if components:
        w["components"] = components
    if targets:
        w["targets"] = targets
    return Step(
        name=f"Install Rust {RUST_VERSION}",
        uses=RUST_TOOLCHAIN,
        with_=w if w else None,
    )


def rust_nightly_toolchain_step() -> Step:
    return Step(name="Install Rust nightly toolchain", uses=RUST_TOOLCHAIN_NIGHTLY)


def cargo_cache_step(*, shared_key: str) -> Step:
    return Step(
        name="Cache Rust build artifacts", uses=RUST_CACHE, with_=omap(**{"shared-key": shared_key})
    )


def setup_qemu_step() -> Step:
    return Step(name="Set up QEMU", uses=SETUP_QEMU)


def setup_buildx_step() -> Step:
    return Step(name="Set up Docker Buildx", uses=SETUP_BUILDX)


def docker_login_ghcr_step() -> Step:
    w = CommentedMap()
    w["registry"] = "${{ env.REGISTRY }}"
    w["username"] = "${{ github.actor }}"
    w["password"] = "${{ secrets.GITHUB_TOKEN }}"
    return Step(name="Log in to GitHub Container Registry", uses=DOCKER_LOGIN, with_=w)


def docker_login_dockerhub_step() -> Step:
    w = CommentedMap()
    w["registry"] = "docker.io"
    w["username"] = "${{ secrets.DOCKERHUB_USERNAME }}"
    w["password"] = "${{ secrets.DOCKERHUB_TOKEN }}"
    return Step(name="Log in to Docker Hub", uses=DOCKER_LOGIN, with_=w)


def docker_metadata_step(
    *,
    tags: list[tuple[str, dict[str, str]]],
    extra_images: list[str] | None = None,
) -> Step:
    tag_lines = []
    for tag_type, params in tags:
        parts = [f"type={tag_type}"]
        parts.extend(f"{k}={v}" for k, v in params.items())
        tag_lines.append(",".join(parts))
    image_lines = ["${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}"]
    if extra_images:
        image_lines.extend(extra_images)
    w = CommentedMap()
    if len(image_lines) == 1:
        w["images"] = image_lines[0]
    else:
        w["images"] = LiteralScalarString("\n".join(image_lines))
    w["tags"] = LiteralScalarString("\n".join(tag_lines))
    return Step(id="meta", name="Extract metadata", uses=DOCKER_METADATA, with_=w)


def docker_build_push_step(push: str = SQ("true"), cache: bool = False) -> Step:
    w = CommentedMap()
    w["context"] = SQ(".")
    w["file"] = SQ(ensure_path("./frogdb-server/docker/Dockerfile.builder"))
    w["platforms"] = "linux/amd64,linux/arm64"
    w["push"] = push
    w["tags"] = "${{ steps.meta.outputs.tags }}"
    w["labels"] = "${{ steps.meta.outputs.labels }}"
    if cache:
        w["cache-from"] = "type=gha"
        w["cache-to"] = "type=gha,mode=max"
    return Step(name="Build and push multi-arch image", uses=DOCKER_BUILD_PUSH, with_=w)


def download_all_artifacts_step() -> Step:
    return Step(name="Download all artifacts", uses=DOWNLOAD_ARTIFACT, with_=omap(path="artifacts"))


def upload_artifact_step(*, name: str, path: str, retention_days: int = 7) -> Step:
    w = CommentedMap()
    w["name"] = name
    w["path"] = path
    w["retention-days"] = SQ(str(retention_days))
    return Step(name=f"Upload {name}", uses=UPLOAD_ARTIFACT, with_=w)


def run_step(*, name: str, run: str) -> Step:
    return Step(name=name, run=run)


# --- Matrix targets ---


def macos_target(*, triple: str, arch: str) -> CommentedMap:
    t = CommentedMap()
    t["arch"] = arch
    t["ext"] = SQ("")
    t["os"] = "macos-latest"
    t["target"] = triple
    return t


MACOS_TARGETS = [
    macos_target(triple="x86_64-apple-darwin", arch="amd64"),
    macos_target(triple="aarch64-apple-darwin", arch="arm64"),
]


def docker_env() -> CommentedMap:
    e = CommentedMap()
    e["CARGO_TERM_COLOR"] = "always"
    e["REGISTRY"] = "ghcr.io"
    e["IMAGE_NAME"] = "${{ github.repository }}"
    return e
