"""Reusable step builders and matrix helpers."""

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
    HELM_VERSION,
    RUST_CACHE,
    RUST_TOOLCHAIN,
    RUST_TOOLCHAIN_NIGHTLY,
    SETUP_BUILDX,
    SETUP_HELM,
    SETUP_QEMU,
    UPLOAD_ARTIFACT,
)
from workflow_gen.schema import Step

DOCKERHUB_IMAGE = "frogdb/frogdb"


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


def rust_toolchain_step(components: str | None = None, targets: str | None = None) -> Step:
    w = CommentedMap()
    if components:
        w["components"] = components
    if targets:
        w["targets"] = targets
    return Step(name="Install Rust toolchain", uses=RUST_TOOLCHAIN, with_=w if w else None)


def rust_nightly_toolchain_step() -> Step:
    return Step(name="Install Rust nightly toolchain", uses=RUST_TOOLCHAIN_NIGHTLY)


def cargo_cache_step(*, shared_key: str) -> Step:
    return Step(
        name="Cache Rust build artifacts", uses=RUST_CACHE, with_=omap(**{"shared-key": shared_key})
    )


def setup_helm_step() -> Step:
    return Step(name="Install Helm", uses=SETUP_HELM, with_=omap(version=HELM_VERSION))


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
