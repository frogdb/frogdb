#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["ruamel.yaml>=0.18"]
# ///
"""Generate GitHub Actions workflow YAML files for FrogDB CI/CD."""

import argparse
import sys
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import LiteralScalarString, SingleQuotedScalarString as SQ

# =============================================================================
# Constants
# =============================================================================

HEADER = """\
# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: tools/workflow-gen.py
# Regenerate with: just workflow-gen
# =============================================================================

"""

# Action versions
CHECKOUT = "actions/checkout@v4"
CACHE = "actions/cache@v4"
UPLOAD_ARTIFACT = "actions/upload-artifact@v4"
DOWNLOAD_ARTIFACT = "actions/download-artifact@v4"
RUST_TOOLCHAIN = "dtolnay/rust-toolchain@stable"
SETUP_ZIG = "goto-bus-stop/setup-zig@v2"
SETUP_HELM = "azure/setup-helm@v3"
SETUP_QEMU = "docker/setup-qemu-action@v3"
SETUP_BUILDX = "docker/setup-buildx-action@v3"
DOCKER_LOGIN = "docker/login-action@v3"
DOCKER_METADATA = "docker/metadata-action@v5"
DOCKER_BUILD_PUSH = "docker/build-push-action@v5"
GH_RELEASE = "softprops/action-gh-release@v1"
SETUP_UV = "astral-sh/setup-uv@v5"
SETUP_JUST = "extractions/setup-just@v2"

ZIG_VERSION = "0.11.0"
HELM_VERSION = "v3.13.0"

CARGO_CACHE_PATH = LiteralScalarString(
    "~/.cargo/registry\n~/.cargo/git\ntarget"
)

HELM_REPO_URL = (
    "https://${{ github.repository_owner }}.github.io"
    "/${{ github.event.repository.name }}/helm"
)


# =============================================================================
# Helpers
# =============================================================================


def omap(**kwargs) -> CommentedMap:
    """Create a CommentedMap preserving insertion order."""
    m = CommentedMap()
    for k, v in kwargs.items():
        m[k] = v
    return m


# --- Reusable steps ---


def checkout_step(
    name: str = "Checkout",
    fetch_depth: str | None = None,
    ref: str | None = None,
    path: str | None = None,
) -> CommentedMap:
    s = CommentedMap()
    s["name"] = name
    s["uses"] = CHECKOUT
    w = CommentedMap()
    if fetch_depth is not None:
        w["fetch-depth"] = fetch_depth
    if ref is not None:
        w["ref"] = ref
    if path is not None:
        w["path"] = path
    if w:
        s["with"] = w
    return s


def rust_toolchain_step(
    components: str | None = None, targets: str | None = None
) -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Install Rust toolchain"
    s["uses"] = RUST_TOOLCHAIN
    w = CommentedMap()
    if components:
        w["components"] = components
    if targets:
        w["targets"] = targets
    if w:
        s["with"] = w
    return s


def cargo_cache_step(key_suffix: str) -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Cache cargo registry"
    s["uses"] = CACHE
    w = CommentedMap()
    w["path"] = CARGO_CACHE_PATH
    w["key"] = (
        f"${{{{ runner.os }}}}-cargo-{key_suffix}"
        f"-${{{{ hashFiles('**/Cargo.lock') }}}}"
    )
    w["restore-keys"] = LiteralScalarString(
        f"${{{{ runner.os }}}}-cargo-{key_suffix}-\n"
    )
    s["with"] = w
    return s


def cargo_cache_matrix_step() -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Cache cargo registry"
    s["uses"] = CACHE
    w = CommentedMap()
    w["path"] = CARGO_CACHE_PATH
    w["key"] = (
        "${{ runner.os }}-cargo-${{ matrix.target }}"
        "-${{ hashFiles('**/Cargo.lock') }}"
    )
    w["restore-keys"] = LiteralScalarString(
        "${{ runner.os }}-cargo-${{ matrix.target }}-\n"
    )
    s["with"] = w
    return s


def setup_zig_step(if_cond: str | None = None) -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Install Zig" + (" (Linux)" if if_cond else "")
    if if_cond:
        s["if"] = if_cond
    s["uses"] = SETUP_ZIG
    s["with"] = omap(version=SQ(ZIG_VERSION))
    return s


def setup_helm_step() -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Install Helm"
    s["uses"] = SETUP_HELM
    s["with"] = omap(version=HELM_VERSION)
    return s


def setup_qemu_step() -> CommentedMap:
    return omap(name="Set up QEMU", uses=SETUP_QEMU)


def setup_buildx_step() -> CommentedMap:
    return omap(name="Set up Docker Buildx", uses=SETUP_BUILDX)


def docker_login_ghcr_step() -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Log in to GitHub Container Registry"
    s["uses"] = DOCKER_LOGIN
    w = CommentedMap()
    w["registry"] = "${{ env.REGISTRY }}"
    w["username"] = "${{ github.actor }}"
    w["password"] = "${{ secrets.GITHUB_TOKEN }}"
    s["with"] = w
    return s


def docker_metadata_step(tags: str) -> CommentedMap:
    s = CommentedMap()
    s["id"] = "meta"
    s["name"] = "Extract metadata"
    s["uses"] = DOCKER_METADATA
    w = CommentedMap()
    w["images"] = "${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}"
    w["tags"] = LiteralScalarString(tags)
    s["with"] = w
    return s


def docker_build_push_step(cache: bool = False) -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Build and push multi-arch image"
    s["uses"] = DOCKER_BUILD_PUSH
    w = CommentedMap()
    w["context"] = SQ(".")
    w["file"] = SQ("./Dockerfile.multiarch")
    w["platforms"] = "linux/amd64,linux/arm64"
    w["push"] = SQ("true")
    w["tags"] = "${{ steps.meta.outputs.tags }}"
    w["labels"] = "${{ steps.meta.outputs.labels }}"
    w["build-args"] = LiteralScalarString(
        "BINARY_AMD64=binaries/amd64/frogdb-server\n"
        "BINARY_ARM64=binaries/arm64/frogdb-server"
    )
    if cache:
        w["cache-from"] = "type=gha"
        w["cache-to"] = "type=gha,mode=max"
    s["with"] = w
    return s


def download_artifact_step(name: str, path: str) -> CommentedMap:
    s = CommentedMap()
    s["name"] = f"Download {name}"
    s["uses"] = DOWNLOAD_ARTIFACT
    w = CommentedMap()
    w["name"] = name
    w["path"] = path
    s["with"] = w
    return s


def download_all_artifacts_step() -> CommentedMap:
    s = CommentedMap()
    s["name"] = "Download all artifacts"
    s["uses"] = DOWNLOAD_ARTIFACT
    s["with"] = omap(path="artifacts")
    return s


def upload_artifact_step(
    name: str, path: str, retention_days: int = 7
) -> CommentedMap:
    s = CommentedMap()
    s["name"] = f"Upload {name}"
    s["uses"] = UPLOAD_ARTIFACT
    w = CommentedMap()
    w["name"] = name
    w["path"] = path
    w["retention-days"] = SQ(str(retention_days))
    s["with"] = w
    return s


def run_step(name: str, run: str) -> CommentedMap:
    return omap(name=name, run=run)


def run_step_if(name: str, if_cond: str, run: str) -> CommentedMap:
    s = CommentedMap()
    s["name"] = name
    s["if"] = if_cond
    s["run"] = run
    return s


# --- Matrix targets ---


def linux_target(triple: str, arch: str) -> CommentedMap:
    t = CommentedMap()
    t["arch"] = arch
    t["ext"] = SQ("")
    t["os"] = "ubuntu-latest"
    t["target"] = triple
    return t


def macos_target(triple: str, arch: str) -> CommentedMap:
    t = CommentedMap()
    t["arch"] = arch
    t["ext"] = SQ("")
    t["os"] = "macos-latest"
    t["target"] = triple
    return t


LINUX_TARGETS = [
    linux_target("x86_64-unknown-linux-gnu", "amd64"),
    linux_target("aarch64-unknown-linux-gnu", "arm64"),
]

MACOS_TARGETS = [
    macos_target("x86_64-apple-darwin", "amd64"),
    macos_target("aarch64-apple-darwin", "arm64"),
]


def matrix_strategy(includes: list[CommentedMap]) -> CommentedMap:
    matrix = CommentedMap()
    matrix["include"] = CommentedSeq(includes)
    strategy = CommentedMap()
    strategy["matrix"] = matrix
    strategy["fail-fast"] = False
    return strategy


# --- Workflow scaffolding ---


def docker_env() -> CommentedMap:
    e = CommentedMap()
    e["CARGO_TERM_COLOR"] = "always"
    e["REGISTRY"] = "ghcr.io"
    e["IMAGE_NAME"] = "${{ github.repository }}"
    return e


def workflow_base(name: str, env: CommentedMap) -> CommentedMap:
    w = CommentedMap()
    w["name"] = name
    w["env"] = env
    wd = CommentedMap()
    wd.fa.set_flow_style()
    on = CommentedMap()
    on["workflow_dispatch"] = wd
    w["on"] = on
    w["jobs"] = CommentedMap()
    return w


def simple_job(name: str, steps: list[CommentedMap]) -> CommentedMap:
    j = CommentedMap()
    j["name"] = name
    j["runs-on"] = "ubuntu-latest"
    j["steps"] = CommentedSeq(steps)
    return j


# =============================================================================
# Test Workflow
# =============================================================================


def test_workflow() -> CommentedMap:
    w = workflow_base("Test", omap(CARGO_TERM_COLOR="always"))
    jobs = w["jobs"]

    jobs["lint"] = simple_job("Lint", [
        checkout_step(),
        rust_toolchain_step(components="rustfmt, clippy"),
        run_step("Check formatting", "cargo fmt --all -- --check"),
        run_step(
            "Run clippy",
            "cargo clippy --all-targets --all-features -- -D warnings",
        ),
    ])

    jobs["unit-tests"] = simple_job("Unit Tests", [
        checkout_step(),
        rust_toolchain_step(),
        cargo_cache_step("test"),
        run_step("Run unit tests", "cargo test --all"),
    ])

    jobs["shuttle-tests"] = simple_job("Shuttle Concurrency Tests", [
        checkout_step(),
        rust_toolchain_step(),
        cargo_cache_step("shuttle"),
        run_step(
            "Run Shuttle concurrency tests",
            "cargo test -p frogdb-core --features shuttle --test concurrency",
        ),
    ])

    jobs["turmoil-tests"] = simple_job("Turmoil Simulation Tests", [
        checkout_step(),
        rust_toolchain_step(),
        cargo_cache_step("turmoil"),
        run_step(
            "Run Turmoil simulation tests",
            "cargo test -p frogdb-server --features turmoil --test simulation",
        ),
    ])

    jobs["helm-gen-check"] = simple_job("Helm Generation Check", [
        checkout_step(),
        rust_toolchain_step(),
        cargo_cache_step("helm"),
        run_step(
            "Check Helm files are up to date",
            "cargo run -p helm-gen -- --check",
        ),
    ])

    # Uses uv + just instead of Rust toolchain
    jobs["workflow-gen-check"] = simple_job("Workflow Generation Check", [
        checkout_step(),
        omap(name="Install uv", uses=SETUP_UV),
        omap(name="Install just", uses=SETUP_JUST),
        run_step(
            "Check workflow files are up to date",
            "just workflow-gen --check",
        ),
    ])

    jobs["helm-lint"] = simple_job("Helm Lint", [
        checkout_step(),
        setup_helm_step(),
        run_step("Lint Helm chart", "helm lint deploy/helm/frogdb"),
        run_step(
            "Template Helm chart",
            "helm template frogdb deploy/helm/frogdb --debug",
        ),
        run_step(
            "Template cluster preset",
            "helm template frogdb deploy/helm/frogdb"
            " -f deploy/helm/frogdb/values-cluster.yaml --debug",
        ),
    ])

    return w


# =============================================================================
# Build Workflow
# =============================================================================


def build_workflow() -> CommentedMap:
    w = workflow_base("Build", docker_env())
    jobs = w["jobs"]

    # Build job (matrix over Linux targets)
    build = CommentedMap()
    build["name"] = "Build (${{ matrix.target }})"
    build["runs-on"] = "${{ matrix.os }}"
    build["strategy"] = matrix_strategy(list(LINUX_TARGETS))
    build["steps"] = CommentedSeq([
        checkout_step(),
        rust_toolchain_step(targets="${{ matrix.target }}"),
        # zigbuild handles glibc version differences for Linux cross-compilation
        run_step("Install cargo-zigbuild", "cargo install cargo-zigbuild"),
        setup_zig_step(),
        cargo_cache_matrix_step(),
        run_step(
            "Build",
            "cargo zigbuild --release --target ${{ matrix.target }}"
            " --bin frogdb-server",
        ),
        upload_artifact_step(
            "frogdb-server-${{ matrix.arch }}",
            "target/${{ matrix.target }}/release/frogdb-server",
        ),
    ])
    jobs["build"] = build

    # Docker job — only on push to main
    docker = CommentedMap()
    docker["needs"] = CommentedSeq(["build"])
    # Only build Docker images on push to main, not for PRs
    docker["if"] = (
        "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    )
    docker["name"] = "Docker Build"
    docker["runs-on"] = "ubuntu-latest"
    perms = CommentedMap()
    perms["contents"] = "read"
    perms["packages"] = "write"
    docker["permissions"] = perms
    docker["steps"] = CommentedSeq([
        checkout_step(),
        download_artifact_step("frogdb-server-amd64", "binaries/amd64"),
        download_artifact_step("frogdb-server-arm64", "binaries/arm64"),
        setup_qemu_step(),
        setup_buildx_step(),
        docker_login_ghcr_step(),
        docker_metadata_step(
            "type=ref,event=branch\n"
            "type=sha,prefix=\n"
            "type=raw,value=latest,enable={{is_default_branch}}"
        ),
        docker_build_push_step(cache=True),
    ])
    jobs["docker"] = docker

    return w


# =============================================================================
# Release Workflow
# =============================================================================


def release_workflow() -> CommentedMap:
    w = workflow_base("Release", docker_env())
    jobs = w["jobs"]

    # Build binaries (matrix over all targets)
    # Linux uses zigbuild for cross-compilation; macOS uses native cargo build
    build = CommentedMap()
    build["name"] = "Build Release Binaries (${{ matrix.target }})"
    build["runs-on"] = "${{ matrix.os }}"
    build["strategy"] = matrix_strategy(
        list(LINUX_TARGETS) + list(MACOS_TARGETS)
    )
    build["steps"] = CommentedSeq([
        checkout_step(),
        rust_toolchain_step(targets="${{ matrix.target }}"),
        run_step_if(
            "Install cargo-zigbuild (Linux)",
            "runner.os == 'Linux'",
            "cargo install cargo-zigbuild",
        ),
        setup_zig_step(if_cond="runner.os == 'Linux'"),
        run_step_if(
            "Build (Linux)",
            "runner.os == 'Linux'",
            "cargo zigbuild --release --target ${{ matrix.target }}"
            " --bin frogdb-server",
        ),
        run_step_if(
            "Build (macOS)",
            "runner.os == 'macOS'",
            "cargo build --release --target ${{ matrix.target }}"
            " --bin frogdb-server",
        ),
        run_step(
            "Create release archive",
            LiteralScalarString(
                "cd target/${{ matrix.target }}/release\n"
                "tar -czvf ../../../frogdb-${{ github.ref_name }}"
                "-${{ matrix.target }}.tar.gz"
                " frogdb-server${{ matrix.ext }}"
            ),
        ),
        upload_artifact_step(
            "frogdb-${{ matrix.target }}",
            "frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz",
        ),
    ])
    jobs["build-binaries"] = build

    # Docker release
    docker = CommentedMap()
    docker["needs"] = CommentedSeq(["build-binaries"])
    docker["name"] = "Docker Release"
    docker["runs-on"] = "ubuntu-latest"
    perms = CommentedMap()
    perms["contents"] = "read"
    perms["packages"] = "write"
    docker["permissions"] = perms
    docker["steps"] = CommentedSeq([
        checkout_step(),
        download_artifact_step(
            "frogdb-x86_64-unknown-linux-gnu", "binaries/amd64"
        ),
        download_artifact_step(
            "frogdb-aarch64-unknown-linux-gnu", "binaries/arm64"
        ),
        run_step(
            "Extract binaries",
            LiteralScalarString(
                "cd binaries/amd64 && tar -xzf *.tar.gz"
                " && mv frogdb-server ../amd64-binary\n"
                "cd ../arm64 && tar -xzf *.tar.gz"
                " && mv frogdb-server ../arm64-binary\n"
                "mv binaries/amd64-binary binaries/amd64/frogdb-server\n"
                "mv binaries/arm64-binary binaries/arm64/frogdb-server"
            ),
        ),
        setup_qemu_step(),
        setup_buildx_step(),
        docker_login_ghcr_step(),
        docker_metadata_step(
            "type=semver,pattern={{version}}\n"
            "type=semver,pattern={{major}}.{{minor}}\n"
            "type=semver,pattern={{major}}"
        ),
        docker_build_push_step(cache=False),
    ])
    jobs["docker"] = docker

    # Helm chart publishing
    helm = CommentedMap()
    helm["needs"] = CommentedSeq(["docker"])
    helm["name"] = "Publish Helm Chart"
    helm["runs-on"] = "ubuntu-latest"
    perms = CommentedMap()
    perms["contents"] = "write"
    perms["pages"] = "write"
    helm["permissions"] = perms
    helm["steps"] = CommentedSeq([
        checkout_step(fetch_depth=SQ("0")),
        setup_helm_step(),
        run_step(
            "Package Helm chart",
            LiteralScalarString(
                "helm package deploy/helm/frogdb"
                " --destination .helm-packages\n"
                "helm repo index .helm-packages"
                f" --url {HELM_REPO_URL}"
            ),
        ),
        checkout_step(
            name="Checkout gh-pages branch", ref="gh-pages", path="gh-pages"
        ),
        run_step(
            "Update Helm repository",
            LiteralScalarString(
                "mkdir -p gh-pages/helm\n"
                "cp .helm-packages/*.tgz gh-pages/helm/\n"
                "# Merge index files\n"
                "if [ -f gh-pages/helm/index.yaml ]; then\n"
                "  helm repo index gh-pages/helm"
                " --merge gh-pages/helm/index.yaml"
                f" --url {HELM_REPO_URL}\n"
                "else\n"
                "  helm repo index gh-pages/helm"
                f" --url {HELM_REPO_URL}\n"
                "fi"
            ),
        ),
        run_step(
            "Commit and push",
            LiteralScalarString(
                "cd gh-pages\n"
                'git config user.name "GitHub Actions"\n'
                'git config user.email "actions@github.com"\n'
                "git add helm/\n"
                "git commit -m"
                ' "Release Helm chart ${{ github.ref_name }}"'
                ' || echo "No changes"\n'
                "git push"
            ),
        ),
    ])
    jobs["helm"] = helm

    # GitHub Release
    release = CommentedMap()
    release["needs"] = CommentedSeq(["build-binaries", "docker"])
    release["name"] = "GitHub Release"
    release["runs-on"] = "ubuntu-latest"
    release["permissions"] = omap(contents="write")
    gh_step = CommentedMap()
    gh_step["name"] = "Create GitHub Release"
    gh_step["uses"] = GH_RELEASE
    gh_w = CommentedMap()
    gh_w["draft"] = SQ("false")
    gh_w["prerelease"] = (
        "${{ contains(github.ref, 'alpha')"
        " || contains(github.ref, 'beta')"
        " || contains(github.ref, 'rc') }}"
    )
    gh_w["generate_release_notes"] = SQ("true")
    gh_w["files"] = "artifacts/frogdb-*/*.tar.gz"
    gh_step["with"] = gh_w
    release["steps"] = CommentedSeq([
        checkout_step(),
        download_all_artifacts_step(),
        gh_step,
    ])
    jobs["github-release"] = release

    return w


# =============================================================================
# Rendering
# =============================================================================

WORKFLOWS = {
    "test.yml": test_workflow,
    "build.yml": build_workflow,
    "release.yml": release_workflow,
}


def render(workflow: CommentedMap) -> str:
    """Render a workflow to YAML string with header."""
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.width = 4096
    stream = StringIO()
    yaml.dump(workflow, stream)
    body = stream.getvalue()
    # Quote 'on' key for GitHub Actions compatibility (YAML 1.1 treats on as boolean)
    body = body.replace("\non:\n", "\n'on':\n")
    return HEADER + body


def generate(output_dir: Path) -> None:
    """Generate all workflow files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, builder in WORKFLOWS.items():
        path = output_dir / filename
        content = render(builder())
        path.write_text(content)
        print(f"Generated {path}")


def check(output_dir: Path) -> bool:
    """Check that existing workflow files match generated content."""
    ok = True
    for filename, builder in WORKFLOWS.items():
        path = output_dir / filename
        expected = render(builder())
        if not path.exists():
            print(f"MISSING: {path}")
            ok = False
            continue
        actual = path.read_text()
        if actual != expected:
            print(f"OUT OF DATE: {path}")
            ok = False
        else:
            print(f"OK: {path}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate GitHub Actions workflow files"
    )
    parser.add_argument(
        "--output", default=".github/workflows", help="Output directory"
    )
    parser.add_argument(
        "--check", action="store_true", help="Check files are up to date"
    )
    args = parser.parse_args()

    output_dir = Path(args.output)

    if args.check:
        if not check(output_dir):
            print(
                "\nWorkflow files are out of date."
                " Run 'just workflow-gen' to regenerate."
            )
            sys.exit(1)
    else:
        generate(output_dir)


if __name__ == "__main__":
    main()
