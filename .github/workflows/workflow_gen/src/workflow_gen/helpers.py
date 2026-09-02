"""Reusable step builders and matrix helpers."""

import functools
import sys
import tomllib
from pathlib import Path
from textwrap import dedent
from types import ModuleType
from typing import TYPE_CHECKING

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import LiteralScalarString
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    CACHE,
    CACHE_RESTORE,
    CHECKOUT,
    DOCKER_BUILD_PUSH,
    DOCKER_LOGIN,
    DOCKER_METADATA,
    DOWNLOAD_ARTIFACT,
    MISE_ACTION,
    RUST_CACHE,
    RUST_TOOLCHAIN_NIGHTLY,
    SETUP_BUILDX,
    SETUP_JAVA,
    SETUP_QEMU,
    UPLOAD_ARTIFACT,
)
from workflow_gen.schema import Job, Permissions, Step

if TYPE_CHECKING:
    # `scripts/` is not on sys.path until `_locked_areas_module()` puts it there,
    # so `Spec` is only ever a name in an annotation — see `locked_areas()` below.
    from locked_areas import Spec

DOCKERHUB_IMAGE = "frogdb/frogdb"

# The repo-root marker: present at the root, nowhere below it.
_ROOT_MARKER = "rust-toolchain.toml"


def _repo_root() -> Path:
    """The repo root, found by walking up from this file for `_ROOT_MARKER`.

    `workflow_gen` is a uv project nested under `.github/workflows/`, so it can
    reach repo-root sources (rust-toolchain.toml, specs/) only by locating the
    root at generation time.
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / _ROOT_MARKER).exists():
            return parent
    raise FileNotFoundError(f"{_ROOT_MARKER} not found in any parent directory")


def _read_rust_version() -> str:
    """Read the pinned Rust version from rust-toolchain.toml at generation time.

    Single source of truth for the Rust version across rust-toolchain.toml
    (authoritative for rustup) and the generated workflows (dtolnay/rust-toolchain).
    `just sync-toolchain-check` additionally verifies .mise.toml agrees.
    """
    data = tomllib.loads((_repo_root() / _ROOT_MARKER).read_text())
    return data["toolchain"]["channel"]


RUST_VERSION = _read_rust_version()
# Use dtolnay/rust-toolchain with the exact pinned version instead of mise's
# rust plugin. mise's rust plugin interacts badly with Swatinem/rust-cache on
# self-hosted runners: the mise cache captures its own state but not the rustup
# proxies in /root/.cargo/bin, which rust-cache scrubs. On cache restore, mise
# believes rust is installed (symlink marker) and skips the rustup install, but
# `cargo` is actually missing from PATH.
RUST_TOOLCHAIN = f"dtolnay/rust-toolchain@{RUST_VERSION}"

# mise install_args for a mutation job, shared by `test.py`'s per-PR
# `mutants-diff` and `mutants_weekly.py`'s full runs so the two cannot drift.
# cargo-mutants shells out to the test tool named in .cargo/mutants.toml, which
# is nextest — so a mutation job needs both binaries, not just cargo-mutants.
# python/uv are for the `uv run --script` shebang on scripts/locked_areas.py,
# which the `just mutants*` recipes call to resolve the crate's perimeter and path.
MISE_JUST_MUTANTS = "python uv just cargo:cargo-mutants cargo:cargo-nextest"


# --- Locked-areas manifest ---


@functools.cache
def _locked_areas_module() -> ModuleType:
    """`scripts/locked_areas.py`, imported across the uv-project boundary.

    The manifest parser lives at the repo root, outside this project's
    dependency set, so `scripts/` goes on `sys.path` here rather than at module
    import — `helpers` is imported by every workflow, most of which never ask
    for the manifest. Same seam as `website/scripts/spec-gen.py`.
    """
    sys.path.append(str(_repo_root() / "scripts"))
    import locked_areas

    return locked_areas


def locked_areas() -> list["Spec"]:
    """Locked-area specs from `specs/*.md` headers, via scripts/locked_areas.py.

    Only LOCKED areas are returned (DRAFT specs carry no gate or crates), in the
    parser's order — sorted by spec path, so a generated matrix is stable across
    runs and `just workflow-gen --check` does not flap.

    Raises if the manifest fails validation — a bad header must break generation,
    not silently drop a crate from a CI matrix.
    """
    specs, errors = _locked_areas_module().validate()
    if errors:
        raise RuntimeError("locked-areas manifest invalid:\n" + "\n".join(errors))
    return [spec for spec in specs if spec.is_locked]


def locked_crate_paths(specs: list["Spec"]) -> dict[str, str]:
    """Locked crate → its repo-relative directory, in manifest order.

    The `mutants-diff` job needs a path per crate twice over: as the
    `paths-filter` glob that decides whether the crate was touched, and as the
    `git diff -- <path>` scope inside `just mutants-diff`. Both read the same
    workspace manifest walk, so a crate that moves directory changes the filter
    on the next `just workflow-gen` rather than quietly matching nothing.

    Takes the spec list rather than calling `locked_areas()` — that re-reads
    and re-validates the manifest on every call, and the caller already has it.
    """
    paths = _locked_areas_module().member_paths()
    return {crate: paths[crate] for spec in specs for crate in spec.crates}


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
    """Install libclang (bindgen dependency) on GitHub-hosted runners."""
    return Step(
        name="Install libclang",
        run="sudo apt-get install -y libclang-dev",
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
    # rust is unconfigured for mise (see RUST_TOOLCHAIN above), so a cargo:
    # tool's declared `rust` install dependency must fall back to ambient PATH
    # instead of failing mise's install-dependency check.
    env = omap(MISE_DISABLE_TOOLS="rust")
    cargo_tools = [tok for tok in (install_args or "").split() if tok.startswith("cargo:")]
    if len(cargo_tools) >= 2:
        # Parallel `cargo install`s race rustup's first-use toolchain install of
        # the rust-toolchain.toml pin, so force them to install one at a time.
        env["MISE_JOBS"] = SQ("1")
    return Step(
        name="Set up mise toolchain",
        uses=MISE_ACTION,
        with_=w,
        env=env,
    )


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


def cache_step(
    *,
    name: str,
    path: str,
    key: str,
    restore_keys: str | None = None,
    restore_only: bool = False,
) -> Step:
    """Generic actions/cache step for persisting an arbitrary directory across runs.

    Unlike `cargo_cache_step` (Swatinem/rust-cache, tuned for the cargo target dir),
    this wraps plain `actions/cache` for content the fuzz campaign must evolve — the
    libFuzzer corpus. Pass a unique `key` (e.g. one embedding `github.run_id`) plus a
    `restore_keys` prefix so every run restores the newest prior cache and always saves
    a fresh entry, letting the corpus grow monotonically across scheduled runs.

    restore_only:
        Use the restore-only sub-action (no post-job save). The PR corpus-replay job
        only reads the corpus; a save would litter the cache with per-PR entries and
        burn cache quota for no benefit.
    """
    w = CommentedMap()
    w["path"] = path
    w["key"] = key
    if restore_keys is not None:
        w["restore-keys"] = restore_keys
    return Step(name=name, uses=CACHE_RESTORE if restore_only else CACHE, with_=w)


def setup_java_step(*, distribution: str = "temurin", java_version: str = "21") -> Step:
    """Install a JVM via actions/setup-java.

    For jobs that need a JVM but have no other reason to invoke mise (e.g.
    quint_verify.py's Apalache-backed `quint verify`, which auto-downloads
    Apalache itself but needs a JVM already on PATH). Version 21 matches the
    `temurin-21` mise pin used elsewhere (.mise.toml, jepsen_nightly.py) for
    consistency, even though this step doesn't go through mise.
    """
    w = CommentedMap()
    w["distribution"] = distribution
    w["java-version"] = java_version
    return Step(name="Set up JVM", uses=SETUP_JAVA, with_=w)


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


def upload_artifact_step(
    *,
    name: str,
    path: str,
    retention_days: int = 7,
    if_: str | None = None,
    if_no_files_found: str | None = None,
) -> Step:
    w = CommentedMap()
    w["name"] = name
    w["path"] = path
    w["retention-days"] = SQ(str(retention_days))
    if if_no_files_found is not None:
        # actions/upload-artifact defaults to "warn" (stays green with an empty upload) — pass
        # "error" for steps where a missing artifact indicates a real regression (e.g. an
        # `if: failure()` step whose whole point is to upload evidence of the failure) rather
        # than an expected "nothing to upload" case.
        w["if-no-files-found"] = if_no_files_found
    return Step(name=f"Upload {name}", uses=UPLOAD_ARTIFACT, with_=w, if_=if_)


def run_step(*, name: str, run: str) -> Step:
    return Step(name=name, run=run)


# --- Credit-thrift change gate for scheduled (nightly) workflows ---


def change_gate_job(*, workflow_file: str) -> Job:
    """Build a lightweight leading job that skips a scheduled run with no new commits.

    Nightly workflows burn CI minutes every night regardless of whether anything
    changed. This job compares the current commit against the head SHA of this same
    workflow's last successful *scheduled* run (via `gh run list --event schedule`)
    and exposes a `skip` output; downstream jobs add `needs: gate` plus
    `if: needs.gate.outputs.skip != 'true'` to honor it.

    The `--event schedule` filter is what makes the comparison mean "since the last
    time the full scheduled run measured this tree". Without it a `workflow_dispatch`
    — which may deliberately run a *narrower* slice, e.g. `mutants-weekly.yml`'s
    one-crate input — becomes the last successful run at its sha, and the next
    scheduled run at that same sha skips entirely, so the rest of the slice goes
    unmeasured that cycle.

    The check only ever applies to `schedule`-triggered runs — `workflow_dispatch`
    (someone explicitly asked for a run) and any other trigger always proceed, so a
    manual dispatch is never silently absorbed by the gate. The job itself always
    runs (rather than being conditioned on `github.event_name == 'schedule'` at the
    job level) so downstream `needs: gate` never has to reason about a *skipped*
    dependency — GitHub Actions' default "skip if a needed job was skipped" behavior
    is exactly the footgun this sidesteps; see `changes` in `workflows/test.py` for
    the same always-runs-leading-job shape.

    No checkout is needed — `gh run list` talks to the Actions API directly, scoped
    to just this job via a minimal `actions: read` token (the job has no other
    permission, including `contents`).
    """
    return Job(
        name="Change Gate",
        runs_on="ubuntu-latest",
        permissions=Permissions(actions="read"),
        outputs=omap(skip="${{ steps.gate.outputs.skip }}"),
        steps=[
            Step(
                id="gate",
                name="Check for new commits since last successful scheduled run",
                env=omap(
                    GH_TOKEN="${{ github.token }}",
                    EVENT_NAME="${{ github.event_name }}",
                    CURRENT_SHA="${{ github.sha }}",
                    WORKFLOW_FILE=workflow_file,
                ),
                run=script("""\
                    set -uo pipefail
                    if [ "${EVENT_NAME}" != "schedule" ]; then
                      echo "skip=false" >> "$GITHUB_OUTPUT"
                      echo "Not a scheduled run (event: ${EVENT_NAME}); proceeding."
                      exit 0
                    fi
                    last_sha=$(gh run list --repo "${GITHUB_REPOSITORY}" --workflow "${WORKFLOW_FILE}" \\
                      --event schedule --status success --limit 1 --json headSha --jq '.[0].headSha // empty')
                    if [ -z "${last_sha}" ]; then
                      echo "skip=false" >> "$GITHUB_OUTPUT"
                      echo "No previous successful scheduled run found; proceeding."
                    elif [ "${last_sha}" = "${CURRENT_SHA}" ]; then
                      echo "skip=true" >> "$GITHUB_OUTPUT"
                      echo "No new commits since last successful scheduled run (${last_sha}); skipping."
                    else
                      echo "skip=false" >> "$GITHUB_OUTPUT"
                      echo "New commits since last successful scheduled run (${last_sha} -> ${CURRENT_SHA}); proceeding."
                    fi
                    """),
            ),
        ],
    )


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
