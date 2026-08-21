#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: every distributable-artifact build passes `--features cmd-full`.

ADR-0005 ruling 1 (`adr/0005-truthful-redis-86-surface.md`): every artifact
FrogDB ships to a user — the Docker image, the cross-compiled Linux binaries,
the macOS release tarballs, the `.deb` package, the Homebrew formula — builds
`cmd-full`, the full Redis command surface (streams, JSON, geo, HLL, ...).
`core-profile` stays the *development* default purely to keep iteration
builds and the build cache small; it is a build-speed tier, not a product
tier. A shipped binary built without `cmd-full` silently breaks the compat
matrix the docs advertise (this is exactly how the ADR's gap was found: a
side-by-side session against real Redis).

The chokepoint a human edits is not one function but one *shape* of command
line: a release-mode `cargo` build/zigbuild that names the server binary
among its targets (see SHIP_LINE below). Every real ship site currently has
this shape and no other tracked line does (verified by running the same
sweep this gate runs), so the gate does not need a hand-maintained file list
to know where to look — it greps the whole tracked tree for the shape and
demands `cmd-full` appear on every line that has it.

Two things are pinned so the invariant cannot go quiet by drifting away
unnoticed:

* **Forward** — a matched line missing `cmd-full` fails.
* **Total count** — the number of matched lines across the tree must equal
  `EXPECTED_TOTAL`. A drop means a ship site was deleted or reworded out of
  the pattern's shape without the pin being told (silently exiting the
  gate's visibility is exactly the failure mode a plain "no violations
  found" grep cannot catch); a rise means a new ship site appeared and needs
  a human to both confirm it belongs here and move the pin.

Excluded: `.github/workflows/*.yml` (but not `workflow_gen/`) — those are
*generated* from the Python sources under `workflow_gen/` (`just
workflow-gen`), so the source of truth this gate checks is the generator,
per the repo's "fix the generator, not the generated file" convention;
`generate-check`/`workflow-gen --check` (a separate gate) is what keeps the
rendered YAML in sync with it. Checking the rendered copy too would just be
the same finding twice, reported at the wrong edit site.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# A ship-shaped cargo invocation: builds in release mode (or the Docker-only
# "docker" cargo profile, which is release-equivalent for the image build)
# and names frogdb-server as one of the binaries it produces.
SHIP_LINE = re.compile(r"\bcargo\s+(?:build|zigbuild)\b.*--bin\s+frogdb-server\b")

CMD_FULL = "cmd-full"

# file (relative to repo root) -> expected number of ship-shaped lines. Checked
# in both directions: see module docstring.
EXPECTED_COUNTS: dict[str, int] = {
    "Justfile": 3,  # release (self-built), cross-build (x86_64), cross-build-arm (aarch64)
    "frogdb-server/docker/Dockerfile.builder": 1,  # in-Docker release build
    ".github/workflows/workflow_gen/src/workflow_gen/workflows/release.py": 1,  # macOS tarball job
    "website/src/content/docs/getting-started/installation.mdx": 1,  # .deb build-the-binaries step
}
EXPECTED_TOTAL = sum(EXPECTED_COUNTS.values())

# Generated from workflow_gen/ — see module docstring.
GENERATED_WORKFLOW_YAML = re.compile(r"^\.github/workflows/[^/]+\.yml$")


def tracked_files() -> list[str]:
    """Every git-tracked file, repo-root-relative. Skips .gitignore'd trees
    (target/, node_modules/, .claude/worktrees/, ...) for free."""
    out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


def join_continuations(lines: list[str]) -> list[tuple[int, str]]:
    """(1-based start line, logical line) pairs, joining trailing `\\`
    continuations so a `--features` split onto its own line is still seen."""
    joined: list[tuple[int, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        start = i
        buf = [lines[i].rstrip("\n")]
        while buf[-1].endswith("\\") and i + 1 < n:
            i += 1
            buf[-1] = buf[-1][:-1]
            buf.append(lines[i].rstrip("\n"))
        joined.append((start + 1, " ".join(buf)))
        i += 1
    return joined


def main() -> int:
    violations: list[tuple[str, int, str]] = []  # (file, line, text)
    counts: dict[str, int] = {}

    for rel in tracked_files():
        if GENERATED_WORKFLOW_YAML.match(rel):
            continue
        path = ROOT / rel
        try:
            text = path.read_text(errors="strict")
        except (UnicodeDecodeError, OSError):
            continue  # binary or unreadable — cargo invocations are never here
        if "cargo" not in text or "frogdb-server" not in text:
            continue  # cheap prefilter before the regex/join pass
        for lineno, logical in join_continuations(text.splitlines()):
            if logical.lstrip().startswith("#"):
                continue  # a comment describing the shape isn't an invocation of it
            if not SHIP_LINE.search(logical):
                continue
            counts[rel] = counts.get(rel, 0) + 1
            if CMD_FULL not in logical:
                violations.append((rel, lineno, logical.strip()))

    status = 0

    if violations:
        print(
            "ERROR: a distributable frogdb-server build is missing --features cmd-full:",
            file=sys.stderr,
        )
        for rel, lineno, text in violations:
            print(f"  {rel}:{lineno}: {text}", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "       ADR-0005 ruling 1: every distributable artifact (Docker image,",
            file=sys.stderr,
        )
        print(
            "       cross-built binaries, macOS tarballs, deb, Homebrew) builds the full",
            file=sys.stderr,
        )
        print(
            "       command surface. Add --features cmd-full (or frogdb-server/cmd-full",
            file=sys.stderr,
        )
        print(
            "       when the invocation is package-qualified) to this invocation.",
            file=sys.stderr,
        )
        status = 1

    total = sum(counts.values())
    if total != EXPECTED_TOTAL:
        print(
            f"ERROR: expected {EXPECTED_TOTAL} distributable-build ship sites, found {total}:",
            file=sys.stderr,
        )
        for rel, expected in sorted(EXPECTED_COUNTS.items()):
            found = counts.get(rel, 0)
            if found != expected:
                print(f"  {rel}: expected {expected}, found {found}", file=sys.stderr)
        unpinned = sorted(set(counts) - set(EXPECTED_COUNTS))
        for rel in unpinned:
            print(f"  {rel}: {counts[rel]} (not in EXPECTED_COUNTS)", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "       A ship site appeared, moved, or disappeared. If it's real, update",
            file=sys.stderr,
        )
        print(
            "       EXPECTED_COUNTS in scripts/ship-cmd-full.py (this is the pin, not a",
            file=sys.stderr,
        )
        print("       thing to silently accept).", file=sys.stderr)
        status = 1

    if status == 0:
        print(f"OK: {total} distributable frogdb-server build(s) all pass --features cmd-full")
    return status


if __name__ == "__main__":
    sys.exit(main())
