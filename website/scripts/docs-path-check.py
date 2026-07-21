#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Verify that repo code paths referenced in the docs actually exist.

Scans every `.md`/`.mdx` file under `website/src/content/docs/` for tokens
that look like repo-relative paths (inside inline code spans and fenced code
blocks) and checks that each one exists relative to the repo root. This lets
CI catch architecture pages that silently rot as the source tree moves,
without pinning the checker to today's page content (later tasks rewrite
these pages).

Extraction policy (deliberately conservative -- see `KNOWN_TOP_LEVEL` below):
  * Only text inside inline code spans (`` `like this` ``) and fenced code
    blocks (``` ``` ```/`~~~`) is scanned. Prose is never scanned, so
    sentences like "1M ops/second" or JSX attributes like
    `title="frogdb-server"` are never candidates -- only the two "this is
    code/a path" markup forms author actually use to write real paths.
  * A token is a candidate only if it contains `/` AND its first path
    segment is one of `KNOWN_TOP_LEVEL` -- the small set of prefixes docs
    authors use for repo paths. This intentionally includes `crates` and
    `ops`, which are *not* real top-level directories (they live under
    `frogdb-server/`); docs commonly write the shorthand `crates/foo` or
    `ops/bar` instead of the real `frogdb-server/crates/foo`. Keeping them
    in the known-prefix list means that common mistake is caught rather
    than silently ignored.
  * Fenced code blocks are scanned regardless of language tag or apparent
    purpose (mermaid diagrams, ASCII tree diagrams, shell examples, "example
    output" blocks). Reliably distinguishing a block of real example output
    from a block of narrative code from syntax alone is infeasible, so
    nothing is special-cased; the candidate-token filter above (must contain
    `/`, must start with a known prefix) is what keeps this from being
    noisy, and the ALLOWLIST below is the escape hatch for any block that
    still needs one.
  * Trailing colon/line-number suffixes (`foo.rs:123`) are dropped for free:
    `:` is not a path-token character, so the tokenizer already splits
    there. Trailing periods and slashes are stripped. Glob markers truncate
    the token back to the last concrete (non-glob) path segment.

Usage:
    uv run website/scripts/docs-path-check.py
    uv run website/scripts/docs-path-check.py --docs-dir DIR --repo-root DIR
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_DOCS_DIR = REPO_ROOT / "website" / "src" / "content" / "docs"

# ---------------------------------------------------------------------------
# Known top-level path prefixes
# ---------------------------------------------------------------------------

# First path segments that mark a code-span/code-block token as "this looks
# like a repo path, go check it" rather than an arbitrary word that happens
# to contain a slash (a Redis command pair, a config key, a URL path, ...).
#
# Most of these are real top-level directories. `crates` and `ops` are not
# (they live under `frogdb-server/`) but are kept here on purpose: they're
# the shorthand docs authors reach for, and the existence check turns that
# shorthand into a caught violation instead of a silent miss.
KNOWN_TOP_LEVEL = frozenset(
    {
        "frogdb-server",
        "frogdb-operator",
        "frogctl",
        "website",
        "testing",
        "crates",
        "ops",
    }
)

# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------

# Repo-relative paths (post-normalization, exactly as they'd be reported
# below) that are allowed to not exist -- e.g. a docs page intentionally
# shows a hypothetical/future path in an example. Keep this empty unless a
# specific doc genuinely needs it; prefer fixing the doc.
ALLOWLIST: frozenset[str] = frozenset()

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})")
INLINE_SPAN_RE = re.compile(r"`([^`]+)`")
TOKEN_RE = re.compile(r"[A-Za-z0-9_.\-/*]+")


@dataclass(frozen=True)
class Candidate:
    line: int
    token: str


def normalize_token(raw: str) -> str | None:
    """Normalize a raw token; return None if it's not a path candidate."""
    token = raw.strip(".")
    if not token or "/" not in token:
        return None

    # Truncate glob markers back to the last concrete path segment.
    if "*" in token or "?" in token:
        parts = token.split("/")
        kept: list[str] = []
        for part in parts:
            if "*" in part or "?" in part:
                break
            kept.append(part)
        token = "/".join(kept)
        if not token:
            return None

    # A single trailing slash marks a directory reference (`ops/`); strip it
    # without re-requiring an internal slash -- a bare `ops/` is still a
    # valid one-segment candidate.
    token = token.rstrip("/")
    return token or None


def iter_candidates(text: str) -> list[Candidate]:
    """Walk a doc file's lines, yielding path-like candidates with line numbers.

    Text inside fenced code blocks is scanned in full; text outside fenced
    blocks is scanned only within inline `code spans`.
    """
    candidates: list[Candidate] = []
    in_fence = False
    fence_marker = ""

    for lineno, line in enumerate(text.splitlines(), start=1):
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)[0]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
            continue

        if in_fence:
            segments = [line]
        else:
            segments = INLINE_SPAN_RE.findall(line)

        for segment in segments:
            for raw in TOKEN_RE.findall(segment):
                candidates.append(Candidate(lineno, raw))

    return candidates


# ---------------------------------------------------------------------------
# Checking
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Violation:
    doc_path: Path
    line: int
    repo_path: str


def check_file(doc_path: Path, repo_root: Path) -> list[Violation]:
    text = doc_path.read_text(encoding="utf-8")
    violations: list[Violation] = []
    seen: set[tuple[int, str]] = set()

    for candidate in iter_candidates(text):
        token = normalize_token(candidate.token)
        if token is None:
            continue

        first_segment = token.split("/", 1)[0]
        if first_segment not in KNOWN_TOP_LEVEL:
            continue

        if token in ALLOWLIST:
            continue

        key = (candidate.line, token)
        if key in seen:
            continue
        seen.add(key)

        if not (repo_root / token).exists():
            violations.append(Violation(doc_path, candidate.line, token))

    return violations


def check_docs(docs_dir: Path, repo_root: Path) -> list[Violation]:
    violations: list[Violation] = []
    for doc_path in sorted(docs_dir.rglob("*")):
        if doc_path.suffix not in (".md", ".mdx"):
            continue
        violations.extend(check_file(doc_path, repo_root))
    return violations


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify repo code paths referenced in the docs actually exist."
    )
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=DEFAULT_DOCS_DIR,
        help="Directory of .md/.mdx docs to scan (default: website/src/content/docs)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repo root that referenced paths are resolved against",
    )
    args = parser.parse_args()

    docs_dir: Path = args.docs_dir.resolve()
    repo_root: Path = args.repo_root.resolve()

    if not docs_dir.is_dir():
        print(f"docs dir not found: {docs_dir}", file=sys.stderr)
        return 1

    violations = check_docs(docs_dir, repo_root)
    if not violations:
        return 0

    for violation in violations:
        try:
            rel_doc = violation.doc_path.relative_to(repo_root)
        except ValueError:
            rel_doc = violation.doc_path
        print(f"{rel_doc}:{violation.line}: {violation.repo_path}", file=sys.stderr)

    print(
        f"\n{len(violations)} doc-referenced path(s) do not exist relative to {repo_root}.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
