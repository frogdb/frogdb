#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: no figment `.nested()` on a config source.

Hardening-2 W1 rule C6 (`.scratch/hardening-2/PRD.md` §3.1). `.nested()` turns a
TOML file's top-level tables into figment *profiles* that an `extract()` under
`Profile::Default` never reads. `config/loader.rs:91` calls it on a discovered
`./frogdb.toml`, so that file is silently dropped (round-2 issue 49) — proven a
bug, not a design choice, by the `--config <path>` branch two lines up that omits
it. `.nested()` appears exactly once in the workspace, and that once is the bug.

The rule is a workspace-wide ban on `.nested(`. Because the *fix* (issue 49) has
not landed yet, a hard failure here would block every commit on unrelated work,
so the one known site rides in the named-gap warn-not-fail idiom
(`spec-lint.py:20-26`): it WARNS while its `[gap: <issue>](<link>)` link
resolves to a real issue file, and the moment issue 49 removes the call the ban
becomes hard (the site count drops to zero, the now-stale allowlist entry fails,
and its removal leaves a plain zero-tolerance grep ban).

Any `.nested(` outside the allowlisted file is a hard failure today.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_ROOT = ROOT / "frogdb-server"

NESTED = re.compile(r"\.nested\s*\(")

# The base the gap link is resolved against (the campaign-2 directory, matching
# how the PRD authored the relative link).
GAP_BASE = ROOT / ".scratch" / "hardening-2"

# relative-path -> (expected count, gap link relative to GAP_BASE, reason).
# Count-pinned: a mismatch (fixed → 0, or a new `.nested(` in the same file)
# fails, so the entry cannot go stale silently.
ALLOWLIST: dict[str, tuple[int, str, str]] = {
    "frogdb-server/crates/server/src/config/loader.rs": (
        1,
        "../testing-improvements-round2/issues/open/49-discovered-config-file-silently-ignored.md",
        "MISSING gap: a discovered ./frogdb.toml is silently ignored because "
        ".nested() files its tables under non-default figment profiles. The ban "
        "is real; the fix is round-2 issue 49, not yet landed.",
    ),
}


def hits(rel_display: str, path: Path) -> list[int]:
    return [i + 1 for i, line in enumerate(path.read_text().splitlines()) if NESTED.search(line)]


def main() -> int:
    found: dict[str, list[int]] = {}
    for path in sorted(SCAN_ROOT.rglob("*.rs")):
        if "target" in path.parts:
            continue
        lines = hits(str(path), path)
        if lines:
            found[str(path.relative_to(ROOT))] = lines

    status = 0

    # Forward: a `.nested(` in a non-allowlisted file is a hard failure.
    unexpected = {f: ls for f, ls in found.items() if f not in ALLOWLIST}
    if unexpected:
        print("ERROR: `.nested()` on a config source drops its top-level tables:", file=sys.stderr)
        for f in sorted(unexpected):
            for ln in unexpected[f]:
                print(f"  {f}:{ln}", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "       `.nested()` files a TOML file's tables under figment profiles that",
            file=sys.stderr,
        )
        print(
            "       an extract() under Profile::Default never reads. Merge the file",
            file=sys.stderr,
        )
        print("       without `.nested()` so its contents are actually applied.", file=sys.stderr)
        status = 1

    warnings: list[str] = []
    # Reverse + gap-link resolution for the allowlisted sites.
    for f, (expected, link, reason) in sorted(ALLOWLIST.items()):
        actual = len(found.get(f, []))
        if actual == 0:
            print(
                f"ERROR: {f}: allowlisted for {expected} `.nested()` call(s), found none — "
                "the fix landed; drop the entry so the ban is fully hard.",
                file=sys.stderr,
            )
            status = 1
            continue
        if actual != expected:
            print(
                f"ERROR: {f}: allowlisted for {expected} `.nested()` call(s), found {actual}. "
                "A new one is not covered by the gap; remove it or re-justify.",
                file=sys.stderr,
            )
            status = 1
            continue
        gap_file = (GAP_BASE / link).resolve()
        if not gap_file.is_file():
            print(
                f"ERROR: {f}: gap link does not resolve to a real issue file: {link}",
                file=sys.stderr,
            )
            status = 1
            continue
        for ln in found[f]:
            warnings.append(f"  {f}:{ln}  [gap: {link}] {reason}")

    if warnings:
        print("WARNING: `.nested()` ban has open named-gap exceptions (issue not yet fixed):")
        print("\n".join(warnings))

    if status == 0:
        print(
            f"OK: no unguarded `.nested()` on config sources ({len(warnings)} named-gap warning(s))"
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
