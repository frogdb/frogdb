#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Insert `// FM-CLUSTER-NNN` tag comments above every test the cluster spec names.

Reads `.scratch/hardening/specs/cluster-failure-modes.md`, builds a
test-name -> [FM ids] map from the `Forced by` rows, locates each `fn <name>(`
under `frogdb-server/crates/`, and inserts (or merges into) a tag line directly
above the attribute block preceding the function.

Idempotent: re-running merges rather than duplicating.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / ".scratch/hardening/specs/cluster-failure-modes.md"
SRC_ROOTS = [ROOT / "frogdb-server/crates"]

SECTION_RE = re.compile(r"^##\s+(FM-CLUSTER-\d+)\b")
FORCED_RE = re.compile(r"^\|\s*Forced by\s*\|(.*)\|\s*$")
NAME_RE = re.compile(r"`([A-Za-z_][A-Za-z0-9_]*)`")
TAG_LINE_RE = re.compile(r"^\s*//\s*FM-[A-Z]+-\d+(?:\s*,\s*FM-[A-Z]+-\d+)*\s*$")
PREAMBLE_RE = re.compile(r"^\s*(//|#!?\[|$)")


def spec_map() -> dict[str, list[str]]:
    """test name -> sorted FM ids that name it."""
    out: dict[str, set[str]] = defaultdict(set)
    current = None
    for line in SPEC.read_text().splitlines():
        m = SECTION_RE.match(line)
        if m:
            current = m.group(1)
            continue
        m = FORCED_RE.match(line)
        if m and current:
            cell = m.group(1)
            if "MISSING" in cell:
                continue
            for name in NAME_RE.findall(cell):
                out[name].add(current)
    return {k: sorted(v) for k, v in out.items()}


def rust_files() -> list[Path]:
    files = []
    for root in SRC_ROOTS:
        files.extend(p for p in root.rglob("*.rs") if "target" not in p.parts)
    return files


def main() -> int:
    wanted = spec_map()
    # name -> (path, fn line index)
    located: dict[str, tuple[Path, int]] = {}
    fn_res = {
        n: re.compile(rf"^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+{n}\s*[(<]") for n in wanted
    }

    for path in rust_files():
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines):
            for name, rx in fn_res.items():
                if rx.match(line):
                    if name in located:
                        print(
                            f"DUPLICATE fn {name}: {located[name][0]} and {path}", file=sys.stderr
                        )
                    located[name] = (path, i)

    missing = sorted(set(wanted) - set(located))
    for name in missing:
        print(f"NOT FOUND: {name} (named by {', '.join(wanted[name])})", file=sys.stderr)

    # Group edits per file so line indices stay valid (apply bottom-up).
    per_file: dict[Path, list[tuple[int, list[str]]]] = defaultdict(list)
    for name, (path, idx) in located.items():
        per_file[path].append((idx, wanted[name]))

    edited = 0
    for path, edits in per_file.items():
        lines = path.read_text().splitlines()
        for fn_idx, ids in sorted(edits, reverse=True):
            # Walk back over the attribute/comment preamble to the insertion point.
            insert_at = fn_idx
            existing_tag = None
            j = fn_idx - 1
            while j >= 0 and PREAMBLE_RE.match(lines[j]):
                if TAG_LINE_RE.match(lines[j]):
                    existing_tag = j
                    break
                if lines[j].strip().startswith("#["):
                    insert_at = j
                    j -= 1
                    continue
                if lines[j].strip().startswith("//") or not lines[j].strip():
                    # A doc/plain comment or blank line: stop, the tag goes
                    # below it and above the attributes we already passed.
                    break
                break
            indent = re.match(r"^\s*", lines[fn_idx]).group(0)
            if existing_tag is not None:
                have = set(re.findall(r"FM-[A-Z]+-\d+", lines[existing_tag]))
                merged = sorted(have | set(ids))
                new = f"{indent}// {', '.join(merged)}"
                if lines[existing_tag] != new:
                    lines[existing_tag] = new
                    edited += 1
            else:
                lines.insert(insert_at, f"{indent}// {', '.join(ids)}")
                edited += 1
        path.write_text("\n".join(lines) + "\n")

    print(f"{len(wanted)} names in spec, {len(located)} located, {edited} tag lines written")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
