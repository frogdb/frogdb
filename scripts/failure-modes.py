#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Enforce failure-mode spec <-> test agreement for the hardening campaign.

Every `FM-<AREA>-NNN` row in `.scratch/hardening/specs/*-failure-modes.md`
names the test(s) that force it; every named test carries a `// FM-<AREA>-NNN`
comment at its definition site. This script checks that the two agree in both
directions, so neither can drift:

  spec -> test   every `Forced by` name resolves to a real test (verified
                 against `cargo nextest list`) and carries the FM's tag.
  test -> spec   every `FM-<AREA>-NNN` tag in the Rust sources names an FM the
                 spec defines, on a test that FM's `Forced by` row lists.

`Forced by | MISSING` is an error: a failure mode nobody forces is a gap, and
the campaign closes gaps by writing the test, not by lowering the spec.

The one exception is a mode that is real but needs machinery the campaign has
not built yet (disk-full injection, a torn-file harness). Those may write
`Forced by | MISSING ([gap: <file>](<link>))`, naming a filed issue under
`.scratch/hardening/issues/`; the link is resolved relative to the spec and the
file must exist. That form warns instead of failing, so the gap stays visible
in every lint run without blocking the spec on machinery that does not exist.

Usage:
    failure-modes.py                          # runs `cargo nextest list`
    failure-modes.py --nextest-output list.txt  # reuse a listing (CI)
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SPEC_DIR = REPO / ".scratch/hardening/specs"
SOURCE_ROOTS = [REPO / "frogdb-server/crates"]

# Crates whose tests a failure-mode row may name. `cargo nextest list` over
# these compiles their test binaries: seconds warm for frogdb-txn/frogdb-vll/
# frogdb-recovery, ~15-25s for frogdb-server (one big `main` binary).
# frogdb-persistence, frogdb-recovery and frogdb-core carry the storage-side
# rows (see persistence-failure-modes.md); frogdb-replication carries the
# full-sync wire rows (see replication-failure-modes.md).
# Pass --nextest-output to reuse a listing produced by an earlier step.
NEXTEST_CRATES = [
    "frogdb-txn",
    "frogdb-vll",
    "frogdb-server",
    "frogdb-persistence",
    "frogdb-recovery",
    "frogdb-core",
    "frogdb-replication",
]

# `## FM-TXN-001 — title`
HEADING_RE = re.compile(r"^##\s+(FM-([A-Z]+)-(\d+))\s*(?:[—-]\s*(.*))?$")
# `| Field | Value |`
ROW_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*(.*?)\s*\|\s*$")
FM_TAG_RE = re.compile(r"\bFM-([A-Z]+)-(\d+)\b")
FN_RE = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")
BACKTICKED_RE = re.compile(r"`([^`]+)`")
# `MISSING ([gap: 03-disk-full-injection.md](../issues/03-disk-full-injection.md))`
MISSING_GAP_RE = re.compile(r"MISSING\s*\(\[gap:[^\]]*\]\(([^)]+)\)\)")

# Every FM row carries the full schema; a missing field is a half-specified
# failure mode.
REQUIRED_FIELDS = (
    "Trigger",
    "Observable",
    "NOT observable",
    "Invariant",
    "Outcome variant",
    "Forced by",
    "Bug refs",
)

# What may sit between a tag comment and the `fn` it annotates: the rest of the
# item's comment/attribute block, nothing else.
PREAMBLE_RE = re.compile(r"^\s*(//|#!?\[|$)")


@dataclass
class FailureMode:
    """One `## FM-<AREA>-NNN` section of a spec."""

    id: str
    area: str
    number: int
    title: str
    spec: Path
    line: int
    tests: list[str] = field(default_factory=list)
    fields: dict[str, str] = field(default_factory=dict)

    def where(self) -> str:
        return f"{self.spec.relative_to(REPO)}:{self.line}"


@dataclass
class Tag:
    """An `FM-<AREA>-NNN` comment attached to a test function."""

    fm_id: str
    test: str
    path: Path
    line: int

    def where(self) -> str:
        return f"{self.path.relative_to(REPO)}:{self.line}"


def parse_spec(path: Path, errors: list[str]) -> list[FailureMode]:
    """Parse one `<area>-failure-modes.md` into its failure modes."""
    area = path.name.removesuffix("-failure-modes.md").upper()
    modes: list[FailureMode] = []
    current: FailureMode | None = None

    for lineno, raw in enumerate(path.read_text().splitlines(), start=1):
        heading = HEADING_RE.match(raw.strip())
        if heading:
            fm_id, fm_area, number, title = heading.groups()
            if fm_area != area:
                errors.append(
                    f"{path.relative_to(REPO)}:{lineno}: {fm_id} does not match the "
                    f"file's area prefix FM-{area}-"
                )
            if not title:
                errors.append(f"{path.relative_to(REPO)}:{lineno}: {fm_id} has no title")
            current = FailureMode(
                id=fm_id,
                area=fm_area,
                number=int(number),
                title=(title or "").strip(),
                spec=path,
                line=lineno,
            )
            modes.append(current)
            continue

        if current is None:
            continue
        row = ROW_RE.match(raw.strip())
        if not row:
            continue
        field_name, value = row.group(1), row.group(2)
        if field_name in ("Field", "---") or set(field_name) <= {"-", ":"}:
            continue
        current.fields[field_name] = value

    for mode in modes:
        for required in REQUIRED_FIELDS:
            if required not in mode.fields:
                errors.append(f"{mode.where()}: {mode.id} has no `{required}` row")
        mode.tests = parse_forced_by(mode, errors)

    return modes


def parse_forced_by(mode: FailureMode, errors: list[str]) -> list[str]:
    """Extract the backtick-wrapped test names from an FM's `Forced by` cell."""
    cell = mode.fields.get("Forced by", "")
    if not cell:
        return []
    if "MISSING" in cell:
        gap = MISSING_GAP_RE.search(cell)
        if gap is None:
            errors.append(
                f"{mode.where()}: {mode.id} ({mode.title}) is forced by no test "
                "(`Forced by | MISSING`) — write the test, or file a gap issue and "
                "cite it as `MISSING ([gap: <file>](<link>))`"
            )
            return []
        target = (mode.spec.parent / gap.group(1)).resolve()
        if not target.is_file():
            errors.append(
                f"{mode.where()}: {mode.id} cites gap issue `{gap.group(1)}`, which does not exist"
            )
            return []
        print(
            f"warning: {mode.where()}: {mode.id} ({mode.title}) is forced by no "
            f"test; tracked by {gap.group(1)}",
            file=sys.stderr,
        )
        return []
    names = [name.strip() for name in BACKTICKED_RE.findall(cell)]
    names = [name for name in names if name]
    if not names:
        errors.append(
            f"{mode.where()}: {mode.id} has an unparseable `Forced by` cell "
            f"({cell!r}) — name each test in backticks"
        )
    return names


def parse_specs(spec_dir: Path, errors: list[str]) -> list[FailureMode]:
    specs = sorted(spec_dir.glob("*-failure-modes.md"))
    if not specs:
        errors.append(f"no *-failure-modes.md under {spec_dir.relative_to(REPO)}")
        return []

    modes: list[FailureMode] = []
    for spec in specs:
        modes.extend(parse_spec(spec, errors))

    seen: dict[str, FailureMode] = {}
    for mode in modes:
        if mode.id in seen:
            errors.append(f"{mode.where()}: {mode.id} redefined (first at {seen[mode.id].where()})")
        seen[mode.id] = mode

    # Numbering is sequential per area; a gap usually means a section was
    # dropped without renumbering. Loud, but not a failure.
    by_area: dict[str, list[int]] = {}
    for mode in modes:
        by_area.setdefault(mode.area, []).append(mode.number)
    for area, numbers in by_area.items():
        expected = set(range(1, max(numbers) + 1))
        gaps = sorted(expected - set(numbers))
        if gaps:
            missing = ", ".join(f"FM-{area}-{n:03d}" for n in gaps)
            print(f"warning: gap in {area} numbering: {missing}", file=sys.stderr)

    return modes


def cargo_env() -> dict[str, str]:
    """Environment for `cargo nextest`, mirroring the Justfile's build vars."""
    env = dict(os.environ)
    # sccache is deliberately off on macOS (see the Justfile); an inherited
    # wrapper would only thrash the cache this script shares with `just`.
    env["RUSTC_WRAPPER"] = env.get("RUSTC_WRAPPER", "")
    libclang = Path("/opt/homebrew/opt/llvm/lib")
    if libclang.is_dir():
        env.setdefault("DYLD_LIBRARY_PATH", str(libclang))
    # System RocksDB, same condition as the Justfile: only where it exists.
    lib_dir = Path(os.environ.get("FROGDB_LIB_DIR", "/opt/homebrew/lib"))
    if (lib_dir / "librocksdb.a").exists() or (lib_dir / "librocksdb.dylib").exists():
        env.setdefault("ROCKSDB_LIB_DIR", str(lib_dir))
        env.setdefault("SNAPPY_LIB_DIR", str(lib_dir))
    return env


def load_test_paths(nextest_output: Path | None) -> set[str]:
    """The set of test paths `cargo nextest list` knows about.

    Each listing line is `<binary-id> <test-path>` (or an indented test path
    under a binary heading); the test path is the last field either way.
    """
    if nextest_output is not None:
        text = nextest_output.read_text()
    else:
        cmd = ["cargo", "nextest", "list", "--color", "never"]
        for crate in NEXTEST_CRATES:
            cmd += ["-p", crate]
        proc = subprocess.run(
            cmd, cwd=REPO, env=cargo_env(), capture_output=True, text=True, check=False
        )
        if proc.returncode != 0:
            sys.exit(f"cargo nextest list failed:\n{proc.stderr}")
        text = proc.stdout

    paths: set[str] = set()
    for line in text.splitlines():
        fields = line.split()
        if len(fields) not in (1, 2):
            continue
        candidate = fields[-1]
        if candidate.endswith(":"):
            # A binary heading in the indented listing format, not a test.
            continue
        paths.add(candidate)
    return paths


def resolve(name: str, test_paths: set[str]) -> bool:
    """Whether `name` names a listed test (exact path or trailing segment)."""
    suffix = "::" + name
    return any(path == name or path.endswith(suffix) for path in test_paths)


def scan_tags(roots: list[Path], errors: list[str]) -> list[Tag]:
    """Collect every `// FM-<AREA>-NNN` comment and the test it annotates."""
    tags: list[Tag] = []
    for root in roots:
        for path in sorted(root.rglob("*.rs")):
            if "target" in path.parts:
                continue
            lines = path.read_text(errors="replace").splitlines()
            for index, line in enumerate(lines):
                if not line.lstrip().startswith("//"):
                    continue
                matches = FM_TAG_RE.findall(line)
                if not matches:
                    continue
                test = annotated_fn(lines, index)
                if test is None:
                    errors.append(
                        f"{path.relative_to(REPO)}:{index + 1}: FM tag is not attached to a "
                        "test function (only comments and attributes may follow it, then `fn`)"
                    )
                    continue
                for area, number in matches:
                    tags.append(
                        Tag(
                            fm_id=f"FM-{area}-{number}",
                            test=test,
                            path=path,
                            line=index + 1,
                        )
                    )
    return tags


def annotated_fn(lines: list[str], index: int) -> str | None:
    """The name of the function a tag comment sits on.

    The tag may go above the whole doc-comment/attribute block or anywhere
    inside it; anything else between the tag and the `fn` means the tag is
    floating in prose and annotates nothing.
    """
    for line in lines[index + 1 :]:
        match = FN_RE.search(line)
        if match:
            return match.group(1)
        if not PREAMBLE_RE.match(line):
            return None
    return None


def check(modes: list[FailureMode], tags: list[Tag], test_paths: set[str]) -> list[str]:
    """Both directions of the spec <-> test agreement."""
    errors: list[str] = []
    known = {mode.id: mode for mode in modes}
    tagged: set[tuple[str, str]] = {(tag.fm_id, tag.test) for tag in tags}

    # spec -> test
    for mode in modes:
        for name in mode.tests:
            if not resolve(name, test_paths):
                errors.append(
                    f"{mode.where()}: {mode.id} names `{name}`, which no test in "
                    f"{'/'.join(NEXTEST_CRATES)} matches"
                )
                continue
            leaf = name.rsplit("::", 1)[-1]
            if (mode.id, leaf) not in tagged:
                errors.append(
                    f"{mode.where()}: {mode.id} names `{name}`, but that test carries "
                    f"no `// {mode.id}` tag at its definition site"
                )

    # test -> spec
    for tag in tags:
        mode = known.get(tag.fm_id)
        if mode is None:
            errors.append(
                f"{tag.where()}: `{tag.test}` is tagged {tag.fm_id}, which no spec defines"
            )
            continue
        if tag.test not in {name.rsplit("::", 1)[-1] for name in mode.tests}:
            errors.append(
                f"{tag.where()}: `{tag.test}` is tagged {tag.fm_id}, but that FM's "
                f"`Forced by` row does not name it ({mode.where()})"
            )

    return errors


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--spec-dir",
        type=Path,
        default=SPEC_DIR,
        help="directory of *-failure-modes.md specs",
    )
    ap.add_argument(
        "--nextest-output",
        type=Path,
        help="reuse a `cargo nextest list` listing instead of running it",
    )
    args = ap.parse_args()

    errors: list[str] = []
    modes = parse_specs(args.spec_dir, errors)
    tags = scan_tags(SOURCE_ROOTS, errors)
    test_paths = load_test_paths(args.nextest_output)
    errors += check(modes, tags, test_paths)

    if errors:
        print("FAILURE-MODE LINT: FAIL", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        sys.exit(1)

    references = sum(len(mode.tests) for mode in modes)
    areas = sorted({mode.area for mode in modes})
    print(
        f"OK: {len(modes)} failure modes ({', '.join(areas)}), "
        f"{references} test references, {len(tags)} tags"
    )


if __name__ == "__main__":
    main()
