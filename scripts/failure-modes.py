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

A third, smaller direction: a spec's rows cite invariant-catalog entries by id
(`INV-REF-1`, in the optional `Catalog` field or in prose), and the catalogs are
Rust. Any `INV-<something>` a spec mentions must be defined in *that area's*
catalog (`INVARIANT_CATALOGS` below), so neither a renamed or deleted entry nor
a citation borrowed from another area's vocabulary can leave the spec pointing
at nothing.

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
# The invariant catalogs, one per area: the vocabulary of `INV-*` ids that
# area's spec may cite. Keyed by the `FM-<AREA>-NNN` prefix, which is also the
# spec filename's stem upper-cased.
#
# Per-area rather than one shared vocabulary because an invariant is a claim
# about *one* area's state projection: `INV-HANDOFF-1` is a statement about
# cluster topology and means nothing to a replication row, so a replication row
# citing it is a mistake the lint has to be able to name. An area with no entry
# here has no catalog yet, and any `INV-*` its spec cites is an error until one
# lands. A dict, not a framework: the persistence and txn ports are one line.
INVARIANT_CATALOGS = {
    "CLUSTER": REPO / "frogdb-server/crates/cluster/src/invariants.rs",
    "REPLICATION": REPO / "frogdb-server/crates/replication/src/invariants.rs",
}

# Crates whose tests a failure-mode row may name. `cargo nextest list` over
# these compiles their test binaries: seconds warm for frogdb-txn/frogdb-vll/
# frogdb-recovery, ~15-25s for frogdb-server (the big `main` binary plus the
# per-concern `cluster_*` binaries).
# frogdb-persistence, frogdb-recovery and frogdb-core carry the storage-side
# rows (see persistence-failure-modes.md); frogdb-replication carries the
# full-sync wire rows (see replication-failure-modes.md); frogdb-cluster and
# frogdb-cluster-runtime carry the topology/slot/failover rows (see
# cluster-failure-modes.md).
# Pass --nextest-output to reuse a listing produced by an earlier step.
NEXTEST_CRATES = [
    "frogdb-txn",
    "frogdb-vll",
    "frogdb-server",
    "frogdb-persistence",
    "frogdb-recovery",
    "frogdb-core",
    "frogdb-replication",
    "frogdb-replication-runtime",
    "frogdb-cluster",
    "frogdb-cluster-runtime",
    "frogdb-config",
    "frogdb-telemetry",
]

# Feature-gated suites a row may also name, listed separately because their
# tests do not exist in the default feature resolution. The turmoil simulations
# (`frogdb-server/crates/server/tests/simulation.rs`) are the end-to-end forcing
# tests for the replication wire contracts.
NEXTEST_FEATURE_VARIANTS = [
    ("frogdb-server", "turmoil"),
]

# `## FM-TXN-001 — title`
HEADING_RE = re.compile(r"^##\s+(FM-([A-Z]+)-(\d+))\s*(?:[—-]\s*(.*))?$")
# `| Field | Value |`
ROW_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*(.*?)\s*\|\s*$")
FM_TAG_RE = re.compile(r"\bFM-([A-Z]+)-(\d+)\b")
# A *tag* is a comment line that is nothing but ids — `// FM-TXN-004`,
# `// FM-TXN-009, FM-TXN-022`, `/// FM-BLOCKING-005`. A comment that merely
# *mentions* an id in prose ("the complement of FM-REPLICATION-018") is a
# cross-reference, not a claim that this item forces that row, and must not be
# linted as one: the invariants are worth citing where the code implements
# them, and treating a citation as a tag makes the lint punish good comments.
FM_TAG_LINE_RE = re.compile(r"^\s*//[/!]?\s*FM-[A-Z]+-\d+(?:\s*,?\s*FM-[A-Z]+-\d+)*\s*$")
FN_RE = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")
BACKTICKED_RE = re.compile(r"`([^`]+)`")
# `MISSING ([gap: 03-disk-full-injection.md](../issues/03-disk-full-injection.md))`
MISSING_GAP_RE = re.compile(r"MISSING\s*\(\[gap:[^\]]*\]\(([^)]+)\)\)")

# An invariant-catalog citation anywhere in a spec: `INV-REF-1`, `INV-REF-3B`.
# The glob form `INV-*` the prose uses to talk *about* the ids does not match,
# because a segment must have at least one alphanumeric character.
INV_REF_RE = re.compile(r"\bINV-[A-Z0-9]+(?:-[A-Z0-9]+)*")
# `pub static CATALOG: &[Invariant] = &[` … `];`
CATALOG_START_RE = re.compile(r"^pub static CATALOG\b")
CATALOG_ID_RE = re.compile(r'^\s*id:\s*"(INV-[A-Z0-9-]+)"\s*,')

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


def rel(path: Path) -> str:
    """`path` relative to the repo root, or as given when it is outside it.

    Only the fixture test drives this script with paths outside the repo; the
    fallback keeps its messages readable instead of raising.
    """
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


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


def load_catalog_ids(path: Path, errors: list[str]) -> set[str]:
    """The `INV-*` ids the invariant catalog defines.

    Bounded to the `CATALOG` static, so the throwaway entries the catalog's own
    unit tests build (`INV-TEST-HARD` and friends) cannot widen the vocabulary a
    spec is checked against.
    """
    if not path.is_file():
        errors.append(f"invariant catalog missing: {rel(path)}")
        return set()

    ids: set[str] = set()
    inside = False
    for line in path.read_text().splitlines():
        if not inside:
            inside = bool(CATALOG_START_RE.match(line))
            continue
        if line.startswith("];"):
            break
        match = CATALOG_ID_RE.match(line)
        if match:
            ids.add(match.group(1))

    if not ids:
        errors.append(
            f"{rel(path)}: no `INV-*` ids found in `CATALOG` — either the "
            'static was renamed or its entries no longer spell `id: "INV-…"`, and '
            "either way the vocabulary check below would pass vacuously"
        )
    return ids


@dataclass(frozen=True)
class Catalog:
    """One area's invariant vocabulary, and where it came from."""

    area: str
    path: Path
    ids: frozenset[str]


def load_catalogs(paths: dict[str, Path], errors: list[str]) -> dict[str, Catalog]:
    """Every registered area's catalog, keyed by area."""
    return {
        area: Catalog(area, path, frozenset(load_catalog_ids(path, errors)))
        for area, path in sorted(paths.items())
    }


def check_invariant_vocabulary(
    spec_dir: Path, catalogs: dict[str, Catalog], errors: list[str]
) -> dict[str, int]:
    """Fail on an `INV-*` id a spec cites and its own area's catalog does not define.

    Two ways to get this wrong, both errors:

      dangling     the id exists nowhere — a renamed or deleted entry. The
                   cross-reference the `Catalog` field promises is only worth
                   reading if it cannot rot silently.
      cross-area   the id exists, but in another area's catalog. An invariant
                   is a pure function of one area's state projection, so a
                   replication row citing `INV-HANDOFF-1` is claiming something
                   nothing checks; the message names the owning area so the
                   fix is obvious.

    The unused direction is not checked; an entry no row cites is fine (see
    `INV-SLOT-1` and `INV-GATE-1`, which generalize no row in their own area's
    spec on purpose).

    Returns the citation count per area, for the summary line.
    """
    counts: dict[str, int] = {}
    owners = {ref: cat for _, cat in sorted(catalogs.items()) for ref in cat.ids}
    for spec in sorted(spec_dir.glob("*-failure-modes.md")):
        area = spec.name.removesuffix("-failure-modes.md").upper()
        own = catalogs.get(area)
        for lineno, line in enumerate(spec.read_text().splitlines(), start=1):
            for ref in INV_REF_RE.findall(line):
                counts[area] = counts.get(area, 0) + 1
                if own is not None and ref in own.ids:
                    continue
                where = f"{rel(spec)}:{lineno}: cites `{ref}`, which"
                owner = owners.get(ref)
                if owner is not None:
                    errors.append(
                        f"{where} belongs to the {owner.area} catalog ({rel(owner.path)}) — a "
                        f"row may only cite invariants over its own area's state"
                    )
                elif own is None:
                    errors.append(
                        f"{where} cannot be resolved: the {area} area has no invariant "
                        "catalog registered in `INVARIANT_CATALOGS` (scripts/failure-modes.py)"
                    )
                else:
                    errors.append(f"{where} {rel(own.path)} does not define")
    return counts


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


def run_listing(cmd: list[str]) -> str:
    """`cargo nextest list` output, or exit with its error."""
    proc = subprocess.run(
        cmd, cwd=REPO, env=cargo_env(), capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        sys.exit(f"{' '.join(cmd)} failed:\n{proc.stderr}")
    return proc.stdout


def load_test_paths(nextest_output: Path | None) -> set[str]:
    """The set of test paths `cargo nextest list` knows about.

    Each listing line is `<binary-id> <test-path>` (or an indented test path
    under a binary heading); the test path is the last field either way.

    `--run-ignored all` because a nightly-budget test (`#[ignore]`d, run by a
    scheduled workflow or a `just` recipe) still forces the failure mode its
    row names; without it such a row reads as a typo.
    """
    if nextest_output is not None:
        text = nextest_output.read_text()
    else:
        cmd = ["cargo", "nextest", "list", "--color", "never", "--run-ignored", "all"]
        for crate in NEXTEST_CRATES:
            cmd += ["-p", crate]
        text = run_listing(cmd)
        # Feature-gated suites are invisible to the default listing, so a row
        # that names one would look like a typo. Each variant is a separate
        # feature resolution and therefore a separate build fingerprint; they
        # coexist in `target/` rather than evicting each other, so the cost is
        # one build the first time and seconds afterwards.
        for crate, feature in NEXTEST_FEATURE_VARIANTS:
            text += run_listing(
                [
                    *["cargo", "nextest", "list", "--color", "never", "--run-ignored", "all"],
                    *["-p", crate, "--features", feature],
                ]
            )

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
    """Whether `name` names a listed test.

    Three shapes, because `cargo nextest list` renders a test's identity
    differently depending on how it was declared:

    * the full path (`integration_replication::test_psync_initial_request`);
    * a trailing segment of it, which is how a spec row names a test — bare, so
      that moving a test between modules is not a spec edit;
    * the *second to last* segment, which is where an `#[rstest]` function's
      name lands: each `#[case]` becomes its own listed test with the case name
      appended (`…::test_wait_no_replicas::in_memory`). A parameterized test is
      still one function with one `// FM-…` tag, so the row names the function
      and every case counts as forcing it.
    """
    suffix = "::" + name
    infix = "::" + name + "::"
    return any(path == name or path.endswith(suffix) or infix in path for path in test_paths)


def scan_tags(roots: list[Path], errors: list[str]) -> list[Tag]:
    """Collect every `// FM-<AREA>-NNN` tag comment and the test it annotates.

    Only a comment line consisting *solely* of ids is a tag — see
    [`FM_TAG_LINE_RE`]. Prose that cites an id is left alone.
    """
    tags: list[Tag] = []
    for root in roots:
        for path in sorted(root.rglob("*.rs")):
            if "target" in path.parts:
                continue
            lines = path.read_text(errors="replace").splitlines()
            for index, line in enumerate(lines):
                if not FM_TAG_LINE_RE.match(line):
                    continue
                matches = FM_TAG_RE.findall(line)
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
    catalogs = load_catalogs(INVARIANT_CATALOGS, errors)
    citations = check_invariant_vocabulary(args.spec_dir, catalogs, errors)
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
    breakdown = ", ".join(
        f"{area} {citations.get(area, 0)}/{len(cat.ids)}" for area, cat in sorted(catalogs.items())
    )
    print(
        f"OK: {len(modes)} failure modes ({', '.join(areas)}), "
        f"{references} test references, {len(tags)} tags, "
        f"{sum(citations.values())} invariant citations over "
        f"{sum(len(cat.ids) for cat in catalogs.values())} catalog entries ({breakdown})"
    )


if __name__ == "__main__":
    main()
