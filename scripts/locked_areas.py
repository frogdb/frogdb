#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""The locked-areas manifest: one parser for the `specs/*.md` header key block.

"Locked" is the campaign's central claim — *behavior in this area cannot change
without a test failing* — and until this module existed no script read it. The
area's gate threshold, the crates the gate covers, and whether the area is
locked at all lived as prose in `CLAUDE.md` and as free text on line 3 of each
spec, so `just mutants-gate frogdb-cluster 0.90` was accepted without complaint
and a crate could leave the perimeter with nothing noticing.

The spec *is* the contract, so the contract carries its own terms. Every
`specs/<area>.md` opens with a key block: the lines after the H1, up to the
first blank line, each `Key: value`.

    # Persistence — failure modes

    Status: LOCKED (2026-08-02)
    Gate: 0.85
    Crates: frogdb-persistence, frogdb-recovery

Rules, all enforced here and nowhere else (`scripts/lint-locked-areas.py` is
this module plus an exit code):

* `Status:` is required on every spec; `LOCKED` or `DRAFT`, nothing else. A
  parenthesised date may follow, and is informational.
* `LOCKED` implies `Gate:` and `Crates:`; `DRAFT` forbids both. There is no
  third state: a crate with a mutation gate has a contract, and that is what
  LOCKED means, so a draft carrying a gate is a lock that forgot to say so.
* `Gate:` is a decimal in `(0, 1]`, applying to every crate the spec lists.
* `Crates:` is the **mutation perimeter** — what `cargo mutants -p` runs on,
  not the witness search space (that is `NEXTEST_CRATES` in `spec-lint.py`, a
  spec-lint implementation detail). Each entry is a workspace member and
  appears in exactly one spec.
* A `crate/path` entry (a sub-tree gate) is *reserved*: rejected until a spec
  needs one, so the extension is one line rather than a redesign.

Consumers: `just lint-locked-areas`, `just locked-areas`, `mutants-gate.py`,
and `just mutants-diff`. Importable as a module (`import locked_areas` works
from a sibling `uv run --script` file — the script's directory is `sys.path[0]`)
so none of them re-derives the header regex.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPEC_DIR = ROOT / "specs"

LOCKED = "LOCKED"
DRAFT = "DRAFT"

# `Status: LOCKED (2026-08-02)` — one key per line, the value is the rest.
KEY_RE = re.compile(r"^([A-Z][A-Za-z-]*):[ \t]+(\S.*)$")
# `LOCKED` / `DRAFT`, optionally dated. Trailing prose is an error: the prose
# that used to live here belongs below the block, where the lint ignores it.
STATUS_RE = re.compile(r"^(LOCKED|DRAFT)(?:\s+\((\d{4}-\d{2}-\d{2})\))?$")
GATE_RE = re.compile(r"^\d*\.?\d+$")

KNOWN_KEYS = ("Status", "Gate", "Crates")


class ManifestError(Exception):
    """A spec header the manifest cannot be trusted to describe."""


@dataclass
class Spec:
    """One `specs/<area>.md` as the manifest sees it."""

    area: str
    status: str
    path: Path
    locked_date: str | None = None
    gate: float | None = None
    crates: list[str] = field(default_factory=list)

    @property
    def is_locked(self) -> bool:
        return self.status == LOCKED

    def as_dict(self) -> dict[str, object]:
        return {
            "area": self.area,
            "status": self.status,
            "locked_date": self.locked_date,
            "gate": self.gate,
            "crates": list(self.crates),
            "path": str(self.path.relative_to(ROOT))
            if self.path.is_relative_to(ROOT)
            else str(self.path),
        }


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def workspace_members(root: Path = ROOT) -> set[str]:
    """Every workspace member's *package name*.

    `Cargo.toml` lists members by path (`frogdb-server/crates/txn`), while a
    spec names them the way `cargo mutants -p` does, so the package name is
    read out of each member's own manifest rather than guessed from the path.
    """
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    names: set[str] = set()
    for member in manifest.get("workspace", {}).get("members", []):
        paths = sorted(root.glob(member)) if "*" in member else [root / member]
        for path in paths:
            cargo = path / "Cargo.toml"
            if not cargo.is_file():
                continue
            name = tomllib.loads(cargo.read_text()).get("package", {}).get("name")
            if name:
                names.add(name)
    return names


def parse_header(path: Path) -> tuple[list[tuple[int, str, str]], list[str]]:
    """The header key block of one spec, as `(line, key, value)` triples.

    The block is the run of non-blank lines after the H1, up to the first blank
    line. Every line in it must be `Key: value`; a spec whose first paragraph
    is prose has no manifest entry at all, which is the error the caller wants
    to hear (`Status:` is required everywhere).
    """
    errors: list[str] = []
    lines = path.read_text().splitlines()
    if not lines or not lines[0].startswith("# "):
        return [], [f"{rel(path)}:1: does not start with an H1 title"]

    index = 1
    while index < len(lines) and not lines[index].strip():
        index += 1

    keys: list[tuple[int, str, str]] = []
    while index < len(lines) and lines[index].strip():
        raw = lines[index]
        match = KEY_RE.match(raw.strip())
        if not match:
            errors.append(
                f"{rel(path)}:{index + 1}: header key block line is not `Key: value`: {raw.strip()!r} "
                "— the block ends at the first blank line; prose goes below it"
            )
        else:
            keys.append((index + 1, match.group(1), match.group(2).strip()))
        index += 1
    return keys, errors


def parse_spec(path: Path, members: set[str], errors: list[str]) -> Spec | None:
    """One spec's manifest record, or None if its header cannot be trusted."""
    keys, header_errors = parse_header(path)
    errors.extend(header_errors)

    seen: dict[str, tuple[int, str]] = {}
    for line, key, value in keys:
        if key not in KNOWN_KEYS:
            errors.append(
                f"{rel(path)}:{line}: unknown header key `{key}:` "
                f"(known keys: {', '.join(KNOWN_KEYS)})"
            )
            continue
        if key in seen:
            errors.append(f"{rel(path)}:{line}: `{key}:` given twice")
            continue
        seen[key] = (line, value)

    if "Status" not in seen:
        errors.append(
            f"{rel(path)}: no `Status:` key in the header block — every spec declares "
            f"`Status: {LOCKED} (<date>)` or `Status: {DRAFT}`"
        )
        return None

    status_line, status_value = seen["Status"]
    status_match = STATUS_RE.match(status_value)
    if not status_match:
        errors.append(
            f"{rel(path)}:{status_line}: `Status: {status_value}` is not "
            f"`{LOCKED}`/`{DRAFT}` (optionally dated `{LOCKED} (YYYY-MM-DD)`) — "
            "prose belongs below the key block"
        )
        return None
    status, locked_date = status_match.group(1), status_match.group(2)

    spec = Spec(area=path.stem.upper(), status=status, path=path, locked_date=locked_date)

    if status == LOCKED:
        for key in ("Gate", "Crates"):
            if key not in seen:
                errors.append(
                    f"{rel(path)}: `Status: {LOCKED}` requires a `{key}:` key — a locked "
                    "area declares its mutation gate and the crates it covers"
                )
    else:
        for key in ("Gate", "Crates"):
            if key in seen:
                errors.append(
                    f"{rel(path)}:{seen[key][0]}: `Status: {DRAFT}` forbids `{key}:` — a "
                    "draft with a gate is a lock that forgot to say so"
                )

    if "Gate" in seen and status == LOCKED:
        gate_line, gate_value = seen["Gate"]
        gate = float(gate_value) if GATE_RE.match(gate_value) else None
        if gate is None or not 0 < gate <= 1:
            errors.append(
                f"{rel(path)}:{gate_line}: `Gate: {gate_value}` is not a decimal in (0, 1]"
            )
        else:
            spec.gate = gate

    if "Crates" in seen and status == LOCKED:
        crates_line, crates_value = seen["Crates"]
        spec.crates = parse_crates(path, crates_line, crates_value, members, errors)

    return spec


def parse_crates(
    path: Path, line: int, value: str, members: set[str], errors: list[str]
) -> list[str]:
    """The comma-separated mutation perimeter of one spec."""
    crates: list[str] = []
    for entry in (part.strip() for part in value.split(",")):
        if not entry:
            errors.append(f"{rel(path)}:{line}: empty entry in `Crates:`")
            continue
        if "/" in entry:
            errors.append(
                f"{rel(path)}:{line}: `Crates:` entry {entry!r} — the `crate/path` form "
                "(a sub-tree gate) is reserved and not implemented yet; name the whole crate"
            )
            continue
        if entry in crates:
            errors.append(f"{rel(path)}:{line}: `Crates:` names {entry!r} twice")
            continue
        if members and entry not in members:
            errors.append(
                f"{rel(path)}:{line}: `Crates:` names {entry!r}, which is not a workspace "
                "member (see the `members` list in Cargo.toml)"
            )
            continue
        crates.append(entry)
    return crates


def validate(
    spec_dir: Path = SPEC_DIR, members: set[str] | None = None
) -> tuple[list[Spec], list[str]]:
    """Every spec's manifest record, plus every reason to distrust the set."""
    errors: list[str] = []
    if members is None:
        members = workspace_members()

    paths = sorted(spec_dir.glob("*.md"))
    if not paths:
        return [], [f"no spec files under {rel(spec_dir)}"]

    specs: list[Spec] = []
    for path in paths:
        spec = parse_spec(path, members, errors)
        if spec is not None:
            specs.append(spec)

    owner: dict[str, Spec] = {}
    for spec in specs:
        for crate in spec.crates:
            if crate in owner:
                errors.append(
                    f"{rel(spec.path)}: `Crates:` names {crate!r}, already claimed by "
                    f"{rel(owner[crate].path)} — a crate belongs to exactly one spec"
                )
                continue
            owner[crate] = spec
    return specs, errors


def load(spec_dir: Path = SPEC_DIR, members: set[str] | None = None) -> list[Spec]:
    """Every spec's manifest record. Raises `ManifestError` if any spec is bad."""
    specs, errors = validate(spec_dir, members)
    if errors:
        raise ManifestError("\n".join(errors))
    return specs


def lookup_crate(crate: str, spec_dir: Path = SPEC_DIR) -> Spec:
    """The locked spec that owns `crate`, or `ManifestError` naming the fix."""
    for spec in load(spec_dir):
        if spec.is_locked and crate in spec.crates:
            return spec
    raise ManifestError(
        f"{crate} is not in the mutation perimeter: no locked spec under "
        f"{rel(spec_dir)} names it in its `Crates:` header key (see `just locked-areas`)"
    )


def format_table(specs: list[Spec]) -> str:
    rows = [
        (
            spec.area.lower(),
            spec.status + (f" ({spec.locked_date})" if spec.locked_date else ""),
            f"{spec.gate:.2f}" if spec.gate is not None else "-",
            ", ".join(spec.crates) or "-",
        )
        for spec in specs
    ]
    header = ("area", "status", "gate", "crates")
    widths = [max(len(row[i]) for row in [header, *rows]) for i in range(4)]
    out = []
    for row in [header, *rows]:
        out.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip())
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="The locked-areas manifest (specs/*.md headers).")
    ap.add_argument("--json", action="store_true", help="print the manifest as JSON")
    ap.add_argument(
        "--check-crate",
        metavar="CRATE",
        help="exit 0 if the crate is inside the mutation perimeter, 1 with a message if not",
    )
    args = ap.parse_args()

    specs, errors = validate()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if args.check_crate:
        try:
            spec = lookup_crate(args.check_crate)
        except ManifestError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        print(f"{args.check_crate}: {spec.area} area, gate {spec.gate:.2f} ({rel(spec.path)})")
        return 0

    if args.json:
        print(json.dumps([spec.as_dict() for spec in specs], indent=2))
    else:
        print(format_table(specs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
