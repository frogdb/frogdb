#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for the locked-areas manifest parser.

Run: ./scripts/tests/test_lint_locked_areas.py   (or `just test-lint-locked-areas`)

`just lint-locked-areas` runs the parser against the real `specs/` tree, where a
green run only ever exercises the *passing* direction: it proves nothing about
whether a `DRAFT` carrying a gate, or a crate claimed by two specs, would
actually fail. Every rejection the gate exists for is pinned here against
synthetic specs and a synthetic member list, so the manifest's rules cannot go
quiet.

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on the first
failure (same shape as test_spec_lint.py).
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import locked_areas  # noqa: E402

MEMBERS = {"frogdb-txn", "frogdb-vll", "frogdb-cluster", "frogdb-cluster-runtime"}

GOOD_TXN = (
    "# Transactions\n\nStatus: LOCKED (2026-08-01)\nGate: 0.90\nCrates: frogdb-txn\n\nProse.\n"
)


def _check(specs: dict[str, str]) -> tuple[list[locked_areas.Spec], list[str]]:
    """Parse a synthetic spec dir, as (records, errors)."""
    with tempfile.TemporaryDirectory() as tmp:
        spec_dir = Path(tmp) / "specs"
        spec_dir.mkdir()
        for name, text in specs.items():
            (spec_dir / name).write_text(text)
        return locked_areas.validate(spec_dir, members=MEMBERS)


def test_a_well_formed_header_parses() -> None:
    specs, errors = _check(
        {"txn.md": GOOD_TXN, "blocking.md": "# Blocking\n\nStatus: DRAFT\n\nProse.\n"}
    )
    assert errors == [], errors
    by_area = {spec.area: spec for spec in specs}
    assert by_area["TXN"].status == "LOCKED", by_area
    assert by_area["TXN"].locked_date == "2026-08-01", by_area
    assert by_area["TXN"].gate == 0.90, by_area
    assert by_area["TXN"].crates == ["frogdb-txn"], by_area
    assert by_area["BLOCKING"].status == "DRAFT", by_area
    assert by_area["BLOCKING"].gate is None and by_area["BLOCKING"].crates == [], by_area


def test_a_spec_without_status_fails() -> None:
    _, errors = _check({"blocking.md": "# Blocking\n\nEvery way a blocking command can end.\n"})
    assert len(errors) == 2, errors  # the prose line is not a key, and Status is absent
    assert any("blocking.md" in e and "no `Status:` key" in e for e in errors), errors


def test_a_draft_with_a_gate_fails() -> None:
    _, errors = _check({"memory.md": "# Memory\n\nStatus: DRAFT\nGate: 0.70\n\nProse.\n"})
    assert len(errors) == 1, errors
    assert "memory.md:4" in errors[0] and "`Gate:`" in errors[0], errors
    assert "DRAFT" in errors[0], errors


def test_a_locked_spec_without_crates_fails() -> None:
    _, errors = _check(
        {"txn.md": "# Transactions\n\nStatus: LOCKED (2026-08-01)\nGate: 0.90\n\nProse.\n"}
    )
    assert len(errors) == 1, errors
    assert "txn.md" in errors[0] and "`Crates:`" in errors[0], errors


def test_a_crate_named_by_two_specs_fails() -> None:
    other = "# VLL\n\nStatus: LOCKED (2026-08-01)\nGate: 0.90\nCrates: frogdb-txn\n\nProse.\n"
    _, errors = _check({"txn.md": GOOD_TXN, "vll.md": other})
    assert len(errors) == 1, errors
    assert "vll.md" in errors[0] and "txn.md" in errors[0], errors
    assert "frogdb-txn" in errors[0], errors


def test_a_crate_that_is_not_a_workspace_member_fails() -> None:
    spec = (
        "# Cluster\n\nStatus: LOCKED (2026-08-05)\nGate: 0.80\nCrates: frogdb-clustre\n\nProse.\n"
    )
    _, errors = _check({"cluster.md": spec})
    assert len(errors) == 1, errors
    assert "cluster.md:5" in errors[0] and "frogdb-clustre" in errors[0], errors
    assert "workspace member" in errors[0], errors


def test_the_crate_path_form_is_reserved() -> None:
    spec = "# Cluster\n\nStatus: LOCKED (2026-08-05)\nGate: 0.80\nCrates: frogdb-cluster/src/slot\n\nProse.\n"
    _, errors = _check({"cluster.md": spec})
    assert len(errors) == 1, errors
    assert "cluster.md:5" in errors[0] and "reserved" in errors[0], errors


def test_prose_on_the_status_line_fails() -> None:
    """The old header shape: `Status: LOCKED (date) — gate passed ...`."""
    spec = "# Transactions\n\nStatus: LOCKED (2026-08-01) — Phase 1 gate passed\nGate: 0.90\nCrates: frogdb-txn\n"
    _, errors = _check({"txn.md": spec})
    assert len(errors) == 1, errors
    assert "txn.md:3" in errors[0] and "prose belongs below" in errors[0], errors


def test_a_gate_outside_the_unit_interval_fails() -> None:
    for value in ("1.5", "0", "0.0", "ninety"):
        spec = f"# VLL\n\nStatus: LOCKED (2026-08-01)\nGate: {value}\nCrates: frogdb-vll\n"
        _, errors = _check({"vll.md": spec})
        assert len(errors) == 1, (value, errors)
        assert "(0, 1]" in errors[0], (value, errors)


def test_an_unknown_header_key_fails() -> None:
    spec = "# VLL\n\nStatus: DRAFT\nOwner: nobody\n"
    _, errors = _check({"vll.md": spec})
    assert len(errors) == 1, errors
    assert "unknown header key `Owner:`" in errors[0], errors


def test_the_real_manifest_loads_and_answers_lookups() -> None:
    """The live tree: the gate `just mutants-gate <crate>` reads must resolve."""
    specs = locked_areas.load()
    assert specs, "no specs found — every check above would be vacuous"
    assert locked_areas.lookup_crate("frogdb-cluster").gate == 0.80
    try:
        locked_areas.lookup_crate("frogdb-server")
    except locked_areas.ManifestError as exc:
        assert "not in the mutation perimeter" in str(exc), exc
    else:
        raise AssertionError("frogdb-server is not a locked crate but the lookup succeeded")


def test_member_paths_locate_every_locked_crate() -> None:
    """The live tree: `just mutants-diff` needs a directory per locked crate."""
    paths = locked_areas.member_paths()
    assert set(paths) == locked_areas.workspace_members(), (
        "member_paths and workspace_members disagree on the member set"
    )
    locked = [crate for spec in locked_areas.load() if spec.is_locked for crate in spec.crates]
    assert locked, "no locked crates — the check below would be vacuous"
    for crate in locked:
        assert crate in paths, (crate, sorted(paths))
        manifest = locked_areas.ROOT / paths[crate] / "Cargo.toml"
        assert manifest.is_file(), (crate, paths[crate])


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Invoke the script the way `just mutants-diff` does — as a subprocess."""
    return subprocess.run(
        [str(locked_areas.ROOT / "scripts" / "locked_areas.py"), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_crate_path_cli_prints_the_directory() -> None:
    """`just mutants-diff` shells out for this: stdout is the path, and only the path."""
    done = _run_cli("--crate-path", "frogdb-txn")
    assert done.returncode == 0, (done.returncode, done.stderr)
    assert done.stdout.strip() == "frogdb-server/crates/txn", done.stdout


def test_crate_path_cli_rejects_a_non_member() -> None:
    """A typo must stop the recipe, not hand `git diff` an empty pathspec."""
    done = _run_cli("--crate-path", "no-such-crate")
    assert done.returncode == 1, (done.returncode, done.stdout)
    assert done.stdout.strip() == "", done.stdout
    assert "no-such-crate" in done.stderr, done.stderr


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
