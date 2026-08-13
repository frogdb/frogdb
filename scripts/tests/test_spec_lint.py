#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for the checks scripts/spec-lint.py cannot exercise live.

Run: ./scripts/tests/test_spec_lint.py   (or `just test-spec-lint`)

The rest of the lint is exercised every run against the real tree, but the
per-area catalog check is only exercised in its *passing* direction there: a
green run proves nothing about whether a dangling or a cross-area `INV-*`
citation would actually fail. Both failure directions are pinned here against
synthetic specs and catalogs, plus the loader's two ways of going vacuous
(missing file, renamed static).

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on the first
failure (same shape as test_continuation_lock_gate.py).
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

# spec-lint.py has hyphens, so it is not importable by name.
_SCRIPT = Path(__file__).resolve().parent.parent / "spec-lint.py"
_spec = importlib.util.spec_from_file_location("spec_lint", _SCRIPT)
assert _spec and _spec.loader
fm = importlib.util.module_from_spec(_spec)
sys.modules["spec_lint"] = fm
_spec.loader.exec_module(fm)


CLUSTER_CATALOG = """\
pub static CATALOG: &[Invariant] = &[
    Invariant {
        id: "INV-HANDOFF-1",
        tier: Tier::Hard,
    },
    Invariant {
        id: "INV-SLOT-1",
        tier: Tier::Hard,
    },
];

#[cfg(test)]
mod tests {
    // A throwaway the catalog's own unit tests build. It must not widen the
    // vocabulary a spec is checked against.
    const FAKE: Invariant = Invariant {
        id: "INV-TEST-HARD",
        tier: Tier::Hard,
    };
}
"""

REPLICATION_CATALOG = """\
pub static CATALOG: &[Invariant] = &[
    Invariant {
        id: "INV-REPLID-1",
        tier: Tier::Hard,
    },
    Invariant {
        id: "INV-GATE-1",
        tier: Tier::Hard,
    },
];
"""


def _tree(specs: dict[str, str], catalogs: dict[str, str]):
    """A temp spec dir plus temp catalog files, as (spec_dir, {area: path})."""
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    spec_dir = root / "specs"
    spec_dir.mkdir()
    for name, text in specs.items():
        (spec_dir / name).write_text(text)
    paths = {}
    for area, text in catalogs.items():
        path = root / f"{area.lower()}-invariants.rs"
        path.write_text(text)
        paths[area] = path
    # The TemporaryDirectory is kept alive by the caller's reference to it.
    return tmp, spec_dir, paths


def _check(specs: dict[str, str], catalogs: dict[str, str]) -> tuple[list[str], dict[str, int]]:
    tmp, spec_dir, paths = _tree(specs, catalogs)
    with tmp:
        errors: list[str] = []
        loaded = fm.load_catalogs(paths, errors)
        counts = fm.check_invariant_vocabulary(spec_dir, loaded, errors)
        return errors, counts


BOTH = {"CLUSTER": CLUSTER_CATALOG, "REPLICATION": REPLICATION_CATALOG}


def test_own_area_citation_passes_and_is_counted() -> None:
    errors, counts = _check(
        {
            "cluster.md": "| Catalog | `INV-HANDOFF-1`, `INV-SLOT-1` |\n",
            "replication.md": "| Catalog | `INV-REPLID-1` |\n",
        },
        BOTH,
    )
    assert errors == [], errors
    assert counts == {"CLUSTER": 2, "REPLICATION": 1}, counts


def test_dangling_citation_is_an_error() -> None:
    errors, _ = _check(
        {"replication.md": "line one\n| Catalog | `INV-REPLID-9` |\n"},
        BOTH,
    )
    assert len(errors) == 1, errors
    assert "replication.md:2" in errors[0], errors
    assert "INV-REPLID-9" in errors[0] and "does not define" in errors[0], errors


def test_cross_area_citation_is_an_error_that_names_the_owner() -> None:
    # The whole point of the per-area map: `INV-HANDOFF-1` exists, so a shared
    # vocabulary would wave this through.
    errors, _ = _check(
        {"replication.md": "| Catalog | `INV-HANDOFF-1` |\n"},
        BOTH,
    )
    assert len(errors) == 1, errors
    assert "INV-HANDOFF-1" in errors[0], errors
    assert "belongs to the CLUSTER catalog" in errors[0], errors
    assert "cluster-invariants.rs" in errors[0], errors


def test_citation_from_an_area_with_no_catalog_is_an_error() -> None:
    # Persistence has no catalog yet; a citation there must not pass silently
    # just because nothing is registered to contradict it.
    errors, _ = _check(
        {"persistence.md": "| Catalog | `INV-FSYNC-1` |\n"},
        BOTH,
    )
    assert len(errors) == 1, errors
    assert "PERSISTENCE area has no invariant catalog" in errors[0], errors


def test_prose_glob_is_not_a_citation() -> None:
    # The specs talk *about* the ids ("every `INV-*` a row cites"); that must
    # not be read as a citation of an entry named `*`.
    errors, counts = _check(
        {"replication.md": "Every `INV-*` id resolves to a catalog entry.\n"},
        BOTH,
    )
    assert errors == [], errors
    assert counts == {}, counts


def test_catalog_ids_are_bounded_to_the_static() -> None:
    errors: list[str] = []
    tmp, _, paths = _tree({}, BOTH)
    with tmp:
        loaded = fm.load_catalogs(paths, errors)
    assert errors == [], errors
    assert loaded["CLUSTER"].ids == frozenset({"INV-HANDOFF-1", "INV-SLOT-1"})
    assert loaded["REPLICATION"].ids == frozenset({"INV-REPLID-1", "INV-GATE-1"})


def test_a_vacuous_catalog_is_an_error() -> None:
    # Both ways the loader can silently start checking against nothing: the
    # static renamed, and the file moved.
    errors: list[str] = []
    tmp, _, paths = _tree({}, {"REPLICATION": "pub static ENTRIES: &[Invariant] = &[];\n"})
    with tmp:
        fm.load_catalogs(paths, errors)
    assert len(errors) == 1 and "no `INV-*` ids found" in errors[0], errors

    errors = []
    fm.load_catalogs({"REPLICATION": Path("/nonexistent/invariants.rs")}, errors)
    assert len(errors) == 1 and "invariant catalog missing" in errors[0], errors


def test_registered_catalogs_are_real_and_nonempty() -> None:
    """End-to-end: the live map resolves, so the check is never vacuous in CI."""
    errors: list[str] = []
    loaded = fm.load_catalogs(fm.INVARIANT_CATALOGS, errors)
    assert errors == [], errors
    for area, catalog in loaded.items():
        assert catalog.ids, area
        # Ids are area-scoped by construction; two catalogs sharing one id would
        # make the owner lookup ambiguous.
        for other, sibling in loaded.items():
            if other != area:
                assert not (catalog.ids & sibling.ids), (area, other)


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
