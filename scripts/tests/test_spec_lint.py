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


def _tree(specs: dict[str, str], catalogs: dict[str, str], quint: dict[str, str] | None = None):
    """A temp spec dir plus temp catalog files, as (spec_dir, {area: path})."""
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    spec_dir = root / "specs"
    spec_dir.mkdir()
    for name, text in specs.items():
        (spec_dir / name).write_text(text)
    if quint:
        quint_dir = spec_dir / "quint"
        quint_dir.mkdir()
        for name, text in quint.items():
            (quint_dir / name).write_text(text)
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


CONSTRUCTIVE_SPEC = """\
# Cluster

## TR-CLUSTER-001 — the leader assigns an unowned slot

| Precondition | `owner[s]` is unset |
| Postcondition | `owner[s] = n`, `epoch[n]` incremented |

## TR-CLUSTER-002 — the leader completes a migration

| Precondition | TR-CLUSTER-001 has run for `s` |
| Postcondition | `owner[s] = target` |
"""

COMPOSITION_SPEC = """\
# Composition

## CO-001 — a handoff barrier outlives the feed gate that holds it

| Areas | cluster, replication |
"""


def _parse(specs: dict[str, str]):
    """(modes, rows, errors) for a synthetic spec dir."""
    tmp, spec_dir, _ = _tree(specs, {})
    with tmp:
        errors: list[str] = []
        modes, rows = fm.parse_specs(spec_dir, errors)
        return modes, rows, errors, spec_dir


def test_constructive_rows_are_parsed_with_their_kind() -> None:
    _, rows, errors, _ = _parse({"cluster.md": CONSTRUCTIVE_SPEC})
    assert errors == [], errors
    assert [(row.id, row.kind, row.area) for row in rows] == [
        ("TR-CLUSTER-001", "TR", "CLUSTER"),
        ("TR-CLUSTER-002", "TR", "CLUSTER"),
    ]


def test_a_constructive_row_must_match_its_file_area() -> None:
    _, _, errors, _ = _parse({"cluster.md": "## TR-REPLICATION-001 — wrong area\n"})
    assert len(errors) == 1, errors
    assert "does not match the file's area prefix TR-CLUSTER-" in errors[0], errors


def test_dangling_spec_reference_is_an_error() -> None:
    tmp, spec_dir, _ = _tree(
        {"cluster.md": CONSTRUCTIVE_SPEC + "\nSee TR-CLUSTER-009 for the retry.\n"}, {}
    )
    with tmp:
        errors: list[str] = []
        modes, rows = fm.parse_specs(spec_dir, errors)
        defined = {entry.id for entry in [*modes, *rows]}
        fm.check_spec_references(spec_dir, defined, errors)
    assert len(errors) == 1, errors
    assert "cites `TR-CLUSTER-009`, which no spec row defines" in errors[0], errors


def test_composition_ids_resolve_across_files() -> None:
    tmp, spec_dir, _ = _tree(
        {
            "composition.md": COMPOSITION_SPEC,
            "cluster.md": CONSTRUCTIVE_SPEC + "\nComposed in CO-001.\n",
        },
        {},
    )
    with tmp:
        errors: list[str] = []
        modes, rows = fm.parse_specs(spec_dir, errors)
        defined = {entry.id for entry in [*modes, *rows]}
        citations = fm.check_spec_references(spec_dir, defined, errors)
    assert errors == [], errors
    # cluster.md: the two TR headings, the TR-CLUSTER-001 precondition citation,
    # and the CO-001 citation; composition.md: the CO-001 heading.
    assert citations == 5, citations


def test_live_specs_have_no_dangling_references() -> None:
    """The real tree: 279 FM rows cross-cite heavily, and every id must resolve."""
    errors: list[str] = []
    modes, rows = fm.parse_specs(fm.SPEC_DIR, errors)
    defined = {entry.id for entry in [*modes, *rows]}
    fm.check_spec_references(fm.SPEC_DIR, defined, errors)
    assert errors == [], errors
    assert len(modes) >= 279, len(modes)
    # The constructive rewrite gave every area a TR section; each spec must
    # keep at least the transition rows it shipped with (20 was the smallest).
    tr_areas = {row.id.split("-")[1] for row in rows if row.id.startswith("TR-")}
    assert tr_areas >= {"BLOCKING", "CLUSTER", "PERSISTENCE", "REPLICATION", "TXN", "VLL"}, tr_areas
    assert len(rows) >= 190, len(rows)


LIVENESS_SPEC = """\
# Cluster

## LV-CLUSTER-001 — a missed failover is eventually retried

| Property | if a primary stays failed, some replica is eventually promoted |
| Forced by | `test_missed_failover_is_retried` |
"""

LIVENESS_SPEC_UNFORCED = """\
# Cluster

## LV-CLUSTER-001 — a missed failover is eventually retried

| Property | if a primary stays failed, some replica is eventually promoted |
"""


def test_lv_row_without_forced_by_is_an_error() -> None:
    _, _, errors, _ = _parse({"cluster.md": LIVENESS_SPEC_UNFORCED})
    assert len(errors) == 1, errors
    assert "LV-CLUSTER-001 has no `Forced by` row" in errors[0], errors


LIVENESS_SPEC_EMPTY_FORCED_BY = """\
# Cluster

## LV-CLUSTER-001 — a missed failover is eventually retried

| Property | if a primary stays failed, some replica is eventually promoted |
| Forced by | |
"""


def test_lv_row_with_empty_forced_by_cell_is_an_error() -> None:
    """A `Forced by` row that resolves to the empty cell is present-but-empty,
    not absent: `test_lv_row_without_forced_by_is_an_error` above pins the
    field-missing-entirely case, and this pins the sibling that used to slip
    through silently — an empty cell parsed to zero tests with no error."""
    _, rows, errors, _ = _parse({"cluster.md": LIVENESS_SPEC_EMPTY_FORCED_BY})
    assert len(errors) == 1, errors
    assert "LV-CLUSTER-001" in errors[0] and "empty" in errors[0], errors
    assert rows[0].tests == [], rows[0].tests


def test_lv_row_forcing_test_must_carry_its_tag() -> None:
    _, rows, errors, _ = _parse({"cluster.md": LIVENESS_SPEC})
    assert errors == [], errors
    assert rows[0].tests == ["test_missed_failover_is_retried"], rows[0].tests

    paths = {"cluster_failover::test_missed_failover_is_retried"}
    untagged = fm.check([], rows, [], paths)
    assert len(untagged) == 1, untagged
    assert "carries no `// LV-CLUSTER-001` tag" in untagged[0], untagged

    tag = fm.Tag(
        row_id="LV-CLUSTER-001",
        test="test_missed_failover_is_retried",
        path=fm.REPO / "frogdb-server/crates/cluster/src/lib.rs",
        line=1,
    )
    assert fm.check([], rows, [tag], paths) == []


def test_lv_tag_naming_no_row_is_an_error() -> None:
    tag = fm.Tag(
        row_id="LV-CLUSTER-404",
        test="test_orphan",
        path=fm.REPO / "frogdb-server/crates/cluster/src/lib.rs",
        line=1,
    )
    problems = fm.check([], [], [tag], {"cluster_failover::test_orphan"})
    assert len(problems) == 1, problems
    assert "is tagged LV-CLUSTER-404, which no spec defines" in problems[0], problems


def _quint_check(specs: dict[str, str], quint: dict[str, str]) -> list[str]:
    tmp, spec_dir, paths = _tree(specs, BOTH, quint)
    with tmp:
        errors: list[str] = []
        modes, rows = fm.parse_specs(spec_dir, errors)
        catalogs = fm.load_catalogs(paths, errors)
        defined = {entry.id for entry in [*modes, *rows]}
        fm.check_quint_citations(spec_dir / "quint", defined, catalogs, errors)
        return errors


def test_quint_header_citations_resolve() -> None:
    errors = _quint_check(
        {"cluster.md": CONSTRUCTIVE_SPEC},
        {
            "handoff.qnt": "// Models TR-CLUSTER-001, TR-CLUSTER-002 and INV-HANDOFF-1.\nmodule handoff {\n}\n"
        },
    )
    assert errors == [], errors


def test_quint_header_citing_an_unknown_row_is_an_error() -> None:
    errors = _quint_check(
        {"cluster.md": CONSTRUCTIVE_SPEC},
        {"handoff.qnt": "// Models TR-CLUSTER-777.\nmodule handoff {\n}\n"},
    )
    assert len(errors) == 1, errors
    assert "handoff.qnt:1" in errors[0] and "TR-CLUSTER-777" in errors[0], errors


def test_quint_header_citing_an_unknown_invariant_is_an_error() -> None:
    errors = _quint_check(
        {"cluster.md": CONSTRUCTIVE_SPEC},
        {"handoff.qnt": "// Models TR-CLUSTER-001 and INV-HANDOFF-9.\nmodule handoff {\n}\n"},
    )
    assert len(errors) == 1, errors
    assert "INV-HANDOFF-9" in errors[0] and "no invariant catalog defines" in errors[0], errors


def test_quint_model_without_citations_is_an_error() -> None:
    errors = _quint_check(
        {"cluster.md": CONSTRUCTIVE_SPEC},
        {"handoff.qnt": "module handoff {\n}\n"},
    )
    assert len(errors) == 1, errors
    assert "header cites no spec ids" in errors[0], errors


def test_only_the_header_block_is_scanned() -> None:
    # A stale id in the model *body* is not this lint's business — the body is
    # Quint, and `quint typecheck` owns it. Only the header makes claims about
    # the spec.
    errors = _quint_check(
        {"cluster.md": CONSTRUCTIVE_SPEC},
        {"handoff.qnt": "// Models TR-CLUSTER-001.\nmodule handoff {\n  // TR-CLUSTER-777\n}\n"},
    )
    assert errors == [], errors


def test_absent_quint_dir_is_vacuous() -> None:
    errors: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        absent = Path(tmp) / "nope"
        models, citations = fm.check_quint_citations(absent, set(), {}, errors)
    assert errors == [] and models == 0 and citations == 0, (errors, models, citations)


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
