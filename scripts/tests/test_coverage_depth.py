#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for scripts/coverage-depth.py's monomorphization dedupe.

Run: ./scripts/tests/test_coverage_depth.py   (or `just test-coverage-depth`)

Guards issue 28: `class_counts` must be computed over one entry per *source*
function, folding a generic's monomorphizations together and dropping the zeroed
`::<_>` "unused function" placeholder records. Before the fix, `index_functions`
folded only by mangled name, so N monomorphizations plus the placeholder produced
N+1 `untested` entries for a single generic.

No test framework: the coverage pipeline is pure-stdlib `uv run --script`, so this
stays a dependency-free assert script that exits nonzero on the first failure.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

# coverage-depth.py has a hyphen, so it is not importable by name; load by path.
_SCRIPT = Path(__file__).resolve().parent.parent / "coverage-depth.py"
_spec = importlib.util.spec_from_file_location("coverage_depth", _SCRIPT)
assert _spec and _spec.loader
cd = importlib.util.module_from_spec(_spec)
# Register before exec so @dataclass can resolve annotations against the module.
sys.modules["coverage_depth"] = cd
_spec.loader.exec_module(cd)


# Classification thresholds mirroring the script's argparse defaults.
ARGS = SimpleNamespace(
    hot_tests=cd.DEFAULT_HOT_TESTS,
    hot_exec_floor=cd.DEFAULT_HOT_EXEC_FLOOR,
    well_covered_tests=5,
)

FILE = "/repo/frogdb-server/crates/foo/src/lib.rs"


def _region(line: int, count: int) -> list[int]:
    # llvm-cov region: [line_start, col_start, line_end, col_end, count, ...]
    return [line, 1, line, 40, count]


def _fn(name: str, count: int, region_count: int) -> dict:
    # Two regions on the same source span (lines 10-11), as every
    # monomorphization of one source function shares.
    return {
        "name": name,
        "count": count,
        "filenames": [FILE],
        "regions": [_region(10, region_count), _region(11, region_count)],
    }


def _export(functions: list[dict], files: list[str]) -> dict:
    return {
        "data": [
            {
                "functions": functions,
                "files": [{"filename": f, "segments": [], "summary": {}} for f in files],
            }
        ]
    }


def _wrap(funcs: dict[str, cd.FuncInfo]) -> list[cd.FuncDepth]:
    return [cd.FuncDepth(info=i) for i in funcs.values()]


def test_placeholder_detection() -> None:
    assert cd.is_generic_placeholder("foo::bar::<_>")
    assert cd.is_generic_placeholder("foo::bar::<_, _>")
    assert cd.is_generic_placeholder("foo::bar::< _ >")
    assert cd.is_generic_placeholder("<foo::Bar<_> as foo::Trait>::baz")
    # Concrete monomorphizations are never placeholders.
    assert not cd.is_generic_placeholder("foo::bar::<u64>")
    assert not cd.is_generic_placeholder("foo::bar::<alloc::vec::Vec<u8>>")
    assert not cd.is_generic_placeholder("foo::bar")


def test_strip_generics() -> None:
    assert cd.strip_generics("foo::bar::<u64>") == "foo::bar"
    assert cd.strip_generics("foo::bar::<i32>") == "foo::bar"
    assert cd.strip_generics("foo::bar::<Vec<u8>>") == "foo::bar"
    assert cd.strip_generics("foo::bar::h0123456789abcdef") == "foo::bar"
    # Trait-method monomorphizations collapse consistently (exact residual is
    # irrelevant so long as every instantiation of one source fn maps identically).
    assert cd.strip_generics("<T as Trait>::m::<u8>") == cd.strip_generics("<T as Trait>::m::<i64>")


def test_two_monos_plus_placeholder_fold_to_one() -> None:
    """The core acceptance criterion of issue 28."""
    export = _export(
        [
            _fn("_ZN3foo3barIxEE", count=5, region_count=1),  # foo::bar::<i64>
            _fn("_ZN3foo3barIyEE", count=3, region_count=1),  # foo::bar::<u64>
            _fn("_ZN3foo3barIT_EE", count=0, region_count=0),  # foo::bar::<_> (zeroed)
        ],
        files=[FILE],
    )
    demangled = {
        "_ZN3foo3barIxEE": "foo::bar::<i64>",
        "_ZN3foo3barIyEE": "foo::bar::<u64>",
        "_ZN3foo3barIT_EE": "foo::bar::<_>",
    }

    keep = cd.export_filenames(export)
    funcs = cd.index_functions(export, keep)
    # index_functions folds by mangled name: 3 distinct symbols -> 3 records.
    assert len(funcs) == 3, f"expected 3 raw records, got {len(funcs)}"

    deduped = cd.dedupe_depths(_wrap(funcs), demangled)
    assert len(deduped) == 1, f"expected 1 deduped function, got {len(deduped)}"

    cd.classify(deduped, hot_floor=cd.DEFAULT_HOT_EXEC_FLOOR, args=ARGS)
    counts: dict[str, int] = {}
    for d in deduped:
        counts[d.klass] = counts.get(d.klass, 0) + 1
    # No tests attached, so the single folded entry is `untested` — exactly one,
    # not the three the pre-fix code produced.
    assert counts == {"untested": 1}, counts

    # Region counts are representative (max), not summed across monos: 2 regions,
    # not 2 + 2 + 0 = 4.
    only = deduped[0]
    assert only.info.regions == 2, only.info.regions
    assert only.info.export_count == 5, only.info.export_count


def test_distinct_functions_not_folded() -> None:
    export = _export(
        [
            {
                "name": "_ZN3foo1aE",
                "count": 1,
                "filenames": [FILE],
                "regions": [_region(10, 1)],
            },
            {
                "name": "_ZN3foo1bE",
                "count": 1,
                "filenames": [FILE],
                "regions": [_region(20, 1)],
            },
        ],
        files=[FILE],
    )
    demangled = {"_ZN3foo1aE": "foo::a", "_ZN3foo1bE": "foo::b"}
    funcs = cd.index_functions(export, cd.export_filenames(export))
    deduped = cd.dedupe_depths(_wrap(funcs), demangled)
    assert len(deduped) == 2, f"distinct functions must not fold: {len(deduped)}"


def test_tests_and_suites_union_across_monos() -> None:
    export = _export(
        [
            _fn("_mono_a", count=5, region_count=1),
            _fn("_mono_b", count=3, region_count=1),
        ],
        files=[FILE],
    )
    demangled = {"_mono_a": "foo::bar::<i64>", "_mono_b": "foo::bar::<u64>"}
    funcs = cd.index_functions(export, cd.export_filenames(export))
    depths = _wrap(funcs)
    # Attach disjoint test evidence to each monomorphization.
    by_name = {d.info.name: d for d in depths}
    by_name["_mono_a"].tests.append("suiteX::t1")
    by_name["_mono_a"].suites.add("suiteX")
    by_name["_mono_b"].tests.append("suiteY::t2")
    by_name["_mono_b"].suites.add("suiteY")

    deduped = cd.dedupe_depths(depths, demangled)
    assert len(deduped) == 1
    folded = deduped[0]
    assert set(folded.tests) == {"suiteX::t1", "suiteY::t2"}, folded.tests
    assert folded.suites == {"suiteX", "suiteY"}, folded.suites

    cd.classify(deduped, hot_floor=cd.DEFAULT_HOT_EXEC_FLOOR, args=ARGS)
    # 2 tests across 2 suites -> not untested, not single-test, not monoculture.
    assert folded.klass in {"covered", "well-covered"}, folded.klass


def test_line_counts_untouched_by_dedupe() -> None:
    """Per-file line_counts is computed from files[].segments, never functions[].

    Issue 28 criterion 6: the dedupe must not disturb the per-file line view. This
    locks the segment-based counter so a future change to the function path cannot
    silently reroute line counting through the (now deduped) functions[].
    """
    # segment: [line, col, count, has_count, is_region_entry, is_gap_region]
    segments = [
        [10, 1, 7, True, True, False],
        [10, 40, 0, False, False, False],
        [11, 1, 0, True, True, False],
        [11, 20, 0, False, False, False],
        [12, 1, 4, True, True, False],
        [12, 30, 0, False, False, False],
    ]
    lc = cd.line_counts(segments)
    assert lc == {10: 7, 11: 0, 12: 4}, lc


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for t in tests:
        try:
            t()
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
