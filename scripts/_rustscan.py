# /// script
# requires-python = ">=3.11"
# ///
"""Shared source-scanning helpers for the seam-lint family.

These two functions decide *what counts as test code* for the grep/regex gates
in this directory. They started life as private helpers in `clock-seam.py`;
`durable-ack.py` and `error-sanitize.py` need the same `#[cfg(test)]`-span
awareness, so — per PRD `.scratch/hardening-2/PRD.md` §3.2 ("Factor into
`scripts/_rustscan.py` before the third copy drifts") — they live here now and
every seam gate imports them.

The module carries no PyPI dependencies, so a `uv run --script` consumer that
imports it needs nothing declared in its own PEP-723 header.
"""

from __future__ import annotations

from pathlib import Path


def cfg_test_spans(lines: list[str]) -> list[tuple[int, int]]:
    """Line spans (0-based, inclusive) covered by `#[cfg(test)]` items.

    A brace-counting scan: from each `#[cfg(test)]` attribute, advance until the
    item it guards closes (net brace depth returns to zero). Good enough for the
    module/function attributes the gates actually see; it does not try to parse
    Rust.
    """
    spans: list[tuple[int, int]] = []
    i, n = 0, len(lines)
    while i < n:
        if "#[cfg(test)]" not in lines[i]:
            i += 1
            continue
        j, depth, opened = i, 0, False
        while j < n:
            depth += lines[j].count("{") - lines[j].count("}")
            opened = opened or "{" in lines[j]
            if opened and depth <= 0:
                break
            j += 1
        spans.append((i, j))
        i = j + 1
    return spans


def is_test_path(rel: Path, src_index: int = 4) -> bool:
    """A test module carried in `src/`: `tests.rs`, `*_tests.rs`, `tests/`.

    `src_index` is the index into `rel.parts` at which a `tests/` module dir may
    appear (the default, 4, matches `frogdb-server/crates/<crate>/src/...`).
    """
    return (
        rel.name == "tests.rs"
        or rel.name.endswith("_tests.rs")
        or "tests" in rel.parts[src_index:]  # a `tests/` module dir inside src/
    )


def in_any_span(idx: int, spans: list[tuple[int, int]]) -> bool:
    """Whether a 0-based line index falls inside any `(lo, hi)` inclusive span."""
    return any(lo <= idx <= hi for lo, hi in spans)
