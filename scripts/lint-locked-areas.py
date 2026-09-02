#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Gate: every locked crate is declared in exactly one spec header.

The `specs/*.md` header key block is the locked-areas manifest — the one place
that says which areas are locked, at what mutation gate, over which crates (see
`scripts/locked_areas.py`, which owns the parser and the rules). This gate is
that parser plus an exit code, so the manifest cannot rot:

  * a spec with no `Status:` (or a value other than `LOCKED`/`DRAFT`) has no
    declared contract, and the tools that read the manifest would silently skip
    it;
  * a `LOCKED` spec without `Gate:`/`Crates:` locks nothing — `just
    mutants-gate <crate>` has no threshold to enforce;
  * a `DRAFT` spec *with* them is a lock that forgot to say so;
  * a crate named by two specs has two gates, and which one applies is whichever
    spec the parser reached first;
  * a crate that is not a workspace member is a perimeter entry `cargo mutants
    -p` cannot run — the shape a crate rename or extraction leaves behind.

Compile-free (it reads markdown and `Cargo.toml`), so it runs in `lint-gates`
on every commit rather than only under `just lint`.
"""

from __future__ import annotations

import sys

import locked_areas


def main() -> int:
    specs, errors = locked_areas.validate()
    if errors:
        print("ERROR: the locked-areas manifest (specs/*.md header keys) is inconsistent:")
        for error in errors:
            print(f"  {error}")
        print()
        print("       The header key block is the contract's own terms; see")
        print("       scripts/locked_areas.py and `just locked-areas`.")
        return 1

    locked = [spec for spec in specs if spec.is_locked]
    crates = sum(len(spec.crates) for spec in locked)
    print(
        f"OK: {len(locked)} locked area(s) over {crates} crate(s), "
        f"{len(specs) - len(locked)} draft(s) — every spec header declares its terms"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
