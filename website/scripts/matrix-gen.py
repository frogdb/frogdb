#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Join the command registry, vendored Redis command list, and regression
exclusions into `website/src/data/command-matrix.json`.

Reads three inputs that must all be up to date before this runs:
  - `website/src/data/commands.json`          (docs-gen: `just docs-gen`)
  - `website/src/data/redis-commands-8x.json` (vendored: `just
    redis-commands-vendor`, refreshed only when `REDIS_COMPAT_TARGET` bumps)
  - `website/src/data/compat-exclusions.json`  (compat-gen: `just compat-gen`)

and computes one row per command over the union of their command names, per
the compatibility/command-matrix spec's "Status derivation":
  - supported   — a real (non-stub) registry entry, with no regression-test
                  exclusions or only exclusions in categories that don't
                  reduce functional coverage (see `FUNCTIONAL_CATEGORIES`
                  below).
  - partial     — registered and functional, but with a non-trivial excluded-
                  test surface in a category that *does* represent a user-
                  visible behavioral limitation.
  - unsupported — present in the vendored Redis list but absent from the
                  registry, or registered only as a deliberate stub
                  (`is_stub` from `commands.json`).
FrogDB-original commands (no match in the vendored Redis list) are always
`supported` and tagged with a FrogDB extension family instead of a Redis
command group.

Usage:
    uv run website/scripts/matrix-gen.py            # generate
    uv run website/scripts/matrix-gen.py --check     # verify (CI)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = REPO_ROOT / "website" / "src" / "data"
COMMANDS_PATH = DATA_DIR / "commands.json"
REDIS_COMMANDS_PATH = DATA_DIR / "redis-commands-8x.json"
EXCLUSIONS_PATH = DATA_DIR / "compat-exclusions.json"
DEFAULT_OUTPUT = DATA_DIR / "command-matrix.json"
REDIS_VERSION_PATH = REPO_ROOT / "frogdb-server" / "crates" / "types" / "src" / "redis_version.rs"

REDIS_COMPAT_TARGET_RE = re.compile(r'pub const REDIS_COMPAT_TARGET:\s*&str\s*=\s*"([^"]+)"')

# ---------------------------------------------------------------------------
# Family taxonomy
# ---------------------------------------------------------------------------

# `commands.json`'s `family` field is the dot-namespace prefix S1 derives
# mechanically (e.g. `JSON.SET` -> `JSON`). Map each prefix to the extension
# family name from the spec's recommended taxonomy.
DOT_PREFIX_TO_FAMILY: dict[str, str] = {
    "JSON": "json",
    "TS": "timeseries",
    "FT": "search",
    "BF": "bloom",
    "CF": "cuckoo",
    "CMS": "cms",
    "TOPK": "topk",
    "TDIGEST": "tdigest",
    "ES": "event-sourcing",
    "FROGDB": "frogdb",
}

# FrogDB-only commands with no dot prefix and no match in the vendored Redis
# list. Vector-set commands (VADD, VSIM, ...) predate any upstream Redis
# vector-set feature, so they get their own family per the spec's taxonomy;
# anything else registry-only and unrecognized falls into a generic `frogdb`
# bucket (currently just STATUS).
VECTORSET_COMMANDS = {
    "VADD",
    "VCARD",
    "VDIM",
    "VEMB",
    "VGETATTR",
    "VINFO",
    "VLINKS",
    "VRANDMEMBER",
    "VRANGE",
    "VREM",
    "VSETATTR",
    "VSIM",
}

# ---------------------------------------------------------------------------
# Status derivation: which compat-exclusions categories represent a genuine
# user-visible functional gap vs. test infrastructure/internal differences.
# ---------------------------------------------------------------------------

# Every category in compat-exclusions.json's `CATEGORY_META` describes *why a
# test was excluded*, not whether the command itself behaves differently for
# callers. Most exclusion categories are about the test's own scaffolding
# (needs a specific config file, asserts an internal encoding, requires a
# second replica) rather than the command under test misbehaving — treating
# those as "partial" would flag ordinary commands like GET or SET as partial
# just because they share a test file with an excluded replication-topology
# test. Only two categories in the current taxonomy describe an actual
# behavioral narrowing of the command itself, and both are the spec's own
# worked examples: CROSSSLOT-limited multi-key operations (cluster) and
# single-shard scripting (scripting). `uncategorized` — exclusions recorded
# without a structured category — is included as a conservative fallback so
# future undocumented exclusions don't silently default to "supported".
FUNCTIONAL_CATEGORIES = {
    "intentional-incompatibility:cluster",
    "intentional-incompatibility:scripting",
    "uncategorized",
}

# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def get_redis_compat_target() -> str:
    """Read REDIS_COMPAT_TARGET from frogdb-types, the single source of truth."""
    content = REDIS_VERSION_PATH.read_text()
    match = REDIS_COMPAT_TARGET_RE.search(content)
    if not match:
        raise SystemExit(f"Could not find REDIS_COMPAT_TARGET in {REDIS_VERSION_PATH}")
    return match.group(1)


def load_json(path: Path, regenerate_hint: str) -> dict:
    if not path.exists():
        raise SystemExit(f"Missing: {path}. Run '{regenerate_hint}' first.")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


def command_family(
    name: str,
    *,
    redis_group: str | None,
    frogdb_dot_family: str | None,
) -> str:
    if redis_group is not None:
        return redis_group
    if frogdb_dot_family is not None:
        return DOT_PREFIX_TO_FAMILY.get(frogdb_dot_family, frogdb_dot_family.lower())
    if name in VECTORSET_COMMANDS:
        return "vectorset"
    return "frogdb"


def exclusion_note(categories: dict[str, dict], impact: dict) -> str:
    """One-line note for a `partial` command, built from its exclusion
    category labels (never invented prose).

    Deliberately omits `impact["total_tests"]` / `impact["total_excluded"]`:
    those are suite-level aggregates (see `compat-gen.py`'s `command_data`
    comment) copied verbatim onto every command a suite covers, and
    "ported" vs. "excluded" are disjoint counts from the upstream test
    corpus, not a subset/total pair — there is no honest "N of M excluded"
    ratio to report per command from this metadata.
    """
    hits = sorted(set(impact["categories"]) & FUNCTIONAL_CATEGORIES)
    labels = [categories[cat]["label"] for cat in hits]
    return f"Regression tests excluded ({'; '.join(labels)})."


def build_matrix(
    commands: dict, redis_commands: dict, exclusions: dict, target_version: str
) -> dict:
    frogdb_by_name = {c["name"]: c for c in commands["commands"]}
    redis_by_name = {c["name"]: c for c in redis_commands["commands"]}
    command_impact = exclusions["command_impact"]
    categories = exclusions["categories"]

    all_names = sorted(set(frogdb_by_name) | set(redis_by_name) | set(command_impact))

    families: dict[str, list[dict]] = {}
    summary = {"supported": 0, "partial": 0, "unsupported": 0, "extension": 0}

    for name in all_names:
        frogdb_entry = frogdb_by_name.get(name)
        redis_entry = redis_by_name.get(name)
        in_frogdb = frogdb_entry is not None
        in_redis = redis_entry is not None

        family = command_family(
            name,
            redis_group=redis_entry["group"] if in_redis else None,
            frogdb_dot_family=frogdb_entry.get("family") if in_frogdb else None,
        )

        arity = frogdb_entry["arity"] if in_frogdb else None

        if not in_frogdb:
            status = "unsupported"
            note = f"Present in Redis {target_version}; not implemented in FrogDB."
        elif frogdb_entry["is_stub"]:
            status = "unsupported"
            note = "Registered in FrogDB but not implemented (deliberate stub)."
        else:
            impact = command_impact.get(name)
            if impact and set(impact["categories"]) & FUNCTIONAL_CATEGORIES:
                status = "partial"
                note = exclusion_note(categories, impact)
            else:
                status = "supported"
                note = ""

        summary[status] += 1
        if in_frogdb and not in_redis:
            summary["extension"] += 1

        families.setdefault(family, []).append(
            {"name": name, "status": status, "arity": arity, "note": note}
        )

    families_list = [
        {"family": family, "commands": families[family]} for family in sorted(families)
    ]

    return {
        "_generated": "DO NOT EDIT — auto-generated by: just matrix-gen",
        "target_redis_version": target_version,
        "families": families_list,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Join commands.json, the vendored Redis command list, and "
        "compat-exclusions.json into command-matrix.json."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output path for the generated JSON file",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit 1 if generated JSON differs from committed file",
    )
    args = parser.parse_args()

    target_version = get_redis_compat_target()
    commands = load_json(COMMANDS_PATH, "just docs-gen")
    redis_commands = load_json(REDIS_COMMANDS_PATH, "just redis-commands-vendor")
    exclusions = load_json(EXCLUSIONS_PATH, "just compat-gen")

    vendored_version = redis_commands["_vendored"]["redis_version"]
    if vendored_version != target_version:
        raise SystemExit(
            f"{REDIS_COMMANDS_PATH} is vendored for Redis {vendored_version}, but "
            f"REDIS_COMPAT_TARGET is {target_version}. Run 'just redis-commands-vendor' "
            "to re-vendor the command list for the new target."
        )

    data = build_matrix(commands, redis_commands, exclusions, target_version)
    generated_json = json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    if args.check:
        output_path: Path = args.output
        if not output_path.exists():
            print(f"Missing: {output_path}. Run 'just matrix-gen' to generate.", file=sys.stderr)
            return 1
        existing = output_path.read_text()
        if existing != generated_json:
            print(f"Differs: {output_path}. Run 'just matrix-gen' to regenerate.", file=sys.stderr)
            return 1
        print("command-matrix.json is up to date.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(generated_json)
    print(f"Generated: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
