#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Vendor the upstream Redis + bundled-module command metadata.

Two vendored snapshots come out of this script, both pinned to
`REDIS_COMPAT_TARGET` (the single source of truth in
`frogdb-server/crates/types/src/redis_version.rs`):

`website/src/data/redis-commands-8x.json` (core)
    Fetched from `src/commands/*.json` in `redis/redis` at the
    `REDIS_COMPAT_TARGET` tag. Carries the `matrix-gen.py` join keys (name,
    group, since), the human-facing documentation the command registry
    serves through `COMMAND DOCS` (summary, complexity, arguments, history),
    and the machine-checkable dispatch metadata `COMMAND INFO` serves
    (arity, command_flags, key_specs).

`website/src/data/redis-module-commands-8x.json` (extension families)
    Fetched from the root `commands.json` of every module Redis bundles:
    the four out-of-tree repos (RedisJSON, RediSearch, RedisTimeSeries,
    RedisBloom) plus the in-tree vector-sets module, which provides the
    undotted `V*` family.

    Out-of-tree module tags are *derived*, not hand-typed:
    `redis/redis@<tag>/modules/<module>/Makefile` pins `MODULE_VERSION`, so
    the module snapshots always match the exact sources the pinned Redis
    release builds its bundled modules from. vector-sets ships inside
    `redis/redis` itself, so its pin *is* the Redis tag. Every resolved tag
    is recorded in the provenance header.

    **Upstream gap**: no module publishes `key_specs` — the modules declare
    key positions in C/Rust at `RedisModule_CreateCommand` time, not as
    data. The four out-of-tree repos additionally publish no `arity` and no
    `command_flags` (vector-sets, being in-tree, does publish both). Those
    fields are simply absent from the rows that lack them; nothing is
    hand-fabricated to fill the hole (ADR-0005), and the generated Rust
    module and its verification tests treat "absent" as "nothing to check"
    rather than as "no keys" or "arity 0".

    RediSearch's `command_tips` is deliberately dropped: tips are routing
    hints that must be judged against FrogDB's own routing rather than
    copied (ADR-0005, `.scratch/redis-feel/issues/open/12-*.md`).

Trimming philosophy: keep what FrogDB actually consumes, drop the rest.
Dropped from core: `reply_schema` (large, and FrogDB's replies are
verified by the regression suite, not by a vendored schema), `function` /
`get_keys_function` (C symbol names — the `{"unknown": null}` marker inside
`key_specs` already flags the specs a static check cannot verify), and
`acl_categories` (FrogDB derives ACL categories from each `CommandSpec`'s
real behavior per ADR-0005).

Neither file is regenerated on every `just docs-build` — both are
point-in-time vendored snapshots requiring network access to GitHub, and
upstream only moves a few times a year. `matrix-gen.py` checks the vendored
`redis_version` against `REDIS_COMPAT_TARGET` on every run and fails loudly
if they've drifted, so a version bump can't silently go un-vendored.

Subcommands (core files with a `container` key, e.g. `ACL CAT`) are still
skipped, and that stays true now that `key_specs` and `arguments` are kept:
FrogDB's registry dispatches subcommands internally and only exposes the
container command (`ACL`, `CONFIG`, ...) as a registry entry, so a
subcommand row would never join against anything. Nothing is lost for the
fields this script newly keeps — upstream container commands carry no
`key_specs` of their own (the key-bearing specs live on the subcommand
rows, which FrogDB's key extraction reaches through container-aware
dispatch rather than through a registry entry), so keeping them would add
phantom rows without adding a single checkable key spec. Revisit if
`COMMAND DOCS` ever needs to emit per-subcommand argument trees.

Container rows do carry `has_subcommands: true` so a consumer can tell
"upstream says this command takes no keys" apart from "upstream's key specs
sit on rows this script skipped" — the join tests need that distinction to
avoid reporting every container as a divergence.

Usage:
    uv run website/scripts/vendor-redis-commands.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = REPO_ROOT / "website" / "src" / "data"
DEFAULT_OUTPUT = DATA_DIR / "redis-commands-8x.json"
DEFAULT_MODULE_OUTPUT = DATA_DIR / "redis-module-commands-8x.json"
REDIS_VERSION_PATH = REPO_ROOT / "frogdb-server" / "crates" / "types" / "src" / "redis_version.rs"

REDIS_COMPAT_TARGET_RE = re.compile(r'pub const REDIS_COMPAT_TARGET:\s*&str\s*=\s*"([^"]+)"')
MODULE_VERSION_RE = re.compile(r"^MODULE_VERSION\s*=\s*(\S+)", re.MULTILINE)
MODULE_REPO_RE = re.compile(r"^MODULE_REPO\s*=\s*(\S+)", re.MULTILINE)

GITHUB_API = "https://api.github.com/repos/redis/redis/contents/src/commands"
GITHUB_TREE = "https://github.com/redis/redis/tree"
GITHUB_RAW = "https://raw.githubusercontent.com/redis/redis"

# `modules/<dir>` in redis/redis -> the family name FrogDB knows it by. The
# upstream repo and tag are read out of `modules/<dir>/Makefile` at the pinned
# Redis tag, so this table never carries a version.
MODULE_DIRS = {
    "redisjson": "RedisJSON",
    "redisearch": "RediSearch",
    "redistimeseries": "RedisTimeSeries",
    "redisbloom": "RedisBloom",
}

# Modules that live inside redis/redis rather than in their own repo, so their
# pin is the Redis tag itself and their commands.json is read straight out of
# the Redis tree.
IN_TREE_MODULES = {
    "vector-sets": "VectorSets",
}

# Per-command fields kept from the core `src/commands/*.json` files, in emit
# order. Fields absent upstream are omitted from the row entirely.
CORE_FIELDS = (
    "arity",
    "command_flags",
    "key_specs",
    "arguments",
    "history",
)

# Per-command fields kept from each module's root `commands.json`, in emit
# order. Same projection as the core one minus `key_specs`, which no module
# publishes; `arity`/`command_flags` survive only for vector-sets, the one
# in-tree module that declares them (see the module docstring above). Fields a
# family does not publish are omitted from its rows rather than defaulted.
MODULE_FIELDS = (
    "arity",
    "command_flags",
    "arguments",
    "history",
)


def get_redis_compat_target() -> str:
    """Read REDIS_COMPAT_TARGET from frogdb-types, the single source of truth."""
    content = REDIS_VERSION_PATH.read_text()
    match = REDIS_COMPAT_TARGET_RE.search(content)
    if not match:
        raise SystemExit(f"Could not find REDIS_COMPAT_TARGET in {REDIS_VERSION_PATH}")
    return match.group(1)


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "frogdb-docs-gen"})
    with urllib.request.urlopen(request) as response:  # noqa: S310 (pinned https:// host)
        return response.read()


def fetch_json(url: str) -> object:
    return json.loads(fetch(url))


def fetch_text(url: str) -> str:
    return fetch(url).decode()


def trim(meta: dict, fields: tuple[str, ...]) -> dict:
    """Project the shared documentation fields plus `fields` out of `meta`."""
    row = {
        "group": meta.get("group", "").replace("_", "-"),
        "since": meta.get("since", ""),
        "summary": meta.get("summary", ""),
        # Upstream omits `complexity` for commands with nothing meaningful to
        # state (e.g. container commands). Carry the absence through as null
        # rather than inventing a string: `COMMAND DOCS` omits the field
        # entirely in that case.
        "complexity": meta.get("complexity") or None,
    }
    for field in fields:
        if field in meta:
            row[field] = meta[field]
    return row


def vendor_core(redis_version: str) -> dict:
    """Fetch and filter the top-level core command list for `redis_version`."""
    try:
        listing = fetch_json(f"{GITHUB_API}?ref={redis_version}")
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"Failed to list src/commands at redis/redis@{redis_version}: {exc}. "
            "Does that tag exist upstream?"
        ) from exc

    commands: dict[str, dict] = {}
    containers: set[str] = set()
    skipped_subcommands = 0
    for entry in listing:
        name = entry.get("name", "")
        if not name.endswith(".json"):
            continue
        data = fetch_json(entry["download_url"])
        for cmd_name, meta in data.items():
            if "container" in meta:
                skipped_subcommands += 1
                containers.add(meta["container"])
                continue  # subcommand (e.g. "ACL CAT") — not a registry entry
            commands[cmd_name] = {"name": cmd_name, **trim(meta, CORE_FIELDS)}

    # Mark the rows whose key specs and argument trees live on the skipped
    # subcommand rows, so the join tests can tell "upstream says this command
    # takes no keys" apart from "upstream's key specs are on rows we skipped".
    for container in sorted(containers):
        row = commands.get(container)
        if row is None:
            continue
        rest = {key: value for key, value in row.items() if key != "name"}
        commands[container] = {"name": row["name"], "has_subcommands": True, **rest}
        if row.get("key_specs"):
            print(f"  note: container {container} carries its own key_specs")

    sorted_commands = [commands[name] for name in sorted(commands)]
    print(
        f"  core: {len(sorted_commands)} commands "
        f"({len(containers)} containers, {skipped_subcommands} subcommands skipped)"
    )

    return {
        "_vendored": {
            "warning": "DO NOT EDIT — vendored from upstream Redis, not regenerated by "
            "just docs-build",
            "source": f"{GITHUB_TREE}/{redis_version}/src/commands",
            "redis_version": redis_version,
            "retrieved": date.today().isoformat(),
            "regenerate": "uv run website/scripts/vendor-redis-commands.py",
        },
        "count": len(sorted_commands),
        "commands": sorted_commands,
    }


def resolve_module_pin(redis_version: str, module_dir: str) -> tuple[str, str]:
    """Read (repo_url, tag) that redis/redis@`redis_version` builds `module_dir` from."""
    url = f"{GITHUB_RAW}/{redis_version}/modules/{module_dir}/Makefile"
    try:
        makefile = fetch_text(url)
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"Failed to read {url}: {exc}. Does redis/redis@{redis_version} still bundle "
            f"the {module_dir} module under modules/?"
        ) from exc

    version = MODULE_VERSION_RE.search(makefile)
    repo = MODULE_REPO_RE.search(makefile)
    if not version or not repo:
        raise SystemExit(
            f"Could not find MODULE_VERSION/MODULE_REPO in {url}. Upstream's module "
            "build layout changed; update MODULE_DIRS and the pin parser."
        )
    return repo.group(1).rstrip("/"), version.group(1)


def split_module_commands(data: dict) -> tuple[list[dict], list[dict]]:
    """Split a module `commands.json` into top-level commands and subcommands.

    Module repos spell subcommands as space-separated top-level keys
    (`"FT.CONFIG GET"`, `"JSON.DEBUG MEMORY"`) instead of tagging them with the
    core `container` key. They are kept in their own list rather than dropped
    so the coverage join can still see that a container FrogDB registers
    (`FT.CONFIG`) is a known upstream command whose metadata lives on its
    subcommand rows.
    """
    commands: dict[str, dict] = {}
    subcommands: dict[str, dict] = {}
    for name, meta in data.items():
        row = {"name": name, **trim(meta, MODULE_FIELDS)}
        if " " in name or "container" in meta:
            container, _, sub = name.partition(" ")
            row["container"] = meta.get("container", container)
            row["subcommand"] = sub
            subcommands[name] = row
        else:
            commands[name] = row
    return (
        [commands[name] for name in sorted(commands)],
        [subcommands[name] for name in sorted(subcommands)],
    )


def vendor_modules(redis_version: str) -> dict:
    """Fetch the bundled-module command metadata for `redis_version`."""
    pins: list[tuple[str, str, str, str]] = []  # (family, repo_url, tag, commands_url)
    for module_dir, family in MODULE_DIRS.items():
        repo_url, tag = resolve_module_pin(redis_version, module_dir)
        # `MODULE_REPO` is lowercased in the Redis Makefiles and
        # raw.githubusercontent.com does not redirect between casings, so keep
        # the URL exactly as upstream spells it.
        owner_repo = repo_url.removeprefix("https://github.com/")
        pins.append(
            (
                family,
                repo_url,
                tag,
                f"https://raw.githubusercontent.com/{owner_repo}/{tag}/commands.json",
            )
        )
    for module_dir, family in IN_TREE_MODULES.items():
        pins.append(
            (
                family,
                "https://github.com/redis/redis",
                redis_version,
                f"{GITHUB_RAW}/{redis_version}/modules/{module_dir}/commands.json",
            )
        )

    families: list[dict] = []
    total = 0
    for family, repo_url, tag, commands_url in sorted(pins):
        try:
            data = fetch_json(commands_url)
        except urllib.error.HTTPError as exc:
            # Recorded rather than papered over: a family whose upstream
            # publishes no structured command JSON is skipped, never
            # hand-written (ADR-0005).
            print(
                f"  {family}@{tag}: NO commands.json ({exc}) — family skipped, "
                "no metadata vendored",
                file=sys.stderr,
            )
            families.append(
                {
                    "family": family,
                    "repo": repo_url,
                    "tag": tag,
                    "source": None,
                    "unavailable": "upstream publishes no structured command JSON at this tag",
                    "count": 0,
                    "commands": [],
                    "subcommands": [],
                }
            )
            continue

        commands, subcommands = split_module_commands(data)
        total += len(commands)
        print(f"  {family}@{tag}: {len(commands)} commands, {len(subcommands)} subcommands")
        families.append(
            {
                "family": family,
                "repo": repo_url,
                "tag": tag,
                "source": f"{repo_url}/tree/{tag}/commands.json",
                "count": len(commands),
                "commands": commands,
                "subcommands": subcommands,
            }
        )

    return {
        "_vendored": {
            "warning": "DO NOT EDIT — vendored from the Redis-bundled module repos, not "
            "regenerated by just docs-build",
            "source": f"{GITHUB_TREE}/{redis_version}/modules",
            "redis_version": redis_version,
            "note": "Out-of-tree module tags are read from redis/redis@<redis_version> "
            "modules/<module>/Makefile (MODULE_VERSION), so they match the sources the "
            "pinned Redis release builds its bundled modules from; the in-tree vector-sets "
            "module is pinned to the Redis tag itself. No module commands.json carries "
            "key_specs, and only vector-sets carries arity/command_flags — the modules "
            "declare the rest at RedisModule_CreateCommand time, not as data — so those "
            "fields are absent here rather than fabricated.",
            "retrieved": date.today().isoformat(),
            "regenerate": "uv run website/scripts/vendor-redis-commands.py",
        },
        "count": total,
        "families": families,
    }


def write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Vendor the upstream Redis + bundled-module command metadata "
        "pinned to REDIS_COMPAT_TARGET."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output path for the vendored core command JSON",
    )
    parser.add_argument(
        "--module-output",
        type=Path,
        default=DEFAULT_MODULE_OUTPUT,
        help="Output path for the vendored module-family command JSON",
    )
    args = parser.parse_args()

    redis_version = get_redis_compat_target()

    print(f"Vendoring redis/redis@{redis_version} src/commands/ ...")
    core = vendor_core(redis_version)
    write(args.output, core)
    print(f"Vendored {core['count']} core commands to {args.output}")

    print(f"Vendoring redis/redis@{redis_version} bundled module commands.json ...")
    modules = vendor_modules(redis_version)
    write(args.module_output, modules)
    print(f"Vendored {modules['count']} module commands to {args.module_output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
