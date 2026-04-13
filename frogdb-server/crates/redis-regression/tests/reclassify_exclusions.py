#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Reclassify documented test exclusions with structured categories.

Rewrites `## Intentional exclusions` bullets from:
    //! - `test name` — old reason
to:
    //! - `test name` — category — prose explanation

Usage:
    uv run --script reclassify_exclusions.py [--dry-run]
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

EM = "\u2014"  # em-dash

# Regex for the current bullet format: //! - `name` — reason
BULLET_RE = re.compile(r"(//!\s*-\s*`[^`]+`\s*)" + EM + r"\s*(.*)")
HEADER_RE = re.compile(r"//!\s*##\s*Intentional exclusions", re.IGNORECASE)
CONT_RE = re.compile(r"^//!\s{2,}(.+)$")

TESTS_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Mapping table: (substring_in_reason, new_category)
#
# Order matters — first match wins. More specific rules must come before
# more general ones. Each entry is (substring, category). The original
# prose is preserved; the category is prepended.
# ---------------------------------------------------------------------------
CATEGORY_MAP: list[tuple[str, str]] = [
    # --- tested-elsewhere ---
    ("fuzzing/stress", "tested-elsewhere"),
    ("fuzz stresser", "tested-elsewhere"),
    ("large-memory tag", "tested-elsewhere"),
    ("large-memory +", "tested-elsewhere"),
    ("large-memory", "tested-elsewhere"),
    ("internal-encoding (quicklist) + stress", "tested-elsewhere"),
    ("internal-encoding (HLL) + stress", "tested-elsewhere"),
    ("internal-encoding (HLL) + fuzzing", "tested-elsewhere"),
    ("internal-encoding (stress)", "tested-elsewhere"),
    ("covered by `tcl_brpop_timeout`", "tested-elsewhere"),
    ("covered by `tcl_brpop_arguments_are_empty", "tested-elsewhere"),
    ("slow " + EM + " fuzz stresser", "tested-elsewhere"),
    # --- intentional-incompatibility:encoding ---
    # Must come before redis-specific catch-alls
    ("internal-encoding", "intentional-incompatibility:encoding"),
    # --- intentional-incompatibility:observability ---
    ("CLIENT TRACKING HOTKEYS", "intentional-incompatibility:observability"),
    ("HOTKEYS", "intentional-incompatibility:observability"),
    ("key-memory-histograms", "intentional-incompatibility:observability"),
    ("DEBUG KEYSIZES", "intentional-incompatibility:observability"),
    ("DEBUG ALLOCSIZE-SLOTS-ASSERT", "intentional-incompatibility:observability"),
    ("INFO keysizes", "intentional-incompatibility:observability"),
    ("FrogDB doesn't populate per-command histograms", "intentional-incompatibility:observability"),
    ("latency event collection not implemented", "intentional-incompatibility:observability"),
    ("CONFIG SET latency-tracking", "intentional-incompatibility:observability"),
    ("latency tracking for", "intentional-incompatibility:observability"),
    ("per-subcommand latency", "intentional-incompatibility:observability"),
    ("verify latency magnitude", "intentional-incompatibility:observability"),
    ("config validation (non-numeric", "intentional-incompatibility:observability"),
    ("histogram always empty", "intentional-incompatibility:observability"),
    ("expire-cycle timing events", "intentional-incompatibility:observability"),
    ("FrogDB strictly validates event names", "intentional-incompatibility:observability"),
    ("FrogDB's LATENCY HELP", "intentional-incompatibility:observability"),
    ("upstream asserts `reset > 0`", "intentional-incompatibility:observability"),
    ("introspection field not implemented", "intentional-incompatibility:observability"),
    ("errorstat", "intentional-incompatibility:observability"),
    ("error tracking across", "intentional-incompatibility:observability"),
    ("error tracking in EVAL", "intentional-incompatibility:observability"),
    ("EVALSHA " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("XGROUP CREATECONSUMER " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("unknown command " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("arity error in MULTI queuing", "intentional-incompatibility:observability"),
    ("wrong arg count", "intentional-incompatibility:observability"),
    ("maxmemory " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("ACL " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("AUTH failure " + EM + " errorstat", "intentional-incompatibility:observability"),
    ("pubsub_clients count", "intentional-incompatibility:observability"),
    ("watching_clients", "intentional-incompatibility:observability"),
    ("Redis-internal cmdstat format", "intentional-incompatibility:observability"),
    ("Redis-internal errorstat format", "intentional-incompatibility:observability"),
    ("Redis-internal 128-error-type cap", "intentional-incompatibility:observability"),
    ("CLIENT UNBLOCK error type tracking", "intentional-incompatibility:observability"),
    ("FrogDB logs each command inside a MULTI", "intentional-incompatibility:observability"),
    ("FrogDB logs blocking commands", "intentional-incompatibility:observability"),
    ("tests Redis-internal command rewriting", "intentional-incompatibility:observability"),
    # --- intentional-incompatibility:config ---
    ("needs:config (notify-keyspace-events)", "intentional-incompatibility:config"),
    ("needs:config-maxmemory", "intentional-incompatibility:config"),
    ("needs:config-resetstat", "intentional-incompatibility:config"),
    ("Redis-internal feature (ACL file)", "intentional-incompatibility:config"),
    ("ACL v2 selectors removed", "intentional-incompatibility:config"),
    ("external:skip; FrogDB port is immutable", "intentional-incompatibility:config"),
    ("external:skip; FrogDB bind is immutable", "intentional-incompatibility:config"),
    (
        "external:skip; FrogDB does not implement bind-source-addr",
        "intentional-incompatibility:config",
    ),
    ("external:skip; depends on CONFIG REWRITE", "intentional-incompatibility:config"),
    ("FrogDB does not implement protected mode", "intentional-incompatibility:config"),
    ("FrogDB does not implement client-idle", "intentional-incompatibility:config"),
    ("Redis-internal config flag", "intentional-incompatibility:config"),
    ("Redis-internal config sanity", "intentional-incompatibility:config"),
    ("Redis-internal config-during-RDB-load", "intentional-incompatibility:config"),
    # --- intentional-incompatibility:replication ---
    ("replication-internal", "intentional-incompatibility:replication"),
    ("needs:repl " + EM, "intentional-incompatibility:replication"),
    ("needs:repl tag", "intentional-incompatibility:replication"),
    ("needs:repl (min-slaves", "intentional-incompatibility:replication"),
    ("needs:repl", "intentional-incompatibility:replication"),
    # --- intentional-incompatibility:protocol ---
    ("RESP3-only", "intentional-incompatibility:protocol"),
    ("needs:reset", "intentional-incompatibility:protocol"),
    ("Redis-internal RESET semantics", "intentional-incompatibility:protocol"),
    ("CLIENT CACHING / TRACKING", "intentional-incompatibility:protocol"),
    ("tested via CLIENT REPLY SKIP", "intentional-incompatibility:protocol"),
    # --- intentional-incompatibility:persistence ---
    ("needs:save", "intentional-incompatibility:persistence"),
    ("needs:debug needs:save", "intentional-incompatibility:persistence"),
    ("DEBUG RELOAD not implemented", "intentional-incompatibility:persistence"),
    ("DEBUG RELOAD +", "intentional-incompatibility:persistence"),
    ("DEBUG RELOAD", "intentional-incompatibility:persistence"),
    ("RDB + key-memory", "intentional-incompatibility:persistence"),
    ("DUMP/RESTORE not implemented", "intentional-incompatibility:persistence"),
    ("aof", "intentional-incompatibility:persistence"),
    # --- intentional-incompatibility:debug ---
    ("needs:debug (OBJECT IDLETIME)", "intentional-incompatibility:debug"),
    ("needs:debug (PFDEBUG)", "intentional-incompatibility:debug"),
    ("needs:debug + singledb:skip", "intentional-incompatibility:debug"),
    ("OBJECT IDLETIME/FREQ not tracked", "intentional-incompatibility:debug"),
    ("uses populate (DEBUG)", "intentional-incompatibility:debug"),
    ("needs:debug " + EM, "intentional-incompatibility:debug"),
    ("needs:debug", "intentional-incompatibility:debug"),
    # --- intentional-incompatibility:memory ---
    ("`maxmemory-clients` not implemented", "intentional-incompatibility:memory"),
    ("client eviction", "intentional-incompatibility:memory"),
    ("HELLO 3 + CLIENT TRACKING feedback loop", "intentional-incompatibility:memory"),
    # --- intentional-incompatibility:single-db ---
    ("single-DB", "intentional-incompatibility:single-db"),
    ("singledb", "intentional-incompatibility:single-db"),
    # --- intentional-incompatibility:cluster ---
    ("cluster:skip", "intentional-incompatibility:cluster"),
    ("cluster-only", "intentional-incompatibility:cluster"),
    ("cluster-specific Redis dict", "intentional-incompatibility:cluster"),
    # --- intentional-incompatibility:scripting ---
    ("intentional behavioral diff", "intentional-incompatibility:scripting"),
    ("needs:ACL (script-level", "intentional-incompatibility:scripting"),
    ("FrogDB's EVAL shebang", "intentional-incompatibility:scripting"),
    ("FrogDB's EVAL handler does not parse", "intentional-incompatibility:scripting"),
    ("FrogDB parses the `no-cluster` flag", "intentional-incompatibility:scripting"),
    ("FrogDB's SPUBLISH metadata", "intentional-incompatibility:scripting"),
    # --- intentional-incompatibility:cli ---
    ("Redis-internal CLI", "intentional-incompatibility:cli"),
    # --- redis-specific (catch-all patterns) ---
    ("Redis-internal feature (script timeout)", "redis-specific"),
    ("Redis-internal feature (FUNCTION DUMP", "redis-specific"),
    ("Redis-internal feature", "redis-specific"),
    ("Redis-internal", "redis-specific"),
    ("Redis single-threaded", "redis-specific"),
    ("Redis event loop", "redis-specific"),
    ("Redis dict/rehashing", "redis-specific"),
    ("Redis DEBUG info section", "redis-specific"),
    ("Redis buffer limit stats", "redis-specific"),
    ("Redis-specific peak timestamp", "redis-specific"),
    ("Redis-internal io-threads", "redis-specific"),
    ("jemalloc", "redis-specific"),
    ("platform-specific", "redis-specific"),
    ("allocator-calibrated", "redis-specific"),
    ("`$::force_failure`", "redis-specific"),
    ("external:skip", "redis-specific"),
    ("MEMORY STATS `db.9", "redis-specific"),
    ("Redis-internal CONFIG behavior", "redis-specific"),
    ("Redis-internal pending command pool", "redis-specific"),
    ("Redis-internal channel-level pipeline", "redis-specific"),
    ("Redis-internal reply buffer sizing", "redis-specific"),
    ("Redis-internal ordering invariant", "redis-specific"),
    ("Redis-internal command name", "redis-specific"),
    ("Redis-internal expire scan", "redis-specific"),
    ("Redis-internal HLL self-test", "redis-specific"),
    ("Redis-internal SIMD path", "redis-specific"),
    ("Redis-internal desync simulation", "redis-specific"),
    ("Redis-internal cron path", "redis-specific"),
    ("Redis-internal session-state ordering", "redis-specific"),
    ("Redis-internal arity error format", "redis-specific"),
    ("Redis-internal error message format", "redis-specific"),
    ("Redis-internal syntax-error format", "redis-specific"),
    ("Redis-internal object model", "redis-specific"),
    ("Redis-internal command spec", "redis-specific"),
    ("Redis-internal stat", "redis-specific"),
    ("Redis-internal Lua runtime", "redis-specific"),
    ("Redis-internal allocator", "redis-specific"),
    ("Redis-internal dict/LUT", "redis-specific"),
    ("jemalloc-only", "redis-specific"),
    ("LRM not implemented", "redis-specific"),
    ("lazyfreed_objects", "redis-specific"),
    ("lazyfree_pending_objects", "redis-specific"),
]


def classify(reason: str) -> str | None:
    """Return the new category for the given reason string, or None if unmatched."""
    for substring, category in CATEGORY_MAP:
        if substring in reason:
            return category
    return None


def process_file(rs_path: Path, *, dry_run: bool) -> tuple[int, int, list[str]]:
    """Process one port file. Returns (total_bullets, changed, unmatched_reasons)."""
    with open(rs_path) as f:
        lines = f.readlines()

    in_section = False
    new_lines: list[str] = []
    total = 0
    changed = 0
    unmatched: list[str] = []

    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.rstrip()

        if not stripped.startswith("//!"):
            if in_section:
                in_section = False
            new_lines.append(raw)
            i += 1
            continue

        if HEADER_RE.search(stripped):
            in_section = True
            new_lines.append(raw)
            i += 1
            continue

        if not in_section:
            new_lines.append(raw)
            i += 1
            continue

        # Inside the exclusions section — check for a bullet
        m = BULLET_RE.match(stripped)
        if not m:
            # Sub-heading, blank //! line, or prose — preserve as-is
            new_lines.append(raw)
            i += 1
            continue

        # Found a bullet: //! - `name` — reason...
        prefix = m.group(1)  # everything up to and including the backtick-close + space
        reason = m.group(2)
        total += 1
        i += 1

        # Collect continuation lines (indented //!   text)
        while i < len(lines):
            cont_line = lines[i].rstrip()
            cm = CONT_RE.match(cont_line)
            if cm:
                reason = reason + " " + cm.group(1)
                i += 1
            else:
                break

        reason = reason.strip()

        # Check if already reclassified (has one of our exact categories)
        VALID_CATEGORIES = {
            "redis-specific",
            "tested-elsewhere",
            "broken",
        }
        VALID_PREFIXES = ("intentional-incompatibility:",)
        already_m = re.match(r"^([a-z][a-z0-9:-]*)\s*" + EM + r"\s*(.+)$", reason)
        if already_m:
            cat = already_m.group(1)
            if cat in VALID_CATEGORIES or cat.startswith(VALID_PREFIXES[0]):
                # Already reclassified — preserve as-is
                new_lines.append(f"{prefix}{EM} {reason}\n")
                continue

        # Classify
        category = classify(reason)
        if category is None:
            unmatched.append(f"{rs_path.name}: {reason}")
            # Leave unchanged
            new_lines.append(f"{prefix}{EM} {reason}\n")
            continue

        # Build new line: //! - `name` — category — prose
        # Clean up the prose: remove redundant category-like prefixes that are
        # now captured by the category itself. Keep the full prose for context.
        new_line = f"{prefix}{EM} {category} {EM} {reason}\n"
        changed += 1
        new_lines.append(new_line)

    if not dry_run and changed > 0:
        with open(rs_path, "w") as f:
            f.writelines(new_lines)

    return total, changed, unmatched


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    total_bullets = 0
    total_changed = 0
    all_unmatched: list[str] = []

    for fname in sorted(os.listdir(TESTS_DIR)):
        if not (fname.endswith("_tcl.rs") or fname.endswith("_regression.rs")):
            continue
        fpath = TESTS_DIR / fname
        bullets, changed_count, unmatched = process_file(fpath, dry_run=dry_run)
        if bullets > 0:
            status = "DRY-RUN" if dry_run else "WRITTEN"
            print(f"  {fname}: {bullets} bullets, {changed_count} changed [{status}]")
        total_bullets += bullets
        total_changed += changed_count
        all_unmatched.extend(unmatched)

    print(f"\nTotal: {total_bullets} bullets, {total_changed} changed")

    if all_unmatched:
        print(f"\nUNMATCHED ({len(all_unmatched)}):", file=sys.stderr)
        for u in all_unmatched:
            print(f"  {u}", file=sys.stderr)
        sys.exit(1)
    else:
        print("All entries matched.")


if __name__ == "__main__":
    main()
