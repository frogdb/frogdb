#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Redis TCL test coverage audit (Rust-side tracker).

Parses upstream Redis 8.6.0 .tcl test files and the Rust port files in
this directory, diffs test names via token-based fuzzy matching, and
classifies missing tests against the per-port `## Intentional exclusions`
sections plus upstream tag-based exclusion rules.

This script is the tracking tool — it does not run any tests. Use
`cargo test -p frogdb-redis-regression` to actually exercise the ports.
Run with `uv run --script audit_tcl.py all > /tmp/claude/audit_results.json`
to refresh the audit JSON, then `show_missing.py <port>.rs` to inspect.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REDIS_ROOT = Path("/tmp/claude/redis-tcl/redis-8.6.0")
REGRESSION_ROOT = Path(__file__).parent

# Maps upstream .tcl relative path → Rust port filename. A single Rust file can
# cover multiple upstream files (e.g. list_tcl.rs → list/list-2/list-3).
PORT_MAP: dict[str, str] = {
    "unit/type/string.tcl": "string_tcl.rs",
    "unit/type/list.tcl": "list_tcl.rs",
    "unit/type/list-2.tcl": "list_tcl.rs",
    "unit/type/list-3.tcl": "list_tcl.rs",
    "unit/type/set.tcl": "set_tcl.rs",
    "unit/type/hash.tcl": "hash_tcl.rs",
    "unit/type/hash-field-expire.tcl": "hash_field_expire_tcl.rs",
    "unit/type/zset.tcl": "zset_tcl.rs",
    "unit/type/stream.tcl": "stream_tcl.rs",
    "unit/type/stream-cgroups.tcl": "stream_cgroups_tcl.rs",
    "unit/type/incr.tcl": "incr_tcl.rs",
    "unit/acl.tcl": "acl_tcl.rs",
    # ACL v2 selectors removed in commit 8121bfee. The 5 non-selector tests
    # moved into acl_tcl.rs; the remaining selector-based tests are OOS and
    # documented in `acl_tcl.rs`'s `## Intentional exclusions` section.
    "unit/acl-v2.tcl": "acl_tcl.rs",
    "unit/auth.tcl": "auth_tcl.rs",
    "unit/bitfield.tcl": "bitfield_tcl.rs",
    "unit/bitops.tcl": "bitops_tcl.rs",
    "unit/cluster/scripting.tcl": "cluster_scripting_tcl.rs",
    "unit/cluster/sharded-pubsub.tcl": "cluster_sharded_pubsub_tcl.rs",
    "unit/dump.tcl": "dump_tcl.rs",
    "unit/expire.tcl": "expire_tcl.rs",
    "unit/functions.tcl": "functions_tcl.rs",
    "unit/geo.tcl": "geo_tcl.rs",
    "unit/hyperloglog.tcl": "hyperloglog_tcl.rs",
    "unit/info-command.tcl": "info_command_tcl.rs",
    "unit/introspection.tcl": "introspection_tcl.rs",
    "unit/introspection-2.tcl": "introspection2_tcl.rs",
    "unit/keyspace.tcl": "keyspace_tcl.rs",
    "unit/multi.tcl": "multi_tcl.rs",
    "unit/pause.tcl": "pause_tcl.rs",
    "unit/protocol.tcl": "protocol_tcl.rs",
    "unit/pubsub.tcl": "pubsub_tcl.rs",
    "unit/pubsubshard.tcl": "pubsubshard_tcl.rs",
    "unit/querybuf.tcl": "querybuf_tcl.rs",
    "unit/replybufsize.tcl": "replybufsize_tcl.rs",
    "unit/scan.tcl": "scan_tcl.rs",
    "unit/scripting.tcl": "scripting_tcl.rs",
    "unit/sort.tcl": "sort_tcl.rs",
    "unit/tracking.tcl": "tracking_tcl.rs",
    "unit/wait.tcl": "wait_tcl.rs",
    # FrogDB-side regression files that already cover the upstream tests
    # but use the `_regression.rs` naming convention.
    "unit/quit.tcl": "quit_regression.rs",
    "unit/info.tcl": "info_regression.rs",
    "unit/maxmemory.tcl": "maxmemory_regression.rs",
}


@dataclass
class TestEntry:
    name: str
    tags: list[str] = field(default_factory=list)
    line: int = 0


def _balanced(content: str, start: int) -> tuple[str, int]:
    """Given content[start] == '{', return (body, end_index) where end_index
    points just past the closing '}' of the balanced brace block."""
    assert content[start] == "{"
    depth = 0
    i = start
    while i < len(content):
        c = content[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return content[start + 1 : i], i + 1
        elif c == "\\" and i + 1 < len(content):
            i += 2
            continue
        i += 1
    raise ValueError("Unbalanced braces")


def extract_tests(tcl_path: Path) -> list[TestEntry]:
    """Extract all test {...} blocks with their inherited tags.

    We walk the file top-to-bottom, tracking a stack of active tag blocks
    (introduced by `tags {...} { ... test blocks ... }`). Also tracks tags
    declared in `start_server { tags {...} } { ... }`.
    """
    content = tcl_path.read_text()

    # Walk the file and emit (position, kind, data) events.
    tests: list[TestEntry] = []
    tag_stack: list[list[str]] = []

    # Use a single regex to find `test`, `tags`, `start_server` constructs.
    # We parse incrementally so we can track brace ranges.
    i = 0
    n = len(content)

    def line_of(idx: int) -> int:
        return content.count("\n", 0, idx) + 1

    # Find all scope ends as we pass them.
    scope_ends: list[tuple[int, list[str]]] = []  # (end_idx, tag_list_to_pop)

    while i < n:
        # Pop any tag scopes we've exited.
        while scope_ends and i >= scope_ends[-1][0]:
            scope_ends.pop()
            tag_stack.pop()

        # Skip line comments
        if content[i] == "#":
            nl = content.find("\n", i)
            i = nl + 1 if nl != -1 else n
            continue

        # Look for `test` keyword at word boundary
        m_test = re.match(r"test[ \t]+", content[i:])
        m_tags = re.match(r"tags[ \t]+", content[i:])
        m_ss = re.match(r"start_server[ \t]+", content[i:])

        # For `test`, require that it's the first non-whitespace token on
        # its line (or preceded by `;` or `{`). Otherwise we pick up things
        # like `r fcall test {*}` where `test` is a command argument.
        if m_test:
            # Scan backwards to find the start of the line
            line_start = content.rfind("\n", 0, i) + 1
            prefix = content[line_start:i]
            if prefix and prefix.strip():
                # Something non-whitespace before `test` on this line.
                # Allow if it ends in `;` or `{`.
                if not re.search(r"[;{]\s*$", prefix):
                    i += 1
                    continue
        # For `tags` and `start_server`, the original boundary check is fine.
        elif i > 0 and not re.match(r"[\s;{]", content[i - 1]):
            i += 1
            continue

        if m_test:
            j = i + m_test.end()
            # Next token is the test name: either {name} or "name"
            if j >= n:
                i += 1
                continue
            test_name: str | None = None
            after_name = j
            if content[j] == "{":
                name, after_name = _balanced(content, j)
                test_name = name.strip()
            elif content[j] == '"':
                # Find closing quote, honoring escapes
                k = j + 1
                while k < n:
                    if content[k] == "\\":
                        k += 2
                        continue
                    if content[k] == '"':
                        break
                    k += 1
                test_name = content[j + 1 : k]
                after_name = k + 1
            else:
                i += 1
                continue

            # Now parse the optional trailing arguments after the body, looking
            # for a `{tags}` arg. Upstream Redis uses two forms:
            #   test {name} {body}
            #   test {name} {body} {expected_result} {tag_list}
            # The trailing `{tag_list}` is at the END and is what carries
            # `needs:debug`, `needs:config-maxmemory`, etc. We collect every
            # subsequent `{...}` block on the same logical line and treat the
            # LAST such block as the test-level tags if it parses as a list of
            # tag tokens (no `;` or `{` characters that suggest TCL code).
            inline_tags: list[str] = []
            k = after_name
            arg_blocks: list[tuple[int, int]] = []
            while k < n:
                # Skip whitespace and TCL line continuations.
                while k < n and content[k] in " \t":
                    k += 1
                if k < n and content[k] == "\\" and k + 1 < n and content[k + 1] == "\n":
                    k += 2
                    continue
                # Stop at a hard newline (end of the test invocation).
                if k >= n or content[k] == "\n":
                    break
                if content[k] == "{":
                    try:
                        body, after_block = _balanced(content, k)
                    except ValueError:
                        break
                    arg_blocks.append((k + 1, after_block - 1))
                    k = after_block
                    continue
                # Some other token (a bare word or quoted string) — skip it.
                if content[k] == '"':
                    m = k + 1
                    while m < n and content[m] != '"':
                        if content[m] == "\\":
                            m += 2
                            continue
                        m += 1
                    k = m + 1
                    continue
                # Bare token: advance until whitespace.
                while k < n and content[k] not in " \t\n":
                    k += 1
            if len(arg_blocks) >= 3:
                # Form: `test {name} {body} {expected_result} {tags}`. The
                # last block is the trailing tag list. Two-block form is
                # `test {name} {body} {expected_result}` (no tags).
                start, end = arg_blocks[-1]
                tag_blob = content[start:end]
                # Tag lists are flat — just whitespace-separated tokens.
                tag_items = re.findall(r'"([^"]*)"|(\S+)', tag_blob)
                inline_tags = [a or b for a, b in tag_items]

            tests.append(
                TestEntry(
                    name=test_name,
                    tags=[t for ts in tag_stack for t in ts] + inline_tags,
                    line=line_of(i),
                )
            )
            i = k
            continue

        if m_tags:
            j = i + m_tags.end()
            if j >= n or content[j] != "{":
                i += 1
                continue
            # tags {taglist} { body }
            tag_body, after_tags = _balanced(content, j)
            # Parse individual tags (may be quoted or bare)
            tag_items = re.findall(r'"([^"]*)"|(\S+)', tag_body)
            tags = [a or b for a, b in tag_items]
            # Skip whitespace
            k = after_tags
            while k < n and content[k] in " \t\n":
                k += 1
            if k < n and content[k] == "{":
                body_start = k
                _, body_end = _balanced(content, body_start)
                tag_stack.append(tags)
                scope_ends.append((body_end, tags))
            i = k + 1 if k < n else n
            continue

        if m_ss:
            j = i + m_ss.end()
            if j >= n or content[j] != "{":
                i += 1
                continue
            # start_server { opts } { body }
            opts_body, after_opts = _balanced(content, j)
            # Try to extract "tags {...}" from opts_body
            m_opts_tags = re.search(r"tags\s*\{([^}]*)\}", opts_body)
            opts_tags: list[str] = []
            if m_opts_tags:
                opts_tags = re.findall(r'"([^"]*)"|(\S+)', m_opts_tags.group(1))
                opts_tags = [a or b for a, b in opts_tags]
            k = after_opts
            while k < n and content[k] in " \t\n":
                k += 1
            if k < n and content[k] == "{":
                body_start = k
                _, body_end = _balanced(content, body_start)
                tag_stack.append(opts_tags)
                scope_ends.append((body_end, opts_tags))
                i = body_start + 1
                continue
            i = k + 1 if k < n else n
            continue

        i += 1

    return tests


STOP_WORDS = {
    "the",
    "a",
    "an",
    "is",
    "are",
    "was",
    "were",
    "of",
    "in",
    "on",
    "at",
    "to",
    "for",
    "with",
    "by",
    "from",
    "it",
    "its",
    "this",
    "that",
    "be",
    "as",
    "and",
    "or",
    "but",
    "not",
    "no",
    "can",
    "cant",
    "cannot",
    "do",
    "does",
    "dont",
    "test",
    "tests",
    "testing",
    "should",
    "when",
    "if",
    "then",
    "else",
    "into",
    "out",
    "up",
    "down",
    "only",
    "also",
    "so",
}


def normalize(name: str) -> str:
    """Normalize a test name so upstream strings and Rust fn names can match."""
    s = name.lower()
    s = s.strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s


def tokens(name: str) -> set[str]:
    """Break a normalized name into meaningful tokens (excluding stop words).

    Strips TCL variables ($encoding, $type, etc.) which are loop parameters
    and would otherwise be treated as literal tokens.
    """
    # Strip TCL variable references
    name = re.sub(r"\$[a-zA-Z_][a-zA-Z0-9_]*", "", name)
    # Strip trailing "- something" clauses (often describe context like encoding)
    # e.g., "ZADD XX existing key - $encoding" -> "ZADD XX existing key"
    parts = re.split(r"[^a-z0-9]+", name.lower())
    return {p for p in parts if p and p not in STOP_WORDS and len(p) > 1}


def fuzzy_score(upstream: str, rust_fn: str) -> tuple[float, int]:
    """Return (score, intersection_count) using max(Jaccard, rust_coverage).

    - Jaccard: |intersection| / |union|
    - rust_coverage: |intersection| / |rust_tokens|   (how much of the rust fn
      name is explained by the upstream name). Good at catching cases where
      the rust name is a shortened subset of the upstream name.
    """
    u = tokens(upstream)
    r_name = rust_fn[len("tcl_") :] if rust_fn.startswith("tcl_") else rust_fn
    r = tokens(r_name)
    if not u or not r:
        return (0.0, 0)
    inter = u & r
    inter_n = len(inter)
    jaccard = inter_n / len(u | r)
    rust_cov = inter_n / len(r)
    return (max(jaccard, rust_cov), inter_n)


def extract_rust_tests(rs_path: Path) -> list[str]:
    """Extract `#[tokio::test] async fn ...` names from a Rust port file.

    Allows intervening attributes like `#[ignore = "..."]` between
    `#[tokio::test]` and the fn declaration. Captures both `tcl_*` (the
    convention for `*_tcl.rs` files) and the bare names used in
    `*_regression.rs` files.
    """
    if not rs_path.exists():
        return []
    content = rs_path.read_text()
    # Match #[tokio::test] followed (after any number of attribute lines
    # and whitespace) by `async fn <name>`. The `fuzzy_score` helper strips
    # the `tcl_` prefix when present, so both naming conventions tokenize
    # consistently.
    names = re.findall(
        r"#\[tokio::test\b[^\]]*\][^\n]*\n(?:\s*#\[[^\]]*\][^\n]*\n)*\s*async\s+fn\s+([a-z_][a-z0-9_]*)",
        content,
    )
    return names


# Markers for the `## Intentional exclusions` section in a port file's
# `//!` doc-comment header. Each bullet has the form:
#     //! - `<upstream test name>` — <one-line reason>
# The em-dash separator (` — `) is required to make the regex unambiguous.
INTENTIONAL_HEADER_RE = re.compile(r"//!\s*##\s*Intentional exclusions", re.IGNORECASE)
INTENTIONAL_BULLET_RE = re.compile(r"//!\s*-\s*`([^`]+)`\s*—\s*(.*)$")


def extract_documented_exclusions(rs_path: Path) -> dict[str, str]:
    """Parse a Rust port file's module doc-comment header and return
    {test_name: reason} for any `## Intentional exclusions` section.

    The section starts at a `//! ## Intentional exclusions` line and runs to
    the end of the doc-comment header (first non-`//!` line). Within the
    section, any line matching the bullet regex contributes an entry; blank
    `//!` lines and non-bullet `//!` lines (sub-headings, prose) are
    tolerated and ignored. Test names are stored verbatim; the caller
    normalizes on lookup.
    """
    if not rs_path.exists():
        return {}
    excluded: dict[str, str] = {}
    in_section = False
    with open(rs_path) as f:
        for line in f:
            if not line.startswith("//!"):
                # Doc-comment header ends at the first non-`//!` line.
                break
            if INTENTIONAL_HEADER_RE.search(line):
                in_section = True
                continue
            if in_section:
                m = INTENTIONAL_BULLET_RE.match(line.rstrip())
                if m:
                    excluded[m.group(1).strip()] = m.group(2).strip()
                # Blank lines, sub-headings, and prose are tolerated.
    return excluded


# Tag patterns → exclusion reason. If any of a missing test's tags contain
# one of these substrings, it's excluded for that reason.
EXCLUSION_RULES = [
    ("external:skip", "external:skip (needs fresh Redis instance)"),
    ("needs:repl", "needs:repl (requires replication)"),
    ("needs:debug", "needs:debug (requires DEBUG command)"),
    ("needs:save", "needs:save (requires SAVE/RDB)"),
    ("needs:reset", "needs:reset (requires RESET command)"),
    ("needs:config-maxmemory", "needs:config-maxmemory"),
    ("singledb:skip", "singledb:skip (multi-DB SELECT test)"),
    ("cluster-only", "cluster-only"),
    ("large-memory", "large-memory (stress test)"),
    ("slow", "slow (performance test)"),
    ("resp3", "resp3 (HELLO 3 protocol)"),
    ("tls", "tls (TLS required)"),
    ("aof", "aof (AOF not supported)"),
    ("cli", "cli (requires redis-cli binary)"),
    ("logreqres:skip", "logreqres:skip"),
    ("moduleapi", "moduleapi (not supported)"),
]


def classify_missing(tags: list[str]) -> tuple[str, str] | None:
    """Return (category, description) if this test's tags justify exclusion.

    Tags may be stored as either individual items or as a single quoted
    string containing multiple space-separated tags. We search substrings.
    """
    blob = " ".join(tags).lower()
    for pattern, description in EXCLUSION_RULES:
        if pattern in blob:
            return (pattern, description)
    return None


def audit_ported() -> dict:
    """Run the port-by-port test-level audit."""
    results = {}
    # Group by port file (since list_tcl.rs covers 3 upstream files).
    ports: dict[str, list[str]] = {}
    for upstream, rust in PORT_MAP.items():
        ports.setdefault(rust, []).append(upstream)

    for rust, upstreams in sorted(ports.items()):
        rs_path = REGRESSION_ROOT / rust
        rust_fns = extract_rust_tests(rs_path)
        # Parse `## Intentional exclusions` bullets from the port file's
        # doc-comment header. Keys are normalized so a missing upstream test
        # can be matched against them by token-equivalence.
        documented_raw = extract_documented_exclusions(rs_path)
        documented_norm: dict[str, tuple[str, str]] = {
            normalize(name): (name, reason) for name, reason in documented_raw.items()
        }
        documented_count = 0

        per_upstream = {}
        total_up = 0
        matched = 0
        missing_records = []
        for upstream in upstreams:
            tcl_path = REDIS_ROOT / "tests" / upstream
            try:
                tests = extract_tests(tcl_path)
            except Exception as e:
                per_upstream[upstream] = {"error": str(e)}
                continue

            file_missing = []
            file_matched = 0
            file_excluded_by_tag = 0
            file_documented = 0
            for t in tests:
                # Find best-matching Rust fn by combined score.
                best_score = 0.0
                best_inter = 0
                best_fn = None
                for fn in rust_fns:
                    s, inter = fuzzy_score(t.name, fn)
                    if s > best_score or (s == best_score and inter > best_inter):
                        best_score = s
                        best_inter = inter
                        best_fn = fn
                # Require at least 2 intersection tokens with reasonable score,
                # or a very high score with 1 token (for short names).
                is_match = (best_score >= 0.5 and best_inter >= 2) or (
                    best_score >= 0.75 and best_inter >= 1
                )
                if is_match:
                    file_matched += 1
                    matched += 1
                    continue
                # Not matched. Check the doc-comment header's intentional
                # exclusions list before falling back to tag-based rules so
                # explicit "we chose not to port this" decisions take
                # precedence over the implicit tag heuristic.
                norm = normalize(t.name)
                doc_hit = documented_norm.get(norm)
                if doc_hit:
                    file_documented += 1
                    documented_count += 1
                    record = {
                        "name": t.name,
                        "line": t.line,
                        "tags": t.tags,
                        "best_match": best_fn,
                        "best_score": round(best_score, 2),
                        "exclusion": "documented",
                        "exclusion_reason": f"documented: {doc_hit[1]}",
                    }
                    file_missing.append(record)
                    missing_records.append({"upstream": upstream, **record})
                    continue
                # Otherwise, check if upstream tags justify exclusion.
                exclusion = classify_missing(t.tags)
                record = {
                    "name": t.name,
                    "line": t.line,
                    "tags": t.tags,
                    "best_match": best_fn,
                    "best_score": round(best_score, 2),
                    "exclusion": exclusion[0] if exclusion else None,
                    "exclusion_reason": exclusion[1] if exclusion else None,
                }
                if exclusion:
                    file_excluded_by_tag += 1
                file_missing.append(record)
                missing_records.append({"upstream": upstream, **record})
            total_up += len(tests)
            per_upstream[upstream] = {
                "upstream_tests": len(tests),
                "matched": file_matched,
                "missing": len(file_missing),
                "documented": file_documented,
                "excluded_by_tag": file_excluded_by_tag,
                "unclassified_gap": (len(file_missing) - file_documented - file_excluded_by_tag),
            }

        results[rust] = {
            "upstreams": upstreams,
            "upstream_total": total_up,
            "rust_fn_count": len(rust_fns),
            "matched": matched,
            "documented_count": documented_count,
            "documented_entries": len(documented_raw),
            "missing_total": total_up - matched,
            "per_upstream": per_upstream,
            "missing": missing_records,
        }

    return results


def audit_non_ported() -> dict:
    """Classify non-ported in-scope files by their tag metadata."""
    in_scope_dirs = ["unit", "unit/type", "unit/cluster", "integration"]
    all_files: list[str] = []
    for d in in_scope_dirs:
        p = REDIS_ROOT / "tests" / d
        if not p.exists():
            continue
        for f in sorted(p.glob("*.tcl")):
            all_files.append(f"{d}/{f.name}")

    ported_upstreams = set(PORT_MAP.keys())
    non_ported = [f for f in all_files if f not in ported_upstreams]

    results = {}
    for rel in non_ported:
        tcl_path = REDIS_ROOT / "tests" / rel
        try:
            tests = extract_tests(tcl_path)
        except Exception as e:
            results[rel] = {"error": str(e)}
            continue

        # Extract tags from the first line of the file (often has top-level
        # start_server tags block).
        content = tcl_path.read_text()
        first_tags: list[str] = []
        m = re.search(r"start_server\s*\{([^{}]*?)tags\s*\{([^}]*)\}", content)
        if m:
            raw = m.group(2)
            first_tags = re.findall(r'"([^"]*)"|(\S+)', raw)
            first_tags = [a or b for a, b in first_tags]

        # Collect unique tags across all tests
        all_tags = set(first_tags)
        for t in tests:
            for tag in t.tags:
                all_tags.add(tag)

        results[rel] = {
            "test_count": len(tests),
            "top_level_tags": first_tags,
            "all_tags": sorted(all_tags),
            "first_5_tests": [t.name for t in tests[:5]],
        }

    return {"files": results, "in_scope_total": len(all_files)}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    out = {}
    if mode in ("all", "ported"):
        out["ported"] = audit_ported()
    if mode in ("all", "nonported"):
        out["nonported"] = audit_non_ported()
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
