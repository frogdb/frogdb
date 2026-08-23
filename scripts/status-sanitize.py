#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: a simple-status reply can never carry an unsanitized dynamic string.

Redis-feel issue 18, the status-reply half of the CRLF-framing invariant its
sibling gate `lint-error-sanitize` pins for errors. A RESP simple string is
framed `+<body>\\r\\n`, so a body containing CR or LF puts *extra frames* on the
wire and desynchronizes the client's reply stream — the exact confused deputy
round-2 issue 38 closed for error replies. The reachable path is Lua: a script
returning `{ok = redis.call('GET', KEYS[1])}` puts a stored, attacker-authored
value straight into a `+…` frame. Redis 8.6.1 closed the same hole upstream with
`addReplyStatusSafe`.

Unlike the error side — where the fix is a sanitizer call at the encoder — the
status fix is a *type*: `WireResponse::Simple` / `Response::Simple` carry a
`SafeStatus`, whose field is private and whose only two constructors are

    SafeStatus::from_static("OK")     const fn, author-written literals only
    SafeStatus::sanitized(dynamic)    maps CR/LF to spaces, like Redis's
                                      sdsmapchars(s, "\\r\\n", "  ", 2)

so the encode paths stay pass-through and an unsanitized dynamic status is
unconstructable. This gate makes that unbypassable by pinning the three
properties the type argument rests on:

  R1  Both enums in `protocol/src/response.rs` declare `Simple(SafeStatus)`, and
      `SafeStatus`'s single field is private. Regressing either to a raw `Bytes`
      re-opens the hole for every crate in the workspace.
  R2  The raw `SafeStatus(..)` tuple construction appears only inside the two
      sanctioned constructors — a third one inside the module could bypass the
      mapping without tripping the privacy rule.
  R3  Repo-wide (non-test): every `SafeStatus::from_static(..)` argument is a
      string literal. `from_static` takes `&'static str`, and a runtime-derived
      `&'static str` (a leaked `String`, a `Box::leak`, an interned key name) is
      exactly the dynamic content that must go through `sanitized` instead.

`#[cfg(test)]` spans and test files are skipped, matching `lint-error-sanitize`.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span, is_test_path  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESPONSE = ROOT / "frogdb-server" / "crates" / "protocol" / "src" / "response.rs"

# R1: the two enum variants and the newtype declaration.
WIRE_VARIANT = re.compile(r"^\s*Simple\(SafeStatus\),\s*$", re.M)
NEWTYPE_DECL = re.compile(r"^pub struct SafeStatus\(Bytes\);\s*$", re.M)
NEWTYPE_DECL_ANY = re.compile(r"^pub struct SafeStatus\b.*$", re.M)
EXPECTED_VARIANTS = 2  # WireResponse::Simple and Response::Simple

# R2: the raw tuple construction, and the two constructors allowed to use it.
RAW_CTOR = re.compile(r"(?<!struct )\bSafeStatus\s*\(")
SANCTIONED_FNS = ("from_static", "sanitized")

# R3: `SafeStatus::from_static(` must be handed a `"..."` string literal.
# Same open/good idiom as `error-sanitize.py`: every `_OPEN` match whose start is
# not also a `_GOOD` match is a violation.
FROM_STATIC_OPEN = re.compile(r"\bSafeStatus::from_static\s*\(")
FROM_STATIC_GOOD = re.compile(
    # `\s` spans newlines and the trailing comma is optional, so a rustfmt-
    # reflowed call over three lines still reads as a literal.
    r'\bSafeStatus::from_static\s*\(\s*(?:r#*)?"(?:[^"\\]|\\.)*"#*\s*,?\s*\)'
)


def line_of(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def blanked(path: Path) -> tuple[str, list[str]]:
    """File text with `#[cfg(test)]` spans blanked out, line numbers preserved."""
    lines = path.read_text().splitlines()
    spans = cfg_test_spans(lines)
    return "\n".join("" if in_any_span(i, spans) else ln for i, ln in enumerate(lines)), lines


def enclosing_fn(lines: list[str], line_no: int) -> str:
    """Name of the nearest `fn` declared at or above `line_no` (1-based)."""
    for i in range(line_no - 1, -1, -1):
        m = re.search(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)", lines[i])
        if m:
            return m.group(1)
    return "<none>"


def tracked_rust_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "*.rs"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [ROOT / p for p in out.split("\0") if p]


def check_response_file(errors: list[str]) -> None:
    if not RESPONSE.is_file():
        errors.append(f"{RESPONSE} not found — did the response types move?")
        return

    text, lines = blanked(RESPONSE)
    rel = RESPONSE.relative_to(ROOT)

    # R1 — variant payload type.
    n = len(WIRE_VARIANT.findall(text))
    if n != EXPECTED_VARIANTS:
        errors.append(
            f"{rel}: expected {EXPECTED_VARIANTS} `Simple(SafeStatus),` variant "
            f"declarations (WireResponse and Response), found {n}. A status reply "
            f"carrying a raw `Bytes` can inject a second wire frame."
        )

    # R1 — the newtype's field stays private.
    if not NEWTYPE_DECL.search(text):
        found = NEWTYPE_DECL_ANY.search(text)
        shape = found.group(0).strip() if found else "<no `pub struct SafeStatus` found>"
        errors.append(
            f"{rel}: `SafeStatus` must be `pub struct SafeStatus(Bytes);` with a "
            f"private field, found: {shape}. A public field lets any crate build "
            f"a status from unsanitized bytes."
        )

    # R2 — raw tuple construction only inside the sanctioned constructors.
    for m in RAW_CTOR.finditer(text):
        ln = line_of(text, m.start())
        fn = enclosing_fn(lines, ln)
        if fn not in SANCTIONED_FNS:
            errors.append(
                f"{rel}:{ln}: raw `SafeStatus(..)` construction inside `fn {fn}` — "
                f"only {SANCTIONED_FNS} may build one, so every status payload is "
                f"either an author-written literal or CR/LF-mapped."
            )


def check_from_static_literals(errors: list[str]) -> None:
    for path in tracked_rust_files():
        rel = path.relative_to(ROOT)
        if is_test_path(rel) or "tests" in rel.parts or "benches" in rel.parts:
            continue
        raw = path.read_text()
        if "SafeStatus::from_static" not in raw:
            continue
        text, _ = blanked(path)
        good = {m.start() for m in FROM_STATIC_GOOD.finditer(text)}
        for m in FROM_STATIC_OPEN.finditer(text):
            if m.start() in good:
                continue
            snippet = text[m.start() : m.start() + 72].splitlines()[0]
            errors.append(
                f"{rel}:{line_of(text, m.start())}: `{snippet}` is not a string "
                f"literal. `from_static` is the const-checked path for author-written "
                f"text; a runtime-derived `&'static str` must use "
                f"`SafeStatus::sanitized(..)`."
            )


def main() -> int:
    errors: list[str] = []
    check_response_file(errors)
    check_from_static_literals(errors)

    if errors:
        print("ERROR: simple-status replies can bypass the CRLF chokepoint:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "       A RESP simple string is framed `+<body>\\r\\n`, so an unsanitized\n"
            "       CR/LF in the body puts a second frame on the wire (redis-feel issue\n"
            "       18; Redis 8.6.1 `addReplyStatusSafe`). Build the payload as\n"
            '         SafeStatus::from_static("OK")       author-written literals\n'
            "         SafeStatus::sanitized(dynamic)      anything else",
            file=sys.stderr,
        )
        return 1

    print("OK: simple-status payloads are SafeStatus — literal or CR/LF-mapped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
