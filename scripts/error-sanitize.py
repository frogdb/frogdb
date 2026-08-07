#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: every CRLF-framed error frame is built through the sanitizer.

Hardening-2 W1 rule C10 (`.scratch/hardening-2/PRD.md` §3.1, re-scoped
2026-08-07 after the round-2 #38 fix landed). The sanitizer
`frogdb_protocol::sanitize_error_message` already ships at the encoder boundary
in `frogdb-server/crates/protocol/src/response.rs`, stripping CR/LF that would
otherwise let an attacker-authored error message inject a second RESP frame
(e.g. a forged `+OK`). This pin makes that boundary unbypassable: the two
CRLF-framed error frame constructions in that file —

    Resp2BytesFrame::Error(...)              RESP2 simple error (and the RESP2
                                             downgrade of a RESP3 blob error)
    Resp3BytesFrame::SimpleError { data: ... }   RESP3 simple error

— must take their payload from `sanitize_error_message(...)` and nowhere else.

The RESP3 `Resp3BytesFrame::BlobError { data: ... }` is *length-framed*
(`!<len>\r\n<bytes>\r\n`), so an embedded CR/LF cannot start a new frame; it is
deliberately NOT sanitized and this gate deliberately does not require it to be.

Analogue: the shipped `lint-redirect-seam`. Zero true violations are expected
(the fix shipped), so the allowlist is empty. A match keys on the `data:` field
so the `match frame { Resp3BytesFrame::SimpleError { attributes, .. } => ... }`
destructuring is not mistaken for a construction.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESPONSE = ROOT / "frogdb-server" / "crates" / "protocol" / "src" / "response.rs"

# RESP2 tuple-variant construction: `Resp2BytesFrame::Error(` — the payload must
# be `sanitize_error_message(...)`.
RESP2_OPEN = re.compile(r"\bResp2BytesFrame::Error\s*\(")
RESP2_GOOD = re.compile(r"\bResp2BytesFrame::Error\s*\(\s*sanitize_error_message\b")

# RESP3 struct-variant construction: `Resp3BytesFrame::SimpleError { ... }`.
# A *construction* has a `data:` field; a match/destructure pattern
# (`{ attributes: a, .. }`) does not, so keying on `data:` excludes patterns.
# `[^}]*` cannot cross the closing brace, and these literals never nest braces.
RESP3_OPEN = re.compile(r"\bResp3BytesFrame::SimpleError\s*\{[^}]*\bdata\s*:")
RESP3_GOOD = re.compile(
    r"\bResp3BytesFrame::SimpleError\s*\{[^}]*\bdata\s*:\s*sanitize_error_message\b"
)


def line_of(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def unsanitized(text: str, open_re: re.Pattern[str], good_re: re.Pattern[str]) -> list[int]:
    """Line numbers of constructions matched by `open_re` but not `good_re`."""
    good_starts = {m.start() for m in good_re.finditer(text)}
    return [
        line_of(text, m.start()) for m in open_re.finditer(text) if m.start() not in good_starts
    ]


def main() -> int:
    if not RESPONSE.is_file():
        print(f"ERROR: {RESPONSE} not found — did the encoder boundary move?", file=sys.stderr)
        return 1

    lines = RESPONSE.read_text().splitlines()
    spans = cfg_test_spans(lines)
    # Blank out `#[cfg(test)]` spans so test constructions (if any) are ignored,
    # while keeping line numbers stable for reporting.
    scanned = "\n".join("" if in_any_span(i, spans) else ln for i, ln in enumerate(lines))
    rel = str(RESPONSE.relative_to(ROOT))

    bad = [("Resp2BytesFrame::Error", ln) for ln in unsanitized(scanned, RESP2_OPEN, RESP2_GOOD)]
    bad += [
        ("Resp3BytesFrame::SimpleError", ln) for ln in unsanitized(scanned, RESP3_OPEN, RESP3_GOOD)
    ]

    if bad:
        print("ERROR: CRLF-framed error frame built without the sanitizer:", file=sys.stderr)
        for kind, ln in sorted(bad, key=lambda x: x[1]):
            print(
                f"  {rel}:{ln}: `{kind}` payload does not come from sanitize_error_message",
                file=sys.stderr,
            )
        print(file=sys.stderr)
        print(
            "       An unsanitized CR/LF in a RESP2 simple error or RESP3 simple error",
            file=sys.stderr,
        )
        print(
            "       lets a client's error text inject a second wire frame. Build it as",
            file=sys.stderr,
        )
        print("         Resp2BytesFrame::Error(sanitize_error_message(e))", file=sys.stderr)
        print(
            "         Resp3BytesFrame::SimpleError { data: sanitize_error_message(e), .. }",
            file=sys.stderr,
        )
        print(
            "       (RESP3 BlobError is length-framed and deliberately not sanitized.)",
            file=sys.stderr,
        )
        return 1

    print("OK: RESP2/RESP3 simple-error frames are built through sanitize_error_message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
