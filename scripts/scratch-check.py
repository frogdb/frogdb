#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Lint the `.scratch/` issue tracker.

The tracker is plain markdown with no schema enforcement, so it drifts. Every rule
below exists because the drift it catches actually happened:

  1. `Status:` must be one of the legal values. The documented vocabulary had no
     terminal state, so 83 of 110 issues invented `done`, and five of those then
     drifted into free text on the line itself (`done (landed 2026-07-22, branch
     workspace-3)`, …) — unparseable by anything.
  2. `Status:` and the `open/`|`done/` subdirectory must agree. The subdirectory is
     the whole point — it makes state legible from `ls` — and it is worthless the
     moment the two can disagree.
  3. Every issue must live in `open/` or `done/`, not loose in `issues/`.
  4. No duplicate issue numbers within a feature. Two files both numbered `66` sat
     in the tracker undetected; references by number were ambiguous.
  5. Every feature directory needs a `README.md` with a `State:` line, so a reader
     can tell an active workspace from an archive without reading the issues.

See `docs/agents/issue-tracker.md` and `docs/agents/triage-labels.md`. If you add or
rename a `Status:` value, update `LEGAL` here in the same commit.

Exit 0 clean, 1 on any violation.
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRATCH = ROOT / ".scratch"

STATUS = re.compile(r"^Status:\s*(.+?)\s*$", re.M)
STATE = re.compile(r"^State:\s*(.+?)\s*$", re.M)
# NN or NN-MM (sub-issues: arch-deepening 13-01/13-02/13-03 are distinct issues)
NUMBER = re.compile(r"^([0-9]+(?:-[0-9]+)*)-[a-z]")

LEGAL = {
    "needs-triage": "open",
    "needs-info": "open",
    "ready-for-agent": "open",
    "ready-for-human": "open",
    "done": "done",
    "wontfix": "done",
}
LEGAL_STATES = {"active", "closed", "archive-of-record"}


def main() -> int:
    errors: list[str] = []

    features = sorted(d for d in SCRATCH.iterdir() if d.is_dir() and not d.name.startswith("."))
    if not features:
        print(f"no feature directories under {SCRATCH}", file=sys.stderr)
        return 1

    for feat in features:
        rel = feat.relative_to(ROOT)

        # rule 5: README.md with a legal State: line
        readme = feat / "README.md"
        if not readme.is_file():
            errors.append(f"{rel}: missing README.md (needs a `State:` line)")
        else:
            m = STATE.search(readme.read_text(encoding="utf-8"))
            if not m:
                errors.append(f"{rel}/README.md: no `State:` line")
            elif m.group(1) not in LEGAL_STATES:
                errors.append(
                    f"{rel}/README.md: State: {m.group(1)!r} not one of {sorted(LEGAL_STATES)}"
                )

        issues = feat / "issues"
        if not issues.is_dir():
            continue

        # rule 3: nothing loose directly under issues/
        for loose in sorted(issues.glob("*.md")):
            errors.append(
                f"{loose.relative_to(ROOT)}: loose in issues/ — "
                f"move into issues/open/ or issues/done/"
            )

        numbers: dict[str, list[str]] = defaultdict(list)
        for state in ("open", "done"):
            for f in sorted((issues / state).glob("*.md")):
                frel = f.relative_to(ROOT)

                # rule 4: collect numbers for the duplicate scan
                nm = NUMBER.match(f.name)
                if nm:
                    numbers[nm.group(1)].append(f"{state}/{f.name}")
                else:
                    errors.append(f"{frel}: filename does not start with `<NN>-<slug>`")

                # rules 1 + 2
                sm = STATUS.search(f.read_text(encoding="utf-8"))
                if not sm:
                    errors.append(f"{frel}: no `Status:` line")
                    continue
                raw = sm.group(1)
                if raw not in LEGAL:
                    errors.append(
                        f"{frel}: Status: {raw!r} is not a legal value "
                        f"({', '.join(sorted(LEGAL))}) — put detail in a "
                        f"`## Resolution` section, not on the Status line"
                    )
                elif LEGAL[raw] != state:
                    errors.append(
                        f"{frel}: Status: {raw!r} belongs in "
                        f"issues/{LEGAL[raw]}/, not issues/{state}/"
                    )

        for num, files in sorted(numbers.items()):
            if len(files) > 1:
                errors.append(f"{rel}/issues: duplicate issue number {num} — {', '.join(files)}")

    if errors:
        print(f"ERROR: {len(errors)} .scratch tracker violation(s):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        print(file=sys.stderr)
        print("       See docs/agents/issue-tracker.md", file=sys.stderr)
        return 1

    total = sum(
        len(list((f / "issues" / s).glob("*.md")))
        for f in features
        if (f / "issues").is_dir()
        for s in ("open", "done")
    )
    print(f"OK: {len(features)} feature dirs, {total} issues, tracker consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
