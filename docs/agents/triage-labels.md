# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the
actual label strings used in this repo's issue tracker, and adds the one terminal state the
canonical five do not cover.

## Legal `Status:` values

| Label in mattpocock/skills | Label in our tracker | Meaning                                  | Directory |
| -------------------------- | -------------------- | ---------------------------------------- | --------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  | `open/`   |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information | `open/`   |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  | `open/`   |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            | `open/`   |
| `wontfix`                  | `wontfix`            | Will not be actioned                     | `done/`   |
| *(none)*                   | `done`               | Implemented and closed                   | `done/`   |

This list is exhaustive. `just scratch-check` rejects anything else.

## Why `done` is a local addition

The five canonical roles are all *pre-work* states — every one of them answers "what happens
next", and none of them answers "is this still live". With no terminal value in the vocabulary,
83 of 110 issues invented one anyway, and five of those then drifted into free text on the
`Status:` line (`done (landed 2026-07-22, branch workspace-3)`, `done (built end-to-end,
2026-07-28, branch workspace-3)`, …), which is exactly what makes a field unparseable. `done` is
now a first-class value so the field stays machine-readable.

Keep supporting detail **out** of the `Status:` line — `Status: done` and nothing else. Landing
metadata goes on an optional sibling `Landed:` line, and the substance goes in a `## Resolution`
section:

```markdown
Status: done
Landed: 2026-07-22, branch workspace-3
```

## Applying a label

Issues are local markdown files, so a label is a string on the `Status:` line at the top of the
file (see [`issue-tracker.md`](issue-tracker.md)):

```markdown
# Fuzzing is manual-only with no persisted corpus

Status: ready-for-agent
```

A status change to or from a terminal value is **two edits** — rewrite the `Status:` line and
`git mv` the file between `issues/open/` and `issues/done/`. The two must agree; the check
enforces it.

Edit the right-hand column to match whatever vocabulary you actually use — but if you add or
rename a value, update `LEGAL` in `scripts/scratch-check.py` in the same commit.
