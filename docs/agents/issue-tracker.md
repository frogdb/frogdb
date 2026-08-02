# Issue tracker: Local Markdown

Issues and PRDs for this repo live as markdown files in `.scratch/`. See
[`.scratch/README.md`](../../.scratch/README.md) for the current directory index and open/done
counts.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`
- Each feature directory has a `README.md` carrying a `State:` line — `active`, `closed`, or
  `archive-of-record`
- The PRD is `.scratch/<feature-slug>/PRD.md`
- Implementation issues are `.scratch/<feature-slug>/issues/<open|done>/<NN>-<slug>.md`,
  numbered from `01`
- Triage state is recorded as a `Status:` line near the top of each issue file (see
  [`triage-labels.md`](triage-labels.md) for the legal strings)
- Comments and conversation history append to the bottom of the file under a `## Comments`
  heading

## Status lives in two places, and they must agree

The `Status:` line says *what happens next*. The `open/`|`done/` subdirectory says *whether it
is still live*. The rule is mechanical:

| `Status:` | directory |
| --------- | --------- |
| `done`, `wontfix` | `issues/done/` |
| everything else | `issues/open/` |

The subdirectory exists so the state of a feature is legible from `ls` alone, without opening
150 files. `just scratch-check` enforces the agreement, the legal `Status:` vocabulary, the
presence of a `README.md` with a `State:` line, and the absence of duplicate issue numbers.

**Closing an issue is two edits**: set `Status: done` *and* `git mv` the file into `done/`.

### `## Resolution` is not a status

An issue can carry a `## Resolution` section describing work that shipped and still be open —
`concurrency-testing/issues/11` carried `## Resolution shipped in phase 5 (CI wiring)` for weeks
while its findings A/B/C stayed un-root-caused, and only closed once each finding was resolved or
split out. Only the `Status:` line is authoritative. Do not grep for `## Resolution` to decide
what is done.

An issue can also be *reopened* after closing — `testing-improvements/issues/open/40` shipped,
then regressed when a later commit deleted its cron. Reopening means flipping `Status:` back,
moving the file to `open/`, and appending a `## Reopened` section with the evidence; the
original `## Resolution` stays for the record.

## Citing an issue

**Cite by number and directory, never by filename.** Filenames move between `open/` and
`done/` and occasionally get renumbered, so a path-exact reference rots:

```
good:  see `.scratch/testing-improvements/issues/40`
good:  (issue 59, `.scratch/testing-improvements/issues/`, CONFIRMED L1/C1)
bad:   see `.scratch/testing-improvements/issues/40-fuzzing-continuous-corpus.md`
```

In markdown, link the directory and put the number in the link text:
`[issue 66](../../.scratch/testing-improvements/issues/)`.

Sub-issue numbers are real — `13-01`, `13-02` and `13-03` under `arch-deepening` are three
distinct issues, not one. Cite the full number.

## When a skill says "publish to the issue tracker"

Create the file under `.scratch/<feature-slug>/issues/open/` (creating the directory and the
feature `README.md` if needed). New issues start in `open/` by definition.

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or the issue number
directly; if given a bare number, look in `open/` first, then `done/`.
