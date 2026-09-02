# The lock is an honor system — run `mutants-diff` in CI on every locked-crate change

Status: done
Type: mechanism (enforcement gap)
Severity: likelihood 3/3 (nothing runs it: 175 commits touched locked crates since lock, 39
mention mutants), consequence 3/3 (new code in a locked crate lands with no forcing test and the
area's score silently falls below its gate — the exact claim LOCKED makes) — score 9
Area: campaign mechanism / CI
Blocked by: 13, 17

## Problem

`just mutants-diff <crate>` is the post-lock ratchet: for a change inside the perimeter, every
new mutant must be caught. It is what turns "spec-first" from a protocol into a property. Today it
runs only when an agent chooses to, before pushing, per a sentence in `CLAUDE.md`. CI has no
mutation job at all.

`lint-spec` (issue 14) cannot fill this hole: it checks rows already written, not that new code
has a witness. Only a mutation run over the diff catches "added a branch, added no test."

## Design (ruled 2026-09-02)

**A `mutants-diff` job in `test.yml`** (`workflow_gen/workflows/test.py`), triggered on
`pull_request` and on `push` to `main`:

- **Pass criterion: zero missed mutants.** Every viable mutant in the diff is caught, or excluded
  in `.cargo/mutants.toml` with a documented-equivalent comment at the code (the existing
  convention). cargo-mutants already exits non-zero on a missed mutant; the job relies on that
  and on `just mutants-diff` (issue 13) propagating it, so the local and CI verdicts read
  identically. Timeouts are reported, not fatal; the >5% timeout-share warning goes to the job
  summary. The area threshold (0.80–0.90) is **not** applied to a diff — on a denominator of 2–20
  it is arbitrary and lenient exactly when the diff is large.
- **Where it bites.** Branch protection on `main` is for contributors; the admin merges
  `worktree-*` branches locally and pushes with bypass, by design. So the PR run **blocks** when
  a PR exists, and the push-to-main run **detects** (turns main red after the fact). Both are the
  same job with a different base. Requiring status checks / forcing PR flow for locked-crate
  changes was considered and dropped.
- **Base for the diff.** PR: `git merge-base origin/main HEAD` (`fetch-depth: 0`). Push:
  `github.event.before`; skip when it is all-zeros (branch creation) or unreachable (force push).
- **Scope.** One matrix leg per *touched* locked crate, from `changes` outputs. The per-crate
  path filters are **generated from the manifest** (issue 13's parser, imported by
  `workflow_gen`) so a crate entering or leaving the perimeter changes the filter on the next
  `just workflow-gen`; `just workflow-gen --check` catches drift. The diff fed to cargo-mutants
  is `git diff <base> -- <crate path>` so an unrelated hunk in another crate pulls no mutants in.
- **Runner / limits.** `ubuntu-latest`, `rust-cache` shared-key `stable`, `timeout-minutes: 90`,
  `cargo mutants --jobs 2`. Promote to `blacksmith-8vcpu-ubuntu-2404-arm` (already used by the
  testbox unit-test workflow) only if measured wall time says so — arm matches the production
  target but adds a second cache lineage.
- **Concurrency group** per ref with cancel-in-progress, so a rapid re-push does not queue two
  mutation runs.
- **Job summary**: the mutants-gate-style line (`N total, caught, missed, unviable, timeout`) and
  the list of missed mutants with file:line.

**`CLAUDE.md`** (with issue 13): the "run `just mutants-diff` before pushing" rule becomes advice —
CI enforces it; run locally to iterate faster.

## Not in scope

- The full-area gate on a schedule (issue 16). A diff run cannot see a deleted or weakened
  test — tests are `exclude_globs` — so survivors it used to kill return silently; only a full
  run re-measures the area.
- A pre-push lefthook hook running `mutants-diff` — 5–30 minutes at push time inside the agent
  harness would recreate the `--no-verify` failure the clippy hook comment records.

## Forcing test

On a branch: add a trivial unforced branch to a locked crate (e.g. an `if` that flips a return on
an impossible input) and open a PR — the job goes red with that mutant listed. Add the killing
test — green. Also: a PR touching only `frogdb-commands` must not spawn the job at all.

## Resolution

Landed on `locked-areas-mechanical/impl` as merge `0c43dc6d` (commits `53aaf37d`, `b0478624`,
`7fffae69`, `2d266018`, fix round `45ae5a22`, `ddc1ae14`, `611c1fb6`) plus `5585b39b` (D4),
2026-09-02.

- `scripts/locked_areas.py`: `member_paths()` (name → repo-relative dir; `workspace_members`
  derives from it) and `--crate-path CRATE`, answered before `validate()` so a bad spec header
  cannot break the CI job's path lookup. Fixture tests pin both.
- `just mutants-diff crate base="" *args`: crate-scoped `git diff <base> -- <path>` into a
  per-crate patch; empty patch exits 0 without invoking cargo. Review found the ruled
  "exit 3 = timeouts, nothing missed" premise false — cargo-mutants 27.1.0 ranks timeout above
  missed — so on exit 3 the recipe re-reads `missed.txt` and exits 2 if it is non-empty.
- `test.yml`: one `locked-<crate>` paths-filter per locked crate plus a `mutants_matrix` JSON
  output, both generated from the manifest; a `mutants-diff` job (fromJSON matrix,
  fail-fast off, 90 min, `-e` base step that fails on an unresolvable base, summary with counts
  and the missed list, >5% timeout-share warning) gated on the matrix being non-empty;
  `ci-pass` needs it. Concurrency group is keyed by ref on PRs and by sha on pushes (D4).
- `CLAUDE.md`: the "before pushing" rule is now advice; CI enforces.

Live forcing test (locked-crate touch → red leg; `frogdb-commands`-only → no leg) runs on the
`[ci-verify]` PR per D3.
