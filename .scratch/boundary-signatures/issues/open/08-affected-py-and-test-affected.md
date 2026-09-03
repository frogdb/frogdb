# `scripts/affected.py` + `just test-affected`: change-based selection for the local loop

Status: ready-for-agent

Size: M

## Why

`just test-core` is one filter over the whole compact suite. Editing one boundary crate does not
need the other three areas' tests. This issue adds a change-based selector that maps the diff to
a run plan, for local iteration and pre-push only — it is deliberately **not** wired into CI
(see issue 09 in this directory and [PRD §8](../../PRD.md#8-ci)).

See [PRD §7](../../PRD.md#7-change-based-selection-just-test-affected).

## What to build

**1. `scripts/affected.py`.** A `uv run --script` script in the same shape as
`scripts/loop-cost.py` — the `#!/usr/bin/env -S uv run --script` shebang with the inline
`# /// script` / `requires-python` block, executable, a module docstring explaining the map and
the safety valve.

It diffs against `git merge-base HEAD main` by default (`--base <ref>` overrides), maps each
changed path through an **ordered glob table, first match wins**, and prints/executes the
resulting plan. The table, verbatim from
[PRD §7](../../PRD.md#7-change-based-selection-just-test-affected):

| path pattern | runs |
|---|---|
| `frogdb-server/crates/{txn,vll}/**` | `test-core txn` |
| `frogdb-server/crates/{persistence,recovery}/**` | `test-core persistence` |
| `frogdb-server/crates/{replication,replication-runtime}/**` | `test-core replication` |
| `frogdb-server/crates/{cluster,cluster-runtime}/**` | `test-core cluster` |
| `frogdb-server/crates/core/src/shard/persistence.rs`, WAL seam files | `test-core persistence` |
| `frogdb-server/crates/core/src/{command.rs,command_spec.rs,signature.rs,shard/post_execution.rs,...}` | `test-core` (all areas) |
| `frogdb-server/crates/commands/src/<family>/**` | `test-core` + `integration_<family>` |
| `frogdb-server/crates/server/tests/sig_<area>.rs` | `test-core <area>` |
| anything else | full `just test` |

Rules that make the map safe:

- **Unknown → full `just test`** is the safety valve. A path nobody thought about must never
  narrow the run.
- A changed path matching several rows takes the **first** row; a diff touching several rows
  runs the **union** of their plans (and any row resolving to full `just test` subsumes the rest).
- The map lives **in Python, not in a data file**, so it is unit-testable with fixtures the way
  `scripts/spec-lint.py` is (decision Q7 in
  [PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)).
- The map is expected to be coarse while `frogdb-core` remains monolithic; issue 10 in this
  directory (the crate split, deferred) sharpens it from module-glob to crate-graph granularity.
  Say so in the docstring so the coarseness reads as deliberate.

**Flags:** `--dry-run` prints the plan (the matched rows and the exact commands) and runs
nothing; `--base <ref>` replaces `main` as the merge-base target. Exit non-zero if any dispatched
command fails.

**2. Fixture unit tests** at `scripts/tests/test_affected.py`, following
`scripts/tests/test_spec_lint.py`: feed the mapper synthetic changed-path lists (no git, no
cargo) and pin the plan for each row of the table, plus first-match-wins ordering, the union of
two rows, and the unknown-path → full-suite valve. These must run in under a second and not shell
out to cargo.

**3. `just test-affected *args`** in the `Justfile`, next to `test-core`, forwarding args to
`./scripts/affected.py` (so `just test-affected --dry-run` and `just test-affected --base
origin/main` work). Wire the fixture suite into a recipe the way `just test-spec-lint` is wired,
and document in the recipe comment that `test-affected` is local/pre-push only and is not a CI
job.

**4. Docs.** Mention the new recipe wherever `test-core` is documented (the `Justfile` comment
block for the locked core areas, and any `agents/` or `.scratch/hardening/` doc that lists the
inner-loop recipes).

## Acceptance criteria

- [ ] `scripts/affected.py` exists, is executable, uses the `uv run --script` shebang and inline
      script metadata like `scripts/loop-cost.py`
- [ ] The ordered glob table matches PRD §7 row for row, first match wins, unknown → full
      `just test`
- [ ] `--dry-run` prints the plan without running anything; `--base <ref>` overrides the
      merge-base target (default `git merge-base HEAD main`)
- [ ] `scripts/tests/test_affected.py` pins every table row, ordering, union and the unknown
      valve; runs in under a second with no cargo invocation
- [ ] `just test-affected *args` forwards to the script; a recipe runs the fixture suite
- [ ] Docstring states that `test-affected` is local/pre-push only and that the map is coarse
      until the crate split (issue 10 in this directory)
- [ ] `just lint-py` and `just fmt-py-check` green

## Blocked by

Issue 02 in this directory (the `test-core` recipe it dispatches). May run alongside issues
04–07.
