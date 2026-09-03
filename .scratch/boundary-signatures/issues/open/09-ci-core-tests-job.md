# CI: add the `core-tests` job to the generated Test workflow

Status: ready-for-agent

Size: S

## Why

The compact suite's value in CI is *reporting first*: a boundary break should be visible in a
couple of minutes instead of after the full `cargo nextest run --all`. This issue adds one job
that runs `just test-core`, with no dependency on `unit-tests`.

See [PRD §8](../../PRD.md#8-ci) and decision Q8 in
[PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02).

## What to build

**Edit the generator, never the generated yml.** The workflow file is generated; the source is
`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`.

Add a `core-tests` job modelled on the existing `unit-tests` job in that file:

- `name="Core Tests"`, `runs_on=RUNS_ON`
- `needs="changes"` and `if_="needs.changes.outputs.rust == 'true'"` — triggered on any rust
  change, using the existing `changes` job's `rust` filter output
- **no dependency on `unit-tests`**, so it reports first
- steps mirroring `unit-tests`: `checkout_step()`, `mise_setup_step(...)` (the variant that
  installs `just` and `nextest`), `rust_toolchain_step()`, `libclang_step()`,
  `cargo_cache_step(shared_key="stable")`, then `run_step(name="Run core tests", run="just
  test-core")`
- a comment above the job explaining what it is: the compact boundary suite (boundary crates +
  `sig_*` binaries), early-reporting, **not** a replacement for `unit-tests`

`unit-tests` (full `cargo nextest run --all`) is unchanged and stays the required merge gate.

**Deliberately out of scope**: change-based skipping in CI. `just test-affected` (issue 08 in
this directory) is local and pre-push only — a map that misses a dependency would merge a break,
and CI wall-clock is dominated by build, not test ([PRD §8](../../PRD.md#8-ci)). Do not wire
`affected.py` into any workflow.

Also note (no work required): the `SIG-` spec lint rides in the existing `lint` job via
`just lint-spec`, and census drift rides in the existing `docs-gen-check` job. Do not add
separate jobs for either.

**Regenerate** with `just workflow-gen` (the recipe is
`uv run --project .github/workflows/workflow_gen python -m workflow_gen`) and commit the
regenerated `.github/workflows/*.yml` alongside the generator change.

## Acceptance criteria

- [ ] `core-tests` job added in
      `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`, running
      `just test-core`, gated on `needs.changes.outputs.rust == 'true'`, with no `needs` on
      `unit-tests`
- [ ] `unit-tests` unchanged and still the required gate
- [ ] Generated workflow yml regenerated via `just workflow-gen` and committed
- [ ] `just workflow-gen --check` (i.e. the `workflow-gen-check` path in
      `just generate-check`) passes
- [ ] No CI job invokes `scripts/affected.py` / `just test-affected`
- [ ] `just lint-py` green

## Blocked by

Issue 02 in this directory (the `test-core` recipe the job runs). May run alongside issues 04–07.
