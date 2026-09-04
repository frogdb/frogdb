# 02 — `main` CI red on seven jobs: mise regression, stale generators, stale doc paths

Status: ready-for-agent

## Parent

`.scratch/build-toolchain/PRD.md` (D1)

## Summary

`main` has failed the `Test` workflow on every run since `cdbd9c3ee` (2026-09-02 22:08Z; last
green `b3981f777`, 02:03Z the same day). Latest run `33704981830` on `76b2a6dae` fails seven
jobs — **Lint**, **Compat Generation Check**, **Command Matrix Generation Check**, **Spec Docs
Generation Check**, **Dashboard Generation Check**, **Docs Path Check**, and **Unit Tests** —
which gates `CI Pass` red for every PR. Three of the four root causes are toolchain/generator
hygiene and are fixed here; the fourth (Unit Tests: real cluster-test regressions) is tracked
separately and is **out of scope** for this issue.

## What to build

Make every non-`Unit Tests` job of the `Test` workflow green on `main`, by fixing the three
causes at their source (generators, not generated output).

### (a) Lint — mise 2026.9.x cargo-backend regression

The Lint job's mise step fails before any code runs:

```
mise ERROR Failed to install cargo:cargo-deny@0.19.0: tool 'cargo:cargo-deny@0.19.0' requires configured install dependency 'rust@1.92.0', but its selected version is not installed
mise ERROR Version: 2026.9.1 linux-x64 (2026-09-02)
```

mise `v2026.9.0` (2026-09-01) / `v2026.9.1` (2026-09-02) made the `cargo:` backend require a
mise-managed `rust` install. This repo deliberately installs Rust with `dtolnay/rust-toolchain`
*after* the mise step, not via mise (see the `RUST_TOOLCHAIN` comment in
`.github/workflows/workflow_gen/src/workflow_gen/helpers.py` — mise's rust plugin fights
`Swatinem/rust-cache` on self-hosted runners). Jobs with a warm mise cache (Unit Tests) still
pass because nothing gets installed; Lint's cache missed.

Fix: pin the mise version in `mise_setup_step()` (`helpers.py`) via `jdx/mise-action`'s
`version` input to **`2026.8.16`** (last 2026.8.x release, 2026-08-31), with a one-line comment
naming the regression, then `just workflow-gen` to regenerate every workflow that uses the
step. Do **not** add `rust` to any job's `install_args` — that reintroduces the rust-cache
conflict the comment describes.

### (b) Stale generated artifacts

All three reproduce locally on a clean tree:

1. `just spec-gen-check` →
   `spec-gen: no AREAS entry for memory — give the new area a sidebar order and a one-line scope blurb in spec-gen.py`.
   `specs/memory.md` landed in `83b4ef51d` without a `website/scripts/spec-gen.py` `AREAS`
   entry. Add `"memory"` with sidebar order **7** (after `blocking` = 6), title **`Memory`**,
   and a one-line scope blurb derived from the spec's own scope statement (top of
   `specs/memory.md`). Regenerate; the new
   `website/src/content/docs/specifications/memory.md` is committed like the other pages.
2. `just compat-gen-check` → `Differs: website/src/data/compat-exclusions.json`. Run
   `just compat-gen`, commit the output.
3. Dashboard Generation Check → `Error: Dashboard differs from generated`. Run
   `just generate` (dashboard → helm → deb → workflow, in that order — the Helm chart bundles a
   copy of `frogdb-overview.json`, so dashboard must precede helm), commit the output.

### (c) Docs Path Check — 7 stale paths

`just docs-path-check` reports:

```
website/src/content/docs/architecture/architecture.md:150: frogdb-server/crates/frogdb-macros
website/src/content/docs/getting-started/installation.mdx:104: frogdb-server/cmd-full
website/src/content/docs/getting-started/installation.mdx:111: frogdb-server/cmd-full
website/src/content/docs/specifications/blocking.md:320: frogdb-server/tests/cluster_pause_barrier.rs
website/src/content/docs/specifications/blocking.md:329: frogdb-server/src/server/shards.rs
website/src/content/docs/specifications/persistence.md:126: frogdb-server/src/commands/stub.rs
website/src/content/docs/specifications/persistence.md:477: frogdb-server/src/server/init.rs
```

- **Spec pages are generated** from `specs/*.md` (`just spec-gen`). Edit the sources, not the
  website copies: `specs/blocking.md:314` and `:323`, `specs/persistence.md:123` and `:474`.
  The docs' crate-shorthand convention (`frogdb-core/src/…`) collides with the real
  `frogdb-server/` workspace directory for the `frogdb-server` crate, so write the real path
  for these four: `frogdb-server/crates/server/tests/cluster_pause_barrier.rs`,
  `frogdb-server/crates/server/src/server/shards.rs`,
  `frogdb-server/crates/server/src/commands/stub.rs`,
  `frogdb-server/crates/server/src/server/init.rs`. Regenerate.
- `architecture.md:150` lists `frogdb-macros/` in the workspace tree; the crate was deleted in
  `554b03129`. Remove the line.
- `installation.mdx:104,111` is a checker false positive: the token is the cargo feature spec
  `--features frogdb-server/cmd-full`, not a path. Add `frogdb-server/cmd-full` to
  `ALLOWLIST` in `website/scripts/docs-path-check.py` with a comment saying why (feature
  spec, `<crate>/<feature>` shape).

## Acceptance criteria

- [ ] `just workflow-gen --check` passes and every generated `.github/workflows/*.yml` mise step carries `version: 2026.8.16`
- [ ] `just matrix-gen-check` passes (docs-gen, compat-gen, spec-gen)
- [ ] `just generate-check` passes (helm, dashboard, deb, workflow)
- [ ] `just docs-path-check` passes with zero violations
- [ ] `website/src/content/docs/specifications/memory.md` exists and is generated, not hand-written
- [ ] `just scratch-check` and `just lint-gates` pass
- [ ] No edits to any `.rs` file, and no edits under `frogdb-server/crates/server/tests/` — the Unit Tests failures are a separate issue

## Files likely touched

- .github/workflows/workflow_gen/src/workflow_gen/helpers.py
- .github/workflows/*.yml (regenerated)
- website/scripts/spec-gen.py
- website/scripts/docs-path-check.py
- website/src/content/docs/specifications/ (regenerated: memory.md new, blocking.md, persistence.md)
- website/src/content/docs/architecture/architecture.md
- website/src/data/compat-exclusions.json (regenerated)
- specs/blocking.md
- specs/persistence.md
- frogdb-server/ops/grafana/frogdb-overview.json (regenerated)
- frogdb-server/ops/ (helm chart dashboard copy, deb — whatever `just generate` rewrites)

## Blocked by

None

## Decisions

D1

## Out of scope

Unit Tests: `frogdb-server::cluster_handoff_barrier` (3–4 tests),
`cluster_migration::{test_e2e_migration_batched_keys,empty_slot,bloom_filter}`,
`cluster_finalization_window::no_write_is_acknowledged_after_the_slot_is_handed_over_under_load`
fail or time out on `main` since the output-buffer series `b012272d3..e090f0335`. Product
regression in a LOCKED area; separate issue.
