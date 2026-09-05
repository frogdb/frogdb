# 04 — CI: mise `npm:` backend refuses to install quint because `node@22` is not installed first

Status: done
Type: AFK
Size: XS
Origin: Test run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779), first full run after 03

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

Same mise ≥ 2026.8.11 install-dependency check that 03 fixed for the `cargo:` backend
(jdx/mise#12234), now surfacing on the `npm:` backend. The `Quint Typecheck & Smoke` job's
`Set up mise toolchain` step fails:

```
mise ERROR Failed to install npm:@informalsystems/quint@0.32.0: tool 'npm:@informalsystems/quint@0.32.0' requires configured install dependency 'node@22', but its selected version is not installed
mise ERROR Version: 2026.9.1 linux-x64 (2026-09-02)
```

`.mise.toml` pins `node = "22"` and `"npm:@informalsystems/quint" = "0.32.0"`, but the
mise-action `install_args` lists only `just npm:@informalsystems/quint`, so `node` is never
installed on the runner. 03's `MISE_DISABLE_TOOLS=rust` does not apply: `rust` is not the missing
dependency here, and node genuinely has to exist for an npm install.

This also breaks every `quint_conformance` test on the runner (`frogdb-replication`), which
shells out to `quint run` — on the Sept 3 `main` run those failed with a node error.

## What to build

In `.github/workflows/workflow_gen/`, add `node` to every `install_args` constant that names the
npm-backed quint tool, so mise installs the dependency in the same invocation:

- `src/workflow_gen/workflows/test.py:38` `MISE_JUST_QUINT` → `just node npm:@informalsystems/quint`
- `src/workflow_gen/workflows/test.py:42` `MISE_JUST_NEXTEST_QUINT` → `just node cargo:cargo-nextest npm:@informalsystems/quint`
- `src/workflow_gen/workflows/quint_verify.py:95` `MISE_JUST_QUINT` → `just node npm:@informalsystems/quint`
- `src/workflow_gen/workflows/cluster_quint_quarantine_nightly.py:41` `MISE_JUST_NEXTEST_QUINT` → `just node cargo:cargo-nextest npm:@informalsystems/quint`

Put the "why" in one comment beside the constants (mise ≥ 2026.8.11 checks install
dependencies; `npm:` tools depend on `node`; see issue 04). If the four constants can be one
shared definition in `helpers.py` without contortion, do that; otherwise leave them where they
are.

Regenerate the workflows with `just workflow-gen` and commit the rendered files together with the
generator change. The step-level `MISE_DISABLE_TOOLS: rust` env from 03 stays exactly as is.

## Acceptance criteria

- [ ] rendered `install_args` for the Quint job in `.github/workflows/test.yml` reads `just node npm:@informalsystems/quint`
- [ ] rendered `install_args` for every job that used `MISE_JUST_NEXTEST_QUINT` reads `just node cargo:cargo-nextest npm:@informalsystems/quint`
- [ ] `quint-verify-nightly.yml` and `cluster-quint-quarantine-nightly.yml` regenerated the same way
- [ ] `just workflow-gen --check` and `just generate-check` green
- [ ] `just lint-gates` green
- [ ] no change to `.mise.toml`, no version pins added, 03's `env:` block untouched

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`
- `.github/workflows/workflow_gen/src/workflow_gen/workflows/quint_verify.py`
- `.github/workflows/workflow_gen/src/workflow_gen/workflows/cluster_quint_quarantine_nightly.py`
- `.github/workflows/workflow_gen/src/workflow_gen/helpers.py` (only if the constant is shared)
- `.github/workflows/test.yml`
- `.github/workflows/quint-verify-nightly.yml`
- `.github/workflows/cluster-quint-quarantine-nightly.yml`

## Blocked by

None (03 is landed).

## Decisions

D4

## Resolution

Landed on `build-toolchain/impl` at merge `2b1db3e03` (2026-09-04). One commit, `43ca011c2`:
`node` prepended to `MISE_JUST_QUINT` / `MISE_JUST_NEXTEST_QUINT` in `test.py`, `quint_verify.py`
and `cluster_quint_quarantine_nightly.py`, with a one-line why beside each constant; the three
workflows regenerated (`test.yml`, `quint-verify-nightly.yml`,
`cluster-quint-quarantine-nightly.yml`). Not consolidated into `helpers.py` — the sibling
`MISE_JUST_NEXTEST` is already duplicated verbatim across eight workflow files by convention.
`.mise.toml` untouched (`node = "22"` already pinned there, so the bare `node` resolves to it);
issue 03's `MISE_DISABLE_TOOLS: rust` env untouched. `just workflow-gen --check`,
`generate-check`, `lint-gates` green. Proof in CI: next `workflow_dispatch` of `test.yml` on
`build-toolchain/impl`.
