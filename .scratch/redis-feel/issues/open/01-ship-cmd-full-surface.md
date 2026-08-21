# Shipped artifacts build `core-profile` only — 371-command matrix, unknown-command errors on every binary

Status: ready-for-agent
Type: bug (release / compat surface)
Area: build / release

## Problem

Every shipped FrogDB binary is built with the default `core-profile` command-family feature set.
`XADD`, `PFADD`, and every other command in the `stream`/`json`/`geo`/`full` family return `ERR
unknown command` on a binary a user actually downloads, while the published compat matrix
advertises 371 commands. The matrix describes a build nobody ships.

## Ruling (ADR-0005, ruling 1)

`--features cmd-full` in every ship path. `core-profile` remains the *development* default
(keeps iteration builds and the build cache small) — it is a build-speed tier, not a product
tier.

## Where to change it

- `frogdb-server/docker/Dockerfile.builder:124` — `cargo build --profile docker` needs
  `--features cmd-full` added.
- `Justfile` `cross-build` (~:1176-1178) and `cross-build-arm` (~:1180-1182) recipes.
- `.github/workflows/release.yml` docker job (:57-65) and `build-macos` job (:92-119) — **this
  file is generated** from `.github/workflows/workflow_gen/`; edit the Python generator, not the
  YAML (`CLAUDE.md` code-generation rule).
- deb and Homebrew release jobs consume the already-built binaries from the above jobs, so they
  need no separate change once those binaries carry `cmd-full`.

## Also add

A `run-full` Justfile recipe — like the existing `run` recipe but with `--features cmd-full` —
so local acceptance/regression runs can exercise the full command surface without a release
build. This is what issue 07's and issue 08's acceptance tests, and any local rerun of the
2026-08-15 feel-test script against the full surface, should use.

## Accepted cost

Shipped binary size and build time increase with `cmd-full`. Ruled acceptable in ADR-0005.

## Acceptance criteria

- [ ] Docker image built via `Dockerfile.builder` runs `XADD`/`PFADD`/other `full`-family
      commands successfully
- [ ] `just cross-build` / `just cross-build-arm` output binaries built with `cmd-full`
- [ ] The generated `release.yml`'s docker and `build-macos` jobs build with `cmd-full` (verify
      generator output, not a hand-edit of the YAML)
- [ ] New `just run-full` recipe exists and starts a server with the full command surface
- [ ] `just docs-gen --check` (compat-matrix drift check, per ADR-0005 consequences) stays green
