# 08 — CI: nothing exercises `just cross-build`; the zig path can re-break silently

Status: needs-triage
Type: AFK
Size: S
Origin: Whole-branch review of `build-toolchain/impl` (76b2a6dae..b6cd9276a, 2026-09-04); the PRD's "Detection gap" follow-up, owed since issue 01 closed

## Parent

`.scratch/build-toolchain/PRD.md`

## Summary

Issue 01 fixed `just cross-build` / `cross-build-arm` (D3: target-scoped `CXXFLAGS_<triple>`
plus `AR="zig ar"`), but the failure class it belonged to is still undetected:

- No workflow runs either recipe. `release.yml` builds the shipped Linux binaries inside
  `Dockerfile.builder` (native clang in the container), so the zig path is exercised only by
  hand — `docker-cross-build`, `docker-build-bench`, and Jepsen `--build-mode cross`
  (`testing/jepsen/run.py`) all depend on it, and 01 was found by a human running it.
- `.mise.toml` pins `zig = "latest"` while `-mevex512` is tied to the clang bundled with zig
  0.15.2 (older clangs reject the flag; a newer zig may change what it strips). The next
  `mise install` on a fresh machine can move zig and break the recipe with no signal until
  the next manual run.
- `cross-verify` checks only the x86-64 artifact (01's Resolution).

Both 01's Resolution and the PRD point here for the detector.

## Options (decision needed)

1. **Recommended:** a `cross-build` job in `test.yml`, `if:` gated on the same
   path-filter the `docker` job uses (Justfile, `Cargo.lock`, `frogdb-server/**`,
   `.mise.toml`), running `just cross-build` + `just cross-verify` on `ubuntu-latest` (zig and
   `cargo-zigbuild` come from `.mise.toml` via the mise step), plus `just cross-build-arm`
   with an aarch64 `cross-verify`. Cache `target/` on a sticky disk like `unit-tests`. Adds
   one ~4–6 min job per qualifying push.
2. Same job, nightly only (`build.yml` or a new `cross-build-nightly.yml`): no per-push
   cost, a day of latency on the detector.
3. Pin `zig` in `.mise.toml` (`"0.15.2"`) without a job: removes the drift vector, leaves
   the usearch/jemalloc class undetected.

Option 3 pairs with 1 or 2 — pinning is cheap and independent of the job.

## Acceptance criteria (for option 1 + pin)

- [ ] `.mise.toml` pins `zig` to the version the recipe is known green on, with a comment
      naming this issue and the `-mevex512` sensitivity
- [ ] a generated `cross-build` job (generator under `.github/workflows/workflow_gen/`,
      `just workflow-gen-check` green) runs `just cross-build && just cross-verify` and
      `just cross-build-arm` with an aarch64 ELF check
- [ ] `cross-verify` (or a sibling recipe) checks the aarch64 artifact too
- [ ] a `workflow_dispatch` run of `test.yml` on the integration branch shows the job green
- [ ] `just lint-gates` ship count unchanged (the job builds `cmd-full` through the existing
      recipes; no new raw `zigbuild` invocation)

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py` (or a new nightly generator)
- `.github/workflows/test.yml` (regenerated)
- `.mise.toml`
- `Justfile` (`cross-verify`)

## Affects

`just cross-build`, `cross-build-arm`, `docker-cross-build`, `docker-build-bench`, Jepsen
`--build-mode cross`. `release.yml` is unaffected (Docker builder path).

## Blocked by

None (01 landed).

## Decisions

Pending: job placement (per-push vs nightly), zig pin.
