# 08 — CI: nothing exercises `just cross-build`; the zig path can re-break silently

Status: ready-for-agent
Type: AFK
Size: M
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
- `cross-verify` only prints `file` output for the x86-64 artifact; it never fails.

## What to build

Three pieces, all through existing recipes — no new raw `cargo zigbuild` line anywhere
(`scripts/ship-cmd-full.py` pins the count of ship-shaped lines per file; `Justfile` stays at 3).

### 1. `.mise.toml`: pin zig

Replace `zig      = "latest"` with `zig      = "0.15.2"` (the version `just cross-build` is green
on locally and the clang whose `-mevex512` handling D3 relies on). Comment on the line, in the
file's existing style, naming build-toolchain issue 08 and the `-mevex512` sensitivity.
`cargo:cargo-zigbuild` stays at `0.22.1`.

### 2. `Justfile`: `cross-verify` asserts, and gets an aarch64 sibling

- `cross-verify` runs `file target/x86_64-unknown-linux-gnu/release/frogdb-server`, prints it,
  and fails unless the output contains `ELF 64-bit` and `x86-64`.
- New `cross-verify-arm`, same shape, on
  `target/aarch64-unknown-linux-gnu/release/frogdb-server`, requiring `ELF 64-bit` and
  `aarch64`.
- Keep recipe comments in the surrounding style (one-line `#` above each). `docker-cross-build`
  / `docker-build-bench` dependencies unchanged.

### 3. `test.yml` (generated): a `cross-build` job

In `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`, add a job after
`cmd-full-build`:

- id `cross-build`, `name="Cross Build (zig)"`, `runs_on=RUNS_ON`, `needs="changes"`,
  `if_="needs.changes.outputs.rust == 'true' || needs.changes.outputs.workflow_gen == 'true'"`
  (`rust` covers `frogdb-server/**`, `Cargo.lock`, `.mise.toml`; `workflow_gen` covers
  `Justfile` — the recipe lives there). No new filter output.
- Steps: `checkout_step()`, `mise_setup_step(install_args=MISE_JUST_ZIGBUILD)` with a new
  constant `MISE_JUST_ZIGBUILD = "just zig cargo:cargo-zigbuild"` beside the other `MISE_*`
  constants, `rust_toolchain_step(targets="x86_64-unknown-linux-gnu,aarch64-unknown-linux-gnu")`,
  `libclang_step()` (bindgen for librocksdb-sys runs on the host),
  `cargo_cache_step(shared_key="cross")`, then two `run_step`s:
  `Cross-build x86_64` → `just cross-build && just cross-verify`, and
  `Cross-build aarch64` → `just cross-build-arm && just cross-verify-arm`.
- A comment above the job (module style, like the `cmd-full-build` comment) saying why it
  exists: nothing else in CI runs the zig path; `release.yml` uses `Dockerfile.builder`;
  build-toolchain issues 01 and 08; D9.
- Regenerate with `just workflow-gen`; `just workflow-gen --check` green.

Expect the job to take a while cold (RocksDB from source under zig, twice); the cargo cache
covers subsequent runs. Do not add a sticky disk or sccache.

## Acceptance criteria

- [ ] `.mise.toml` pins `zig = "0.15.2"` with a comment naming issue 08 and `-mevex512`
- [ ] `just cross-verify` fails on a missing or wrong-arch artifact and passes on the x86-64
      one; `just cross-verify-arm` likewise for aarch64 (show both directions in the report —
      e.g. point one at the other's artifact)
- [ ] `just cross-build && just cross-verify` and `just cross-build-arm && just cross-verify-arm`
      green locally (macOS, zig 0.15.2)
- [ ] `test.py` gains the `cross-build` job exactly as specified; `test.yml` regenerated;
      `just workflow-gen --check` green
- [ ] `just lint-gates` green — `ship-cmd-full` count unchanged (`Justfile: 3`, no new
      ship-shaped line in `test.py`)
- [ ] Controller verification after landing: a `workflow_dispatch` run of `test.yml` on
      `build-toolchain/impl` shows `Cross Build (zig)` green

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`
- `.github/workflows/test.yml` (regenerated)
- `.mise.toml`
- `Justfile` (`cross-verify`, new `cross-verify-arm`)

## Affects

`just cross-build`, `cross-build-arm`, `docker-cross-build`, `docker-build-bench`, Jepsen
`--build-mode cross`. `release.yml` is unaffected (Docker builder path).

## Blocked by

None (01 landed).

## Decisions

D3 (the recipe flags this job protects), D9.
