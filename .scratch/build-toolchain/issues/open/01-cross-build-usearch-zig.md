# 01 — `just cross-build` is broken: usearch/simsimd rejects zig's C++ driver

Status: needs-triage

## Summary

`just cross-build` fails on a macOS host. The `usearch` crate's `build.rs` compiles
`simsimd/c/lib.c` — a **C** translation unit — through cargo-zigbuild's C++ driver wrapper
with `-std=c++17`, and zig's clang refuses the combination:

```
error: invalid argument '-std=c++17' not allowed with 'C'
```

`build.rs` treats the failure as a SIMD-target problem: it retries six times, peeling off one
`SIMSIMD_TARGET_*` define per pass (`SAPPHIRE`, `GENOA`, `ICE`, `SKYLAKE`, `HASWELL`, then
none). The offending flag is present in every pass, so all six fail identically and it panics:

```
thread 'main' panicked at usearch-2.24.0/build.rs:113:24:
called `Result::unwrap()` on an `Err` value: Error { kind: ToolExecError, ... }
error: failed to run custom build command for `usearch v2.24.0`
error: Recipe `cross-build` failed on line 1236 with exit code 101
```

## Why it matters

`cross-build` is the shipping path. Per ADR-0005 ruling 1, every distributable artifact builds
`cmd-full` — Docker image, cross-built binaries, macOS tarballs, deb, Homebrew — and the
`lint-ship-cmd-full` seam lint enforces that. `usearch` is reachable only under `cmd-full`; the
`core-profile` dev default never compiles it. So the whole class of `cmd-full` builds is
unreachable from a mac host via zig, while every ordinary dev command stays green. Nothing
local detects it.

It also blocks `just jepsen-suite <suite> --build`, whose default `--build-mode cross` shells
out to `just cross-build`.

## Workaround

`--build-mode docker` builds in-Docker via `Dockerfile.builder` with native Linux clang and
compiles `usearch` without complaint:

```
just jepsen-suite single --build-mode docker
```

This confirms the break is zig-specific, not a defect in usearch or in our source.

## Not a regression from recent work

`usearch` has been in-tree since 2026-03-15 (`761ce770e`, VECTOR field type) and 2026-03-18
(`cd0473598`, vector set commands). Either the mac-host zig path never worked for `cmd-full`,
or a `cc`-crate bump (currently 1.2.58) changed C-vs-C++ driver selection under it. Worth
establishing which before choosing a fix — `git bisect` on `just cross-build` would settle it.

## Options

1. Patch `usearch` upstream so the `.c` unit is compiled by the C driver, not the C++ one.
2. Vendor/fork the build script with that fix.
3. Pin a `cc` version known to work, if a bisect shows a bump caused it.
4. Make `docker` the default `--build-mode` for the Jepsen path and stop depending on zig for
   `cmd-full` artifacts.

Option 4 is the smallest change but leaves `just cross-build` broken for the deb/Homebrew/
tarball artifacts, which is the larger exposure.

## Reproduction

```
just cross-build
```

Observed 2026-09-02 on darwin 25.5.0 (Apple Silicon), cargo-zigbuild 0.22.1, target
`x86_64-unknown-linux-gnu`, usearch 2.24.0, cc 1.2.58.

## Detection gap

No CI job or lint catches this. `lint-ship-cmd-full` checks that shipping recipes *ask for*
`cmd-full`; it does not check that the resulting build succeeds. A smoke job that runs
`just cross-build` would have caught it at the commit that introduced it.
