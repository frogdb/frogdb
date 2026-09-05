# 01 — `just cross-build` is broken: usearch/simsimd rejects zig's C++ driver

Status: done

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

## Investigation 2026-09-04 (experiments on `cargo zigbuild -p usearch --target x86_64-unknown-linux-gnu`)

Two independent zig quirks, both absent under the Docker builder's native clang:

1. zig's `c++` driver wrapper rejects `-std=c++17` for the C unit `simsimd/c/lib.c`
   (`invalid argument '-std=c++17' not allowed with 'C'`). `CXXFLAGS="-x c++"` makes the
   driver treat the unit as C++ and the error goes away.
2. With that fixed, zig 0.15.2 (clang 20) passes an explicit `-evex512` target feature, and
   simsimd 6.5's AVX-512 `target` attributes (`SKYLAKE`/`ICE`/`GENOA`/`SAPPHIRE`) do not add
   it back, so every AVX-512 probe fails and `build.rs` peels targets until it panics.
   `-mevex512` restores the feature.

Results:

| usearch | flags | outcome |
|---|---|---|
| 2.24.0 | none | `-std=c++17` error, six retries, panic (the report above) |
| 2.24.0 | `CXXFLAGS="-x c++"` | past the driver error; `evex512` attribute errors |
| 2.24.0 | `CXXFLAGS="-x c++ -mevex512"` | **builds**, 2m02s, no SIMD targets peeled |
| 2.26.2 (numkong 7.8.2) | none | numkong probes fail `evex512`, `sapphireamx.h` hard-fails |
| 2.26.2 | `-mevex512` in CXXFLAGS and CFLAGS | still fails (`popcnt` attribute, diamond probe) |

So options 1–3 above are dead ends (upstream 2.26 is worse under zig; no `cc` bump is
involved — the driver selection is zig's) and option 4 leaves the shipping recipes broken.

## Decision (D3)

Fix at the recipe: target-scoped env on the two `cargo zigbuild` invocations so nothing else
(the Docker builder, native builds, `just check`) sees the flags. Not `.cargo/config.toml`
(the Docker builder's older clang has no `-mevex512`).

## What to build

- `Justfile` `cross-build`: prefix the existing zigbuild line (unchanged otherwise) with
  `CXXFLAGS_x86_64_unknown_linux_gnu="-x c++ -mevex512"`.
- `Justfile` `cross-build-arm`: prefix the existing zigbuild line with
  `CXXFLAGS_aarch64_unknown_linux_gnu="-x c++"`.
  (aarch64 has no AVX-512; verify whether the `-x c++` half alone suffices there, and if the
  aarch64 build needs anything further, report it rather than guessing.)
- A recipe comment of one or two lines above each naming the two zig quirks and why the env
  is target-scoped; point at this issue.
- Any doc that shows the raw `cargo zigbuild` line (grep `zigbuild` under `website/` and
  `docs/`) updated to match if it would otherwise mislead.

## Acceptance criteria

- [ ] `just cross-build` succeeds on a macOS Apple Silicon host with cargo-zigbuild 0.22.1 / zig from `.mise.toml`, and `just cross-verify` reports a Linux x86-64 ELF
- [ ] `just cross-build-arm` succeeds on the same host (aarch64 ELF under `target/aarch64-unknown-linux-gnu/release/`)
- [ ] `usearch`'s build log shows no `SIMSIMD_TARGET_*` peeling (the six-retry loop does not run)
- [ ] no change to `.cargo/config.toml`, `Cargo.toml`, or the Docker builder
- [ ] `just lint-gates` green (`lint-ship-cmd-full` still sees `cmd-full` on both recipes)

## Files likely touched

- `Justfile` (`cross-build`, `cross-build-arm`)
- docs mentioning the raw zigbuild invocation, if any

## Resolution

Landed on `build-toolchain/impl` at merge `f7928f807` (2026-09-04). Three commits:
`6659226fc` target-scoped `CXXFLAGS_x86_64_unknown_linux_gnu="-x c++ -mevex512"` /
`CXXFLAGS_aarch64_unknown_linux_gnu="-x c++"` on the two `cargo zigbuild` recipe lines (D3;
aarch64 needs no SIMD flag — its simsimd targets are NEON/SVE, no AVX-512 probe runs),
`2061a0cef` `AR="zig ar"` on both recipes for a second, previously unreachable defect
(tikv-jemalloc-sys builds jemalloc with autoconf, whose `configure` reads only plain `AR`, so the
host's Mach-O-only `ar` archived none of the ELF objects — 96-byte empty `libjemalloc_pic.a`,
undefined `mallctl`/`mallocx`/`rallocx`/`sdallocx` at link; `RANLIB` verified unnecessary),
`8437ce917` recipe comments trimmed after review. `just cross-build` green in 3m36s
(`cross-verify`: Linux x86-64 ELF), `just cross-build-arm` green (aarch64 ELF), zero
`SIMSIMD_TARGET_*` peels on either target, `lint-gates` ship count unchanged at 6. `Justfile` only.

Still open (not this issue): nothing in CI runs `just cross-build`, and `.mise.toml` pins
`zig = "latest"` while `-mevex512` is tied to zig 0.15.2's clang — issue 08 (the PRD's
"Detection gap" follow-up) is the detector. `cross-verify` still checks only the x86-64 artifact.
