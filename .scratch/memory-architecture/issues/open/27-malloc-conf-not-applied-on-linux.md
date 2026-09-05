# 27: `_rjem_malloc_conf` is not applied on Linux CI, and the ALL_ARENAS decay sentinel is accepted there

Status: needs-triage
Type: AFK
Origin: build-toolchain CI run on `build-toolchain/impl` @ c66fc0fb5 (https://github.com/nathanjordan/frogdb/actions/runs/33937564779); identical on the Sept 3 `main` run (76b2a6dae). Filed from the build-toolchain session.
Area: frogdb-server (`main.rs` malloc_conf export), frogdb-telemetry (`jemalloc`)
Phase: 5 — polish

## Why

Two jemalloc tests fail deterministically on `ubuntu-latest` (x86-64) and pass on macOS:

```
frogdb-server::bin/frogdb-server tests::jemalloc_applies_the_requested_options
  frogdb-server/crates/server/src/main.rs:195
  jemalloc ignored `narenas:1,dirty_decay_ms:10000,muzzy_decay_ms:0`
  (it reports narenas=Some(16), decay=Some(ArenaDecay { dirty_ms: 10000, muzzy_ms: 0 }));
  check the `_rjem_` symbol prefix
  left: Some(false)  right: Some(true)

frogdb-telemetry jemalloc::tests::the_all_arenas_sentinel_is_rejected_by_the_decay_mallctl
  frogdb-server/crates/telemetry/src/jemalloc.rs:1028
  assertion failed: set_arena_decay(ALL_ARENAS, ArenaDecay { dirty_ms: 0, muzzy_ms: 0 }).is_err()
```

The first one matters in production, not just in CI: `main.rs:18` overrides jemalloc's weak
`malloc_conf` via `#[unsafe(export_name = "_rjem_malloc_conf")]`, and on the Linux runner
jemalloc reports `narenas = 16` (its 4 × ncpu default) — the `narenas:1` request did not take
effect, while the decay values did (defaults? or applied through another path — verify). Linux is
the shipping platform, so if the override is a no-op there the whole arena-per-shard design runs
on jemalloc defaults in the Docker image and cross-built binaries.

The second is the opposite sign: Linux jemalloc accepts `arena.<MALLCTL_ARENAS_ALL>.dirty_decay_ms`
writes, macOS's build rejects them. The test pins the macOS behaviour as universal.

Suspects: the jemalloc symbol prefix on Linux under `tikv-jemalloc-sys` 0.6 (`je_` vs `_rjem_`
depending on the `unprefixed_malloc_on_supported_platforms` feature and platform), link order of
a strong symbol in the bin crate vs the weak one in the static lib, and jemalloc version
differences between the two platforms' builds.

## What to build

1. Reproduce on Linux (testbox or CI). Dump the effective `opt.narenas`, `opt.dirty_decay_ms`,
   `opt.muzzy_decay_ms` and `nm | grep malloc_conf` for the test binary and for the release
   `frogdb-server` binary.
2. Make the `malloc_conf` override take effect on Linux (correct symbol name / linkage), or, if it
   provably cannot, set the same options at startup through mallctl before any arena is created
   and keep the test as the proof.
3. Make `the_all_arenas_sentinel_is_rejected_by_the_decay_mallctl` state the real contract: either
   the sentinel is rejected by *our* wrapper regardless of what jemalloc does, or the test becomes
   platform-aware.
4. Both tests green on both platforms; the Linux proof is CI.

## Acceptance criteria

- [ ] `tests::jemalloc_applies_the_requested_options` passes on ubuntu-latest and macOS
- [ ] `jemalloc::tests::the_all_arenas_sentinel_is_rejected_by_the_decay_mallctl` passes on both, with the contract it pins written in its doc comment
- [ ] the shipped Linux binary reports `opt.narenas = 1` (checked once, noted in the resolution)

## Files likely touched

- `frogdb-server/crates/server/src/main.rs`
- `frogdb-server/crates/server/src/malloc_conf.rs`
- `frogdb-server/crates/telemetry/src/jemalloc.rs`

## Depends on

Nothing. Independent of 25 and 26.
