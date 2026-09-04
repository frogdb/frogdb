# 25: shard arenas and the arena sampler run in binaries where jemalloc is not the global allocator

Status: needs-triage
Type: AFK
Origin: issue 24 investigation, 2026-09-04 (incidental finding)
Area: frogdb-server (net.rs `JemallocShardArenas`), frogdb-telemetry (`jemalloc`, `shard_arenas`)
Phase: 5 — polish

## Why

`JemallocShardArenas::arenas_available()` (`frogdb-server/crates/server/src/net.rs:39`) answers
`frogdb_telemetry::jemalloc::narenas().is_some()`, i.e. "does the `arenas.narenas` mallctl
succeed". That is true whenever jemalloc is *linked*, not only when it is the process's *global
allocator*. `tikv-jemalloc-sys` is linked into every `frogdb-server` test binary through
`frogdb-telemetry`, but `#[global_allocator] Jemalloc` is declared only in `main.rs`,
`telemetry/src/lib.rs` (`cfg(test)`), and `tests/arena_reading.rs` (its own `[[test]]` for exactly
this reason, `server/Cargo.toml:58-62`).

So every integration-test server (`frogdb-net/src/lib.rs:386` gate passes) creates one jemalloc
arena per shard, binds the shard threads to it, and runs the `ArenaSampler` at 10–100 Hz advancing
the process-global jemalloc epoch (`EPOCH_COST_PER_ARENA_NANOS = 13_400`,
`telemetry/src/shard_arenas.rs:329`) — for an allocator that serves none of that process's
allocations. Arenas are never destroyed, so they accumulate across the tests in one binary. Pure
overhead in tests; a contributor (not the cause) to the cluster-suite slowdown bisected in issue 24.

## What to build

`arenas_available()` returns true only when jemalloc is the global allocator. Cheapest honest probe:
allocate something, read `thread.allocated` (or `stats.allocated`) before/after via mallctl, and
require it to move — or expose a `const`/static set by the `#[global_allocator]` site. Pick the
one that does not need a new seam. Integration-test binaries without the global allocator then
skip arena creation and never start the sampler; `main.rs` and `arena_reading.rs` are unchanged.

## Acceptance criteria

- [ ] `arenas_available()` false in a test binary that links jemalloc but does not declare it global
- [ ] `arenas_available()` true in `arena_reading.rs` and the server binary (existing tests keep passing)
- [ ] no `ArenaSampler` thread and no `ArenaSamplerOnShardThreadTotal` increments in an ordinary `frogdb-server` integration test
- [ ] unit test pinning the probe

## Files likely touched

- `frogdb-server/crates/server/src/net.rs`
- `frogdb-server/crates/telemetry/src/jemalloc.rs`
- `frogdb-server/crates/net/src/lib.rs` (gate site, probably unchanged)

## Depends on

Nothing. Independent of the issue 24 fix; do not fold into it.
