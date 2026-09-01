# 07: wire the shard arena registry into the broker's arena-sampler seam

Status: ready-for-agent
Type: AFK
Origin: issues 03 + 05 merge review, 2026-09-01
Area: frogdb-server (adapter) + frogdb-core shard builder wiring
Phase: 2 (observability first) — closes the 03↔05 seam left open when both landed in parallel

## Why

[Issue 05](../) landed the broker with a stubbed arena reading (`frogdb_memory::ArenaSampler`
trait, `NoArenaReading` default, plug-in point in `ShardWorkerBuilder`). [Issue 03](../)
landed the real figure: `frogdb_telemetry::ShardArenaRegistry`, sampled 10–100 Hz, upper-bound
semantics. Nobody connected them: today every broker reads `None` for its core's allocator
figure.

The connection cannot live in either owning crate — `frogdb-telemetry` depends on
`frogdb-core`, so `frogdb-core` (the builder) cannot name the registry. The adapter belongs in
the server crate, exactly like `JemallocShardArenas` adapts `ShardArenaSource`.

## What to build

1. An adapter in `frogdb-server/crates/server` implementing `frogdb_memory::ArenaSampler`
   over `Arc<ShardArenaRegistry>` + the shard's id (registry reachable via
   `SpawnedShards::arenas` / `Server::shard_arenas`).
2. Pass it into `ShardWorkerBuilder` where `NoArenaReading` goes today, real executor only —
   sim path keeps the stub (arena figures are vacuous under simulation).
3. Respect 03's read discipline: check `is_sampled()` (absent-not-zero for unbound shards),
   never call `refresh()` on a decision path — the sampler task owns epoch advances
   (`arena_sampling_is_not_on_a_command_path` pins this; do not add a third
   `advance_epoch(` site).
4. A test that a broker on a shard with a bound arena reports the sampled upper bound, and a
   broker on an unbound shard (or under sim) reports the same "no reading" it does today.

## Out of scope

Acting on the figure (maxmemory verdicts, eviction) — later phases. Changing either side's
API; this is wiring only.

## Depends on

[Issue 03](../) and [issue 05](../) — both landed.
