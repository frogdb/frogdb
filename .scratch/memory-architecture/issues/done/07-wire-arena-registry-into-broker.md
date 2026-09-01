# 07: wire the shard arena registry into the broker's arena-sampler seam

Status: done
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

## Resolution (2026-09-01)

Landed as one commit, merged to main after review. The adapter is
`frogdb-server/crates/server/src/shard_arena_reading.rs`, in the server crate for the planned
reason: `frogdb-telemetry` (registry) depends on `frogdb-core` (broker construction), so only
the server may name both — same shape and rationale as `JemallocShardArenas`.

Deviations from the "what to build" sketch, all improvements:

- **Setter injection, not a builder parameter.** `ShardWorker::set_arena_sampler` →
  `MemoryBroker::set_arena_sampler` (both `&mut self`, boot-time only) matches the ~15
  existing `worker.set_*` calls in `spawn_shard_workers` instead of threading a parameter
  through three `ShardWorker` constructors and 17+ call sites.
- **Late binding via a shared slot.** A shard's arena is created on the shard's own thread as
  its first act, and the registry is assembled only after every shard launches — after the
  workers are gone into their threads. `ShardArenaReadings` hands each worker a per-shard
  reading over a shared `Arc<OnceLock<Arc<ShardArenaRegistry>>>` before launch;
  `publish()` fills the slot once, right after `report_arena_binding`. First publish wins.
- **Sim path wired unconditionally.** Instead of gating the wiring on executor kind, the sim
  registry simply has no entry for any shard, so every broker keeps reading `None` — the same
  answer as before the wiring, with one code path instead of two.

Read discipline held: `OnceLock::get` → `registry.sample(shard_id)` → `is_sampled()`; the
broker path never refreshes; no third `advance_epoch(` site (seam lint clean). Absent-not-zero
at every point in the sequence — unfilled slot, unbound shard, bound-but-unticked arena.

Eight tests, including two real-thread end-to-end tests (a bound shard's broker reports a
figure ≥ the bytes the thread holds live; a second shard's broker must not pick up shard 0's
bytes). Those two are environment-sensitive by nature (real jemalloc arenas; 64 KiB chunks
exceed tcache max so bytes hit the arena directly; allocations held live across the read).

**Flag for human:** `frogdb-server/crates/server/src/lib.rs` now declares a `#[cfg(test)]`
`#[global_allocator] tikv_jemallocator::Jemalloc` — without it the lib's test binary allocates
from the system allocator and every arena reads zero. Mirrors what `main.rs` does for the
production binary and what `frogdb-telemetry` already does for its tests.

Verification on the merged tree: lint-gates clean, frogdb-memory 21/21, frogdb-core
1042/1042, frogdb-server 2116/2116. Budget-growth ratchet unchanged (no ALLOWLIST edit).
