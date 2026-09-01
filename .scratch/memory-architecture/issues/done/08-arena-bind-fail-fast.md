# 08: arena bind failure fails boot instead of degrading

Status: done
Type: AFK
Origin: post-wave review grill, 2026-09-01 — human reopened issue 03's degraded-not-fatal ruling
Area: frogdb-net (shard launch) + frogdb-server boot path

## Why

Issue 03 shipped arena binding degraded-not-fatal: if `arenas.create` or the bind fails on a
shard thread, the shard logs ERROR, runs with `arena=None`, and the server continues with a
blind spot in per-shard memory accounting. That was honest while the figures were
observability-only.

Phase 3+ makes the arena reading an *enforcement* input (maxmemory verdicts through the
broker's `ArenaSampler` seam, wired in issue 07). Degraded then means silently blind
accounting on the shard's decision path. Human ruling: **fail boot by default, no config
knob** — correctness-first, don't run with blind accounting; nobody has asked for an escape
hatch yet.

## What to build

1. When the real executor has arenas available (`ShardArenaSource::arenas_available()`) and a
   shard thread's bind fails, shard launch returns an error and boot aborts with a clear
   message naming the shard and the mallctl failure. The plumbing mostly exists: `launch`
   already blocks on the mpsc channel until the thread reports its arena — report a bind
   *error* through that channel instead of a degraded `None`.
2. The sim path and `NoShardArenas` (`arenas_available() == false`) are untouched: no arenas
   is a configuration, not a failure. `arena=None` remains legal only when no arena source is
   in play.
3. Update `specs/memory.md` (DRAFT) where it records the degraded-not-fatal behavior; the
   draft spec follows the ruling.
4. Tests: a failing bind aborts launch with the shard id in the error; the no-arena-source
   path still boots. Kill the now-dead degraded path and any test pinning it.

## Out of scope

A `memory.allow-degraded-arena-accounting` opt-out (speculative until someone needs it).
Changing bind order or sampling.

## Depends on

Nothing — issues 03 and 07 are merged.

## Resolution (2026-09-01)

Landed in `feat(memory)!: fail boot on a refused shard arena bind; move real-thread arena
tests out` (one combined commit with issue 09, same code area). `ShardExecutor::launch` now
returns `Result<ShardHandle, ShardLaunchError>`; the shard thread reports its bind through
the existing mpsc channel as a `Result`, returns before running a line of the worker on
refusal, and `spawn_shard_workers` aborts boot via its `anyhow::Result` with the shard id
and the refusing mallctl (`arenas.create` or `thread.arena`) in the message. No config
knob. `NoShardArenas`/sim untouched: `Ok(None)` — and `arena_of() == None` — now mean one
thing only, no arena source in this build. The degraded path, its two `tracing::error!`
logs, the partial-binding `warn!` in `report_arena_binding`, and the test pinning
degradation are all gone; mixed bound/unbound is impossible. `specs/memory.md` (DRAFT)
allocation-substrate bullet records the ruling.

Tests: `a_failed_arena_bind_fails_the_launch_and_names_the_shard` covers both refusing
mallctls (asserts shard id, mallctl name, empty `bound_arenas()`, worker ran 0 times);
`real_executor_without_a_source_reports_no_arena` extended with an unavailable source that
would fail create/bind if asked. frogdb-net 15/15, frogdb-server 2116/2116, lint-gates OK.
