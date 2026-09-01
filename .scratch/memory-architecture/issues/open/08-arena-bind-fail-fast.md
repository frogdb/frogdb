# 08: arena bind failure fails boot instead of degrading

Status: ready-for-agent
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
