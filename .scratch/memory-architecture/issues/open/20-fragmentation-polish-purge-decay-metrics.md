# 20: fragmentation polish — decay tuning, per-arena metrics, re-encode escape hatch

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R13
Area: frogdb-telemetry (jemalloc) + frogdb-server (info/) + frogdb-memory
Phase: 6 — polish

## Why

R13's ruling: FrogDB gets **no active defrag**. The packed value representations (phase 4)
and per-shard arenas ([issue 03](../)) remove the main fragmentation drivers; what
remains is (a) tuning jemalloc's own reclamation, (b) making per-arena fragmentation
*visible* so an operator can tell the difference between real growth and dirty-page lag, and
(c) a manual per-key escape hatch for the rare pathological key. Redis's activedefrag exists
because its values are pointer forests that can be rewritten in place; FrogDB's answer is
"values are packed and arenas are observable" — this issue delivers the observable half.

Two Linux-validation findings fold in
([spike-report-linux.md](../../spike-report-linux.md), E5b):

- The arena sampler's 10–100 Hz clamp is **not shard-count-aware**: per-arena epoch+read cost
  is a platform constant, so at ~32+ shards the sampling budget per arena at the clamp
  ceiling starts to matter — the clamp needs to scale with shard count (or the sampler needs
  to stride arenas across ticks).
- The sampler must stay **off shard cores** — it runs on a utility thread; keep it that way
  and assert it (a sampler migrating onto a pinned shard core is a silent tail-latency bug).

## What to build

### 1. Decay tuning

Set explicit `dirty_decay_ms`/`muzzy_decay_ms` per shard arena (via `_RJEM_MALLOC_CONF` /
per-arena mallctl — note it is the `_RJEM_` prefix with our vendored jemalloc, per the spike
report). Pick defaults measured on the Linux rig (start: dirty 10s, muzzy 0 — jemalloc
defaults are close to right; the deliverable is that they are *explicit, documented, and
per-arena settable at runtime* via a debug/frogctl surface, not that they change).

### 2. Per-arena fragmentation metrics

The INFO memory section already derives global `allocator_frag_ratio` /
`mem_fragmentation_ratio` (`frogdb-server/src/info/sections.rs:192–213`, Redis-formula
compatible). Add per-arena depth from mallctl (`stats.arenas.<i>.{small,large}.allocated`,
`pdirty`, `pmuzzy`, `retained`):

- Prometheus: `frogdb_arena_{allocated,active,dirty_pages,muzzy_pages,retained}_bytes`
  labeled by shard/core (regenerate grafana per the codegen rule).
- INFO: a per-shard line in the memory section (or DEBUG-level section if line count is a
  concern — follow the sections.rs house rule of never emitting placeholder zeros).
- Replace any remaining stubbed jemalloc figures (sections.rs:1130 documents the "real
  reading, not a stub" contract — extend it to the per-arena set).

### 3. Sampler clamp scaling

Make the [issue 03](../) sampler's rate clamp shard-count-aware: budget total sampling
CPU (arenas x per-arena epoch cost) and derive the per-tick rate, striding arenas across
ticks past ~32 shards rather than reading all arenas every tick. Assert the sampler thread's
affinity excludes shard cores (fail-fast in debug, metric in release — same spirit as
[issue 08](../)'s bind fail-fast).

### 4. Per-key re-encode escape hatch

`DEBUG RE-ENCODE <key>` (or frogctl equivalent): rewrites one key's value through its current
encoding — a packed value re-packs (compaction), dropping dead block space and returning
pages to the arena. This is the manual "defrag one key" tool for the pathological case; it is
O(value) and explicitly operator-invoked. No background scanning, no automatic trigger —
that is the active-defrag door R13 keeps shut.

## Acceptance criteria

- [ ] Decay settings explicit per arena and adjustable at runtime; documented in the ops docs
      (website — check for a memory/operations page to update).
- [ ] Per-arena metrics exported (Prometheus + INFO), grafana regenerated, no stubbed values.
- [ ] Sampler cost bounded as shard count grows (test with a mocked arena count of 64
      asserting per-tick reads are strided); sampler affinity asserted off shard cores.
- [ ] `DEBUG RE-ENCODE` compacts a churned packed value (test: churn a hash, re-encode,
      `memory_size()` drops to compact size).
- [ ] `just lint-gates` clean (metrics emission goes through the telemetry chokepoint — the
      existing seam lint applies).

## Test boundary

Level 1 for sampler striding math. Level 2 for re-encode. Metric presence via the existing
INFO/metrics test patterns. Decay measurements are bench-side (Linux rig), recorded in the
resolution, not CI-asserted.

## Spec rows at R15

The "no active defrag; re-encode is manual and O(value)" stance is a documentable invariant
but likely ADR/spec-prose, not an FM row. The sampler-off-shard-cores property could become a
row if [issue 22](../) locks scheduling invariants; flag it there.

## Out of scope

Active/background defrag (permanently, per R13), changing `narenas`/binding (issues 03/07/08,
done), THP policy, allocator swaps.

## Depends on

[Issue 03](../) sampler (done), [issue 07](../) registry→broker wiring (done).
Benefits from phase-4 packed values (re-encode has something to compact) but does not block
on them — re-encode of a listpack-small hash already compacts today.
