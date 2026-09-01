# 03: bind a jemalloc arena per shard thread and sample per-arena stats

Status: done
Type: AFK
Origin: memory-architecture phase-1 spike, 2026-08-31
Area: frogdb-telemetry / jemalloc + frogdb-server shard startup
Phase: 2 (observability first) — [PRD.md](../../PRD.md) R2, and the accounting half of R8

## Why now

Per-shard arenas are what turn accounting from hand-maintained `memory_size()` estimates into
allocator truth, and [spike-report.md](../../spike-report.md) §(a) says they deliver it:
attribution was byte-exact for three of four arenas and within 0.19 % for the fourth, with
**zero** cross-bleed, and binding cost nothing measurable (0.97–1.00× allocation cost across
64 B–64 KB). `resident` ran 15–23 % above `allocated`, which is the per-shard fragmentation
number R13 wants, available for free.

Observability lands before anything it would measure, so every later phase has a
before/after. The three implementation conditions the spike attached are in
[adr/0006](../../../../adr/0006-memory-architecture-seams.md) §3 and are acceptance criteria
here.

## What exists today

`frogdb-server/crates/telemetry/src/jemalloc.rs` is already the one place anything reaches into
jemalloc — `INFO memory`, `MEMORY PURGE` (`arena.<all>.purge`, real since `56c9019d`) and the
`frogdb_allocator_*` gauges all go through it, and its module doc says no other module calls
`tikv_jemalloc_ctl`/`tikv_jemalloc_sys`. Its `AllocatorStats` is process-wide
(`stats.{allocated,active,resident,mapped,retained}` after an `epoch::advance()`). This issue
extends that module; it does not add a second caller.

## What to build

1. **Arena creation and binding at shard-thread start.** The real `ShardExecutor` calls
   `arenas.create`, then `thread.arena` **on the shard thread, as its first act, before it
   allocates anything**, and records the arena index so `arena_of(shard_id)` returns `Some`.
   Bind-once is not a style preference: spike §(a) E4 measured a rebind without a
   `thread.tcache.flush` bleeding **1.00 %** of subsequently allocated bytes back to the old
   arena. If a rebind path is ever added (migration, teardown) it is a `thread.arena` write
   **plus** a cache flush, or the no-cross-arena-bleed invariant is quietly false.
2. **`narenas:1` in `MALLOC_CONF`.** jemalloc auto-creates `4 × ncpu` arenas (40 on the spike
   machine) and `epoch` merges every one of them, so the default costs ~5× more per sample at
   8 shards than N explicit arenas do. Set it at the same place the global allocator is
   declared and assert the live arena count at startup.
3. **Sampled per-arena stats.** Extend the telemetry module with a per-arena read
   (`stats.arenas.<i>.{small,large}.allocated`, `.resident`) behind a sampler that advances the
   epoch at a configurable **10–100 Hz**, not per request. Spike §(a) E5: an epoch advance
   costs ~2.5 µs *per live arena* (40 µs at 48 arenas, 340 µs at 136), while an individual
   by-name stat read is 146 ns once the epoch has moved. Per-request accounting rides
   `thread.allocated` instead — 23 ns, exact, per thread — with periodic reconciliation against
   the sampled arena figure.
4. **Say, in the type, that the arena figure is an upper bound.** Freed-into-cache objects
   still count as `allocated` on their arena until the owning thread flushes (E4 row 1: 25,600 B
   still charged after every object was freed; 0 after an explicit flush). Name the field or
   document it so no later caller treats it as live bytes. The overstatement direction is the
   safe one for a refusal, which is the property issue 05's budgets will rely on.
5. **Surface it.** Per-shard `allocated`/`resident` and the derived fragmentation ratio as
   `frogdb_allocator_*` gauges labelled by shard, plus whatever `INFO memory` can carry without
   deviating from Redis's field set. Keep the process-wide fields exactly as they are.

Minor, worth 20 minutes while in here: `mallctlnametomib` for `stats.arenas.<i>.small.allocated`
returned an error under `tikv-jemalloc-ctl 0.6.1` in the spike, so the pre-resolved-MIB fast path
was never measured. The by-name path at 146 ns is already far below the epoch cost, so this
changes no conclusion — resolve it or record why it does not resolve.

## Acceptance criteria

- [ ] Each shard thread is bound to a distinct arena; `arena_of(shard_id)` returns `Some(i)`
      under the real executor and **still `None` under the sim executor** (issue 01's permanent
      assertion must not be weakened).
- [ ] A real-thread regression test asserts **zero cross-arena attribution**: N shard threads
      each allocate a distinct known volume, and every arena reports its own thread's bytes and
      no other's. This is the executable form of PRD R3's no-cross-arena-bleed rule and the
      generalisation of the spike's E1b, per spike §"Follow-ups" item 6.
- [ ] A test asserts the bind happens before the shard thread's first allocation — e.g. an
      arena freshly created for a shard reports a plausible-and-attributed figure rather than a
      near-zero one with the bytes on the default arena.
- [ ] `MALLOC_CONF` sets `narenas:1`; a startup assertion or test pins that `arenas.narenas`
      after shard startup equals `1 + shard_count` (or documents the exact expected value).
- [ ] The sampler runs at a configured rate in 10–100 Hz, never on a command path; a test pins
      that no per-command code path calls `epoch::advance`.
- [ ] Per-shard `allocated`/`resident`/fragmentation are exported; process-wide `INFO memory`
      fields are byte-identical to before.
- [ ] `frogdb-telemetry`'s jemalloc module is still the only caller of `tikv_jemalloc_*`
      (its module doc's claim, checked by a grep in the test or a note in the review).
- [ ] `just lint-gates` clean; `just test frogdb-server` and the turmoil suite green.

## Test boundary

Level 2 for the telemetry reads themselves (a crate-local harness can create arenas and read
stats). Level 4 / real-thread for cross-bleed and bind-order, which are exactly the assertions
[`specs/memory.md`](../../../../specs/memory.md) forbids forcing from a turmoil test — under
simulation every shard allocates from one thread's arena and the invariant is vacuously true.
The `msvc` target has no jemalloc; gate the new tests the same way the module already gates
itself (`cfg(not(target_env = "msvc"))`).

## Out of scope

Making anything *act* on these numbers: `maxmemory` verdicts, eviction, budgets and the broker
are issue 05 and later. This issue makes memory visible per shard and nothing else.

## Depends on

[Issue 02](../) — there is no shard thread to bind an arena to until the real executor spawns
one. (Ordering only: nothing here changes the executor's interface.)

## Resolution

Landed on main (5 commits, reviewed and cherry-picked from the implementing agent's branch).
Dependency inversion instead of a second mallctl caller: `frogdb-net` defines a
`ShardArenaSource` trait (`arenas_available`/`create_arena`/`bind_current_thread`); the sole
implementation, `JemallocShardArenas`, lives in the server crate wrapping
`frogdb_telemetry::jemalloc`, which stays the one module touching `tikv_jemalloc_*`
(pinned by the `arena_sampling_is_not_on_a_command_path` grep test — `advance_epoch(` appears
in exactly two files, neither on a command path).

Binding: the shard thread's **first act** — before `pin_current_thread`, before `block_on` —
is `bind_shard_arena`; bind-once, no rebind path (spike E4 tcache bleed). Degraded-not-fatal:
a failed create/bind logs ERROR and the shard runs unattributed (`arena_of() -> None`);
`launch` blocks on an mpsc until each thread reports its arena, so `bound_arenas()` is
complete at startup. Sim executor untouched — still `None`.

Sampling: `ShardArenaRegistry` (absent-not-zero for unbound shards, relaxed atomics, one
epoch advance per refresh) + `ArenaSampler::spawn` tokio task at `ArenaSampleRate` (clamped
10–100 Hz, default 20; `memory.arena-sample-hz`, boot-only `#[param(skip)]`). Sample type is
`ShardArenaSample` with upper-bound naming (`allocated_upper_bound`), `is_sampled()`,
`fragmentation_ratio()`. `narenas:1` via `_rjem_malloc_conf` weak symbol defined only in
`main.rs` (a lib definition duplicates the symbol in every test binary); startup
`arena_layout(num_shards, narenas)` reports AsIntended/Excess/Unknown (expected
`num_shards + 1`).

Surfaced: `frogdb_allocator_shard_{allocated,resident}_bytes` + `frogdb_allocator_shard_frag_ratio`
labelled by shard; `INFO memory` deliberately unchanged (per-shard figures are
Prometheus-only — Redis field-set compatibility). Attribution pinned by real-thread test
`each_arena_reports_its_own_shards_bytes_and_no_others` plus bind-before-first-allocation and
failed-bind-leaves-shard-running tests in `frogdb-net`.

Merge notes: `metrics/definitions.rs` union-merged with issue 05's five broker metrics;
`budget-growth.py` ALLOWLIST for `net/src/lib.rs` bumped 1→2 (`bound_arenas` push — startup,
bounded by shard count); grafana/deb/docs regenerated from the merged definitions.

For broker integration (follow-up): issue 05's `NoArenaReading` stub in `ShardWorkerBuilder`
is **not yet wired** to this issue's `ShardArenaRegistry`. The broker should read the registry
via `Server::shard_arenas` / `SpawnedShards::arenas`, check `is_sampled()`, and never call
`refresh()` on a decision path.
