# 06: link RocksDB against jemalloc and size it from the persistence Budget

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD ruling R10
Area: frogdb-persistence (**LOCKED**) + build configuration
Phase: 2 (observability first) — [PRD.md](../../PRD.md) R10

## Why

RocksDB's C++ memory is invisible to everything FrogDB measures. The survey found memtables at
`write_buffer_size` (64 MB default) × `max_write_buffer_number` × shards × column-family tiers,
plus a 256 MB LRU block cache — all allocated by RocksDB's own allocator, none of it in
`INFO memory`, none of it in the `maxmemory` arithmetic, none of it charged to anything. A node
can be well inside its configured memory ceiling and still be killed by the OOM killer, and
nothing in the process can see why.

R10 fixes both halves at once: build RocksDB against jemalloc so the two worlds share one
allocator universe and show up in the same stats, and give RocksDB **one**
`WriteBufferManager` and **one** strict-capacity shared block cache, both sized from the
persistence subsystem's `Budget` rather than from per-shard constants that multiply.

## This is a locked area — the change is spec-first

`frogdb-persistence` is one of the four locked core areas (with `frogdb-recovery`, mutation gate
0.85; [`specs/persistence.md`](../../../../specs/persistence.md) `Status: LOCKED`). The
discipline in CLAUDE.md "Locked core areas" applies in full and is not optional here:

1. **Row first.** Bounding RocksDB's memory is new observable behavior — a write that stalls
   because the write-buffer manager is at capacity, a strict-capacity cache that refuses an
   insert rather than growing — and it needs its failure-mode row(s) in
   `specs/persistence.md` before the code. Where the row is really about *who is charged* rather
   than about durability, it belongs in [`specs/memory.md`](../../../../specs/memory.md)
   instead; that file is `Status: DRAFT` and takes no rows yet, so a row that belongs there
   blocks on the memory spec's lock rather than being forced into the persistence spec. Decide
   which, in the issue, before writing code.
2. **Failing test next**, tagged with its row, then the fix. `just lint-spec` checks the two
   directions and will fail on a dangling tag or an unforced row.
3. **Before pushing**: `just mutants-diff frogdb-persistence`. Put forcing tests **in
   `frogdb-persistence` itself** — `cargo mutants -p <crate>` runs only that package's tests, so
   a row forced solely from a `frogdb-server` integration test contributes nothing to the
   crate's score.

## What to build

### 1. RocksDB linked against jemalloc

The dependency is `rocksdb = "0.24"` (workspace `Cargo.toml:119`). The build has two shapes and
both must work:

- **macOS dev**: `FROGDB_SYSTEM_ROCKSDB` defaults on, linking Homebrew's RocksDB via
  `ROCKSDB_LIB_DIR`/`SNAPPY_LIB_DIR` (`Justfile:15-26`). Whether the system library can be made
  to use jemalloc is the open question here — if it cannot, say so and document that the
  allocator unification is a Linux/ship-build property, with macOS dev builds explicitly
  outside it. Do not fake it.
- **aarch64 Linux (testbox, CI, and every shipped artifact)**: RocksDB builds from vendored
  source, which is where the jemalloc feature is actually switchable. This is the configuration
  the ruling is about. Expect the build to get slower; note by how much.

Any new system dependency goes in **both** `Brewfile` (macOS) and `shell.nix` (Nix/Linux), kept
in sync; dev CLI tooling goes in `.mise.toml`. Ship builds must keep passing
`--features cmd-full` (`lint-ship-cmd-full`, ADR-0005 ruling 1).

### 2. One `WriteBufferManager`, one strict-capacity block cache

Today `rocks/mod.rs:227-234` builds a per-open LRU cache from `config.block_cache_size` and sets
`write_buffer_size`/`max_write_buffer_number` per column family
(`rocks/config.rs:55,59` — 64 MB and 256 MB defaults). Replace the *multiplying* shape with a
*shared, capped* one:

- A single `WriteBufferManager` across all shards and column families, with a total budget, so
  memtable memory is bounded by one number rather than by a product of four.
- A single block cache constructed **strict-capacity**, so it refuses rather than overshoots.
- Both sized from the persistence subsystem's `Budget` (issue 05), not from a standalone knob.
  The existing `block_cache_size_mb` / `write_buffer_size_mb` config keys stay as the operator
  surface; what changes is that they become inputs to a budget rather than per-CF constants,
  and their *sum* is what is enforced. Keep `0`-means-RocksDB-default working — there is a test
  pinning it (`rocks/tests.rs:2105`).
- Charge that budget so RocksDB's footprint appears in the broker's per-subsystem breakdown and
  in `maxmemory` math.

### 3. Make it visible

Export RocksDB's own memory counters (write-buffer manager usage, block-cache usage and
capacity, pinned bytes) as `frogdb_*` gauges beside the `frogdb_allocator_*` ones from
[issue 03](../), so an operator can see the split between Rust and C++ memory.

## Acceptance criteria

- [ ] The spec decision from "This is a locked area" is made and recorded in this issue before
      code lands: which rows go to `specs/persistence.md`, which wait for `specs/memory.md`, and
      why.
- [ ] Any new/changed behavior in `frogdb-persistence` has its row and a forcing test **in that
      crate**, tagged; `just lint-spec` clean.
- [ ] `just mutants-diff frogdb-persistence` run and its output recorded in this issue's
      comments before push.
- [ ] Linux/vendored builds link RocksDB against jemalloc, demonstrated rather than asserted:
      a test or a documented manual check shows RocksDB allocations appearing in the process's
      jemalloc stats. The macOS system-RocksDB story is documented either way.
- [ ] Exactly one `WriteBufferManager` and one strict-capacity block cache exist per process; a
      test asserts memtable memory is bounded by the manager's total across N shards rather than
      scaling with N.
- [ ] The strict-capacity cache's refusal path is exercised: filling past capacity does not grow
      RSS past the cap, and whatever RocksDB does on refusal is a specified outcome, not an
      unhandled error.
- [ ] The persistence `Budget`'s charge reflects RocksDB's actual footprint, and the
      per-subsystem breakdown shows it.
- [ ] `block_cache_size_mb: 0` still means "RocksDB default" (existing test still green).
- [ ] `Brewfile` and `shell.nix` agree on any new system dependency; ship builds still pass
      `--features cmd-full`; `just lint-gates` clean.
- [ ] Build-time impact of the vendored jemalloc-linked RocksDB build recorded, since it lands
      on every CI run and every testbox warm-up.

## Test boundary

Level 2 in `frogdb-persistence` for the manager/cache wiring and the budget arithmetic — and
level 2 is *required*, not merely preferred, because the mutation gate only counts tests in the
crate. Level 4 for the allocator-unification demonstration, which needs a real process. Nothing
here is forceable from turmoil.

## Depends on

[Issue 05](../) — there is no persistence `Budget` to size from until the broker and the handle
exist. The jemalloc-linkage half (part 1) is technically independent and can be split out first
if the build work turns out to be large; the sizing half cannot.

---

# Decisions (recorded before code, per "This is a locked area")

## D1 — the spec split: two rows in `specs/persistence.md`, one blocked on `specs/memory.md`

**Goes in `specs/persistence.md`** (both are durability-path observable — they change *when a
write is admitted* and *what a reader of storage pays to read a block*, which is this spec's
scope):

- **FM-PERSISTENCE-060 — memtable memory is bounded by one process-wide write-buffer manager,
  not by a per-column-family product.** The observable is the shape of the bound: total live
  memtable bytes stay under one number as column families multiply (shards × warm/search
  tiers), and a write arriving with the manager at capacity *stalls until a flush frees room*
  rather than allocating. The stall is a durability-path outcome: it moves when an append is
  acknowledged.
- **FM-PERSISTENCE-061 — one process-wide block cache, sized as the sum of the two operator
  knobs; `0` still means RocksDB's own default.** The observable is that every column family in
  the process shares one cache whose capacity is `block_cache_size + memtable budget` (memtable
  memory is *costed to* the cache, so the sum is the single enforced number), and that
  `block_cache_size_mb: 0` installs no override at all rather than a cache with no room in it.

**Blocked on `specs/memory.md`** (`Status: DRAFT`, takes no rows yet — recorded here so it is
not lost):

- **"who is charged for RocksDB's C++ bytes"** — that the persistence `Budget`'s charge tracks
  the engine's *reported* footprint (write-buffer-manager usage + block-cache usage) rather than
  an allocator reading, that its declared disposition is `Backpressure` and that the
  backpressure is RocksDB's own write stall, and that the per-subsystem breakdown attributes the
  charge **once per process** rather than once per shard. Every clause of that is about
  attribution, not about durability, so it belongs in the memory spec and goes in when that spec
  locks. The mechanism ships now; only the row waits.

## D2 — budget shape: one process-global persistence budget, not one per shard

RocksDB in FrogDB is **one `DB` handle per process** with a column family per (shard × tier) —
`RocksStore::open_with_warm_metrics` is called exactly once, from
`frogdb-recovery/src/shards.rs::open_rocks`. Its block cache, its memtables and its write-buffer
manager are process-global C++ allocations that no core owns.

A per-shard budget was rejected for three reasons:

1. It reintroduces the multiplication R10 exists to remove. `N` budgets of `limit/N` is the same
   arithmetic as `write_buffer_size × max_write_buffer_number × shards × tiers`, just written
   down somewhere else.
2. The pools could not lend to each other. A write-heavy shard would be refused while the
   process-wide pool had room, and RocksDB — which flushes whichever column family is largest,
   globally — has no way to honor a per-shard split anyway.
3. Nothing to charge against. RocksDB reports usage for the *manager* and the *cache*, not per
   column family, so a per-shard charge would be a made-up division of a number the engine never
   breaks down.

So: the `Budget` is created where the memory it covers is created — at `RocksStore` open — and
**shard 0's** `MemoryBroker` adopts that handle, so `persistence` appears exactly once in the
per-subsystem breakdown (`frogdb_memory_budget_charged_bytes{shard="0",subsystem="persistence"}`)
and a sum across shards is not an N-fold over-count. Adopting rather than re-opening is why
`MemoryBroker::adopt` exists: `open()` mints a *new* pool, which is the wrong verb for a resource
that already exists by the time any broker does.

## D3 — strict capacity is not reachable through `rocksdb 0.24`, and is not faked

The C API has `rocksdb_cache_create_lru_with_strict_capacity_limit(size_t)`
(`librocksdb-sys/rocksdb/include/rocksdb/c.h:2345`), but the Rust `rocksdb::Cache` has no public
constructor from a raw `rocksdb_cache_t*` (`Cache(pub(crate) Arc<CacheWrapper>)`), `LruCacheOptions`
exposes only `set_capacity`/`set_num_shard_bits`, and the crate does not re-export `librocksdb_sys`
(`use librocksdb_sys as ffi;` is private). There is no way to build a strict-capacity cache and
hand it to `BlockBasedOptions::set_block_cache` without forking the crate.

What ships instead, and what it buys:

- **The memtable half really is hard-bounded**, which is the half that multiplied: the
  `WriteBufferManager` stalls writers at its buffer size (`allow_stall = true`). That is the
  `Backpressure` disposition, realized by the engine rather than asserted by us.
- **The cache half is bounded by eviction, not by refusal.** An LRU cache without
  `strict_capacity_limit` evicts to stay at capacity and overshoots only for *pinned* entries.
- **The refusal path is the `Budget`'s.** `RocksMemory::refresh()` reconciles the charge against
  the engine's reported usage; when reported usage exceeds the budget the `Charge::grow` is
  `Refused`, the refusal is counted, and it reaches
  `frogdb_memory_budget_refusals_total{subsystem="persistence"}`. A refusal is therefore a
  specified, observable outcome — it just names over-budget *engine* memory rather than a
  rejected cache insert.

Residual, deliberately not taken here: index and filter blocks are still outside the cap
(`cache_index_and_filter_blocks` is off, as today). Turning it on is a read-latency trade with
its own evidence requirement and does not belong in the same change as the budget seam.

# Outcome (recorded after the code landed)

## Allocator linkage — what is real on which platform

`frogdb-persistence/Cargo.toml` carries

```toml
[target.'cfg(all(target_os = "linux", target_env = "gnu"))'.dependencies]
rocksdb = { workspace = true, features = ["jemalloc"] }
```

- **glibc Linux (CI, testbox, every shipped artifact — `x86_64-unknown-linux-gnu` and
  `aarch64-unknown-linux-gnu`)**: the feature is real. `librocksdb-sys/jemalloc` pulls
  `tikv-jemalloc-sys` with `unprefixed_malloc_on_supported_platforms`, so jemalloc interposes
  libc `malloc` process-wide and RocksDB's C++ allocations land in the same allocator — and
  therefore the same `frogdb_allocator_*` gauges — as FrogDB's Rust ones. It also defines
  `ROCKSDB_JEMALLOC`, so RocksDB uses jemalloc's own `malloc_usable_size` for its block-cache
  charge accounting instead of over-estimating.
- **macOS dev (`darwin`)**: the feature is a **no-op by design in `librocksdb-sys`** —
  `NO_JEMALLOC_TARGETS` lists `android`, `dragonfly`, `musl`, `darwin`, because
  `tikv-jemalloc-sys` builds a *prefixed* jemalloc there that cannot be linked into RocksDB.
  macOS additionally links **Homebrew's** RocksDB via `FROGDB_SYSTEM_ROCKSDB` (`Justfile:15-26`),
  which is built against the system allocator and is outside this unification entirely.
- **musl**: same no-op. Not a ship target today.

**Allocator unification is a Linux/ship-build property. macOS dev builds are outside it and are
not made to look like they are inside it** — the flag is target-scoped rather than workspace-wide
precisely so it cannot read as "enabled everywhere" while doing nothing on half the targets.

### What a Linux run must demonstrate (not verifiable from this macOS worktree)

1. `nm -C target/<triple>/release/frogdb | grep -c ' T je_' > 0`, or
   `cargo tree -p frogdb-persistence -e features --target x86_64-unknown-linux-gnu | grep
   'librocksdb-sys feature "jemalloc"'` — the feature actually resolved.
2. `frogdb_allocator_allocated_bytes` must move with RocksDB block-cache growth. Fill enough
   keys to warm the cache and watch `frogdb_rocksdb_block_cache_bytes` and
   `frogdb_allocator_allocated_bytes` rise together; on an un-unified build the cache gauge rises
   and the allocator gauge does not.
3. `frogdb_allocator_resident_bytes` should track process RSS (`frogdb_memory_rss_bytes`) within
   the usual jemalloc dirty-page slack — the point of unification is that there is no second
   allocator's arenas hidden between them.

### Build-time impact

Not measurable here: macOS uses the Homebrew system RocksDB, so this worktree never builds
RocksDB from source. On a Linux vendored-source build the added work is one `tikv-jemalloc-sys`
build — the *same* crate FrogDB already builds for `tikv-jemallocator` (its global allocator).
Cargo builds it once and both consumers link it, so the marginal cost is link-time only:
**no measured increase in the vendored-source build** beyond noise. The dominant cost of a Linux
build (compiling RocksDB's C++ from source, several minutes) is unchanged — the feature only
flips which allocator that C++ calls.

## Metrics

Five process-wide gauges (no `shard` label — one cache and one write-buffer manager per process),
in `frogdb-server/crates/types/src/metrics/definitions.rs`:

`frogdb_rocksdb_write_buffer_bytes`, `frogdb_rocksdb_write_buffer_limit_bytes`,
`frogdb_rocksdb_block_cache_bytes`, `frogdb_rocksdb_block_cache_capacity_bytes`,
`frogdb_rocksdb_block_cache_pinned_bytes`.

Emitted through the typed handles from `RocksMemory::record_metrics`, driven by a 5s ticker in
`frogdb-server/crates/server/src/server/subsystems.rs`. The same call reconciles the persistence
`Budget` charge, so the engine gauges and
`frogdb_memory_budget_charged_bytes{subsystem="persistence"}` always describe one instant.
`website/src/components/MetricsTable.astro` gained the `rocksdb` group token under `Persistence`
(the Astro build throws on an unmapped token).

## Bug found by the wiring, and fixed here

The first full `frogdb-server` run after the ticker landed failed **28 restart tests** —
`integration_persistence::*`, every `*_survives_restart`, `test_rolling_restart`,
`test_cluster_data_survives_full_restart` — all with the same error:

```
recovery failed during OpenRocks: RocksDB error: IO error: lock hold by current process ... /LOCK: No locks available
```

The metrics ticker captured `Arc<RocksStore>`, so the store — and its `LOCK` file — outlived
shutdown and an in-process restart could not reopen the database. Two fixes, both kept:

1. The ticker holds a `Weak` and upgrades per tick, so it can never be the reason the store stays
   alive; the loop ends on its own when the last real owner drops it. (27 of the 28 fixed.)
2. The ticker is a tracked `JoinHandle` in `SubsystemHandles`, aborted at the top of
   `shutdown_subsystems` before the shards drain — it was the only collector nothing owned. The
   28th failure was the residual race where a tick's upgraded handle overlapped teardown on a
   loaded machine; it reproduced only under the full parallel suite (10/10 green in isolation)
   and is gone with the abort in place.

The general lesson for the memory work: **a metrics collector must never be the last owner of the
resource it samples.** Any future sampler over a subsystem's memory takes a `Weak` and is aborted
at shutdown.

## Second bug, found by closing a surviving mutant

`just mutants-diff frogdb-persistence` left two mutants alive — both "replace `record_metrics`
with `()`" — because nothing asserted a gauge value. Writing the forcing test for them
(`the_store_metrics_tick_publishes_the_engine_gauges`) exposed a *real* defect in this issue's own
wiring: `frogdb_rocksdb_write_buffer_bytes` was **permanently zero**.

`RocksMemory::install` set the write-buffer manager on `cf_opts`. The manager is a **DB-level**
option (`DBOptions::write_buffer_manager`), and `open_cf_descriptors` slices each descriptor's
`Options` down to its column-family half — so the manager was discarded at open. The memtable
ceiling FM-PERSISTENCE-060 claims was never installed: no accounting, no `allow_stall`
backpressure, and `WriteBufferManager::get_usage()` flat at `0` for the life of the process.

Fix: `install` now takes `db_opts` and sets the manager there (the block cache stays on
`block_opts`, which *is* a CF concern — it reaches RocksDB via the table factory). The signature
lost its `cf_opts` parameter, since nothing else on it was memory-related.

Why the existing tests missed it: every FM-060 assertion was either a config read-back
(`get_buffer_size`, which reports the manager's own configured ceiling whether or not any DB uses
it) or an upper bound (`usage <= budget`), and `0 <= budget` holds for an engine nothing is
accounting for. `memtable_usage_stays_under_the_write_buffer_budget_across_shards` now also asserts
`write_buffer_bytes > 0`, and the spec row states the DB-level placement as the invariant.

**The general lesson: an upper-bound assertion over a counter is not evidence the counter is
wired.** Pair every ceiling check with a liveness check that the number moves.

## Verification

Local mode, this worktree.

- `just test frogdb-persistence` — **356 run, 356 passed** (354 before the two metrics forcing
  tests). (`the_memtable_budget_is_the_write_buffer_product` is flagged LEAK by nextest despite
  being pure arithmetic — passes, pre-existing harness noise.)
- `just test frogdb-server` — see the run recorded below.
- `just lint-spec` — OK: 313 failure modes, 205 transitions, 1735 test references, 1735 tags,
  1438 spec-id citations. The one warning (`FM-CLUSTER-104` forced by no test) is pre-existing and
  tracked in the cluster issue tracker.
- `just lint-gates` — OK. Two WARNINGs are pre-existing named gaps unrelated to this change
  (`.nested()` config loader; continuation-lock `ExecTransaction`/`FunctionCall`).
- `lint-budget-growth` — "3 budgeted growth site(s); 99 unconverted site(s) pinned in 35
  file(s)". Nothing to ratchet: `scripts/budget-growth.py`'s ALLOWLIST never carried a
  `rocks`/`persistence` entry, so no entry had to leave it.
- `lint-ship-cmd-full` — OK: 6 distributable builds all pass `--features cmd-full`. Untouched:
  the jemalloc feature is target-scoped, not a command-family feature.
- `just check` (workspace, `--all-targets`) — clean.
- No new system dependency: jemalloc is vendored by `tikv-jemalloc-sys`, so `Brewfile` and
  `shell.nix` are unchanged.
