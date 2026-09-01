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
