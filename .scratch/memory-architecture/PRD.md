# Memory Architecture PRD

Status: RULED (design only — implementation not yet scheduled)
Date: 2026-08-31
Survey: full subsystem memory survey at `main@1f3cc12c`; shareable report at
https://claude.ai/code/artifact/952c39ae-b28f-40fe-bd34-9105f9537819

## Goal

Establish the ideal memory-management architecture for FrogDB, designed as if from
scratch, optimizing for: (1) minimal allocations / minimal allocation cost on hot paths,
(2) minimal fragmentation / maximal memory efficiency, (3) fewer memory failure modes.
Big architectural changes are in scope; FrogDB is pre-production and sweeping changes
are encouraged.

## Current-state summary (what the survey found)

- jemalloc is the global allocator but entirely passive: no mallctl, no stats, no
  tuning; `INFO memory` allocator fields are hardcoded stubs; `MEMORY MALLOC-SIZE` /
  `PURGE` are no-ops; no active defrag. A stale comment in
  `redis-regression/tests/memefficiency_tcl.rs` claims mimalloc — wrong.
- Accounting is hand-maintained per-value `memory_size()` estimates; eviction, OOM
  refusal, and health thresholds all key off the estimate. RSS is measured separately
  (sysinfo) for Prometheus/status but not INFO. RocksDB C++ memory (memtables
  64MB×4×shards×CF-tiers + 256MB block cache) is invisible to everything.
- Data plane: per-shard single-threaded tokio task owning `griddle::HashMap<Bytes,
  Entry>`; values are `Arc<Value>` with `make_mut` COW; `Bytes` everywhere; hand-rolled
  listpacks for small hash/set; lists are `VecDeque<Bytes>` (one heap alloc per
  element); JSON is a full serde tree; arena skiplist for zsets.
- Network: tokio-util `Framed`, 8KB buffers that never shrink; RESP decode is zero-copy
  but `ParsedCommand::try_from` memcpys every arg; no output-buffer limit for normal
  clients; RESP2 write-buffer bytes invisible to client accounting.
- Persistence: bounded flume channel (8192, blocks-not-drops) → per-shard flush thread
  → RocksDB WriteBatch; full-copy serialization, no streaming encode; checkpoints are
  cheap (hardlink); MULTI/EXEC write-groups deliberately un-size-capped
  (FM-PERSISTENCE-001).
- Replication/cluster: backlog is a bounded dual-cap ring; checkpoint fullsync is
  chunked with pre-alloc ceilings; LiveDataset fullsync materializes the whole keyspace
  in RAM twice and the replica allocates `vec![0u8; header.size]` from the wire with no
  ceiling; feed sequencer hold buffer lacks its spec-ruled byte cap (TR-CLUSTER-016);
  tracking-table `lru_order` grows unbounded outside accounting (issue 66).

## Rulings

Each ruling below was made explicitly in the grill session, with the recommended
option chosen in all cases except scope (user chose the bigger bet) and framing
(user reframed to greenfield-ideal, implementation deferred).

### R1 — Framing: ideal architecture, all axes

Design the ideal end-state (table + eviction + arena/allocation model + everything
that hangs off them) as if starting from scratch. Implementation is phased later;
this document is the ruling on where we are going, not a delivery plan.

### R2 — Heap model: thread-per-core, per-shard jemalloc arenas

Each shard is pinned to an OS thread; each shard thread gets a dedicated jemalloc
arena (`arenas.create` + thread tcache binding). Consequences:

- Accounting becomes allocator-truth per shard (arena stats), not estimates.
- Fragmentation is isolated and measurable per shard; shard teardown can purge.
- NUMA-friendly; matches ScyllaDB/Dragonfly practice.
- Requires async-runtime rework (already contemplated in
  `.scratch/roadmap/optimizations/ASYNC_RUNTIME.md`).

### R3 — Topology: symmetric cores, strict shared-nothing, copy at boundary

Every core runs networking + storage for its slot range (Dragonfly/Scylla model).
Connections are pinned to a core; same-core commands are zero-hop. Cross-slot
commands hop via message with explicit ownership transfer or copy into the target
arena. No shared refcounts across cores — no foreign-thread frees, no cross-arena
bleed. Multi-key cross-slot ops pay a copy; that is the accepted trade.

### R4 — Runtime: tokio current-thread per core

One tokio current-thread runtime pinned per core. Preserves the turmoil-based
deterministic concurrency-test infrastructure, macOS dev, and ecosystem compat.
io_uring is a possible later backend behind an abstraction, but the design does not
pre-build for it (see R12).

### R5 — Keyspace: Dashtable-style segmented table with inline small values

Segmented extendible-hash table (cache-line-sized buckets, ~1 byte/entry directory
overhead, incremental splits). Entry slots are tagged 8-byte words: tiny
strings/ints inline in the bucket; larger values as arena pointers. Per-segment
metadata hosts eviction bits (R9) and rehash state. SCAN cursors map to segment ids
and stay stable across splits (Redis-compat hard requirement). This is an unsafe
core structure: budget miri + fuzz coverage from day one.

### R6 — Value ownership: inline small; same-core refcount + COW for heap values

Small values have no header at all (inline in slot). Heap values use a non-atomic
(same-core-only) refcount with copy-on-write. Snapshot = clone the handles (O(keys)
pointer work), then stream lazily. This is the in-process substitute for Redis's
fork-COW and is the architectural fix for LiveDataset fullsync's double-RAM
materialization: export becomes a bounded-memory stream from a handle snapshot;
writes during the sync window pay COW.

### R7 — Composite encodings: contiguous block forms everywhere

Every type gets a contiguous small form and a chained-block large form:

- Lists: quicklist-style chains of listpack blocks (kills per-element `Bytes`).
- Hash/set: block-packed beyond the small threshold.
- Zset: keep arena skiplist; member bytes move into block storage.
- Streams: packed segments.
- JSON: compact tape/arena format replacing the serde_json tree.

Element data never gets its own allocation. Accept memmove-on-edit inside blocks
(Redis-proven trade). This plus R2/R11 is the fragmentation-prevention layer.

### R8 — Accounting: per-core memory broker + subsystem budgets

Each core runs a memory broker. Arena stats are ground truth for maxmemory. Every
non-keyspace subsystem (network output, replication ring, tracking, WAL channel,
fullsync staging, txn buffering) holds a Budget handle and charges growth against
it; a structure that cannot charge cannot grow — it backpressures or sheds at the
seam. Per-key estimates remain only for eviction attribution and `MEMORY USAGE`
compat. Operators get per-subsystem memory breakdowns. This kills the issue-66 bug
class by construction.

### R9 — Eviction: segment-integrated 2Q

2Q LFRU driven by per-segment metadata: eviction walks victim segments, no per-key
LRU/clock fields, O(1) candidate selection. Tiered spill (warm tier / RocksDB)
remains a policy variant — victim spills instead of deletes. Redis sampling
policies (`allkeys-lru` etc.) become compat aliases mapping onto the 2Q machinery.

### R10 — RocksDB: jemalloc-linked and budget-charged

Build RocksDB against jemalloc so both worlds share one allocator universe and show
up in the same stats. One WriteBufferManager + strict-capacity shared block cache,
sized from the persistence subsystem's Budget. RocksDB memory becomes visible,
capped, and part of maxmemory math.

### R11 — Network buffers: per-core pools, budget-charged output

Connection read/write buffers are leased from a per-core size-classed pool and
returned/shrunk when idle (fixes never-shrink growth). All output buffering charges
the network Budget, with Redis-style client-output-buffer-limit classes
(normal/replica/pubsub) enforced at one seam. RESP2 out-bytes become visible to
client accounting.

### R12 — Parse path: zero-copy same-core, copy on hop

Args are refcounted slices of the leased read buffer; the buffer chunk returns to
the pool when the last arg drops. Cross-slot hops copy args into the target core's
arena (required by R3 anyway). Blocking commands copy out their args when parking so
they never pin read buffers. Eliminates today's per-request memcpy for the common
case.

### R13 — Fragmentation: prevention + purge, no active defrag

Rely on R2/R7/R11 to prevent fragmentation. Tune arena decay; implement
`MEMORY PURGE` for real; expose per-arena fragmentation metrics from mallctl in
INFO/telemetry (replacing the stubbed fields). Escape hatch for residual cases:
per-key re-encode (rewrite a value into fresh blocks), not an allocator-integrated
defrag pass. Ship nothing speculative.

### R14 — Transactions: budget-charged with hard cap

Txn buffering (queued MULTI commands, staged WriteBatch, replica-side pending
groups) charges its Budget; a configurable hard cap (default generous, e.g. 512MB)
aborts the transaction with a clear error before EXEC applies anything. Documented
deviation from Redis (which has no limit) — atomicity preserved via
abort-before-apply. Replaces FM-PERSISTENCE-001's open-ended non-guarantee and
generalizes the replica-txn caps that already exist.

### R15 — Governance: locked spec + budget seam lint

When implementation starts, memory becomes the fifth locked area:

- `specs/memory.md` (Status: LOCKED) owning FM rows for the memory contract: every
  buffer bounded/budget-charged, OOM verdicts, eviction invariants, snapshot-COW
  guarantees, txn cap semantics.
- A seam lint enforcing "buffer growth goes through Budget".
- Mutation gate on the broker/table crates once they exist.

Spec skeleton and ADRs are deliberately NOT drafted now (would churn before design
details firm up); they are the first artifact of implementation kickoff.

## Known defects this architecture subsumes (fix earlier if implementation is far off)

These are live today and worth stand-alone fixes if the big build is distant:

1. `core/src/tracking.rs` — tracking-table `lru_order` unbounded outside accounting
   (issue 66; subsumed by R8).
2. `cluster-runtime feed_sequencer.rs` — barrier hold buffer missing its spec-ruled
   byte cap (TR-CLUSTER-016 / cluster issue 17; subsumed by R8).

Also worth noting: stale mimalloc comment in
`frogdb-server/crates/redis-regression/tests/memefficiency_tcl.rs` (~line 12) should
say jemalloc.

## Rough dependency order for eventual implementation

1. R2+R3+R4 (runtime/topology) — everything else assumes core-local ownership.
2. R8 broker skeleton + R10 RocksDB linkage (observability first: arena stats,
   real INFO fields) — measurable before/after for every later phase.
3. R5 table + R6 value ownership (+ miri/fuzz harness).
4. R9 eviction, R7 encodings (per-type, incremental).
5. R11 net pools + R12 parse path.
6. R13 purge/metrics polish, R14 txn caps, R15 spec lock.

## Open questions (not yet ruled)

- Exact inline-value threshold in table slots (needs slot-layout prototype).
- Cross-slot txn interaction between VLL lock tables and copy-at-boundary arg
  ownership (design detail for the R3 hop protocol).
- Whether replicas/AOF-style consumers get their own budget class or share the
  replication Budget.
- Per-type small→block promotion thresholds (default to Redis values initially).
- How turmoil tests model per-core pinning (simulation fidelity for R2/R3).
