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
  *(Partially stale as of 56c9019d, 2026-08-31: real mallctl stats now feed
  `INFO memory` + Prometheus via `telemetry/src/jemalloc.rs`, `MEMORY PURGE` is real,
  and the mimalloc comment is fixed. Tuning/arenas still absent — that's this PRD.)*
- Accounting is hand-maintained per-value `memory_size()` estimates; eviction, OOM
  refusal, and health thresholds all key off the estimate. RSS is measured separately
  (sysinfo) for Prometheus/status but not INFO. RocksDB C++ memory (memtables
  64MB×4×shards×CF-tiers + 256MB block cache) is invisible to everything.
- Data plane: per-shard single-threaded tokio task owning `griddle::HashMap<Bytes,
  Entry>`; values are `Arc<Value>` with `make_mut` COW; `Bytes` everywhere; hand-rolled
  listpacks for small hash/set; lists are `VecDeque<Bytes>` (one heap alloc per
  element, despite a stale linked-list doc comment at `list.rs:11`); JSON is a full
  serde tree; zsets use an index-based skiplist (safe indices, not a raw-pointer
  arena) plus a selectable BTree score-index backend (`SCORE_INDEX_BACKEND`); streams
  are a `VecDeque<Bytes>` order list with a BTreeMap PEL.
- Network: tokio-util `Framed`, 8KB buffers that never shrink; RESP decode is zero-copy
  but `ParsedCommand::try_from` memcpys every arg; no output-buffer limit for normal
  clients; RESP2 write-buffer bytes invisible to client accounting.
  *(Partially stale since issue 05 landed: `Subsystem::NetworkOutput` and
  `Subsystem::TxnBuffering` budget classes exist, unwired — issues 18/21 wire them.)*
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

1. Replica-feed hold buffer missing its spec-ruled byte cap (TR-CLUSTER-016 — spec row
   says "byte cap not yet in code"; subsumed by R8). Deliberately deferred: campaign-31
   (`.scratch/cluster-correctness/campaign-31-decomposition/c31-06`) rewrites
   TR-CLUSTER-016 to ordinary replica-feed backpressure, and issue 31 is
   ready-for-human — implementing the cap against the current row risks throwaway work.

## Spike findings (2026-08-31)

Phase-1 spike ran — see [spike-report.md](spike-report.md). Verdicts: R2 GO
(byte-exact arena attribution, zero-cost binding), R3 GO (colocated thread-per-core
2.27× throughput / 7.5× p99 vs work-stealing in the microbench), R4 GO **only
together with R3** (current-thread runtimes without connection pinning measured 3.7×
*worse* than today), turmoil GO via a mandatory `ShardExecutor` seam (real-threads
impl vs sim impl; determinism verified). Amendments the report argues for, to fold in
when specs/memory.md is drafted: R4 must bundle connection→core pinning; R8 arena
stats are a sampled upper bound (epoch ~2.5µs/arena — sample at 10–100Hz, ride
`thread.allocated` per-request; bind arena once at thread start or tcache bleeds;
set `narenas:1`); R3's real cross-slot cost is the thread hop (~8×), not the copy;
the turmoil seam must exist from the first R2 commit and specs/memory.md must state
that sim tests logic, not execution shape.

## Rough dependency order for eventual implementation

1. R2+R3+R4 (runtime/topology) — everything else assumes core-local ownership.
2. R8 broker skeleton + R10 RocksDB linkage (observability first: arena stats,
   real INFO fields) — measurable before/after for every later phase.
3. R5 table + R6 value ownership (+ miri/fuzz harness).
4. R9 eviction, R7 encodings (per-type, incremental).
5. R11 net pools + R12 parse path.
6. R13 purge/metrics polish, R14 txn caps, R15 spec lock.

## Open questions (each owned by an issue — D5)

- Exact inline-value threshold in table slots → [issue 10](issues/) reports the ruling
  with numbers.
- Cross-slot txn interaction between VLL lock tables and copy-at-boundary arg
  ownership → ruled at issue-11 drafting (bites only once values are refcounted, the
  R6 heap half decided there).
- Per-type small→block promotion thresholds → Redis defaults; re-tuned only if issue
  13/14 benches say otherwise.
- Turmoil per-core-pinning fidelity → issue-11 test plan.

## Grill-dispatch decisions (2026-09-01)

Session decisions layered on the R1–R15 rulings above; D-numbers are cited by issues.

- D1: resume slug `memory-architecture` — drafts 10, 13–22 are the carve backlog; spike
  dispatches on approval; remaining open questions grilled while agents run.
- D2: landing flow — agents commit in own worktrees, session cherry-picks onto
  `mem-arch-integration`, gates, pushes `mem-arch-integration:main` directly. No PRs, no
  merge commits.
- D3: drafts 10, 13–22 bulk-approved as filed; promoted to `issues/open/`. Per-issue
  ambiguity resolved at dispatch time.
- D4: replica feed buffers account under `Subsystem::NetworkOutput` with issue 18's
  `replica` output-limit class (Redis semantics); backlog stays `ReplicationBacklog`, WAL
  stays `WalChannel`. Closes the replica/AOF budget-class open question — already split
  correctly, no new class.
- D5: remaining open questions assigned owners — inline threshold → issue 10 report;
  cross-slot txn/VLL and turmoil pinning fidelity → issue-11 drafting; promotion
  thresholds → Redis defaults unless 13/14 benches disagree. Interview design tree
  closed 2026-09-01.
- D6: `just lint-spec`'s `RUSTC_WRAPPER=""` sccache bypass (concurrent full recompiles
  starved sim suites → phantom nextest timeouts) gets investigate-and-fix, not a doc-only
  rule — filed as `.scratch/concurrency-testing/issues/19`.
- D7: follow-up round scope (2026-09-05) — in: drafts 26, 28, 29, 30, 31 + three unfiled items
  from the whole-branch re-review filed as 33 (feed_account test-double guard), 34 (quint
  resolution in agent shells), and worktree prune (session task, no issue). Deferred: 24 (XL,
  split per subsystem later), 25 (XL, own design interview), 27 (blocked on cluster
  TR-CLUSTER-016 rewrite). 32 stays ready-for-human pending the atomicity ruling.
