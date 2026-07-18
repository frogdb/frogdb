# Concurrency Invariant Testing — Design

**Date:** 2026-07-17
**Status:** Approved
**Scope:** Single-shard interleaving races and cross-shard atomicity. Durability/crash,
replication, and cluster-operation testing are explicitly future phases — the architecture
here must accommodate them, but this design does not implement them.

## Motivation

FrogDB's hardest bugs live in concurrency edge cases: MULTI/EXEC vs concurrent writes,
blocking-command wake/timeout races, expiry interacting with waiters and transactions, and
VLL/scatter-gather partial failures. Recent codebase-improvement proposals
(`todo/proposals/`) surfaced 20+ concrete bugs in exactly these areas, including:

- **Lost-element BLPOP timeout race** — dual timeout authorities could pop a list element
  into an abandoned oneshot after the client already received a timeout nil
  (proposal 12).
- **Cross-shard keyspace events never delivered** — SUBSCRIBE registers on shard 0 but
  notifications published into the key-owner shard's table (proposal 22, confirmed).
- **Stale expiry index deleting a persistent key** — `Store::set` (MSET path) never
  cleared `expiry_index` on overwrite (proposal 30, confirmed).
- **VLL abort-by-vector-position lock leak** — phase failure with sparse participants
  aborted the wrong shards, leaking write intents permanently (proposal 37, confirmed).
- **Silent wrong-shard write from Lua** — `block_in_place` panic fallback executed a
  remote-owned key locally (proposal 28, confirmed).

### Current test-infrastructure gaps

| Layer | State | Gap |
|---|---|---|
| Shuttle (`core/tests/concurrency.rs`, 47 tests) | Models MULTI/EXEC, WATCH, snapshots, XREAD | Tests **hand-rolled mocks** (`TestCluster`), not production shard/dispatch code |
| Turmoil (`server/tests/simulation.rs`, ~50 tests) | Real server, simulated net, real WGL linearizability checker (`crates/testing`) | All scenarios hand-written; no blocking commands, no generated workloads; persistence disabled |
| redis-regression (~2,246 tests) | Static Rust port of Redis 8.6 TCL suite | Sequential semantics only |
| Jepsen (28 workloads) | Real kills/partitions | Heavy, slow, not per-PR |
| proptest (179) / fuzz (30 targets) | Parsing/single-op | Zero concurrency properties |

Structural problems: (1) shuttle proves a mock model correct, not the implementation;
(2) the linearizability checker exists but nothing generates op permutations to feed it;
(3) VLL, scripting, blocking (BLPOP/WAIT), and the wait queue have zero shuttle/turmoil
coverage.

## Goals

1. Generated-workload testing: many permutations of operations against the **real server
   code**, checked against a model oracle plus explicit invariants.
2. Deterministic, seed-reproducible failures (`seed → exact repro`).
3. Fix the confirmed bugs the harness trips over; each fix pinned by a named regression
   test carrying its seed.
4. Per-PR fast tier + nightly deep tier.

## Non-goals (future phases)

- Durability / crash-window testing (wake-before-WAL, fsync, recovery).
- Replication correctness (WAIT quorum, offset drift, full-sync gap).
- Cluster operations (slot migration, failover, Raft).
- Real-kernel networking, real-time jitter, multi-process chaos (remains Jepsen's job).

## Architecture

Four components. Rejected alternative: porting production code onto shuttle-swappable
primitives — the architecture is message-passing with single-task shard ownership and
near-zero shared-memory locking, so shuttle's thread/lock interleaving exploration has
little to bite on. The existing mock-based shuttle suite stays as cheap semantic
documentation but is not extended.

### 1. Oracle library (extend `frogdb-server/crates/testing`)

- **Models** for lists, hashes, zsets, strings, streams; MULTI/EXEC/WATCH semantics;
  blocking-op semantics (see Invariants below).
- **History format**: serializable `History` of
  `(client, node, op, invoke_ts, complete_ts, result)`. Transport-agnostic — no turmoil
  types in this crate. The same checkers must later ingest histories from Jepsen runs,
  real-network tests, and replication tests (node field exists now for that reason).
- **Checkers**: existing WGL linearizability checker + conservation checkers +
  quiescence checkers.

### 2. Workload generator (same crate)

- Seeded RNG → `Workload { seed, clients: Vec<ClientScript>, profile }`; each client
  script is a sequence of ops with sim-time think-time hints.
- Ops emitted at **RESP command level**, not internal types — identical workloads
  replayable later against real clusters and replicated deployments.
- Profiles: tx-heavy, blocking-heavy, mixed; each controls op mix and
  same-shard/cross-shard ratio.
- **Key-space discipline**: ~8–16 keys per run, hash-tagged to pin shard placement.
  Small key space → high contention → races surface; also bounds checker cost.

Op vocabulary (v1):

- Strings: SET/GET/DEL/INCR/GETSET, EXPIRE/PEXPIRE with short TTLs (drives expiry races)
- Lists: LPUSH/RPUSH/LPOP/RPOP/LMOVE + BLPOP/BRPOP/BLMOVE/BLMPOP, mixed timeouts
  including 0 (infinite)
- Hashes: HSET/HDEL/HINCRBY/HGETEX/HSETEX (field TTLs → field-expiry races)
- ZSets: ZADD/ZREM/BZPOPMIN/BZPOPMAX
- Streams: XADD/XREAD BLOCK/XREADGROUP BLOCK (expiry-vs-NOGROUP gap)
- Transactions: MULTI…EXEC/DISCARD, WATCH+CAS patterns, blocking-op-inside-MULTI
  (must degrade to non-blocking)
- Admin/chaos ops: CLIENT UNBLOCK, CLIENT PAUSE WRITE, RESET, FLUSHDB, cross-shard
  MGET/MSET/DEL, cross-shard EVAL (exercises continuation locks)

### 3. Turmoil harness v2 (`frogdb-server/crates/server/tests/`)

- Real server under turmoil, N concurrent sim clients executing generated workloads,
  full history recording, seed → exact schedule repro.
- Quiescence checks run through the new `DEBUG` introspection commands (below), not
  test-only hooks — deliberately, so the same probes work against real processes in the
  Jepsen/replication phases.
- Sim config gains `persistence.mode = fake` (see persistence seam) replacing today's
  `enabled: false`.

### 4. Shard-driver harness (`frogdb-server/crates/core/tests/`)

- Drives a real `ShardWorker` (and a real `VllCoordinator`) directly with controlled
  `ShardMessage` ordering; proptest-shrinkable permutations for races too narrow for
  schedule sampling to hit reliably.
- Bypasses the connection layer by design (that is the turmoil harness's job).
  Known risk: hand-crafted message orders can represent interleavings the real
  coordinator never produces. Mitigations: derive permutation constraints from real
  message-emission points; verify any shard-driver failure against the turmoil/real path
  before fixing.

### Persistence seam

Deterministic in-process **persistence fake** implementing the existing sink interface:

- Records an `(effect, order)` log → turmoil tests assert `WRITE_EFFECT_ORDER`
  compliance. Wake-before-WAL-persist becomes *observable* now; changing that ordering
  is deferred to the durability phase, but the currently documented order is asserted.
- Failure injection by op-index or predicate → exercises rollback-mode snapshots-restore
  and persist-failure branches (`EXECABORT` shape) without real RocksDB.
- Future phases swap in: crash-injecting fake (durability), real RocksDB (Jepsen).
  RocksDB stays out of turmoil — its background threads live outside the deterministic
  scheduler.

## Invariants

Tiered by cost; all run per generated-workload execution.

1. **Response legality** (always, cheap): every reply type-legal for its command and RESP
   version; nil shapes correct on every blocking timeout path (`*-1` vs `$-1`).
2. **Linearizability** (per-key partition): single-key ops checked with WGL against the
   per-type model. Multi-key atomic ops (MSET, EXEC) enter each touched key's history as
   atomic sub-ops sharing one invoke/complete window — checks cross-key atomic
   visibility without whole-history state-space blowup.
3. **Conservation** (whole history):
   - Every pushed element consumed **exactly once**: delivered to exactly one popper XOR
     present at quiesce. Catches lost-element and double-delivery races.
   - Transaction transfer workloads: sum conserved (Jepsen bank model, ported to the
     fast harness).
   - WATCH no-false-negative: if the watched key's model state changed between WATCH and
     EXEC due to another client, EXEC must abort.
4. **Quiescence** (after workload drains and expiry settles, via DEBUG probes):
   - VLL lock table empty (no leaked intents or continuation locks)
   - Wait queue empty (no orphaned waiters after disconnects/timeouts)
   - `memory_used` matches recomputed live size
   - Expiry index has no entry pointing at a persistent or deleted key
   - All connections responsive

**Checker scaling guards**: history capped at ~200 ops per key; on WGL timeout, that key
downgrades to weaker checks (sequential-consistency spot checks + conservation) with a
logged warning — never a silent pass.

**Blocking ops in the model**: invoke-window operations. Legal outcomes = { an element
that was head-of-list at some linearization point inside the window, or timeout-nil if
some legal linearization leaves the list empty for the full window }. FIFO wake fairness
asserted separately per key (waiters served in registration order — pinned FrogDB
behavior, matches Redis).

**Pinned divergences** (modeled as FrogDB spec, marked `// DIVERGENCE:` with doc links,
never reported as failures):

- WATCH uses a per-shard version, not per-key — over-aborts on unrelated same-shard
  writes and expiry sweeps are legal. Zero false negatives is the asserted invariant;
  false-positive rate is characterized (logged), not asserted.
- `DEBUG SLEEP` sleeps only the calling connection's task (non-global, unlike Redis).
- `CLIENT PAUSE` is cooperative per-connection polling, not a global stop.

## DEBUG introspection commands

Production code, always available (Redis-style DEBUG). Each is one `ShardMessage`
handled inside the shard event loop — a naturally consistent per-shard snapshot.
Structured RESP map replies.

- `DEBUG LOCKTABLE` — per-shard VLL intents, grants, continuation locks
- `DEBUG WAITQUEUE` — waiters by key / kind / connection
- `DEBUG MEMORY-CHECK` — recompute live size, report diff vs `memory_used`
- `DEBUG EXPIRY-INDEX-CHECK` — index entries cross-checked against entry deadlines

## Targeted shard-driver scenarios

Each a named test, proptest-permuted around its critical window:

1. **Dual-timeout race**: LPUSH arrives in the same tick as the waiter deadline / after
   the coordinator timeout but before `UnregisterWait` → element neither lost nor
   double-delivered.
2. **WATCH vs expiry vs unrelated write**: no false negative; over-abort characterized.
3. **VLL phase-2/3 failure with sparse participants** (e.g. shards `[2,5,7]`): all real
   shard ids aborted; lock table empty afterward.
4. **Continuation-lock holder panics**: guard drop releases the lock; shard resumes.
5. **XREADGROUP blocked + key dies via TTL vs explicit DEL**: pin both paths; fix the
   NOGROUP-on-expiry gap (expiry currently never drains stream waiters).
6. **Persist-failure mid-transaction in rollback mode**: snapshots restored in reverse
   order; `EXECABORT` response shape.
7. **CLIENT PAUSE WRITE vs in-flight EXEC**: no write-containing EXEC slips past the
   pause check.
8. **Expiry sweep interleaved with EXEC on the same shard**: serialization holds;
   notifications and version consistent.

## Bug workflow

Failing seed → auto-emit repro file (seed + profile + config) →
`just concurrency-repro <file>` → fix → pin as a named regression test with the seed
hardcoded.

Confirmed proposal bugs expected to trip on day 1 are fixed **in-plan** (decision:
tests + fix trips): lost-element race hardening, XREADGROUP NOGROUP-on-expiry,
cross-shard keyspace delivery (verify current state first — proposal 22 work may have
landed fixes already).

## CI

- **Per-PR**: `just concurrency` extended to include ~20 seeds × short workloads
  (~2–3 min budget), part of `just test-all`.
- **Nightly**: cron + `workflow_dispatch` workflow (generated via `workflow_gen/`),
  1000+ seeds, long histories, all profiles; artifacts = failing repro files.

## Harness self-tests

Seeded fault-injection mode: a deliberately broken model/shim (drop an element,
duplicate a delivery, reorder effects) must be caught by the checkers. Guards against
silent-green checker bugs.

## Known blind spots (accepted)

- Persistence real path (RocksDB, FrogDB WAL) — fake only; durability phase later.
- Kernel TCP, wall-clock jitter, OS scheduling — turmoil simulates all three.
- Schedule coverage is sampling, not exhaustive model checking.
- Replication/cluster/Raft not booted in sim (standalone only).
- Multi-process chaos, kill -9, fsync — remains Jepsen.

## Phasing

Each phase lands independently:

1. Oracle library: models + history format + checker extensions + self-tests.
2. DEBUG introspection commands + quiescence checkers.
3. Turmoil harness v2 + workload generator + persistence fake; first seed sweeps;
   fix what trips.
4. Shard-driver harness + 8 targeted scenarios; fix what trips.
5. CI wiring (per-PR + nightly).

Future phases (accommodated, not built): durability/crash-injection via the persistence
seam; replication histories via the node-aware history format; cluster-op workloads via
the RESP-level generator and DEBUG probes.

## References

- Implementation map sources: `frogdb-server/CONTEXT.md`,
  `website/src/content/docs/architecture/{concurrency,execution,vll,blocking,consistency}.md`
- Known bugs: `todo/proposals/` — 12, 22, 28, 30, 37, 39, 03, 07, 10, 14, 18, 33
- Existing checker: `frogdb-server/crates/testing/src/{checker,history,models}.rs`
- Existing harnesses: `frogdb-server/crates/server/tests/simulation.rs`,
  `frogdb-server/crates/core/tests/concurrency.rs`
