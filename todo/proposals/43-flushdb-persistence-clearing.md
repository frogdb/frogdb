# 43 — FLUSHDB/FLUSHALL never clears persisted data (keys resurrect after restart)

**Status:** implemented (2026-07-12)
**Severity:** Critical (durability correctness — silent data resurrection)
**Found:** 2026-07-10, during proposal 42's final review (the reviewer could not trace a
persistence-clearing path for FLUSHDB; a dedicated investigation confirmed none exists).

## Problem

`FLUSHDB`/`FLUSHALL` clear only the in-memory store. With persistence enabled, every
previously-persisted key survives in RocksDB and is restored on restart — a flush followed by a
restart silently resurrects the entire pre-flush keyspace.

### Evidence (verified 2026-07-10, branch tip `615aa95a`)

Call chain:

1. `frogdb-server/crates/server/src/connection/dispatch.rs:46-47` routes FLUSHDB/FLUSHALL to
   `handle_flushdb`/`handle_flushall`.
2. `frogdb-server/crates/server/src/connection/handlers/scatter.rs:321-347` scatter-gathers
   `ScatterOp::FlushDb` to every shard.
3. `frogdb-server/crates/core/src/shard/execution.rs:804-835` (`scatter_flushdb`) calls
   `self.store.clear()` (in-memory) then routes a synthetic FLUSHDB write record through
   `run_scatter_effects`.
4. `frogdb-server/crates/core/src/store/hashmap.rs:828-846` (`HashMapStore::clear`) empties the
   hashmap and, **only for currently-warm entries**, calls `warm_tier.delete(key)` per key — the
   sole RocksDB-touching effect, and it hits only the warm CF.
5. The FLUSHDB/FLUSHALL `CommandSpec`s use `WalStrategy::NoOp`
   (`frogdb-server/crates/server/src/commands/server.rs:56-70`, `:101-116`), which resolves to
   **zero** `WalAction`s (`frogdb-server/crates/core/src/command.rs` — the
   `WalStrategy::Dynamic | WalStrategy::NoOp => SmallVec::new()` arm). `persist_by_strategy`
   therefore never touches the primary CF.
6. Recovery (`frogdb-server/crates/persistence/src/recovery.rs:87-137`, `recover_shard_into`)
   iterates the primary CF unconditionally and restores every non-expired entry. No epoch,
   generation marker, or flush sentinel exists anywhere in `recovery.rs`, `rocks/mod.rs`, or
   `wal/*`.

The doc comment on `WalStrategy::NoOp` claims FLUSHDB is "handled by RocksDB clear"
(`frogdb-server/crates/core/src/command.rs:228`) — **no such mechanism exists**. The persistence
crate exposes no `delete_range`, `drop_cf`, CF recreation, or clear-style API at all.

### Test-coverage gap

No test performs flush → restart → assert-empty:

- `integration_persistence.rs` (the restart suite): zero FLUSHDB/FLUSHALL references.
- `integration_cluster.rs:9424-9509` (`test_cluster_flushdb_on_single_node`): in-process only.
- `core/tests/tiered_storage.rs:205-221`: asserts warm-CF cleanup only.

## Prior art

- **Redis/Valkey:** in-memory dicts; FLUSHDB propagates to AOF/replicas as a logical command, and
  the next RDB save reflects the empty state. `ASYNC`/lazyfree swaps the dict pointer and frees in
  the background. Not directly applicable (our durable store is RocksDB, not a rewritten log).
- **Kvrocks** (RocksDB-backed Redis): FLUSHDB issues RocksDB **`DeleteRange`** over the database's
  full key range in each affected column family, optionally followed by compaction to reclaim
  space. This is the established pattern for bulk-clearing a CF without disturbing readers,
  iterators, checkpoints, or the write pipeline.
- **DragonflyDB:** in-memory tables; snapshot-based persistence — flush rewrites state wholesale.
  Not applicable.

## Design

Route the clear **through the existing WAL flush pipeline** so it holds the same single-writer,
seq-ordered guarantees as every Put/Delete/Merge (the invariant proposal 42 relies on), and apply
it in RocksDB as a full-range `DeleteRange` on the shard's CFs.

### 1. New WAL action + strategy plumbing

- Add `WalAction::ClearShard` (no payload) and have FLUSHDB/FLUSHALL specs declare a new
  `WalStrategy::ClearShard` instead of `NoOp`. (`Dynamic` is not appropriate: the clear is
  unconditional.) Fix the now-false doc comment on `NoOp` at `core/src/command.rs:228` in the same
  change.
- `execute_wal_action` (`core/src/shard/persistence.rs`) maps it to a new `WalWriter::write_clear`
  that enqueues `WalEntry::Clear` with the next seq (same `fetch_add` discipline as
  `write_set`/`write_merge`).

### 2. Flush-thread + sink support

- `WalEntry::Clear` in `persistence/src/wal/flush.rs`; `WriteSink` gains `stage_clear(shard_id)`.
- `RocksSink` implements it as `WriteBatch::delete_range_cf` over the **full key range** of the
  shard's primary CF **and its warm CF**. RocksDB's `delete_range` end bound is exclusive, so use
  `(begin = [], end = [0xFF; MAX_KEY_LEN + 1])` or, more robustly, `delete_range_cf` to a
  sentinel upper bound plus a point `delete_cf` of any key ≥ the sentinel found via a bounded
  reverse iterator — decide in implementation; Kvrocks uses the computed-max-key approach.
  Staying inside the `WriteBatch` keeps the clear atomic and ordered with neighbouring entries
  from the same flush drain.
- Because the flush thread is the only RocksDB writer per shard and entries are seq-ordered, a
  write accepted **after** the flush (client-visible ordering) necessarily lands **after** the
  DeleteRange — no resurrection and no loss of post-flush writes. This mirrors the delete-vs-merge
  ordering argument from proposal 42.

### 3. Warm-tier cleanup consolidation

`HashMapStore::clear`'s per-key synchronous `warm_tier.delete(key)` loop
(`store/hashmap.rs:834`) becomes redundant once the warm CF is covered by the DeleteRange —
remove it (it is also the slow path: O(warm keys) point deletes vs one range tombstone). Keep the
in-memory clear.

### 4. Space reclamation (optional, same PR or follow-up)

A range tombstone hides data but does not reclaim disk until compaction. Follow Kvrocks: after
staging the DeleteRange, optionally trigger `compact_range` on the affected CFs in the background.
Deferrable — correctness does not depend on it.

### Non-goals

- `FLUSHDB ASYNC` semantics (lazyfree) — FrogDB's in-memory clear is already synchronous per
  shard; async freeing is orthogonal.
- Multiple logical databases: FrogDB scatter-flushes every shard for both FLUSHDB and FLUSHALL
  (single logical DB), so both commands take the same path.
- Snapshot/Checkpoint interaction needs no work: checkpoints are file-level and see whatever
  sequence-consistent state includes (or excludes) the range tombstone.

## Tests

1. **Resurrection regression (the bug):** SET + dense PFADD (persisted via Put and Merge) →
   FLUSHDB → restart → `DBSIZE == 0`, `GET == nil`, `PFCOUNT == 0`. (integration_persistence.rs)
2. **Post-flush writes survive:** SET a → FLUSHDB → SET b → restart → only b present. Exercises
   the ordering guarantee.
3. **Warm-tier key flushed:** force demotion of a key to the warm CF → FLUSHDB → restart → gone
   (extends `tiered_storage.rs`).
4. **FLUSHALL parity:** same as (1) via FLUSHALL.
5. **Merge-operand interaction:** dense PFADD deltas outstanding → FLUSHDB → PFADD same key →
   restart → count reflects only post-flush adds (no stale base or operands fold in). Guards the
   proposal-42 delta-base invariant across a clear.
6. **Unit:** `WalEntry::Clear` ordering within a flush drain (mock sink, mirrors existing
   `round_trips_format_through_mock_sink` style); `WalStrategy::ClearShard` routing in
   `wal_actions()`.

## Open decisions

1. DeleteRange upper-bound technique (sentinel max-key vs iterate-and-point-delete) — implementer
   picks whichever the rocksdb crate expresses most safely.
2. Whether to fold the optional `compact_range` into the same change or file it as a follow-up.
3. Replication: FLUSHDB already broadcasts as a logical command to replicas (unchanged), but
   confirm the replica apply path routes through the same scatter seam so its persistence clears
   too.

## Implementation status (2026-07-12)

**Shipped** on `refactor/command-spec-single-source`, commits `57bcc568`..`96b251cc` (2 commits).
Implementer report: [`.superpowers/sdd/task-43-report.md`](../../.superpowers/sdd/task-43-report.md).

The clear routes through the WAL flush pipeline exactly as designed above: a new
`WalStrategy::ClearShard` → keyless `WalAction::ClearShard` → `WalEntry::Clear`, applied as a
full-range `delete_range_cf` on the shard's primary CF.

Open-decision resolutions (see report §"Open-decision resolutions"):

1. **DeleteRange upper bound** = the CF's max key + a `0x00` suffix (`range_delete_upper_bound`),
   made resurrection-safe by an **engine-level barrier flush** in `FlushEngine::apply_clear`: all
   lower-seq entries are committed before the bound is computed and the clear is committed as its
   own batch, so post-flush writes (higher seq) survive. No sentinel/iterate-and-point-delete
   fallback needed.
2. **`compact_range` space reclamation deferred** as a follow-up — the tombstone hides data
   immediately; reclamation is orthogonal to correctness (todo: compact affected CFs post-clear).
3. **Replica apply path verified spec-driven** — because the fix lives in the command spec, the
   replica picks up `ClearShard` through the same write-effect seam; no extra work.

**Deviation from the design:** the warm CF is cleared **synchronously on the shard thread**
(`WarmTier::clear_range` → `RocksStore::clear_tier_shard`) rather than through `RocksSink`. The
warm CF never rides the async WAL pipeline (demotions in `WarmTier::try_put` also write directly on
the shard thread — the existing single-writer invariant there), so a range tombstone on the shard
thread is correct and replaces the old O(warm keys) per-key delete loop.

All 6 proposal tests implemented (see report §"Tests"); task review approved.
