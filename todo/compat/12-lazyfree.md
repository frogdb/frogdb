# 12. Lazyfree & Async Deletion (7 tests)

**Source file**: `frogdb-server/crates/redis-regression/tests/lazyfree_tcl.rs`
**Status**: Partial — UNLINK exists (aliased to synchronous DEL); lazyfree counters are hard-coded to 0; blocking FLUSHALL ASYNC not implemented.

## Current Architecture

### UNLINK (synchronous)

`UnlinkCommand` in `frogdb-server/crates/commands/src/generic.rs` (line ~250) delegates to the same `scatter_del` path as DEL via `ScatterOp::Unlink`. The shard worker in `execution.rs` treats `ScatterOp::Del | ScatterOp::Unlink` identically — iterating keys, calling `self.store.delete(key)`, persisting WAL deletes, and invalidating client tracking.

### FLUSHALL / FLUSHDB (synchronous)

`FlushallCommand` in `frogdb-server/crates/server/src/commands/server.rs` parses the ASYNC/SYNC argument but ignores it. The handler `handle_flushall` in `scatter.rs` (line 426) delegates to `handle_flushdb`, which broadcasts `ScatterOp::FlushDb` to all shards and waits for each shard to call `self.store.clear()` synchronously.

### Store deletion internals

`HashMapStore::delete()` in `frogdb-server/crates/core/src/store/hashmap.rs` (line 492):
1. Removes entry from the in-memory `HashMap`.
2. Adjusts `memory_used` counter.
3. Deletes warm-tier RocksDB entry if key is warm (`warm_store.delete_warm`).
4. Removes from `expiry_index`, `label_index`, `field_expiry_index`.

`HashMapStore::clear()` (line 596):
1. Iterates warm keys and issues `delete_warm` to RocksDB per-key.
2. Clears the `HashMap`, all indexes, and resets `memory_used`.

### INFO memory counters

`info.rs` (line 269–270) hard-codes:
```
lazyfree_pending_objects:0
lazyfreed_objects:0
```

### CONFIG RESETSTAT

`handle_config_resetstat()` in `frogdb-server/crates/server/src/connection/handlers/config.rs` broadcasts `ShardMessage::ResetStats` to each shard. The shard worker calls `ShardObservability::reset_stats()` which clears latency, slowlog, peak memory, and `evicted_keys`. It does **not** reset any lazyfree counters (they don't exist yet).

### Stream metadata ("all types of metadata")

`StreamValue` in `frogdb-server/crates/types/src/types/stream.rs` (line 594) contains:
- `entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>` — the log entries
- `groups: BTreeMap<Bytes, ConsumerGroup>` — consumer groups, each with PEL and consumers
- `idempotency: Option<Box<IdempotencyState>>` — ES.APPEND dedup state
- `last_id`, `first_id`, `max_deleted_id`, `entries_added`, `total_appended` — metadata counters

The Redis tests "lazy free a stream with all types of metadata" and "lazy free a stream with deleted cgroup" verify that UNLINK of a stream with consumer groups and PEL entries correctly reports `lazyfreed_objects` incrementing.

## Implementation Plan

### Step 1: Add lazyfree counters to ShardObservability

**File**: `frogdb-server/crates/core/src/shard/types.rs`

Add two fields to `ShardObservability`:
```rust
pub lazyfreed_objects: u64,       // total objects freed via UNLINK/FLUSHALL ASYNC
pub lazyfree_pending_objects: u64, // objects queued but not yet freed
```

Update `reset_stats()` to zero `lazyfreed_objects` (matching Redis behavior where CONFIG RESETSTAT clears the total counter). `lazyfree_pending_objects` is a gauge and should NOT be reset.

### Step 2: Add counter fields to ShardMemoryStats

**File**: `frogdb-server/crates/core/src/shard/types.rs` (around line 322)

Add `lazyfreed_objects: u64` and `lazyfree_pending_objects: u64` to `ShardMemoryStats` so they can be aggregated in the INFO handler.

### Step 3: Increment counters in UNLINK and FLUSHDB/FLUSHALL paths

**File**: `frogdb-server/crates/core/src/shard/execution.rs`

In `scatter_del` (called for both DEL and UNLINK): when the operation comes from `ScatterOp::Unlink`, increment `self.observability.lazyfreed_objects` for each successfully deleted key. This is consistent with Redis semantics where UNLINK always reports objects as "lazyfreed" even though the actual free may happen inline for small objects.

In `scatter_flushdb`: count the number of keys before `self.store.clear()` and add that count to `self.observability.lazyfreed_objects`. This is the simplest model — all objects freed via flush count as lazyfreed.

### Step 4: Wire counters into INFO memory section

**File**: `frogdb-server/crates/server/src/commands/info.rs` (around line 269)

Replace the hard-coded `0` values with actual counter values from `CommandContext`. This requires:
1. Adding lazyfree counters to the `CommandContext` struct (or passing them through `ShardMemoryStats`).
2. In the server-level INFO handler (`scatter.rs`), aggregate `lazyfreed_objects` across shards.
3. For `lazyfree_pending_objects`: since FrogDB's deletion is currently synchronous, this will always be 0 until true async deletion is implemented. Report the aggregated gauge anyway to support future work.

### Step 5: Implement blocking FLUSHALL ASYNC

This is the major behavioral change. In Redis 8, standalone `FLUSHALL SYNC` is transparently optimized to run as a "blocking FLUSHALL ASYNC" — it blocks the calling client (but not the event loop) until deletion completes.

**Design**: Since FrogDB already executes FLUSHALL synchronously in the shard event loop (blocking the shard), the optimization target is different. The key behavioral requirements from the tests are:

1. **Single client blocking**: The calling client is "blocked" (cannot process subsequent pipelined commands) until all shards confirm flush completion.
2. **Counter increment**: `lazyfreed_objects` must reflect the total keys freed.
3. **MULTI exception**: FLUSHALL inside MULTI/EXEC must NOT optimize to blocking-async — it must run as part of the transaction normally.
4. **Client disconnect**: If the client disconnects mid-flush, the flush still completes (fire-and-forget from the server's perspective).
5. **Pipelined commands**: Commands queued in the client's read buffer after FLUSHALL must only execute after the flush completes.

**Implementation approach**:

**File**: `frogdb-server/crates/server/src/connection/handlers/scatter.rs`

Modify `handle_flushall`:
- Add a parameter or context flag indicating whether the call is inside MULTI.
- If inside MULTI with explicit SYNC: keep current synchronous behavior.
- If standalone (not in MULTI): treat as blocking flush — broadcast to all shards, await completion, then sum the key counts returned by each shard into `lazyfreed_objects`.
- Handle client disconnect by wrapping the await in `tokio::select!` with the connection's shutdown signal — if the client disconnects, the flush tasks continue to completion but the response is discarded.

**File**: `frogdb-server/crates/core/src/shard/execution.rs`

Modify `scatter_flushdb` to return the count of keys that were cleared:
```rust
fn scatter_flushdb(&mut self) -> Vec<(Bytes, Response)> {
    let key_count = self.store.len() as i64;
    // ... existing clear logic ...
    self.observability.lazyfreed_objects += key_count as u64;
    vec![(Bytes::from_static(b"__flushdb__"), Response::Integer(key_count))]
}
```

### Step 6: CONFIG RESETSTAT clears lazyfreed_objects

**File**: `frogdb-server/crates/core/src/shard/types.rs`

Already addressed in Step 1 — `reset_stats()` zeros `lazyfreed_objects`.

### Step 7: Consecutive blocking FLUSHALL ASYNC

No special implementation needed beyond Step 5. Each invocation independently broadcasts to all shards, awaits, and increments counters. The test verifies that multiple consecutive calls each observe `lazyfreed_objects` incrementing correctly.

## Integration Points

| Trigger | Location | Counter Effect |
|---------|----------|----------------|
| UNLINK key1 key2 ... | `scatter_del` (Unlink variant) | `lazyfreed_objects += deleted_count` |
| FLUSHALL [ASYNC\|SYNC] | `scatter_flushdb` via `handle_flushall` | `lazyfreed_objects += key_count_per_shard` |
| FLUSHDB [ASYNC\|SYNC] | `scatter_flushdb` via `handle_flushdb` | `lazyfreed_objects += key_count_per_shard` |
| CONFIG RESETSTAT | `ShardObservability::reset_stats` | `lazyfreed_objects = 0` |
| INFO memory | `build_memory_info` aggregation | reads sum of all shards' `lazyfreed_objects` |

## FrogDB Adaptations (RocksDB Interaction)

FrogDB does not have Redis's background thread pool for "lazy free." The semantic difference:

- **Redis**: UNLINK enqueues the object's deallocation on a bio thread. The counter `lazyfree_pending_objects` reflects the queue depth and `lazyfreed_objects` increments when the bio thread completes.
- **FrogDB**: Deletion is synchronous within the shard worker. The `delete()` call removes the HashMap entry (O(1) amortized) and issues a RocksDB tombstone for warm keys. RocksDB tombstone cleanup happens asynchronously via compaction — but this is invisible to the counters.

**Practical consequences**:
- `lazyfree_pending_objects` will always be 0 in FrogDB (deletion is synchronous from the shard's perspective). This is compatible — Redis also reports 0 when the bio queue drains instantly.
- `lazyfreed_objects` increments synchronously at deletion time. From a test perspective, this is indistinguishable from Redis's behavior since the test polls `INFO` after the command returns.
- For `clear()`: RocksDB `delete_warm` calls issue tombstones per-key. In the future, this could be optimized with `DeleteRange` or by dropping and recreating the column family — but this is a performance optimization, not a correctness requirement for the lazyfree tests.

## Tests

- `lazy free a stream with all types of metadata`
- `lazy free a stream with deleted cgroup`
- `FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC`
- `Run consecutive blocking FLUSHALL ASYNC successfully`
- `FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC`
- `Client closed in the middle of blocking FLUSHALL ASYNC`
- `Pending commands in querybuf processed once unblocking FLUSHALL ASYNC`

## Verification

1. `just test frogdb-server lazyfree` — run the lazyfree regression test file
2. Manually verify via `redis-cli`:
   - `SET foo bar && UNLINK foo` then `INFO memory | grep lazyfreed_objects` shows 1
   - `CONFIG RESETSTAT` then `INFO memory | grep lazyfreed_objects` shows 0
   - `FLUSHALL` on a populated database shows `lazyfreed_objects` equal to prior DBSIZE
3. Verify MULTI exception: `MULTI; FLUSHALL SYNC; EXEC` should NOT optimize to async path
4. Verify pipelined commands execute after flush completes (send FLUSHALL + GET in pipeline)
