# FrogDB Persistence Internals

Contributor-facing documentation for FrogDB's persistence architecture: RocksDB topology, WAL design, key-value schema, and snapshot internals.

For operator-facing configuration and recovery procedures, see [../operators/persistence.md](../operators/persistence.md).

---

## RocksDB Topology

FrogDB uses a **single shared RocksDB instance** with one column family per shard:

```
+----------------------------------------------------------+
|                    RocksDB Instance                       |
+----------------------------------------------------------+
|  +-----------+ +-----------+ +-----------+ +---------+   |
|  |  CF: s0   | |  CF: s1   | |  CF: s2   | | CF: sN  |  |
|  | (Shard 0) | | (Shard 1) | | (Shard 2) | |(Shard N)|  |
|  +-----------+ +-----------+ +-----------+ +---------+   |
|                                                           |
|  +---------------------------------------------------+   |
|  |              Shared WAL                            |   |
|  +---------------------------------------------------+   |
+----------------------------------------------------------+
```

### Design Rationale

**Benefits:**
- Single backup/restore operation for entire database
- Shared WAL simplifies recovery
- Atomic cross-shard operations possible via WriteBatch

**Trade-off:**
- Potential lock contention on WAL writes (mitigated by batching)

---

## Key-Value Schema

Each column family (shard) stores keys with this format:

**Key Format:**
```
[user_key_bytes]
```

**Value Format:**
```
+---------------------------------------------------------+
| Header (fixed 24 bytes)                                  |
+---------------------------------------------------------+
| type: u8           | Value type (0=String, 1=List, etc.) |
| flags: u8          | Reserved for future use              |
| expires_at: i64    | Unix timestamp ms (0 = no expiry)    |
| lfu_counter: u8    | LFU access counter                   |
| padding: [u8; 5]   | Alignment padding                    |
| value_len: u64     | Length of value data                  |
+---------------------------------------------------------+
| Value Data (variable)                                    |
+---------------------------------------------------------+
```

**Type Encoding:**

| Type | Code | Serialization |
|------|------|---------------|
| String | 0 | Raw bytes |
| List | 1 | `[len:u32][elem1_len:u32][elem1]...` |
| Set | 2 | `[len:u32][member1_len:u32][member1]...` |
| Hash | 3 | `[len:u32][k1_len:u32][k1][v1_len:u32][v1]...` |
| SortedSet | 4 | `[len:u32][score:f64][member_len:u32][member]...` |
| Stream | 5 | Full entry + consumer group state |
| HyperLogLog | 6 | Sparse/dense encoding |
| JSON | 7 | UTF-8 encoded JSON string (via `serde_json`) |
| Bloom | 8 | `[num_bits:u64][num_hashes:u8][bits...]` |
| TimeSeries | 9 | `[len:u32][timestamp:i64][value:f64]...` |
| Geo | 10 | Stored as SortedSet with geohash scores |

**Byte Order:** All multi-byte integers are stored in **little-endian** format.

### Metadata Persistence

- `lfu_counter` persisted with value
- `last_access` (LRU) **NOT persisted** -- reset to recovery time on startup

After recovery, all keys appear "fresh" for LRU purposes (idle time = 0). This matches Redis behavior. Eviction accuracy self-corrects within minutes as keys are accessed during normal operation.

**Expiry Index:** NOT persisted separately. Rebuilt during recovery from `expires_at` field in each value. Active expiry index is in-memory only.

**Recovery Conversion:** Unix timestamps (persisted as `i64` milliseconds) are converted to `std::time::Instant` (monotonic clock) during recovery.

---

## Write-Ahead Log (WAL)

Every write operation is appended to RocksDB's WAL before acknowledgment:

```
Client Write (SET key value)
         |
         v
    Shard Worker
         |
         +-- 1. Apply to in-memory store
         |
         +-- 2. Append to WAL (async batch)
         |      +-- RocksDB WriteBatch
         |
         +-- 3. Return OK to client
```

### WAL Failure Handling

| Failure Point | In-Memory State | Client Response | Recovery |
|---------------|-----------------|-----------------|----------|
| Before in-memory apply | Unchanged | Error returned | None needed |
| After in-memory, WAL fails | **Write visible** | Error returned | May be lost on restart |
| After WAL, before fsync (Async) | Write visible | OK returned | May be lost on crash |
| After fsync (Sync) | Write visible | OK returned | Guaranteed durable |

**Design rationale:** In-memory is the source of truth during operation (for low latency). WAL provides durability, not correctness during normal operation. This matches Redis AOF semantics. The alternative (rollback on WAL failure) would require undo logs and complex rollback logic with significant performance overhead.

### WAL Failure Policy

FrogDB supports a configurable `wal_failure_policy`:

| Policy | Behavior | Default |
|--------|----------|---------|
| `continue` | Log error, return success (Redis/DragonflyDB semantics) | Yes |
| `rollback` | Undo in-memory state, return `IOERR` to client | No |

**Rollback mode details:**
- Before executing a write, affected keys' current state is snapshotted (cheap `Arc<Value>` clones)
- If WAL fails: snapshot is restored, `IOERR` returned
- Single-shard write commands only; scatter-gather always uses `continue`
- Performance impact: `flush_async()` forces synchronous disk I/O per command (~0.1-2ms vs ~1-10us)

### WAL Corruption Recovery

| Corruption Type | Detection | Default Recovery |
|-----------------|-----------|------------------|
| **Truncated entry** | Entry length exceeds remaining file bytes | Truncate WAL at corruption point |
| **Checksum mismatch** | CRC32 of entry data doesn't match header | Truncate WAL at corruption point |
| **Invalid type marker** | Unknown operation type byte | Truncate WAL at corruption point |
| **Sequence gap** | Expected sequence N, found N+k | Policy-dependent |

**Why truncation is the default:** Crashes during write leave partial entries at WAL end. Truncation is safe, snapshots provide fallback, and this matches Redis AOF behavior.

---

## Durability Modes

| Mode | Durability | Latency |
|------|------------|---------|
| `Async` | Best-effort (may lose data) | ~1-10 us |
| `Periodic(1000ms)` | Bounded loss (~1s, matches Redis `appendfsync everysec`) | ~1-10 us |
| `Sync` | Guaranteed (fsync per write) | ~100-500 us |

### Periodic Mode Timer Semantics

The `Periodic` mode uses a **wall-clock timer** that fires on a fixed schedule (not reset-on-write). If previous fsync is still in progress when timer fires, that interval is skipped. This matches Redis `appendfsync everysec` behavior.

### Write Visibility

In `Async` and `Periodic` modes, writes are visible to other clients BEFORE they are durably persisted. This is by design and matches Redis behavior.

---

## Forkless Snapshot Algorithm

FrogDB uses epoch-based forkless snapshots instead of Redis's fork-based approach:

1. Snapshot begins: current epoch is recorded
2. All shards iterate keys, writing values from epoch start
3. Concurrent writes during snapshot go to a COW buffer
4. COW buffer memory is explicitly tracked in `total_memory_used()`

**maxmemory enforcement** uses total bytes (store + COW), preventing OOM during snapshots.

**Eviction during snapshot:**

| Condition | Behavior |
|-----------|----------|
| Memory pressure during snapshot | Eviction proceeds normally |
| Key has pending COW entry | **Skip** -- already captured for snapshot |
| No evictable keys remain | Abort snapshot (`cow_memory_abort_threshold`) |

---

## Sequence Number Assignment

Sequence numbers are assigned at WAL append time, not at command execution time. This ensures:
- Monotonically increasing sequences for replication ordering
- Gaps are possible if batched writes fail partially
- Replicas can request resumption from any sequence number

```rust
impl WalWriter {
    fn append(&mut self, operation: &Operation) -> u64 {
        let batch = WriteBatch::new();
        batch.put(/* key, value encoding */);
        let seq = self.db.write(batch)?;
        self.replication_notify.send(ReplicationEntry {
            sequence: seq,
            operation: operation.clone(),
        });
        seq
    }
}
```
