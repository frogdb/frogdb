# FrogDB Persistence

This document details FrogDB's persistence architecture including RocksDB integration, write-ahead logging, durability modes, and forkless snapshots.

## RocksDB Topology

FrogDB uses a **single shared RocksDB instance** with one column family per shard:

```
┌─────────────────────────────────────────────────────────┐
│                    RocksDB Instance                      │
├─────────────────────────────────────────────────────────┤
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌─────────┐ │
│  │  CF: s0   │ │  CF: s1   │ │  CF: s2   │ │ CF: sN  │ │
│  │ (Shard 0) │ │ (Shard 1) │ │ (Shard 2) │ │(Shard N)│ │
│  └───────────┘ └───────────┘ └───────────┘ └─────────┘ │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Shared WAL                          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Benefits

- Single backup/restore operation for entire database
- Shared WAL simplifies recovery
- Atomic cross-shard operations possible via WriteBatch

### Trade-off

- Potential lock contention on WAL writes (mitigated by batching)

### Key-Value Schema

Each column family (shard) stores keys with this format:

**Key Format:**
```
[user_key_bytes]
```

**Value Format:**
```
┌─────────────────────────────────────────────────────────────┐
│ Header (fixed 24 bytes)                                      │
├─────────────────────────────────────────────────────────────┤
│ type: u8           │ Value type (0=String, 1=List, etc.)    │
│ flags: u8          │ Reserved for future use                 │
│ expires_at: i64    │ Unix timestamp ms (0 = no expiry)       │
│ lfu_counter: u8    │ LFU access counter                      │
│ padding: [u8; 5]   │ Alignment padding                       │
│ value_len: u64     │ Length of value data                    │
├─────────────────────────────────────────────────────────────┤
│ Value Data (variable)                                        │
│ - String: raw bytes                                          │
│ - List: length-prefixed elements                             │
│ - Set: length-prefixed members                               │
│ - Hash: length-prefixed key-value pairs                      │
│ - SortedSet: length-prefixed (score, member) pairs           │
└─────────────────────────────────────────────────────────────┘
```

**Type Encoding:**

| Type | Code | Serialization |
|------|------|---------------|
| String | 0 | Raw bytes |
| List | 1 | `[len:u32][elem1_len:u32][elem1]...` |
| Set | 2 | `[len:u32][member1_len:u32][member1]...` |
| Hash | 3 | `[len:u32][k1_len:u32][k1][v1_len:u32][v1]...` |
| SortedSet | 4 | `[len:u32][score:f64][member_len:u32][member]...` |

**Expiry Index:**
- NOT persisted separately
- Rebuilt during recovery from `expires_at` field in each value
- Active expiry index is in-memory only

**LRU/LFU Metadata (Matches Redis Behavior):**
- `lfu_counter` persisted with value
- `last_access` (LRU) **NOT persisted** - reset to recovery time on startup

**Recovery Impact on Eviction:**
After recovery, all keys appear "fresh" for LRU purposes (idle time = 0). This matches Redis behavior:
- Redis intentionally does not persist LRU timestamps ([GitHub Issue #1261](https://github.com/redis/redis/issues/1261))
- Salvatore rejected persisting LRU due to limited benefit vs. implementation cost
- Eviction accuracy **self-corrects within minutes** as keys are accessed during normal operation
- The `RESTORE` command supports `IDLETIME` and `FREQ` modifiers for explicit migration tools

---

## Write-Ahead Log (WAL)

Every write operation is appended to RocksDB's WAL before acknowledgment:

```
Client Write (SET key value)
         │
         ▼
    Shard Worker
         │
         ├── 1. Apply to in-memory store
         │
         ├── 2. Append to WAL (async batch)
         │      └── RocksDB WriteBatch
         │
         └── 3. Return OK to client
```

---

## Durability Modes

```rust
pub enum DurabilityMode {
    /// Fastest: WAL write, no fsync (data loss on crash possible)
    Async,

    /// Balanced: fsync every N ms or M writes
    Periodic { interval_ms: u64, write_count: usize },

    /// Safest: fsync every write (slowest)
    Sync,
}
```

### Mode Comparison

| Mode | Durability | Latency |
|------|------------|---------|
| `Async` | Best-effort (may lose data) | ~1-10 μs |
| `Periodic(100ms, 1000)` | Bounded loss (100ms or 1000 writes) | ~1-10 μs |
| `Sync` | Guaranteed (fsync per write) | ~100-500 μs |

---

## Forkless Snapshot Algorithm

Based on Dragonfly's approach - no fork(), no memory spike:

```
1. Coordinator signals all shards: "Start snapshot epoch N"

2. Each shard:
   a. Sets snapshot_epoch = N
   b. Continues processing commands normally
   c. In background, iterates owned keys:
      - For each key, serialize (key, value, expiry)
      - Send batch to snapshot writer
   d. For writes DURING snapshot:
      - If key not yet visited, serialize OLD value first (COW semantics)
      - Then apply new value

3. Snapshot writer:
   - Receives batches from all shards
   - Writes to RocksDB snapshot column family
   - On completion, updates metadata with epoch N

4. Recovery:
   - Load latest snapshot epoch
   - Replay WAL entries after snapshot LSN
```

### Advantages

- No 2x memory spike from fork() + copy-on-write
- Server continues processing during snapshot
- Consistent point-in-time capture across all shards

### Edge Cases During Snapshot

**Keys Modified During Snapshot:**
- If key not yet visited by iterator: Serialize OLD value first (COW), then apply new value
- If key already visited: Just apply new value (old value already in snapshot)

**Keys Deleted During Snapshot:**
- If not yet visited by iterator: **Skip** (key excluded from snapshot)
- If already visited: Already serialized, deletion is part of subsequent WAL entries

**Note on Point-in-Time Semantics:**
Unlike fork-based snapshots (Redis), our epoch-based approach does NOT capture a perfect point-in-time.
A key that exists at snapshot start may be excluded if deleted before the iterator visits it.
This is an acceptable trade-off vs. fork's 2x memory spike. DragonflyDB uses the same approach.

**Keys Created During Snapshot:**
- New keys are NOT part of this snapshot (snapshot captures point-in-time at epoch start)
- New key writes go to WAL and will be captured in next snapshot
- Recovery: Load snapshot, replay WAL (includes new keys)

**Concurrent Iteration State:**

```rust
struct SnapshotIterator {
    epoch: u64,
    visited: HashSet<Bytes>,   // Keys already serialized
    current_position: usize,   // Iterator position in shard's HashMap
}

fn on_write_during_snapshot(key: &Bytes, old_value: &FrogValue, new_value: &FrogValue) {
    if !self.visited.contains(key) {
        // COW: Serialize old value before overwriting
        self.batch_tx.send((key.clone(), old_value.clone())).await;
        self.visited.insert(key.clone());
    }
    // Now safe to apply new value
}
```

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `snapshot_interval_s` | `3600` | Seconds between snapshots (0 = disabled) |
| `data_dir` | `./data` | Directory for RocksDB and snapshots |

---

## WAL Retention for Replication

When clustering is enabled, WAL files are retained beyond normal durability needs to support replica synchronization.

### Purpose

Replicas that disconnect briefly need to catch up without full resynchronization:

```
Primary                              Replica
   │                                    │
   │ ── WAL entries 1000-1500 ─────────▶│
   │                                    │
   │              [Replica disconnects] │
   │                                    │
   │  WAL entries 1501-2000 (buffered)  │
   │                                    │
   │              [Replica reconnects]  │
   │                                    │
   │◀──── PSYNC repl_id 1500 ──────────│
   │                                    │
   │ ── WAL entries 1501-2000 ─────────▶│  (Partial sync succeeds)
```

If WAL entries 1501-2000 were already purged, a full resync would be required.

### RocksDB WAL Archive

RocksDB moves obsolete WAL files to an archive directory before deletion:

```
data/
├── rocksdb/
│   ├── 000001.log      (current WAL)
│   └── archive/
│       ├── 000000.log  (archived for replication)
│       └── ...
```

### APIs for Replication

| API | Purpose |
|-----|---------|
| `GetLatestSequenceNumber()` | Get current WAL position |
| `GetUpdatesSince(seq)` | Iterate WAL entries from sequence |
| `GetSortedWalFiles()` | List available WAL files |

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `wal_retention_size` | `100MB` | Keep at least this much WAL for replicas |
| `wal_retention_time` | `3600s` | Keep WAL files for at least this duration |
| `repl_backlog_size` | `1MB` | In-memory buffer for fast reconnection |

### Trade-offs

| Aspect | Larger Retention | Smaller Retention |
|--------|------------------|-------------------|
| Disk usage | Higher | Lower |
| Partial sync success | More likely | Less likely |
| Stale replica recovery | Better | Worse (triggers full sync) |

See [CLUSTER.md](CLUSTER.md) for complete replication protocol details.

---

## Recovery Process

### Startup Sequence

1. **Open RocksDB** with all column families (one per shard)
2. **Find latest snapshot** from metadata
3. **Replay WAL** from snapshot's sequence number to end
4. **Rebuild expiry index** from `expires_at` field in each value
5. **Initialize shard workers** with recovered data

### Column Families and Sequence Numbers

RocksDB uses a **single global sequence number** across all column families:

```
Sequence 1000: CF:s0 PUT key1 value1
Sequence 1001: CF:s2 PUT key2 value2
Sequence 1002: CF:s0 DEL key3
...
```

**Recovery implications:**
- Snapshot captures global sequence number at start
- WAL replay begins from that global sequence
- Cross-shard operations in a WriteBatch are atomic (single sequence)

### Partial Snapshot Handling

If server crashes during snapshot:
- Incomplete snapshot is detected (missing completion marker)
- Previous complete snapshot is used instead
- WAL contains all operations since that older snapshot

---

## Persistence Backpressure

When RocksDB can't keep up with write rate, backpressure propagates to clients.

### Write Pipeline

```
Client → Shard Worker → Persistence Queue → RocksDB
                              │
                              └── WriteBatch accumulator
```

### Backpressure Mechanism

```rust
struct PersistenceQueue {
    /// Bounded queue of pending WriteBatches
    queue: ArrayQueue<WriteBatch>,
    /// High watermark (80% full)
    high_watermark: usize,
}

impl PersistenceQueue {
    async fn enqueue(&self, batch: WriteBatch) {
        // If queue is at high watermark, block
        while self.queue.len() >= self.high_watermark {
            // Apply backpressure - shard worker blocks here
            tokio::task::yield_now().await;
        }
        self.queue.push(batch);
    }
}
```

### Backpressure Flow

```
RocksDB slow (disk I/O)
       │
       ▼
PersistenceQueue fills
       │
       ▼
Shard worker blocks on enqueue
       │
       ▼
Shard channel fills (1024 messages)
       │
       ▼
Connection blocks on send
       │
       ▼
Client TCP buffer fills
       │
       ▼
Client experiences latency
```

### Monitoring

| Metric | Description |
|--------|-------------|
| `frogdb_persistence_queue_depth` | Current queue size |
| `frogdb_persistence_write_latency_ms` | RocksDB write latency |
| `frogdb_persistence_batch_size` | Average WriteBatch size |
| `frogdb_persistence_backpressure_events` | Times backpressure applied |

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `persistence_queue_size` | 64 | Max pending WriteBatches |
| `persistence_batch_max_size` | 4MB | Max size per WriteBatch |
| `persistence_batch_timeout_ms` | 10 | Max time to accumulate batch |
