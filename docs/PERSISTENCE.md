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

**LRU/LFU Metadata:**
- `lfu_counter` persisted with value
- `last_access` (LRU) NOT persisted - reset to recovery time on startup

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
