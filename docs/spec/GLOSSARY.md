# FrogDB Glossary

This document defines terms used throughout the FrogDB specification documents.

---

## Architecture Terms

### Internal Shard
A thread-local partition within a single FrogDB node. Each internal shard has its own:
- HashMap (key-value store)
- Memory budget
- Expiry index
- WAL writer

Keys are assigned to internal shards via: `internal_shard = hash(key) % num_shards`

**Not to be confused with:** Hash slot (cluster-level distribution).

See: [STORAGE.md](STORAGE.md), [CONCURRENCY.md](CONCURRENCY.md)

### Hash Slot
A logical partition (0-16383) for distributing data across nodes in cluster mode. Redis Cluster compatible.

Keys are assigned to slots via: `slot = CRC16(key) % 16384`

**Relationship:** `key → hash_slot(key) → node → internal_shard(key)`

See: [STORAGE.md](STORAGE.md#hash-tags-and-slot-validation), [CLUSTER.md](CLUSTER.md)

### Slot vs Shard: Quick Reference

| Term | Scope | Algorithm | Range | Purpose |
|------|-------|-----------|-------|---------|
| **Hash Slot** | Cluster (multi-node) | CRC16 | 0-16383 | Which node owns a key |
| **Internal Shard** | Node (multi-thread) | xxhash64 | 0 to num_cpus | Which thread processes a key |

**Key distinctions:**
- Transactions require same **internal shard** (all keys on same thread)
- Multi-key commands (MGET/MSET) check **hash slots** by default
- `allow_cross_slot_standalone` relaxes slot checks, but not shard requirements for transactions
- `-CROSSSLOT` error uses "slot" terminology for Redis compatibility

### Hash Tag
A `{tag}` syntax in key names that controls hash slot assignment. Only the content between the first `{` and `}` is hashed.

**Example:** `{user:123}:profile` and `{user:123}:sessions` hash to the same slot.

**Purpose:** Enables multi-key operations (MGET, transactions) on related keys.

See: [STORAGE.md](STORAGE.md#hash-tag-examples)

### Terminology Note: Cross-Shard vs Cross-Slot

FrogDB uses two levels of key distribution:

| Term | Level | Context |
|------|-------|---------|
| **Hash slot** | Cluster | 16,384 slots for cluster routing |
| **Internal shard** | Node | Thread-local partitions within a node |

**In documentation:**
- "Cross-slot" refers to keys in different cluster hash slots
- "Cross-shard" refers to keys on different internal shards (same node)

**Error codes:**
- `-CROSSSLOT` is always used (Redis compatibility), even for internal shard violations

**Example:**
- Cluster mode: `{user:1}:name` and `{user:2}:name` are cross-slot
- Standalone mode: `key1` and `key2` may be cross-shard (different internal shards)

### Orchestrator
An external control plane that manages FrogDB cluster topology. Responsibilities include:
- Node health monitoring
- Failover decisions
- Slot migration coordination
- Configuration distribution

FrogDB uses an orchestrated model (like DragonflyDB) rather than gossip protocol (like Redis Cluster).

See: [CLUSTER.md](CLUSTER.md)

### VLL (Very Lightweight Locking)
A synchronization mechanism for coordinating multi-shard operations without traditional locks. Ensures atomicity across shards while maintaining the shared-nothing architecture.

See: [VLL.md](VLL.md)

---

## Persistence Terms

### WAL (Write-Ahead Log)
A sequential log of all write operations, persisted to RocksDB. Enables durability and crash recovery.

**Modes:**
- `async`: WAL writes queued, fsync by OS
- `periodic`: fsync every N milliseconds (default: 1000ms)
- `sync`: fsync after every write

See: [PERSISTENCE.md](PERSISTENCE.md)

### Snapshot
A point-in-time capture of all key-value data, stored as an RDB-compatible file or RocksDB checkpoint.

**FrogDB approach:** Epoch-based (logical point-in-time) rather than fork-based (physical point-in-time).

See: [PERSISTENCE.md](PERSISTENCE.md#snapshot-process)

### Epoch
A logical timestamp used for snapshot coordination. When a snapshot begins:
1. Current epoch is recorded
2. All shards iterate keys, writing values from epoch start
3. Concurrent writes go to WAL (not snapshot)

See: [PERSISTENCE.md](PERSISTENCE.md)

### COW (Copy-on-Write)
A technique where data is copied only when modified. Used by Redis for fork-based snapshots.

**FrogDB:** Does not use fork/COW; uses epoch-based snapshots instead to avoid memory spikes.

---

## Protocol Terms

### RESP (Redis Serialization Protocol)
The wire protocol used by Redis and compatible systems.

| Version | Features |
|---------|----------|
| RESP2 | Simple strings, errors, integers, bulk strings, arrays |
| RESP3 | Adds maps, sets, booleans, doubles, push messages, attributes |

See: [PROTOCOL.md](PROTOCOL.md)

### CROSSSLOT
An error returned when a multi-key operation references keys in different hash slots.

```
-CROSSSLOT Keys in request don't hash to the same slot
```

**Solution:** Use hash tags to colocate related keys.

See: [STORAGE.md](STORAGE.md#crossslot-validation)

### Error Code Reference

| Error | Context | Meaning |
|-------|---------|---------|
| `-CROSSSLOT` | Multi-key ops, transactions, Lua | Keys don't hash to same slot |
| `-MOVED slot host:port` | Cluster mode | Key's slot is on different node |
| `-ASK slot host:port` | Cluster migration | Key is being migrated |
| `-WRONGTYPE` | Type mismatch | Operation against wrong data type |
| `-OOM` | Memory limit | Out of memory, writes rejected |
| `-TIMEOUT` | VLL queue | Operation timed out waiting for locks |
| `-NOSCRIPT` | Lua scripting | Script not found in cache |
| `-BUSY` | Lua scripting | Script exceeded time limit |

**Note:** FrogDB uses `-CROSSSLOT` (not `-CROSSSHARD`) for Redis compatibility, even when the underlying check is at the internal shard level.

---

## Eviction Terms

### LRU (Least Recently Used)
An eviction policy that removes keys based on last access time. FrogDB uses **sampling LRU** (like Redis) rather than exact LRU.

See: [EVICTION.md](EVICTION.md#lru-implementation)

### LFU (Least Frequently Used)
An eviction policy that removes keys based on access frequency. Uses a logarithmic counter with decay.

See: [EVICTION.md](EVICTION.md#lfu-implementation)

### Eviction Pool
A buffer of eviction candidates maintained across sampling rounds. Improves eviction accuracy by accumulating worst candidates over time.

See: [EVICTION.md](EVICTION.md)

### 2Q / LFRU
A hybrid eviction algorithm used by DragonflyDB. Combines LRU and LFU with probationary/protected buffers.

**FrogDB:** Not currently implemented; noted as future consideration if Dashtable is adopted.

See: [EVICTION.md](EVICTION.md#dragonflydb-2q-lfru-future-consideration)

---

## Data Structure Terms

### Dashtable
A segment-based hash table used by DragonflyDB. Features:
- Zero per-key overhead for eviction metadata
- No resize spikes (segments split independently)
- 30-60% more memory efficient than Redis

**FrogDB:** Uses `griddle::HashMap` currently; Dashtable is a future consideration.

See: [STORAGE.md](STORAGE.md#hashmap-implementation-choice)

### Griddle
A Rust crate providing incremental-resize HashMap. Spreads resize cost across inserts to avoid latency spikes.

See: [STORAGE.md](STORAGE.md#hashmap-implementation-choice)

---

## Transaction Terms

### MULTI/EXEC
Redis transaction commands. MULTI starts queuing, EXEC executes atomically.

**FrogDB guarantees:**
- Execution atomicity: Yes (no interleaving)
- Durability: Configurable (depends on sync mode)
- Rollback: No (partial failures don't undo prior commands)

See: [TRANSACTIONS.md](TRANSACTIONS.md)

### WATCH
Optimistic locking command. Monitors keys for changes; EXEC fails if watched keys were modified.

See: [TRANSACTIONS.md](TRANSACTIONS.md#watch-optimistic-locking)

### Pipelining
Sending multiple commands without waiting for responses. Reduces round-trip latency.

**Note:** Pipelining is not transactional; commands may interleave with other clients.

See: [TRANSACTIONS.md](TRANSACTIONS.md#pipelining)

---

## Replication Terms

### Primary
The node accepting writes for a set of hash slots. Also called "master" in Redis terminology.

### Replica
A node receiving replicated data from a primary. Serves read traffic and can be promoted on failover.

### PSYNC
Partial synchronization protocol for replication. Allows replicas to catch up from replication backlog without full sync.

See: [CLUSTER.md](CLUSTER.md)

### Replication Backlog
A circular buffer of recent write operations. Enables PSYNC when a replica reconnects after brief disconnection.

---

## Metrics Terms

### Tail Latency
The latency experienced by the slowest requests (e.g., p99, p999). FrogDB optimizes for low tail latency via:
- Shared-nothing architecture (no lock contention)
- Incremental resize (griddle)
- Sampling eviction (no full scans)

### Throughput
Operations per second. FrogDB targets high throughput via:
- Thread-per-core model
- Connection pinning
- Batched WAL writes

---

## Abbreviations

| Abbrev | Meaning |
|--------|---------|
| AOF | Append-Only File (Redis persistence format) |
| RDB | Redis Database (snapshot format) |
| TTL | Time To Live (key expiration) |
| OOM | Out Of Memory |
| ACL | Access Control List |
| TLS | Transport Layer Security |
| FIFO | First In, First Out |
| CRC16 | Cyclic Redundancy Check (16-bit) |

---

## See Also

- [INDEX.md](INDEX.md) - Main documentation index
- [CONSISTENCY.md](CONSISTENCY.md) - Consistency guarantees
- [FAILURE_MODES.md](FAILURE_MODES.md) - Error handling
