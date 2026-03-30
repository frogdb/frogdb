---
title: "Glossary"
description: "Terminology definitions used throughout FrogDB documentation."
sidebar:
  order: 16
---
Terminology definitions used throughout FrogDB documentation.

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

See: [storage.md](/architecture/storage/), [concurrency.md](/architecture/concurrency/)

### Hash Slot
A logical partition (0-16383) for distributing data across nodes in cluster mode. Redis Cluster compatible.

Keys are assigned to slots via: `slot = CRC16(key) % 16384`

**Relationship:** `key -> hash_slot(key) -> node -> internal_shard(key)`

See: [storage.md](/architecture/storage/), [clustering.md](/architecture/clustering/)

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

See: [storage.md](/architecture/storage/)

### Terminology Note: Cross-Shard vs Cross-Slot

| Term | Level | Context |
|------|-------|---------|
| **Hash slot** | Cluster | 16,384 slots for cluster routing |
| **Internal shard** | Node | Thread-local partitions within a node |

**Error codes:**
- `-CROSSSLOT` is always used (Redis compatibility), even for internal shard violations

### Orchestrator
An external control plane that manages FrogDB cluster topology. Responsibilities include:
- Node health monitoring
- Failover decisions
- Slot migration coordination
- Configuration distribution

FrogDB uses an orchestrated model (like DragonflyDB) rather than gossip protocol (like Redis Cluster).

See: [clustering.md](/architecture/clustering/)

### VLL (Very Lightweight Locking)
A synchronization mechanism for coordinating multi-shard operations without traditional locks. Ensures atomicity across shards while maintaining the shared-nothing architecture.

See: [vll.md](/architecture/vll/)

---

## Persistence Terms

### WAL (Write-Ahead Log)
A sequential log of all write operations, persisted to RocksDB. Enables durability and crash recovery.

**Modes:**
- `async`: WAL writes queued, fsync by OS
- `periodic`: fsync every N milliseconds (default: 1000ms)
- `sync`: fsync after every write

See: [persistence.md](/architecture/persistence/)

### Snapshot
A point-in-time capture of all key-value data, stored as an RDB-compatible file or RocksDB checkpoint.

**FrogDB approach:** Epoch-based (logical point-in-time) rather than fork-based (physical point-in-time).

### Epoch
A logical timestamp used for snapshot coordination. When a snapshot begins:
1. Current epoch is recorded
2. All shards iterate keys, writing values from epoch start
3. Concurrent writes go to WAL (not snapshot)

### COW (Copy-on-Write)
A technique where data is copied only when modified. FrogDB uses explicit COW buffers for forkless snapshots instead of Redis's fork-based approach.

---

## Protocol Terms

### RESP (Redis Serialization Protocol)
The wire protocol used by Redis and compatible systems.

| Version | Features |
|---------|----------|
| RESP2 | Simple strings, errors, integers, bulk strings, arrays |
| RESP3 | Adds maps, sets, booleans, doubles, push messages, attributes |

See: [protocol.md](/architecture/protocol/)

### CROSSSLOT
An error returned when a multi-key operation references keys in different hash slots.

```
-CROSSSLOT Keys in request don't hash to the same slot
```

**Solution:** Use hash tags to colocate related keys.

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

### LFU (Least Frequently Used)
An eviction policy that removes keys based on access frequency. Uses a logarithmic counter with decay.

### Eviction Pool
A buffer of eviction candidates maintained across sampling rounds. Improves eviction accuracy by accumulating worst candidates over time.

---

## Data Structure Terms

### Dashtable
A segment-based hash table used by DragonflyDB. 30-60% more memory efficient than Redis. FrogDB uses `griddle::HashMap` currently; Dashtable is a future consideration.

### Griddle
A Rust crate providing incremental-resize HashMap. Spreads resize cost across inserts to avoid latency spikes.

See: [storage.md](/architecture/storage/)

---

## Transaction Terms

### MULTI/EXEC
Redis transaction commands. MULTI starts queuing, EXEC executes atomically.

**FrogDB guarantees:**
- Execution atomicity: Yes (no interleaving)
- Durability: Configurable (depends on sync mode)
- Rollback: No (partial failures don't undo prior commands)

### WATCH
Optimistic locking command. Monitors keys for changes; EXEC fails if watched keys were modified.

### Pipelining
Sending multiple commands without waiting for responses. Reduces round-trip latency. Not transactional; commands may interleave with other clients.

---

## Replication Terms

### Primary
The node accepting writes for a set of hash slots. Also called "master" in Redis terminology.

### Replica
A node receiving replicated data from a primary. Serves read traffic and can be promoted on failover.

### PSYNC
Partial synchronization protocol for replication. Allows replicas to catch up from replication backlog without full sync.

### Replication Backlog
A circular buffer of recent write operations. Enables PSYNC when a replica reconnects after brief disconnection.

---

## Metrics Terms

### Tail Latency
The latency experienced by the slowest requests (e.g., p99, p999). FrogDB optimizes via shared-nothing architecture, incremental resize (griddle), and sampling eviction.

### Throughput
Operations per second. FrogDB targets high throughput via thread-per-core model, connection pinning, and batched WAL writes.

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
