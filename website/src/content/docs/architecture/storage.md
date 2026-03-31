---
title: "Storage Layer"
description: "Design of the in-memory storage architecture, key metadata, and memory management."
sidebar:
  order: 4
---
Design of the in-memory storage architecture, key metadata, and memory management.

## Terminology Note

Throughout this document, "shard" refers to **internal shards** (thread-local partitions within a single FrogDB node). Each shard has its own HashMap, memory budget, and expiry index. Keys are assigned via: `internal_shard = hash(key) % num_shards`.

This is distinct from **hash slots** used in cluster mode (0-16383) for distributing data across nodes. See [glossary.md](/architecture/glossary/) for full definitions.

## Store Trait

Each shard owns a `Store` implementation for key-value storage. The `Store` trait provides methods for get, set, delete, contains, key_type, len, memory_used, and scan operations.

### Ownership Semantics

**`Store::get()` returns `Option<Value>` (owned), not `Option<&Value>` (borrowed).**

Rationale:
- **Async compatibility**: Returning references would require complex lifetime annotations with async command execution. Owned values are simpler to work with across `.await` points.
- **Simplicity over optimization**: The slight overhead of cloning values is acceptable. Optimization can be added later if profiling shows it's a bottleneck.
- **Command execution pattern**: Most commands need to inspect values, compute responses, and potentially modify them. Owned values fit this pattern naturally.

Future optimization path: Add a `get_ref()` method returning `Option<&Value>` for read-only operations where the caller can guarantee no `.await` across the borrow.

## Value Types

FrogDB stores values as variants of a `Value` enum, with a corresponding `KeyType` enum for type identification (used by the `TYPE` command and `WRONGTYPE` errors).

### Supported Data Types

| Type | Description | Implementation Notes |
|------|-------------|---------------------|
| String | Binary-safe string with integer encoding | `Bytes` |
| List | Ordered collection | `VecDeque<Bytes>` |
| Set | Unordered unique members | `HashSet<Bytes>` |
| Hash | Field-value map | `HashMap<Bytes, Bytes>` |
| Sorted Set | Scored members with rank queries | `HashMap` + `BTreeMap`/`SkipList` (configurable) |
| Stream | Append-only log with consumer groups | Radix tree + listpack |
| Bitmap | Bit operations on strings | Operations on String value |
| Geospatial | Coordinate-indexed members | Sorted Set + geohash encoding |
| JSON | JSON document with JSONPath queries | `serde_json::Value` |
| HyperLogLog | Probabilistic cardinality estimation | 12KB fixed structure |
| Bloom Filter | Probabilistic membership test | Bit array + hash functions |
| Cuckoo Filter | Probabilistic membership with deletion | Bucket-based filter |
| Count-Min Sketch | Probabilistic frequency estimation | Matrix of counters |
| Top-K | Approximate heavy hitters | HeavyKeeper algorithm |
| T-Digest | Approximate quantile estimation | Centroid-based digest |
| Time Series | Timestamped samples with aggregation | Sorted by timestamp |
| Vector Set | Similarity search vectors | HNSW index via usearch |

---

## Default Implementation

The default store uses a `HashMap` with per-key metadata, tracking each key's value alongside its `KeyMetadata` (expiry, LRU timestamp, LFU counter, memory size).

---

## Key Metadata

Each key tracks metadata for expiry and eviction:

- **expires_at**: Expiration time (None = no expiry)
- **last_access**: Last access time for LRU eviction. **NOT persisted** -- reset to recovery time on startup.
- **lfu_counter**: Access frequency counter for LFU eviction. Uses logarithmic counter (Redis-compatible). Persisted with value.
- **memory_size**: Approximate memory size of this entry.

**Persistence Note:** The `last_access` field is NOT persisted to disk. After recovery, all keys have fresh access timestamps (idle time = 0). This matches Redis behavior and eviction accuracy self-corrects within minutes. See [persistence.md](/architecture/persistence/) for details.

### Clock Sources

| Use Case | Clock Type | Implementation | Rationale |
|----------|------------|----------------|-----------|
| **TTL checking** | Monotonic | `std::time::Instant` | Immune to clock adjustments |
| **last_access** | Monotonic | `std::time::Instant` | LRU needs relative time only |
| **Persistence format** | Wall clock | Unix timestamp ms | Portable across restarts |
| **Latency metrics** | Monotonic | `std::time::Instant` | Accurate measurement |
| **EXPIREAT command** | Wall clock | Unix timestamp | User specifies absolute time |

**Conversion on Recovery:** Persisted Unix timestamps are converted to monotonic `Instant` values by computing the remaining duration. Already-expired keys are discarded during recovery.

### LFU Counter

The `lfu_counter` uses a probabilistic logarithmic counter (Redis-compatible):

```rust
fn lfu_log_incr(counter: u8) -> u8 {
    if counter == 255 { return 255; }
    let r: f64 = random();
    let base_probability = 1.0 / (counter as f64 * LFU_LOG_FACTOR + 1.0);
    if r < base_probability { counter + 1 } else { counter }
}
```
- New keys start at 5 (not immediately evicted)
- Higher counter = lower increment probability
- `lfu-log-factor` default: 10

**Decay:** Applied lazily on access. `lfu-decay-time` default: 1 (decay by 1 per minute of inactivity).

---

## Memory Accounting

Memory is tracked per-key and aggregated per-shard. Each entry's size includes key length, value memory, metadata overhead, and HashMap entry overhead.

### HashMap Implementation Choice

FrogDB uses [`griddle::HashMap`](https://crates.io/crates/griddle) instead of `std::collections::HashMap` to avoid memory and latency spikes during resizing.

**The Resize Spike Problem:** Standard hash tables must resize when the load factor exceeds a threshold, requiring ~2x memory during resize and causing latency spikes of 30-40ms on a single insert.

| Aspect | std::HashMap | griddle::HashMap |
|--------|--------------|------------------|
| Max insert latency | ~38ms | ~1.8ms |
| Mean insert latency | ~94ns | ~126ns |
| Memory during resize | 2x (allocated upfront) | 2x (but amortized) |
| API compatibility | - | Drop-in replacement |

Griddle spreads resize work across inserts, ensuring no single operation pays the full cost. The trade-off is slightly slower reads during active resizing (must check both old and new tables).

### Total Memory (Including COW Buffers)

FrogDB uses forkless snapshots with explicit Copy-on-Write (COW) buffering. Unlike Redis's fork-based approach, COW buffer memory is **explicitly tracked and included in maxmemory enforcement**. Total memory usage includes both the store (keys + values) and the snapshot COW buffer (old values pending serialization).

| Aspect | Redis | FrogDB |
|--------|-------|--------|
| Snapshot method | Fork-based COW | Forkless epoch-based COW |
| COW memory tracking | OS-level (not explicit) | Explicit in `total_memory_used()` |
| maxmemory includes COW | No (operators provision headroom) | Yes (prevents OOM) |
| Memory predictability | Can spike to 2x | Bounded by `cow-buffer-max-bytes` |

---

## Expiry Index

Each shard maintains an expiry index for efficient TTL handling using a `BTreeMap<(Instant, Bytes), ()>` for O(log n) insertion/removal with lower memory overhead than a HashSet per expiry time.

### Expiry Strategy

FrogDB uses hybrid expiry (lazy + active):

1. **Lazy expiry**: Check TTL on every read; delete if expired
2. **Active expiry**: Background task samples keys periodically

### Expiry Atomicity

Key expiration is checked **at command entry**, before execution begins. Once command execution begins, the key will not be expired mid-operation. Background scanner skips keys with active operations (shard-level coordination).

---

## Hash Algorithms

FrogDB uses two different hash algorithms for different purposes:

| Purpose | Algorithm | Range | Description |
|---------|-----------|-------|-------------|
| **Cluster slot** | CRC16 | 0-16383 | Redis-compatible, determines which node owns a key |
| **Internal shard** | xxhash64 | 0 to num_shards | Determines which thread within a node processes a key |

Both algorithms use the same `extract_hash_tag` function. Keys with the same hash tag will be colocated on both the same cluster slot AND the same internal shard.

### CROSSSLOT Validation

Multi-key operations must have all keys in the same hash slot. FrogDB validates this **before execution**. If any key's slot differs from the first key's slot, a `CROSSSLOT` error is returned before execution begins.

---

## Persistence Integration

The store provides hooks for persistence via RocksDB. A `PersistentStore` trait extends `Store` with methods for persisting writes and loading data on startup.

See [persistence.md](/architecture/persistence/) for RocksDB integration, WAL, and snapshots.
