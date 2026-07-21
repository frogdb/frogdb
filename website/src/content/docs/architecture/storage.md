---
title: "Storage Layer"
description: "Design of the in-memory storage architecture, key metadata, and memory management."
sidebar:
  order: 4
---
Design of the in-memory storage architecture, key metadata, and memory management.

## Terminology Note

Throughout this document, "shard" refers to **internal shards** (thread-local partitions within a single FrogDB node). Each shard owns its own `Store`, memory budget, and expiry index. Keys are routed to a shard by deriving the Redis cluster slot and reducing it modulo the shard count: `internal_shard = CRC16(key) % 16384 % num_shards` (see [Hash Algorithms](#hash-algorithms)).

This is distinct from **hash slots** used in cluster mode (0-16383) for distributing data across nodes. The [concurrency model](/architecture/concurrency/) page is the canonical home for the shard-per-task routing story; see the [glossary](/architecture/glossary/) for full definitions.

## Store Trait

Each shard owns a `Store` implementation for key-value storage (`frogdb-server/crates/core/src/store/mod.rs`). The trait exposes the operations a shard needs: core CRUD (`get`, `set`, `delete`, `contains`, `key_type`, `len`, `memory_used`), cursor-based iteration (`scan`), TTL/expiry management, sampling and metadata accessors for eviction, and cluster-slot support. Type-checked access to a single value family goes through the `typed` extension layer (`StoreTypedExt`) rather than a narrower trait bound. The default implementation is `HashMapStore`.

### Ownership Semantics

**`Store::get()` returns `Option<Arc<Value>>`, not `Option<&Value>` (borrowed).**

Rationale:
- **Zero-copy reads**: Returning an `Arc<Value>` bumps a reference count instead of deep-cloning large values like hashes or lists. Reads stay cheap even for large collections.
- **Async compatibility**: Returning a borrow would require lifetime annotations that fight async command execution across `.await` points. An owned handle is simpler to hold.
- **Copy-on-write mutation**: `get_mut()` returns `Option<&mut Value>` and clones the value first only when it is shared (`Arc` refcount > 1), so in-place modification of an unshared value pays no copy.

`get_and_delete()` (GETDEL) returns an owned `Value` because the entry is removed from the store; it unwraps the `Arc` when it holds the sole reference and clones only otherwise.

## Value Types

FrogDB stores values as variants of a `Value` enum, with a corresponding `KeyType` enum for type identification (used by the `TYPE` command and `WRONGTYPE` errors). The table below lists the **logical data types** exposed to clients. Several logical types share one underlying enum variant: the `Value` enum has **15 variants**, and Bitmap and Geospatial are not among them — a Bitmap is bit-level operations layered on a `String`, and a Geospatial index is a `SortedSet` whose scores are geohash-encoded coordinates.

### Supported Data Types

| Type | Description | Implementation Notes |
|------|-------------|---------------------|
| String | Binary-safe string with integer encoding | `Bytes` |
| List | Ordered collection | `VecDeque<Bytes>` |
| Set | Unordered unique members | `HashSet<Bytes>` |
| Hash | Field-value map | `HashMap<Bytes, Bytes>` |
| Sorted Set | Scored members with rank queries | `HashMap` + `BTreeMap`/`SkipList` (configurable) |
| Stream | Append-only log with consumer groups | Radix tree + listpack |
| Bitmap | Bit operations on strings | Operations on a String value (not a distinct variant) |
| Geospatial | Coordinate-indexed members | Sorted Set + geohash encoding (not a distinct variant) |
| JSON | JSON document with JSONPath queries | `serde_json::Value` |
| HyperLogLog | Probabilistic cardinality estimation | Sparse/dense register encoding |
| Bloom Filter | Probabilistic membership test | Bit array + hash functions |
| Cuckoo Filter | Probabilistic membership with deletion | Bucket-based filter |
| Count-Min Sketch | Probabilistic frequency estimation | Matrix of counters |
| Top-K | Approximate heavy hitters | HeavyKeeper algorithm |
| T-Digest | Approximate quantile estimation | Centroid-based digest |
| Time Series | Timestamped samples with aggregation | Sorted by timestamp |
| Vector Set | Similarity search vectors | HNSW index via `usearch` |

The 15 enum variants are: `String`, `SortedSet`, `Hash`, `List`, `Set`, `Stream`, `BloomFilter`, `HyperLogLog`, `TimeSeries`, `Json`, `CuckooFilter`, `TopK`, `TDigest`, `CountMinSketch`, `VectorSet`.

---

## Default Implementation

The default store, `HashMapStore` (`frogdb-server/crates/core/src/store/hashmap.rs`), keeps each key's value in an [`Arc<Value>`](#ownership-semantics) alongside its `KeyMetadata`, backed by a [`griddle::HashMap`](#hashmap-implementation-choice).

---

## Key Metadata

Each key tracks metadata (`KeyMetadata`, `frogdb-server/crates/types/src/types/mod.rs`) for expiry and eviction:

- **expires_at**: `Option<Instant>` (None = no expiry).
- **last_access**: Last-access `Instant` for LRU eviction. **NOT persisted** -- reset to recovery time on startup.
- **lfu_counter**: Access-frequency counter for LFU eviction (`u8`, logarithmic, Redis-compatible). New keys start at 5. Persisted with the value.
- **memory_size**: Approximate memory size of this entry.

**Persistence Note:** The `last_access` field is not persisted to disk. After recovery, all keys have fresh access timestamps (idle time = 0). This matches Redis behavior, and eviction accuracy self-corrects as keys are accessed during normal operation. See [Persistence Internals](/architecture/persistence/) for details.

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

The `lfu_counter` uses a probabilistic logarithmic counter (`lfu_log_incr` in `frogdb-server/crates/core/src/eviction/lfu.rs`, Redis-compatible). The increment probability falls as the counter rises, so the counter saturates rather than growing linearly with access count:

```rust
pub fn lfu_log_incr(counter: u8, log_factor: u8) -> u8 {
    if counter == 255 { return 255; }
    let base_val = counter.saturating_sub(5) as f64; // subtract the start value
    let p = 1.0 / (base_val * log_factor as f64 + 1.0);
    if rand::random::<f64>() < p { counter.saturating_add(1) } else { counter }
}
```
- New keys start at 5, so a fresh key is not an immediate eviction candidate.
- Higher counter → lower increment probability.
- `lfu-log-factor` default: 10.

**Decay:** Applied on access via `lfu_decay`. The counter drops by 1 for every `lfu-decay-time` minutes since last access. `lfu-decay-time` default: 1.

---

## Memory Accounting

Memory is tracked per-key and aggregated per-shard. Each entry's size includes key length, value memory, metadata overhead, and hash-table entry overhead.

### HashMap Implementation Choice

FrogDB uses [`griddle::HashMap`](https://crates.io/crates/griddle) instead of `std::collections::HashMap` to avoid the resize latency spike of a standard hash table.

**The Resize Spike Problem:** A standard hash table resizes when its load factor crosses a threshold, moving every element into a larger table in a single O(n) step. That one insert pays the full cost of rehashing the whole table, producing a latency spike that scales with the table size — the larger the keyspace, the worse the tail.

Griddle amortizes the resize: it keeps the old and new tables side by side and migrates a bounded number of entries on each subsequent insert, so no single operation pays the full rehash cost. The trade-off is a slightly slower read while a resize is in progress, because a lookup may have to consult both tables. It is a drop-in API replacement for `std::collections::HashMap`.

FrogDB publishes no benchmark numbers for this trade-off yet; the choice is a tail-latency argument, not a throughput claim.

### Memory During Snapshots

FrogDB's snapshots are forkless: they cut a RocksDB checkpoint at a sequence number
(`RocksStore::create_checkpoint`) rather than forking the process. There is no in-memory
copy-on-write buffer holding pre-snapshot values, and nothing snapshot-related is counted
toward `maxmemory` — memory accounting during a snapshot is the same as at any other time.
See [Forkless Snapshot Algorithm](/architecture/persistence/#forkless-snapshot-algorithm) for
the full mechanism.

| Aspect | Redis | FrogDB |
|--------|-------|--------|
| Snapshot method | Fork-based COW (OS page-level) | Forkless RocksDB checkpoint at a sequence number |
| Extra memory during snapshot | OS-level COW pages, can grow under write load | None — no buffer, no fork |
| maxmemory accounting | Unaffected by snapshotting | Unaffected by snapshotting |

---

## Eviction

When `maxmemory` is exceeded, FrogDB either rejects writes (OOM) or evicts keys according to the configured `EvictionPolicy` (`frogdb-server/crates/core/src/eviction/policy.rs`). The policy names match Redis:

| Policy | Scope | Selection |
|--------|-------|-----------|
| `noeviction` | — | Reject writes with an OOM error (default) |
| `volatile-lru` | Keys with a TTL | Least recently used |
| `allkeys-lru` | Any key | Least recently used |
| `volatile-lfu` | Keys with a TTL | Least frequently used |
| `allkeys-lfu` | Any key | Least frequently used |
| `volatile-random` | Keys with a TTL | Random |
| `allkeys-random` | Any key | Random |
| `volatile-ttl` | Keys with a TTL | Shortest remaining TTL first |
| `tiered-lru` | Any key | Least recently used |
| `tiered-lfu` | Any key | Least frequently used |

**Tiered policies** are FrogDB extensions. Instead of deleting the selected value, a tiered policy *spills* it to a warm on-disk tier (`is_tiered()` on the policy). A later access transparently unspills the value back to the hot in-memory tier. This trades some access latency for a larger effective working set; the other policies delete the value outright.

### Sampled-pool eviction

Like Redis, FrogDB evicts by **approximation, not exact global ordering**. Maintaining a perfectly ordered LRU/LFU list across the whole keyspace would cost too much on the hot path, so eviction samples:

1. When memory must be freed, the shard samples up to `maxmemory-samples` random keys (default 5).
2. Each sampled key is ranked by the policy's criterion — idle time (LRU), decayed frequency (LFU), or remaining TTL (`volatile-ttl`).
3. Good candidates accumulate in a fixed-size **eviction pool** (16 entries per shard) that persists across sampling rounds, so strong candidates seen earlier are not forgotten.
4. The worst-ranked candidate in the pool is evicted, and the loop repeats until the shard is back under budget.

Larger `maxmemory-samples` values approximate true LRU/LFU more closely at the cost of more work per eviction.

---

## Expiry Index

Each shard maintains an expiry index (`ExpiryIndex`) for efficient TTL handling, using a `BTreeMap<(Instant, Bytes), ()>` ordered by deadline. This gives O(log n) insertion/removal and lets active expiry scan due keys in deadline order and stop early.

### Expiry Strategy

FrogDB uses hybrid expiry (lazy + active):

1. **Lazy expiry**: TTL is checked on read; an expired key reads as absent and is deleted.
2. **Active expiry**: A background task samples due keys from the expiry index periodically.

### Expiry Atomicity

Key expiration is checked **at command entry**, before execution begins. Once execution begins, the key will not be expired mid-operation. The active-expiry scanner coordinates at the shard level so it does not delete a key underneath an in-flight operation.

---

## Hash Algorithms

FrogDB uses CRC16 for key routing at both levels (`slot_for_key` / `shard_for_key` in `frogdb-server/crates/core/src/shard/partition.rs`):

| Purpose | Algorithm | Range | Description |
|---------|-----------|-------|-------------|
| **Cluster slot** | CRC16 (XMODEM) | 0-16383 | Redis-compatible; determines which node owns a key |
| **Internal shard** | CRC16 (XMODEM) | 0 to num_shards-1 | Determines which thread within a node processes a key (`CRC16(key) % 16384 % num_shards`) |

Both derive from the same `extract_hash_tag` step, so keys sharing a hash tag `{tag}` are colocated on both the same cluster slot **and** the same internal shard. (xxhash64, which appears elsewhere in the codebase, is used only inside probabilistic data structures — never for key routing.)

### CROSSSLOT Validation

Multi-key operations must have all keys in the same hash slot. FrogDB validates this **before execution**; if any key's slot differs from the first key's slot, a `CROSSSLOT` error is returned before any command runs. See [Clustering Internals](/architecture/clustering/) for how the same slot derivation drives cross-node routing.

---

## Persistence Integration

The in-memory store is the source of truth during operation. Writes are mirrored to a RocksDB-backed WAL for durability, and the on-disk representation is described in [Persistence Internals](/architecture/persistence/) — RocksDB topology, the key-value schema, the WAL, durability modes, and the forkless snapshot algorithm.
