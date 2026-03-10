# FrogDB Two-Tier Storage

This document specifies two-tier storage for FrogDB: Hot (in-memory) and Warm (local SSD via RocksDB). When memory pressure triggers eviction, values are **demoted** to disk instead of deleted, and transparently **promoted** back to RAM on access.

## Overview

```
┌──────────────────────────────────────────────────────────────┐
│                  Two-Tier Storage Architecture                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   HOT (RAM)                    WARM (Disk)                   │
│   ┌──────────────────┐        ┌──────────────────┐          │
│   │ HashMapStore      │ demote │ RocksDB column   │          │
│   │ keys + metadata + │ ─────▶ │ family stores    │          │
│   │ hot values        │        │ demoted values   │          │
│   └──────────────────┘        └──────────────────┘          │
│          ▲                          │                        │
│          │         promote          │                        │
│          └──────────────────────────┘                        │
│                                                              │
│   Keys and metadata ALWAYS remain in RAM.                    │
│   Only values are demoted to disk.                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

| Tier | Storage | Latency | Contents |
|------|---------|---------|----------|
| **Hot** | In-memory (HashMapStore) | < 1μs | Keys + metadata + values |
| **Warm** | Local SSD (RocksDB CF) | ~50–200μs (NVMe) | Demoted values only |

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Promotion strategy** | Eager on access | Accessed warm values immediately promoted to hot |
| **Eviction integration** | Demote instead of delete | When hot tier hits maxmemory, demote value to warm |
| **Warm tier backend** | RocksDB column family | Reuse existing persistence infrastructure |
| **Key/metadata location** | Always in RAM | O(1) EXISTS, SCAN, eviction sampling, TTL checks |
| **Value storage** | `ValueLocation` enum | Tombstone in existing store (Option B) |
| **I/O model** | Synchronous | RocksDB reads are blocking but fast (~50–200μs on NVMe) |
| **TTL keys** | Eligible for demotion | Expiry checked on promotion |

---

## Core Abstraction: ValueLocation

Instead of maintaining a separate index for warm keys, the existing `Entry` struct is extended with a `ValueLocation` enum. This preserves all existing SCAN, EXISTS, key counting, sampling, and eviction logic without changes — they already iterate entries.

```rust
/// Where a key's value currently resides
#[derive(Debug)]
enum ValueLocation {
    /// Value is in RAM (normal case)
    Hot(Arc<Value>),
    /// Value has been demoted to RocksDB warm tier
    Warm,
}

/// Entry in the store with value location and metadata.
///
/// Keys and metadata are ALWAYS in RAM. Only the value may be on disk.
#[derive(Debug)]
struct Entry {
    location: ValueLocation,
    metadata: KeyMetadata,
}

impl Entry {
    /// Returns the in-memory value, or None if demoted to warm tier.
    fn hot_value(&self) -> Option<&Arc<Value>> {
        match &self.location {
            ValueLocation::Hot(v) => Some(v),
            ValueLocation::Warm => None,
        }
    }

    /// Returns true if the value is currently in RAM.
    fn is_hot(&self) -> bool {
        matches!(self.location, ValueLocation::Hot(_))
    }
}
```

This approach (Option B from design discussion) is simpler than a separate `TierIndex` because:
- Existing SCAN iterates `data: HashMap<Bytes, Entry>` — warm keys appear naturally
- EXISTS checks the same HashMap — no secondary lookup needed
- Eviction sampling already iterates entries — warm keys are skippable via `is_hot()`
- Key counting, TTL expiry, and metadata access all work unchanged
- `memory_used()` only counts `Hot` entries toward maxmemory

---

## Tier Operations

### Demotion (Hot → Warm)

When memory pressure triggers eviction and a tiered policy is active, values are demoted to the RocksDB warm column family instead of deleted:

```rust
impl HashMapStore {
    /// Demote a key's value from RAM to the warm tier (RocksDB).
    ///
    /// The key and metadata remain in RAM. Only the serialized value
    /// is written to the warm column family.
    fn demote_key(
        &mut self,
        key: &[u8],
        warm: &RocksStore,
    ) -> Result<usize, DemotionError> {
        let entry = self.data.get_mut(key)
            .ok_or(DemotionError::KeyNotFound)?;

        // Only demote hot values
        let value = match &entry.location {
            ValueLocation::Hot(v) => v.clone(),
            ValueLocation::Warm => return Err(DemotionError::AlreadyWarm),
        };

        let memory_freed = value.memory_size();

        // Serialize and write value to RocksDB warm column family
        let serialized = serialize_value(&value);
        warm.put_cf(WARM_CF, key, &serialized)?;

        // Replace hot value with warm marker — key + metadata stay in RAM
        entry.location = ValueLocation::Warm;
        self.memory_used -= memory_freed;

        Ok(memory_freed)
    }
}
```

### Promotion (Warm → Hot)

When a warm key is accessed, its value is eagerly promoted back to RAM:

```rust
impl HashMapStore {
    /// Promote a warm key's value back to RAM.
    ///
    /// Reads the serialized value from RocksDB, deserializes it,
    /// and replaces the Warm marker with a Hot value.
    fn promote_key(
        &mut self,
        key: &[u8],
        warm: &RocksStore,
    ) -> Option<Arc<Value>> {
        let entry = self.data.get_mut(key)?;

        // Only promote warm values
        if entry.is_hot() {
            return entry.hot_value().cloned();
        }

        // Check TTL before promoting
        if let Some(expires_at) = entry.metadata.expires_at {
            if Instant::now() >= expires_at {
                // Key expired — delete entirely
                self.delete(key);
                warm.delete_cf(WARM_CF, key).ok();
                return None;
            }
        }

        // Read from RocksDB
        let serialized = warm.get_cf(WARM_CF, key)?;
        let value = Arc::new(deserialize_value(&serialized).ok()?);

        let memory_added = value.memory_size();

        // Promote to hot
        entry.location = ValueLocation::Hot(value.clone());
        self.memory_used += memory_added;

        // Remove from warm tier
        warm.delete_cf(WARM_CF, key).ok();

        Some(value)
    }
}
```

### GET with Transparent Promotion

```rust
impl Store for HashMapStore {
    fn get(&mut self, key: &[u8]) -> Option<Arc<Value>> {
        // Fast path: key not in store at all
        if !self.data.contains_key(key) {
            return None;
        }

        let entry = self.data.get(key)?;

        match &entry.location {
            ValueLocation::Hot(v) => {
                // Hot path — same as today, no disk I/O
                Some(v.clone())
            }
            ValueLocation::Warm => {
                // Warm path — promote from disk
                self.promote_key(key, &self.warm_store)
            }
        }
    }
}
```

---

## Eviction Integration

### TieredLru and TieredLfu Policies

New eviction policies that demote instead of delete:

```rust
pub enum EvictionPolicy {
    // Existing policies (delete keys)
    NoEviction,
    VolatileLru,
    AllkeysLru,
    VolatileLfu,
    AllkeysLfu,
    VolatileRandom,
    AllkeysRandom,
    VolatileTtl,

    // Tiered policies (demote values to disk)
    /// Demote least recently used values instead of deleting
    TieredLru,
    /// Demote least frequently used values instead of deleting
    TieredLfu,
}
```

### Eviction Flow with Tiering

```
┌──────────────────────────────────────────────────────────────┐
│                    Tiered Eviction Flow                       │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   Write Operation                                            │
│        │                                                     │
│        ▼                                                     │
│   Check Memory: store.memory_used() > config.hot_maxmemory ? │
│        │                                                     │
│        ├── No ──▶ Proceed with write                         │
│        │                                                     │
│        ▼ Yes                                                 │
│   Select victim key (LRU/LFU sampling, skip Warm entries)    │
│        │                                                     │
│        ▼                                                     │
│   ┌────────────────────────────────────────────┐             │
│   │ Policy == TieredLru or TieredLfu ?         │             │
│   └────────────────┬───────────────────────────┘             │
│        │           │                                         │
│        │ Yes       │ No (standard policy)                    │
│        ▼           ▼                                         │
│   demote_key()   delete_key()                                │
│        │           │                                         │
│        ▼           │                                         │
│   Memory freed     │                                         │
│   Value on disk    │                                         │
│   Key+meta in RAM  │                                         │
│        │           │                                         │
│        └───────────┴──▶ Retry write                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

Note: Eviction sampling must **skip entries that are already `Warm`** — their values are already on disk and freeing them would only reclaim the ~80 bytes of key + metadata overhead, which is not worthwhile.

### Memory Accounting

```rust
impl HashMapStore {
    fn memory_used(&self) -> usize {
        // Only count Hot values toward maxmemory.
        // Warm entries contribute ~80 bytes each (key + metadata)
        // but their values are NOT counted.
        self.memory_used
    }
}
```

---

## Warm Tier: RocksDB Integration

The warm tier reuses FrogDB's existing RocksDB persistence infrastructure with a dedicated column family:

### Column Family Layout

| Column Family | Purpose |
|---------------|---------|
| `shard_N` | Persistence (WAL recovery) |
| `tiered_warm` | Warm tier value storage |

Using a separate column family allows:
- Independent compaction settings (warm tier data is write-heavy during demotion bursts)
- Different compression (warm tier uses heavier compression since data is accessed less frequently)
- Clear separation of persistence vs. tiering concerns

### Initialization

```rust
impl HashMapStore {
    /// Initialize warm tier using existing RocksDB instance.
    fn init_warm_tier(
        rocks: &RocksStore,
        config: &WarmTierConfig,
    ) -> Result<(), StorageError> {
        // Create dedicated column family for warm tier
        rocks.create_column_family(&config.column_family)?;
        Ok(())
    }
}
```

### Serialization

Warm tier uses the same serialization format as persistence — no new serialization code needed:

```rust
/// Serialize a value for warm tier storage.
/// Reuses existing persistence serialization.
fn serialize_value(value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    persistence::serialize_value(value, &mut buf);
    buf
}

/// Deserialize a value from warm tier storage.
fn deserialize_value(data: &[u8]) -> Result<Value, DeserializeError> {
    persistence::deserialize_value(data)
}
```

Note: Only the **value** is serialized to the warm tier. Metadata (TTL, LRU/LFU counters, type info) stays in the in-memory `Entry` and is never written to the warm column family.

---

## SCAN Command Behavior

SCAN naturally covers both tiers because all keys (hot and warm) are in the same in-memory `HashMap<Bytes, Entry>`:

```rust
impl HashMapStore {
    fn scan(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        // Unchanged — iterates self.data which contains ALL keys
        // (both hot and warm). No special cursor encoding needed.
        // Both tiers are local, so SCAN is always fast.
        self.data.scan(cursor, count, pattern)
    }
}
```

This is a significant simplification over the three-tier SCAN which required cursor encoding to track tier transitions (2 bits for tier + 62 bits for tier-specific cursor) and could be slow when scanning the cold tier.

---

## Shard Integration

### ShardWorker Changes

```rust
pub struct ShardWorker {
    /// Store — HashMapStore with optional warm tier support
    store: HashMapStore,

    /// RocksDB instance for warm tier (shared with persistence)
    warm_rocks: Option<Arc<RocksStore>>,

    // ... other fields unchanged
}

impl ShardWorker {
    fn execute_command(&mut self, cmd: &Command, args: &[Bytes]) -> Response {
        // Store trait is unchanged — tiering is transparent.
        // GET may trigger a promotion; SET always writes to hot tier.
        let mut ctx = CommandContext {
            store: &mut self.store,
            // ...
        };
        cmd.execute(&mut ctx, args)
    }
}
```

### SET Behavior

Writes always go to the hot tier. If the key was previously demoted, the warm copy is cleaned up:

```rust
impl Store for HashMapStore {
    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        // If key exists and is Warm, remove from RocksDB
        if let Some(entry) = self.data.get(&key) {
            if !entry.is_hot() {
                if let Some(warm) = &self.warm_store {
                    warm.delete_cf(WARM_CF, &key).ok();
                }
            }
        }

        // Write to hot tier (standard path)
        self.data.insert(key, Entry {
            location: ValueLocation::Hot(Arc::new(value)),
            metadata: KeyMetadata::new(),
        })
    }
}
```

### DELETE Behavior

Deletes remove the key from both RAM and warm tier:

```rust
impl Store for HashMapStore {
    fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.remove(key) {
            // If value was warm, clean up RocksDB
            if !entry.is_hot() {
                if let Some(warm) = &self.warm_store {
                    warm.delete_cf(WARM_CF, key).ok();
                }
            }
            true
        } else {
            false
        }
    }
}
```

---

## Configuration

### TOML Configuration

```toml
[tiered_storage]
enabled = false

[tiered_storage.hot]
# Memory limit for hot tier (triggers demotion when exceeded).
# When tiered storage is enabled, this replaces the global maxmemory setting.
maxmemory = "1gb"

[tiered_storage.warm]
# Column family name for warm tier data in the existing RocksDB instance.
column_family = "tiered_warm"
# Compression for warm tier (heavier than hot since data is accessed less).
compression = "lz4"

[tiered_storage.policy]
# Eviction policy when tiered storage is enabled.
# Should be "tiered-lru" or "tiered-lfu" for demotion behavior.
eviction_policy = "tiered-lru"
```

### CONFIG GET/SET Support

```
CONFIG GET tiered_storage.enabled
CONFIG GET tiered_storage.hot.maxmemory
CONFIG SET tiered_storage.hot.maxmemory 2gb
```

### Configuration Struct

```rust
pub struct TieredConfig {
    /// Enable tiered storage
    pub enabled: bool,

    /// Hot tier memory limit (triggers demotion when exceeded).
    /// Replaces maxmemory when tiered storage is enabled.
    pub hot_maxmemory: u64,

    /// Warm tier settings
    pub warm: WarmTierConfig,

    /// Eviction policy (should be TieredLru or TieredLfu)
    pub eviction_policy: EvictionPolicy,
}

pub struct WarmTierConfig {
    /// Column family name for warm tier data
    pub column_family: String,

    /// Compression algorithm for warm tier
    pub compression: CompressionType,
}
```

---

## Recovery Behavior

### Startup with Tiered Storage

On startup, all keys are loaded into RAM with their metadata, but values start as `Warm`:

```
1. Scan RocksDB warm column family for all keys
2. For each key: load key + metadata into HashMapStore as Entry { location: Warm, metadata }
3. Hot tier starts with all keys but no values in RAM
4. Values are promoted lazily on first access
```

This gives fast startup (no need to deserialize all values) and naturally adapts to access patterns — frequently accessed keys are promoted quickly.

### Index Reconstruction

```rust
impl HashMapStore {
    /// Rebuild store from warm tier on startup.
    ///
    /// Loads all keys and metadata into RAM, marking values as Warm.
    /// Values will be promoted lazily on access.
    fn rebuild_from_warm(&mut self, warm: &RocksStore) -> Result<(), StorageError> {
        let iter = warm.iter_cf(WARM_CF)?;

        for (key, serialized_value) in iter {
            // Extract metadata from the serialized entry
            // (or store metadata separately in a metadata CF)
            let metadata = extract_metadata(&key, &serialized_value)?;

            self.data.insert(Bytes::from(key), Entry {
                location: ValueLocation::Warm,
                metadata,
            });
        }

        Ok(())
    }
}
```

### Persistence Interaction

The warm tier and persistence (WAL) are independent:
- **Persistence** captures the full key+value for durability/recovery
- **Warm tier** stores demoted values for memory overflow

On crash recovery, data is restored from the WAL/RocksDB persistence column families as usual. If tiered storage is enabled, the warm column family may also contain demoted values that need to be reconciled — keys present in both persistence and warm should prefer the persistence copy (it's newer).

---

## Replication

Tiered storage is **primary-local** — it does not affect the replication protocol:

- **Primary** demotes/promotes values locally based on its own memory pressure
- **Replication stream** sends standard SET/DEL commands as before
- **Replicas** receive full values via replication (they may independently decide to tier based on their own memory pressure)

A demoted key on the primary is invisible to replicas — they just see the key as if it were hot. This means replicas may have different hot/warm distributions than the primary, which is fine since tiering is a local memory optimization.

---

## Latency Characteristics

| Operation | Hot Key | Warm Key | Notes |
|-----------|---------|----------|-------|
| GET | < 1μs | ~50–200μs | NVMe point read + deserialization |
| EXISTS | < 1μs | < 1μs | Key always in RAM |
| TTL | < 1μs | < 1μs | Metadata always in RAM |
| SCAN | < 1μs/key | < 1μs/key | Keys always in RAM, no value fetch |
| SET | < 1μs | < 1μs + RocksDB delete | Warm cleanup if overwriting demoted key |
| DEL | < 1μs | < 1μs + RocksDB delete | Warm cleanup for demoted key |

The warm key read latency (~50–200μs on NVMe) is the same order of magnitude as a network round-trip to the client, so the client-perceived latency for a warm key is roughly **2x** that of a hot key.

---

## Metrics

### Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_tiered_keys` | Gauge | tier={hot,warm}, shard | Keys in each tier |
| `frogdb_tiered_bytes` | Gauge | shard | Hot tier bytes (memory used) |
| `frogdb_tiered_promotions_total` | Counter | shard | Warm → Hot promotion count |
| `frogdb_tiered_demotions_total` | Counter | shard | Hot → Warm demotion count |
| `frogdb_tiered_warm_read_seconds` | Histogram | shard | Warm tier read latency |
| `frogdb_tiered_expired_on_promote_total` | Counter | shard | Keys found expired during promotion |

### INFO Command Extensions

```
# Tiered Storage
tiered_enabled:1
tiered_hot_keys:1000000
tiered_hot_bytes:1073741824
tiered_warm_keys:500000
tiered_promotions:12345
tiered_demotions:9876
tiered_expired_on_promote:42
```

---

## Implementation Files

### Modified Files

| File | Changes |
|------|---------|
| `crates/core/src/store/hashmap.rs` | Add `ValueLocation` enum to `Entry`, `demote_key()`, `promote_key()` |
| `crates/core/src/store/mod.rs` | Store trait: `get()` may now do disk I/O for warm keys |
| `crates/core/src/eviction/policy.rs` | Add `TieredLru`, `TieredLfu` variants |
| `crates/core/src/shard/eviction.rs` | `check_memory_for_write()`: demote instead of delete for tiered policies |
| `crates/core/src/shard/worker.rs` | Initialize warm RocksDB column family, wire through to store |
| `crates/server/src/config/mod.rs` | Add `TieredStorageConfig` section |
| `crates/server/src/server.rs` | Initialize warm column family if tiered storage enabled |

### No New Crate Needed

All changes fit within existing crates. No `tiered/` module tree, no `ColdStorage` trait, no async backend infrastructure.

---

## Complex Data Types

### Whole-Value Tiering

This spec demotes the **entire `Value` enum** as a serialized blob — all types (String, Hash, List, Set,
SortedSet, Stream, BloomFilter, HyperLogLog, TimeSeries, Json) are eligible for demotion. On
promotion, the full in-memory structure is reconstructed from the serialized form.

This is the same approach Redis Enterprise originally used and is the simplest correct design. It
works because we already serialize every type for persistence — no new serialization code is needed.

### The Partial-Access Problem

Strings are ideal for whole-value tiering because every access (`GET`, `GETRANGE`, `APPEND`) reads
or replaces the entire value. For collection types, most commands touch a fraction of the data:

| Command | Work required on a warm key |
|---------|-----------------------------|
| `GET key` (string) | Deserialize whole value — required anyway |
| `HGET key field` | Deserialize entire hash to extract one field |
| `HSET key field val` | Deserialize entire hash, modify one field, promote whole hash |
| `LINDEX key 5` | Deserialize entire list to access one element |
| `SISMEMBER key member` | Deserialize entire set for one membership check |
| `ZSCORE key member` | Deserialize entire sorted set for one score lookup |

This is why DragonflyDB limited their SSD tiering to strings only — their architecture (io_uring with
direct I/O, 4KB page binpacking per thread) doesn't lend itself to deserializing complex structures,
and strings avoid the partial-access overhead entirely.

### Why Whole-Value Is Acceptable

For most workloads, the partial-access cost is negligible:

1. **Most collections are small.** The median Redis hash/set has < 100 elements. Deserializing 100
   fields takes single-digit microseconds — negligible next to the ~50–200μs disk read.

2. **Large collections free the most memory.** A 10MB hash that gets demoted frees 10MB of RAM. The
   one-time deserialization cost on re-access is a good tradeoff for that memory savings.

3. **Eager promotion is self-correcting.** After the first access, the entire structure is back in
   RAM and all subsequent commands are fast. If a collection is accessed frequently enough for
   deserialization cost to matter, LRU/LFU would keep it hot and never demote it in the first place.

4. **The pathological case is already pathological.** A 100K-field hash where you access one field
   and never touch it again is a case where application-level partitioning (splitting the hash into
   smaller keys) is the right solution regardless of tiering.

### Future: Per-Field Tiering

A more sophisticated approach keeps collection scaffolding in RAM and demotes individual field values:

```
Hash "user:123" (10K fields)
  RAM:  HashMap<field_name → FieldLocation>   ← scaffolding stays in memory
  Disk: individual field values               ← only values on disk

HGET user:123 email → scaffolding lookup (RAM) → read one value (disk)
```

This would be more efficient for partial access to large collections, but dramatically increases
complexity:

- Every collection type needs its own tiering strategy (HashMap, VecDeque, SkipList, etc.)
- RocksDB key scheme becomes `{key}\x00{field}` — many more RocksDB keys, more compaction overhead
- `HGETALL` on a demoted hash becomes N disk reads instead of 1 whole-value read
- Sorted set range queries need the score index (SkipList/BTreeMap) in RAM with values on disk
- Per-field serialization format diverges from persistence serialization

This is a potential future optimization once whole-value tiering proves its value in production. It
would primarily benefit workloads with very large collections (10K+ elements) and partial-access
patterns.

---

## Limitations and Trade-offs

| Aspect | Trade-off |
|--------|-----------|
| **Per-key RAM overhead** | ~80 bytes per warm key (key bytes + metadata). Keys can never be fully evicted to reclaim this. |
| **Warm access latency** | ~50–200μs for NVMe reads, vs < 1μs for hot. Acceptable for infrequently accessed data. |
| **Recovery time** | Must scan warm column family on startup to rebuild key index |
| **Atomicity** | Demotion/promotion are not atomic with commands (same as current eviction) |
| **Collection deserialization** | Partial-access commands (HGET, LINDEX) on warm collections deserialize the entire value. Acceptable for typical sizes; see [Complex Data Types](#complex-data-types). |
| **No cold tier** | Very large datasets (>> local disk) still require application-level tiering to remote storage |

---

## Future Enhancements

| Enhancement | Description |
|-------------|-------------|
| **Lazy promotion** | Option to read warm values without promoting (saves memory for one-off reads) |
| **Compression** | Per-tier compression settings (heavier for warm) |
| **Key patterns** | Route specific key patterns to always stay hot |
| **Warm-only writes** | Write large values directly to warm tier |
| **Per-field tiering** | Demote individual field values within collections, keeping scaffolding in RAM |
| **Cold tier** | S3/DynamoDB backends for archival (see [INDEX.md](../todo/INDEX.md)) |

---

## References

- [STORAGE.md](STORAGE.md) — Storage layer architecture
- [EVICTION.md](EVICTION.md) — Eviction policies
- [PERSISTENCE.md](PERSISTENCE.md) — RocksDB integration
- [CONFIGURATION.md](CONFIGURATION.md) — Configuration reference
