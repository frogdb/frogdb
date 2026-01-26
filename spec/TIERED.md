# FrogDB Tiered Storage

This document specifies tiered storage architecture for FrogDB: Hot (in-memory), Warm (SSD/RocksDB), and Cold (remote object storage).

## Overview

Tiered storage automatically moves data between storage tiers based on access patterns:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Tiered Storage Architecture                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   HOT (Memory)         WARM (SSD)           COLD (Remote)            │
│   ┌──────────┐        ┌──────────┐        ┌──────────────┐           │
│   │HashMapSt │  ───▶  │ RocksDB  │  ───▶  │ S3/DynamoDB  │           │
│   │  (fast)  │ demote │  (local) │ demote │   (cheap)    │           │
│   └──────────┘        └──────────┘        └──────────────┘           │
│        ▲                   │                    │                     │
│        │      promote      │       promote      │                     │
│        └───────────────────┴────────────────────┘                     │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

| Tier | Storage | Latency | Cost | Use Case |
|------|---------|---------|------|----------|
| **Hot** | In-memory (HashMapStore) | < 1μs | $$$ | Frequently accessed data |
| **Warm** | Local SSD (RocksDB) | < 1ms | $$ | Recently accessed, overflow |
| **Cold** | Remote (S3, DynamoDB) | 10-100ms | $ | Archival, rarely accessed |

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Promotion strategy** | Eager | Accessed cold/warm keys immediately promoted to hot |
| **Eviction integration** | Demote instead of delete | When hot tier hits maxmemory, demote to warm/cold |
| **Warm tier backend** | RocksDB | Reuse existing persistence infrastructure |
| **Cold tier** | Pluggable trait | Support S3, DynamoDB, or custom backends |
| **Tier metadata** | In-memory index | Fast O(1) lookup of key location |
| **Async model** | Block until complete | Simple semantics, connection waits for cold read |
| **TTL keys** | Eligible for demotion | Expiry checked on promotion |

---

## New Abstractions

### TierLocation Enum

```rust
/// Identifies which storage tier a key resides in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierLocation {
    /// In-memory HashMapStore (fastest)
    Hot,
    /// Local SSD via RocksDB (fast, persistent)
    Warm,
    /// Remote object storage (slow, cheapest)
    Cold,
}
```

### TierIndex

Tracks the location of keys not in the hot tier:

```rust
/// Index mapping keys to their storage tier
///
/// Note: Hot tier keys are NOT tracked here - they exist in HashMapStore.
/// This index only tracks warm and cold tier keys.
pub struct TierIndex {
    /// Key -> tier location (warm or cold only)
    locations: HashMap<Bytes, TierLocation>,

    /// Statistics
    warm_count: usize,
    cold_count: usize,
}

impl TierIndex {
    /// Check if a key exists in warm or cold tier
    pub fn get(&self, key: &[u8]) -> Option<TierLocation> {
        self.locations.get(key).copied()
    }

    /// Track a key as being in warm or cold tier
    pub fn insert(&mut self, key: Bytes, location: TierLocation) {
        debug_assert!(location != TierLocation::Hot);
        // Update counts
        if let Some(old) = self.locations.insert(key, location) {
            self.decrement_count(old);
        }
        self.increment_count(location);
    }

    /// Remove tracking when key is promoted to hot or deleted
    pub fn remove(&mut self, key: &[u8]) -> Option<TierLocation> {
        if let Some(loc) = self.locations.remove(key) {
            self.decrement_count(loc);
            Some(loc)
        } else {
            None
        }
    }
}
```

### ColdStorage Trait

Pluggable interface for remote storage backends:

```rust
use async_trait::async_trait;

/// Trait for cold storage backends (S3, DynamoDB, etc.)
///
/// All operations are async since cold storage involves network I/O.
#[async_trait]
pub trait ColdStorage: Send + Sync {
    /// Retrieve a value and its metadata from cold storage
    async fn get(&self, key: &[u8]) -> Result<Option<ColdEntry>, ColdStorageError>;

    /// Store a value with metadata in cold storage
    async fn set(&self, key: &[u8], entry: &ColdEntry) -> Result<(), ColdStorageError>;

    /// Delete a key from cold storage
    async fn delete(&self, key: &[u8]) -> Result<bool, ColdStorageError>;

    /// Check if a key exists (without retrieving value)
    async fn contains(&self, key: &[u8]) -> Result<bool, ColdStorageError>;

    /// Scan keys with cursor-based pagination
    /// Returns (next_cursor, keys) where empty cursor means end
    async fn scan(
        &self,
        cursor: &str,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> Result<(String, Vec<Bytes>), ColdStorageError>;

    /// Batch get for efficiency (optional, default calls get() in loop)
    async fn batch_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<ColdEntry>>, ColdStorageError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }
}

/// Entry stored in cold storage
pub struct ColdEntry {
    pub value: Value,
    pub metadata: KeyMetadata,
    /// Unix timestamp (ms) when demoted to cold
    pub demoted_at: u64,
}

/// Errors from cold storage operations
#[derive(Debug, thiserror::Error)]
pub enum ColdStorageError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("timeout after {0}ms")]
    Timeout(u64),
    #[error("key not found")]
    NotFound,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("backend error: {0}")]
    Backend(String),
}
```

### TieredStore

Wraps hot, warm, and cold tiers behind the `Store` trait:

```rust
/// Tiered storage implementation
///
/// Provides transparent access to keys across all tiers.
/// Keys are automatically promoted on access and demoted on memory pressure.
pub struct TieredStore {
    /// Hot tier: in-memory HashMap (fastest)
    hot: HashMapStore,

    /// Warm tier: local RocksDB (optional)
    warm: Option<Arc<RocksStore>>,

    /// Cold tier: remote storage (optional)
    cold: Option<Arc<dyn ColdStorage>>,

    /// Index tracking warm/cold key locations
    index: TierIndex,

    /// Tiered storage configuration
    config: TieredConfig,

    /// Tokio runtime handle for blocking on async cold operations
    runtime: tokio::runtime::Handle,

    /// Metrics
    metrics: TieredMetrics,
}

impl Store for TieredStore {
    fn get(&self, key: &[u8]) -> Option<Value> {
        // 1. Check hot tier first
        if let Some(value) = self.hot.get(key) {
            return Some(value);
        }

        // 2. Check tier index for warm/cold location
        match self.index.get(key) {
            Some(TierLocation::Warm) => self.get_from_warm_and_promote(key),
            Some(TierLocation::Cold) => self.get_from_cold_and_promote(key),
            None => None, // Key doesn't exist
            Some(TierLocation::Hot) => unreachable!(),
        }
    }

    fn set(&mut self, key: Bytes, value: Value) -> Option<Value> {
        // Always write to hot tier
        // If key was in warm/cold, remove from those tiers
        if let Some(location) = self.index.remove(&key) {
            self.delete_from_tier(&key, location);
        }
        self.hot.set(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        // Delete from all tiers
        let hot_deleted = self.hot.delete(key);
        let index_removed = self.index.remove(key);

        if let Some(location) = index_removed {
            self.delete_from_tier(key, location);
        }

        hot_deleted || index_removed.is_some()
    }

    fn memory_used(&self) -> usize {
        // Only report hot tier memory for maxmemory enforcement
        self.hot.memory_used()
    }

    // ... other Store methods
}
```

---

## Tier Operations

### Promotion (Cold/Warm → Hot)

When a key in warm or cold tier is accessed, it is immediately promoted to hot:

```rust
impl TieredStore {
    fn get_from_warm_and_promote(&mut self, key: &[u8]) -> Option<Value> {
        // Read from RocksDB warm tier
        let entry = self.warm.as_ref()?.get_entry(key)?;

        // Check TTL before promoting
        if let Some(expires_at) = entry.metadata.expires_at {
            if Instant::now() >= expires_at {
                // Key expired - delete and return None
                self.warm.as_ref()?.delete(key);
                self.index.remove(key);
                self.metrics.expired_on_promote.inc();
                return None;
            }
        }

        // Promote to hot tier
        self.hot.set_with_metadata(
            Bytes::copy_from_slice(key),
            entry.value.clone(),
            entry.metadata,
        );

        // Remove from warm tier and index
        self.warm.as_ref()?.delete(key);
        self.index.remove(key);

        self.metrics.promotions_warm_to_hot.inc();
        Some(entry.value)
    }

    fn get_from_cold_and_promote(&mut self, key: &[u8]) -> Option<Value> {
        let cold = self.cold.as_ref()?;

        // Block on async cold storage read
        let entry = self.runtime.block_on(async {
            cold.get(key).await
        }).ok()??;

        // Check TTL before promoting
        if let Some(expires_at) = entry.metadata.expires_at {
            if Instant::now() >= expires_at {
                // Key expired - delete from cold and return None
                let _ = self.runtime.block_on(cold.delete(key));
                self.index.remove(key);
                self.metrics.expired_on_promote.inc();
                return None;
            }
        }

        // Promote to hot tier
        self.hot.set_with_metadata(
            Bytes::copy_from_slice(key),
            entry.value.clone(),
            entry.metadata,
        );

        // Remove from cold tier and index
        let _ = self.runtime.block_on(cold.delete(key));
        self.index.remove(key);

        self.metrics.promotions_cold_to_hot.inc();
        Some(entry.value)
    }
}
```

### Demotion (Hot → Warm/Cold)

When memory pressure triggers eviction, keys are demoted instead of deleted:

```rust
impl TieredStore {
    /// Demote a key from hot tier to warm (or cold if warm unavailable)
    pub fn demote_key(&mut self, key: &[u8]) -> Result<usize, DemotionError> {
        // Get entry from hot tier
        let entry = self.hot.get_entry(key)
            .ok_or(DemotionError::KeyNotFound)?;

        let memory_freed = entry.metadata.memory_size;

        // Try warm tier first
        if let Some(warm) = &self.warm {
            warm.set_entry(key, &entry)?;
            self.index.insert(Bytes::copy_from_slice(key), TierLocation::Warm);
            self.hot.delete(key);
            self.metrics.demotions_hot_to_warm.inc();
            return Ok(memory_freed);
        }

        // Fall back to cold tier
        if let Some(cold) = &self.cold {
            let cold_entry = ColdEntry {
                value: entry.value,
                metadata: entry.metadata,
                demoted_at: current_unix_ms(),
            };

            self.runtime.block_on(async {
                cold.set(key, &cold_entry).await
            })?;

            self.index.insert(Bytes::copy_from_slice(key), TierLocation::Cold);
            self.hot.delete(key);
            self.metrics.demotions_hot_to_cold.inc();
            return Ok(memory_freed);
        }

        Err(DemotionError::NoTierAvailable)
    }
}
```

---

## Eviction Integration

### TieredLru and TieredLfu Policies

New eviction policies that demote instead of delete:

```rust
/// Eviction policy variants
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

    // New tiered policies (demote keys)
    /// Demote least recently used keys instead of deleting
    TieredLru,
    /// Demote least frequently used keys instead of deleting
    TieredLfu,
}
```

### Eviction Flow with Tiering

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Tiered Eviction Flow                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   Write Operation                                                     │
│        │                                                              │
│        ▼                                                              │
│   Check Memory: hot.memory_used() > config.hot_maxmemory ?           │
│        │                                                              │
│        ├── No ──▶ Proceed with write                                 │
│        │                                                              │
│        ▼ Yes                                                          │
│   Select victim key (LRU/LFU sampling)                               │
│        │                                                              │
│        ▼                                                              │
│   ┌────────────────────────────────────────────┐                     │
│   │ Policy == TieredLru or TieredLfu ?         │                     │
│   └────────────────┬───────────────────────────┘                     │
│        │           │                                                  │
│        │ Yes       │ No (standard policy)                            │
│        ▼           ▼                                                  │
│   demote_key()   delete_key()                                        │
│        │           │                                                  │
│        ▼           │                                                  │
│   Memory freed     │                                                  │
│   Data preserved   │                                                  │
│        │           │                                                  │
│        └───────────┴──▶ Retry write                                  │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Memory Threshold Configuration

```rust
/// Configuration for tiered storage
pub struct TieredConfig {
    /// Enable tiered storage
    pub enabled: bool,

    /// Hot tier memory limit (triggers demotion when exceeded)
    /// This replaces maxmemory when tiered storage is enabled
    pub hot_maxmemory: u64,

    /// Warm tier settings
    pub warm: WarmTierConfig,

    /// Cold tier settings
    pub cold: ColdTierConfig,

    /// Eviction policy (should be TieredLru or TieredLfu for tiered storage)
    pub eviction_policy: EvictionPolicy,
}

pub struct WarmTierConfig {
    pub enabled: bool,
    /// Use existing RocksDB persistence or separate instance
    pub use_persistence_rocks: bool,
    /// Column family name for warm tier data
    pub column_family: String,
}

pub struct ColdTierConfig {
    pub enabled: bool,
    /// Backend type: "s3", "dynamodb", "noop", etc.
    pub backend: String,
    /// Backend-specific configuration (JSON)
    pub backend_config: serde_json::Value,
    /// Timeout for cold storage operations (ms)
    pub timeout_ms: u64,
}
```

---

## Warm Tier: RocksDB Integration

The warm tier reuses FrogDB's existing RocksDB infrastructure:

```rust
impl TieredStore {
    /// Initialize warm tier using existing RocksDB instance
    fn init_warm_tier(
        rocks: Arc<RocksStore>,
        config: &WarmTierConfig,
    ) -> Result<(), StorageError> {
        // Create dedicated column family for warm tier
        // Separate from persistence column families (shard_0, shard_1, etc.)
        rocks.create_column_family(&config.column_family)?;
        Ok(())
    }
}
```

### Warm Tier Column Family

| Column Family | Purpose |
|---------------|---------|
| `shard_N` | Persistence (WAL recovery) |
| `tiered_warm` | Warm tier storage |

Using a separate column family allows:
- Independent compaction settings
- Different compression (warm tier may use heavier compression)
- Clear separation of persistence vs. tiering concerns

### Serialization

Warm tier uses the same serialization as persistence:

```rust
/// Serialize entry for warm/cold storage
fn serialize_entry(entry: &Entry) -> Vec<u8> {
    // Reuse existing persistence serialization
    let mut buf = Vec::new();
    serialize_value(&entry.value, &mut buf);
    serialize_metadata(&entry.metadata, &mut buf);
    buf
}

/// Deserialize entry from warm/cold storage
fn deserialize_entry(data: &[u8]) -> Result<Entry, DeserializeError> {
    let (value, rest) = deserialize_value(data)?;
    let metadata = deserialize_metadata(rest)?;
    Ok(Entry { value, metadata })
}
```

---

## Cold Tier: Backend Interface

### NoopColdStorage (Testing)

```rust
/// No-op cold storage for testing and disabled cold tier
pub struct NoopColdStorage;

#[async_trait]
impl ColdStorage for NoopColdStorage {
    async fn get(&self, _key: &[u8]) -> Result<Option<ColdEntry>, ColdStorageError> {
        Ok(None)
    }

    async fn set(&self, _key: &[u8], _entry: &ColdEntry) -> Result<(), ColdStorageError> {
        Ok(())
    }

    async fn delete(&self, _key: &[u8]) -> Result<bool, ColdStorageError> {
        Ok(false)
    }

    async fn contains(&self, _key: &[u8]) -> Result<bool, ColdStorageError> {
        Ok(false)
    }

    async fn scan(
        &self,
        _cursor: &str,
        _count: usize,
        _pattern: Option<&[u8]>,
    ) -> Result<(String, Vec<Bytes>), ColdStorageError> {
        Ok((String::new(), vec![]))
    }
}
```

### Future Backend Implementations

| Backend | Module | Key Characteristics |
|---------|--------|---------------------|
| S3 | `tiered/backends/s3.rs` | Cheap, high latency, good for large values |
| DynamoDB | `tiered/backends/dynamodb.rs` | Lower latency, item size limits |
| Azure Blob | `tiered/backends/azure.rs` | Azure-native workloads |
| GCS | `tiered/backends/gcs.rs` | GCP-native workloads |

Backend implementations are deferred - only the trait is defined initially.

---

## Shard Integration

### ShardWorker Changes

```rust
pub struct ShardWorker {
    /// Tiered store (replaces HashMapStore when tiering enabled)
    store: TieredStore,  // Was: HashMapStore

    /// Tokio runtime handle for async cold operations
    runtime: tokio::runtime::Handle,

    // ... other fields unchanged
}

impl ShardWorker {
    fn execute_command(&mut self, cmd: &Command, args: &[Bytes]) -> Response {
        // Store trait is unchanged - tiering is transparent
        let mut ctx = CommandContext {
            store: &mut self.store,
            // ...
        };
        cmd.execute(&mut ctx, args)
    }
}
```

### Background Demotion Task

Proactively demote warm tier keys to cold tier:

```rust
impl ShardWorker {
    /// Background task: demote warm keys to cold based on age
    async fn background_warm_to_cold_demotion(&mut self) {
        if !self.config.tiered.cold.enabled {
            return;
        }

        let threshold = Duration::from_secs(
            self.config.tiered.warm_to_cold_age_secs
        );

        // Scan warm tier for old keys
        let candidates = self.store.scan_warm_tier_by_age(threshold);

        for key in candidates.iter().take(BATCH_SIZE) {
            if let Err(e) = self.store.demote_warm_to_cold(key) {
                tracing::warn!("warm->cold demotion failed: {}", e);
            }
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
# Memory limit for hot tier (triggers demotion when exceeded)
# When tiered storage is enabled, this replaces the global maxmemory setting
maxmemory = "1gb"

[tiered_storage.warm]
enabled = true
# Use a dedicated column family in the existing RocksDB instance
column_family = "tiered_warm"
# Compression for warm tier (heavier than hot, lighter than cold)
compression = "lz4"

[tiered_storage.cold]
enabled = false
# Backend: "noop", "s3", "dynamodb" (only noop implemented initially)
backend = "noop"
# Timeout for cold storage operations
timeout_ms = 30000

# Backend-specific configuration (when backend = "s3")
# [tiered_storage.cold.s3]
# bucket = "my-frogdb-cold-storage"
# prefix = "frogdb/"
# region = "us-east-1"

# Backend-specific configuration (when backend = "dynamodb")
# [tiered_storage.cold.dynamodb]
# table = "frogdb-cold-storage"
# region = "us-east-1"

[tiered_storage.policy]
# Eviction policy when tiered storage is enabled
# Should be "tiered-lru" or "tiered-lfu" for demotion behavior
eviction_policy = "tiered-lru"

# Age threshold for automatic warm->cold demotion (0 = disabled)
warm_to_cold_age_secs = 3600  # 1 hour
```

### CONFIG GET/SET Support

```
CONFIG GET tiered_storage.enabled
CONFIG GET tiered_storage.hot.maxmemory
CONFIG SET tiered_storage.hot.maxmemory 2gb
```

---

## Metrics

### New Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_tiered_keys` | Gauge | tier, shard | Keys in each tier |
| `frogdb_tiered_bytes` | Gauge | tier, shard | Bytes in each tier (hot only) |
| `frogdb_tiered_promotions_total` | Counter | from_tier, to_tier, shard | Promotion count |
| `frogdb_tiered_demotions_total` | Counter | from_tier, to_tier, shard | Demotion count |
| `frogdb_tiered_cold_latency_ms` | Histogram | operation, shard | Cold storage latency |
| `frogdb_tiered_expired_on_promote_total` | Counter | tier, shard | Keys found expired during promotion |

### INFO Command Extensions

```
# Tiered Storage
tiered_enabled:1
tiered_hot_keys:1000000
tiered_hot_bytes:1073741824
tiered_warm_keys:500000
tiered_cold_keys:2000000
tiered_promotions_warm_to_hot:12345
tiered_promotions_cold_to_hot:678
tiered_demotions_hot_to_warm:9876
tiered_demotions_hot_to_cold:543
tiered_demotions_warm_to_cold:210
```

---

## Implementation Files

### New Files

| File | Purpose |
|------|---------|
| `crates/core/src/tiered/mod.rs` | Module root, `TierLocation` enum, re-exports |
| `crates/core/src/tiered/store.rs` | `TieredStore` implementation |
| `crates/core/src/tiered/index.rs` | `TierIndex` key location tracking |
| `crates/core/src/tiered/cold.rs` | `ColdStorage` trait, `ColdEntry`, errors |
| `crates/core/src/tiered/config.rs` | `TieredConfig` and sub-configs |
| `crates/core/src/tiered/backends/mod.rs` | Backend implementations module |
| `crates/core/src/tiered/backends/noop.rs` | `NoopColdStorage` for testing |

### Modified Files

| File | Changes |
|------|---------|
| `crates/core/src/lib.rs` | Add `pub mod tiered;` |
| `crates/core/src/store.rs` | Add `get_entry()` method to `Store` trait |
| `crates/core/src/eviction/policy.rs` | Add `TieredLru`, `TieredLfu` variants |
| `crates/core/src/eviction/mod.rs` | Add demotion logic path |
| `crates/core/src/shard.rs` | Use `TieredStore`, add background demotion task |
| `crates/server/src/config/mod.rs` | Add `TieredStorageConfig` section |
| `crates/server/src/server.rs` | Initialize tiered storage if enabled |

---

## Recovery Behavior

### Startup with Tiered Storage

```
1. Load TierIndex from warm tier (RocksDB scan)
2. Cold tier index is NOT preloaded (lazy discovery)
3. Hot tier starts empty (populated via access)
```

### Index Reconstruction

```rust
impl TieredStore {
    /// Rebuild tier index from warm tier on startup
    fn rebuild_index_from_warm(&mut self) -> Result<(), StorageError> {
        if let Some(warm) = &self.warm {
            let mut cursor = String::new();
            loop {
                let (next, keys) = warm.scan(&cursor, 1000)?;
                for key in keys {
                    self.index.insert(key, TierLocation::Warm);
                }
                if next.is_empty() {
                    break;
                }
                cursor = next;
            }
        }
        Ok(())
    }
}
```

### Cold Tier Discovery

Cold tier keys are discovered lazily:
1. Key not found in hot or warm tier
2. Key not in TierIndex
3. Check cold tier (expensive)
4. If found, add to TierIndex and promote

This avoids loading the entire cold tier inventory on startup.

---

## SCAN Command Behavior

SCAN must enumerate keys across all tiers:

```rust
impl TieredStore {
    fn scan(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        // Cursor encoding: tier (2 bits) + tier-specific cursor (62 bits)
        let tier = (cursor >> 62) as u8;
        let tier_cursor = cursor & 0x3FFFFFFFFFFFFFFF;

        match tier {
            0 => {
                // Scan hot tier
                let (next, keys) = self.hot.scan(tier_cursor, count, pattern);
                if next == 0 {
                    // Hot tier exhausted, move to warm
                    (1 << 62, keys)
                } else {
                    (next, keys)
                }
            }
            1 => {
                // Scan warm tier
                // ...
            }
            2 => {
                // Scan cold tier (expensive!)
                // ...
            }
            _ => (0, vec![]),
        }
    }
}
```

**Note:** SCAN across cold tier may be slow. Consider warning users or limiting cold tier SCAN.

---

## Limitations and Trade-offs

| Aspect | Trade-off |
|--------|-----------|
| **Cold access latency** | Blocking model adds 10-100ms latency for cold key access |
| **TierIndex memory** | ~50 bytes per warm/cold key for index overhead |
| **SCAN performance** | Full SCAN touching cold tier will be slow |
| **Consistency** | Demotion/promotion are not atomic with commands |
| **Recovery time** | Warm tier index must be rebuilt on startup |

---

## Future Enhancements

| Enhancement | Description |
|-------------|-------------|
| **Lazy promotion** | Option to read from cold without promoting |
| **Async promotion** | Non-blocking cold reads with callback |
| **Tiered SCAN** | Skip cold tier option for SCAN |
| **Key patterns** | Route specific key patterns to specific tiers |
| **Compression** | Different compression per tier |
| **Encryption** | Encrypt data in cold storage |

---

## References

- [STORAGE.md](STORAGE.md) - Storage layer architecture
- [EVICTION.md](EVICTION.md) - Eviction policies
- [PERSISTENCE.md](PERSISTENCE.md) - RocksDB integration
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration reference
