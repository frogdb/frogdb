# FrogDB Storage Layer

This document details the in-memory storage architecture, key metadata, and memory management.

## Store Trait

Each shard owns a `Store` implementation for key-value storage:

```rust
pub trait Store: Send {
    /// Get a value by key
    fn get(&self, key: &[u8]) -> Option<&FrogValue>;

    /// Set a value, returns previous value if any
    fn set(&mut self, key: Bytes, value: FrogValue) -> Option<FrogValue>;

    /// Delete a key, returns true if existed
    fn delete(&mut self, key: &[u8]) -> bool;

    /// Check if key exists
    fn contains(&self, key: &[u8]) -> bool;

    /// Get key type (string, list, set, zset, hash, stream, none)
    fn key_type(&self, key: &[u8]) -> KeyType;

    /// Number of keys
    fn len(&self) -> usize;

    /// Memory used by store (bytes)
    fn memory_used(&self) -> usize;

    /// Iterate keys (for SCAN)
    fn scan(&self, cursor: u64, count: usize, pattern: Option<&[u8]>) -> (u64, Vec<Bytes>);
}
```

## Value Types

FrogDB stores values as variants of the `FrogValue` enum:

```rust
pub enum FrogValue {
    String(FrogString),
    SortedSet(FrogSortedSet),
    Hash(FrogHash),
    List(FrogList),
    Set(FrogSet),
    Stream(FrogStream),
}
```

See individual type documentation in [types/](types/) for data structure implementations and commands.

### Supported Data Types

| Type | Implementation | Phase | Status |
|------|---------------|-------|--------|
| String | `Bytes` | 1 | Core |
| Sorted Set | `HashMap` + `BTreeMap` | 1 | Core |
| Hash | `HashMap<Bytes, Bytes>` | 2 | Planned |
| List | `VecDeque<Bytes>` | 2 | Planned |
| Set | `HashSet<Bytes>` | 2 | Planned |
| Stream | Radix tree + listpack | Future | Planned |
| Bitmap | Operations on String | Future | Planned |
| Bitfield | Operations on String | Future | Planned |
| Geospatial | Sorted Set + geohash | Future | Planned |
| JSON | `serde_json::Value` | Future | Planned |
| HyperLogLog | 12KB fixed structure | Future | Planned |
| Bloom Filter | Bit array + hashes | Future | Planned |
| Time Series | Sorted by timestamp | Future | Planned |

---

## Default Implementation

The default store uses a `HashMap` with per-key metadata:

```rust
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    memory_used: usize,
}

struct Entry {
    value: FrogValue,
    metadata: KeyMetadata,
}
```

---

## Key Metadata

Each key tracks metadata for expiry and eviction:

```rust
pub struct KeyMetadata {
    /// Expiration time (None = no expiry)
    pub expires_at: Option<Instant>,

    /// Last access time (for LRU eviction)
    /// NOTE: NOT persisted - reset to recovery time on startup
    pub last_access: Instant,

    /// Access frequency counter (for LFU eviction)
    /// Uses logarithmic counter like Redis
    /// NOTE: Persisted with value
    pub lfu_counter: u8,

    /// Approximate memory size of this entry
    pub memory_size: usize,
}
```

**Persistence Note:** The `last_access` field is NOT persisted to disk. After recovery,
all keys have fresh access timestamps (idle time = 0). This matches Redis behavior and
eviction accuracy self-corrects within minutes. See [PERSISTENCE.md](PERSISTENCE.md#lrulfu-metadata-matches-redis-behavior)
for details.

### Clock Sources

FrogDB uses different clock sources for different purposes:

| Use Case | Clock Type | Implementation | Rationale |
|----------|------------|----------------|-----------|
| **TTL checking** | Monotonic | `std::time::Instant` | Immune to clock adjustments |
| **last_access** | Monotonic | `std::time::Instant` | LRU needs relative time only |
| **Persistence format** | Wall clock | Unix timestamp ms | Portable across restarts |
| **Latency metrics** | Monotonic | `std::time::Instant` | Accurate measurement |
| **EXPIREAT command** | Wall clock | Unix timestamp | User specifies absolute time |

**Conversion on Recovery:**

```rust
// When loading persisted expiry time
fn load_expiry(persisted_unix_ms: u64) -> Option<Instant> {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if persisted_unix_ms <= now_unix {
        None // Already expired
    } else {
        let remaining_ms = persisted_unix_ms - now_unix;
        Some(Instant::now() + Duration::from_millis(remaining_ms))
    }
}
```

**Clock Skew Considerations:**
- Monotonic clocks avoid issues with NTP adjustments
- Wall clock only used at persistence boundaries
- Cluster nodes should use NTP (±1s skew acceptable for TTL)
- EXPIREAT with past timestamp immediately expires key

### LFU Counter

The `lfu_counter` uses a probabilistic logarithmic counter (Redis-compatible):

**Increment Algorithm:**
```rust
fn lfu_log_incr(counter: u8) -> u8 {
    if counter == 255 { return 255; }
    let r: f64 = random();  // 0.0 to 1.0
    let base_probability = 1.0 / (counter as f64 * LFU_LOG_FACTOR + 1.0);
    if r < base_probability { counter + 1 } else { counter }
}
```
- New keys start at 5 (not immediately evicted)
- Higher counter = lower increment probability
- `lfu_log_factor` default: 10

**Decay Algorithm:**
```rust
fn lfu_decay(counter: u8, last_access: Instant, now: Instant) -> u8 {
    let minutes_elapsed = (now - last_access).as_secs() / 60;
    let decay_amount = minutes_elapsed / LFU_DECAY_TIME;
    counter.saturating_sub(decay_amount as u8)
}
```
- `lfu_decay_time` default: 1 (decay by 1 per minute of inactivity)
- Decay applied lazily on access, not continuously

**Configuration:**

| Setting | Default | Description |
|---------|---------|-------------|
| `lfu_log_factor` | 10 | Higher = slower counter growth |
| `lfu_decay_time` | 1 | Minutes of inactivity per decay point |

This allows representing high access counts in a single byte.

---

## Memory Accounting

Memory is tracked per-key and aggregated per-shard:

```rust
impl HashMapStore {
    fn update_memory(&mut self, key: &Bytes, value: &FrogValue) -> usize {
        let size = key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>();  // HashMap overhead approximation
        size
    }
}
```

### FrogValue Memory

Each value type calculates its memory footprint:

```rust
impl FrogValue {
    pub fn memory_size(&self) -> usize {
        match self {
            FrogValue::String(s) => s.data.len(),
            FrogValue::List(l) => l.iter().map(|b| b.len()).sum(),
            FrogValue::Set(s) => s.iter().map(|b| b.len()).sum(),
            FrogValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
            FrogValue::SortedSet(z) => {
                // HashMap + BTreeMap entries
                z.scores.iter().map(|(k, _)| k.len() + 8).sum::<usize>()
                    + z.by_score.len() * 16  // BTreeMap overhead
            }
        }
    }
}
```

---

## Expiry Index

Each shard maintains an expiry index for efficient TTL handling:

```rust
pub struct ExpiryIndex {
    /// Keys with expiry, sorted by (expiration_time, key) for uniqueness
    by_time: BTreeMap<(Instant, Bytes), ()>,
    /// Quick lookup: key -> expiration time
    by_key: HashMap<Bytes, Instant>,
}

impl ExpiryIndex {
    /// Add/update expiry for a key
    pub fn set(&mut self, key: Bytes, expires_at: Instant);

    /// Remove expiry for a key
    pub fn remove(&mut self, key: &[u8]);

    /// Get expired keys up to `now`
    pub fn get_expired(&self, now: Instant) -> Vec<Bytes>;

    /// Sample N keys for active expiry
    pub fn sample(&self, n: usize) -> Vec<Bytes>;
}
```

**Note:** The `by_time` BTreeMap uses `(Instant, Bytes)` as the key instead of `BTreeMap<Instant, HashSet<Bytes>>`
to avoid HashSet allocation overhead per expiry time. Since (time, key) is unique, this provides O(log n)
insertion/removal with lower memory overhead.

### Expiry Strategy

FrogDB uses hybrid expiry (lazy + active):

1. **Lazy expiry**: Check TTL on every read; delete if expired
2. **Active expiry**: Background task samples keys periodically

See [DESIGN.md Key Expiry](INDEX.md#key-expiry-ttl) for configuration options.

---

## Persistence Integration

The store provides hooks for persistence via RocksDB:

```rust
pub trait PersistentStore: Store {
    /// Called after write operations
    fn persist(&mut self, key: &[u8], value: Option<&FrogValue>) -> Result<(), StorageError>;

    /// Load from persistence on startup
    fn load(&mut self) -> Result<(), StorageError>;
}
```

See [PERSISTENCE.md](PERSISTENCE.md) for RocksDB integration, WAL, and snapshots.

---

## References

- [INDEX.md - Data Structures](INDEX.md#data-structures)
- [COMMANDS.md](COMMANDS.md) - Command reference index
- [types/](types/) - Data type implementations and commands
- [EVICTION.md](EVICTION.md) - Memory eviction policies (planned)
- [PERSISTENCE.md](PERSISTENCE.md) - RocksDB integration
