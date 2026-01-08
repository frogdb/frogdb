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
    pub last_access: Instant,

    /// Access frequency counter (for LFU eviction)
    /// Uses logarithmic counter like Redis
    pub lfu_counter: u8,

    /// Approximate memory size of this entry
    pub memory_size: usize,
}
```

### LFU Counter

The `lfu_counter` uses a probabilistic logarithmic counter (like Redis):
- Starts at 5 (new keys aren't immediately evicted)
- Increments with probability `1 / (counter * lfu_log_factor + 1)`
- Decays over time based on `lfu_decay_time`

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
    /// Keys sorted by expiration time
    by_expiry: BTreeMap<Instant, HashSet<Bytes>>,
    /// Reverse lookup: key → expiry time
    key_expiry: HashMap<Bytes, Instant>,
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

### Expiry Strategy

FrogDB uses hybrid expiry (lazy + active):

1. **Lazy expiry**: Check TTL on every read; delete if expired
2. **Active expiry**: Background task samples keys periodically

See [DESIGN.md Key Expiry](../DESIGN.md#key-expiry-ttl) for configuration options.

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

- [DESIGN.md - FrogValue](../DESIGN.md#data-structures)
- [COMMANDS.md](COMMANDS.md) - Command reference and type implementations
- [EVICTION.md](EVICTION.md) - Memory thresholds and policies
- [PERSISTENCE.md](PERSISTENCE.md) - RocksDB integration
