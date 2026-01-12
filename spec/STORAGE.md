# FrogDB Storage Layer

This document details the in-memory storage architecture, key metadata, and memory management.

## Terminology Note

> **Shard Terminology Clarification**
>
> Throughout this document, "shard" refers to **internal shards** (also called "thread shards"):
> - Thread-local partitions within a single FrogDB node
> - Each shard has its own HashMap, memory budget, and expiry index
> - Keys are assigned to shards via: `internal_shard = hash(key) % num_shards`
>
> This is distinct from **hash slots** used in cluster mode:
> - Logical partitions (0-16383) for distributing data across nodes
> - Keys are assigned to slots via: `slot = CRC16(key) % 16384`
>
> **Relationship:** `key → hash_slot(key) → node → internal_shard(key)`
>
> See [GLOSSARY.md](GLOSSARY.md) for full terminology definitions.
> See [CLUSTER.md](CLUSTER.md#internal-vs-cluster-sharding) for detailed explanation.

## Store Trait

Each shard owns a `Store` implementation for key-value storage:

```rust
pub trait Store: Send {
    /// Get a value by key (returns owned/cloned value)
    fn get(&self, key: &[u8]) -> Option<Value>;

    /// Set a value, returns previous value if any
    fn set(&mut self, key: Bytes, value: Value) -> Option<Value>;

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

### Ownership Semantics

**`Store::get()` returns `Option<Value>` (owned), not `Option<&Value>` (borrowed).**

Rationale:
- **Async compatibility**: Returning references would require complex lifetime annotations
  with async command execution. Owned values are simpler to work with across `.await` points.
- **Simplicity over optimization**: The slight overhead of cloning values is acceptable
  for Phase 1. Optimization can be added later if profiling shows it's a bottleneck.
- **Command execution pattern**: Most commands need to inspect values, compute responses,
  and potentially modify them. Owned values fit this pattern naturally.

Future optimization path: Add a `get_ref()` method returning `Option<&Value>` for
read-only operations where the caller can guarantee no `.await` across the borrow.

## Value Types

FrogDB stores values as variants of the `Value` enum:

```rust
pub enum Value {
    String(StringValue),
    SortedSet(SortedSetValue),
    Hash(HashValue),
    List(ListValue),
    Set(SetValue),
    Stream(StreamValue),
}
```

See individual type documentation in [types/](types/) for data structure implementations and commands.

### Supported Data Types

| Type | Implementation | Phase | Status |
|------|---------------|-------|--------|
| String | `Bytes` | 1 | Core |
| Sorted Set | `HashMap` + `BTreeMap` | 3 | Core |
| Hash | `HashMap<Bytes, Bytes>` | 6 | Planned |
| List | `VecDeque<Bytes>` | 6 | Planned |
| Set | `HashSet<Bytes>` | 6 | Planned |
| Stream | Radix tree + listpack | Future | Planned |
| Bitmap | Operations on String | Future | Planned |
| Bitfield | Operations on String | Future | Planned |
| Geospatial | Sorted Set + geohash | Future | Planned |
| JSON | `serde_json::Value` | Future | Planned |
| HyperLogLog | 12KB fixed structure | Future | Planned |
| Bloom Filter | Bit array + hashes | Future | Planned |
| Time Series | Sorted by timestamp | Future | Planned |

> **Note:** See [ROADMAP.md](ROADMAP.md) for authoritative phase definitions and implementation order.

---

## Default Implementation

The default store uses a `HashMap` with per-key metadata:

```rust
pub struct HashMapStore {
    data: HashMap<Bytes, Entry>,
    memory_used: usize,
}

struct Entry {
    value: Value,
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
    fn update_memory(&mut self, key: &Bytes, value: &Value) -> usize {
        let size = key.len()
            + value.memory_size()
            + std::mem::size_of::<KeyMetadata>()
            + std::mem::size_of::<Entry>();  // HashMap overhead per entry
        size
    }
}
```

### HashMap Implementation Choice

FrogDB uses [`griddle::HashMap`](https://crates.io/crates/griddle) instead of `std::collections::HashMap` to avoid memory and latency spikes during resizing.

> **The Resize Spike Problem**
>
> Standard hash tables (including Rust's `std::HashMap` and Redis's `dict`) must resize when
> the load factor exceeds a threshold. This involves:
> 1. Allocating a new table (typically 2x the size)
> 2. Rehashing and copying all entries
> 3. Deallocating the old table
>
> **Impact:**
> - **Memory spike**: Temporarily requires ~2x memory during resize
> - **Latency spike**: Single insert can take 30-40ms instead of <1μs
>
> Redis mitigates this with incremental rehashing (spreading work across operations),
> but still requires 2x memory allocation upfront.

**FrogDB's Approach: Griddle**

| Aspect | std::HashMap | griddle::HashMap |
|--------|--------------|------------------|
| Max insert latency | ~38ms | ~1.8ms |
| Mean insert latency | ~94ns | ~126ns |
| Memory during resize | 2x (allocated upfront) | 2x (but amortized) |
| API compatibility | - | Drop-in replacement |

Griddle spreads resize work across inserts, ensuring no single operation pays the full cost.
The trade-off is slightly slower reads during active resizing (must check both old and new tables).

```rust
// Usage is identical to std::HashMap
use griddle::HashMap;

let mut store: HashMap<Bytes, Value> = HashMap::new();
store.insert(key, value);
```

> **Future Optimization: Dashtable**
>
> DragonflyDB achieves even better memory efficiency with a custom "Dashtable" structure
> based on the paper "Dash: Scalable Hashing on Persistent Memory". Key benefits:
> - Per-entry overhead: ~20 bits (vs 64 bits in Redis)
> - No resize spikes: segments split independently
> - 30-60% less memory than Redis
>
> A custom Dashtable implementation for FrogDB could be considered if memory efficiency
> becomes critical. No existing Rust crate implements this algorithm.

### Value Memory

Each value type calculates its memory footprint:

```rust
impl Value {
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.data.len(),
            Value::List(l) => l.iter().map(|b| b.len()).sum(),
            Value::Set(s) => s.iter().map(|b| b.len()).sum(),
            Value::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
            Value::SortedSet(z) => {
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

## Hash Tags and Slot Validation

### Industry Comparison

| Aspect | Redis Cluster | DragonflyDB | FrogDB |
|--------|---------------|-------------|--------|
| Hash slots | 16,384 (CRC16 % 16384) | 16,384 (Redis-compatible) | 16,384 (Redis-compatible) |
| Hash tag syntax | `{tag}` | `{tag}` | `{tag}` |
| CROSSSLOT error | Multi-key ops across slots | Multi-key ops across slots | Multi-key ops across slots |
| Validation timing | Before execution | Before execution | Before execution |

All three systems follow the same Redis Cluster specification for hash slots.

### Hash Algorithms

FrogDB uses two different hash algorithms for different purposes:

| Purpose | Algorithm | Range | Description |
|---------|-----------|-------|-------------|
| **Cluster slot** | CRC16 | 0-16383 | Redis-compatible, determines which node owns a key |
| **Internal shard** | xxhash64 | 0 to num_shards | Determines which thread within a node processes a key |

```rust
// Cluster slot routing (Redis Cluster compatible)
fn hash_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16(hash_key) % 16384
}

// Internal shard routing (within a single node)
fn internal_shard(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    xxhash64(hash_key) as usize % num_shards
}
```

**Important:** Both algorithms use the same `extract_hash_tag` function. Keys with the same hash tag
will be colocated on both the same cluster slot AND the same internal shard.

### Hash Slot Calculation

```rust
/// Redis Cluster compatible hash slot calculation
fn hash_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16(hash_key) % 16384
}

/// Extract hash tag content from key
/// Rules:
/// - First occurrence of `{...}` with non-empty content is used
/// - If no valid hash tag, entire key is hashed
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|&b| b == b'{')?;
    let end = key[start + 1..].iter().position(|&b| b == b'}')?;

    if end == 0 {
        // Empty braces `{}` - hash entire key
        return None;
    }

    Some(&key[start + 1..start + 1 + end])
}
```

### Hash Tag Examples

| Key | Hash Tag | Slot Based On |
|-----|----------|---------------|
| `user:1234` | None | `user:1234` |
| `{user}:profile:1234` | `user` | `user` |
| `{user}:session:1234` | `user` | `user` (same slot as above) |
| `foo{bar}{zap}` | `bar` | `bar` (first valid match) |
| `foo{}bar` | None | `foo{}bar` (empty braces ignored) |
| `{}{user}:data` | None | `{}{user}:data` (empty first braces) |

### CROSSSLOT Validation

Multi-key operations must have all keys in the same hash slot. FrogDB validates this **before execution**:

```rust
fn validate_same_slot(keys: &[&[u8]]) -> Result<u16, Error> {
    if keys.is_empty() {
        return Ok(0);  // No keys, any slot is fine
    }

    let first_slot = hash_slot(keys[0]);

    for key in &keys[1..] {
        if hash_slot(key) != first_slot {
            return Err(Error::CrossSlot);
        }
    }

    Ok(first_slot)
}
```

**Error response:**
```
-CROSSSLOT Keys in request don't hash to the same slot
```

### Commands Requiring Same-Slot Validation

| Command Type | Examples | Validation |
|--------------|----------|------------|
| Multi-key reads | `MGET`, `EXISTS` (multi) | All keys same slot |
| Multi-key writes | `MSET`, `DEL` (multi), `RENAME` | All keys same slot |
| Transactions | `MULTI`/`EXEC` with multiple keys | All keys same slot |
| Blocking | `BLPOP key1 key2` | All keys same slot |
| Lua scripts | Keys passed to script | All keys same slot |
| Set operations | `SUNION`, `SINTER`, `SDIFF` | All keys same slot |
| Sorted set ops | `ZUNION`, `ZINTER` | All keys same slot |

### Validation Points in FrogDB

```
┌─────────────────────────────────────────────────────────────────┐
│                    CROSSSLOT Validation Flow                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Client Command ──▶ Parse ──▶ Extract Keys ──▶ Validate Slots   │
│                                                     │            │
│                              ┌──────────────────────┴──────┐     │
│                              ▼                             ▼     │
│                        Same slot?                    Different?  │
│                              │                             │     │
│                              ▼                             ▼     │
│                     Route to shard              Return CROSSSLOT │
│                              │                                   │
│                              ▼                                   │
│                        Execute                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Key principle:** Validation happens at the **command parsing stage**, before any shard receives the command. This ensures:
- No partial execution of multi-key commands
- Consistent error response regardless of key existence
- Matches Redis Cluster behavior

### Design Recommendation: Key Naming Conventions

To avoid CROSSSLOT errors, use consistent hash tags in related keys:

```
# Good: All user data in same slot
{user:1234}:profile
{user:1234}:sessions
{user:1234}:preferences

# Bad: Related data in different slots
user:1234:profile
user:1234:sessions    # Different slot!
```

See [CONSISTENCY.md](CONSISTENCY.md#cross-slot-handling) for consistency implications.

---

## Persistence Integration

The store provides hooks for persistence via RocksDB:

```rust
pub trait PersistentStore: Store {
    /// Called after write operations
    fn persist(&mut self, key: &[u8], value: Option<&Value>) -> Result<(), StorageError>;

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
