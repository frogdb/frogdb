# FrogDB Data Structures

This document specifies the implementation approach for all Redis-compatible data types in FrogDB.

## Overview

| Type | Implementation | Phase | Key Commands |
|------|---------------|-------|--------------|
| String | `Bytes` | 1 | GET, SET, APPEND, STRLEN |
| Hash | `HashMap<Bytes, Bytes>` | 3+ | HGET, HSET, HMGET, HGETALL |
| List | `VecDeque<Bytes>` | 3+ | LPUSH, RPUSH, LPOP, LRANGE |
| Set | `HashSet<Bytes>` | 3+ | SADD, SMEMBERS, SINTER, SUNION |
| Sorted Set | `HashMap` + `BTreeMap` | 3 | ZADD, ZRANGE, ZSCORE, ZRANK |
| Stream | Radix tree + listpack | Future | XADD, XREAD, XREADGROUP |
| Bitmap | Operations on String | Future | SETBIT, GETBIT, BITCOUNT |
| Bitfield | Operations on String | Future | BITFIELD |
| Geospatial | Sorted Set + geohash | Future | GEOADD, GEORADIUS, GEOPOS |
| JSON | `serde_json::Value` | Future | JSON.GET, JSON.SET |
| HyperLogLog | 12KB fixed structure | Future | PFADD, PFCOUNT, PFMERGE |
| Bloom Filter | Bit array + hashes | Future | BF.ADD, BF.EXISTS |
| Time Series | Sorted by timestamp | Future | TS.ADD, TS.RANGE |

---

## Core Types

### String

The fundamental type - a binary-safe byte sequence.

**Implementation:**
```rust
pub struct FrogString {
    data: Bytes,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| GET | O(1) | Get value |
| SET | O(1) | Set value (with EX/PX/NX/XX options) |
| SETNX | O(1) | Set if not exists |
| SETEX | O(1) | Set with expiry |
| MGET | O(N) | Get multiple keys (scatter-gather) |
| MSET | O(N) | Set multiple keys (scatter-gather) |
| APPEND | O(1) | Append to value |
| STRLEN | O(1) | Get length |
| INCR/DECR | O(1) | Increment/decrement as integer |
| INCRBY/DECRBY | O(1) | Increment/decrement by amount |
| INCRBYFLOAT | O(1) | Increment as float |
| GETRANGE | O(N) | Get substring |
| SETRANGE | O(N) | Overwrite substring |

**Encoding Detection:**
- Integer: If value parses as i64, store as integer-encoded (optimization)
- Raw: Binary data stored as-is

---

### Hash

Field-value maps within a single key.

**Implementation:**
```rust
pub struct FrogHash {
    fields: HashMap<Bytes, Bytes>,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| HSET | O(1) | Set field(s) |
| HGET | O(1) | Get field value |
| HMGET | O(N) | Get multiple fields |
| HGETALL | O(N) | Get all fields and values |
| HDEL | O(N) | Delete field(s) |
| HEXISTS | O(1) | Check field exists |
| HLEN | O(1) | Get field count |
| HKEYS | O(N) | Get all field names |
| HVALS | O(N) | Get all values |
| HINCRBY | O(1) | Increment field as integer |
| HINCRBYFLOAT | O(1) | Increment field as float |
| HSETNX | O(1) | Set field if not exists |
| HSCAN | O(1)/call | Iterate fields |

**Memory Optimization (Future):**
- Ziplist encoding for small hashes (< 512 fields, < 64 bytes each)
- Threshold configurable via `hash-max-ziplist-entries` and `hash-max-ziplist-value`

---

### List

Ordered sequence of elements with O(1) push/pop at both ends.

**Implementation:**
```rust
pub struct FrogList {
    elements: VecDeque<Bytes>,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| LPUSH/RPUSH | O(1) | Push to left/right |
| LPOP/RPOP | O(1) | Pop from left/right |
| LLEN | O(1) | Get length |
| LRANGE | O(S+N) | Get range of elements |
| LINDEX | O(N) | Get element by index |
| LSET | O(N) | Set element by index |
| LINSERT | O(N) | Insert before/after pivot |
| LREM | O(N) | Remove elements by value |
| LTRIM | O(N) | Trim to range |
| LPOS | O(N) | Find element position |
| LMOVE | O(1) | Move element between lists |
| BLPOP/BRPOP | O(1) | Blocking pop (requires special handling) |
| BLMOVE | O(1) | Blocking move |

**Blocking Commands:**
- Requires per-shard wait queue for blocked connections
- Timeout handling with configurable default
- Wake blocked connections on PUSH to watched key

---

### Set

Unordered collection of unique members.

**Implementation:**
```rust
pub struct FrogSet {
    members: HashSet<Bytes>,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| SADD | O(N) | Add member(s) |
| SREM | O(N) | Remove member(s) |
| SMEMBERS | O(N) | Get all members |
| SISMEMBER | O(1) | Check membership |
| SMISMEMBER | O(N) | Check multiple memberships |
| SCARD | O(1) | Get cardinality |
| SPOP | O(1) | Remove random member |
| SRANDMEMBER | O(N) | Get random member(s) |
| SMOVE | O(1) | Move member between sets |
| SINTER | O(N*M) | Intersection |
| SUNION | O(N) | Union |
| SDIFF | O(N) | Difference |
| SINTERSTORE | O(N*M) | Store intersection |
| SUNIONSTORE | O(N) | Store union |
| SDIFFSTORE | O(N) | Store difference |
| SSCAN | O(1)/call | Iterate members |

**Cross-Shard Operations:**
- SINTER/SUNION/SDIFF with keys on different shards require scatter-gather
- Or reject unless all keys have same hash tag

---

### Sorted Set

Members ordered by score with O(log N) operations.

**Implementation:**
```rust
pub struct FrogSortedSet {
    /// Member -> Score for O(1) lookup
    members: HashMap<Bytes, f64>,
    /// (Score, Member) for range queries
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| ZADD | O(log N) | Add member(s) with score |
| ZREM | O(log N) | Remove member(s) |
| ZSCORE | O(1) | Get score |
| ZRANK/ZREVRANK | O(log N) | Get rank |
| ZRANGE | O(log N + M) | Get range by rank |
| ZRANGEBYSCORE | O(log N + M) | Get range by score |
| ZRANGEBYLEX | O(log N + M) | Get range lexicographically |
| ZCARD | O(1) | Get cardinality |
| ZCOUNT | O(log N) | Count in score range |
| ZINCRBY | O(log N) | Increment score |
| ZPOPMIN/ZPOPMAX | O(log N) | Pop min/max |
| BZPOPMIN/BZPOPMAX | O(log N) | Blocking pop |
| ZRANGESTORE | O(log N + M) | Store range |
| ZINTER/ZUNION | O(N*K + M*log M) | Intersection/union |
| ZSCAN | O(1)/call | Iterate members |

**Alternative Implementation:**
Skip list for potentially better cache locality during traversal.

---

## Advanced Types

### Stream

Append-only log with consumer groups for reliable message processing.

**Implementation:**
```rust
pub struct FrogStream {
    /// Entries stored in radix tree for memory efficiency
    entries: RadixTree<StreamEntry>,
    /// Last generated ID
    last_id: StreamId,
    /// Consumer groups
    groups: HashMap<Bytes, ConsumerGroup>,
}

pub struct StreamEntry {
    id: StreamId,
    fields: Vec<(Bytes, Bytes)>,
}

pub struct StreamId {
    ms: u64,      // Milliseconds timestamp
    seq: u64,     // Sequence number
}

pub struct ConsumerGroup {
    name: Bytes,
    last_delivered_id: StreamId,
    /// Pending entries list (PEL)
    pending: BTreeMap<StreamId, PendingEntry>,
    consumers: HashMap<Bytes, Consumer>,
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| XADD | O(1) | Append entry |
| XREAD | O(N) | Read entries |
| XREADGROUP | O(M) | Read as consumer group |
| XRANGE/XREVRANGE | O(N) | Get range |
| XLEN | O(1) | Get length |
| XINFO | O(N) | Get stream/group info |
| XGROUP CREATE | O(1) | Create consumer group |
| XACK | O(1) | Acknowledge processing |
| XCLAIM | O(1) | Claim pending entry |
| XPENDING | O(N) | List pending entries |
| XTRIM | O(N) | Trim stream |
| XDEL | O(1) | Delete entry |

**Key Design Points:**
- Radix tree provides memory-efficient storage with ID-based indexing
- Listpack encoding for entries (field-value pairs stored compactly)
- Consumer groups enable reliable, distributed processing
- PEL tracks unacknowledged messages per consumer

---

### Bitmap

Bit operations on string values.

**Implementation:**
Bitmap commands operate on the underlying String type, treating bytes as bit arrays.

```rust
impl FrogString {
    fn setbit(&mut self, offset: usize, value: bool) {
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);  // Redis uses MSB-first
        // Extend if needed, then set bit
    }

    fn getbit(&self, offset: usize) -> bool {
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);
        // Return bit value
    }
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| SETBIT | O(1) | Set bit at offset |
| GETBIT | O(1) | Get bit at offset |
| BITCOUNT | O(N) | Count set bits |
| BITPOS | O(N) | Find first 0 or 1 bit |
| BITOP | O(N) | Bitwise AND/OR/XOR/NOT |
| BITFIELD | O(1) | Arbitrary bit field operations |

---

### Bitfield

Multi-bit integer operations on strings.

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| BITFIELD GET | O(1) | Read integer at offset |
| BITFIELD SET | O(1) | Write integer at offset |
| BITFIELD INCRBY | O(1) | Increment integer at offset |
| BITFIELD OVERFLOW | - | Set overflow behavior (WRAP/SAT/FAIL) |

**Supported Types:**
- Signed: i1 to i64
- Unsigned: u1 to u63

---

### Geospatial

Location-based operations using sorted sets with geohash scores.

**Implementation:**
```rust
// Geospatial is built on top of Sorted Set
// Score = geohash(latitude, longitude) as 52-bit integer

pub fn geohash_encode(lat: f64, lon: f64) -> u64 {
    // Interleave bits of normalized lat/lon
    // Returns 52-bit geohash
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| GEOADD | O(log N) | Add location(s) |
| GEOPOS | O(1) | Get coordinates |
| GEODIST | O(1) | Distance between members |
| GEOHASH | O(1) | Get geohash string |
| GEORADIUS | O(N+log N) | Query by radius |
| GEOSEARCH | O(N+log N) | Query by radius/box |
| GEORADIUSBYMEMBER | O(N+log N) | Query around member |
| GEOSEARCHSTORE | O(N+log N) | Store query results |

---

### JSON

Nested document storage with path-based operations.

**Implementation:**
```rust
pub struct FrogJson {
    root: serde_json::Value,
}
```

**Commands (RedisJSON compatible):**
| Command | Complexity | Description |
|---------|------------|-------------|
| JSON.SET | O(M+N) | Set value at path |
| JSON.GET | O(N) | Get value at path |
| JSON.DEL | O(N) | Delete at path |
| JSON.MGET | O(M*N) | Get from multiple keys |
| JSON.TYPE | O(1) | Get type at path |
| JSON.STRLEN | O(1) | String length at path |
| JSON.ARRAPPEND | O(1) | Append to array |
| JSON.ARRINSERT | O(N) | Insert in array |
| JSON.ARRLEN | O(1) | Array length |
| JSON.OBJKEYS | O(N) | Object keys |
| JSON.OBJLEN | O(1) | Object length |

**Path Syntax:**
- JSONPath: `$.store.books[0].title`
- Root: `$` or `.`

---

## Probabilistic Types

### HyperLogLog

Cardinality estimation with fixed 12KB memory.

**Implementation:**
```rust
pub struct FrogHyperLogLog {
    /// 16384 6-bit registers (12KB total)
    registers: [u8; 12304],  // Packed representation
}
```

**Commands:**
| Command | Complexity | Description |
|---------|------------|-------------|
| PFADD | O(1) | Add element(s) |
| PFCOUNT | O(1) | Estimate cardinality |
| PFMERGE | O(N) | Merge HLLs |

**Accuracy:** ~0.81% standard error

---

### Bloom Filter

Probabilistic membership testing.

**Implementation:**
```rust
pub struct FrogBloomFilter {
    bits: BitVec,
    num_hashes: u32,
    capacity: usize,
}
```

**Commands (Redis Stack compatible):**
| Command | Complexity | Description |
|---------|------------|-------------|
| BF.ADD | O(k) | Add element |
| BF.EXISTS | O(k) | Check membership |
| BF.MADD | O(k*n) | Add multiple |
| BF.MEXISTS | O(k*n) | Check multiple |
| BF.RESERVE | O(1) | Create with capacity/error rate |
| BF.INFO | O(1) | Get filter info |

**Trade-offs:**
- False positives possible (configurable rate)
- False negatives impossible
- No element deletion

---

### Cuckoo Filter

Alternative to Bloom with deletion support.

**Commands (Redis Stack compatible):**
| Command | Description |
|---------|-------------|
| CF.ADD | Add element |
| CF.DEL | Delete element |
| CF.EXISTS | Check membership |
| CF.RESERVE | Create with capacity |

---

### Time Series

Timestamped data points with aggregation.

**Implementation:**
```rust
pub struct FrogTimeSeries {
    /// Samples sorted by timestamp
    samples: BTreeMap<i64, f64>,
    /// Retention period (ms, 0 = forever)
    retention: u64,
    /// Labels for filtering
    labels: HashMap<Bytes, Bytes>,
}
```

**Commands (Redis Stack compatible):**
| Command | Complexity | Description |
|---------|------------|-------------|
| TS.CREATE | O(1) | Create time series |
| TS.ADD | O(1) | Add sample |
| TS.MADD | O(N) | Add multiple samples |
| TS.GET | O(1) | Get latest sample |
| TS.RANGE | O(N) | Get range |
| TS.MRANGE | O(N*M) | Multi-key range |
| TS.INFO | O(1) | Get metadata |
| TS.ALTER | O(1) | Modify metadata |

**Aggregations:** AVG, SUM, MIN, MAX, COUNT, FIRST, LAST, RANGE, STD.P, STD.S, VAR.P, VAR.S

---

## Memory Considerations

### Per-Type Memory Overhead

| Type | Overhead | Notes |
|------|----------|-------|
| String | 0 bytes | Just the data |
| Hash | ~40 bytes + fields | HashMap overhead |
| List | ~40 bytes + elements | VecDeque overhead |
| Set | ~40 bytes + members | HashSet overhead |
| Sorted Set | ~80 bytes + 2x members | Dual index |
| Stream | Variable | Depends on entry count |
| HyperLogLog | 12KB fixed | Always |

### Memory Optimization Strategies

1. **Small Encodings** (Future)
   - Ziplist for small Hashes/Lists/Sets
   - Intset for integer-only Sets
   - Listpack for Streams

2. **Shared Objects**
   - Small integers (0-10000) shared across keys
   - Common strings ("OK", "nil") shared

3. **Lazy Deletion**
   - Large structures deleted in background
   - Avoid blocking on DEL of large keys

---

## Cross-Shard Considerations

### Single-Key Commands
All commands operate on the owning shard directly.

### Multi-Key Commands
Commands operating on multiple keys require coordination:

| Pattern | Examples | Approach |
|---------|----------|----------|
| Read multiple | MGET, SMEMBERS multiple | Scatter-gather |
| Write multiple | MSET | Scatter-gather |
| Set operations | SINTER, SUNION | Require same shard (hash tags) |
| Move operations | RENAME, SMOVE | Require same shard |
| Blocking | BLPOP on multiple | Require same shard |

**Recommendation:** Use hash tags `{tag}` to colocate related keys.
