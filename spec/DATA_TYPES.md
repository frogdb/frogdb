# FrogDB Data Types

This document details FrogDB's supported data types, their internal representations, and implementation status.

## Value Types

FrogDB stores values as variants of the `FrogValue` enum:

```rust
pub enum FrogValue {
    String(FrogString),
    SortedSet(FrogSortedSet),
    // Future: List, Hash, Set, Stream, etc.
}
```

---

## String Type

Simple byte string with optional metadata. This is the fundamental type supporting all string commands.

```rust
pub struct FrogString {
    data: Bytes,
}
```

**Characteristics:**
- Binary-safe (can contain any bytes including null)
- Max size: 512 MB (configurable via `proto-max-bulk-len`)
- Supports numeric operations (INCR, DECR) when value is parseable as integer

See [command-groups/STRING.md](command-groups/STRING.md) for string commands.

---

## Sorted Set Type

Dual-indexed structure for O(log n) operations by score and member:

```rust
pub struct FrogSortedSet {
    // Member -> Score mapping for O(1) score lookup
    members: HashMap<Bytes, f64>,
    // Score-ordered structure for range queries
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}
```

**Characteristics:**
- O(1) score lookup by member
- O(log n) range queries by score
- Supports lexicographical ordering for same-score members
- Max elements: 2^32 - 1 (~4 billion)

**Alternative:** Skip list implementation for cache-friendlier traversal (under consideration).

See [command-groups/SORTED_SET.md](command-groups/SORTED_SET.md) for sorted set commands.

---

## All Supported Data Types

| Type | Implementation | Phase | Status |
|------|---------------|-------|--------|
| String | `Bytes` | 1 | Core |
| Hash | `HashMap<Bytes, Bytes>` | 3+ | Planned |
| List | `VecDeque<Bytes>` | 3+ | Planned |
| Set | `HashSet<Bytes>` | 3+ | Planned |
| Sorted Set | `HashMap` + `BTreeMap` | 3 | Core |
| Stream | Radix tree + listpack | Future | Planned |
| Bitmap | Operations on String | Future | Planned |
| Bitfield | Operations on String | Future | Planned |
| Geospatial | Sorted Set + geohash | Future | Planned |
| JSON | `serde_json::Value` | Future | Planned |
| HyperLogLog | 12KB fixed structure | Future | Planned |
| Bloom Filter | Bit array + hashes | Future | Planned |
| Time Series | Sorted by timestamp | Future | Planned |

---

## Type Implementation Details

### Hash (Future)

Field-value maps stored as:

```rust
pub struct FrogHash {
    fields: HashMap<Bytes, Bytes>,
}
```

**Encoding:** Length-prefixed key-value pairs in persistence layer.

### List (Future)

Ordered sequences with O(1) push/pop at both ends:

```rust
pub struct FrogList {
    elements: VecDeque<Bytes>,
}
```

**Encoding:** Length-prefixed elements in persistence layer.

### Set (Future)

Unique unordered collections:

```rust
pub struct FrogSet {
    members: HashSet<Bytes>,
}
```

**Encoding:** Length-prefixed members in persistence layer.

### Stream (Future)

Append-only log with consumer groups. Complex structure involving:
- Radix tree for entry IDs
- Listpack for entry data
- Consumer group state

See [command-groups/STREAM.md](command-groups/STREAM.md) for stream design.

---

## Type Conversion and Errors

FrogDB is strictly typed. Attempting wrong-type operations returns:

```
-WRONGTYPE Operation against a key holding the wrong kind of value
```

**Type checking behavior:**

| Scenario | Result |
|----------|--------|
| GET on string | Value returned |
| GET on sorted set | WRONGTYPE error |
| ZADD on string | WRONGTYPE error |
| SET on existing sorted set | Overwrites (type changes to string) |
| DEL | Works on any type |
| TYPE | Returns type name |
| EXISTS | Works on any type |

---

## References

- [COMMANDS.md](COMMANDS.md) - Command reference and index
- [STORAGE.md](STORAGE.md) - Storage layer and key metadata
- [PERSISTENCE.md](PERSISTENCE.md) - Value serialization format
- [command-groups/](command-groups/) - Type-specific command documentation
