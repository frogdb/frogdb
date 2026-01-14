# FrogDB String Commands

Binary-safe string operations. Strings are the most fundamental Redis data type - a sequence of bytes that can hold any data up to 512MB.

## Data Structure

```rust
pub struct StringValue {
    data: Bytes,
}
```

**Encoding Detection:**
- If value parses as i64, may be stored as integer-encoded (memory optimization)
- Otherwise stored as raw binary data

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| GET | O(1) | Get value |
| SET | O(1) | Set value |
| SETNX | O(1) | Set if not exists |
| SETEX | O(1) | Set with expiry (seconds) |
| PSETEX | O(1) | Set with expiry (milliseconds) |
| MGET | O(N) | Get multiple keys |
| MSET | O(N) | Set multiple keys |
| MSETNX | O(N) | Set multiple if none exist |
| APPEND | O(1)* | Append to value |
| STRLEN | O(1) | Get length |
| INCR | O(1) | Increment by 1 |
| DECR | O(1) | Decrement by 1 |
| INCRBY | O(1) | Increment by amount |
| DECRBY | O(1) | Decrement by amount |
| INCRBYFLOAT | O(1) | Increment as float |
| GETRANGE | O(N) | Get substring |
| SETRANGE | O(N) | Overwrite substring |
| GETSET | O(1) | Set and return old value (deprecated) |
| GETDEL | O(1) | Get and delete |
| GETEX | O(1) | Get and set expiration |

*APPEND is amortized O(1) due to buffer reallocation.

---

## Command Details

### GET

Get the value of a key.

```
GET key
```

| Aspect | Behavior |
|--------|----------|
| Returns | Bulk string: the value, or nil if key doesn't exist |
| Wrong type | Error: `WRONGTYPE Operation against a key holding the wrong kind of value` |

### SET

Set key to hold a string value.

```
SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] [NX | XX] [GET]
```

| Option | Meaning |
|--------|---------|
| EX seconds | Set expiry in seconds |
| PX milliseconds | Set expiry in milliseconds |
| EXAT timestamp | Set expiry as Unix timestamp (seconds) |
| PXAT timestamp | Set expiry as Unix timestamp (milliseconds) |
| KEEPTTL | Retain existing TTL |
| NX | Only set if key does not exist |
| XX | Only set if key already exists |
| GET | Return the old value (like GETSET) |

Returns: `OK` on success, nil if NX/XX condition not met.

**Examples:**
```
> SET mykey "Hello"
OK

> SET mykey "World" XX
OK

> SET newkey "Value" NX
OK

> SET newkey "Other" NX
(nil)

> SET session "data" EX 3600
OK

> SET counter 100 GET
(nil)

> SET counter 200 GET
"100"
```

### SETNX

Set if not exists. Equivalent to `SET key value NX`.

```
SETNX key value
```

Returns: `1` if set, `0` if key already exists.

### SETEX / PSETEX

Set with expiration.

```
SETEX key seconds value
PSETEX key milliseconds value
```

Equivalent to `SET key value EX seconds` / `SET key value PX milliseconds`.

### MGET

Get values of multiple keys.

```
MGET key [key ...]
```

Returns: Array of values (nil for non-existent keys).

**Cross-shard:** Same hash slot required by default. See [Cross-Shard Behavior](#cross-shard-behavior) for config options.

**Example:**
```
> SET key1 "Hello"
OK
> SET key2 "World"
OK
> MGET key1 key2 nonexistent
1) "Hello"
2) "World"
3) (nil)
```

### MSET

Set multiple keys.

```
MSET key value [key value ...]
```

Returns: Always `OK` (never fails for existing keys).

**Cross-shard:** Same hash slot required by default (atomic). See [Cross-Shard Behavior](#cross-shard-behavior) for config options.

### MSETNX

Set multiple keys only if none exist.

```
MSETNX key value [key value ...]
```

Returns: `1` if all keys were set, `0` if any key already existed.

**Cross-shard:** Always requires same hash slot (atomic semantics cannot be guaranteed across shards).

### APPEND

Append to existing value or create if doesn't exist.

```
APPEND key value
```

Returns: Integer length of the string after append.

### STRLEN

Get length of string value.

```
STRLEN key
```

Returns: Integer length, or `0` if key doesn't exist.

### INCR / DECR

Increment or decrement value as integer.

```
INCR key
DECR key
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: value after operation |
| Non-existent | Initialized to `0`, then incremented |
| Not integer | Error: `ERR value is not an integer or out of range` |
| Overflow | Error on 64-bit signed integer overflow |

**Example:**
```
> SET counter 10
OK
> INCR counter
(integer) 11
> INCR counter
(integer) 12
> INCR newcounter
(integer) 1
> SET mykey "not-a-number"
OK
> INCR mykey
(error) ERR value is not an integer or out of range
```

### INCRBY / DECRBY

Increment or decrement by specified amount.

```
INCRBY key increment
DECRBY key decrement
```

Same behavior as INCR/DECR with custom increment value.

### INCRBYFLOAT

Increment as floating point.

```
INCRBYFLOAT key increment
```

| Aspect | Behavior |
|--------|----------|
| Returns | Bulk string: value after operation |
| Precision | Double precision (IEEE 754) |
| Non-existent | Initialized to `0.0` |
| Representation | Trailing zeros removed, exponential notation when needed |

### GETRANGE

Get substring of string value.

```
GETRANGE key start end
```

| Aspect | Behavior |
|--------|----------|
| Indexing | 0-based, negative indexes from end (-1 = last) |
| Out of range | Clamped to string bounds |
| Non-existent | Returns empty string |

### SETRANGE

Overwrite part of string starting at offset.

```
SETRANGE key offset value
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: length of string after operation |
| Padding | Zero-bytes if offset > current length |
| Non-existent | Treated as empty string |

### GETSET (Deprecated)

Set value and return old value. Use `SET key value GET` instead.

```
GETSET key value
```

### GETDEL

Get value and delete the key atomically.

```
GETDEL key
```

Returns: Bulk string value, or nil if key doesn't exist.

### GETEX

Get value and optionally set expiration.

```
GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
```

---

## Cross-Shard Behavior

### Single-Key Commands

All single-key string commands (GET, SET, INCR, APPEND, etc.) are routed directly to the owning shard.

### Multi-Key Commands

By default, FrogDB enforces CROSSSLOT validation for Redis Cluster compatibility:

| Command | Default Behavior | With `allow_cross_slot_standalone` |
|---------|------------------|-----------------------------------|
| MGET | Same hash slot required | Scatter-gather across shards |
| MSET | Same hash slot required (atomic) | VLL-coordinated (atomic) |
| MSETNX | Same hash slot required | Same hash slot required (always) |

**Note**: MSETNX always requires same-slot because its atomic semantics (set all or none) cannot be guaranteed across shards.

### CROSSSLOT Validation

When keys hash to different slots, FrogDB returns:

```
-CROSSSLOT Keys in request don't hash to the same slot
```

This matches Redis Cluster behavior and ensures applications work unchanged when migrating to a clustered deployment.

### Hash Tags for Multi-Key Operations

Use hash tags `{tag}` to ensure keys hash to the same slot:

```
MSET {user:123}:name "Alice" {user:123}:email "alice@example.com"
MGET {user:123}:name {user:123}:email
```

### Optional Cross-Slot Mode

For single-node convenience, set `allow_cross_slot_standalone = true` in config. This enables scatter-gather for MGET/MSET across shards:

```
Client: MGET key1 key2 key3  (different slots)
         │
         ▼
    Coordinator (receiving thread)
         │
         ├── Hash keys to shards
         │   key1 -> Shard 0
         │   key2 -> Shard 2
         │   key3 -> Shard 0
         │
         ├── Send requests to each unique shard
         │   Shard 0: [key1, key3]
         │   Shard 2: [key2]
         │
         ├── Await all responses (fail-all semantics)
         │
         └── Reorder results by original key position
              │
              ▼
         Response: [val1, val2, val3]
```

**Note**: With `allow_cross_slot_standalone`, MSET uses VLL coordination for atomic execution across shards. MGET uses scatter-gather for efficient reads. MSETNX always requires same-slot because its "set all or none" semantics require atomicity.

---

## Persistence

All string operations follow FrogDB's unified persistence model:
- WAL writes for durability (SET, APPEND, INCR, etc.)
- String values included in snapshots
- Binary data preserved exactly across crashes

See [PERSISTENCE.md](../PERSISTENCE.md) for configuration.

---

## Implementation Notes

### Memory Efficiency

- Strings use the `bytes` crate for zero-copy operations where possible
- Integer-encoded strings may use less memory than raw bytes
- Consider using Redis protocol bulk strings for large values

### Limits

- Maximum string size: 512 MB (Redis compatible)
- Integer range: signed 64-bit (-9223372036854775808 to 9223372036854775807)

### Crate Dependencies

```toml
bytes = "1.5"           # Efficient byte buffers
```
