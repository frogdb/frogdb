# FrogDB String Commands

Binary-safe string operations. Strings are the most fundamental Redis data type - a sequence of bytes that can hold any data up to 512MB.

## Data Structure

```rust
pub struct FrogString {
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

**Cross-shard:** Uses scatter-gather - keys may be on different shards.

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

**Cross-shard:** Uses scatter-gather. Not atomic across shards.

### MSETNX

Set multiple keys only if none exist.

```
MSETNX key value [key value ...]
```

Returns: `1` if all keys were set, `0` if any key already existed.

**Cross-shard:** Requires all keys on same shard (use hash tags).

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

| Command | Behavior |
|---------|----------|
| MGET | Scatter-gather: fetch from each shard, combine results preserving order |
| MSET | Scatter-gather: set on each shard (not atomic across shards) |
| MSETNX | **Requires same shard** - use hash tags for atomicity |

### Scatter-Gather for MGET/MSET

```
Client: MGET key1 key2 key3
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

### Hash Tags for Atomicity

For atomic multi-key operations, ensure all keys hash to the same shard:

```
MSETNX {user:123}:name "Alice" {user:123}:email "alice@example.com"
```

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
