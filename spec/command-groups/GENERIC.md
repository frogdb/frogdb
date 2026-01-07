# FrogDB Generic Commands

Key-level operations that work across all data types. These commands manage key lifecycle, expiration, and discovery.

## Data Structure

N/A - Generic commands operate on keys of any type.

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| DEL | O(N) | Delete one or more keys |
| EXISTS | O(N) | Check if keys exist |
| EXPIRE | O(1) | Set timeout in seconds |
| PEXPIRE | O(1) | Set timeout in milliseconds |
| EXPIREAT | O(1) | Set expiration as Unix timestamp (seconds) |
| PEXPIREAT | O(1) | Set expiration as Unix timestamp (milliseconds) |
| TTL | O(1) | Get time-to-live in seconds |
| PTTL | O(1) | Get time-to-live in milliseconds |
| PERSIST | O(1) | Remove expiration |
| TYPE | O(1) | Get the type of a key |
| RENAME | O(1) | Rename a key |
| RENAMENX | O(1) | Rename only if new key doesn't exist |
| KEYS | O(N) | Find keys matching pattern |
| SCAN | O(1)/call | Cursor-based key iteration |
| TOUCH | O(N) | Update access time |
| UNLINK | O(1) | Async delete |

---

## Command Details

### DEL

Delete one or more keys.

```
DEL key [key ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: number of keys deleted |
| Multi-key | Scatter-gather across shards |
| Non-existent | Silently ignored (counts as 0) |

### EXISTS

Check if keys exist.

```
EXISTS key [key ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: count of existing keys |
| Multi-key | Scatter-gather, sums counts |
| Duplicate keys | Each counted separately |

### EXPIRE / PEXPIRE

Set key expiration in seconds / milliseconds.

```
EXPIRE key seconds [NX | XX | GT | LT]
PEXPIRE key milliseconds [NX | XX | GT | LT]
```

| Option | Meaning |
|--------|---------|
| NX | Only set if key has no expiry |
| XX | Only set if key already has expiry |
| GT | Only set if new expiry > current |
| LT | Only set if new expiry < current |

Returns: `1` if set, `0` if not set or key doesn't exist.

### EXPIREAT / PEXPIREAT

Set key expiration as Unix timestamp (seconds / milliseconds).

```
EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
```

Same options and return values as EXPIRE.

### TTL / PTTL

Get time-to-live in seconds / milliseconds.

```
TTL key
PTTL key
```

| Return Value | Meaning |
|--------------|---------|
| Positive | Remaining TTL |
| `-1` | Key exists but has no expiry |
| `-2` | Key does not exist |

### PERSIST

Remove expiration from a key.

```
PERSIST key
```

Returns: `1` if expiry removed, `0` if key has no expiry or doesn't exist.

### TYPE

Get the type of a key.

```
TYPE key
```

| Return Value | Meaning |
|--------------|---------|
| `string` | String value |
| `list` | List |
| `set` | Set |
| `zset` | Sorted set |
| `hash` | Hash |
| `stream` | Stream |
| `none` | Key doesn't exist |

### RENAME / RENAMENX

Rename a key.

```
RENAME key newkey
RENAMENX key newkey
```

| Aspect | Behavior |
|--------|----------|
| RENAME | Overwrites newkey if exists |
| RENAMENX | Fails if newkey exists |
| Returns | `OK` / `1` (success), `0` (RENAMENX fail) |
| Cross-shard | **Rejected with error** |
| Missing key | Error: `ERR no such key` |

**Cross-shard note:** Use hash tags `{tag}key` to ensure keys are on same shard.

### KEYS

Find keys matching pattern. **Warning:** Blocks server; avoid in production.

```
KEYS pattern
```

| Pattern | Matches |
|---------|---------|
| `*` | All keys |
| `h?llo` | hello, hallo, hxllo |
| `h*llo` | hllo, heeeello |
| `h[ae]llo` | hello, hallo |
| `h[^e]llo` | hallo, hbllo (not hello) |
| `h[a-b]llo` | hallo, hbllo |

Implementation: Scatter to all shards, gather results.

**Prefer SCAN for production use.**

### TOUCH

Update the last access time of keys.

```
TOUCH key [key ...]
```

Returns: Integer count of keys that exist.

### UNLINK

Delete keys asynchronously (non-blocking).

```
UNLINK key [key ...]
```

Returns: Integer count of keys removed. Unlike DEL, actual memory reclamation happens in background.

---

## Key Iteration: SCAN

### Overview

SCAN provides cursor-based iteration over the keyspace without blocking. Unlike KEYS, it returns results incrementally and is safe for production use.

```
SCAN cursor [MATCH pattern] [COUNT hint] [TYPE type]
```

### Cursor Format (Shard-Aware)

FrogDB encodes shard information in the cursor for transparent cross-shard iteration:

```
┌─────────────────────────────────────────────────────────────┐
│                     64-bit Cursor                            │
├─────────────────────────┬───────────────────────────────────┤
│      Shard ID           │      Position within Shard        │
│      (bits 48-63)       │      (bits 0-47)                  │
└─────────────────────────┴───────────────────────────────────┘
```

**Bit allocation:**
- Bits 48-63 (16 bits): Shard ID (supports up to 65,535 shards)
- Bits 0-47 (48 bits): Position within shard (supports very large dictionaries)

### Algorithm

```rust
fn scan(cursor: u64, count: usize, pattern: Option<&str>) -> (u64, Vec<Bytes>) {
    let shard_id = (cursor >> 48) as u16;
    let position = cursor & 0x0000_FFFF_FFFF_FFFF;

    // Determine target shard
    let target_shard = shard_id as usize;
    if target_shard >= num_shards {
        return (0, vec![]); // Invalid cursor
    }

    // Execute SCAN on target shard
    let (new_position, keys) = shards[target_shard].scan_local(position, count, pattern);

    // Determine next cursor
    let next_cursor = if new_position == 0 {
        // Shard exhausted, move to next
        let next_shard = target_shard + 1;
        if next_shard >= num_shards {
            0 // All shards exhausted
        } else {
            (next_shard as u64) << 48 // Start of next shard
        }
    } else {
        // Continue in same shard
        ((target_shard as u64) << 48) | new_position
    };

    (next_cursor, keys)
}
```

### Local Shard SCAN

Each shard implements dictionary iteration using the hash table cursor technique:

```rust
fn scan_local(position: u64, count: usize, pattern: Option<&str>) -> (u64, Vec<Bytes>) {
    let mut keys = Vec::new();
    let mut cursor = position;

    // Iterate hash buckets
    while keys.len() < count {
        let bucket = cursor % table_size;
        for key in bucket_entries(bucket) {
            if pattern.map_or(true, |p| matches(key, p)) {
                keys.push(key.clone());
            }
        }
        cursor = next_cursor(cursor);
        if cursor == 0 {
            break; // Wrapped around
        }
    }

    (cursor, keys)
}

// Reverse bits for cursor advancement (handles table resizing)
fn next_cursor(cursor: u64) -> u64 {
    reverse_bits(reverse_bits(cursor) + 1)
}
```

### Properties

| Property | Guarantee |
|----------|-----------|
| Blocking | No - returns incrementally |
| Completeness | All keys present for entire scan are returned |
| Duplicates | May return duplicates |
| Missing | Keys added/removed during scan may be missed |
| State | Stateless on server - cursor encodes all state |

### Cursor Validation and Security

**Cursor Format:**
- Cursors are opaque 64-bit integers encoding iteration state
- Clients should treat cursors as opaque (not parse or modify them)
- Cursors are stateless - no server-side cursor management needed

**Validation:**

```rust
fn validate_cursor(cursor: u64, num_shards: usize) -> Result<(usize, u64), Error> {
    let shard_id = (cursor >> 48) as usize;
    let position = cursor & 0x0000_FFFF_FFFF_FFFF;

    if shard_id >= num_shards {
        // Invalid shard ID - restart iteration
        return Ok((0, 0)); // Shard 0, position 0
    }

    Ok((shard_id, position))
}
```

**Invalid Cursor Behavior:**
- Malformed cursor (shard ID out of range): Restart iteration from shard 0
- Returns empty result array and cursor 0
- No error returned - matches Redis behavior

**Security Considerations:**
- Cursors are **not** injection vectors (they're iteration state, not query logic)
- No SQL/command injection possible through cursor values
- Position value is bounds-checked against hash table size
- Worst case: Invalid cursor restarts iteration (no data exposure)

**No TTL/Expiration:**
- Cursors are stateless - no server-side expiration needed
- Clients can resume iteration at any time (unless keyspace changed significantly)
- After major keyspace changes (rehashing), cursor position may be invalid (iteration restarts)

### Related SCAN Commands

| Command | Description |
|---------|-------------|
| SCAN cursor [MATCH pattern] [COUNT hint] [TYPE type] | Iterate keys |
| SSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Set members |
| HSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Hash fields |
| ZSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Sorted Set members |

---

## Cross-Shard Behavior

### Single-Key Commands

Commands operating on a single key (EXPIRE, TTL, TYPE, etc.) are routed directly to the owning shard.

### Multi-Key Commands

| Command | Behavior |
|---------|----------|
| DEL | Scatter-gather: delete from each shard, sum results |
| EXISTS | Scatter-gather: check each shard, sum counts |
| KEYS | Scatter to all shards, gather matching keys |
| RENAME | **Requires same shard** - use hash tags |
| TOUCH | Scatter-gather: touch on each shard |
| UNLINK | Scatter-gather: unlink from each shard |

### Hash Tags for Colocation

To ensure keys are on the same shard (required for RENAME):

```
{user:123}:profile   → hash("user:123")
{user:123}:settings  → hash("user:123") → same shard!
```

---

## Persistence

All generic operations follow FrogDB's unified persistence model:
- WAL writes for durability (DEL, RENAME, expiration changes)
- Key state included in snapshots
- Expiration metadata preserved across crashes

See [PERSISTENCE.md](../PERSISTENCE.md) for configuration.

---

## Key Eviction

When memory limits are reached, keys may be evicted based on the configured policy. See [EVICTION.md](../EVICTION.md) for eviction policies and configuration.
