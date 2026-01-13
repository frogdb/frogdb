# FrogDB Limits

This document details FrogDB's size limits, enforcement points, and configuration options.

## Size Limits

FrogDB enforces Redis-compatible size limits:

| Limit | Value | Configuration |
|-------|-------|---------------|
| Max key size | 512 MB | `proto-max-bulk-len` |
| Max value size | 512 MB | `proto-max-bulk-len` |
| Max elements per List/Set/Hash/SortedSet | 2^32 - 1 (~4 billion) | Hardcoded |
| Max Lua script size | 512 MB | `proto-max-bulk-len` |
| Max command arguments | Unlimited (memory-bound) | - |
| Max internal shards per node | 65,536 | 16-bit cursor encoding |
| Max bulk string | 512 MB | Any single RESP bulk string |

---

## Configuration

```toml
[protocol]
proto_max_bulk_len = 536870912  # 512 MB default, in bytes
```

**Notes:**
- `proto-max-bulk-len` controls maximum size of any bulk string in RESP protocol
- Exceeding limits returns `-ERR` with descriptive message
- Collection element limits are theoretical; practical limit is available memory

---

## Enforcement Points

Size limits are checked at multiple points in the command pipeline:

| Point | Behavior |
|-------|----------|
| **Protocol parsing** | Reject before reading data if declared size exceeds limit |
| **SET command** | Key/value size checked during parsing |
| **APPEND command** | Rejected if result would exceed limit |
| **SETRANGE command** | Rejected if offset + value would exceed limit |
| **Aggregate operations** | Checked before execution (ZUNIONSTORE, etc.) |

---

## Protocol-Level Enforcement

Size limits are checked during RESP parsing before command execution. This prevents denial-of-service by rejecting oversized data before buffering:

```rust
fn parse_bulk_string(reader: &mut BufReader) -> Result<Bytes, Error> {
    let len: i64 = parse_length(reader)?;

    if len > config.proto_max_bulk_len as i64 {
        // Reject immediately without reading data
        return Err(Error::Protocol("bulk string length exceeds maximum"));
    }

    // Read the actual data
    read_exact(reader, len as usize)
}
```

---

## Error Messages

| Scenario | Error |
|----------|-------|
| Bulk string too large | `-ERR Protocol error: bulk string length exceeds maximum allowed` |
| APPEND would exceed | `-ERR string exceeds maximum allowed size (512MB)` |
| SETRANGE would exceed | `-ERR string exceeds maximum allowed size (512MB)` |
| Collection too large | `-ERR number of elements exceeds maximum allowed` |

---

## Cursor Limits

SCAN commands use a 64-bit cursor encoding:

```
┌─────────────────────────────────────────┐
│           64-bit Cursor                  │
├─────────────────┬───────────────────────┤
│   Shard ID      │  Position in Shard    │
│   (bits 48-63)  │  (bits 0-47)          │
└─────────────────┴───────────────────────┘
```

**Practical limits:**
- Shard ID: 16 bits → max 65,536 internal shards per node
- Position: 48 bits → max ~281 trillion keys per shard
- These limits are theoretical; practical deployments use far fewer shards (typically ≤ CPU count)

---

## Memory Limits

When configured memory limit (`max_memory`) is reached:

| Operation | Behavior |
|-----------|----------|
| Write (SET, ZADD, etc.) | Return `-OOM command not allowed when used memory > 'maxmemory'` error |
| Read (GET, ZRANGE, etc.) | Continue normally |
| Delete (DEL, EXPIRE) | Continue normally |
| Expiry background task | Continues to reclaim memory |

See [EVICTION.md](EVICTION.md) for eviction policies that provide alternatives to OOM rejection.

---

## Lua Script Limits

Scripts have additional resource limits beyond size:

| Limit | Default | Description |
|-------|---------|-------------|
| `lua_time_limit_ms` | 5000 | Max execution time (5 seconds) |
| `lua_heap_limit_mb` | 256 | Max memory per Lua VM |

See [SCRIPTING.md](SCRIPTING.md) for detailed script resource management.

---

## References

- [PROTOCOL.md](PROTOCOL.md) - RESP parsing details
- [COMMANDS.md](COMMANDS.md) - Command reference
- [EVICTION.md](EVICTION.md) - Memory management and eviction
- [SCRIPTING.md](SCRIPTING.md) - Lua script limits
