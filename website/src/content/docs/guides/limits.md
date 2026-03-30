---
title: "Limits"
description: "FrogDB enforces Redis-compatible size limits on command input and resources."
sidebar:
  order: 8
---
FrogDB enforces Redis-compatible size limits on command input and resources.

## Size Limits

| Limit | Value | Configuration |
|-------|-------|---------------|
| Max key size | 512 MB | `proto-max-bulk-len` |
| Max value size | 512 MB | `proto-max-bulk-len` |
| Max elements per List/Set/Hash/Sorted Set | ~4 billion (2^32 - 1) | Hardcoded |
| Max Lua script size | 512 MB | `proto-max-bulk-len` |
| Max command arguments | Unlimited (memory-bound) | -- |
| Max bulk string (RESP protocol) | 512 MB | `proto-max-bulk-len` |

### Configuration

The `proto-max-bulk-len` setting controls the maximum size of any single bulk string in the RESP protocol:

```toml
[protocol]
proto_max_bulk_len = 536870912  # 512 MB default, in bytes
```

Collection element limits are theoretical; the practical limit is available memory.

## Enforcement

Size limits are checked at multiple points:

| Point | Behavior |
|-------|----------|
| **Protocol parsing** | Rejects before reading data if declared size exceeds limit |
| **SET command** | Key/value size checked during parsing |
| **APPEND command** | Rejected if result would exceed limit |
| **SETRANGE command** | Rejected if offset + value would exceed limit |
| **Aggregate operations** | Checked before execution (ZUNIONSTORE, etc.) |

Oversized data is rejected at the protocol level before buffering, preventing denial-of-service from large payloads.

## Error Messages

| Scenario | Error |
|----------|-------|
| Bulk string too large | `-ERR Protocol error: bulk string length exceeds maximum allowed` |
| APPEND would exceed limit | `-ERR string exceeds maximum allowed size (512MB)` |
| SETRANGE would exceed limit | `-ERR string exceeds maximum allowed size (512MB)` |
| Collection too large | `-ERR number of elements exceeds maximum allowed` |

## Memory Limits

When the configured memory limit (`max_memory`) is reached:

| Operation | Behavior |
|-----------|----------|
| Write (SET, ZADD, etc.) | Returns `-OOM command not allowed when used memory > 'maxmemory'` |
| Read (GET, ZRANGE, etc.) | Continues normally |
| Delete (DEL, EXPIRE) | Continues normally |
| Background expiry | Continues to reclaim memory |

See [Configuration](/operations/configuration/) for `max_memory` and eviction policy settings.

## Lua Script Limits

| Limit | Default | Description |
|-------|---------|-------------|
| `lua_time_limit_ms` | 5000 | Max execution time (5 seconds) |
| `lua_heap_limit_mb` | 256 | Max memory per Lua VM |

See [Lua Scripting](/guides/lua-scripting/) for details.

## SCAN Cursor Limits

SCAN commands use a 64-bit cursor with a 16-bit shard ID prefix, supporting up to 65,536 internal shards per node and ~281 trillion keys per shard. In practice, deployments use far fewer shards (typically equal to or less than the CPU count).
