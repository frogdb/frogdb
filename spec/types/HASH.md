# FrogDB Hash Commands

## Overview

Hash keys will store field-value mappings, ideal for representing objects with multiple attributes.

## Data Structure

```rust
// Planned implementation
pub struct HashValue {
    data: HashMap<Bytes, Bytes>,
}
```

**Design Notes:**
- Standard HashMap for O(1) field access
- Consider ziplist-style encoding for small hashes (< 64 entries)

## Commands (Planned)

| Command | Description |
|---------|-------------|
| HSET | Set field(s) in hash |
| HGET | Get field value |
| HMGET | Get multiple field values |
| HGETALL | Get all fields and values |
| HDEL | Delete field(s) |
| HEXISTS | Check if field exists |
| HLEN | Return number of fields |
| HKEYS | Return all field names |
| HVALS | Return all values |
| HINCRBY | Increment field by integer |
| HINCRBYFLOAT | Increment field by float |
| HSETNX | Set field only if not exists |
| HSCAN | Incrementally iterate fields |
| HSTRLEN | Return length of field value |

## Implementation Notes

- **Memory accounting:** Sum of field + value lengths + HashMap overhead
- **Persistence format:** `[len:u32][field1_len:u32][field1][val1_len:u32][val1]...`
- **Cross-shard behavior:** Entire hash on single shard; use hash tags for related hashes
- **Small hash optimization:** Consider compact encoding when field count < threshold

## References

- [Redis Hash Commands](https://redis.io/docs/latest/commands/?group=hash)
- [COMMANDS.md](../COMMANDS.md) - Command execution model
- [STORAGE.md](../STORAGE.md) - Storage layer integration
