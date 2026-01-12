# FrogDB Set Commands

> **Status: PLANNED** - Set support is planned for Phase 3+.

## Overview

Set keys will store unordered collections of unique string elements, supporting set operations like union, intersection, and difference.

## Data Structure

```rust
// Planned implementation
pub struct SetValue {
    data: HashSet<Bytes>,
}
```

**Design Notes:**
- Standard HashSet for O(1) membership testing
- Consider intset encoding for small sets of integers

## Commands (Planned)

| Command | Description |
|---------|-------------|
| SADD | Add member(s) to set |
| SREM | Remove member(s) from set |
| SMEMBERS | Return all members |
| SISMEMBER | Check if member exists |
| SMISMEMBER | Check multiple members |
| SCARD | Return set cardinality |
| SPOP | Remove and return random member(s) |
| SRANDMEMBER | Return random member(s) |
| SMOVE | Move member between sets |
| SUNION | Return union of sets |
| SUNIONSTORE | Store union result |
| SINTER | Return intersection of sets |
| SINTERSTORE | Store intersection result |
| SINTERCARD | Return intersection cardinality |
| SDIFF | Return difference of sets |
| SDIFFSTORE | Store difference result |
| SSCAN | Incrementally iterate members |

## Implementation Notes

- **Memory accounting:** Sum of member lengths + HashSet overhead
- **Persistence format:** `[len:u32][member1_len:u32][member1]...`
- **Cross-shard behavior:**
  - Single set operations: on owning shard
  - Multi-set operations (SUNION, SINTER, SDIFF): require hash tags for colocation, or scatter-gather
- **Set operations:** SINTER/SUNION/SDIFF with keys on different shards return `-CROSSSLOT` error unless hash tags used

## References

- [Redis Set Commands](https://redis.io/docs/latest/commands/?group=set)
- [COMMANDS.md](../COMMANDS.md) - Command execution model
- [STORAGE.md](../STORAGE.md) - Storage layer integration
