# FrogDB List Commands

## Overview

List keys store ordered sequences of string elements, supporting operations from both ends (head and tail).

## Data Structure

```rust
pub struct ListValue {
    data: VecDeque<Bytes>,
}
```

**Design Notes:**
- `VecDeque` provides O(1) push/pop at both ends
- Alternative: chunked linked list for better memory locality on large lists

## Commands

| Command | Description |
|---------|-------------|
| LPUSH | Insert elements at head |
| RPUSH | Insert elements at tail |
| LPOP | Remove and return from head |
| RPOP | Remove and return from tail |
| LLEN | Return list length |
| LRANGE | Return range of elements |
| LINDEX | Get element by index |
| LSET | Set element at index |
| LINSERT | Insert before/after pivot |
| LREM | Remove elements by value |
| LTRIM | Trim to specified range |
| LMOVE | Move element between lists |
| BLPOP | Blocking pop from head |
| BRPOP | Blocking pop from tail |
| BLMOVE | Blocking move between lists |

## Implementation Notes

- **Memory accounting:** Sum of all element lengths + VecDeque overhead
- **Persistence format:** `[len:u32][elem1_len:u32][elem1]...`
- **Cross-shard behavior:** Use hash tags for atomic multi-list operations
- **Blocking commands:** Require List implementation before blocking can be enabled

## References

- [Redis List Commands](https://redis.io/docs/latest/commands/?group=list)
- [COMMANDS.md](../COMMANDS.md) - Command execution model
- [STORAGE.md](../STORAGE.md) - Storage layer integration
