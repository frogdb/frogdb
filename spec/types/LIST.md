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
| LPUSHX | Insert element at head only if list exists |
| RPUSHX | Insert element at tail only if list exists |
| LPOS | Return position of element in list |
| LMOVE | Move element between lists |
| RPOPLPUSH | Pop tail and push to head of another list (deprecated, use LMOVE) |
| LMPOP | Pop elements from multiple lists |
| BLPOP | Blocking pop from head |
| BRPOP | Blocking pop from tail |
| BLMOVE | Blocking move between lists |
| BLMPOP | Blocking pop from multiple lists |
| BRPOPLPUSH | Blocking pop tail and push head (deprecated, use BLMOVE) |

## Implementation Notes

- **Memory accounting:** Sum of all element lengths + VecDeque overhead
- **Persistence format:** `[len:u32][elem1_len:u32][elem1]...`
- **Cross-shard behavior:** Use hash tags for atomic multi-list operations
- **Blocking commands:** All blocking list commands (BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH) are implemented

## References

- [Redis List Commands](https://redis.io/docs/latest/commands/?group=list)
- [COMMANDS.md](../COMMANDS.md) - Command execution model
- [STORAGE.md](../STORAGE.md) - Storage layer integration
