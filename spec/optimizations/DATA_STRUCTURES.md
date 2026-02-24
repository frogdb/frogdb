# Data Structure Optimizations

- [ ] **Streaming SCAN iterator** - Cursor-based iteration without full key materialization
  - Problem: `store.rs:352` collects all keys into Vec
  - Expected: Memory reduced from O(n) to O(count)
- [ ] **Sorted set clone elimination** - Consume arguments, only clone when necessary
  - Expected: 15-25% improvement for sorted set operations
  - Critical file: `crates/core/src/types.rs`
- [ ] **Skip list for sorted sets** - Replace BTreeMap with `crossbeam_skiplist`
  - Expected: Better concurrent read performance
