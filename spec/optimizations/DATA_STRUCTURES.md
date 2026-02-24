# Data Structure Optimizations

- [ ] **Sorted set clone elimination** - Consume arguments, only clone when necessary
  - Expected: 15-25% improvement for sorted set operations
  - Critical file: `crates/core/src/types.rs`
- [ ] **Skip list for sorted sets** - Replace BTreeMap with `crossbeam_skiplist`
  - Expected: Better concurrent read performance
