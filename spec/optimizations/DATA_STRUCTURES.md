# Data Structure Optimizations

- [x] **Sorted set `remove_range_by_*` clone elimination** - Collect BTreeMap keys directly instead of going through `range_by_*` which clones all members into a `Vec<(Bytes, f64)>` intermediary
  - Implemented in `remove_range_by_rank`, `remove_range_by_score`, `remove_range_by_lex`
- [ ] **Skip list for sorted sets** - Replace BTreeMap with `crossbeam_skiplist`
  - Expected: Better concurrent read performance
