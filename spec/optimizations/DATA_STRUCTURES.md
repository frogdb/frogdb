# Data Structure Optimizations

- [x] **Sorted set `remove_range_by_*` clone elimination** - Collect BTreeMap keys directly instead of going through `range_by_*` which clones all members into a `Vec<(Bytes, f64)>` intermediary
  - Implemented in `remove_range_by_rank`, `remove_range_by_score`, `remove_range_by_lex`
- [x] **Skip list for sorted sets** - Added custom skip list as configurable alternative to BTreeMap with O(log n) rank queries
  - Implemented as dual-backend architecture: `ScoreIndex` enum dispatches to either `BTreeMap` or `SkipList`
  - Default backend is SkipList; configurable via `sorted_set_backend` server config
  - See `crates/types/src/skiplist.rs` and `crates/types/src/types.rs`
