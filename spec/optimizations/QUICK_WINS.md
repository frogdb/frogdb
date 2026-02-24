# Quick Wins (Low Effort, High Impact)

- [x] **Cache RocksDB CF names** - Pre-allocate `"shard_{}"` strings in `RocksStore`
  - Stores `cf_names: Vec<String>` on the struct, populated once in `open()`, used by `cf_handle()` and `create_batch_for_shard()` instead of `format!()` per call
- [x] **RESP3 response buffer reuse** - Reusable `BytesMut` on `ConnectionHandler`
  - `resp3_buf` field grows to high-water mark and stays, `clear()` per response instead of `BytesMut::new()`
- [x] **Glob matching fast paths** - Short-circuit common SCAN/KEYS patterns before recursive matcher
  - `*` (match-all), exact match (no special chars), `prefix*`, `*suffix` — avoids iterator cloning and recursion
- [x] **SCAN iterator optimization** - Iterate HashMap directly instead of cloning all keys upfront
  - `scan_filtered()` skips to cursor position via `iter()` + skip, only clones matched keys, eliminates redundant `data.get()` re-lookup for type filtering
- [x] **Sorted set `remove_range_by_*` optimization** - Collect BTreeMap keys directly instead of going through `range_by_*` intermediary
  - `remove_range_by_rank`, `remove_range_by_score`, `remove_range_by_lex` now collect `(score, member)` keys and remove directly from both `scores` and `members` maps, avoiding intermediate `Vec<(Bytes, f64)>` allocation and per-member `remove()` re-lookups
