# Quick Wins (Low Effort, High Impact)

- [ ] **jemalloc allocator** - Add `tikv_jemallocator` in `crates/server/src/main.rs`
  - Expected: 10-20% improvement for allocation-heavy workloads
- [ ] **Cache RocksDB CF names** - Pre-allocate `"shard_{}"` strings in `rocks.rs:162`
  - Expected: 5-10% for persistence operations
- [ ] **`Bytes::from_static` for constants** - Replace `Bytes::from("OK")` with `Bytes::from_static(b"OK")`
  - Expected: Eliminates allocations for common responses
- [ ] **Pre-size vectors** - Use `Vec::with_capacity()` in array-returning commands
  - Expected: 5-15% for commands like KEYS, MGET, LRANGE
- [ ] **RocksDB tuning** - Increase write buffer to 256MB, max_write_buffer_number=4
