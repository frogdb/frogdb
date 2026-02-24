# Quick Wins (Low Effort, High Impact)

- [ ] **Cache RocksDB CF names** - Pre-allocate `"shard_{}"` strings in `rocks.rs:162`
  - Expected: 5-10% for persistence operations
