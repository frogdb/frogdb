# Memory Optimizations

- [ ] **Arc-wrapped Values for GET** - Change `store.rs:285` to use `Arc<Value>` instead of cloning
  - Expected: 30-50% improvement for read-heavy workloads with large values
  - Critical files: `crates/core/src/store.rs`, `crates/core/src/types.rs`
- [x] **RESP3 response buffer reuse** - `resp3_buf: BytesMut` field on `ConnectionHandler`, reused via `clear()` per response
  - Buffer grows to high-water mark and stays allocated, eliminating per-response `BytesMut::new()` allocation churn
- [ ] **Memory arena allocator** - Use `bumpalo` for per-command allocation batching
  - Expected: 20-40% reduction in allocator calls
