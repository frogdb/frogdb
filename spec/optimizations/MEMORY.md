# Memory Optimizations

- [x] **RESP3 response buffer reuse** - `resp3_buf: BytesMut` field on `ConnectionHandler`, reused via `clear()` per response
  - Buffer grows to high-water mark and stays allocated, eliminating per-response `BytesMut::new()` allocation churn
- [ ] **Memory arena allocator** - Use `bumpalo` for per-command allocation batching
  - Expected: 20-40% reduction in allocator calls
