# Advanced Optimizations

- [ ] **SIMD pattern matching** - Use `memchr` crate for glob matching in SCAN/KEYS
  - Expected: 2-5x faster pattern matching
- [ ] **Vectored I/O** - Batch small writes into single syscalls
