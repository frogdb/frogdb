# Advanced Optimizations

- [ ] **SIMD pattern matching** - Use `memchr` crate for glob matching in SCAN/KEYS for complex
  patterns
  - Expected: 2-5x faster pattern matching for patterns that still require the full matcher
- [ ] **Vectored I/O** - Batch small writes into single syscalls
