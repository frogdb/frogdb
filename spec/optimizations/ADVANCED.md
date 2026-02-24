# Advanced Optimizations

- [x] **Glob matching fast paths** - Short-circuit `*`, exact match, `prefix*`, and `*suffix` patterns before the recursive byte-by-byte matcher
  - Covers the vast majority of real-world SCAN/KEYS patterns with zero iterator cloning or recursion
- [ ] **SIMD pattern matching** - Use `memchr` crate for glob matching in SCAN/KEYS for complex patterns
  - Expected: 2-5x faster pattern matching for patterns that still require the full matcher
- [ ] **Vectored I/O** - Batch small writes into single syscalls
