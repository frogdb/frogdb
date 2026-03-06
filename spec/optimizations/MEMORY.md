# Memory Optimizations

- [ ] **Memory arena allocator** - Use `bumpalo` for per-command allocation batching
  - Expected: 20-40% reduction in allocator calls
