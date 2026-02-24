# I/O Optimizations

- [ ] **RocksDB write path tuning** - Vectored I/O, larger write buffers
- [ ] **io_uring / completion-based I/O** - Replace tokio with compio for thread-per-core completion-based I/O
  - Expected: 30-50% throughput improvement
  - Compio recommended over monoio/glommio (most active development, richest ecosystem, cross-platform)
  - See [IO_URING.md](IO_URING.md) for detailed design and runtime comparison
