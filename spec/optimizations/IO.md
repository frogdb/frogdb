# I/O Optimizations

- [ ] **WAL batching improvements** - Lock-free batch accumulation with crossbeam queues
  - Problem: Mutex lock per write in `wal.rs:116`
  - Expected: 2-5x throughput for write-heavy workloads
  - Critical file: `crates/core/src/persistence/wal.rs`
- [ ] **RocksDB write path tuning** - Vectored I/O, larger write buffers
- [ ] **io_uring integration** - Use `tokio-uring` for network and WAL I/O (Linux 5.1+)
  - Expected: 30-50% throughput improvement
  - Requires Linux-specific code paths
  - See [IO_URING.md](../IO_URING.md) for detailed design
