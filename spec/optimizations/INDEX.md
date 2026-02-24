# FrogDB Performance Optimizations

This document details comprehensive performance profiling and optimization strategies for FrogDB. Target: Linux primary, macOS for development.

## Sub-Documents

| Document | Description |
|----------|-------------|
| [PROFILING.md](PROFILING.md) | Build profiles, CPU/memory profiling tools, async task profiling, benchmarks |
| [QUICK_WINS.md](QUICK_WINS.md) | Low-effort, high-impact optimizations (jemalloc, caching, pre-sizing) |
| [MEMORY.md](MEMORY.md) | Arc-wrapped values, response buffer pools, arena allocator |
| [IO.md](IO.md) | WAL batching, RocksDB write path, io_uring integration |
| [DATA_STRUCTURES.md](DATA_STRUCTURES.md) | Streaming SCAN, sorted set clone elimination, skip list |
| [CONCURRENCY.md](CONCURRENCY.md) | Lock-free WAL, pub/sub zero-copy, connection pooling |
| [ADVANCED.md](ADVANCED.md) | SIMD pattern matching, vectored I/O |
| [SINGLE_SHARD.md](SINGLE_SHARD.md) | Three-tier single-shard mode optimizations |

---

## Common Bottlenecks Reference

| Category | Anti-Pattern | Solution |
|----------|--------------|----------|
| Memory | Deep cloning on read | Arc/Cow wrappers |
| Memory | Unbounded buffers | Bounded channels, backpressure |
| Memory | Per-request allocations | Object pools |
| Concurrency | Global locks | Sharded locks, lock-free structures |
| Concurrency | Blocking in async | `spawn_blocking` |
| I/O | Sync per write | Batched fsync |
| I/O | Small syscalls | Vectored I/O |
| Data Structures | HashMap for ordered data | Skip list, B-tree |

---

## Critical Files Reference

| File | Optimizations |
|------|---------------|
| `crates/core/src/store.rs` | GET cloning, SCAN iteration |
| `crates/core/src/persistence/wal.rs` | WAL batching, mutex contention |
| `crates/core/src/types.rs` | Value enum, Arc wrapping |
| `crates/core/src/persistence/rocks.rs` | CF caching, RocksDB tuning |
| `crates/core/src/pubsub.rs` | Zero-copy broadcasting |
| `crates/server/src/main.rs` | jemalloc allocator, single-thread runtime |
| `crates/server/src/connection/routing.rs` | Shard routing, scatter-gather bypass, channel hop elimination |
| `crates/core/src/shard/helpers.rs` | CRC16 skip for single-shard |
| `crates/server/src/acceptor.rs` | Round-robin skip for single-shard |
