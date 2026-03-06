# FrogDB Performance Optimizations

This document details comprehensive performance profiling and optimization strategies for FrogDB. Target: Linux primary, macOS for development.

## Sub-Documents

| Document | Description |
|----------|-------------|
| [PROFILING.md](PROFILING.md) | Build profiles, CPU/memory profiling tools, async task profiling, benchmarks |
| [MEMORY.md](MEMORY.md) | Arena allocator |
| [ASYNC_RUNTIME.md](ASYNC_RUNTIME.md) | Async runtime strategy: io_uring, completion-based I/O, runtime comparison (compio vs monoio vs glommio), migration options, abstraction feasibility |
| [CONCURRENCY.md](CONCURRENCY.md) | Lock-free WAL, connection pooling |
| [ADVANCED.md](ADVANCED.md) | SIMD pattern matching, vectored I/O |
| [SINGLE_SHARD.md](SINGLE_SHARD.md) | Three-tier single-shard mode optimizations |

---

## Common Bottlenecks Reference

| Category | Anti-Pattern | Solution |
|----------|--------------|----------|
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
| `crates/core/src/store/hashmap.rs` | SCAN iteration (optimized: direct iterator) |
| `crates/core/src/store.rs` | Arc-wrapped values (optimized) |
| `crates/core/src/persistence/wal.rs` | WAL batching, mutex contention |
| `crates/types/src/types.rs` | Value enum, sorted set remove_range (optimized), sorted set argument consumption (optimized) |
| `crates/types/src/glob.rs` | Glob matching (optimized: fast paths) |
| `crates/persistence/src/rocks.rs` | CF caching (optimized), RocksDB tuning (optimized: bloom filter, block cache, per-level compression, compaction triggers) |
| `crates/server/src/connection.rs` | RESP3 buffer reuse (optimized) |
| `crates/core/src/pubsub.rs` | Zero-copy broadcasting |
| `crates/server/src/main.rs` | jemalloc allocator, single-thread runtime |
| `crates/server/src/connection/routing.rs` | Shard routing, scatter-gather bypass, channel hop elimination |
| `crates/core/src/shard/helpers.rs` | CRC16 skip for single-shard |
| `crates/server/src/acceptor.rs` | Round-robin skip for single-shard |
