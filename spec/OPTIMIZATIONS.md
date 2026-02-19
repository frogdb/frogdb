# FrogDB Performance Optimizations

This document details comprehensive performance profiling and optimization strategies for FrogDB. Target: Linux primary, macOS for development.

---

## 7.1 Profiling Infrastructure

**Build Profile** - Add to `Cargo.toml`:
```toml
[profile.profiling]
inherits = "release"
debug = true
strip = false
```

**CPU Profiling Tools:**

| Tool | Platform | Command |
|------|----------|---------|
| cargo-flamegraph | All | `cargo flamegraph --profile profiling --bin frogdb-server` |
| samply | All | `samply record ./target/profiling/frogdb-server` |
| perf | Linux | `perf record -g --call-graph dwarf ./target/profiling/frogdb-server` |

**Memory Profiling Tools:**

| Tool | Platform | Use Case |
|------|----------|----------|
| heaptrack | Linux | Allocation tracking, leak detection |
| DHAT (valgrind) | Linux | Heap profiling |
| jemalloc prof | All | Production heap profiling |

**Profiling Workflow:**
```bash
# 1. Build with debug symbols
cargo build --profile profiling

# 2. Start server and run workload
./target/profiling/frogdb-server &
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t get,set

# 3. Generate flamegraph
cargo flamegraph --profile profiling --bin frogdb-server
```

**Async Task Profiling:**

For profiling tokio task-level performance, use `tokio-metrics` and `tracing` spans to measure per-task busy/idle/scheduled timing and identify latency bottlenecks:

| Tool | Crate | Use Case |
|------|-------|----------|
| tokio-metrics | `tokio-metrics` | Per-task busy/idle/scheduled timing via `TaskMonitor` |
| tracing spans | `tracing` | Span-based latency analysis at task/operation granularity |
| tracing-timing | `tracing-timing` | Latency histograms derived from tracing spans |
| tracing-flame | `tracing-flame` | Async-aware flamegraphs — captures await/idle time, not just CPU |
| tracing-tracy | `tracing-tracy` | Real-time interactive profiling via Tracy with nanosecond precision |

- [ ] Add `tokio-metrics` `TaskMonitor` instrumentation for key task types (connection handler, shard operations, WAL writes)
- [ ] Add `tracing` span instrumentation for critical request paths
- [ ] Integrate `tracing-timing` for per-operation latency histograms
- [ ] Add `tracing-flame` `FlameLayer` for async-aware flamegraph generation during profiling
- [ ] Evaluate `tracing-tracy` for interactive real-time profiling during development

> **Research concept:** For causal profiling adapted to async runtimes, see [TOKIO_CAUSAL_PROFILER.md](TOKIO_CAUSAL_PROFILER.md).
- [ ] Add `[profile.profiling]` build profile to `Cargo.toml`
- [ ] Add `scan.rs` benchmark - SCAN with 10K, 100K, 1M keys
- [ ] Add `large_values.rs` benchmark - GET/SET with 1KB, 10KB, 100KB values
- [ ] Add `sorted_set_scale.rs` benchmark - ZADD/ZRANGE at large cardinalities
- [ ] Extend `slowlog.rs` with latency histograms via `tracing-timing`

---

## 7.2 Quick Wins (Low Effort, High Impact)

- [ ] **jemalloc allocator** - Add `tikv_jemallocator` in `crates/server/src/main.rs`
  - Expected: 10-20% improvement for allocation-heavy workloads
- [ ] **Cache RocksDB CF names** - Pre-allocate `"shard_{}"` strings in `rocks.rs:162`
  - Expected: 5-10% for persistence operations
- [ ] **`Bytes::from_static` for constants** - Replace `Bytes::from("OK")` with `Bytes::from_static(b"OK")`
  - Expected: Eliminates allocations for common responses
- [ ] **Pre-size vectors** - Use `Vec::with_capacity()` in array-returning commands
  - Expected: 5-15% for commands like KEYS, MGET, LRANGE
- [ ] **RocksDB tuning** - Increase write buffer to 256MB, max_write_buffer_number=4

---

## 7.3 Memory Optimizations

- [ ] **Arc-wrapped Values for GET** - Change `store.rs:285` to use `Arc<Value>` instead of cloning
  - Expected: 30-50% improvement for read-heavy workloads with large values
  - Critical files: `crates/core/src/store.rs`, `crates/core/src/types.rs`
- [ ] **Response buffer pools** - Reuse response buffers across commands
  - Expected: Reduced allocator pressure
- [ ] **Memory arena allocator** - Use `bumpalo` for per-command allocation batching
  - Expected: 20-40% reduction in allocator calls

---

## 7.4 I/O Optimizations

- [ ] **WAL batching improvements** - Lock-free batch accumulation with crossbeam queues
  - Problem: Mutex lock per write in `wal.rs:116`
  - Expected: 2-5x throughput for write-heavy workloads
  - Critical file: `crates/core/src/persistence/wal.rs`
- [ ] **RocksDB write path tuning** - Vectored I/O, larger write buffers
- [ ] **io_uring integration** - Use `tokio-uring` for network and WAL I/O (Linux 5.1+)
  - Expected: 30-50% throughput improvement
  - Requires Linux-specific code paths

---

## 7.5 Data Structure Optimizations

- [ ] **Streaming SCAN iterator** - Cursor-based iteration without full key materialization
  - Problem: `store.rs:352` collects all keys into Vec
  - Expected: Memory reduced from O(n) to O(count)
- [ ] **Sorted set clone elimination** - Consume arguments, only clone when necessary
  - Expected: 15-25% improvement for sorted set operations
  - Critical file: `crates/core/src/types.rs`
- [ ] **Skip list for sorted sets** - Replace BTreeMap with `crossbeam_skiplist`
  - Expected: Better concurrent read performance

---

## 7.6 Concurrency Optimizations

- [ ] **Lock-free WAL writes** - Replace mutex with lock-free queue
- [ ] **Pub/Sub zero-copy broadcasting** - Share `Arc<PubSubMessage>` across subscribers
  - Problem: Clone message for each subscriber in `pubsub.rs:530`
  - Expected: N-fold reduction for N subscribers
- [ ] **Connection pooling optimizations** - Reduce per-connection overhead

---

## 7.7 Advanced Optimizations

- [ ] **SIMD pattern matching** - Use `memchr` crate for glob matching in SCAN/KEYS
  - Expected: 2-5x faster pattern matching
- [ ] **Vectored I/O** - Batch small writes into single syscalls

---

## 7.8 Common Bottlenecks Reference

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
| `crates/server/src/main.rs` | jemalloc allocator |
