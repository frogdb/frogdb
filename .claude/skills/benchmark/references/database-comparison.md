# Database Architecture Comparison

Reference for interpreting benchmark results and answering "why is X faster than Y?"

## FrogDB

**Architecture:** Shared-nothing Tokio tasks, multi-shard single-process, message-passing
via mpsc channels, pin-based connections (each connection pinned to a worker), jemalloc
allocator, RocksDB persistence backend.

**Threading model:** N Tokio worker threads servicing M connections. Each connection has a
dedicated `ConnectionHandler` task. Commands are routed to shard workers via mpsc channels
based on key hash. Shards are independent — no locks between them.

**CPU profile:** 91% I/O-bound (kernel syscalls). Only 7.4% in userspace code. Socket I/O
(read + write) is 15% of total CPU.

**Strengths:**
- Parallel command processing across shards without pipelining
- Low overhead for multi-key operations on same shard
- Single-process simplicity (no cluster redirects)
- Shared-nothing eliminates most lock contention

**Weaknesses:**
- Per-command syscall overhead — each response triggers an individual write syscall
- Write coalescing not yet implemented (would batch multiple responses into one writev)
- clock_gettime overhead from CommandTimer on every command

## Redis

**Architecture:** Single-threaded event loop, epoll/kqueue I/O multiplexing. All commands
processed sequentially on the main thread. Optional I/O threads (6+) for read/write offload
since Redis 6.0.

**Threading model:** Main thread handles all command execution. I/O threads (when enabled)
handle socket reads and response writes in parallel, but command execution remains
single-threaded.

**Strengths:**
- Natural write batching — event loop drains all ready responses in one pass
- Minimal per-command overhead (no cross-thread coordination)
- Extremely consistent latency (no jitter from thread scheduling)
- Pipelined workloads are very efficient (amortize single syscall over many commands)

**Weaknesses:**
- Single-core ceiling — throughput capped by one CPU core
- Without pipelining, limited by event loop iteration speed
- I/O threads help read/write but don't parallelize execution

## Valkey

**Architecture:** Redis fork with incremental threading improvements. Nearly identical
performance characteristics to Redis in most benchmarks.

**Key differences from Redis:**
- Some improvements to I/O thread utilization
- Multi-threaded command execution being developed (not yet general-purpose)

**Benchmark expectation:** Within 5% of Redis for most workloads. Treat as equivalent
for comparison purposes.

## Dragonfly

**Architecture:** Multi-threaded proactor pattern, shared-nothing design. Each thread
owns a slice of the keyspace. VLL (Very Lightweight Locking) for multi-key transactions.
Epoch-based snapshots for persistence.

**Threading model:** N proactor threads, each with its own event loop and keyspace slice.
Connections are distributed across threads. Cross-thread operations use VLL for coordination.

**Strengths:**
- Highest raw throughput — most mature multi-threaded optimization
- Efficient write batching per proactor
- Low-overhead cross-key transactions via VLL
- Scales linearly with cores for most workloads

**Weaknesses:**
- Higher baseline memory usage
- Cross-thread transactions add latency for multi-key operations spanning threads

## Per-Workload Expectations

### GET/SET (basic key-value)

Pipeline depth is the dominant factor:
- **No pipelining:** FrogDB ≈ Redis ≈ Valkey (all ~190k ops/sec). Dragonfly typically 10-30% higher.
- **Pipeline=10:** FrogDB ~440k, Redis ~500-600k (better write batching). Dragonfly highest.
- **Pipeline=50+:** Redis catches up significantly due to amortized syscall overhead.

### Sorted sets / data structures

FrogDB benefits from multi-shard parallelism when operations span different key ranges.
Single-key operations (ZADD/ZRANGE on one key) are comparable across all databases.

### Pub/sub

FrogDB uses shard-0 as coordinator for broadcast pub/sub. Performance is comparable to Redis
(both single-coordinator). Dragonfly may have higher throughput for high fan-out.

### YCSB workloads

- **YCSB-A (50/50 read/write):** Write-heavy, persistence mode matters most. FrogDB ≈ Redis
  with async durability.
- **YCSB-B (95/5 read/write):** Read-dominated, all databases perform well.
- **YCSB-C (100% read):** Pure read, favors caching efficiency. All databases similar.

### Cluster mode

FrogDB (single-process, multi-shard) avoids network hops for hash-slot routing. Redis Cluster
requires MOVED/ASK redirects for cross-slot operations, adding 1+ round trips. This gives
FrogDB an advantage for mixed-key workloads in cluster mode.

## Known Baselines

From `docs/spec/PROFILING-RESULTS.md` (macOS Apple Silicon, 128-byte values, read-heavy):

| Config | Ops/sec | GET p50 | GET p99 | GET p99.9 |
|--------|---------|---------|---------|-----------|
| 1 shard, 100 clients | 193,349 | 0.49ms | 1.22ms | 3.44ms |
| 4 shards, 100 clients | 186,369 | 0.50ms | 1.26ms | 3.22ms |
| 4 shards, pipeline=10 | 438,557 | 1.70ms | 13.50ms | 42.50ms |

## Current Optimization Opportunities

| Optimization | Impact | Description |
|-------------|--------|-------------|
| Write coalescing | **HIGH** | Batch multiple response writes into one writev syscall. Would reduce socket I/O from 15% → ~5% of CPU. |
| Read batching | **HIGH** | Buffer and batch-parse multiple commands from the same read. Reduces read syscalls. |
| clock_gettime reduction | **MEDIUM** | CommandTimer calls clock_gettime on every command. Batch or amortize via coarsened clock. |
