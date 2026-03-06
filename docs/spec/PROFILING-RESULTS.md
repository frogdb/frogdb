# FrogDB Profiling Results

**Date:** 2026-03-03
**Platform:** macOS Darwin 24.5.0 (Apple Silicon)
**Binary:** profiling build (release + debug symbols, jemalloc)
**Tool:** memtier_benchmark + samply

## Baseline Throughput

All tests use 128-byte values, 100k key space, read-heavy workload (1:19 SET:GET ratio).

| Config | Ops/sec | GET p50 | GET p99 | GET p99.9 |
|--------|---------|---------|---------|-----------|
| 1 shard, 100 clients | 193,349 | 0.49ms | 1.22ms | 3.44ms |
| 4 shards, 100 clients | 186,369 | 0.50ms | 1.26ms | 3.22ms |
| 1 shard, 200 clients | 188,325 | 0.98ms | 2.82ms | 8.58ms |
| 4 shards, 200 clients | 194,320 | 0.94ms | 2.86ms | 7.42ms |
| 4 shards, 100 clients, pipeline=10 | **438,557** | 1.70ms | 13.50ms | 42.50ms |

### Key Observations

1. **1→4 shard overhead is negligible** (~3.6% at 100 clients). At 200 clients, 4 shards is actually faster (+3.2%) because the single shard event loop becomes a contention point.

2. **Pipeline=10 gives 2.3x throughput** (186k → 439k ops/sec). This indicates ~57% of per-command cost is fixed overhead (syscalls, scheduling) rather than data processing. Pipelining amortizes read/write syscalls across multiple commands.

3. **Latency scales linearly with concurrency**: p50 doubles from 0.50ms→0.94ms when clients double (100→200), confirming queueing-dominated latency.

## CPU Profile Analysis

Captured with samply under sustained 190k ops/sec load (4 shards, 100 clients, 5M requests).

### Self-Time by Library

| Library | % of CPU | Notes |
|---------|----------|-------|
| libsystem_kernel.dylib | **91.05%** | Syscalls: socket I/O, epoll/kqueue, clock_gettime |
| frogdb-server | 7.36% | Application code + jemalloc + tokio runtime |
| libsystem_platform.dylib | 0.98% | CPU primitives (atomics, memory barriers) |
| libsystem_pthread.dylib | 0.35% | Thread synchronization |

**The system is overwhelmingly I/O-bound.** Only 7.36% of CPU time is spent in userspace code.

### Kernel Syscall Breakdown

| Syscall Caller | % of CPU | What It Does |
|----------------|----------|--------------|
| tokio park_internal | 58.05% | Workers idle waiting for tasks (expected) |
| tokio IO driver turn | 15.14% | Polling kqueue for socket readiness |
| TcpStream::poll_write_vectored | 7.59% | **Writing responses to clients** |
| TcpStream::poll_read_priv | 7.48% | **Reading requests from clients** |
| Timespec::now | 0.70% | clock_gettime for CommandTimer |
| Other | 2.04% | Miscellaneous |

**Socket I/O accounts for 15.07% of total CPU** (read + write). This is the primary throughput limiter after idle time.

### Top Application Code Hotspots

| Function | Self % | Category |
|----------|--------|----------|
| tokio scheduler (work-stealing) | 0.19% | Runtime overhead |
| tokio wake_by_val | 0.14% | Task waking |
| jemalloc sdallocx (dealloc) | 0.34% | Memory allocation |
| tokio ScheduledIo::wake | 0.14% | I/O readiness notification |
| SipHash::write (HashMap) | 0.09% | Command registry lookup |
| ConnectionHandler::run | 0.04% | Main connection loop |
| ShardWorker::run | 0.03% | Shard event loop |
| mpsc::send | 0.05% | Channel send to shard |
| execute_on_shard | 0.03% | Shard routing |
| PrometheusRecorder | 0.03% | Metrics recording |
| ParsedCommand::try_from | 0.02% | RESP parsing |
| Bytes::copy_from_slice | 0.03% | Memory copying |

### Inclusive Time (Total Time Including Callees)

| Function | Total % | Description |
|----------|---------|-------------|
| ConnectionHandler::run | 7.98% | Entire connection hot path |
| send_response | 7.76% | Response encoding + TCP write |
| TcpStream write | 7.60% | Actual TCP send syscall |
| TcpStream read | 7.49% | Actual TCP recv syscall |
| route_and_execute_with_transaction | 1.02% | Command dispatch pipeline |
| route_and_execute | 0.66% | Shard routing |
| ShardWorker::run | 0.57% | Shard execution |

## Hypothesis Validation

### Hypothesis 1: Per-command channel round-trip is a bottleneck
**REJECTED.** The oneshot channel + mpsc send + recv overhead is only 0.03-0.05% self-time. The 1-shard vs 4-shard delta (~3.6%) is negligible. The channel hop is NOT a meaningful bottleneck.

### Hypothesis 2: Duplicate work in shard execution
**REJECTED.** The duplicate `to_ascii_uppercase` and registry lookup are invisible in the profile. At ~190k ops/sec, each command's execution path is dominated by I/O, not string processing.

### Hypothesis 3: Per-command allocations
**PARTIALLY CONFIRMED.** jemalloc accounts for ~0.36% of CPU (0.34% dealloc + 0.02% alloc). This is measurable but not the primary bottleneck. The `Arc<ParsedCommand>` + oneshot pair allocations are real but small.

### Hypothesis 4: Shard event loop select! overhead
**REJECTED.** The shard event loop (`ShardWorker::run`) is only 0.57% total time. The timer tick branches (expiry, metrics, waiter timeout) are negligible since tokio's `select!` with intervals is very efficient when the interval hasn't fired.

### Hypothesis 5: Tracing span overhead
**NOT DIRECTLY MEASURABLE** from the profile, but the inclusive times show that the tracing infrastructure (span creation/enter/exit) is not showing up as a distinct hotspot. The spans use `info_span!` which is lightweight when no subscriber processes them at that level.

## Top Optimization Opportunities (Ranked by Impact)

### 1. Response Write Coalescing (HIGH IMPACT)

**Evidence:** `send_response` is 7.76% total time. Each command triggers a separate `flush()` syscall.

**Proposal:** Instead of flushing after every response, buffer responses and flush once per batch:
- After processing a command, check if more data is available on the socket (non-blocking peek)
- If yes, process all available commands first, accumulating responses
- Flush all responses in a single write syscall
- This is what Redis does with its "reply buffer" + "writable event" pattern

**Expected impact:** Could reduce write syscalls by 5-10x under load, potentially 30-50% throughput improvement. This is exactly why pipeline=10 gives 2.3x — it naturally batches writes.

### 2. Read Batching / Multi-Command Processing (HIGH IMPACT)

**Evidence:** `TcpStream::poll_read_priv` is 7.48% total time.

**Proposal:** When a read returns multiple complete RESP frames (common under load), process all of them before yielding to the event loop. Currently the `select!` loop in `ConnectionHandler::run` processes one frame per iteration.

**Expected impact:** Reduces read syscalls and select! iterations under load. Combined with write coalescing, this approaches pipelining-level throughput without client-side changes.

### 3. Reduce clock_gettime Syscalls (MEDIUM IMPACT)

**Evidence:** `Timespec::now` accounts for 0.70% of CPU — on macOS this is a real syscall (not VDSO like on Linux).

**Proposal:**
- Cache `Instant::now()` at the start of each command and reuse it for CommandTimer, slowlog timing, and stats
- Currently `Instant::now()` is called in: CommandTimer creation (line 556), `start_time` (line 555), and potentially in the timer drop
- On Linux this is free (VDSO), but on macOS every call is a syscall

**Expected impact:** ~0.5% CPU reduction on macOS. Minor but easy.

### 4. Eliminate Duplicate Command Processing (LOW IMPACT)

**Evidence:** Invisible in the profile, but architecturally wasteful.

The command name is uppercased twice (`connection.rs:549` and `execution.rs:23`) and the registry is looked up twice (`routing.rs:31` and `execution.rs:26`). While this doesn't show up in the profile because the cost is dwarfed by I/O, it could be eliminated by passing the pre-resolved handler through the `ShardMessage`.

**Expected impact:** Negligible at current throughput. Would matter at >500k ops/sec.

### 5. Local-Shard Fast Path (LOW IMPACT)

**Evidence:** Channel overhead is only 0.03-0.05% self-time. However, the total overhead including task wake + schedule is harder to measure.

**Proposal:** When a command routes to the local shard (same shard the connection is assigned to), bypass the channel entirely and execute inline. This eliminates: oneshot allocation, mpsc send, task wake, context switch, oneshot recv.

**Expected impact:** Marginal at current throughput (~2-5%). Would matter more at higher ops/sec or with 1-shard configurations.

## Redis Comparison (Informational)

Redis was tested in Docker (Linux VM on macOS), so numbers are ~2-3x lower than native. **Not directly comparable.**

| System | No Pipeline | Pipeline=10 |
|--------|-------------|-------------|
| FrogDB (native, 4 shards) | 186,369 ops/sec | 438,557 ops/sec |
| Redis 7 (Docker) | 48,514 ops/sec | 572,925 ops/sec |

FrogDB's multi-shard architecture gives it a significant advantage without pipelining. Redis' single-threaded design handles pipelining extremely efficiently (natural write batching).

## Summary

**The #1 bottleneck is per-command syscall overhead**, not application logic. The server spends 91% of CPU in kernel syscalls, primarily:
- Socket reads (7.48%)
- Socket writes (7.59%)
- IO polling (15.14%)
- Idle parking (58.05%)

The single highest-ROI optimization is **response write coalescing** — buffering multiple responses before flushing. This is exactly why pipelining (which naturally batches) gives 2.3x throughput. Implementing server-side write coalescing would give similar gains without requiring client-side pipeline support.
