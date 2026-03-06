# Profiling Infrastructure

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

For profiling tokio task-level performance, use `tokio-metrics` and `tracing` spans to measure per-task busy/idle/scheduled timing and identify latency bottlenecks.

Instrumentation is split into two tiers:

*Always-on (production)* — The instrumentation points themselves. These live in production code unconditionally. `tracing` spans compile to a single atomic load when no matching subscriber is active; `tokio-metrics` `TaskMonitor` wrappers add only lightweight atomic counters.

| Tool | Crate | Use Case |
|------|-------|----------|
| tokio-metrics | `tokio-metrics` | Per-task busy/idle/scheduled timing via `TaskMonitor` |
| tracing spans | `tracing` | Span-based latency analysis at task/operation granularity |

- [x] Add `tokio-metrics` `TaskMonitor` instrumentation for key task types (connection handler, shard workers, WAL sync)
- [x] Add `tracing` span instrumentation for critical request paths (WAL sync, snapshots, active expiry, transactions)

*Feature-gated (`profiling`)* — The subscriber layers that consume spans. These have non-trivial CPU/memory overhead and are compiled out by default behind a cargo feature flag (`--features profiling`), following the existing pattern in `crates/server/Cargo.toml` (cf. `causal-profile = ["dep:tokio-coz"]`).

| Tool | Crate | Use Case |
|------|-------|----------|
| tracing-timing | `tracing-timing` | Latency histograms derived from tracing spans |
| tracing-flame | `tracing-flame` | Async-aware flamegraphs — captures await/idle time, not just CPU |
| tracing-tracy | `tracing-tracy` | Real-time interactive profiling via Tracy with nanosecond precision |

- ~Integrate `tracing-timing` for per-operation latency histograms~ — Skipped: redundant with existing Prometheus latency histograms via CommandTimer
- [x] Add `tracing-flame` `FlameLayer` for async-aware flamegraph generation
- ~Evaluate `tracing-tracy` for interactive real-time profiling during development~ — Skipped: requires Tracy GUI; samply and cargo-flamegraph cover interactive profiling

> **Research concept:** For causal profiling adapted to async runtimes, see [TOKIO_CAUSAL_PROFILER.md](../TOKIO_CAUSAL_PROFILER.md).
- [ ] Add `scan.rs` benchmark - SCAN with 10K, 100K, 1M keys
- [ ] Add `large_values.rs` benchmark - GET/SET with 1KB, 10KB, 100KB values
- [ ] Add `sorted_set_scale.rs` benchmark - ZADD/ZRANGE at large cardinalities
- [ ] Extend `slowlog.rs` with latency histograms via `tracing-timing`
