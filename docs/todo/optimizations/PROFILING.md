# Profiling Infrastructure

## Build Profiles

A `profiling` Cargo profile is configured in the workspace `Cargo.toml`:

```toml
[profile.profiling]
inherits = "release"
debug = true
strip = false
```

Build with `just build-profile`.

## CPU Profiling (sampled)

| Tool | Platform | Command |
|------|----------|---------|
| cargo-flamegraph | All | `just profile-flamegraph` |
| samply | All | `just profile-samply` |
| perf | Linux | `just profile-perf` |

## Memory Profiling

| Tool | Platform | Use Case |
|------|----------|----------|
| heaptrack | Linux | Allocation tracking, leak detection |
| DHAT (valgrind) | Linux | Heap profiling |
| jemalloc prof | All | Production heap profiling |

## tokio-metrics (always-on)

Per-task busy/idle/scheduled timing via `TaskMonitor` wrappers. These add only lightweight atomic counters and are always compiled in.

**Instrumented tasks:**

| Task | Monitor name | Location |
|------|-------------|----------|
| Connection handlers | `connection` | `crates/server/src/acceptor.rs` |
| Shard workers | `shard_worker` | `crates/server/src/server/mod.rs` |
| WAL periodic sync | `wal_sync` | `crates/persistence/src/wal.rs` |

**Registry:** `TaskMonitorRegistry` (`crates/telemetry/src/task_monitors.rs`) creates monitors and spawns a background collector that drains interval stats every 10 seconds into the Prometheus metrics recorder.

**Exposed metrics** (label `task="<monitor name>"`):

- `frogdb_task_instrumented_count` — tasks created in the interval
- `frogdb_task_dropped_count` — tasks dropped in the interval
- `frogdb_task_total_poll_duration_seconds` — total CPU time spent polling
- `frogdb_task_total_scheduled_duration_seconds` — time waiting in the executor queue
- `frogdb_task_total_idle_duration_seconds` — time spent awaiting I/O / timers
- `frogdb_task_mean_poll_duration_seconds` — mean poll duration

**Usage:**
```bash
just run
curl -s localhost:9090/metrics | grep frogdb_task_
```

## Tracing spans (always-on)

Span instrumentation for critical paths. Spans compile to a single atomic load when no matching subscriber is active.

| Span | Location | Gate |
|------|----------|------|
| `wal_sync` | `crates/persistence/src/wal.rs` — periodic flush loop | Unconditional |
| `snapshot_create` | `crates/persistence/src/snapshot.rs` — background snapshot task | Unconditional |
| `shard_execute` | `crates/core/src/shard/event_loop.rs` — command execution | `per_request_spans` |
| `shard_exec_txn` | `crates/core/src/shard/event_loop.rs` — transaction execution | `per_request_spans` |
| `active_expiry` | `crates/core/src/shard/event_loop.rs` — TTL expiry sweep | `per_request_spans` |

Spans gated on `per_request_spans` are controlled at runtime via `CONFIG SET per-request-spans yes|no`.

## tracing-flame (feature-gated)

Async-aware flamegraphs that capture await/idle time in addition to CPU time. Compiled out by default behind `--features profiling`.

**Build & run:**
```bash
just build-profiling
just run-profiling

# Or with a custom output path:
FROGDB_FLAME_OUTPUT=my-trace.folded just run-profiling
```

**Generate flamegraph:**
```bash
# Run a workload
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t get,set

# Stop server (Ctrl-C) — the FlushGuard writes the folded stack file on drop
# Then convert to SVG:
inferno-flamegraph < tracing-flame.folded > flamegraph.svg
```

The `FlameLayer` is set up in `crates/server/src/main.rs`, following the same `#[cfg(feature = "...")]` pattern as `causal-profile`.

## Causal Profiling (tokio-coz)

See [TOKIO_CAUSAL_PROFILER.md](../TOKIO_CAUSAL_PROFILER.md) for the async-adapted causal profiling design.

```bash
just build-causal
just causal-profile
```

## Not integrated

| Tool | Reason |
|------|--------|
| tracing-timing | Redundant with existing Prometheus latency histograms via `CommandTimer` |
| tracing-tracy | Requires Tracy GUI; samply and cargo-flamegraph cover interactive profiling |

## Benchmark TODOs

- [ ] Add `scan.rs` benchmark — SCAN with 10K, 100K, 1M keys
- [ ] Add `large_values.rs` benchmark — GET/SET with 1KB, 10KB, 100KB values
- [ ] Add `sorted_set_scale.rs` benchmark — ZADD/ZRANGE at large cardinalities
