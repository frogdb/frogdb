---
name: profile
description: >
  Profile FrogDB performance: CPU sampling (samply), async flamegraphs (tracing-flame),
  causal profiling (tokio-coz), memory profiling (heaptrack), and always-on metrics
  (tokio-metrics). Use when the user asks to profile, investigate performance, generate
  flamegraphs, do causal/memory profiling, or analyze CPU/async hotspots.
  For benchmarking, load testing, or comparing against Redis/Valkey/Dragonfly,
  use the `/benchmark` skill instead.
---

# FrogDB Profiling

All profiling commands go through `just` (see `Justfile` at repo root).
Never run `cargo` directly — the Justfile sets required environment variables.

## Decision Tree

Pick the technique based on what the user wants to learn:

| Question | Technique | Command |
|----------|-----------|---------|
| Where is CPU time spent? | CPU sampling (samply) | `just profile-load` |
| Which async spans are slow? | tracing-flame | `just build-profiling` + `just run-profiling` |
| What if X were faster? | Causal profiling (tokio-coz) | `just causal-profile` |
| Where are allocations? | Memory profiling (heaptrack) | `just profile-heap` (Linux) |
| How fast vs Redis/Valkey? | **Use `/benchmark` skill** | `just benchmark` |
| How fast under load? | **Use `/benchmark` skill** | `just memtier` |
| Quick CPU flamegraph? | cargo-flamegraph | `just profile-flamegraph` |
| Tokio task metrics? | tokio-metrics (always-on) | `just run` + `curl localhost:9090/metrics` |

If the user's question is vague ("profile FrogDB"), start with **CPU sampling via samply**
(`just profile-load`) — it's the most general-purpose technique.

## 1. CPU Profiling with samply (recommended default)

### Automated workflow (profile under load)

```bash
just profile-load [workload] [requests] [extra-args]
# Defaults: workload=mixed, requests=10000
# Example: just profile-load read-heavy 50000
```

This builds with profiling symbols, starts FrogDB under samply, runs memtier load, and
captures a profile. Output: JSON profile at `$TMPDIR/frogdb-profiles/frogdb-profile-<timestamp>.json`.
Opens Firefox Profiler by default; pass `--save-only` to skip.

### Manual workflow

```bash
just build-profile          # Build: release + debug info + no strip
just profile-samply [args]  # Run samply on the binary (pass server args)
```

### Platform support
macOS + Linux. On macOS, samply uses `dtrace`; on Linux, `perf`.

## 2. Async-aware Flamegraphs (tracing-flame)

Captures **await/idle time**, not just CPU — shows where async tasks spend time waiting.
Use when you need the full async story.

```bash
just build-profiling          # Enables `profiling` feature -> tracing-flame
just run-profiling [args]     # Outputs folded stacks on shutdown (Ctrl-C)
```

Output: `tracing-flame.folded` (override with `FROGDB_FLAME_OUTPUT=path`).
Convert to SVG:

```bash
inferno-flamegraph < tracing-flame.folded > flamegraph.svg
```

## 3. Causal Profiling (tokio-coz)

Answers counterfactual questions: "if this code path were N% faster, how much would
overall throughput improve?" Useful when you know something is slow but need to know
**what matters most**.

### Automated workflow

```bash
just causal-profile [workload] [duration] [extra-args]
# Defaults: workload=mixed, duration=90 (seconds)
# Example: just causal-profile write-heavy 120 --profile release
```

Sets `COZ_PROFILE=1`, runs load, outputs `causal-profile.json` with impact curves.

### Manual build

```bash
just build-causal [debug|release]   # Sets RUSTFLAGS="--cfg tokio_unstable"
```

Progress point: `commands_processed` counter in `connection.rs`.

## 4. CPU Flamegraph (cargo-flamegraph)

Quick one-off investigation — no Python scripts, no load generation.

```bash
just profile-flamegraph [args]   # Pass server args directly
# Output: flamegraph.svg
```

## 5. Memory Profiling

### heaptrack (Linux only)

```bash
just profile-heap [args]
```

### jemalloc

Always linked via `tikv-jemallocator`. No special build needed.

## 6. perf (Linux only)

```bash
just profile-perf [args]    # Output: perf.data
```

Low-level CPU analysis with hardware counter access.

## Load Generation

Profiling-under-load workflows use **memtier_benchmark** via `just profile-load` and
`just causal-profile`. Both accept a workload preset:

| Preset | GET:SET ratio | Key distribution |
|--------|--------------|-----------------|
| `read-heavy` | 19:1 | Gaussian |
| `write-heavy` | 1:19 | Sequential |
| `mixed` (default) | 9:1 | Gaussian |

For the full workload catalog (16 YAML workloads), comparative benchmarks, and standalone
load testing, see the **`/benchmark` skill**.

## Always-On Instrumentation

These require no special build — available in any `just run` build.

### tokio-metrics

Task monitors on connection handlers, shard workers, WAL sync. Exposed via Prometheus:

```bash
just run
curl -s localhost:9090/metrics | grep frogdb_task_
```

### Tracing spans

Critical path spans: `wal_sync`, `snapshot_create`, `shard_execute`, `shard_exec_txn`,
`active_expiry`. Per-request spans gated on runtime config:

```
CONFIG SET per-request-spans yes
CONFIG SET per-request-spans no
```

## Build Profiles

| Profile | Command | Use Case |
|---------|---------|----------|
| `profiling` (Cargo profile) | `just build-profile` | CPU sampling (release + debug symbols) |
| `profiling` feature flag | `just build-profiling` | tracing-flame async flamegraphs |
| `causal-profile` feature flag | `just build-causal` | tokio-coz causal profiling |

## Prerequisites

| Tool | Install | Used by |
|------|---------|---------|
| memtier_benchmark | `brew install memtier_benchmark` | All load generation |
| samply | `cargo install samply` | CPU profiling |
| cargo-flamegraph | `cargo install cargo-flamegraph` | Quick flamegraphs |
| inferno | `cargo install inferno` | tracing-flame SVG |
| Docker | Docker Desktop / `docker` CLI | Comparative benchmarks |
| heaptrack | Linux package manager | Memory profiling |

## Reference Files

- `todo/optimizations/PROFILING.md` — Full profiling infrastructure guide
- `docs/TOKIO_CAUSAL_PROFILER.md` — tokio-coz design document
- `testing/load/scripts/profile_load.py` — CPU profiling under load
- `testing/load/scripts/causal_profile.py` — Causal profiling under load
- `testing/load/scripts/analyze_profile.py` — samply profile analysis

## When to Defer

| Situation | What to do |
|-----------|------------|
| Benchmarking, load testing, database comparison | Use the `/benchmark` skill |
| Build/lint/test checks | Use the `/check` skill |
| Jepsen distributed tests | Use the `/jepsen-testing` skill |
| Fuzz testing | `cd fuzz && cargo +nightly fuzz run <target>` |
