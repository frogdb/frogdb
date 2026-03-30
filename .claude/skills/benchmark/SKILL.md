---
name: benchmark
description: >
  Benchmark FrogDB: standalone load testing (memtier_benchmark), comparative benchmarks
  against Redis/Valkey/Dragonfly, cluster-mode scaling, Criterion micro-benchmarks,
  durability mode comparison, and result interpretation with architectural context.
  Use when the user asks to benchmark, compare performance, run load tests, measure
  throughput/latency, compare against Redis/Valkey/Dragonfly, run Criterion benchmarks,
  test different durability modes, or understand why one database outperforms another.
---

# FrogDB Benchmarking

All benchmark commands go through `just` (see `Justfile` at repo root).
Never run `cargo` or `uv run` directly — the Justfile sets required environment variables.

## Decision Tree

Pick the technique based on what the user wants:

| Question | Technique | Command |
|----------|-----------|---------|
| How fast is FrogDB? | Standalone load test | `just memtier [workload] [requests]` |
| FrogDB vs Redis/Valkey/Dragonfly? | Docker comparative | `just benchmark [workload] [requests]` |
| Cluster-mode scaling? | Cluster comparison | `just compare-cluster [workload] [requests]` |
| Full comparison + CPU isolation? | Advanced comparison | `just compare-all [workload] [requests]` |
| FrogDB vs local Redis? | Local comparison | `just compare-redis [workload] [requests]` |
| Micro-benchmark internals? | Criterion | `just bench` |
| Quick sanity check? | redis-benchmark | `just redis-bench [workload] [requests]` |
| Durability mode impact? | Manual 3-config workflow | See [Durability Mode Comparison](#6-durability-mode-comparison) |
| Parse/report results? | Post-processing | `just benchmark-parse` / `just benchmark-report` |
| Profile CPU/memory? | Use `/profile` skill | — |

**Default for vague requests:** `just benchmark mixed-realistic` (comparative benchmark with
a realistic mixed workload against all backends).

## 1. Comparative Benchmarks (Docker)

### Single-node comparison (`just benchmark`)

Runs FrogDB, Redis, Valkey, and Dragonfly in Docker, then benchmarks each with
memtier_benchmark using the same workload.

```bash
just benchmark [workload] [requests]
# Defaults: workload=ycsb-a, requests=100000
# Examples:
just benchmark mixed-realistic 200000
just benchmark read-heavy 50000
```

Uses `--all --start-docker` automatically. Results saved to `testing/load/reports/`.

Docker compose: `testing/load/docker-compose.benchmarks.yml`

| Container | Port | Image |
|-----------|------|-------|
| frogdb | 16379 | `frogdb:latest` (local build) |
| redis | 26379 | `redis:latest` |
| valkey | 36379 | `valkey/valkey:latest` |
| dragonfly | 46379 | `docker.dragonflydb.io/dragonflydb/dragonfly` |

**Important:** Build the FrogDB Docker image first: `just docker-build-bench` (ARM/Apple Silicon)
or `just docker-build-prod` (x86).

### Cluster-mode comparison (`just compare-cluster`)

Compares FrogDB (3 shards, single process) vs Redis Cluster (3 masters) vs Valkey Cluster
(3 masters) vs Dragonfly (3 proactor threads) — all in Docker.

```bash
just compare-cluster [workload] [requests]
# Defaults: workload=mixed, requests=10000
# Examples:
just compare-cluster read-heavy 50000
just compare-cluster mixed 100000 --keep  # keep containers running
```

Docker compose: `testing/load/docker-compose.cluster-benchmarks.yml`

| Architecture | FrogDB | Redis/Valkey | Dragonfly |
|-------------|--------|-------------|-----------|
| Process model | Single process, 3 shards | 3 separate processes | Single process, 3 proactors |
| Routing | Internal hash-slot | Client-side MOVED/ASK | Internal |
| Network hops | 1 | 1-2 (redirects) | 1 |

### Advanced comparison with CPU isolation (`just compare-all`)

Full comparison with CPU pinning (cpuset), scaling tests, and resource isolation.

```bash
just compare-all [workload] [requests] [extra-args]
# Defaults: workload=mixed, requests=10000
# Examples:
just compare-all mixed 50000 --isolate --cpus 2        # pin 2 CPUs per server
just compare-all read-heavy 100000 --scaling            # test 1, 2, 4 cores
just compare-all mixed 50000 --frogdb-native            # run FrogDB outside Docker
```

Key flags (passed as extra args):
- `--isolate` — Pin each server to dedicated CPU cores via cpuset
- `--scaling` — Run scaling test across 1, 2, 4 cores
- `--cpus N` — CPUs per server (default: 1)
- `--frogdb-native` — Run FrogDB natively (not in Docker) for fairer comparison
- `--extended` — Include hash, list, and sorted-set workloads

### Local FrogDB vs Redis comparison (`just compare-redis`)

Quick head-to-head when both are running locally (no Docker).

```bash
just compare-redis [workload] [requests]
# Defaults: workload=mixed, requests=10000
# Assumes: FrogDB on :6379, Redis on :6380
# Override ports via extra args: --frogdb-port 6379 --redis-port 6380
```

## 2. Standalone Load Testing

### memtier_benchmark (`just memtier`)

Runs memtier_benchmark directly against a running FrogDB instance.

```bash
just memtier [workload] [requests]
# Defaults: workload=mixed, requests=10000
# Examples:
just memtier read-heavy 100000
just memtier write-heavy 50000 --pipeline 10
just memtier mixed 200000 --threads 8 --clients 50
```

Extra flags (passed through to `run_memtier.py`):
- `--pipeline N` — Pipeline N requests (default: 1)
- `--threads N` — Number of threads (default: 4)
- `--clients N` — Clients per thread (default: 25)
- `--host HOST` / `--port PORT` — Target server (default: 127.0.0.1:6379)
- `--json FILE` — Output JSON results to file
- `--hdr FILE` — Output HDR histogram to file
- `--test-time N` — Run for N seconds instead of fixed request count

Workload presets: `read-heavy`, `write-heavy`, `mixed`

### redis-benchmark (`just redis-bench`)

Quick sanity check using the standard `redis-benchmark` tool.

```bash
just redis-bench [workload] [requests]
# Defaults: workload=all, requests=100000
# Examples:
just redis-bench mixed 50000
just redis-bench read-heavy 200000 --csv
just redis-bench all 100000 -P 16   # with pipelining
```

Extra flags: `--csv` (CSV output), `-P N` (pipeline), `-c N` (clients), `-d N` (data size)

## 3. Workloads

16 YAML workloads in `testing/load/workloads/`:

| Category | Workload | Description | Read:Write |
|----------|----------|-------------|------------|
| **Basic** | `read-heavy` | Read-dominated cache workload | 90:10 |
| | `write-heavy` | Write-dominated ingestion | 10:90 |
| | `mixed-realistic` | Realistic production mix | 70:30 |
| **Data-type** | `user-profile` | Hash-based user profiles | HGET/HSET |
| | `leaderboard` | Sorted-set leaderboard | ZADD/ZRANGE |
| | `pubsub-fanout` | Pub/sub fan-out | PUB/SUB |
| | `geo-nearby` | Geospatial queries | GEOADD/GEORADIUS |
| | `timeseries` | Time-series ingestion | ZADD/ZRANGEBYSCORE |
| | `session-store` | Session CRUD | GET/SET/DEL + TTL |
| | `cache-aside` | Cache-aside pattern | GET/SET/DEL |
| | `counter` | Atomic counters | INCR/DECR/GET |
| | `rate-limiter` | Sliding-window rate limiting | INCR + EXPIRE |
| | `message-queue` | List-based message queue | LPUSH/BRPOP |
| **YCSB** | `ycsb-a` | Update-heavy (50/50) | 50:50 |
| | `ycsb-b` | Read-heavy (95/5) | 95:5 |
| | `ycsb-c` | Read-only (100/0) | 100:0 |

### YAML workload schema

```yaml
name: workload-name
description: "..."
commands:
  - GET: 50%
  - SET: 50%
keys:
  pattern: zipfian | uniform | sequential
  space_size: 1000000
  prefix: "key:"
data:
  size_bytes: 1000
  size_distribution: fixed | uniform
concurrency:
  clients: 100
  threads: 4
  pipeline: 1
targets:                    # optional pass/fail thresholds
  ops_per_sec: 50000
  p99_latency_ms: 5.0
```

### Choosing a workload

- **Vague "benchmark it"** → `mixed-realistic` (closest to production)
- **Comparing databases** → `ycsb-a` (industry standard, default for `just benchmark`)
- **Testing data structures** → Use the data-type workloads (user-profile, leaderboard, etc.)
- **Max throughput** → `read-heavy` or `ycsb-c` with `--pipeline 10`
- **Write stress** → `write-heavy` or `ycsb-a`

## 4. Criterion Micro-Benchmarks

Criterion benchmarks live in `frogdb-server/benchmarks/frogdb-benches/`.

```bash
just bench                    # Run all Criterion benchmarks
```

Benchmark targets: store operations, command execution, persistence (serialization/WAL),
RESP protocol parsing.

### Baseline comparison workflow

```bash
just bench                              # Run and save current results
# ... make changes ...
just bench                              # Criterion auto-compares to previous run
```

Criterion outputs comparison statistics automatically (% change, significance).

**Note:** `frogdb-benches` may have pre-existing compile errors (e.g., missing `channel_capacity`
in `WalConfig`). If so, fix the struct initialization or skip with `--bench <specific-target>`.

## 5. Result Post-Processing

### Parse memtier results (`just benchmark-parse`)

```bash
just benchmark-parse results/frogdb.json
just benchmark-parse results/frogdb.json --redis results/redis.json   # side-by-side
just benchmark-parse results/frogdb.json --json                       # raw metrics
```

### Generate Markdown report (`just benchmark-report`)

```bash
just benchmark-report results/combined_results.json
just benchmark-report results/combined_results.json --cpus 2 --isolated
```

Generates a comprehensive Markdown report with throughput/latency tables, performance
comparisons, and methodology notes.

## 6. Durability Mode Comparison

FrogDB supports three durability modes. Compare the same workload across all three to
measure the persistence cost.

### Configs

| Mode | Config file | Behavior |
|------|------------|----------|
| Async (fastest) | `testing/load/configs/frogdb-async.toml` | WAL writes buffered, no fsync |
| Periodic (default) | `testing/load/configs/frogdb-periodic.toml` | fsync every N ms |
| Sync (safest) | `testing/load/configs/frogdb-sync.toml` | fsync on every write |

### Workflow

```bash
# Run the same workload against each durability mode
just run -- --config testing/load/configs/frogdb-async.toml &
just memtier mixed-realistic 100000 --json results/async.json
kill %1

just run -- --config testing/load/configs/frogdb-periodic.toml &
just memtier mixed-realistic 100000 --json results/periodic.json
kill %1

just run -- --config testing/load/configs/frogdb-sync.toml &
just memtier mixed-realistic 100000 --json results/sync.json
kill %1

# Compare results
just benchmark-parse results/async.json
just benchmark-parse results/periodic.json
just benchmark-parse results/sync.json
```

### Expected impact

- Async → Periodic: ~5-15% throughput reduction
- Periodic → Sync: ~30-60% throughput reduction (highly workload-dependent)
- Read-heavy workloads: minimal impact across all modes

## 7. Configuration

### Docker compose files

| File | Purpose |
|------|---------|
| `testing/load/docker-compose.benchmarks.yml` | Single-node comparison (FrogDB, Redis, Valkey, Dragonfly) |
| `testing/load/docker-compose.cluster-benchmarks.yml` | Cluster-mode comparison |

### FrogDB config files

| File | Purpose |
|------|---------|
| `testing/load/configs/frogdb-async.toml` | Async durability (no fsync) |
| `testing/load/configs/frogdb-periodic.toml` | Periodic fsync |
| `testing/load/configs/frogdb-sync.toml` | Sync durability (fsync per write) |

### Shard count effects

1→4 shards: ~3.6% overhead at 100 clients. At 200+ clients, 4 shards is faster
because the single shard event loop becomes a contention point.

## 8. Interpreting Results

### Key metrics

- **Ops/sec** — Primary throughput metric
- **p50 latency** — Median response time
- **p99 latency** — Tail latency (most important for SLAs)
- **p99.9 latency** — Extreme tail (connection issues, GC pauses)

### Known baselines (from PROFILING-RESULTS.md)

| Config | Ops/sec | GET p50 | GET p99 |
|--------|---------|---------|---------|
| 1 shard, 100 clients | 193k | 0.49ms | 1.22ms |
| 4 shards, 100 clients | 186k | 0.50ms | 1.26ms |
| 4 shards, pipeline=10 | 439k | 1.70ms | 13.50ms |

### Red flags

- Ops/sec significantly below 150k for basic GET/SET → check Docker resource limits
- p99 > 10ms without pipelining → potential contention or resource exhaustion
- Large variance between runs → unstable environment, run more iterations
- Dragonfly >> FrogDB by large margin → expected for highly optimized workloads

### Why is X faster than Y?

Read `references/database-comparison.md` for detailed architectural explanations.
Key factors: threading model, syscall batching, pipelining support, persistence mode.

## 9. Methodology

For reliable, reproducible results:

1. **Warmup** — Run 1-2 short warmup iterations before the real benchmark
2. **Multiple runs** — Run at least 3 iterations; report median
3. **Consistent environment** — Close background apps, use consistent Docker resource limits
4. **Match persistence** — Compare like-for-like (all async, or all sync)
5. **Docker fairness** — Use `--isolate` with `just compare-all` for CPU pinning
6. **Resource monitoring** — Watch `docker stats` during runs for resource saturation
7. **Pipeline parity** — Ensure pipeline depth matches across all backends

## 10. Prerequisites

| Tool | Install | Used by |
|------|---------|---------|
| memtier_benchmark | `brew install memtier_benchmark` | `just memtier`, `just benchmark`, comparisons |
| Docker | Docker Desktop / `docker` CLI | All comparative benchmarks |
| redis-benchmark | Bundled with `redis` (`brew install redis`) | `just redis-bench` |
| redis-cli | Bundled with `redis` | Cluster setup verification |

## 11. Reference Files

### Scripts

| Script | Justfile target | Purpose |
|--------|----------------|---------|
| `testing/load/scripts/benchmark.py` | `just benchmark` | Docker comparative benchmark |
| `testing/load/scripts/run_memtier.py` | `just memtier` | Standalone memtier wrapper |
| `testing/load/scripts/run_redis_benchmark.py` | `just redis-bench` | redis-benchmark wrapper |
| `testing/load/scripts/compare_all.py` | `just compare-all` | Full comparison + CPU isolation |
| `testing/load/scripts/compare_cluster.py` | `just compare-cluster` | Cluster-mode comparison |
| `testing/load/scripts/compare_redis.py` | `just compare-redis` | Local FrogDB vs Redis |
| `testing/load/scripts/generate_report.py` | `just benchmark-report` | Markdown report generation |
| `testing/load/scripts/parse_results.py` | `just benchmark-parse` | Parse memtier results |

### Config & workloads

- `testing/load/workloads/` — 16 YAML workload definitions
- `testing/load/configs/` — FrogDB durability config files
- `testing/load/docker-compose.benchmarks.yml` — Single-node Docker setup
- `testing/load/docker-compose.cluster-benchmarks.yml` — Cluster Docker setup

### Documentation

- `references/database-comparison.md` — Architecture comparison (FrogDB vs Redis vs Valkey vs Dragonfly), baseline throughput data

## 12. When to Defer

| Situation | What to do |
|-----------|------------|
| CPU/memory profiling | Use the `/profile` skill |
| Flamegraphs, causal profiling | Use the `/profile` skill |
| Build/lint/test checks | Use the `/check` skill |
| Jepsen distributed tests | Use the `/jepsen-testing` skill |
| Fuzz testing | `cd fuzz && cargo +nightly fuzz run <target>` |
