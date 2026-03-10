# FrogDB Load Testing Tools

This directory contains tools for load testing and benchmarking FrogDB, including scripts for running external Redis ecosystem tools and comparing performance against real Redis.

## Directory Structure

```
testing/load-test/
├── README.md           # This file
├── scripts/
│   ├── run_redis_benchmark.py   # redis-benchmark wrapper
│   ├── run_memtier.py           # memtier_benchmark wrapper
│   ├── compare_redis.py         # FrogDB vs Redis comparison
│   └── parse_results.py         # Result aggregation and reporting
├── workloads/
│   ├── read-heavy.yaml          # 95% reads (session store scenario)
│   ├── write-heavy.yaml         # 95% writes (logging scenario)
│   └── mixed-realistic.yaml     # 90% reads, 10% writes (typical app)
├── configs/
│   ├── frogdb-async.toml        # Maximum throughput
│   ├── frogdb-periodic.toml     # Balanced (default)
│   └── frogdb-sync.toml         # Maximum durability
└── reports/                      # Generated test results
```

## Prerequisites

### Install redis-benchmark (included with Redis)

```bash
# macOS
brew install redis

# Ubuntu/Debian
sudo apt install redis-tools
```

### Install memtier_benchmark (recommended)

```bash
# macOS
brew install memtier_benchmark

# Ubuntu/Debian (build from source)
git clone https://github.com/RedisLabs/memtier_benchmark.git
cd memtier_benchmark
autoreconf -ivf
./configure
make
sudo make install
```

## Quick Start

### 1. Start FrogDB

```bash
# Default configuration
cargo run --release

# With specific durability mode
cargo run --release -- -c testing/load-test/configs/frogdb-periodic.toml
```

### 2. Run Load Tests

```bash
# Quick sanity check with redis-benchmark
uv run testing/load-test/scripts/run_redis_benchmark.py

# Comprehensive test with memtier_benchmark
uv run testing/load-test/scripts/run_memtier.py

# Predefined workload
uv run testing/load-test/scripts/run_memtier.py -w read-heavy
```

### 3. Compare Against Redis (optional)

```bash
# Start Redis on port 6380
redis-server --port 6380 &

# Run comparison
uv run testing/load-test/scripts/compare_redis.py
```

## Micro-benchmarks (Criterion.rs)

In addition to external load testing tools, FrogDB includes Rust-native micro-benchmarks using Criterion.rs.

```bash
# Run all micro-benchmarks
cargo bench

# Run specific benchmark group
cargo bench --bench store
cargo bench --bench commands
cargo bench --bench persistence
cargo bench --bench protocol

# Compare against saved baseline
cargo bench -- --baseline main
```

Micro-benchmark categories:
- **store**: HashMapStore operations (get, set, delete, scan)
- **commands**: Data type operations (string, hash, list, set, sorted set)
- **persistence**: WAL write with different durability modes
- **protocol**: RESP2/RESP3 encoding and parsing

## Workload Descriptions

### Read-Heavy (95% reads)
Simulates a session store or cache scenario with hot keys.
- 95% GET, 5% SET
- Gaussian key distribution
- Target: <500μs GET p99

### Write-Heavy (95% writes)
Simulates logging or queue scenarios.
- 5% GET, 95% SET
- Sequential keys
- Target: <500μs SET p99 (async)

### Mixed Realistic (90% reads)
Typical web application workload.
- 90% GET, 10% SET
- Gaussian key distribution
- Target: 80k+ ops/sec

## Performance Targets

From `spec/OBSERVABILITY.md`:

| Operation | p50 | p95 | p99 |
|-----------|-----|-----|-----|
| GET (hot) | <100μs | <200μs | <500μs |
| SET (async) | <100μs | <200μs | <500μs |
| SET (sync) | <1ms | <2ms | <5ms |

Throughput targets (single-core, pipelined):
- GET: 100k+ ops/sec
- SET: 50k+ ops/sec

## Durability Mode Comparison

Test persistence overhead by running the same workload with different durability configurations:

```bash
# Async (fastest)
cargo run --release -- -c testing/load-test/configs/frogdb-async.toml &
uv run testing/load-test/scripts/run_memtier.py -w write-heavy --json reports/async.json

# Periodic (balanced)
cargo run --release -- -c testing/load-test/configs/frogdb-periodic.toml &
uv run testing/load-test/scripts/run_memtier.py -w write-heavy --json reports/periodic.json

# Sync (most durable)
cargo run --release -- -c testing/load-test/configs/frogdb-sync.toml &
uv run testing/load-test/scripts/run_memtier.py -w write-heavy --json reports/sync.json

# Compare results
uv run testing/load-test/scripts/parse_results.py --frogdb reports/periodic.json
```

## Script Reference

All scripts use PEP 723 inline script metadata and can be run via `uv run`.

### run_redis_benchmark.py

```bash
uv run run_redis_benchmark.py [OPTIONS]

Options:
  -h, --host HOST       Server hostname (default: 127.0.0.1)
  -p, --port PORT       Server port (default: 6379)
  -c, --clients N       Parallel connections (default: 50)
  -n, --requests N      Total requests (default: 100000)
  -d, --datasize N      Value size in bytes (default: 128)
  -P, --pipeline N      Pipeline depth (default: 1)
  -w, --workload NAME   Preset: read-heavy, write-heavy, mixed, all
  --csv                 Output in CSV format
```

### run_memtier.py

```bash
uv run run_memtier.py [OPTIONS]

Options:
  -h, --host HOST       Server hostname (default: 127.0.0.1)
  -p, --port PORT       Server port (default: 6379)
  -t, --threads N       Number of threads (default: 4)
  -c, --clients N       Clients per thread (default: 25)
  -n, --requests N      Requests per client (default: 10000)
  -d, --datasize N      Value size in bytes (default: 128)
  --ratio R:W           Read:Write ratio (default: 1:1)
  --key-pattern P       P=parallel, R=random, S=sequential, G=gaussian
  --pipeline N          Pipeline depth (default: 1)
  -w, --workload NAME   Preset: read-heavy, write-heavy, mixed
  --json FILE           Output JSON results
  --hdr FILE            Output HDR histogram
```

### compare_redis.py

```bash
uv run compare_redis.py [OPTIONS]

Options:
  --frogdb-port PORT    FrogDB port (default: 6379)
  --redis-port PORT     Redis port (default: 6380)
  -w, --workload NAME   Workload preset (default: mixed)
  -n, --requests N      Requests per client (default: 10000)
  -o, --output DIR      Output directory (default: ./reports)
```

### parse_results.py

```bash
uv run parse_results.py [OPTIONS]

Options:
  --frogdb FILE         FrogDB memtier JSON results (required)
  --redis FILE          Redis memtier JSON results (for comparison)
  --output FILE         Output file (default: stdout)
  --json                Output raw metrics as JSON
```

## Interpreting Results

### Key Metrics

- **ops/sec**: Overall throughput
- **p50/p95/p99/p99.9**: Latency percentiles in milliseconds
- **KB/sec**: Data throughput

### Example Output

```
============================================================
FrogDB Load Test Report
============================================================
Throughput:     85.2K ops/sec
GET Latency:    p50=0.08ms  p95=0.15ms  p99=0.35ms  p99.9=1.20ms

Performance vs Targets:
  GET p99 target: <500us    Actual: 350us    [OK]
  SET p99 target: <500us    Actual: 280us    [OK]
============================================================
```

## Tips

1. **Warm up first**: Run a short warmup before the main benchmark
2. **Consistent environment**: Close other applications during benchmarks
3. **Multiple runs**: Run benchmarks 3+ times and take the median
4. **Match configurations**: When comparing to Redis, match persistence settings
5. **Monitor resources**: Watch CPU, memory, and disk I/O during tests
