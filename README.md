# FrogDB (Sunbird?)- Agent Context

FrogDB is a Redis-compatible memory-first database written in Rust. Its goals are to to be correct,
very very fast/efficient, memory safe, durable, scalable, and easy to operate in the wild. It intends to supplant prior
solutions by trying to be better in every way.

It is wire-compatible with Redis v6+ (RESP2 + RESP3 compatible) so you can use it with existing
Redis clients. [Planned] If you don't need redis compatibility It includes a custom high-performance network protocol with clients for major languages built around

[Planned] It includes a migration mode + tooling to migrate your existing Redis/Valkey/DragonflyDB
deployment to a FrogDB deployment with zero or close-to-zero downtime with no data loss.

It supports many configurable options for performance tuning with sensible defaults.

Operationally it supports Prometheus as well as OpenTelemetry metrics, tracing, and logging.

To manage FrogDB in cluster mode you can use a Kubernetes operator, TKTK.

## Goals

- _Correctness_: clear guarantees about consistency and failure modes that are verified using
  comprehensive testing, including Jepsen(tm) testing, as well as extensive testing of Redis
  compatibility.
- _Fastness/Efficiency_: Extensive benchmarking to ensure the performance cost of every change or
  feature detail across memory, compute and I/O are understood and kept to a minimum.
- _Memory/Thread Safety_: Using Rust while avoiding usages of `unsafe` as much as possible to minimize
  bugs/crashes and security vulnerabilities.
- _Durability_: supports multiple modes of persistence that are tunable to balance performance and safety
  depending on use case.
- _Scalable_: Built with clustered operation in mind from the start. Scales vertically with additional cores.
- _Easy to operate_: Provide those responsible for running the software with the information they need
  to diagnose problems and take action to resolve them as much as possible. Integrate with existing
  CNCF and other ecosystems to make integration easy. Online cluster resizing, recovery tools, and
  more.

## Design Spec

Root document located in `spec/INDEX.md`. Supplemental documents describing various parts of the system are located within the `spec/` directory.

## Building & Running

### Prerequisites

- Rust 1.75+ (2024 edition)

### Build

```bash
cargo build --release
```

### Run

```bash
# Start with defaults (127.0.0.1:6379, 1 shard)
cargo run --release --bin frogdb-server

# With options
cargo run --release --bin frogdb-server -- --port 6380 --shards auto --log-level debug

# Generate default config file
cargo run --release --bin frogdb-server -- --generate-config > frogdb.toml
```

### Test

```bash
# Unit tests (all crates)
cargo test

# Concurrency tests (using Shuttle for deterministic testing)
cargo test -p frogdb-core --features shuttle --test concurrency

# Integration tests (uses dynamic port allocation)
cargo test --test integration

# All tests
cargo test --all
```

#### Concurrency Testing

FrogDB uses [Shuttle](https://github.com/awslabs/shuttle) for deterministic concurrency testing. Shuttle tests run concurrent code under a randomized scheduler to catch race conditions and ordering bugs.

```bash
# Run all shuttle concurrency tests (1000 iterations each)
cargo test -p frogdb-core --features shuttle --test concurrency

# Run a specific concurrency test
cargo test -p frogdb-core --features shuttle --test concurrency test_read_your_writes
```

The concurrency tests cover:

- Connection ID uniqueness under concurrent access
- Round-robin shard assignment correctness
- Read-your-writes consistency
- Command ordering guarantees
- Concurrent increment operations

### Linting

```bash
# Run clippy on all targets (library, binaries, tests)
cargo clippy --all-targets

# Treat warnings as errors (useful for CI)
cargo clippy --all-targets -- -D warnings

# Run with all features enabled
cargo clippy --all-targets --all-features
```

## Configuration

FrogDB uses a layered configuration system (highest priority first):

1. CLI arguments (`--port 6379`)
2. Environment variables (`FROGDB_SERVER__PORT=6379`)
3. TOML config file (`frogdb.toml`)
4. Built-in defaults

### Example Configuration (frogdb.toml)

```toml
[server]
bind = "127.0.0.1"
port = 6379
num_shards = 1  # 0 = auto-detect CPU cores

[logging]
level = "info"   # trace, debug, info, warn, error
format = "pretty" # pretty, json
```

### CLI Arguments

```
frogdb-server [OPTIONS]

Options:
  -c, --config <FILE>     Configuration file path
  -b, --bind <ADDR>       Bind address
  -p, --port <PORT>       Listen port
  -s, --shards <N>        Number of shards (or "auto")
  -l, --log-level <LEVEL> Log level
      --log-format <FMT>  Log format (pretty/json)
      --generate-config   Print default config and exit
  -h, --help              Print help
  -V, --version           Print version
```

## Usage with Redis CLI

```bash
# Start the server
cargo run --release --bin frogdb-server

# In another terminal, use redis-cli
redis-cli -p 6379 PING           # PONG
redis-cli -p 6379 SET foo bar    # OK
redis-cli -p 6379 GET foo        # "bar"
redis-cli -p 6379 DEL foo        # (integer) 1
redis-cli -p 6379 EXISTS foo     # (integer) 0
```
