<p align="center">
  <img src="assets/frogdb-logo-transparent.png" alt="FrogDB" width="280" />
</p>

<h3 align="center">A Redis 8.0-compatible, memory-first database built in Rust.</h3>

<p align="center">
  <img alt="Status: Pre-production" src="https://img.shields.io/badge/status-pre--production-orange" />
  <img alt="Language: Rust" src="https://img.shields.io/badge/language-Rust-dea584" />
  <img alt="Protocol: RESP2/RESP3" src="https://img.shields.io/badge/protocol-RESP2%20%2F%20RESP3-blue" />
  <img alt="License: BSL-1.1 / AGPLv3 / Commercial" src="https://img.shields.io/badge/license-BSL--1.1%20%7C%20AGPLv3%20%7C%20Commercial-green" />
</p>

---

## What is FrogDB?

FrogDB is a sharded, multi-threaded, memory-first database built on Tokio. It speaks the Redis wire
protocol (RESP2 and RESP3) so you can use it with any existing Redis client. FrogDB aims to be
faster, more correct, and easier to operate than existing solutions — while supporting the full
breadth of Redis 8.0 data structures and capabilities.

## Goals

- **Fast** 
- **Correct** 
- **Scalable**
- **Easy to operate**

## Features

### Redis 8.0 Compatibility

Full RESP2/RESP3 wire protocol support with coverage across Redis data structures:

- **Core types** — Strings, Lists, Sets, Sorted Sets, Hashes, Streams
- **Bitmaps & Bitfields** — BITCOUNT, BITOP, BITPOS, BITFIELD
- **JSON** — RedisJSON-compatible document storage with JSONPath
- **Time Series** — Gorilla-compressed time series with aggregation and downsampling
- **Vector Sets** — Approximate nearest-neighbor search (HNSW via USearch)
- **Probabilistic** — Bloom filters, Cuckoo filters, HyperLogLog, Count-Min Sketch, Top-K, T-Digest
- **Geospatial** — Geohash indexing and distance queries (built on Sorted Sets)
- **Pub/Sub** — Channel and pattern-based publish/subscribe, event sourcing
- **Scripting & Transactions** — Lua scripting, MULTI/EXEC

### Clustering & Replication

- Raft-based consensus for cluster coordination
- Leader-follower replication
- Cross-slot operations in single-node mode
- Automatic cluster rebalancing (TBD)

### Persistence

- RocksDB-backed durable storage
- Configurable durability modes balancing performance and safety

### Operations

- Online configuration changes — no restart required
- Zero-downtime rolling upgrades
- Prometheus metrics and Grafana dashboard templates
- OpenTelemetry metrics, tracing, and logging
- HTTP debug pages with hot key tracking and backpressure stats
- DTrace probes
- _TODO_: Kubernetes support
- _TODO_: Terraform/CDK constructs

### Testing

- [Shuttle](https://github.com/awslabs/shuttle) and [Turmoil](https://github.com/tokio-rs/turmoil)
  deterministic concurrency testing
- Redis regression compatibility suite
- Load testing and benchmarking harness
- Fuzz testing (cargo-fuzz, integration fuzz targets)
- Jepsen verification using Knossos (linearizability) and Elle (serializability)

### Performance

- Custom causal profiler integration
- Comparative benchmarking against Redis, Valkey, and Dragonfly
- Per-shard metrics and hot key detection

## Quick Start

### Prerequisites

- **Rust 1.75+** (2024 edition)
- [just](https://github.com/casey/just) — command runner
- [cargo-nextest](https://nexte.st/) — test runner
- [LLVM/libclang](https://llvm.org/) — required by RocksDB bindings

On macOS: `brew bundle` installs everything from the Brewfile.

### Build & Run

```bash
just build            # debug build
just run              # start server on 127.0.0.1:6379
```

### Connect

```bash
redis-cli PING        # PONG
redis-cli SET hello world
redis-cli GET hello   # "world"
```

## Documentation

| Audience     | Path                                     | Description                                                     |
| ------------ | ---------------------------------------- | --------------------------------------------------------------- |
| Users        | [docs/users/](docs/users/)               | Commands, scripting, pub/sub, event sourcing, transactions      |
| Operators    | [docs/operators/](docs/operators/)       | Configuration, deployment, persistence, replication, monitoring |
| Contributors | [docs/contributors/](docs/contributors/) | Architecture, concurrency model, storage engine, VLL            |

## Contributing

Contributions require a signed Contributor License Agreement (CLA). See the [contributor
documentation](docs/contributors/) for architecture guides and development setup.

## License

FrogDB is tri-licensed. Choose whichever fits your use case:

| License | Best for |
|---------|----------|
| **BSL-1.1** | Most users — use freely, with a restriction on competing database products. Converts to Apache 2.0 after 2 years. |
| **AGPLv3** | Users who need an OSI-approved copyleft license. |
| **Commercial** | Organizations needing custom terms. |

See [LICENSE.md](LICENSE.md) for full details.
