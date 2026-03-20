<p align="center">
  <img src="assets/frogdb-logo-transparent.png" alt="FrogDB" width="280" />
</p>

<h3 align="center">A Redis 8.0-compatible, memory-first database built in Rust.</h3>

---

## What is FrogDB?

FrogDB is a multi-threaded, memory-first database built in Rust with Tokio as the async engine (more
on this below). It is fully Redis wire protocol (RESP2 and RESP3) compatible so you can use it with
any existing Redis client. FrogDB aims to be faster, safer, and easier to operate than existing
solutions while supporting the full Redis 8 feature set.

## Goals

- **Correct** 
- **Fast** 
- **Scalable**
- **Easy to operate**

## Features

### Redis 8 Compatibility

Full RESP2/RESP3 wire protocol support with coverage across all Redis data structures:

- **Core types** — Strings, Lists, Sets, Sorted Sets, Hashes, Streams
- **Bitmaps & Bitfields** — BITCOUNT, BITOP, BITPOS, BITFIELD
- **JSON** — RedisJSON-compatible document storage with JSONPath
- **Time Series** — Gorilla-compressed time series with aggregation and downsampling
- **Vector Sets** — Approximate nearest-neighbor search
- **Probabilistic** — Bloom filters, Cuckoo filters, HyperLogLog, Count-Min Sketch, Top-K, T-Digest
- **Geospatial** — Geohash indexing and distance queries
- **Pub/Sub** — Channel and pattern-based publish/subscribe, event sourcing
- **Scripting & Transactions** — Lua scripting, MULTI/EXEC
- **Search** — Full text search, secondary indexing, vector search, aggregations, JSON documents

### Clustering & Replication

- Supports cluster operation with keyspace sharding
- Raft-based consensus for cluster state coordination
- Read replicas supported 
- _TODO_: Automatic cluster rebalancing

### Additional Features

- Cross-slot operations allowed in single-node operation
  - MULTI/EXEC/MGET/etc
- Event sourcing
- TBD

### Persistence

- WAL using RocksDB for storage and replication
- Configurable durability modes (write through or async)

### Operations

- Online configuration changes for many values
- _WIP_ Zero-downtime rolling upgrades
- Prometheus metrics/alarms
- Grafana dashboard templates
- OpenTelemetry metrics, tracing, and logging
- HTTP debug pages
  - JSON API
  - Monitoring UI
  - Configuration
- DTrace probes
- _WIP_: Kubernetes support
- _TODO_: Terraform/CDK constructs
- Tons of stats/logs and debugging information (all configurable)

### Testing

- [Shuttle](https://github.com/awslabs/shuttle) and [Turmoil](https://github.com/tokio-rs/turmoil)
  deterministic concurrency testing
- Redis regression compatibility suite
- Load testing and benchmarking
- Fuzz testing
- Jepsen verification using both Knossos (linearizability) and Elle (serializability)

### Performance

- Profiling with custom causal profiler support
- _WIP_: Comparative benchmarking against Redis, Valkey, and Dragonfly
- _TODO_: io_uring/other runtimes like compio for faster I/O

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

It should work seamlessly with your standard Redis clients, including cluster commands.

```bash
redis-cli PING        # PONG
redis-cli SET hello world
redis-cli GET hello   # "world"
```

## Documentation

Documentation is currently WIP (a lot of AI slop at the moment but will be improving).

| Audience     | Path                                     | Description                                                     |
| ------------ | ---------------------------------------- | --------------------------------------------------------------- |
| Users        | [docs/users/](docs/users/)               | Commands, scripting, pub/sub, event sourcing, transactions      |
| Operators    | [docs/operators/](docs/operators/)       | Configuration, deployment, persistence, replication, monitoring |
| Contributors | [docs/contributors/](docs/contributors/) | Architecture, concurrency model, storage engine, VLL            |

## Contributing

Contributions require a signed Contributor License Agreement (CLA). See the [contributor
documentation](docs/contributors/) for architecture guides and development setup.

## License

FrogDB is tri-licensed.

Choose whichever fits your use case:

| License        | Best for                                                                                                          |
| -------------- | ----------------------------------------------------------------------------------------------------------------- |
| **BSL-1.1**    | Most users — use freely, with a restriction on competing database products. Converts to Apache 2.0 after 2 years. |
| **AGPLv3**     | Users who need an OSI-approved copyleft license.                                                                  |
| **Commercial** | Organizations needing custom terms.                                                                               |

See [LICENSE.md](LICENSE.md) for full details.
