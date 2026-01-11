# FrogDB Design Document

A Redis-compatible, high-performance in-memory database written in Rust.

## Overview

FrogDB is designed to be a fast, memory-safe alternative to Redis, leveraging Rust's ownership model to provide thread-safety without the overhead of garbage collection. The primary use cases are high-throughput caching, session storage, and fast operations on data structures.

### Goals

1. **Redis compatibility** - Support RESP2/RESP3 protocols and Redis commands for drop-in replacement
2. **High performance** - Thread-per-core architecture with shared-nothing design
3. **Memory safety** - Leverage Rust's guarantees to prevent data races and memory corruption
4. **Durability** - Configurable persistence with RocksDB backend
5. **Extensibility** - Clean abstractions for adding data types, storage backends, and protocols
6. **Correctness** - Eventually pass Jepsen distributed systems tests

### Non-Goals (Initial)

- Full Redis API compatibility from day one (gradual adoption)
- Clustering (single-node first, see [CLUSTER.md](CLUSTER.md) for design)
- RESP3 (RESP2 first with abstraction layer)
- Blocking commands (BLPOP, BRPOP, BLMOVE) in initial phases

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FrogDB Server                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                                │
│  │  Acceptor   │  (Single thread, accepts connections)          │
│  └──────┬──────┘                                                │
│         │ Distributes connections via round-robin               │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Shard Workers                         │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │   │
│  │  │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard N │        │   │
│  │  │  Data   │ │  Data   │ │  Data   │ │  Data   │        │   │
│  │  │  Lua VM │ │  Lua VM │ │  Lua VM │ │  Lua VM │        │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘        │   │
│  │       └───────────┴─────┬─────┴───────────┘              │   │
│  │                         │ Message Passing                 │   │
│  └─────────────────────────┼───────────────────────────────┘   │
│                            ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  Persistence Layer                       │   │
│  │  RocksDB Engine  │  Snapshot Manager  │  WAL Writer      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Responsibility |
|-----------|----------------|
| **Acceptor** | Accept TCP connections, assign to threads via round-robin |
| **Shard Worker** | Own a partition of data, execute commands, manage connections |
| **Data Store** | In-memory key-value storage. See [STORAGE.md](STORAGE.md) |
| **Lua VM** | Execute Lua scripts atomically within shard |
| **Persistence Layer** | WAL writes, snapshot management, recovery |

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Shared-nothing threading | Avoids lock contention, scales linearly with cores |
| Message-passing between threads | Clean coordination for scatter-gather |
| Pinned connections (Dragonfly-style) | Simple lifecycle, no migration complexity |
| Single RocksDB with column families | Simpler backup/restore, shared WAL |
| Forkless snapshots | Avoids fork() memory spike (2x worst case) |
| VLL-style transaction ordering | Atomic multi-shard operations via global txid counter |
| Strict Lua key validation | DragonflyDB-style; optional compatibility flag |

### Key Tradeoffs

**Shared-Nothing vs Shared-State:** We chose shared-nothing with message passing. This avoids lock contention on hot paths and enables linear scalability with cores. The tradeoff is that cross-shard operations require coordination via scatter-gather. Hash tags mitigate this by colocating related keys.

**In-Memory vs Disk-Based:** We chose in-memory primary with RocksDB for durability. This provides microsecond latencies while RocksDB handles persistence complexity. The tradeoff is that data must fit in RAM.

---

## Concurrency Model

FrogDB uses a shared-nothing, thread-per-core architecture inspired by DragonflyDB. Connections are pinned to threads for their lifetime but can coordinate with any shard via message-passing. Keys are hashed to determine shard ownership, with hash tags supporting key colocation.

See [CONCURRENCY.md](CONCURRENCY.md) for thread architecture, VLL transaction ordering, and scatter-gather implementation.

### Hash Tag Colocation

Hash tags (e.g., `{user:1}:profile`) guarantee that related keys land on the **same internal shard**, enabling atomic transactions and Lua scripts across tagged keys.

---

## Data Structures

FrogDB supports multiple Redis-compatible data types including Strings, Sorted Sets, and planned support for Hashes, Lists, Sets, and Streams. Each type has optimized internal representations.

See [STORAGE.md](STORAGE.md) for the FrogValue enum and [types/](types/) for data structure implementations and commands.

---

## Limits

FrogDB enforces Redis-compatible size limits: 512 MB max key/value size, 2^32-1 max collection elements, and 65,536 max internal shards per node (for cursor encoding).

See [LIMITS.md](LIMITS.md) for complete limit documentation and configuration.

---

## Key Expiry (TTL)

FrogDB uses a **hybrid expiration model** matching Redis/Valkey behavior: lazy expiration on every key access, plus an active background task (~10Hz per shard) that samples and deletes expired keys within a time budget.

See [STORAGE.md](STORAGE.md) for expiry index implementation.

---

## Memory Management

When configured memory limit (`max_memory`) is reached, write operations return `-OOM` errors while reads continue normally. Planned eviction policies include LRU, LFU, and volatile variants.

See [EVICTION.md](EVICTION.md) for eviction policy implementation.

---

## Persistence

FrogDB uses a single shared RocksDB instance with one column family per shard. All writes append to the WAL with configurable durability modes (async/periodic/sync). Forkless snapshots avoid the 2x memory spike of fork-based approaches.

See [PERSISTENCE.md](PERSISTENCE.md) for RocksDB topology, WAL, snapshots, and backup/restore procedures.

---

## Protocol

FrogDB implements RESP2 (Redis Serialization Protocol version 2) using the `redis-protocol` crate. A `Protocol` trait abstraction allows RESP3 to be added later without changing command implementations.

See [PROTOCOL.md](PROTOCOL.md) for frame processing and Response type details.

---

## Connection Management

Each connection maintains state for transactions, pub/sub, authentication, and blocking operations. Connections are pinned to threads with configurable limits, timeouts, and output buffer controls.

See [CONNECTION.md](CONNECTION.md) for lifecycle, state machine, and rate limiting.

---

## Command Execution

Commands flow through protocol parsing, key routing, shard dispatch, execution, persistence, and response encoding. Each command implements the `Command` trait with arity validation and behavior flags.

See [EXECUTION.md](EXECUTION.md) for command flow and trait definition, [COMMANDS.md](COMMANDS.md) for command index.

---

## Transactions & Pipelining

FrogDB supports MULTI/EXEC transactions with WATCH for optimistic locking. All keys in a transaction must be on the same shard (use hash tags). Pipelining is a separate client-side optimization that batches commands without atomicity guarantees.

See [TRANSACTIONS.md](TRANSACTIONS.md) for implementation details.

---

## Key Iteration (SCAN)

SCAN provides cursor-based iteration without blocking. The 64-bit cursor encodes shard ID (16 bits) and position (48 bits), making iteration stateless and resumable.

See [types/GENERIC.md](types/GENERIC.md) for SCAN algorithm details.

---

## Lua Scripting

Lua scripts execute atomically within a single shard, blocking that shard's event loop. Scripts must declare all keys in the KEYS array (DragonflyDB-style strict validation). Cross-shard scripts require hash tags for colocation.

See [SCRIPTING.md](SCRIPTING.md) for execution model and resource limits.

---

## Pub/Sub

FrogDB supports broadcast (SUBSCRIBE/PUBLISH) and sharded (SSUBSCRIBE/SPUBLISH) pub/sub modes with a unified per-shard architecture. Broadcast fans out to all shards; sharded routes to the channel's owner shard.

See [PUBSUB.md](PUBSUB.md) for architecture and commands.

---

## Blocking Commands

Blocking commands (BLPOP, BRPOP, BLMOVE, etc.) are a future feature. They require special handling in the shared-nothing architecture, with keys constrained to the same shard via hash tags.

See [BLOCKING.md](BLOCKING.md) for design considerations.

---

## Testing Strategy

Testing follows an integration-first approach using real Redis clients against a test server. Categories include protocol tests, command tests, concurrency tests, persistence tests, and stress tests.

See [TESTING.md](TESTING.md) for test categories and CI configuration.

---

## Observability

FrogDB provides Prometheus-compatible metrics (connections, memory, commands, keyspace), OpenTelemetry tracing (span per command), and structured logging. Debug commands include SLOWLOG, DEBUG OBJECT, and INFO.

See [OBSERVABILITY.md](OBSERVABILITY.md) for metrics reference and [OPERATIONS.md](OPERATIONS.md) for configuration.

---

## Security

FrogDB implements Redis-compatible ACL (Access Control Lists) with an abstracted checker interface. ACL checks occur at command permission, key access, and pub/sub channel hook points.

See [AUTH.md](AUTH.md) for ACL architecture and commands.

---

## Clustering

FrogDB is designed for single-node operation initially, with abstractions for future clustering. The design uses an orchestrated control plane (no gossip), 16384 hash slots (Redis Cluster compatible), and RocksDB WAL streaming for replication.

See [CLUSTER.md](CLUSTER.md) for full clustering architecture.

---

## Server Lifecycle

FrogDB follows a structured startup and shutdown sequence including RocksDB recovery, shard initialization, and graceful connection draining.

See [LIFECYCLE.md](LIFECYCLE.md) for startup/shutdown procedures and health checks.

---

## Consistency Model

FrogDB provides **eventual consistency** within a single node: read-your-writes, monotonic reads, and total order per key. Cross-key atomicity requires same-shard colocation. Durability depends on the configured mode.

See [CONSISTENCY.md](CONSISTENCY.md) for detailed guarantees.

---

## Crate Structure

```
frogdb/
├── frogdb-server/        # Main server binary
├── frogdb-core/          # Core data structures & logic
├── frogdb-protocol/      # RESP protocol handling
├── frogdb-lua/           # Lua scripting support
├── frogdb-persistence/   # Persistence layer
└── tests/                # Integration tests
```

---

## Configuration

FrogDB uses a layered configuration approach:

1. **Startup configuration** via [Figment](https://docs.rs/figment): CLI > environment variables (`FROGDB_` prefix) > TOML file > defaults
2. **Runtime configuration** via Redis-compatible `CONFIG SET/GET` commands
3. **TLS certificate hot-reloading** via file watching (future feature)

**Key design decisions:**
- TOML format (modern, structured) rather than redis.conf
- Native env var support (like DragonflyDB, unlike Redis/Valkey)
- No `CONFIG REWRITE` - runtime changes are transient
- Parameters classified as mutable or immutable

See [CONFIGURATION.md](CONFIGURATION.md) for full configuration system design, [OPERATIONS.md](OPERATIONS.md) for operational guide, and [DEPLOYMENT.md](DEPLOYMENT.md) for Docker/K8s deployment.

---

## Roadmap

| Phase | Focus |
|-------|-------|
| 0 | Design documents (current) |
| 1 | Foundation: RESP2, single-threaded, Strings |
| 2 | Multi-threading: shards, message passing, scatter-gather |
| 3 | Sorted Sets |
| 4 | Persistence: RocksDB, WAL, snapshots |
| 5 | Lua Scripting |
| 6 | Production: metrics, tracing, config |
| 7 | Advanced: eviction, ACL, more types, transactions |
| Future | RESP3, clustering, Jepsen tests |

---

## References

### Prior Art

- [Redis](https://redis.io/) - The original in-memory data store
- [DragonflyDB](https://dragonflydb.io/) - Modern Redis alternative with shared-nothing architecture
- [Valkey](https://valkey.io/) - Redis fork by Linux Foundation
- [KeyDB](https://keydb.dev/) - Multi-threaded Redis fork

### Rust Ecosystem

- [Tokio](https://tokio.rs/) - Async runtime
- [RocksDB Rust](https://github.com/rust-rocksdb/rust-rocksdb) - RocksDB bindings
- [mlua](https://github.com/khvzak/mlua) - Lua bindings for Rust
- [bytes](https://docs.rs/bytes/) - Zero-copy byte handling

---

## Document Index

| Document | Description |
|----------|-------------|
| [CONCURRENCY.md](CONCURRENCY.md) | Thread architecture, VLL, scatter-gather |
| [STORAGE.md](STORAGE.md) | Store trait, key metadata, expiry |
| [PERSISTENCE.md](PERSISTENCE.md) | RocksDB, WAL, snapshots, backup/restore |
| [PROTOCOL.md](PROTOCOL.md) | RESP2/RESP3, frame processing |
| [CONNECTION.md](CONNECTION.md) | Connection lifecycle, rate limiting |
| [COMMANDS.md](COMMANDS.md) | Command index |
| [EXECUTION.md](EXECUTION.md) | Command trait, arity, flags, type checking |
| [types/](types/) | Data type implementations and commands |
| [DEBUGGING.md](DEBUGGING.md) | Debugging tools, dtrace/eBPF, developer debugging |
| [LIMITS.md](LIMITS.md) | Size limits and enforcement |
| [TRANSACTIONS.md](TRANSACTIONS.md) | MULTI/EXEC, WATCH, pipelining |
| [BLOCKING.md](BLOCKING.md) | Blocking commands design |
| [SCRIPTING.md](SCRIPTING.md) | Lua execution model |
| [PUBSUB.md](PUBSUB.md) | Pub/sub architecture |
| [CLUSTER.md](CLUSTER.md) | Clustering design |
| [AUTH.md](AUTH.md) | ACL system |
| [EVICTION.md](EVICTION.md) | Memory eviction policies |
| [LIFECYCLE.md](LIFECYCLE.md) | Startup/shutdown |
| [TESTING.md](TESTING.md) | Test strategy |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Symptom-driven diagnosis runbooks |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Metrics, logging, tracing |
| [CONFIGURATION.md](CONFIGURATION.md) | Configuration system, CONFIG commands, TLS hot-reload |
| [OPERATIONS.md](OPERATIONS.md) | Operational guide, debugging |
| [FAILURE_MODES.md](FAILURE_MODES.md) | Error handling, recovery |
| [CONSISTENCY.md](CONSISTENCY.md) | Consistency guarantees |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Docker, K8s deployment |
