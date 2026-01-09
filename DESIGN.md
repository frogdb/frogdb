# FrogDB Design Document

A Redis-compatible, high-performance in-memory database written in Rust.

## Overview

FrogDB is designed to be a fast, memory-safe alternative to Redis, leveraging Rust's ownership model to provide thread-safety without the overhead of garbage collection. The primary use cases are:

- High-throughput caching
- Session storage
- Fast operations on data structures

### Goals

1. **Redis compatibility** - Support RESP2/RESP3 protocols and Redis commands for drop-in replacement
2. **High performance** - Thread-per-core architecture with shared-nothing design
3. **Memory safety** - Leverage Rust's guarantees to prevent data races and memory corruption
4. **Durability** - Configurable persistence with RocksDB backend
5. **Extensibility** - Clean abstractions for adding data types, storage backends, and protocols
6. **Correctness** - Eventually pass Jepsen distributed systems tests

### Non-Goals (Initial)

- Full Redis API compatibility from day one (gradual adoption)
- Clustering (single-node first, see [docs/CLUSTER.md](docs/CLUSTER.md) for design)
- RESP3 (RESP2 first with abstraction layer)
- Blocking commands (BLPOP, BRPOP, BLMOVE) in initial phases (defer to later)

---

## Architecture

### High-Level System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FrogDB Server                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                                │
│  │  Acceptor   │  (Single thread, accepts connections)          │
│  └──────┬──────┘                                                │
│         │ Distributes connections by consistent hash            │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Shard Workers                         │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │   │
│  │  │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard N │        │   │
│  │  │         │ │         │ │         │ │         │        │   │
│  │  │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │        │   │
│  │  │ │Data │ │ │ │Data │ │ │ │Data │ │ │ │Data │ │        │   │
│  │  │ │Store│ │ │ │Store│ │ │ │Store│ │ │ │Store│ │        │   │
│  │  │ └─────┘ │ │ └─────┘ │ │ └─────┘ │ │ └─────┘ │        │   │
│  │  │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │        │   │
│  │  │ │ Lua │ │ │ │ Lua │ │ │ │ Lua │ │ │ │ Lua │ │        │   │
│  │  │ │ VM  │ │ │ │ VM  │ │ │ │ VM  │ │ │ │ VM  │ │        │   │
│  │  │ └─────┘ │ │ └─────┘ │ │ └─────┘ │ │ └─────┘ │        │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘        │   │
│  │       │           │           │           │              │   │
│  │       └───────────┴─────┬─────┴───────────┘              │   │
│  │                         │ Message Passing (mpsc channels) │   │
│  └─────────────────────────┼───────────────────────────────┘   │
│                            │                                    │
│  ┌─────────────────────────▼───────────────────────────────┐   │
│  │                  Persistence Layer                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │   │
│  │  │   RocksDB    │  │   Snapshot   │  │     WAL      │   │   │
│  │  │   Engine     │  │   Manager    │  │   Writer     │   │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Acceptor** | Accept TCP connections, assign to threads via round-robin (ConnectionAssigner abstraction) |
| **Shard Worker** | Own a partition of data, execute commands, manage connections |
| **Data Store** | In-memory key-value storage (HashMap-based). See [docs/STORAGE.md](docs/STORAGE.md) |
| **Lua VM** | Execute Lua scripts atomically within shard |
| **Persistence Layer** | WAL writes, snapshot management, recovery |

---

## Design Decisions

### Decision Log

| Decision | Rationale | Alternatives Considered |
|----------|-----------|------------------------|
| Shared-nothing threading | Avoids lock contention, scales linearly with cores, proven by Dragonfly | Lock-based sharding, Actor model |
| Message-passing between threads | Clean coordination for scatter-gather, easier to reason about | Shared memory with atomics |
| Pinned connections (Dragonfly-style) | Simple lifecycle, no migration complexity | Key-owner handles connection |
| Single RocksDB with column families | Simpler backup/restore, shared WAL | Separate RocksDB per shard |
| In-memory + RocksDB persistence | Fast hot path, durable restarts | Pure in-memory, custom engine |
| Forkless snapshots | Avoids fork() memory spike (2x worst case) | Fork + COW (Redis-style) |
| RESP2 first | Simpler, wider client compatibility | RESP3 first |
| Scatter-gather fail-all + timeout | Predictable semantics, configurable timeout | Partial results with errors |
| Hybrid key expiry (lazy + active) | Matches Redis/Valkey, balances memory and CPU | Lazy only, active only |
| OOM rejects writes | Explicit, predictable behavior | LRU eviction |
| Block-shard Lua execution | Redis-compatible atomicity | Async scripts on snapshot |
| Tokio async runtime | Industry standard, excellent ecosystem | async-std, Glommio |
| Eventual consistency model | Appropriate for caching use cases | Strong consistency |
| Figment for configuration | Hierarchical merging (CLI > env > file), TOML support, derive macro | config-rs, manual parsing |
| Cursor-encodes-shard for SCAN | Stateless, Redis-compatible, transparent cross-shard iteration | Per-shard cursors, server-side state |
| Defer eviction policies | OOM-reject simpler for v1, add LRU/LFU/LFRU later | Implement eviction immediately |
| Full observability from start | Production-ready, OpenTelemetry standard, Prometheus metrics | Add observability later |
| Hybrid documentation | DESIGN.md overview + detailed spec files in docs/ | Single large file |
| Round-robin connection assignment | Simple load balancing, proven approach (DragonflyDB), swappable via ConnectionAssigner trait | Consistent hash, Least-connections |
| xxhash64 for internal key sharding | Fast, good distribution, separate from cluster CRC16, swappable via KeyHasher trait | CRC16, FNV-1a |

### Key Tradeoffs

#### 1. Shared-Nothing vs Shared-State

**Chosen: Shared-nothing with message passing**

*Pros:*
- No lock contention on hot paths
- Linear scalability with cores
- Each thread can be pinned to a CPU core
- Simpler reasoning about data ownership

*Cons:*
- Cross-shard operations require coordination
- Memory overhead from per-shard data structures
- Scatter-gather latency for multi-key operations

*Mitigation:*
- Efficient message passing via Tokio mpsc channels
- Support for hash tags to colocate related keys

#### 2. In-Memory vs Disk-Based

**Chosen: In-memory primary with RocksDB for durability**

*Pros:*
- Microsecond latencies for all operations
- Simple data structure implementations
- RocksDB handles persistence complexity

*Cons:*
- Data must fit in RAM
- Crash can lose unflushed writes (configurable)

*Mitigation:*
- Configurable durability modes (async/periodic/sync)
- Clear documentation of durability guarantees

#### 3. Redis Compatibility vs Innovation

**Chosen: Compatibility first, innovate where beneficial**

We prioritize Redis protocol and command compatibility for easy adoption, but diverge where we can provide better guarantees:
- Forkless snapshots (no memory spike)
- Shared-nothing multi-threading (vs Redis's single-threaded model)
- Better eventual consistency documentation

---

## Concurrency Model

FrogDB uses a shared-nothing, thread-per-core architecture inspired by DragonflyDB.
Connections are pinned to threads for their lifetime but can coordinate with any shard
via message-passing. Keys are hashed to determine shard ownership, with hash tags
supporting key colocation.

See [docs/CONCURRENCY.md](docs/CONCURRENCY.md) for thread architecture, connection model,
and scatter-gather implementation details.

---

## Data Structures

### Value Types

```rust
pub enum FrogValue {
    String(FrogString),
    SortedSet(FrogSortedSet),
    // Future: List, Hash, Set, Stream, etc.
}
```

### String Type

Simple byte string with optional metadata:

```rust
pub struct FrogString {
    data: Bytes,
}
```

### Sorted Set Type

Dual-indexed structure for O(log n) operations by score and member:

```rust
pub struct FrogSortedSet {
    // Member -> Score mapping for O(1) score lookup
    members: HashMap<Bytes, f64>,
    // Score-ordered structure for range queries
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}
```

Alternative: Skip list implementation for cache-friendlier traversal.

### All Supported Data Types

| Type | Implementation | Phase | Status |
|------|---------------|-------|--------|
| String | `Bytes` | 1 | Core |
| Hash | `HashMap<Bytes, Bytes>` | 3+ | Planned |
| List | `VecDeque<Bytes>` | 3+ | Planned |
| Set | `HashSet<Bytes>` | 3+ | Planned |
| Sorted Set | `HashMap` + `BTreeMap` | 3 | Core |
| Stream | Radix tree + listpack | Future | Planned |
| Bitmap | Operations on String | Future | Planned |
| Bitfield | Operations on String | Future | Planned |
| Geospatial | Sorted Set + geohash | Future | Planned |
| JSON | `serde_json::Value` | Future | Planned |
| HyperLogLog | 12KB fixed structure | Future | Planned |
| Bloom Filter | Bit array + hashes | Future | Planned |
| Time Series | Sorted by timestamp | Future | Planned |

See [docs/COMMANDS.md](docs/COMMANDS.md) for command reference and [docs/command-groups/](docs/command-groups/) for detailed type implementations.

---

## Key Expiry (TTL)

FrogDB uses a **hybrid expiration model** matching Redis/Valkey behavior:

### Expiration Strategies

1. **Lazy expiration**: On every key access, check if expired. Delete immediately if so.
2. **Active expiration**: Background task runs ~10Hz per shard, samples keys with TTL, deletes expired ones within time budget.

```
┌─────────────────────────────────────────────────────────┐
│                  Per-Shard Expiry                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Access Path (Lazy):                                    │
│    GET key → check expiry → if expired: delete, nil    │
│                                                         │
│  Background Task (Active, ~10Hz):                       │
│    1. Sample N keys from expiry index                  │
│    2. Delete expired ones                              │
│    3. Stop when time budget (~1ms) exhausted           │
│    4. If >25% expired, continue next cycle             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Data Structure

Per-shard expiry index for efficient sampling:

```rust
pub struct ExpiryIndex {
    /// Keys with expiry, sorted by expiration time
    by_time: BTreeMap<(Instant, Bytes), ()>,
    /// Quick lookup: key -> expiration time
    by_key: HashMap<Bytes, Instant>,
}
```

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `active_expiry_hz` | 10 | Background sweep frequency per shard |
| `active_expiry_cycle_ms` | 1 | Max time budget per sweep cycle |
| `active_expiry_sample_size` | 20 | Keys sampled per cycle |

---

## Memory Management

### Memory Limit Behavior

When configured memory limit (`max_memory`) is reached:

| Operation | Behavior |
|-----------|----------|
| Write (SET, ZADD, etc.) | Return `-OOM out of memory` error |
| Read (GET, ZRANGE, etc.) | Continue normally |
| Delete (DEL, EXPIRE) | Continue normally |
| Expiry background task | Continues to reclaim memory |

**Rationale:** Explicit OOM errors are predictable. Users must manage capacity or enable expiry policies.

### Key Eviction Policies (Future)

Planned Redis-compatible eviction policies:

| Policy | Scope | Description |
|--------|-------|-------------|
| noeviction | - | Return OOM error (current default) |
| volatile-lru | Keys with TTL | Evict least recently used |
| allkeys-lru | All keys | Evict least recently used |
| volatile-lfu | Keys with TTL | Evict least frequently used |
| allkeys-lfu | All keys | Evict least frequently used |
| volatile-random | Keys with TTL | Evict random keys |
| allkeys-random | All keys | Evict random keys |
| volatile-ttl | Keys with TTL | Evict keys with shortest TTL |

**DragonflyDB LFRU:** Consider hybrid LFU+LRU with zero per-key overhead.

See [docs/EVICTION.md](docs/EVICTION.md) for eviction implementation details.

---

## Persistence

FrogDB uses a single shared RocksDB instance with one column family per shard.
All writes append to the WAL with configurable durability modes (async/periodic/sync).
Forkless snapshots avoid the 2x memory spike of fork-based approaches.

See [docs/PERSISTENCE.md](docs/PERSISTENCE.md) for RocksDB topology, WAL implementation,
and snapshot algorithm details.

---

## Protocol

### RESP2 Implementation

RESP2 is the Redis Serialization Protocol version 2:

```
Simple String: +OK\r\n
Error:         -ERR message\r\n
Integer:       :1000\r\n
Bulk String:   $5\r\nhello\r\n
Array:         *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
Null:          $-1\r\n
```

FrogDB uses the [`redis-protocol`](https://crates.io/crates/redis-protocol) crate for parsing and encoding.
See [docs/PROTOCOL.md](docs/PROTOCOL.md) for integration details.

### Protocol Abstraction

```rust
pub trait Protocol: Send + Sync {
    type Frame: ProtocolFrame;

    fn parse(&self, buf: &mut BytesMut) -> Result<Option<Self::Frame>, ProtocolError>;
    fn encode(&self, response: &Response) -> BytesMut;
}
```

This abstraction allows RESP3 to be added later without changing command implementations.

### Connection State

Each connection maintains state for transactions, pub/sub, authentication, and blocking operations.
The ConnectionState struct is owned by the connection handler and tracks the current mode
(normal, transaction, pub/sub, blocked) along with associated data.

See [docs/CONNECTION.md](docs/CONNECTION.md) for connection lifecycle, state machine,
ConnectionAssigner abstraction, and client commands.

---

## Command Execution

Commands flow through protocol parsing, key routing, shard dispatch, execution,
persistence, and response encoding. Each command implements the `Command` trait
with arity validation and behavior flags (WRITE, READONLY, MULTI_KEY, etc.).

See [docs/EXECUTION.md](docs/EXECUTION.md) for command flow, trait definition, and flag documentation.

---

## Transactions & Pipelining

### Pipelining

Client-side optimization - batch commands in single network round-trip:
- No server state required
- Commands may interleave with other clients
- NOT atomic - purely a performance optimization
- Supported automatically by RESP protocol

### Transactions (MULTI/EXEC)

Atomic command execution with queuing:

1. **MULTI**: Start transaction, server enters queuing mode
2. **Commands**: Queued, server responds QUEUED
3. **EXEC**: Execute all queued commands atomically
4. **DISCARD**: Abort transaction, clear queue

**Key semantics:**
- Atomic: All commands execute without interleaving
- NOT rollback: If one command fails, others still execute
- Single-shard requirement: All keys must hash to same shard (use hash tags)

**Cross-shard transactions:** Rejected at EXEC time if keys span multiple shards.

### WATCH (Optimistic Locking)

- `WATCH key [key...]`: Monitor keys for changes
- If watched key modified before EXEC, transaction aborts (returns nil)
- `UNWATCH`: Clear all watches

See [docs/COMMANDS.md](docs/COMMANDS.md) for detailed transaction implementation.

---

## Key Iteration (SCAN/KEYS)

### KEYS Command

Pattern matching across all keys. **Warning:** Blocks entire keyspace - avoid in production.

### SCAN Command (Recommended)

Cursor-based iteration without blocking. Uses shard-aware cursor encoding:

```
┌─────────────────────────────────────────┐
│           64-bit Cursor                  │
├─────────────────┬───────────────────────┤
│   Shard ID      │  Position in Shard    │
│   (bits 48-63)  │  (bits 0-47)          │
└─────────────────┴───────────────────────┘
```

**Properties:**
- Stateless: No server-side cursor state
- Resumable: Can stop/resume anytime
- Eventual: May return duplicates, may miss keys added during scan

**Related:** SSCAN, HSCAN, ZSCAN for iterating data structure members.

See [docs/COMMANDS.md](docs/COMMANDS.md) for detailed SCAN algorithm.

---

## Lua Scripting

Lua scripts execute atomically within a single shard, blocking that shard's event loop
during execution. Cross-shard scripts require all keys to use hash tags for colocation.

See [docs/SCRIPTING.md](docs/SCRIPTING.md) for execution model and cross-shard requirements.

---

## Pub/Sub

FrogDB supports broadcast (SUBSCRIBE/PUBLISH) and sharded (SSUBSCRIBE/SPUBLISH) pub/sub
modes with a unified per-shard architecture. Broadcast fans out to all shards while
sharded routes to the channel's owner shard for higher throughput.

See [docs/PUBSUB.md](docs/PUBSUB.md) for architecture, mode comparison, and command details.

---

## Testing Strategy

### Integration-First Approach

```rust
#[tokio::test]
async fn test_set_get() {
    let server = TestServer::start().await;
    let client = redis::Client::open(server.url()).unwrap();
    let mut con = client.get_connection().unwrap();

    let _: () = con.set("mykey", "myvalue").unwrap();
    let val: String = con.get("mykey").unwrap();

    assert_eq!(val, "myvalue");
}
```

### Test Categories

1. **Protocol tests** - Verify RESP2 parsing/encoding
2. **Command tests** - Each command via redis-cli/client
3. **Concurrency tests** - Multi-client, multi-key operations
4. **Persistence tests** - Crash recovery, snapshot correctness
5. **Lua tests** - Script execution, atomicity
6. **Stress tests** - High throughput, memory pressure
7. **Future: Jepsen tests** - Distributed correctness

See [docs/TESTING.md](docs/TESTING.md) for how to run tests, coverage expectations, and CI configuration.

---

## Observability

### Metrics (Prometheus)

FrogDB exposes Prometheus-compatible metrics:
- Connection stats: `connected_clients`, `blocked_clients`
- Memory: `used_memory`, `peak_memory`, `fragmentation_ratio`
- Commands: `commands_processed`, latency percentiles (p50, p95, p99)
- Keyspace: `keys_total`, `hit_rate`, `miss_rate`, `expired_keys`
- Per-shard breakdown available

### Tracing (OpenTelemetry)

- Span per command execution
- Child spans for cross-shard operations
- Integration via `tracing` crate + OTLP exporter

### Logging

- Structured logging (JSON format for production)
- Configurable log levels: ERROR, WARN, INFO, DEBUG, TRACE

### Debug Commands

- `SLOWLOG`: Commands exceeding latency threshold
- `DEBUG OBJECT`: Inspect key internals
- `CLIENT LIST`: Connected client details
- `MEMORY DOCTOR`: Memory health analysis
- `INFO`: Comprehensive server statistics

See [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) for detailed logging, tracing, metrics, and debugging documentation.
See [docs/OPERATIONS.md](docs/OPERATIONS.md) for operational configuration.
See [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) for error handling, failure recovery, and client recommendations.

---

## Security

FrogDB implements Redis-compatible authentication and ACL (Access Control Lists).
The ACL system uses an abstracted checker interface that starts as "allow all" and
can be enabled for full Redis 6.0 ACL support, with a path to Redis 7.0 selectors.

Authentication state is per-connection with immutable permission snapshots. ACL checks
occur at three hook points: command permission, key access (read/write), and pub/sub channels.

See [docs/AUTH.md](docs/AUTH.md) for ACL architecture, traits, and command reference.
See [docs/OPERATIONS.md](docs/OPERATIONS.md) for security configuration.

---

## Clustering (Future)

FrogDB is designed for single-node operation initially, but includes abstractions for future clustering support.

### Architecture Overview

| Aspect | Design Choice |
|--------|---------------|
| Control plane | Orchestrated (DragonflyDB-style), no gossip |
| Hash slots | 16384 (Redis Cluster compatible) |
| Replication | Full dataset, RocksDB WAL streaming |
| Client protocol | MOVED/ASK redirections, CLUSTER commands |

### Key Concepts

- **Internal shards** (threads) are separate from **cluster slots** (distribution units)
- External orchestrator pushes topology to nodes via admin API
- Replicas copy full dataset for simpler failover
- Replication leverages existing RocksDB WAL infrastructure

### Abstractions Needed Now

These types and traits should be designed into single-node implementation:

- `NodeId`, `SlotId`, `SlotRange`, `ReplicationId` types
- `ClusterTopology` trait for slot→node mapping
- `ReplicationStream` abstraction over WAL tailing
- Admin API for topology updates

See [docs/CLUSTER.md](docs/CLUSTER.md) for full clustering architecture, replication protocol, failover, and slot migration design.

---

## Crate Structure

```
frogdb/
├── Cargo.toml                 # Workspace root
├── DESIGN.md                  # This document
│
├── frogdb-server/             # Main server binary
│   └── src/
│       ├── main.rs            # Entry point, CLI
│       ├── server.rs          # Server lifecycle
│       └── config.rs          # Configuration
│
├── frogdb-core/               # Core data structures & logic
│   └── src/
│       ├── shard/             # Shard worker, executor
│       ├── data/              # Value types (String, SortedSet)
│       ├── store/             # Storage trait + implementations
│       └── command/           # Command trait + implementations
│
├── frogdb-protocol/           # RESP protocol handling
│   └── src/
│       ├── trait.rs           # Protocol abstraction
│       └── resp2/             # RESP2 parser/encoder
│
├── frogdb-lua/                # Lua scripting support
│   └── src/
│       ├── vm.rs              # Lua VM wrapper
│       └── commands.rs        # redis.call() bindings
│
├── frogdb-persistence/        # Persistence layer
│   └── src/
│       ├── snapshot.rs        # Forkless snapshot
│       ├── wal.rs             # Write-ahead log
│       └── recovery.rs        # Startup recovery
│
└── tests/                     # Integration tests
    └── src/
        └── *.rs               # Test files
```

---

## Configuration

FrogDB uses [Figment](https://docs.rs/figment) for hierarchical configuration with priority:
1. Command-line arguments
2. Environment variables (prefix: `FROGDB_`)
3. Configuration file (TOML format)
4. Built-in defaults

See [docs/OPERATIONS.md](docs/OPERATIONS.md) for complete configuration guide and example TOML file.
See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for Docker, systemd, and Kubernetes deployment.

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `bind` | `127.0.0.1` | Address to bind to |
| `port` | `6379` | Port to listen on |
| `num_shards` | `num_cpus` | Number of shard workers (threads) |

### Memory Options

| Option | Default | Description |
|--------|---------|-------------|
| `max_memory` | `0` (unlimited) | Memory limit in bytes; 0 = no limit |

### Persistence Options

| Option | Default | Description |
|--------|---------|-------------|
| `durability_mode` | `periodic` | `async`, `periodic`, or `sync` |
| `periodic_sync_ms` | `100` | Sync interval for periodic mode |
| `periodic_sync_writes` | `1000` | Write count trigger for periodic mode |
| `snapshot_interval_s` | `3600` | Seconds between snapshots (0 = disabled) |
| `data_dir` | `./data` | Directory for RocksDB and snapshots |

### Timeout Options

| Option | Default | Description |
|--------|---------|-------------|
| `scatter_gather_timeout_ms` | `1000` | Timeout for multi-shard operations |
| `client_timeout_s` | `0` | Idle client timeout (0 = no timeout) |

### Expiry Options

| Option | Default | Description |
|--------|---------|-------------|
| `active_expiry_hz` | `10` | Background expiry sweep frequency |
| `active_expiry_cycle_ms` | `1` | Max time per sweep cycle |

---

## Roadmap

### Phase 0: Design Document (Current)
- [x] Create DESIGN.md
- [x] Document architecture decisions
- [x] Define abstractions and interfaces

### Phase 1: Foundation
- [ ] Project scaffolding (Cargo workspace)
- [ ] RESP2 protocol parser/encoder
- [ ] Single-threaded server
- [ ] In-memory store with String type
- [ ] Basic commands: PING, SET, GET, DEL, EXISTS, EXPIRE, TTL
- [ ] Integration tests with redis-cli

### Phase 2: Multi-Threading
- [ ] Shard worker implementation
- [ ] Message passing infrastructure
- [ ] Acceptor with connection distribution
- [ ] Key hashing and routing
- [ ] Scatter-gather for MGET/MSET

### Phase 3: Sorted Sets
- [ ] FrogSortedSet data structure
- [ ] ZADD, ZREM, ZSCORE, ZRANK, ZRANGE commands

### Phase 4: Persistence
- [ ] RocksDB integration
- [ ] WAL implementation
- [ ] Forkless snapshot algorithm
- [ ] Recovery on startup

### Phase 5: Lua Scripting
- [ ] Lua VM integration (mlua)
- [ ] EVAL, EVALSHA commands
- [ ] redis.call() bindings

### Phase 6: Production Readiness
- [ ] Prometheus metrics endpoint (/metrics)
- [ ] OpenTelemetry tracing integration
- [ ] Structured logging (JSON format)
- [ ] SLOWLOG command
- [ ] Figment configuration (TOML + env vars)
- [ ] Graceful shutdown
- [ ] Memory limits and OOM handling

### Phase 7: Advanced Features
- [ ] Key eviction policies (LRU/LFU)
- [ ] ACL implementation (Redis 6+ compatible)
- [ ] Additional data types (Hash, List, Set)
- [ ] Stream data type with consumer groups
- [ ] Transactions (MULTI/EXEC/WATCH)

### Future
- RESP3 protocol
- Bitmap, Geo, JSON, HyperLogLog types
- Clustering and replication (see [docs/CLUSTER.md](docs/CLUSTER.md))
- Jepsen testing

---

## References

### Prior Art

- [Redis](https://redis.io/) - The original in-memory data store
- [DragonflyDB](https://dragonflydb.io/) - Modern Redis alternative with shared-nothing architecture
- [Valkey](https://valkey.io/) - Redis fork by Linux Foundation
- [KeyDB](https://keydb.dev/) - Multi-threaded Redis fork

### Papers & Articles

- [Dragonfly Architecture](https://www.dragonflydb.io/blog/scaling-performance-redis-vs-dragonfly) - Shared-nothing design
- [Redis Persistence](https://redis.io/docs/management/persistence/) - RDB + AOF model
- [RESP Protocol](https://redis.io/docs/reference/protocol-spec/) - Redis serialization protocol

### Rust Ecosystem

- [Tokio](https://tokio.rs/) - Async runtime
- [RocksDB Rust](https://github.com/rust-rocksdb/rust-rocksdb) - RocksDB bindings
- [mlua](https://github.com/khvzak/mlua) - Lua bindings for Rust
- [bytes](https://docs.rs/bytes/) - Zero-copy byte handling

---

## Server Lifecycle

FrogDB follows a structured startup and shutdown sequence for reliable operation and recovery.

See [docs/LIFECYCLE.md](docs/LIFECYCLE.md) for detailed startup sequence, shutdown procedure,
recovery process, and health check endpoints.

---

## Consistency Model

### Guarantees

FrogDB provides **eventual consistency** within a single node:

1. **Read-your-writes**: A client will always see its own writes
2. **Monotonic reads**: Once a value is seen, older values won't be returned
3. **Total order per key**: All operations on a single key are totally ordered

### Non-Guarantees

1. **Cross-key atomicity**: Multi-key operations (MGET, MSET) are not atomic unless all keys are on the same shard
2. **Linearizability**: Operations may be reordered across shards
3. **Durability by default**: Async durability mode may lose recent writes on crash

### Configuration Impact

| Mode | Durability | Latency |
|------|------------|---------|
| `Async` | Best-effort (may lose data) | ~1-10 μs |
| `Periodic(100ms, 1000)` | Bounded loss (100ms or 1000 writes) | ~1-10 μs |
| `Sync` | Guaranteed (fsync per write) | ~100-500 μs |
