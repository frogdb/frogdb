# FrogDB Crate Map

## Dependency Layers

```
Layer 5 — Server Binary
  frogdb-server

Layer 4 — Commands & Observability
  frogdb-commands, frogdb-telemetry, frogdb-debug

Layer 3 — Core Engine
  frogdb-core

Layer 2 — Features
  frogdb-acl, frogdb-scripting, frogdb-search,
  frogdb-replication, frogdb-cluster, frogdb-persistence, frogdb-vll

Layer 1 — Foundation
  frogdb-types, frogdb-protocol

Layer 0 — Macros (no internal deps)
  frogdb-macros, frogdb-metrics-derive
```

## Crate Reference

| Crate | Path | Responsibility |
|-------|------|----------------|
| `frogdb-protocol` | `crates/protocol` | RESP2/RESP3 wire protocol parsing and encoding. No dependency on core/server. |
| `frogdb-types` | `crates/types` | Shared value types (String, List, Hash, Set, SortedSet, Stream, Bitmap, HyperLogLog, Geo, JSON, TimeSeries, Bloom), error types, argument parsing, glob matching. Depends on `frogdb-protocol`. |
| `frogdb-persistence` | `crates/persistence` | RocksDB storage engine, WAL (Async/Periodic/Sync), serialization, snapshots. Depends on `frogdb-types`. |
| `frogdb-vll` | `crates/vll` | Very Lightweight Locking — intent-based multi-shard atomicity. Depends on `frogdb-protocol`. |
| `frogdb-replication` | `crates/replication` | Primary-replica streaming (PSYNC/FULLRESYNC), WAL-based replication frames. Depends on `frogdb-protocol`, `frogdb-types`, `frogdb-persistence`. |
| `frogdb-cluster` | `crates/cluster` | Raft-based cluster coordination, slot assignment, node topology. Uses `openraft`, `rocksdb`. |
| `frogdb-acl` | `crates/acl` | Redis 7.0 ACL system — users, rules, permission checking, ACL log. Depends on `frogdb-types`. |
| `frogdb-scripting` | `crates/scripting` | Redis Functions (Lua library-based scripting), function registry, persistence. Depends on `frogdb-types`, `mlua`. |
| `frogdb-search` | `crates/search` | RediSearch-compatible full-text search (FT.* commands), index management, query parsing. Depends on `frogdb-types`, `tantivy`, `nom`. |
| `frogdb-core` | `crates/core` | Core engine: Command trait, CommandContext, Store trait, CommandRegistry, shard worker, pub/sub, eviction, scripting integration, latency/slowlog, persistence integration. Depends on ALL Layer 1-2 crates. |
| `frogdb-commands` | `crates/commands` | Data-structure command implementations (strings, hashes, lists, sets, sorted sets, streams, bitmaps, geo, bloom, HLL, JSON, timeseries, blocking, scan, sort, expiry). Depends on `frogdb-core`. |
| `frogdb-server` | `crates/server` | Server binary: connection handler, command dispatch, routing, config, admin API, cluster bus, replication setup, migration, TLS, server-level commands (ACL, CLIENT, CONFIG, FUNCTION, INFO, etc.). Depends on all crates. |
| `frogdb-telemetry` | `crates/telemetry` | Prometheus metrics, OpenTelemetry tracing, structured logging, health checks, latency bands. Depends on `frogdb-core`. |
| `frogdb-debug` | `crates/debug` | Debug web UI, hot shard detection, memory diagnostics. Depends on `frogdb-core`, `frogdb-telemetry`. |
| `frogdb-macros` | `crates/frogdb-macros` | `#[derive(Command)]` proc macro for command definitions. Standalone proc-macro crate. |
| `frogdb-metrics-derive` | `crates/metrics-derive` | Proc macros for typed metrics. Standalone proc-macro crate. |
| `frogdb-test-harness` | `crates/test-harness` | Test infrastructure: `TestServer`, `ClusterHarness`, response helpers. |
| `frogdb-testing` | `crates/testing` | Consistency checker, operation history, test models. |
| `frogdb-redis-regression` | `crates/redis-regression` | Redis compatibility regression test suite. |
| `frogdb-browser-tests` | `crates/browser-tests` | Browser integration tests for debug UI (WebDriver). |
| `tokio-coz` | `crates/tokio-coz` | Causal profiler for Tokio async runtimes. Standalone. |

All crate paths are relative to `frogdb-server/`.

## Key Files by Task

### Adding a new command
- **Command trait**: `crates/core/src/command.rs` — `Command`, `ExecutionStrategy`, `WalStrategy`, `MergeStrategy`, `Arity`, `CommandFlags`
- **Command impls**: `crates/commands/src/{module}.rs` — organized by data type
- **Registration**: `crates/commands/src/lib.rs` — `register_all()`
- **Integration tests**: `crates/server/tests/integration_{type}.rs`

### Adding a server/admin command
- **Handler**: `crates/server/src/commands/{name}.rs`
- **Dispatch**: `crates/server/src/connection/dispatch.rs`
- **Metadata registration**: `crates/server/src/connection/handlers/`

### Modifying persistence
- **RocksDB backend**: `crates/persistence/src/rocks.rs`
- **Serialization**: `crates/persistence/src/serialization.rs`
- **WAL**: `crates/persistence/src/wal.rs`
- **Snapshots**: `crates/persistence/src/snapshot.rs`
- **Integration in core**: `crates/core/src/persistence/`

### Modifying replication
- **Primary logic**: `crates/replication/src/primary.rs`
- **Replica logic**: `crates/replication/src/replica.rs`
- **Frame protocol**: `crates/replication/src/frame.rs`
- **Server integration**: `crates/server/src/commands/replication.rs`

### Modifying cluster
- **Raft state**: `crates/cluster/src/state.rs`
- **Network**: `crates/cluster/src/network.rs`
- **Server integration**: `crates/server/src/commands/cluster/`

### Modifying connection handling
- **Main handler**: `crates/server/src/connection.rs`
- **Dispatch logic**: `crates/server/src/connection/dispatch.rs`
- **Routing**: `crates/server/src/connection/router.rs`, `routing.rs`
- **State**: `crates/server/src/connection/state.rs`
- **Specialized handlers**: `crates/server/src/connection/handlers/`

### Modifying the store/data layer
- **Store trait**: `crates/core/src/store/`
- **Value types**: `crates/types/src/types.rs`
- **Shard worker**: `crates/core/src/shard/`

### Adding metrics/observability
- **Metric definitions**: `crates/telemetry/src/definitions.rs`
- **INFO sections**: `crates/telemetry/src/status.rs`
- **Prometheus recorder**: `crates/telemetry/src/prometheus_recorder.rs`
- **Dashboard gen**: `ops/grafana/dashboard-gen/`
