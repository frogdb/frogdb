# Doc Map: Code Area → Documentation Files

Maps code areas to documentation files that reference them. Used in change-triggered mode
to efficiently find affected docs instead of scanning all 50+ files.

## Crate-Level Mapping

| Crate | Primary Docs | Secondary Docs |
|-------|-------------|----------------|
| `frogdb-protocol` | PROTOCOL.md | ARCHITECTURE.md, REPO.md, EXECUTION.md |
| `frogdb-types` | STORAGE.md, types/*.md | ARCHITECTURE.md, LIMITS.md |
| `frogdb-persistence` | PERSISTENCE.md | ARCHITECTURE.md, REPO.md, LIFECYCLE.md, STORAGE.md |
| `frogdb-vll` | VLL.md | CONCURRENCY.md, CONSISTENCY.md, TRANSACTIONS.md |
| `frogdb-replication` | REPLICATION.md | ARCHITECTURE.md, CONSISTENCY.md, CLUSTER.md |
| `frogdb-cluster` | CLUSTER.md | ARCHITECTURE.md, DEPLOYMENT.md, CONSISTENCY.md |
| `frogdb-acl` | AUTH.md | ARCHITECTURE.md, CONNECTION.md |
| `frogdb-scripting` | SCRIPTING.md | ARCHITECTURE.md, REPO.md, INDEX.md |
| `frogdb-search` | (no spec doc yet) | COMMANDS.md |
| `frogdb-core` | EXECUTION.md, STORAGE.md | ARCHITECTURE.md, CONCURRENCY.md, EVICTION.md |
| `frogdb-commands` | COMMANDS.md, types/*.md | EXECUTION.md |
| `frogdb-server` | ARCHITECTURE.md, CONNECTION.md | CONFIGURATION.md, LIFECYCLE.md, DEPLOYMENT.md |
| `frogdb-telemetry` | OBSERVABILITY.md | CONFIGURATION.md, DEBUGGING.md |
| `frogdb-debug` | DEBUGGING.md | OBSERVABILITY.md |
| `frogdb-macros` | EXECUTION.md | REPO.md |
| `frogdb-metrics-derive` | OBSERVABILITY.md | — |
| `frogdb-test-harness` | TESTING.md | — |
| `frogdb-testing` | TESTING.md, CONSISTENCY.md | — |
| `frogdb-redis-regression` | COMPATIBILITY.md | TESTING.md |

## High-Value File-Level Mapping

Specific source files frequently quoted or referenced in docs.

| Source File | Docs That Reference It | What's Referenced |
|------------|----------------------|-------------------|
| `Cargo.toml` (root) | REPO.md, ARCHITECTURE.md, INDEX.md | Workspace members, edition, deps, profiles |
| `crates/core/src/command.rs` | EXECUTION.md, ARCHITECTURE.md | `Command` trait, `ExecutionStrategy`, `Arity`, `CommandFlags` |
| `crates/core/src/store/` | STORAGE.md, ARCHITECTURE.md | `Store` trait, `HashMapStore` |
| `crates/types/src/types.rs` | STORAGE.md, types/*.md | `Value` enum, type definitions |
| `crates/server/src/config/` | CONFIGURATION.md, DEPLOYMENT.md | Config struct, defaults, parameters |
| `crates/server/src/connection.rs` | CONNECTION.md, ARCHITECTURE.md | Connection state, lifecycle |
| `crates/server/src/connection/dispatch.rs` | EXECUTION.md, REQUEST_FLOWS.md | Command dispatch logic |
| `crates/persistence/src/wal.rs` | PERSISTENCE.md, ARCHITECTURE.md | WAL modes, `WalWriter` |
| `crates/persistence/src/serialization.rs` | PERSISTENCE.md, STORAGE.md | Serialization format |
| `crates/acl/src/` | AUTH.md | `AclChecker` trait, ACL rules |
| `crates/scripting/src/` | SCRIPTING.md | Lua VM, script execution |
| `crates/replication/src/` | REPLICATION.md | PSYNC, frames |
| `crates/cluster/src/` | CLUSTER.md | Raft state, network |
| `crates/telemetry/src/definitions.rs` | OBSERVABILITY.md | Metric names |
| `crates/telemetry/src/status.rs` | OBSERVABILITY.md | INFO sections |
| `Justfile` | CLAUDE.md, REPO.md, TESTING.md | Recipe names, usage examples |

## Configuration Mapping

| Config Section | Source Location | Docs |
|---------------|----------------|------|
| Server (bind, port, num_shards) | `crates/server/src/config/` | CONFIGURATION.md, DEPLOYMENT.md |
| Persistence (wal_mode, snapshot) | `crates/server/src/config/` | CONFIGURATION.md, PERSISTENCE.md |
| Replication | `crates/server/src/config/` | CONFIGURATION.md, REPLICATION.md |
| TLS | `crates/server/src/config/` | CONFIGURATION.md, TLS.md |
| ACL | `crates/server/src/config/` | CONFIGURATION.md, AUTH.md |
| Eviction | `crates/server/src/config/` | CONFIGURATION.md, EVICTION.md |
| Cluster | `crates/server/src/config/` | CONFIGURATION.md, CLUSTER.md |

## Project Docs Mapping

| Doc | Sensitive To |
|-----|-------------|
| `CLAUDE.md` | Justfile recipes, build commands, crate names, test runner config |
| `README.md` | Feature list, getting started commands, crate names |
| `AGENTS.md` | Crate names, skill references |
| `testing/jepsen/README.md` | Jepsen workloads, Docker commands, namespace names |
| `testing/redis-compat/README.md` | Test runner flags, skiplist format |

## Data Type Docs Mapping

Each `docs/spec/types/*.md` file maps to command implementations in `crates/commands/src/`.

| Type Doc | Command Source | Commands Covered |
|----------|---------------|-----------------|
| types/STRING.md | `commands/src/string.rs` | GET, SET, MGET, MSET, INCR, etc. |
| types/HASH.md | `commands/src/hash.rs` | HGET, HSET, HDEL, HGETALL, etc. |
| types/LIST.md | `commands/src/list.rs` | LPUSH, RPUSH, LPOP, LRANGE, etc. |
| types/SET.md | `commands/src/set.rs` | SADD, SREM, SMEMBERS, SINTER, etc. |
| types/SORTED_SET.md | `commands/src/sorted_set.rs` | ZADD, ZRANGE, ZRANK, etc. |
| types/STREAM.md | `commands/src/stream.rs` | XADD, XREAD, XRANGE, etc. |
| types/BITMAP.md | `commands/src/bitmap.rs` | SETBIT, GETBIT, BITCOUNT, etc. |
| types/HYPERLOGLOG.md | `commands/src/hyperloglog.rs` | PFADD, PFCOUNT, PFMERGE |
| types/GEO.md | `commands/src/geo.rs` | GEOADD, GEODIST, GEOSEARCH, etc. |
| types/JSON.md | `commands/src/json.rs` | JSON.SET, JSON.GET, etc. |
| types/TIMESERIES.md | `commands/src/timeseries.rs` | TS.ADD, TS.RANGE, etc. |
| types/BLOOM.md | `commands/src/bloom.rs` | BF.ADD, BF.EXISTS, etc. |
| types/GENERIC.md | `commands/src/generic.rs` | DEL, EXISTS, KEYS, SCAN, etc. |
| types/SERVER.md | `crates/server/src/commands/` | INFO, CONFIG, CLIENT, etc. |
