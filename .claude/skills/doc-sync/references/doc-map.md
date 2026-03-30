# Doc Map: Code Area → Documentation Files

Maps code areas to documentation files that reference them. Used in change-triggered mode
to efficiently find affected docs instead of scanning all files.

Documentation lives in `website/src/content/docs/` organized into three sections:
- `architecture/` — contributor/internals docs (was `docs/contributors/`)
- `operations/` — operator docs (was `docs/operators/`)
- `guides/` — user-facing docs (was `docs/users/`)

## Crate-Level Mapping

| Crate | Primary Docs | Secondary Docs |
|-------|-------------|----------------|
| `frogdb-protocol` | `architecture/protocol.md` | `architecture/architecture.md`, `architecture/execution.md` |
| `frogdb-types` | `architecture/storage.md` | `architecture/architecture.md`, `guides/limits.md` |
| `frogdb-persistence` | `architecture/persistence.md` | `architecture/architecture.md`, `operations/persistence.md`, `architecture/storage.md` |
| `frogdb-vll` | `architecture/vll.md` | `architecture/concurrency.md`, `architecture/consistency.md`, `guides/transactions.md` |
| `frogdb-replication` | `architecture/replication.md` | `architecture/architecture.md`, `architecture/consistency.md`, `operations/replication.md` |
| `frogdb-cluster` | `architecture/clustering.md` | `architecture/architecture.md`, `operations/clustering.md`, `architecture/consistency.md` |
| `frogdb-acl` | — | `architecture/architecture.md`, `architecture/connection.md` |
| `frogdb-scripting` | `guides/lua-scripting.md` | `architecture/architecture.md` |
| `frogdb-search` | — | `guides/commands.md` |
| `frogdb-core` | `architecture/execution.md`, `architecture/storage.md` | `architecture/architecture.md`, `architecture/concurrency.md`, `operations/eviction.md` |
| `frogdb-commands` | `guides/commands.md` | `architecture/execution.md` |
| `frogdb-server` | `architecture/architecture.md`, `architecture/connection.md` | `operations/configuration.md`, `operations/lifecycle.md`, `operations/deployment.md` |
| `frogdb-telemetry` | `operations/monitoring.md` | `operations/configuration.md`, `architecture/debugging.md` |
| `frogdb-debug` | `architecture/debugging.md` | `operations/monitoring.md` |
| `frogdb-macros` | `architecture/execution.md` | — |
| `frogdb-metrics-derive` | `operations/monitoring.md` | — |
| `frogdb-test-harness` | `architecture/testing.md` | — |
| `frogdb-testing` | `architecture/testing.md`, `architecture/consistency.md` | — |
| `frogdb-redis-regression` | `guides/compatibility.md` | `architecture/testing.md` |

All doc paths above are relative to `website/src/content/docs/`.

## High-Value File-Level Mapping

Specific source files frequently quoted or referenced in docs.

| Source File | Docs That Reference It | What's Referenced |
|------------|----------------------|-------------------|
| `Cargo.toml` (root) | `architecture/architecture.md` | Workspace members, edition, deps, profiles |
| `crates/core/src/command.rs` | `architecture/execution.md`, `architecture/architecture.md` | `Command` trait, `ExecutionStrategy`, `Arity`, `CommandFlags` |
| `crates/core/src/store/` | `architecture/storage.md`, `architecture/architecture.md` | `Store` trait, `HashMapStore` |
| `crates/types/src/types.rs` | `architecture/storage.md` | `Value` enum, type definitions |
| `crates/server/src/config/` | `operations/configuration.md`, `operations/deployment.md` | Config struct, defaults, parameters |
| `crates/server/src/connection.rs` | `architecture/connection.md`, `architecture/architecture.md` | Connection state, lifecycle |
| `crates/server/src/connection/dispatch.rs` | `architecture/execution.md`, `architecture/request-flows.md` | Command dispatch logic |
| `crates/persistence/src/wal.rs` | `architecture/persistence.md`, `architecture/architecture.md` | WAL modes, `WalWriter` |
| `crates/persistence/src/serialization.rs` | `architecture/persistence.md`, `architecture/storage.md` | Serialization format |
| `crates/acl/src/` | `operations/security.md` | `AclChecker` trait, ACL rules |
| `crates/scripting/src/` | `guides/lua-scripting.md` | Lua VM, script execution |
| `crates/replication/src/` | `architecture/replication.md` | PSYNC, frames |
| `crates/cluster/src/` | `architecture/clustering.md` | Raft state, network |
| `crates/telemetry/src/definitions.rs` | `operations/monitoring.md` | Metric names |
| `crates/telemetry/src/status.rs` | `operations/monitoring.md` | INFO sections |
| `Justfile` | `CLAUDE.md`, `architecture/testing.md` | Recipe names, usage examples |

## Configuration Mapping

| Config Section | Source Location | Docs |
|---------------|----------------|------|
| Server (bind, port, num_shards) | `crates/server/src/config/` | `operations/configuration.md`, `operations/deployment.md` |
| Persistence (wal_mode, snapshot) | `crates/server/src/config/` | `operations/configuration.md`, `operations/persistence.md` |
| Replication | `crates/server/src/config/` | `operations/configuration.md`, `operations/replication.md` |
| TLS | `crates/server/src/config/` | `operations/configuration.md` |
| ACL | `crates/server/src/config/` | `operations/configuration.md`, `operations/security.md` |
| Eviction | `crates/server/src/config/` | `operations/configuration.md`, `operations/eviction.md` |
| Cluster | `crates/server/src/config/` | `operations/configuration.md`, `operations/clustering.md` |

## Project Docs Mapping

| Doc | Sensitive To |
|-----|-------------|
| `CLAUDE.md` | Justfile recipes, build commands, crate names, test runner config |
| `README.md` | Feature list, getting started commands, crate names |
| `AGENTS.md` | Crate names, skill references |
| `testing/jepsen/README.md` | Jepsen workloads, Docker commands, namespace names |
| `testing/redis-compat/README.md` | Test runner flags, skiplist format |
