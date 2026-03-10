# FrogDB Implementation Roadmap

> **Active roadmap lives at [`docs/todo/INDEX.md`](../todo/INDEX.md).**
> This document summarizes what has been accomplished and provides external references.

---

## Completed Milestones

The following major areas have been implemented:

- **Core abstractions**: Store trait, Command trait, Value enum (all types), shard channels, ExpiryIndex, ProtocolVersion (RESP2+RESP3), Config (CLI + TOML + env)
- **Persistence**: RocksDB WAL, snapshot support, tiered storage (hot/warm with eviction-driven demotion)
- **Replication**: Primary/replica with WAL streaming, auto full-resync on lag
- **Cluster**: Slot-based sharding, gossip protocol, cluster bus, replication failover
- **Security**: Full ACL system
- **Observability**: Prometheus metrics, OpenTelemetry tracing, latency tracking, SLOWLOG, MEMORY DOCTOR, hot shard detection, machine-readable status JSON, admin port separation
- **Code quality**: `connection.rs` decomposed into `connection/handlers/` (17 modules), eviction extracted to `shard/eviction.rs` + `crates/core/src/eviction/`, specialized types extracted to `crates/types/src/`

### Critical Abstractions

| Abstraction             | Initial (Stub)          | Status              |
| ----------------------- | ----------------------- | ------------------- |
| `Store` trait           | HashMapStore            | Same                |
| `Command` trait         | Full                    | Same                |
| `Value` enum            | StringValue only        | All types           |
| `WalWriter` trait       | NoopWalWriter           | RocksDB WAL         |
| `ReplicationConfig`     | Standalone              | Primary/Replica     |
| `ReplicationTracker`    | NoopTracker             | WAL streaming       |
| `AclChecker` trait      | AllowAllChecker         | Full ACL            |
| `MetricsRecorder` trait | NoopRecorder            | Prometheus          |
| `Tracer` trait          | NoopTracer              | OpenTelemetry       |
| Shard channels          | 1 shard                 | N shards            |
| `ExpiryIndex`           | Empty                   | Functional          |
| `ProtocolVersion`       | Resp2 only              | Resp2 + Resp3       |
| `Config` + Figment      | Full (CLI + TOML + env) | CONFIG GET/SET      |
| Logging format          | pretty + json           | Same                |

---

## References

- [CockroachDB Metrics](https://www.cockroachlabs.com/docs/stable/metrics)
- [CockroachDB Hot Ranges](https://www.cockroachlabs.com/docs/stable/understand-hotspots)
- [CockroachDB Statement Diagnostics](https://www.cockroachlabs.com/docs/stable/cockroach-statement-diag)
- [DragonflyDB Monitoring](https://www.dragonflydb.io/docs/managing-dragonfly/monitoring)
- [FoundationDB Administration](https://apple.github.io/foundationdb/administration.html)
- [FoundationDB Machine-Readable Status](https://apple.github.io/foundationdb/mr-status.html)
- [Redis Latency Monitoring](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency-monitor/)
- [Redis MEMORY DOCTOR](https://redis.io/docs/latest/commands/memory-doctor/)
- [Valkey Administration](https://valkey.io/topics/admin/)

### Spec Documents

- [INDEX.md](INDEX.md) — Architecture overview
- [EXECUTION.md](EXECUTION.md) — Command flow
- [STORAGE.md](STORAGE.md) — Data structures
- [CONCURRENCY.md](CONCURRENCY.md) — Threading model
- [PROTOCOL.md](PROTOCOL.md) — RESP handling
- [PERSISTENCE.md](PERSISTENCE.md) — RocksDB integration
- [CONFIGURATION.md](CONFIGURATION.md) — Configuration system
- [TESTING.md](TESTING.md) — Test strategy
- [optimizations/](../todo/optimizations/INDEX.md) — Performance profiling and optimization
