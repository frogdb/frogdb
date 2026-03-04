# FrogDB — Not Yet Implemented

Tracking document for known unimplemented spec areas. Each item lists affected files, a brief description, and a cross-reference to the relevant spec.

---

## Critical / Data-Integrity

| Item | Files | Description | Spec |
|------|-------|-------------|------|
| DUMP/RESTORE serialization: Stream | `crates/types/src/types.rs:219-222` | `serialize_for_copy` returns empty bytes for streams — DUMP/RESTORE and MIGRATE lose data | [PERSISTENCE.md](PERSISTENCE.md#stream-serialization) |
| DUMP/RESTORE serialization: BloomFilter | `crates/types/src/types.rs:224-228` | Same: empty bytes for bloom filters | [PERSISTENCE.md](PERSISTENCE.md) |
| DUMP/RESTORE serialization: TimeSeries | `crates/types/src/types.rs:233-237` | Same: empty bytes for time series | [PERSISTENCE.md](PERSISTENCE.md) |
| Slot migration doesn't move keys | `crates/server/src/commands/cluster/` | CLUSTER SETSLOT updates metadata but actual key movement is not implemented | [ROADMAP.md](ROADMAP.md), [CLUSTER_PLAN.md](CLUSTER_PLAN.md) |
| Proactive lag-threshold full-resync | `crates/replication/src/`, `crates/server/src/replication/primary.rs` | A FULLRESYNC does occur reactively when the broadcast buffer overflows (disconnect → reconnect → PSYNC). What's missing is a configurable lag threshold that triggers proactive FULLRESYNC before buffer overflow. | [ROADMAP.md](ROADMAP.md) |

---

## Cluster (Phases 4–6)

| Item | Files | Description | Spec |
|------|-------|-------------|------|
| Self-fencing | — | Active write rejection on quorum loss not explicitly implemented (Raft provides only implicit fencing) | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#43-self-fencing) |
| Replica-lag scoring | `crates/server/src/failure_detector.rs` | Auto-failover picks first available replica arbitrarily instead of scoring by replication lag | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#phase-4-failover-support--partial) |
| Split-brain discarded-writes log | — | No `split_brain_discarded.log` for divergent writes after partition healing | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#45-split-brain-handling) |
| DFLYMIGRATE streaming protocol | — | High-throughput streaming slot migration not implemented; only standard key-by-key MIGRATE exists | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#52-migration-protocol-commands-dflymigrate) |

---

## Spec-Described but Unimplemented

Items described in spec documentation as if implemented, but not present in the codebase. Spec files have been annotated with `[Not Yet Implemented]` markers.

| Item | Description | Spec |
|------|-------------|------|
| Cluster pub/sub forwarding | `ClusterPubSubForwarder` trait, `LocalOnlyForwarder`, `broadcast_to_cluster()`, `forward_to_slot_owner()` — entire cross-node pub/sub design (~440 lines). Current pub/sub is single-node only. | [PUBSUB.md](PUBSUB.md#cluster-mode) |
| Blocking commands during slot migration | `on_slot_migration_key_transferred()` callback to send `-MOVED` to blocked clients during migration. `ShardWaitQueue` has no migration awareness. | [BLOCKING.md](BLOCKING.md#cluster-mode-slot-migration-interaction) |
| DEBUG PUBSUB LIMITS | Debug command to report per-connection and per-shard subscription usage | [PUBSUB.md](PUBSUB.md) |
| Pub/sub graceful degradation thresholds | 80%/90% warning thresholds before subscription limits hit (100% enforcement IS implemented) | [PUBSUB.md](PUBSUB.md) |
| DTrace/USDT probes | `usdt` Cargo feature and probe definitions for zero-overhead tracing. Feature does not exist in Cargo.toml. | [DEBUGGING.md](DEBUGGING.md#dtrace-usdt-probes) |
| DEBUG STRUCTSIZE | Show sizes of internal data structures | [types/SERVER.md](types/SERVER.md) |
| JSON.DEBUG | Debug info for JSON values | [types/JSON.md](types/JSON.md) |
| TS.MGET | Multi-key get for time series | [types/TIMESERIES.md](types/TIMESERIES.md) |
| TS.MRANGE / TS.MREVRANGE | Multi-key range queries for time series | [types/TIMESERIES.md](types/TIMESERIES.md) |
| TS.QUERYINDEX | Find time series keys by labels | [types/TIMESERIES.md](types/TIMESERIES.md) |
| TS.CREATERULE / TS.DELETERULE | Create/delete downsample rules for time series | [types/TIMESERIES.md](types/TIMESERIES.md) |

---

## Stub / Unimplemented Commands

| Command | Status | Notes | Spec |
|---------|--------|-------|------|
| MONITOR | Not planned | Spec describes behavior but implementation not prioritized (~50% throughput impact) | [DEBUGGING.md](DEBUGGING.md#monitor-command), [COMPATIBILITY.md](COMPATIBILITY.md) |
| MODULE commands | Not planned | No modular architecture | [COMPATIBILITY.md](COMPATIBILITY.md) |
| DEBUG SEGFAULT | Not planned | Intentionally omitted (dangerous) | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented) |
| DEBUG RELOAD | Not planned | Intentionally omitted (dangerous) | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented) |
| DEBUG CRASH-AND-RECOVER | Not planned | Intentionally omitted (dangerous) | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented) |
| CONFIG REWRITE | Not supported | Intentional: runtime changes are transient | [CONFIGURATION.md](CONFIGURATION.md) |
| SELECT | Not supported | Intentional: single database per instance | [COMPATIBILITY.md](COMPATIBILITY.md#single-database) |

---

## Deferred Features

| Item | Description | Spec |
|------|-------------|------|
| Client tracking / client-side caching | CLIENT TRACKING — complex feature with high memory overhead | [COMPATIBILITY.md](COMPATIBILITY.md#not-yet-implemented) |
| File log output with rotation | `tracing-appender` integration for non-blocking file writes — stdout/stderr only today | [OBSERVABILITY.md](OBSERVABILITY.md) |
| TLS certificate hot-reload | Certificate hot-reloading via file watching | [CONFIGURATION.md](CONFIGURATION.md) |
| Rolling upgrade (cluster mode) | Only single-node upgrade documented | [DEPLOYMENT.md](DEPLOYMENT.md) |

---

## Operational Readiness

| Item | Description | Spec |
|------|-------------|------|
| Enhanced LATENCY DOCTOR | Correlation detection, SLOWLOG cross-reference, scatter-gather analysis | [ROADMAP.md](ROADMAP.md) |
| Automated alert rule generation | `/alerts/prometheus` endpoint for generated alerting rules | [ROADMAP.md](ROADMAP.md) |

---

## Refactoring

| Item | Files | Description | Spec |
|------|-------|-------------|------|
| types.rs split (partial) | `crates/types/src/types.rs` | Core value types (String, List, Set, Hash, SortedSet, Stream) still in monolithic `types.rs` | [ROADMAP.md](ROADMAP.md#split-typesrs-partially-done) |
| Config magic numbers | `crates/server/src/config.rs` | Timeout values and sizes use inline literals instead of named constants | [ROADMAP.md](ROADMAP.md#config-magic-numbers-low-effort) |

---

## Future / Research

These are documented design aspirations, not near-term work:

| Item | Spec |
|------|------|
| Tiered storage backends (S3, DynamoDB, Azure, GCS) | [TIERED.md](TIERED.md) |
| io_uring integration | [optimizations/IO_URING.md](optimizations/IO_URING.md) |
| Dashtable (DragonflyDB-style hash table) | [STORAGE.md](STORAGE.md) |
