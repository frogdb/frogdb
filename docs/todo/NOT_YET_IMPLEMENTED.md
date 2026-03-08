# FrogDB — Not Yet Implemented

Tracking document for known unimplemented spec areas. Each item lists affected files, a brief
description, and a cross-reference to the relevant spec.

---

## Critical / Data-Integrity

| Item                                    | Files                     | Description                                                                                                                                                         | Spec |
| --------------------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| ~~Replication-mode partition recovery~~ | `crates/replication/src/` | ✅ Implemented: `ReplicationQuorumChecker` fences primary when all replica ACKs go stale (`self_fence_on_replica_loss` config, default true). Stream write timeout forces TCP disconnect during iptables partitions. | —    |

---

## Cluster (Phases 4–6)

| Item                           | Files                                   | Description                                                                                                                      | Spec                                                                          |
| ------------------------------ | --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Replica-lag scoring            | `crates/server/src/failure_detector.rs` | Auto-failover picks first available replica arbitrarily instead of scoring by replication lag                                    | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#phase-4-failover-support--partial)          |
| Point-in-time rollback         | —                                       | Full resync replaces all local state (matches Redis behavior). Deferred; split-brain log provides audit trail for manual replay. | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#45-split-brain-handling)                    |
| DFLYMIGRATE streaming protocol | —                                       | High-throughput streaming slot migration not implemented; only standard key-by-key MIGRATE exists                                | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#52-migration-protocol-commands-dflymigrate) |

---

## Spec-Described but Unimplemented

Items described in spec documentation as if implemented, but not present in the codebase. Spec files
have been annotated with `[Not Yet Implemented]` markers.

| Item                       | Description                                                                                                                                                                                     | Spec                                            |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Cluster pub/sub forwarding | `ClusterPubSubForwarder` trait, `LocalOnlyForwarder`, `broadcast_to_cluster()`, `forward_to_slot_owner()` — entire cross-node pub/sub design (~440 lines). Current pub/sub is single-node only. | [PUBSUB.md](PUBSUB.md#cluster-mode)             |
| DTrace/USDT probes         | `usdt` Cargo feature and probe definitions for zero-overhead tracing. Feature does not exist in Cargo.toml.                                                                                     | [DEBUGGING.md](DEBUGGING.md#dtrace-usdt-probes) |

---

## Stub / Unimplemented Commands

| Command                 | Status        | Notes                                                                               | Spec                                                                               |
| ----------------------- | ------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| MONITOR                 | Not planned   | Spec describes behavior but implementation not prioritized (~50% throughput impact) | [DEBUGGING.md](DEBUGGING.md#monitor-command), [COMPATIBILITY.md](COMPATIBILITY.md) |
| MODULE commands         | Not planned   | No modular architecture                                                             | [COMPATIBILITY.md](COMPATIBILITY.md)                                               |
| DEBUG SEGFAULT          | Not planned   | Intentionally omitted (dangerous)                                                   | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented)                    |
| DEBUG RELOAD            | Not planned   | Intentionally omitted (dangerous)                                                   | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented)                    |
| DEBUG CRASH-AND-RECOVER | Not planned   | Intentionally omitted (dangerous)                                                   | [DEBUGGING.md](DEBUGGING.md#dangerous-commands-not-implemented)                    |
| CONFIG REWRITE          | Not supported | Intentional: runtime changes are transient                                          | [CONFIGURATION.md](CONFIGURATION.md)                                               |
| SELECT                  | Not supported | Intentional: single database per instance                                           | [COMPATIBILITY.md](COMPATIBILITY.md#single-database)                               |

---

## Deferred Features

| Item                                  | Description                                                       | Spec                                                     |
| ------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------------- |
| Client tracking / client-side caching | CLIENT TRACKING — complex feature with high memory overhead       | [COMPATIBILITY.md](COMPATIBILITY.md#not-yet-implemented) |
| TLS (full implementation)             | Server TLS, mTLS, hot-reloading, replication TLS, cluster bus TLS | [TLS.md](TLS.md)                                         |
| Rolling upgrade (cluster mode)        | Only single-node upgrade documented                               | [DEPLOYMENT.md](DEPLOYMENT.md)                           |

---

## Operational Readiness

| Item                            | Description                                                             | Spec                     |
| ------------------------------- | ----------------------------------------------------------------------- | ------------------------ |
| Enhanced LATENCY DOCTOR         | Correlation detection, SLOWLOG cross-reference, scatter-gather analysis | [ROADMAP.md](ROADMAP.md) |
| Automated alert rule generation | `/alerts/prometheus` endpoint for generated alerting rules              | [ROADMAP.md](ROADMAP.md) |

---

## Jepsen Test Status

33 tests across three topologies (31 PASS, 2 Expected Failure). Tests marked **Expected Failure**
detect real unimplemented features and will pass once the corresponding item above is implemented.

### Single-node (19/19 PASS)

All single-node tests pass.

### Replication (4/5 PASS, 1 Expected Failure)

| Test              | Status           | Root Cause                                                                                                       | Cross-ref                                   |
| ----------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| basic-replication | PASS             | —                                                                                                                | —                                           |
| failover          | PASS             | —                                                                                                                | —                                           |
| split-brain       | PASS             | —                                                                                                                | —                                           |
| zombie            | PASS             | Primary self-fences when replica ACKs go stale (`self_fence_on_replica_loss`).                                   | ~~Replication-mode partition recovery~~ (above) |
| replication-chaos | Expected Failure | Reads observe older values during replication lag. Checker may be too strict for async replication under faults. | —                                           |

### Raft cluster (8/9 PASS, 1 Expected Failure)

| Test       | Status           | Root Cause                                                                                                                  | Cross-ref                       |
| ---------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| cross-slot | Expected Failure | Slot balancing after clean restart leaves some slots unserved during `CLUSTERDOWN` windows; cross-slot balance checks fail. | Separate cluster recovery issue |

---

## Future / Research

These are documented design aspirations, not near-term work:

| Item                                               | Spec                                                             |
| -------------------------------------------------- | ---------------------------------------------------------------- |
| Tiered storage backends (S3, DynamoDB, Azure, GCS) | [TIERED.md](TIERED.md)                                           |
| io_uring integration                               | [optimizations/ASYNC_RUNTIME.md](optimizations/ASYNC_RUNTIME.md) |
| Dashtable (DragonflyDB-style hash table)           | [STORAGE.md](STORAGE.md)                                         |
