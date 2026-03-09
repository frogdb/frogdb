# FrogDB — Not Yet Implemented

Tracking document for known unimplemented spec areas. Each item lists affected files, a brief
description, and a cross-reference to the relevant spec.

---

## Cluster (Phases 4–6)

| Item                           | Files | Description                                                                                                                      | Spec                                                                          |
| ------------------------------ | ----- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Point-in-time rollback         | —     | Full resync replaces all local state (matches Redis behavior). Deferred; split-brain log provides audit trail for manual replay. | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#45-split-brain-handling)                    |
| DFLYMIGRATE streaming protocol | —     | High-throughput streaming slot migration not implemented; only standard key-by-key MIGRATE exists                                | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#52-migration-protocol-commands-dflymigrate) |

---

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

## Future / Research

These are documented design aspirations, not near-term work:

| Item                                               | Spec                                                             |
| -------------------------------------------------- | ---------------------------------------------------------------- |
| Tiered storage backends (S3, DynamoDB, Azure, GCS) | [TIERED.md](TIERED.md)                                           |
| io_uring integration                               | [optimizations/ASYNC_RUNTIME.md](optimizations/ASYNC_RUNTIME.md) |
| Dashtable (DragonflyDB-style hash table)           | [STORAGE.md](STORAGE.md)                                         |
