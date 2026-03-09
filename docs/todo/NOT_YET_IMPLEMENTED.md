# FrogDB — Not Yet Implemented

Tracking document for known unimplemented spec areas. Each item lists affected files, a brief
description, and a cross-reference to the relevant spec.

---

## Cluster (Phases 4–6)

| Item                                     | Files | Description                                                                                                                      | Spec                                                               |
| ---------------------------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Point-in-time rollback                   | —     | Full resync replaces all local state (matches Redis behavior). Deferred; split-brain log provides audit trail for manual replay. | [CLUSTER_PLAN.md](../spec/CLUSTER_PLAN.md#45-split-brain-handling) |
| Split-brain replay                       | —     | `SPLITBRAIN` server command, automatic replay on recovery, and `frogdb-admin split-brain-replay` CLI tool for replaying divergent writes after split-brain events | [SPLIT_BRAIN_REPLAY.md](SPLIT_BRAIN_REPLAY.md) |
| Atomic slot migration + auto-rebalancing | —     | Valkey 9-style atomic slot transfer and built-in multi-dimensional rebalancing; replaces DFLYMIGRATE design                      | [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md)                   |

---

## Spec-Described but Unimplemented

Items described in spec documentation as if implemented, but not present in the codebase. Spec files
have been annotated with `[Not Yet Implemented]` markers.

| Item               | Description                                                                                                 | Spec                                            |
| ------------------ | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| DTrace/USDT probes | `usdt` Cargo feature and probe definitions for zero-overhead tracing. Implementation exists on `origin/nathan/usdt-probes` but is not yet merged to main (may be unstable). | [DEBUGGING.md](../spec/DEBUGGING.md#dtrace-usdt-probes) |

---

## Stub / Unimplemented Commands

| Command                 | Status        | Notes                                                                               | Spec                                                                                               |
| ----------------------- | ------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| MONITOR                 | Not planned   | Spec describes behavior but implementation not prioritized (~50% throughput impact) | [DEBUGGING.md](../spec/DEBUGGING.md#monitor-command), [COMPATIBILITY.md](../spec/COMPATIBILITY.md) |
| MODULE commands         | Not planned   | No modular architecture                                                             | [COMPATIBILITY.md](../spec/COMPATIBILITY.md)                                                       |
| SELECT                  | Not supported | Intentional: single database per instance                                           | [COMPATIBILITY.md](../spec/COMPATIBILITY.md#single-database)                                       |

---

## Deferred Features

| Item                                  | Description                                                       | Spec                                                             |
| ------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| Client tracking / client-side caching | CLIENT TRACKING — complex feature with high memory overhead       | [COMPATIBILITY.md](../spec/COMPATIBILITY.md#not-yet-implemented) |
| TLS (full implementation)             | Server TLS, mTLS, hot-reloading, replication TLS, cluster bus TLS | [TLS.md](../spec/TLS.md)                                         |
| Rolling upgrade (cluster mode)        | Full spec written; implementation not started                     | [ROLLING_UPGRADE.md](../spec/ROLLING_UPGRADE.md)         |
| Two-tier storage (RAM + disk)         | Demote values to RocksDB on memory pressure instead of evicting; promote on access | [TIERED.md](../spec/TIERED.md)                           |

---

## Operational Readiness

| Item                            | Description                                                             | Spec                             |
| ------------------------------- | ----------------------------------------------------------------------- | -------------------------------- |
| Automated alert rule generation | `/alerts/prometheus` endpoint for generated alerting rules              | [ROADMAP.md](../spec/ROADMAP.md) |

## Future / Research

These are documented design aspirations, not near-term work:

| Item                                               | Spec                                                             |
| -------------------------------------------------- | ---------------------------------------------------------------- |
| Cold storage backends (S3, DynamoDB, Azure, GCS)   | [TIERED.md](../spec/TIERED.md)                                           |
| io_uring integration                               | [optimizations/ASYNC_RUNTIME.md](optimizations/ASYNC_RUNTIME.md) |
| Dashtable (DragonflyDB-style hash table)           | [STORAGE.md](../spec/STORAGE.md)                                 |
