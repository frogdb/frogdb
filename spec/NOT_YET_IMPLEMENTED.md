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
| Auto full-resync trigger | `crates/replication/src/`, `crates/server/src/replication/primary.rs:451` | When replica lag exceeds threshold, only a warning is logged — no automatic FULLRESYNC is triggered | [ROADMAP.md](ROADMAP.md) |

---

## Cluster (Phases 4–6)

| Item | Files | Description | Spec |
|------|-------|-------------|------|
| Self-fencing | — | Active write rejection on quorum loss not explicitly implemented (Raft provides only implicit fencing) | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#43-self-fencing) |
| Replica-lag scoring | `crates/server/src/failure_detector.rs` | Auto-failover picks first available replica arbitrarily instead of scoring by replication lag | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#phase-4-failover-support--partial) |
| Split-brain discarded-writes log | — | No `split_brain_discarded.log` for divergent writes after partition healing | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#45-split-brain-handling) |
| DFLYMIGRATE streaming protocol | — | High-throughput streaming slot migration not implemented; only standard key-by-key MIGRATE exists | [CLUSTER_PLAN.md](CLUSTER_PLAN.md#52-migration-protocol-commands-dflymigrate) |
| Chaos / Jepsen / Turmoil cluster testing | — | No distributed correctness tests for cluster scenarios | [ROADMAP.md](ROADMAP.md), [TESTING.md](TESTING.md) |

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
| ACL selectors (Redis 7.0) | ACL selector support | [AUTH.md](AUTH.md) |
| File log output with rotation | `tracing-appender` integration for non-blocking file writes — stdout/stderr only today | [OBSERVABILITY.md](OBSERVABILITY.md) |
| TLS certificate hot-reload | Certificate hot-reloading via file watching | [CONFIGURATION.md](CONFIGURATION.md) |
| Diagnostic bundles | Automated collection of debug info into a single archive | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) |
| Rolling upgrade (cluster mode) | Only single-node upgrade documented | [DEPLOYMENT.md](DEPLOYMENT.md) |

---

## Operational Readiness

| Item | Description | Spec |
|------|-------------|------|
| Grafana dashboard templates | Overview, Performance, Shards, Persistence JSON dashboards | [ROADMAP.md](ROADMAP.md) |
| Enhanced LATENCY DOCTOR | Correlation detection, SLOWLOG cross-reference, scatter-gather analysis | [ROADMAP.md](ROADMAP.md) |
| Automated alert rule generation | `/alerts/prometheus` endpoint for generated alerting rules | [ROADMAP.md](ROADMAP.md) |

---

## Refactoring

| Item | Files | Description | Spec |
|------|-------|-------------|------|
| types.rs split (partial) | `crates/types/src/types.rs` | Core value types (String, List, Set, Hash, SortedSet, Stream) still in monolithic `types.rs` | [ROADMAP.md](ROADMAP.md#split-typesrs-partially-done) |
| Config magic numbers | `crates/server/src/config.rs` | Timeout values and sizes use inline literals instead of named constants | [ROADMAP.md](ROADMAP.md#config-magic-numbers-low-effort) |
| Sorted set parsing helpers | `crates/server/src/` | `parse_score_bound()`, `parse_lex_bound()`, `parse_set_op_options()` could be extracted to utils | [ROADMAP.md](ROADMAP.md#sorted-set-parsing-helpers-low-effort) |

---

## Future / Research

These are documented design aspirations, not near-term work:

| Item | Spec |
|------|------|
| Tiered storage backends (S3, DynamoDB, Azure, GCS) | [TIERED.md](TIERED.md) |
| io_uring integration | [optimizations/IO_URING.md](optimizations/IO_URING.md) |
| Deterministic simulation testing (DST / Turmoil / MadSim) | [TESTING.md](TESTING.md) |
| Jepsen linearizability testing | [TESTING.md](TESTING.md) |
| Tokio causal profiler | [TOKIO_CAUSAL_PROFILER.md](TOKIO_CAUSAL_PROFILER.md) |
| Dashtable (DragonflyDB-style hash table) | [STORAGE.md](STORAGE.md) |
