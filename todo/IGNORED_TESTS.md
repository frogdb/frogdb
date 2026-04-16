# Remaining Ignored Tests

## 1. Cluster Scripting — cross-slot key validation (1 test)

**File:** `crates/redis-regression/tests/cluster_scripting_tcl.rs`

| Test | Line |
| ---- | ---- |
| `tcl_cross_slot_commands_are_allowed_by_default_if_they_disagree_with_pre_declared_keys` | 125 |

**Root cause:** FrogDB's strict key validation rejects accessing `foo` when only `bar` is declared
via `KEYS[...]`. Redis allows cross-slot access when `allow-cross-slot-keys` is enabled (the
default). Fixing requires `CLUSTER COUNTKEYSINSLOT` and relaxed key access semantics in the Lua
scripting engine.

## 2. Cluster Sharded PubSub — SPUBLISH cross-slot (1 test)

**File:** `crates/redis-regression/tests/cluster_sharded_pubsub_tcl.rs`

| Test | Line |
| ---- | ---- |
| `tcl_sharded_pubsub_within_multi_exec_with_cross_slot_operation` | 71 |

**Root cause:** SPUBLISH command metadata declares no keys. Cross-slot detection within MULTI/EXEC
requires SPUBLISH to declare its channel argument as a key so the slot can be checked against the
transaction's slot.

## 3. Metrics Usage (1 test)

**File:** `crates/telemetry/tests/metrics_usage.rs`

| Test                   | Line |
| ---------------------- | ---- |
| `all_metrics_are_used` | 19   |

**Current state:** 24 of ~60 metrics defined in `telemetry/src/definitions.rs` are never referenced
outside the definitions file. (3 split-brain metrics were previously listed here but are now wired
in `server/src/server/mod.rs`.)

```
frogdb_memory_rss_bytes            frogdb_cpu_user_seconds_total
frogdb_cpu_system_seconds_total    frogdb_connections_max
frogdb_connections_rejected_total  frogdb_shard_queue_depth
frogdb_shard_queue_latency_seconds frogdb_wal_pending_ops
frogdb_wal_pending_bytes           frogdb_wal_durability_lag_ms
frogdb_wal_sync_lag_ms             frogdb_wal_last_flush_timestamp
frogdb_wal_last_sync_timestamp     frogdb_snapshot_last_timestamp
frogdb_pubsub_channels             frogdb_pubsub_patterns
frogdb_pubsub_subscribers          frogdb_pubsub_messages_total
frogdb_net_input_bytes_total       frogdb_net_output_bytes_total
frogdb_memory_maxmemory_bytes      frogdb_memory_fragmentation_ratio
frogdb_eviction_samples_total      frogdb_blocked_keys
```

**Instrumentation plans:**
- All 22 wireable metrics are now instrumented (including `frogdb_shard_queue_latency_seconds`).
- Connection metrics (`connections_max`, `connections_rejected_total`) are wired via the `max_clients` feature.
