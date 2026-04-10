# Remaining Ignored Tests

## 1. Metrics Usage (1 test)

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

## ~~2. Stream Consumer Group Lag and Active-Time (5 tests)~~ **FIXED**

All 5 tests now pass. Consumer `active_time` tracked separately from `seen_time`,
`XINFO STREAM FULL` returns detailed group info, and lag is computed from
`entries_added - entries_read` with tombstone-awareness.
