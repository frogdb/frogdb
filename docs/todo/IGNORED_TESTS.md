# Remaining Ignored Tests

## 1. Metrics Usage (1 test)

**File:** `crates/telemetry/tests/metrics_usage.rs`

| Test                   | Line |
| ---------------------- | ---- |
| `all_metrics_are_used` | 19   |

**Current state:** 27 of ~60 metrics defined in `telemetry/src/definitions.rs` are never referenced
outside the definitions file:

```
frogdb_memory_rss_bytes          frogdb_cpu_user_seconds_total
frogdb_cpu_system_seconds_total  frogdb_connections_max
frogdb_connections_rejected_total frogdb_shard_queue_depth
frogdb_shard_queue_latency_seconds frogdb_wal_pending_ops
frogdb_wal_pending_bytes         frogdb_wal_durability_lag_ms
frogdb_wal_sync_lag_ms           frogdb_wal_last_flush_timestamp
frogdb_wal_last_sync_timestamp   frogdb_snapshot_last_timestamp
frogdb_pubsub_channels           frogdb_pubsub_patterns
frogdb_pubsub_subscribers        frogdb_pubsub_messages_total
frogdb_net_input_bytes_total     frogdb_net_output_bytes_total
frogdb_memory_maxmemory_bytes    frogdb_memory_fragmentation_ratio
frogdb_eviction_samples_total    frogdb_blocked_keys
frogdb_split_brain_events_total  frogdb_split_brain_ops_discarded_total
frogdb_split_brain_recovery_pending
```

**Options:**
1. Wire each metric into its corresponding subsystem (significant — touches WAL, eviction, pub/sub,
   network, OS-level collection for RSS/CPU).
2. Remove unused definitions and re-add them when features are implemented.
3. Keep ignored until enough subsystems are instrumented to bring the count down.