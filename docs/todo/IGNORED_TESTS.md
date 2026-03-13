# Remaining Ignored Tests

1 test remains ignored in 1 test file. It requires non-trivial infrastructure work.

Originally 51 tests were ignored. 28 were un-ignored and 4 stubs were removed across these
completed workstreams: WATCH/EXEC dirty-flag rewrite (6), CLIENT PAUSE fixes (6 of 6),
Lua script timeout (4), OOM transaction tests (3), Cluster READONLY/READWRITE (4),
OBJECT IDLETIME/FREQ (2), Replication checkpoint SHA256 verification (1),
Evicted/Expired keys stats (1), CLUSTER RESET (2),
PubSub slot migration notification (1),
Active expiry suppression during pause (1).
DEBUG set-active-expire stubs were removed (4).
6 architecturally incompatible tests were removed: 3 gossip protocol stats (FrogDB uses Raft,
not gossip), 2 EVAL shebang tests (non-standard Redis extension), and 1 proactive lag threshold
test (FrogDB uses TCP backpressure instead).

---

## 1. Metrics Usage (1 test)

**File:** `crates/telemetry/tests/metrics_usage.rs`

| Test | Line |
|------|------|
| `all_metrics_are_used` | 19 |

**Current state:** 27 of ~60 metrics defined in `telemetry/src/definitions.rs` are never
referenced outside the definitions file:

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
1. Wire each metric into its corresponding subsystem (significant — touches WAL, eviction,
   pub/sub, network, OS-level collection for RSS/CPU).
2. Remove unused definitions and re-add them when features are implemented.
3. Keep ignored until enough subsystems are instrumented to bring the count down.

---

## Suggested Priority

1. **Metrics usage** — bulk instrumentation pass
