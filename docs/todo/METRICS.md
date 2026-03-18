# Unwired Metrics Status

21 of the 22 previously unwired metrics have been instrumented. Only
`frogdb_shard_queue_latency_seconds` remains deferred (requires adding a timestamp to
`ShardMessage`).

## Wired metrics (21)

### Shard periodic gauges (11) — `core/src/shard/event_loop.rs:collect_shard_metrics()`
- `frogdb_pubsub_channels` — `subscriptions.unique_channel_count()`
- `frogdb_pubsub_patterns` — `subscriptions.unique_pattern_count()`
- `frogdb_pubsub_subscribers` — `subscriptions.total_subscription_count()`
- `frogdb_blocked_keys` — `wait_queue.blocked_keys_count()`
- `frogdb_shard_queue_depth` — `message_rx.len()`
- `frogdb_wal_pending_ops` — `wal.lag_stats().pending_ops`
- `frogdb_wal_pending_bytes` — `wal.lag_stats().pending_bytes`
- `frogdb_wal_last_flush_timestamp` — `wal.lag_stats().last_flush_timestamp_ms`
- `frogdb_wal_durability_lag_ms` — `wal.lag_stats().durability_lag_ms`
- `frogdb_wal_last_sync_timestamp` — `wal.lag_stats().last_sync_timestamp_ms`
- `frogdb_wal_sync_lag_ms` — `wal.lag_stats().sync_lag_ms`

### Point counters (4) — various call sites
- `frogdb_pubsub_messages_total` — `core/src/shard/event_loop.rs` (Publish + ShardedPublish)
- `frogdb_net_input_bytes_total` — `server/src/connection.rs:sync_stats_to_registry()`
- `frogdb_net_output_bytes_total` — `server/src/connection.rs:sync_stats_to_registry()`
- `frogdb_eviction_samples_total` — `core/src/shard/eviction.rs` (all three sample methods)

### System metrics (4) — `telemetry/src/system.rs:collect()`
- `frogdb_cpu_user_seconds_total` — `getrusage(RUSAGE_SELF)` (unix only)
- `frogdb_cpu_system_seconds_total` — `getrusage(RUSAGE_SELF)` (unix only)
- `frogdb_memory_maxmemory_bytes` — shared `AtomicU64` from config
- `frogdb_memory_fragmentation_ratio` — `rss / sum(shard_memory_used)`

### Snapshot (1) — `persistence/src/snapshot.rs`
- `frogdb_snapshot_last_timestamp` — recorded after successful snapshot completion

### Already wired (1)
- `frogdb_memory_rss_bytes` — already in `telemetry/src/system.rs`

## Deferred (1)

| Metric | Reason |
| --- | --- |
| `frogdb_shard_queue_latency_seconds` | Requires adding an `Instant` timestamp to `ShardMessage` for enqueue→dequeue delta |

## Other notes

- 2 connection metrics (`connections_max`, `connections_rejected_total`) depend on an
  unimplemented `max_clients` feature — see [CONNECTION_LIMITS.md](CONNECTION_LIMITS.md).
- System metrics collection interval changed from 15s to 5s.
- Per-shard memory is shared via `Arc<Vec<AtomicU64>>` from server to `SystemMetricsCollector`.
