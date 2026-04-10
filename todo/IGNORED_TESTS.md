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

## 2. Stream Consumer Group Lag and Active-Time (5 tests)

**File:** `crates/redis-regression/tests/stream_cgroups_tcl.rs`

| Test                                                    | Line  |
| ------------------------------------------------------- | ----- |
| `tcl_consumer_seen_time_and_active_time`                | ~2570 |
| `tcl_consumer_group_read_counter_and_lag_sanity`        | ~2630 |
| `tcl_consumer_group_lag_with_xdels`                     | ~2690 |
| `tcl_consumer_group_lag_with_xdels_and_tombstone`       | ~2810 |
| `tcl_consumer_group_lag_with_xtrim`                     | ~2870 |

**Root causes:**

1. **Consumer `active-time` not tracked separately from `seen-time`.** `Consumer` in
   `frogdb-server/crates/types/src/types/stream.rs` only has a single `last_seen:
   Instant` field. Both the `idle` and `inactive` fields in `XINFO CONSUMERS` return
   the same `idle_ms()` value. Redis tracks `seen-time` (updated on any interaction)
   and `active-time` (updated only when the consumer actually reads new entries)
   separately, returning `-1` for `inactive` when active-time has never been set.
   Fix: add an `active_time: Option<Instant>` field to `Consumer`, update it only
   when XREADGROUP delivers new entries, and return `-1` for `inactive` when `None`.

2. **`XINFO STREAM FULL` does not return detailed group info.** The FULL mode in
   `frogdb-server/crates/commands/src/stream/info.rs` returns the group count as an
   integer rather than an array of group objects with `entries-read`, `lag`,
   `consumers`, and PEL details. Redis returns a nested array with full group
   metadata including per-consumer PEL entries.
   Fix: serialize each group as an array containing `name`, `last-delivered-id`,
   `entries-read`, `lag`, `pel-count`, `pel` (array), `consumers` (array with
   per-consumer PEL).

3. **Consumer group `lag` not computed.** `XINFO GROUPS` in
   `frogdb-server/crates/commands/src/stream/info.rs:196` hardcodes `lag` to
   `Response::null()`. Redis computes lag as `entries-added - entries-read` when
   `entries-read` is known and no tombstones interfere, or `nil` when computation
   is unreliable due to deletions.
   Fix: implement the lag algorithm from Redis `streamEstimateDistanceFromFirstEverEntry`.
