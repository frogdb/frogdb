# Unwired Metrics Instrumentation Plan

24 of ~60 metrics defined in `telemetry/src/definitions.rs` are never referenced outside the
definitions file. 3 split-brain metrics were previously in this category but are now wired
(see `server/src/server/mod.rs`). 2 connection metrics (`connections_max`, `connections_rejected_total`)
depend on an unimplemented `max_clients` feature — see [CONNECTION_LIMITS.md](CONNECTION_LIMITS.md).

This document covers the remaining **22 wireable metrics**, grouped by effort tier.

---

## Tier 1 — Gauge snapshots from existing state (low effort)

These metrics have backing data already accessible; they just need periodic gauge updates
(e.g., in the stats collection loop or on-demand via INFO).

| Metric | Type | Source location | Notes |
| --- | --- | --- | --- |
| `frogdb_pubsub_channels` | gauge | `server/src/server/mod.rs` | Count of active unsharded pub/sub channels |
| `frogdb_pubsub_patterns` | gauge | `server/src/server/mod.rs` | Count of active pattern subscriptions |
| `frogdb_pubsub_subscribers` | gauge | `server/src/server/mod.rs` | Total subscriber count across channels |
| `frogdb_blocked_keys` | gauge | `core/src/shard/event_loop.rs` | Keys with blocked clients waiting |
| `frogdb_memory_maxmemory_bytes` | gauge | `server/src/runtime_config.rs` | Config value, set once at startup + on CONFIG SET |
| `frogdb_shard_queue_depth` | gauge | `core/src/shard/event_loop.rs` | Per-shard channel `.len()` |
| `frogdb_snapshot_last_timestamp` | gauge | `core/src/shard/persistence.rs` | Record after successful snapshot completion |

**Implementation pattern:** Add `MetricName::set(&*recorder, value)` calls at the point where
the backing data is already computed or trivially queryable.

---

## Tier 2 — Counters/histograms at existing call sites (medium effort)

These require instrumenting an existing code path with an `inc()` or `observe()` call.

| Metric | Type | Source location | Notes |
| --- | --- | --- | --- |
| `frogdb_pubsub_messages_total` | counter | `server/src/server/mod.rs`, `connection/router.rs` | Increment on PUBLISH dispatch |
| `frogdb_net_input_bytes_total` | counter | `server/src/connection/` | Bytes read from client socket |
| `frogdb_net_output_bytes_total` | counter | `server/src/connection/` | Bytes written to client socket |
| `frogdb_eviction_samples_total` | counter | `core/src/store/hashmap.rs` | Increment per sample during eviction |
| `frogdb_shard_queue_latency_seconds` | histogram | `core/src/shard/event_loop.rs` | Timestamp delta from enqueue to dequeue |
| `frogdb_wal_pending_ops` | gauge | `persistence/src/wal.rs` | Ops buffered but not yet flushed |
| `frogdb_wal_pending_bytes` | gauge | `persistence/src/wal.rs` | Bytes buffered but not yet flushed |
| `frogdb_wal_last_flush_timestamp` | gauge | `persistence/src/wal.rs` | Record after each flush completes |
| `frogdb_wal_last_sync_timestamp` | gauge | `persistence/src/wal.rs` | Record after each fsync completes |

**Implementation pattern:** Find the exact call site (flush, sync, publish, read/write) and
add the metric call adjacent to the existing logic.

---

## Tier 3 — Derived or OS-level metrics (higher effort)

These require new data sources (OS APIs, arithmetic from other gauges).

| Metric | Type | Source location | Notes |
| --- | --- | --- | --- |
| `frogdb_memory_rss_bytes` | gauge | `telemetry/src/status.rs` | Needs platform-specific RSS query (`getrusage` / `/proc/self/status`) |
| `frogdb_cpu_user_seconds_total` | counter | `telemetry/src/status.rs` | `getrusage(RUSAGE_SELF).ru_utime` |
| `frogdb_cpu_system_seconds_total` | counter | `telemetry/src/status.rs` | `getrusage(RUSAGE_SELF).ru_stime` |
| `frogdb_memory_fragmentation_ratio` | gauge | `telemetry/src/status.rs` | `rss / used_memory` — requires both values |
| `frogdb_wal_durability_lag_ms` | gauge | `core/src/shard/persistence.rs` | `now - last_flush_timestamp` in ms |
| `frogdb_wal_sync_lag_ms` | gauge | `core/src/shard/persistence.rs` | `now - last_sync_timestamp` in ms |

**Implementation pattern:** Add a periodic stats collection task (e.g., every 1s) that queries
OS APIs and computes derived values. The `telemetry/src/status.rs` file already has related
helpers for INFO output.

---

## Implementation notes

- All metric types (`gauge`, `counter`, `histogram`) use the typed API from `define_metrics!` —
  call `TypeName::set()`, `TypeName::inc()`, or `TypeName::observe()` with `&*recorder`.
- Shard-level metrics (`shard_queue_depth`, `shard_queue_latency`) need the `shard` label.
- `pubsub_channels` tracks unsharded channels only. Sharded pub/sub channels
  (`SSUBSCRIBE`/`SPUBLISH`) would need a separate `frogdb_pubsub_sharded_channels` metric
  if/when sharded pub/sub stats are needed.
- Network byte counters should be placed at the codec/framing layer to capture all traffic
  including protocol overhead, not just payload bytes.
- The OS-level metrics (Tier 3) should use `#[cfg(unix)]` guards and provide no-op fallbacks
  on unsupported platforms.
