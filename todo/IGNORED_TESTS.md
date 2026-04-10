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

## 2. Redis Compatibility (10 tests)

**File:** `crates/redis-regression/tests/stream_tcl.rs`

| Test                                                    | Line  |
| ------------------------------------------------------- | ----- |
| `tcl_xadd_with_limit_delete_entries_no_more_than_limit` | ~1351 |
| `tcl_xsetid_offset_without_max_tombstone`               | ~1532 |
| `tcl_xsetid_max_tombstone_without_offset`               | ~1555 |
| `tcl_xread_xadd_del_lpush_should_not_awake_client`      | ~1147 |

**File:** `crates/redis-regression/tests/list_tcl.rs`

| Test                                                                                       | Line  |
| ------------------------------------------------------------------------------------------ | ----- |
| `tcl_blpop_when_new_key_is_moved_into_place`                                               | ~2318 |
| `tcl_blpop_when_result_key_is_created_by_sort_store`                                       | ~2349 |
| `tcl_unblock_fairness_is_kept_during_nested_unblock`                                       | ~2440 |
| `tcl_command_being_unblocked_cause_another_command_execution_order`                        | ~2511 |

**File:** `crates/redis-regression/tests/stream_cgroups_tcl.rs`

| Test                                            | Line  |
| ----------------------------------------------- | ----- |
| `tcl_rename_can_unblock_xreadgroup_with_data`    | ~1561 |
| `tcl_rename_can_unblock_xreadgroup_with_nogroup` | ~1642 |

**Bugs:**

3. **RENAME does not signal blocked-key watchers.** When `RENAME src dst` creates or
   overwrites the destination key, FrogDB does not fire the "key modified" signal that
   wakes clients blocked on `BLPOP dst 0` (or `XREADGROUP` on a stream). Redis fires
   `signalModifiedKey` inside `renameGenericCommand`, which triggers
   `handleClientsBlockedOnKeys`. Fix: after the rename completes, call the
   key-modified notification for the destination key.
4. **SORT..STORE does not signal blocked-key watchers.** Same root cause as above —
   `SORT notfoo ALPHA STORE foo` writes to the destination key but does not notify the
   blocking subsystem. Fix: fire the key-modified signal for the STORE destination key.
5. ~~**BZPOPMIN reblock-after-expire reprocessing needs DEBUG SLEEP.**~~ **FIXED.** Tests
   rewritten to use MULTI/EXEC with PEXPIRE 0. Satisfy is deferred to end-of-EXEC;
   `purge_if_expired` added to satisfy paths as lazy expiry safety net. DEBUG SLEEP
   gated behind `enable-debug-command` config flag.
6. ~~**Blocking wake-chain not implemented.**~~ **FIXED.** `try_satisfy_list_waiters`
   now recursively calls itself on the BLMove/BRPOPLPUSH destination key with a depth
   cap of 16.
7. ~~**`INFO commandstats` is a stub.**~~ **FIXED.** Per-command call counts aggregated
   in `ClientRegistry` and rendered by `handle_info`. Blocking commands force-sync
   stats immediately. `usec`/`rejected_calls`/`failed_calls` are still hardcoded to 0.
8. ~~**`CLIENT UNBLOCK` is a stub.**~~ **FIXED.** `handle_blocking_wait` now uses a
   three-way `tokio::select!` over the shard response, `ClientHandle::unblocked()`,
   and the timeout deadline.

9. **`XADD MAXLEN ~ 0 LIMIT 1` trims one entry instead of zero.** `StreamValue::trim`
   in `frogdb-server/crates/types/src/types/stream.rs:1025` ignores the
   `StreamTrimMode::Approximate` flag — approximate trimming is treated the same as
   exact trimming. Upstream Redis' approximate trim only drops whole radix-tree
   nodes, so with `stream-node-max-entries` defaulting to a large value, `MAXLEN ~
   0 LIMIT 1` is typically a no-op. Fix: honor `Approximate` mode by skipping trims
   that would remove fewer than a full node's worth of entries, or by using the
   `LIMIT` as an upper-bound-per-node budget.
10. **`XSETID` ignores trailing `ENTRIESADDED` / `MAXDELETEDID` arguments.**
    `XsetidCommand::execute` in
    `frogdb-server/crates/commands/src/stream/basic.rs:387` parses only the key and
    the new ID, silently ignoring the optional `ENTRIESADDED entries-added` and
    `MAXDELETEDID max-deleted-id` clauses. Redis performs syntactic validation
    (requires both to appear together, rejects negative ENTRIESADDED, rejects a
    `MAXDELETEDID` greater than the new ID). Fix: parse the optional clauses, enforce
    pairing, validate bounds, and store the values on the stream.
11. ~~**List / zset satisfy paths silently drop stream waiters.**~~ **FIXED.** Added
   type-filtered `pop_oldest_waiter_of_kind` / `has_waiters_for_kind` to
   `ShardWaitQueue`. All three satisfy functions (list, zset, stream) now only pop
   waiters whose `BlockingOp` matches the expected `WaiterKind`, leaving mismatched
   waiters untouched in the queue.

## 3. Stream Consumer Group Lag and Active-Time (5 tests)

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
