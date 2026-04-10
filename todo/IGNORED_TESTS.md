# Remaining Ignored Tests

## 0. BZMPOP argument validation gaps (2 tests)

**File:** `crates/redis-regression/tests/zset_tcl.rs`

| Test                                           | Line  |
| ---------------------------------------------- | ----- |
| `tcl_bzmpop_illegal_argument_negative_numkeys` | ~2014 |
| `tcl_bzmpop_count_behavioral_diffs`            | ~2027 |

**Bugs:**

1. **Negative `numkeys` panic.** `BZMPOP` in
   `frogdb-server/crates/commands/src/blocking.rs:633` parses `numkeys` via
   `parse_int(&args[1])? as usize`, which silently wraps `-1` to `usize::MAX`.
   The subsequent `2 + numkeys + 1` arithmetic then overflows and panics in
   debug mode. Fix: reject negative numkeys before the `as usize` cast (return
   `SyntaxError` / `ERR numkeys`).
2. **Repeated `COUNT` clauses / negative `COUNT` accepted.** The COUNT-parsing
   loop in `blocking.rs:657-672` accepts any number of repeated `COUNT` clauses
   (only the last wins) and also casts negative `COUNT` values via `as usize`,
   wrapping to a huge positive. Upstream Redis rejects both with a syntax
   error. Fix: track whether COUNT was already seen and reject negative values
   before the cast.

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

## 2. Redis Compatibility (17 tests)

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
| `tcl_blocking_command_accounted_only_once_in_commandstats`                                 | ~2586 |
| `tcl_blocking_command_accounted_only_once_in_commandstats_after_timeout`                   | ~2620 |
| `tcl_blpop_unblock_but_the_key_is_expired_and_then_block_again_reprocessing_command`       | ~2661 |
| `tcl_client_unblock_default_mode`                                              | ~2711 |
| `tcl_client_unblock_timeout_mode`                                              | ~2741 |
| `tcl_client_unblock_error_mode`                                              | ~2773 |

**File:** `crates/redis-regression/tests/stream_cgroups_tcl.rs`

| Test                                            | Line  |
| ----------------------------------------------- | ----- |
| `tcl_rename_can_unblock_xreadgroup_with_data`    | ~1561 |
| `tcl_rename_can_unblock_xreadgroup_with_nogroup` | ~1642 |

**File:** `crates/redis-regression/tests/zset_tcl.rs`

| Test                                                             | Line  |
| ---------------------------------------------------------------- | ----- |
| `tcl_bzpopmin_unblock_but_key_expired_then_reblock_reprocessing` | ~2196 |

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
5. **BZPOPMIN reblock-after-expire reprocessing needs DEBUG SLEEP.** The upstream test
   `BZPOPMIN unblock but the key is expired and then block again - reprocessing command`
   is tagged `needs:debug` upstream because it races `DEBUG SLEEP 0.2` inside `EXEC`
   against a `PEXPIRE 100` to guarantee the key is expired by the time the blocked
   client is woken. FrogDB implements only `DEBUG OBJECT` (see
   `frogdb-server/crates/commands/src/generic.rs:474`), so `DEBUG SLEEP` / `DEBUG
   SET-ACTIVE-EXPIRE` are unavailable and the scenario cannot be reproduced without a
   server-side injected delay. Fix option A: add `DEBUG SLEEP` support (gated on a
   test-only config flag). Fix option B: expose an internal expire-now test hook on
   `TestServer` that pauses the shard's expire path during a specific tick. The same
   gap affects the BLPOP variant
   `tcl_blpop_unblock_but_the_key_is_expired_and_then_block_again_reprocessing_command`.
6. **Blocking wake-chain not implemented.** When a blocking command (BRPOPLPUSH,
   BLMOVE) wakes and writes to a destination key, FrogDB does not re-run
   `try_satisfy_list_waiters` on that destination. Upstream Redis processes wake
   effects recursively via `handleClientsBlockedOnKeys`, so a BLMOVE that pushes
   to `dst` will immediately wake any blocker waiting on `dst`. In FrogDB the
   second blocker stays blocked until an external push arrives. Affects
   `tcl_unblock_fairness_is_kept_during_nested_unblock` (BRPOPLPUSH -> BLMPOP
   chain) and
   `tcl_command_being_unblocked_cause_another_command_execution_order` (BLMOVE
   -> BLMOVE chain). Fix: invoke `try_satisfy_list_waiters` on the destination
   key inside the blocking `BLMove` / `BRpopLPush` branches of
   `ShardWorker::try_satisfy_list_waiters` (see
   `frogdb-server/crates/core/src/shard/blocking.rs:200`).
7. **`INFO commandstats` is a stub.** `build_commandstats_info` in
   `frogdb-server/crates/server/src/commands/info.rs:394` hardcodes a single
   `cmdstat_ping:calls=0,...` line regardless of actual command usage. There is
   no per-command call/latency/rejected/failed counter. Upstream Redis emits
   one `cmdstat_<name>:...` line per command with live counters. Affects
   `tcl_blocking_command_accounted_only_once_in_commandstats` and
   `tcl_blocking_command_accounted_only_once_in_commandstats_after_timeout`.
   Fix: wire per-command counters into the command dispatch path and render
   them in the commandstats section.
8. **`CLIENT UNBLOCK` is a stub.** The handler in
   `frogdb-server/crates/server/src/connection/handlers/client.rs:916` calls
   `ClientRegistry::unblock`, which only sets a `watch::Sender<Option<UnblockMode>>`
   flag on the client entry. Nothing in the connection's blocking wait path
   (`handle_blocking_wait` in
   `frogdb-server/crates/server/src/connection/handlers/blocking.rs:22`) ever
   reads that signal — the wait `await`s only on the shard's `response_tx`
   oneshot. As a result, the blocked BLPOP is never woken by CLIENT UNBLOCK
   and the caller hangs until the command's own timeout fires. Affects
   `tcl_client_unblock_default_mode`,
   `tcl_client_unblock_timeout_mode`, and
   `tcl_client_unblock_error_mode`. Fix: add a new
   `ShardMessage::UnblockClient { conn_id, mode }` variant that the shard
   routes to `wait_queue.unregister(conn_id)`, then sends back the appropriate
   `Response::Null` or `Response::Error("UNBLOCKED ...")` on the stored
   `response_tx`. Alternatively, `select!` over the existing
   `ClientHandle::unblocked()` future inside `handle_blocking_wait`.

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
11. **List / zset satisfy paths silently drop stream waiters.**
   `try_satisfy_list_waiters` and `try_satisfy_zset_waiters` in
   `frogdb-server/crates/core/src/shard/blocking.rs` pop the oldest waiter for a
   key without checking its `BlockingOp`, then the `_ => continue` branch of the
   inner match drops the popped `WaitEntry` (and its `response_tx` `oneshot::Sender`)
   on the floor. In the transaction `MULTI; XADD s1 ...; DEL s1; LPUSH s1 ...; EXEC`,
   the LPUSH at the end of EXEC triggers `try_satisfy_list_waiters(s1)`, which pops
   the outstanding XRead waiter and discards its `response_tx`. The dropped channel
   then surfaces as a spurious `Response::Null` in `handle_blocking_wait`
   (`frogdb-server/crates/server/src/connection/handlers/blocking.rs:108-110`),
   prematurely waking the client. Redis only notifies blocked clients whose
   `BlockingOp` matches the value type of the written key. Fix: filter waiters
   by `BlockingOp` type before popping (or re-register popped, type-mismatched
   waiters), so XRead waiters are left untouched by list/zset write paths — and
   vice-versa. Same root cause applies to BLPOP/BRPOP being silently eaten by a
   XADD-on-stream satisfy path.

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
