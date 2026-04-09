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

## 2. Redis Compatibility (3 tests)

**File:** `crates/redis-regression/tests/stream_tcl.rs`

| Test                                                    | Line  |
| ------------------------------------------------------- | ----- |
| `tcl_xadd_with_limit_delete_entries_no_more_than_limit` | ~1351 |
| `tcl_xsetid_offset_without_max_tombstone`               | ~1532 |
| `tcl_xsetid_max_tombstone_without_offset`               | ~1555 |

**Bugs:**

1. **`XADD MAXLEN ~ 0 LIMIT 1` trims one entry instead of zero.** `StreamValue::trim`
   in `frogdb-server/crates/types/src/types/stream.rs:1025` ignores the
   `StreamTrimMode::Approximate` flag — approximate trimming is treated the same as
   exact trimming. Upstream Redis' approximate trim only drops whole radix-tree
   nodes, so with `stream-node-max-entries` defaulting to a large value, `MAXLEN ~
   0 LIMIT 1` is typically a no-op. Fix: honor `Approximate` mode by skipping trims
   that would remove fewer than a full node's worth of entries, or by using the
   `LIMIT` as an upper-bound-per-node budget.
2. **`XSETID` ignores trailing `ENTRIESADDED` / `MAXDELETEDID` arguments.**
   `XsetidCommand::execute` in
   `frogdb-server/crates/commands/src/stream/basic.rs:387` parses only the key and
   the new ID, silently ignoring the optional `ENTRIESADDED entries-added` and
   `MAXDELETEDID max-deleted-id` clauses. Redis performs syntactic validation
   (requires both to appear together, rejects negative ENTRIESADDED, rejects a
   `MAXDELETEDID` greater than the new ID). Fix: parse the optional clauses, enforce
   pairing, validate bounds, and store the values on the stream.
