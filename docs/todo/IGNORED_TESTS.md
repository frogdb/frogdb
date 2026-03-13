# Remaining Ignored Tests

6 tests remain ignored across 4 test files. All require non-trivial infrastructure work.

Originally 51 tests were ignored. 25 were un-ignored and 4 stubs were removed across these
completed workstreams: WATCH/EXEC dirty-flag rewrite (6), CLIENT PAUSE fixes (5 of 6),
Lua script timeout (4), OOM transaction tests (3), Cluster READONLY/READWRITE (4),
OBJECT IDLETIME/FREQ (2), Replication checkpoint SHA256 verification (1).
DEBUG set-active-expire stubs were removed (4).
6 architecturally incompatible tests were removed: 3 gossip protocol stats (FrogDB uses Raft,
not gossip), 2 EVAL shebang tests (non-standard Redis extension), and 1 proactive lag threshold
test (FrogDB uses TCP backpressure instead).

---

## 1. CLUSTER RESET (2 tests)

**File:** `crates/server/tests/integration_cluster.rs`

| Test | Line |
|------|------|
| `test_cluster_reset_soft_clears_slot_assignments` | 6871 |
| `test_cluster_reset_hard_clears_node_id` | 6899 |

**Current state:** Handler at `commands/cluster/admin.rs:357-375` parses HARD/SOFT flags but
returns `Ok(Response::ok())` without doing anything.

**What's needed:**

- **SOFT reset:** Clear all slot assignments from this node's cluster state, remove all other nodes
  from the known-nodes table, revert this node to primary role. Likely requires a new Raft operation
  to atomically clear cluster metadata.

- **HARD reset:** Everything in SOFT, plus reset the config epoch to 0 and generate a new random
  40-character node ID. The node ID generation infrastructure already exists (used at startup).

- **Raft implications:** Clearing cluster state on a Raft member is non-trivial. A reset node
  effectively leaves the cluster — need to handle the case where a reset node rejoins or starts a
  new single-node cluster.

---

## 2. Evicted Keys Stat (1 test)

**File:** `crates/redis-regression/tests/maxmemory_regression.rs`

| Test | Line |
|------|------|
| `evicted_keys_stat_tracked` | 484 |

**Current state:** `commands/info.rs:330` hardcodes `evicted_keys:0\r\n`. The eviction subsystem
runs on each shard worker but does not maintain a counter.

**What's needed:**

1. Add an `Arc<AtomicU64>` eviction counter to the shard worker (or a shared stats struct).
2. Increment it in the eviction code path when keys are actually evicted.
3. Aggregate across shards in the INFO stats handler and replace the hardcoded `0`.

This is cross-layer plumbing: eviction happens on shard workers (core crate), INFO runs on the
connection level (server crate). Similar pattern to how `keys_total` is already aggregated.

---

## 3. Active/Passive Expires Skipped During Pause (1 test)

**File:** `crates/redis-regression/tests/pause_regression.rs`

| Test | Line |
|------|------|
| `active_passive_expires_skipped_during_pause` | 362 |

**Current state:** `run_active_expiry()` in `core/src/shard/event_loop.rs:368` runs on a timer
interval with 25ms budget. There are **no pause-related checks** — active expiry runs regardless
of CLIENT PAUSE state. The pause check (`wait_if_paused()`) only applies to command execution
in `server/src/connection.rs:1055-1079`.

**What's needed:**

1. Thread `Arc<AtomicBool>` pause flag (or `Arc<RwLock<PauseState>>`) from the server's pause
   state to each `ShardWorker`.
2. In `event_loop.rs`, skip `run_active_expiry()` when the pause flag is set.
3. For passive expiry (lazy expiry on key access), this is harder — the test comment suggests
   verifying via `expired_keys` stat, which is also not yet tracked per-key-access.

**Test approach:** SET key with 1s TTL → CLIENT PAUSE ALL for 3s → sleep 2s → verify key still
exists → unpause → verify key expires.

---

## 4. PubSub Slot Migration Notification (1 test)

**File:** `crates/server/tests/integration_pubsub.rs`

| Test | Line |
|------|------|
| `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` | 730 |

**What's needed:** During slot migration, enumerate sharded pubsub (`SSUBSCRIBE`) subscribers for
channels whose slot is being migrated. Send `SUNSUBSCRIBE` notification to each affected
subscriber before completing migration. Currently the test body is essentially empty — just starts
a cluster and shuts down. Inspired by Redis `25-pubsubshard-slot-migration.tcl`.

---

## 5. Metrics Usage (1 test)

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

1. **Evicted keys stat** — straightforward counter plumbing
2. **Active expires during pause** — localized flag threading
3. **CLUSTER RESET** — moderate Raft work
4. **Metrics usage** — bulk instrumentation pass
5. **PubSub slot migration** — deep slot migration integration
