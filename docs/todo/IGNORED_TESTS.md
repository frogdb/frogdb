# Remaining Ignored Tests

16 tests remain ignored across 6 test files. All require non-trivial infrastructure work.

Originally 51 tests were ignored. 24 were un-ignored and 4 stubs were removed across these
completed workstreams: WATCH/EXEC dirty-flag rewrite (6), CLIENT PAUSE fixes (5 of 6),
Lua script timeout (4), OOM transaction tests (3), Cluster READONLY/READWRITE (4),
OBJECT IDLETIME/FREQ (2). DEBUG set-active-expire stubs were removed (4).

---

## 1. Cluster Stats & Metadata (5 tests)

**File:** `crates/server/tests/integration_cluster.rs`

| Test | Line | Hardcoded location |
|------|------|--------------------|
| `test_cluster_shards_replication_offset_nonzero` | 5594 | `commands/cluster/mod.rs:476` — `replication-offset` is `0` |
| `test_cluster_slots_replication_offset_nonzero` | 5632 | `commands/cluster/mod.rs:502` — `offset` is `0` |
| `test_cluster_nodes_link_state_tracks_failures` | 5669 | `commands/cluster/mod.rs:330` — link-state is always `"connected"` |
| `test_cluster_nodes_ping_pong_nonzero` | 5714 | `commands/cluster/mod.rs:330` — ping-sent/pong-recv are `0 0` |
| `test_cluster_info_gossip_stats_nonzero` | 5742 | `commands/cluster/mod.rs:222` — `cluster_stats_messages_*` are `0` |

**What's needed:**

- **Replication offset (2 tests):** The WAL already tracks offsets internally. Expose the current
  WAL offset through cluster state so CLUSTER SHARDS and CLUSTER SLOTS can report it. This is
  likely the easiest of the 5 — just plumbing a value that already exists.

- **Ping/pong timestamps (1 test):** FrogDB uses Raft for consensus rather than Redis-style gossip
  with ping/pong. Need to either (a) add timestamp tracking in the Raft health-check layer, or
  (b) decide these fields are N/A for FrogDB's architecture and update tests accordingly.

- **Link-state tracking (1 test):** When a node fails health checks, its link-state in CLUSTER
  NODES output should switch from `"connected"` to `"disconnected"`. Requires tracking node
  liveness state in the cluster metadata struct.

- **Gossip stats (1 test):** `cluster_stats_messages_{ping,pong}_{sent,received}` counters in
  CLUSTER INFO. Same architectural question as ping/pong — FrogDB doesn't use gossip protocol.
  May need to map these to Raft heartbeat equivalents or mark as unsupported.

**Key decision:** FrogDB uses Raft, not Redis gossip. Several of these stats (ping/pong, gossip
messages) may need to be reframed or explicitly documented as N/A rather than implemented 1:1.

---

## 2. CLUSTER RESET (2 tests)

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

## 3. CLUSTER FAILOVER FORCE (1 test)

**File:** `crates/server/tests/integration_cluster.rs`

| Test | Line |
|------|------|
| `test_cluster_failover_force_works_when_leader_unreachable` | 7020 |

**Current state:** `commands/cluster/admin.rs:241-317` already sends
`RaftClusterOp::Failover { force: true }` to the Raft layer.

**What's needed:** Verify the Raft implementation handles `force: true` correctly. FORCE failover
should proceed without leader acknowledgment (unilateral leader election). The test kills the
leader, then issues FAILOVER FORCE on a surviving node and expects it to become the new leader.
May already work — needs testing to confirm.

---

## 4. TRYAGAIN During Slot Migration (1 test)

**File:** `crates/server/tests/integration_cluster.rs`

| Test | Line |
|------|------|
| `test_mset_keys_in_migrating_slot_returns_tryagain` | 6211 |

**Current state:** `guards.rs:validate_cluster_slots()` handles MIGRATING/IMPORTING states
(lines 234-305) but has no TRYAGAIN path. The `Response` enum in `protocol/src/response.rs`
does not define a TRYAGAIN variant.

**What's needed:**

1. Add `Response::try_again(msg)` or use `Response::Error(b"-TRYAGAIN ...")`.
2. In `validate_cluster_slots()`: when a multi-key command targets a MIGRATING slot and some keys
   exist locally while others don't, return TRYAGAIN instead of serving locally or redirecting.
   The current code (lines 234-242) handles the MIGRATING case by serving locally and converting
   nil results to ASK — it doesn't detect the partial-presence scenario.
3. The test sets up migration with SETSLOT IMPORTING/MIGRATING, writes key1 to source but not key2,
   then issues MSET on both keys.

---

## 5. EVAL Shebang + allow-cross-slot (2 tests)

**File:** `crates/redis-regression/tests/cluster_scripting_regression.rs`

| Test | Line | Feature |
|------|------|---------|
| `no_cluster_flag_eval_with_shebang` | 82 | Parse `#!lua flags=no-cluster` in EVAL scripts |
| `allow_cross_slot_keys_flag` | 127 | Parse `#!lua flags=allow-cross-slot-keys` in EVAL scripts |

**Current state:** `parse_shebang()` in `scripting/src/parser.rs:20-81` extracts engine and library
name but **not flags**. It's only called from `load_library()` (FUNCTION LOAD path).
`ScriptExecutor::eval()` in `core/src/scripting/executor.rs:55` does not call `parse_shebang()`
at all — scripts are cached and executed without any flag parsing.

**Design decision needed:** In Redis 7, EVAL does **not** parse shebangs — shebang support is
exclusively for `FUNCTION LOAD`. These tests represent a FrogDB extension beyond Redis
compatibility. Options:

1. **Implement as extension:** Extend `parse_shebang()` to extract flags, call it from
   `ScriptExecutor::eval()`, wire flags into the execution context. ~5-10 files touched.
   - Extend `ShebangInfo` struct with `flags: Option<FunctionFlags>`
   - Parse `flags=...` in shebang line
   - In connection handler (`handlers/scripting.rs:27-79`): parse flags, use them to skip
     CROSSSLOT validation when `allow-cross-slot-keys` is set
   - Flag infrastructure exists: `FunctionFlags` in `scripting/src/function.rs:8-17` already
     defines `NO_WRITES`, `ALLOW_OOM`, `ALLOW_STALE`, `NO_CLUSTER`
   - `allow_cross_slot` config field exists in deps.rs

2. **Match Redis behavior:** Remove these tests. EVAL shebang is not part of Redis 7.

---

## 6. Evicted Keys Stat (1 test)

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

## 7. Active/Passive Expires Skipped During Pause (1 test)

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

## 8. Replication: Checkpoint SHA256 Verification (1 test)

**File:** `crates/server/tests/integration_replication.rs`

| Test | Line |
|------|------|
| `test_replica_checkpoint_sha256_not_verified` | 2516 |

**What's needed:** During full resync, the primary sends checkpoint data to the replica. Add a
SHA256 hash to the checkpoint header. The replica computes the hash over received data and
compares. On mismatch, trigger a new full resync instead of loading corrupt data. Currently the
test just verifies the replica is healthy after sync — no integrity checking exists.

---

## 9. Replication: Proactive Lag Threshold (1 test)

**File:** `crates/server/tests/integration_replication.rs`

| Test | Line |
|------|------|
| `test_proactive_lag_threshold_triggers_fullresync` | 2558 |

**What's needed:** Add a configurable `repl-lag-threshold-bytes` parameter. The primary monitors
each replica's acknowledged WAL offset. When the lag exceeds the threshold, proactively trigger
FULLRESYNC instead of waiting for WAL buffer overflow. Currently the WAL buffer silently overflows
and the replica only discovers the gap on its next PSYNC attempt.

---

## 10. PubSub Slot Migration Notification (1 test)

**File:** `crates/server/tests/integration_pubsub.rs`

| Test | Line |
|------|------|
| `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` | 730 |

**What's needed:** During slot migration, enumerate sharded pubsub (`SSUBSCRIBE`) subscribers for
channels whose slot is being migrated. Send `SUNSUBSCRIBE` notification to each affected
subscriber before completing migration. Currently the test body is essentially empty — just starts
a cluster and shuts down. Inspired by Redis `25-pubsubshard-slot-migration.tcl`.

---

## 11. Metrics Usage (1 test)

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

1. **CLUSTER FAILOVER FORCE** — may already work, just needs verification
2. **Evicted keys stat** — straightforward counter plumbing
3. **Active expires during pause** — localized flag threading
4. **TRYAGAIN migration** — small addition to existing slot validation
5. **EVAL shebang** — needs design decision first (Redis extension vs not)
6. **CLUSTER RESET** — moderate Raft work
7. **Cluster stats** — needs architectural decision (Raft vs gossip mapping)
8. **Metrics usage** — bulk instrumentation pass
9. **Replication features** — significant protocol work
10. **PubSub slot migration** — deep slot migration integration
