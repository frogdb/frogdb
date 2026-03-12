# Ignored Tests: Fix Plan

FrogDB originally had 51 ignored tests across 9 test files. These cover known bugs, stubbed
implementations, and missing features. This plan addresses all of them, grouped into independent
workstreams that can be tackled sequentially.

## Status (2026-03-11)

**Completed (28 tests addressed: 24 un-ignored, 4 stubs removed):**
- WS1: WATCH/EXEC dirty-flag rewrite — 6 tests fixed
- WS2: CLIENT PAUSE — 5 of 6 tests fixed (1 remaining: `active_passive_expires_skipped_during_pause`)
- WS3: Lua script timeout — 4 tests fixed (CONFIG SET lua-time-limit wired through Arc<AtomicU64>)
- WS4: OOM transaction tests — 3 tests implemented
- WS5: DEBUG set-active-expire — 4 stubs removed (feature punted; test active expiration differently)
- WS6: Cluster READONLY/READWRITE — 4 tests fixed
- WS12: Maxmemory stats — 2 of 3 tests fixed (OBJECT IDLETIME + FREQ; evicted_keys stat deferred)

**Skipped — deep infrastructure required (16 tests remaining):**
- WS7: Cluster stats & metadata (5 tests) — needs gossip layer, WAL offset exposure, ping/pong tracking
- WS8: CLUSTER RESET (2 tests) — Raft state clearing operations needed
- WS9: CLUSTER FAILOVER FORCE (1 test) — Raft force-failover semantics unverified
- WS10: TRYAGAIN during slot migration (1 test) — slot migration response handling
- WS11: EVAL shebang + allow-cross-slot (2 tests) — EVAL shebang is a Redis 7 extension, needs design decision
- WS12 remainder: evicted_keys stat (1 test) — cross-layer plumbing (shard→INFO)
- WS13: Replication features (2 tests) — SHA256 checkpoint verification + lag threshold
- WS14: PubSub slot migration (1 test) — subscriber notification on slot migration
- WS15: Metrics usage (1 test) — 27 of ~60 metrics defined but not wired up

---

## Workstream 1: WATCH/EXEC Dirty-Flag Rewrite (6 tests)

**Tests fixed:**
- `multi_regression.rs`: `exec_works_on_unwatched_key_not_modified` (L135), `after_successful_exec_key_no_longer_watched` (L237), `flushall_touches_watched_keys` (L396), `flushdb_touches_watched_keys` (L442), `flushall_watching_several_keys` (L487)
- Also fixes `blocking_commands_ignore_timeout_in_multi` (L520) — see step 6

**Root cause:** Current WATCH uses a per-shard version counter (`shard_version`). `get_key_version()` ignores the key parameter and returns the global shard version. This means ANY write on the shard dirties ALL watchers. Additionally, EXEC sends `CheckWatches` to only the shard running the transaction, not all shards with watched keys — so FLUSHALL on a different shard goes undetected.

**Approach:** Adopt a dirty-flag callback mechanism (like Redis/DragonflyDB). One `Arc<AtomicBool>` per WATCH session, shared across shards. Shards maintain a per-key watcher registry and set dirty flags on key mutation.

Redis uses `signalModifiedKey()` → `touchWatchedKey()` which iterates watching clients and sets
`CLIENT_DIRTY_CAS`. DragonflyDB uses `atomic_bool*` pointers stored per-shard per-key, set on
mutation. FrogDB already uses `Arc<AtomicBool>` for cross-thread shared state (`per_request_spans`,
`is_replica`), so this pattern fits naturally.

### Implementation

**Step 1: Create `WatchedKeys` registry** — new file `crates/core/src/shard/watch.rs`
```rust
pub struct WatchedKeys {
    // key → list of (conn_id, dirty_flag) pairs
    key_watchers: HashMap<Bytes, Vec<(u64, Arc<AtomicBool>)>>,
    // conn_id → list of watched keys (reverse index for cleanup)
    conn_keys: HashMap<u64, Vec<Bytes>>,
}
```
Methods: `register()`, `unregister_connection()`, `notify_key_modified()`, `notify_all_dirty()`, `is_empty()`

**Step 2: New ShardMessage variants** — `crates/core/src/shard/message.rs`
- Add `WatchKeys { keys, conn_id, dirty_flag: Arc<AtomicBool>, response_tx }`
- Add `UnwatchKeys { conn_id }`
- Modify `ExecTransaction` to carry `dirty_flag: Option<Arc<AtomicBool>>` instead of `watches: Vec<(Bytes, u64)>`
- Remove `GetVersion`

**Step 3: Handle messages in event loop** — `crates/core/src/shard/event_loop.rs`
- `WatchKeys` → call `self.watched_keys.register()` for each key
- `UnwatchKeys` → call `self.watched_keys.unregister_connection()`
- Extend `ConnectionClosed` to also unregister watches

**Step 4: Hook notifications into write paths**
- `crates/core/src/shard/pipeline.rs`: In `run_post_execution`, after version increment, call `self.watched_keys.notify_key_modified(key)` for each written key (guarded by `!self.watched_keys.is_empty()`)
- `crates/core/src/shard/execution.rs`:
  - `ScatterOp::FlushDb` → call `self.watched_keys.notify_all_dirty()`
  - `ScatterOp::MSet`, `Del/Unlink`, `CopySet` → notify per affected key
  - `execute_transaction` → check `dirty_flag.load(Acquire)` before executing; return `WatchAborted` if dirty
- Active expiry in `event_loop.rs` → notify on expired key deletion

**Step 5: Update connection-side** — `crates/server/src/connection/state.rs`, `handlers/transaction.rs`
- Replace `TransactionState.watches: HashMap<Bytes, (usize, u64)>` with `Option<WatchState>` containing `dirty: Arc<AtomicBool>`, `registered_shards: HashSet<usize>`, `watched_keys: HashMap<Bytes, usize>`
- `handle_watch`: Create `Arc<AtomicBool>`, send `WatchKeys` to each shard
- `handle_exec`: Check dirty flag locally (fast path), send `ExecTransaction` with dirty_flag. Shard also checks dirty_flag authoritatively to close the TOCTOU window.
- `handle_unwatch`/`handle_discard`/connection close: Send `UnwatchKeys` to registered shards

**Step 6: Allow blocking commands inside MULTI** — `crates/server/src/connection/dispatch.rs`
- Remove the `is_blocking_command` rejection inside `route_and_execute_with_transaction()`. Queue blocking commands like any other command. At EXEC time, execute them with timeout=0 (non-blocking).

**Step 7: Remove stale code** — Remove `get_key_version`, `check_watches`, `shard_version` (if unused elsewhere)

**Key files:**
- `crates/core/src/shard/watch.rs` (new)
- `crates/core/src/shard/worker.rs`
- `crates/core/src/shard/pipeline.rs`
- `crates/core/src/shard/execution.rs`
- `crates/core/src/shard/event_loop.rs`
- `crates/core/src/shard/message.rs`
- `crates/server/src/connection/state.rs`
- `crates/server/src/connection/handlers/transaction.rs`
- `crates/server/src/connection/dispatch.rs`

---

## Workstream 2: CLIENT PAUSE Fixes (6 tests)

**Tests fixed:**
- `pause_regression.rs`: `pause_starts_at_end_of_transaction` (L136), `scripts_blocked_by_pause_write` (L173), `ro_scripts_not_blocked_by_pause_write` (L205), `write_commands_paused_by_write_mode` (L298), `special_commands_paused_by_write_pfcount_publish` (L327), `active_passive_expires_skipped_during_pause` (L362)

### Bug A: CLIENT PAUSE inside MULTI executes immediately (L136)
**Root cause:** `wait_if_paused()` is called in `process_command()` (connection.rs L623) before transaction queuing. CLIENT is dispatched as a ConnectionLevel handler before the queue check.

**Fix:** Move `wait_if_paused()` from `process_command()` into `route_and_execute_with_transaction()` in `dispatch.rs`. Place it after AUTH/HELLO/pre-checks but BEFORE connection-level dispatch AND transaction queue check. Exempt only transaction-control commands (MULTI/EXEC/DISCARD/WATCH) from pause blocking. During MULTI, CLIENT PAUSE should be queued like any other command.

### Bug B: EVAL not blocked by PAUSE WRITE (L173)
**Root cause:** `EvalCommand` flags are `SCRIPT | NONDETERMINISTIC` — no `WRITE` flag. So `wait_if_paused()` skips it during PAUSE WRITE.

**Fix:** In `wait_if_paused()`, treat `CommandFlags::SCRIPT` as write-equivalent for PAUSE WRITE. All scripts are conservatively treated as writes (matches Redis).

### Bug C: EVAL_RO not implemented (L205)
**Fix:** Register `EVAL_RO` and `EVALSHA_RO` commands in `crates/server/src/commands/scripting.rs`. These are read-only variants that:
- Have `CommandFlags::READONLY | CommandFlags::SCRIPT` flags
- Execute identically to EVAL/EVALSHA but with a read-only execution context
- Should NOT be blocked by PAUSE WRITE

### Bug D: PFCOUNT/PUBLISH not paused by WRITE mode (L327)
**Root cause:** PFCOUNT has `READONLY` flag. PUBLISH has `PUBSUB` flag. Neither has `WRITE`. PUBLISH also bypasses `wait_if_paused` entirely since it routes through connection-level dispatch.

**Fix:** In `wait_if_paused()`, add explicit command name matching for special cases:
```rust
let is_write_for_pause = matches!(cmd_upper, b"PFCOUNT" | b"PUBLISH" | b"SPUBLISH")
    || is_write_command || is_script_command;
```

### Bug E: Active expires not skipped during pause (L362)
**Fix:** Thread `Arc<RwLock<PauseState>>` (or a simpler `Arc<AtomicBool>` pause flag) to `ShardWorker`. In `event_loop.rs`, skip `run_active_expiry()` when pause is active. Defer passive expiry pause to follow-up.

**Key files:**
- `crates/server/src/connection.rs` — remove `wait_if_paused` from `process_command`
- `crates/server/src/connection/dispatch.rs` — add `wait_if_paused` in correct location
- `crates/server/src/commands/scripting.rs` — add EVAL_RO/EVALSHA_RO
- `crates/core/src/shard/event_loop.rs` — skip active expiry during pause

---

## Workstream 3: Lua Script Timeout (4 tests)

**Tests fixed:**
- `multi_regression.rs`: `multi_and_script_timeout` (L583), `exec_and_script_timeout` (L589), `multi_exec_body_and_script_timeout` (L595), `just_exec_and_script_timeout` (L601)

**Current state:** The Lua timeout infrastructure is already built — `LuaVm` has `setup_timeout_hook()` checking every 10K instructions, `ScriptExecutor` has `kill_script()`, `SCRIPT KILL` command works. But `CONFIG SET lua-time-limit` is a no-op stub in `runtime_config.rs`.

### Implementation

**Step 1:** Add `Arc<AtomicU64>` for `lua_time_limit_ms` to `ConfigManager` — `crates/server/src/runtime_config.rs`
- Convert `lua-time-limit` from no-op to real mutable parameter
- Setter writes to the atomic; getter reads from it

**Step 2:** Thread the `Arc<AtomicU64>` to `LuaVm` — through shard builder → `ScriptExecutor` → `LuaVm`
- `crates/core/src/scripting/lua_vm.rs`: In `prepare_execution()`, read from shared atomic instead of `self.config.lua_time_limit_ms`

**Step 3:** Implement test bodies for the 4 `todo!()` tests — write actual test logic that:
- Sets a low `lua-time-limit` via CONFIG SET
- Runs a busy script inside MULTI/EXEC contexts
- Verifies scripts time out with `-BUSY` error

**Key files:**
- `crates/server/src/runtime_config.rs`
- `crates/core/src/scripting/lua_vm.rs`
- `crates/core/src/scripting/executor.rs`
- `crates/core/src/shard/builder.rs`

---

## Workstream 4: CONFIG SET maxmemory + OOM Tests (3 tests)

**Tests fixed:**
- `multi_regression.rs`: `exec_fails_with_queuing_error_oom` (L544), `discard_should_not_fail_during_oom` (L577), `exec_with_only_read_commands_not_rejected_when_oom` (L607)

**Current state:** `CONFIG SET maxmemory` already works. These tests are `todo!()` stubs.

### Implementation
Write actual test bodies:
1. `exec_fails_with_queuing_error_oom`: Set maxmemory to low value → MULTI → SET (should queue) → EXEC → verify EXECABORT/OOM error
2. `discard_should_not_fail_during_oom`: Same setup → MULTI → DISCARD → verify OK
3. `exec_with_only_read_commands_not_rejected_when_oom`: OOM → MULTI → GET → EXEC → verify succeeds

---

## Workstream 5: DEBUG set-active-expire (4 tests)

**Tests fixed:**
- `multi_regression.rs`: `exec_fails_on_lazy_expired_watched_key` (L552), `watch_stale_keys_should_not_fail_exec` (L559), `delete_watched_stale_keys_should_not_fail_exec` (L565), `flushdb_while_watching_stale_keys_should_not_fail_exec` (L571)

### Implementation

**Step 1:** Add `DEBUG SET-ACTIVE-EXPIRE <0|1>` subcommand — `crates/commands/src/generic.rs`
- Add matching arm in the DEBUG handler
- Add a shard-level flag `active_expire_enabled: Arc<AtomicBool>` (default true)
- Send message to all shards to toggle the flag

**Step 2:** Check the flag in `run_active_expiry()` — `crates/core/src/shard/event_loop.rs`
- Skip active expiry cycle when flag is false

**Step 3:** Implement `todo!()` test bodies — These test WATCH behavior with lazy-expired keys:
1. Disable active expire → SET x with short TTL → WATCH x → sleep past TTL → MULTI/EXEC
2. Verify that lazy expiry during EXEC correctly (or doesn't) abort the transaction

**Key files:**
- `crates/commands/src/generic.rs` — DEBUG handler
- `crates/core/src/shard/event_loop.rs` — active expiry check
- `crates/core/src/shard/worker.rs` — new flag field

---

## Workstream 6: Cluster READONLY/READWRITE (4 tests)

**Tests fixed:**
- `integration_cluster.rs`: `test_readonly_allows_reads_on_non_owner_node` (L5777), `test_readwrite_restores_moved_redirects` (L5833), `test_multi_exec_reads_succeed_on_replica_with_readonly` (L6657), `test_multi_exec_write_inside_readonly_session_returns_moved` (L6718)

### Implementation

**Step 1:** Add `readonly_mode: bool` to connection state — `crates/server/src/connection/state.rs`

**Step 2:** Make READONLY/READWRITE set the flag — `crates/server/src/commands/cluster/mod.rs` (L730-765)
- `ReadonlyCommand::execute()` → set `ctx.connection_state.readonly_mode = true`
- `ReadwriteCommand::execute()` → set to false

**Step 3:** Consult the flag in `validate_cluster_slots()` — `crates/server/src/connection/guards.rs` (L203-306)
- When `readonly_mode = true` AND command has `READONLY` flag AND slot is owned by a different node: allow the command to execute locally instead of returning MOVED
- When `readonly_mode = true` AND command has `WRITE` flag: still return MOVED

**Step 4:** Handle READONLY during MULTI/EXEC — ensure the readonly flag is consulted during transaction execution, not just single commands

**Key files:**
- `crates/server/src/connection/state.rs`
- `crates/server/src/commands/cluster/mod.rs`
- `crates/server/src/connection/guards.rs`

---

## Workstream 7: Cluster Stats & Metadata Stubs (5 tests)

**Tests fixed:**
- `integration_cluster.rs`: `test_cluster_shards_replication_offset_nonzero` (L5594), `test_cluster_slots_replication_offset_nonzero` (L5632), `test_cluster_nodes_link_state_tracks_failures` (L5669), `test_cluster_nodes_ping_pong_nonzero` (L5714), `test_cluster_info_gossip_stats_nonzero` (L5742)

### Implementation

**Replication offset (2 tests):** Track actual replication offset in cluster state. The WAL already tracks offsets — expose `wal_offset` through the cluster state and use it in CLUSTER SHARDS (L487, L513) and CLUSTER SLOTS (L555) responses. File: `crates/server/src/commands/cluster/mod.rs`

**Ping/pong timestamps (1 test):** Record timestamps of gossip ping/pong exchanges in cluster state. Update the CLUSTER NODES format string (L341) to use real values instead of `0 0`. File: `crates/server/src/commands/cluster/mod.rs`

**Link-state (1 test):** Track node connectivity state. When a node fails health checks, mark its link-state as "disconnected". File: `crates/server/src/commands/cluster/mod.rs` L341

**Gossip stats (1 test):** Add counters for `cluster_stats_messages_{ping,pong}_{sent,received}` in the cluster gossip layer. Expose in CLUSTER INFO (L240-245). File: `crates/server/src/commands/cluster/mod.rs`

**Key files:**
- `crates/server/src/commands/cluster/mod.rs` — all 5 fixes

---

## Workstream 8: CLUSTER RESET (2 tests)

**Tests fixed:**
- `integration_cluster.rs`: `test_cluster_reset_soft_clears_slot_assignments` (L6875), `test_cluster_reset_hard_clears_node_id` (L6903)

### Implementation
- `crates/server/src/commands/cluster/admin.rs` (L357-375): Replace no-op with actual logic
- SOFT: Clear slot assignments, remove all other nodes from cluster state, make this node a primary
- HARD: Also reset config epoch to 0 and generate a new node ID
- Likely requires new Raft operations for state clearing

---

## Workstream 9: CLUSTER FAILOVER FORCE (1 test)

**Tests fixed:**
- `integration_cluster.rs`: `test_cluster_failover_force_works_when_leader_unreachable` (L7024)

### Implementation
The command already sends a `RaftClusterOp::Failover { force: true }` to the Raft layer. Verify the Raft implementation handles the `force` flag correctly — FORCE should proceed without leader acknowledgment. File: `crates/server/src/commands/cluster/admin.rs` (L241-317) and the Raft failover handler.

---

## Workstream 10: TRYAGAIN During Slot Migration (1 test)

**Tests fixed:**
- `integration_cluster.rs`: `test_mset_keys_in_migrating_slot_returns_tryagain` (L6213)

### Implementation
In `validate_cluster_slots()` (`guards.rs` L203-306): When a multi-key command targets a migrating slot and some keys are present locally while others are not, return `TRYAGAIN` instead of `MOVED`. This signals the client to retry (the migration may complete soon).

---

## Workstream 11: Scripting — EVAL Shebang + allow-cross-slot (2 tests)

**Tests fixed:**
- `cluster_scripting_regression.rs`: `no_cluster_flag_eval_with_shebang` (L82), `allow_cross_slot_keys_flag` (L127)

### Implementation

**Shebang in EVAL (L82):** EVAL does not parse shebangs — this is correct per Redis 7 (shebangs are FUNCTION LOAD only). The test may need updating to reflect this as expected behavior, OR we implement shebang parsing in EVAL as an extension. **Clarification needed from test intent.**

**allow-cross-slot-keys (L127):** The flag infrastructure already exists (`allow_cross_slot` in deps.rs). The test expects that a script with `#!lua flags=allow-cross-slot-keys` shebang enables cross-slot access. Since shebang parsing in EVAL isn't supported (see above), this depends on the shebang decision.

---

## Workstream 12: Maxmemory Stats (3 tests)

**Tests fixed:**
- `maxmemory_regression.rs`: `evicted_keys_stat_tracked` (L484), `object_idletime_tracks_lru` (L519), `object_freq_tracks_lfu` (L538)

### Implementation

**evicted_keys (L484):** The eviction subsystem already runs. Add an atomic counter that increments on each eviction. Expose it in INFO stats instead of hardcoded `0`. Files: `crates/server/src/commands/info.rs` (L330), eviction module.

**OBJECT IDLETIME (L519):** Track last-access timestamp per key in the store. `KeyMetadata` likely already has space for this. Return `(now - last_access).as_secs()` instead of `0`. File: `crates/commands/src/generic.rs` (L395-407).

**OBJECT FREQ (L538):** Track LFU frequency counter per key when `maxmemory-policy` is an LFU variant. Return the counter instead of `0`. File: `crates/commands/src/generic.rs` (L382-394).

---

## Workstream 13: Replication Features (2 tests)

**Tests fixed:**
- `integration_replication.rs`: `test_replica_checkpoint_sha256_not_verified` (L2516), `test_proactive_lag_threshold_triggers_fullresync` (L2558)

### Implementation

**SHA256 verification (L2516):** During full resync, primary sends checkpoint data. Add SHA256 hash to checkpoint header. Replica computes hash over received data and compares. On mismatch, trigger a new full resync.

**Proactive lag threshold (L2558):** Add configurable `repl-lag-threshold-bytes` parameter. Primary monitors replica's acknowledged offset. When lag exceeds threshold, proactively trigger FULLRESYNC instead of waiting for WAL buffer overflow.

---

## Workstream 14: PubSub Slot Migration (1 test)

**Tests fixed:**
- `integration_pubsub.rs`: `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` (L730)

### Implementation
During slot migration, enumerate sharded pubsub subscribers for channels in the migrating slot range. Send `SUNSUBSCRIBE` notification to each subscriber before completing migration. File: slot migration handler + pubsub state.

---

## Workstream 15: Metrics Usage (1 test)

**Tests fixed:**
- `metrics_usage.rs`: `all_metrics_are_used` (L19)

### Implementation
Audit all 60+ metrics in `crates/telemetry/src/definitions.rs`. Either:
- Wire unused metrics into their corresponding code paths (e.g., `EvictionKeysTotal`, `BlockedClients`, etc.)
- Remove metrics that are no longer planned

---

## Verification

After each workstream:
1. Un-ignore the fixed tests
2. `just test <crate> <test_name>` for each specific test
3. `just lint <crate>` — zero warnings
4. `just test <crate>` — full crate test suite passes

Final verification:
```bash
just build && just lint && just test
```

---

## Suggested Priority Order

1. **Workstream 2** (CLIENT PAUSE) — mostly localized bug fixes
2. **Workstream 1** (WATCH/EXEC) — foundational correctness fix
3. **Workstream 3** (Lua timeout) — infrastructure already exists
4. **Workstream 4** (OOM tests) — just writing test bodies
5. **Workstream 5** (DEBUG set-active-expire) — small feature + test bodies
6. **Workstream 6** (READONLY/READWRITE) — important cluster feature
7. **Workstream 12** (maxmemory stats) — wiring up existing counters
8. **Workstream 7** (cluster stats) — exposing real values
9. **Workstream 15** (metrics) — cleanup
10. **Workstreams 8-14** — larger features, lower priority
