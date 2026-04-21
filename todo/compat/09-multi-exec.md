# 9. MULTI/EXEC Enhancements (17 tests — `multi_tcl.rs`)

**Status**: MULTI/EXEC core implemented; missing OOM handling in EXEC, replication verification,
script timeout interaction, stale-key WATCH behavior. Regression tests in `multi_regression.rs`
already cover OOM-in-EXEC, script timeout, and read-only OOM exemption (7 of 17 tests).

**Scope**: Fill gaps in transaction behavior across five sub-features.

**Source files** (primary):

| Area | File |
|------|------|
| EXEC dispatch | `frogdb-server/crates/server/src/connection/handlers/transaction.rs` |
| Shard transaction execution | `frogdb-server/crates/core/src/shard/execution.rs` |
| Post-execution pipeline | `frogdb-server/crates/core/src/shard/pipeline.rs` |
| OOM / eviction check | `frogdb-server/crates/core/src/shard/eviction.rs` |
| Command flags | `frogdb-server/crates/core/src/command.rs` (`CommandFlags`) |
| WATCH state | `frogdb-server/crates/server/src/connection/state.rs` (`TransactionState`) |
| Shard version / check_watches | `frogdb-server/crates/core/src/shard/worker.rs` |
| Replication broadcast | `frogdb-server/crates/core/src/shard/pipeline.rs` (step 6) |
| Replication traits | `frogdb-server/crates/replication/src/lib.rs` (`ReplicationBroadcaster`) |
| Scripting config | `frogdb-server/crates/core/src/scripting/config.rs` (`ScriptingConfig`) |
| Scripting execution | `frogdb-server/crates/core/src/shard/scripting.rs` |
| Memory config | `frogdb-server/crates/config/src/memory.rs` (`MemoryConfig`) |
| Runtime config | `frogdb-server/crates/server/src/runtime_config.rs` (`ConfigManager`) |
| Connection dispatch | `frogdb-server/crates/server/src/connection/dispatch.rs` |
| Regression tests | `frogdb-server/crates/redis-regression/tests/multi_regression.rs` |
| TCL-ported tests | `frogdb-server/crates/redis-regression/tests/multi_tcl.rs` |

---

## Architecture

### OOM check placement in EXEC flow

Redis checks OOM per-command inside EXEC: write commands that would exceed `maxmemory` return an
OOM error in the response array, but read-only commands proceed normally. The transaction is NOT
atomically aborted -- each command's OOM status is independent.

FrogDB already has the right mechanism. In `ShardWorker::execute_command_inner()` (execution.rs
line 61-65):

```rust
let is_write = handler.flags().contains(crate::command::CommandFlags::WRITE);
if is_write && let Err(err) = self.check_memory_for_write() {
    return (err.to_response(), None);
}
```

This check runs for every command inside `execute_transaction()` because the transaction loop calls
`execute_command_inner()` per queued command. So when EXEC runs a write command while OOM, it
returns an OOM error in that slot of the result array. Read-only commands skip the OOM check and
succeed normally.

**Current status**: Already works. The `multi_regression.rs` tests
`exec_fails_with_queuing_error_oom` and `exec_with_only_read_commands_not_rejected_when_oom`
validate this behavior. The remaining task is to port these tests to the `multi_tcl.rs` TCL
regression suite format with the exact Redis test names.

### WATCH stale-key handling

Redis's WATCH tests for "stale keys" use `DEBUG SET-ACTIVE-EXPIRE 0` to disable background
expiration, then set a key with a short TTL, wait for it to expire, and verify that WATCH on the
now-stale (logically expired but not yet reclaimed) key does not cause EXEC to fail.

FrogDB's WATCH uses a shard-level monotonic version counter (`shard_version: u64` in
`ShardWorker`). `get_key_version()` returns `self.shard_version` regardless of the key -- it is a
global shard version, not per-key. When any write increments the version, ALL watches on that shard
are invalidated.

The stale-key tests require:
1. A way to disable active/background expiration (equivalent of `DEBUG SET-ACTIVE-EXPIRE 0`)
2. The WATCH mechanism must distinguish between "key was modified by a user command" and "key was
   passively expired by the TTL subsystem"

In Redis, WATCH tracks per-key versions. A stale key that expires passively (lazy expiration on
access) is treated as "not modified" for WATCH purposes -- the key's version doesn't change when it
silently expires. FrogDB's shard-level version doesn't have this distinction.

**Implementation approach**: FrogDB does not need `DEBUG SET-ACTIVE-EXPIRE` to make these tests
work. The underlying requirement is: expired keys that are never touched should not dirty WATCH.
Since FrogDB's version is shard-global, any background expiry sweep that deletes keys will bump the
version and break all watches. The fix paths are:

- **Option A (targeted)**: Skip `increment_version()` when the expiry subsystem removes keys
  (distinguish expiry-driven deletes from user-driven deletes). This is simple but means watches
  won't detect expiry-driven changes, matching Redis behavior.
- **Option B (per-key versioning)**: Track per-key versions in the store. This is more correct but
  significantly more complex and memory-intensive.
- **Recommended**: Option A. Add a `skip_version_increment` flag or use a separate code path for
  TTL expiry so it doesn't bump `shard_version`. Then the stale-key WATCH tests will pass because
  expired keys don't change the shard version.

### Script timeout state machine

Redis's script timeout (`lua-time-limit`) interaction with MULTI works as follows:
- A running script that exceeds `lua-time-limit` enters a "busy" state where the server rejects
  most commands with `BUSY` errors
- MULTI is rejected during busy state; EXEC is rejected during busy state
- Non-script commands inside MULTI are unaffected by the lua-time-limit config

FrogDB already has:
- `ScriptingConfig` with `lua_time_limit_ms` (default 5000ms) and runtime override via
  `AtomicU64`
- `CONFIG SET lua-time-limit` support
- Script timeout hook in the Lua VM that terminates long-running scripts with `BUSY` errors
- SCRIPT KILL / FUNCTION KILL support via shard messages

**Current status**: Already works. The `multi_regression.rs` tests `multi_and_script_timeout`,
`exec_and_script_timeout`, and `just_exec_and_script_timeout` validate this. The remaining task is
to port these to TCL regression format.

### Replication propagation in MULTI

Redis tests verify that specific commands inside MULTI propagate correctly to the replication
stream. The tests use `WAIT` to verify replication and inspect the replica's state after EXEC.

FrogDB's replication uses an async WAL streaming model (PSYNC2-compatible). The post-execution
pipeline in `run_transaction_post_execution()` (pipeline.rs line 271-279) wraps all write commands
in MULTI/EXEC framing via `broadcast_transaction()`:

```rust
if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
    let commands: Vec<(&str, &[Bytes])> = write_infos
        .iter()
        .map(|&(handler, args)| (handler.name(), args))
        .collect();
    self.replication_broadcaster.broadcast_transaction(&commands);
}
```

The `broadcast_transaction()` default implementation (replication/src/lib.rs line 99-105) sends
MULTI, each command, then EXEC. This works for standard data commands.

Commands that need verification:
- **EVAL**: Scripts are connection-level commands, executed in
  `execute_connection_level_in_transaction()`. They don't go through the shard transaction path, so
  they are NOT included in the replication broadcast. This is correct -- Redis replicates the
  side-effects of EVAL, not the EVAL itself, via command propagation.
- **PUBLISH**: Also connection-level. PubSub messages are not replicated in FrogDB (they use a
  separate broadcast mechanism). This differs from Redis where PUBLISH propagates to replicas.
- **SCRIPT LOAD/FLUSH**: Connection-level. Script cache operations need to propagate to replicas so
  EVALSHA works on replicas.
- **XREADGROUP**: A shard-level command that may create consumer groups as a side effect. The
  command itself should propagate normally through the transaction pipeline.

### Config error handling

Redis's `MULTI with config error` test verifies that when a CONFIG SET inside MULTI returns an
error, the EXEC result array contains the error at the correct position. FrogDB handles CONFIG as a
connection-level command inside transactions (via `execute_connection_level_in_transaction`), so the
error response is correctly placed in the merged result array.

---

## Implementation Steps

### Sub-feature 1: OOM handling in EXEC (2 tests)

**Status**: Already implemented and tested in `multi_regression.rs`. Tests need to be added to
`multi_tcl.rs`.

1. **Port tests to `multi_tcl.rs`**: Add the two TCL-format tests using single-shard
   `TestServer` with `CONFIG SET maxmemory 1` to trigger OOM.
   - File: `frogdb-server/crates/redis-regression/tests/multi_tcl.rs`
   - Remove the exclusion comments for these tests from the file header

### Sub-feature 2: Replication verification (8 tests)

**Status**: Requires primary+replica test infrastructure and command-specific verification.

1. **Create replication test helpers for MULTI**: Add helper functions to
   `frogdb-server/crates/redis-regression/tests/multi_tcl.rs` (or a shared module) that:
   - Start a primary+replica pair using `start_primary_replica_pair()`
   - Execute MULTI/EXEC on the primary
   - Call `wait_for_replication()` to ensure propagation
   - Verify the replica's state matches
   - Reference: `frogdb-server/crates/server/tests/integration_replication.rs` for patterns

2. **Implement `MULTI propagation of EVAL`**: Run EVAL inside MULTI on primary, verify side
   effects appear on replica. EVAL writes propagate via command rewriting (the actual SET/DEL
   commands issued by the script, not the EVAL itself).

3. **Implement `MULTI propagation of PUBLISH`**: In Redis, PUBLISH inside MULTI propagates to
   replicas. In FrogDB, PUBLISH is broadcast via the PubSub system, not the replication stream.
   This test needs adaptation -- either:
   - Propagate PUBLISH through the replication stream (new feature)
   - Mark as intentional incompatibility with documented reasoning

4. **Implement `MULTI propagation of SCRIPT LOAD`**: SCRIPT LOAD inside MULTI should propagate
   the script to replicas so EVALSHA works. Requires adding SCRIPT LOAD to the replication
   broadcast path.
   - File: `frogdb-server/crates/server/src/connection/handlers/scripting/script.rs`
   - After loading a script, broadcast `SCRIPT LOAD <script>` to replicas

5. **Implement `MULTI propagation of SCRIPT FLUSH`**: SCRIPT FLUSH inside MULTI should clear the
   script cache on replicas.

6. **Implement `MULTI propagation of XREADGROUP`**: XREADGROUP creates consumer groups as a side
   effect. Verify the consumer group and entries appear on replica after EXEC.
   - This should already work since XREADGROUP is a shard-level command in the transaction path

7. **Implement `MULTI with $cmd` inner-command propagation matrix**: Parameterized test that runs
   various commands (SET, DEL, INCR, LPUSH, etc.) inside MULTI and verifies propagation.

8. **Implement state-change tests** (`exec with write commands and state change`,
   `exec with read commands and stale replica state change`): These test Redis's
   `min-replicas-to-write` behavior. FrogDB adaptation: verify that Raft commit ensures writes
   inside MULTI are durable before EXEC returns. May need to adapt to test FrogDB's replication
   guarantees rather than Redis's exact `min-replicas-to-write` semantics.

### Sub-feature 3: WATCH with stale keys (3 tests)

**Status**: Requires changes to TTL expiry path to avoid bumping shard version.

1. **Add `active_expire_enabled` flag to shard worker**: Add a boolean flag (default `true`) to
   `ShardWorker` that controls whether background TTL expiry increments the shard version.
   - File: `frogdb-server/crates/core/src/shard/worker.rs`
   - Add field: `pub(crate) active_expire_enabled: bool`

2. **Implement `DEBUG SET-ACTIVE-EXPIRE` command**: Add a DEBUG subcommand that sets the flag.
   - File: `frogdb-server/crates/server/src/connection/handlers/debug.rs`
   - Add handler for `DEBUG SET-ACTIVE-EXPIRE 0|1`

3. **Skip version increment for TTL-driven deletes**: In the TTL expiry code path, check
   `active_expire_enabled` before calling `increment_version()`. When disabled, expired keys are
   still removed but the shard version is not bumped.
   - File: Find the TTL expiry tick in `frogdb-server/crates/core/src/shard/event_loop.rs` or
     equivalent
   - Modify to conditionally skip `increment_version()`

4. **Alternative approach (simpler)**: Instead of implementing DEBUG SET-ACTIVE-EXPIRE, make the
   WATCH stale-key behavior work by default. When a key expires (either actively or lazily),
   treat the version as unchanged for WATCH purposes. This requires separating "expiry version"
   from "mutation version" at the shard level.
   - More robust: Always skip version increment for TTL expiry (not just when debug flag is set)
   - This matches Redis's actual behavior where passive expiry does not dirty watches

5. **Port tests**: Add the three stale-key WATCH tests to `multi_tcl.rs`:
   - Set key with short TTL, disable active expire, WATCH, wait for expiry, MULTI/EXEC
   - DEL a watched stale key (already expired), verify EXEC succeeds
   - FLUSHDB while watching stale keys, verify EXEC succeeds

### Sub-feature 4: Script timeout interaction (3 tests)

**Status**: Already implemented and tested in `multi_regression.rs`. Tests need to be ported.

1. **Port tests to `multi_tcl.rs`**: Add the three script timeout tests using `CONFIG SET
   lua-time-limit`.
   - File: `frogdb-server/crates/redis-regression/tests/multi_tcl.rs`
   - Remove the exclusion comments for these tests from the file header

### Sub-feature 5: Config error handling (1 test)

**Status**: Likely already works; needs test.

1. **Add test**: Queue `CONFIG SET` with an invalid value inside MULTI, verify EXEC returns the
   error at the correct position in the result array.
   - File: `frogdb-server/crates/redis-regression/tests/multi_tcl.rs`
   - Pattern: `MULTI -> CONFIG SET invalid-param value -> SET x 1 -> EXEC`
   - Verify: result[0] is error, result[1] is OK

---

## Integration Points

### OOM check in EXEC

The OOM check is already correctly placed per-command in `execute_command_inner()`. No changes
needed to the EXEC flow. The check happens at:

```
handle_exec() [transaction.rs]
  -> ShardMessage::ExecTransaction
    -> ShardWorker::execute_transaction() [execution.rs]
      -> execute_command_inner() per command [execution.rs:252]
        -> check_memory_for_write() [eviction.rs]  <-- OOM check here
```

### Replication hooks

Replication broadcast happens in the post-execution pipeline. For transactions:

```
execute_transaction() [execution.rs]
  -> run_transaction_post_execution() [pipeline.rs:209]
    -> replication_broadcaster.broadcast_transaction() [pipeline.rs:272-279]
      -> broadcast_command("MULTI", &[])
      -> broadcast_command(cmd_name, args) per write command
      -> broadcast_command("EXEC", &[])
```

Connection-level commands (EVAL, PUBLISH, SCRIPT LOAD) bypass this path because they are executed
in `execute_connection_level_in_transaction()` on the connection handler, not the shard. To
propagate them, either:
- Have the connection handler send a separate replication message after execution
- Move script side-effect replication to the shard's post-execution path

### WATCH dirty detection

WATCH uses shard-global version tracking:

```
WATCH key -> get shard version -> store (key, shard_id, version)
EXEC -> send watches to shard -> check_watches() compares stored version vs current
```

- `increment_version()` is called in `run_post_execution()` step 2 (per-command) and
  `run_transaction_post_execution()` step 1 (once per transaction)
- `check_watches()` compares stored version against `get_key_version()` which returns
  `self.shard_version`
- Any write on the shard bumps the version, invalidating ALL watches on that shard
- TTL expiry also bumps the version (this is the stale-key problem)

---

## FrogDB Adaptations

### Raft replication verification (vs Redis replication stream)

Redis tests use `WAIT <numreplicas> <timeout>` to verify replication. FrogDB's replication is
WAL-based streaming (PSYNC2-compatible), not Raft-based for data replication. The `WAIT` command
should work similarly -- block until replicas have acknowledged the replication offset.

For the replication verification tests:
- Use `wait_for_replication()` helper from `integration_replication.rs` which polls replica state
- Alternatively, implement/use `WAIT` command if not already available
- Test infrastructure: `start_primary_replica_pair()` from test helpers

### Stale keys with RocksDB TTL

FrogDB's TTL expiry works differently from Redis:
- In-memory store (`HashMapStore`): Background tick checks expired keys periodically
- With RocksDB: Keys may have TTL set in RocksDB compaction filters

The stale-key fix (skip version increment on TTL expiry) must apply to both paths:
- In-memory: Modify the background expiry tick to not call `increment_version()`
- RocksDB: Compaction-driven expiry happens outside the shard worker, so it shouldn't affect
  `shard_version` at all (verify this)

### PUBLISH replication

Redis replicates PUBLISH to replicas so that replica-connected subscribers receive messages.
FrogDB uses a separate PubSub broadcast infrastructure. The replication tests for PUBLISH inside
MULTI may need adaptation:
- If FrogDB intends to support pub/sub on replicas, PUBLISH must propagate through replication
- If not, mark the PUBLISH propagation test as intentional incompatibility

---

## Test List

### OOM handling (2 tests)

Reject write commands in EXEC when OOM, allow read-only.

- `EXEC with at least one use-memory command should fail`
- `EXEC with only read commands should not be rejected when OOM`

### Replication verification (8 tests -- *adapt*)

Verify EVAL, PUBLISH, SCRIPT LOAD/FLUSH, XREADGROUP propagate correctly through replication;
test on primary+follower topology.

- `MULTI propagation of EVAL`
- `MULTI propagation of PUBLISH`
- `MULTI propagation of SCRIPT LOAD`
- `MULTI propagation of SCRIPT FLUSH`
- `MULTI propagation of XREADGROUP`
- `MULTI with $cmd` -- inner-command propagation matrix
- `exec with write commands and state change`
- `exec with read commands and stale replica state change`

### WATCH with stale keys (3 tests)

Implement `DEBUG SET-ACTIVE-EXPIRE` equivalent; ensure expired watched keys don't fail EXEC.

- `WATCH stale keys should not fail EXEC`
- `Delete WATCHed stale keys should not fail EXEC`
- `FLUSHDB while watching stale keys should not fail EXEC`

### Script timeout (3 tests)

Configurable lua-time-limit; MULTI interaction with script timeout state.

- `MULTI and script timeout`
- `EXEC and script timeout`
- `just EXEC and script timeout`

### Config error handling (1 test -- *adapt*)

- `MULTI with config error` -- test against FrogDB config error behavior

---

## Verification

### Running existing regression tests

```bash
# Run existing multi_regression.rs tests (OOM, script timeout already pass)
just test frogdb-server multi_regression

# Run existing multi_tcl.rs tests (core MULTI/EXEC behavior)
just test redis-regression multi_tcl
```

### After implementation

```bash
# Run all MULTI tests
just test redis-regression multi_tcl
just test frogdb-server multi_regression

# Run replication tests (after adding replication verification)
just test frogdb-server integration_replication

# Verify no regressions in related areas
just test redis-regression maxmemory_tcl
just test redis-regression scripting_tcl
```

### Manual verification checklist

- [ ] OOM: Set maxmemory=1, MULTI+SET+EXEC returns OOM error; MULTI+GET+EXEC succeeds
- [ ] WATCH stale: Set key with 1s TTL, WATCH, wait 2s, MULTI+PING+EXEC succeeds
- [ ] Script timeout: CONFIG SET lua-time-limit 1, MULTI works, EVAL busy script times out
- [ ] Replication: Primary MULTI+SET+EXEC, replica has the key after WAIT
- [ ] Config error: MULTI+CONFIG SET bad-param+EXEC returns error in correct array position
