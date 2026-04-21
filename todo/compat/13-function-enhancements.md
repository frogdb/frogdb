# 13. FUNCTION Enhancements (6 tests — `functions_tcl.rs`)

**Status**: FUNCTION LOAD/DELETE/FLUSH/LIST/STATS/DUMP/RESTORE/KILL implemented; OOM enforcement
and stale-follower behavior not yet wired up.

**Source files**:
- `frogdb-server/crates/server/src/connection/handlers/scripting/function.rs` — connection-level FCALL/FUNCTION dispatch
- `frogdb-server/crates/core/src/shard/functions.rs` — shard-level `handle_function_call` (FCALL execution)
- `frogdb-server/crates/core/src/shard/eviction.rs` — `check_memory_for_write()` and `is_over_memory_limit()`
- `frogdb-server/crates/core/src/shard/execution.rs` — standard command pipeline with OOM check (line 64)
- `frogdb-server/crates/scripting/src/function.rs` — `FunctionFlags` bitflags (`ALLOW_OOM`, `ALLOW_STALE`)
- `frogdb-server/crates/scripting/src/persistence.rs` — DUMP/RESTORE binary format and file persistence
- `frogdb-server/crates/server/src/commands/function.rs` — `FcallCommand` / `FcallRoCommand` flag declarations
- `frogdb-server/crates/server/src/connection/guards.rs` — pre-execution checks (replica write-block, quorum fencing)
- `frogdb-server/crates/server/src/server/init.rs` — startup function restore from `functions.fdb`

---

## Architecture

### OOM Check Placement for Functions

The standard command pipeline (`execute_command_inner` in `shard/execution.rs`) calls
`check_memory_for_write()` before any command that has the `WRITE` flag. However, FCALL/FCALL_RO
bypass this pipeline entirely — they are handled at the connection level and dispatched via
`ShardMessage::FunctionCall` to `handle_function_call` in `shard/functions.rs`, which does **not**
call `check_memory_for_write()`.

Redis behavior:
- FCALL with a function that can write (no `no-writes` flag): reject if OOM, unless the function
  has `allow-oom` flag.
- FCALL with a `no-writes` function: always allow (reads never trigger OOM rejection).
- FCALL_RO: always allow (read-only path).

### Stale-Follower Flag Propagation

In Redis, `replica-serve-stale-data` controls whether a disconnected replica serves reads. Functions
with `allow-stale` bypass this restriction. FrogDB uses Raft followers instead of Redis replicas.
The analogous state is a Raft follower that has lost contact with the leader.

Currently, FrogDB has:
- `is_replica` flag + `READONLY` error in `guards.rs` (blocks writes on replicas).
- `CommandFlags::STALE` on `FcallCommand` and `FcallRoCommand` (marking them as allowed on stale
  replicas at the command metadata level).
- No runtime enforcement yet of stale state gating for function execution based on individual
  function flags.

### Persistence Model

Functions are persisted to `{data_dir}/functions.fdb` using a custom binary format
(`frogdb-scripting::persistence`). On startup, `init.rs` loads from this file. DUMP/RESTORE uses
the same binary format (FNV-1a checksummed, version-tagged). This differs from Redis which
serializes functions inside the RDB file.

---

## Implementation Steps

### Step 1: Add OOM check to `handle_function_call`

**File**: `frogdb-server/crates/core/src/shard/functions.rs`

After resolving the function from the registry and before executing, add an OOM guard:

```rust
// After determining effective_read_only and before executor.execute_function():
if !effective_read_only && !function.flags.contains(FunctionFlags::ALLOW_OOM) {
    if let Err(err) = self.check_memory_for_write() {
        return err.to_response();
    }
}
```

Logic:
- If `effective_read_only` (FCALL_RO or `no-writes` function): skip OOM check (reads always pass).
- If function has `allow-oom`: skip OOM check.
- Otherwise: call `check_memory_for_write()`, which triggers eviction or returns
  `CommandError::OutOfMemory`.

### Step 2: Add stale-follower check to function execution

**File**: `frogdb-server/crates/core/src/shard/functions.rs`

After resolving the function, check if the shard/node is in a stale follower state. If so, reject
unless the function has `ALLOW_STALE`:

```rust
// After resolving function and before executing:
if self.is_stale_follower() && !function.flags.contains(FunctionFlags::ALLOW_STALE) {
    return Response::error(
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
    );
}
```

This requires implementing `is_stale_follower()` on `ShardWorker` (or piping the stale state from
the Raft layer). The stale determination would check:
- Node is a Raft follower AND
- Leader heartbeat has timed out (no recent AppendEntries from leader)

**New helper** (suggested location: `frogdb-server/crates/core/src/shard/worker.rs` or a new
`shard/stale.rs`):

```rust
fn is_stale_follower(&self) -> bool {
    // Return true if this node is a follower that has lost contact with leader
    self.identity.is_replica.load(Ordering::Relaxed)
        && self.cluster.raft.as_ref()
            .is_some_and(|raft| raft.is_leader_stale())
}
```

### Step 3: Add `is_leader_stale()` to Raft interface

**File**: `frogdb-server/crates/cluster/src/raft/...` (Raft module)

Add a method that returns `true` when the follower has not received a heartbeat from the leader
within the configured election timeout. This is the equivalent of Redis's
`replica-serve-stale-data` check.

### Step 4: Verify DUMP/RESTORE round-trip

The DUMP/RESTORE format is already implemented in `scripting/src/persistence.rs` with unit tests.
The compatibility test needs to verify the end-to-end flow through the FUNCTION DUMP and FUNCTION
RESTORE commands, including:
- Checksums survive network round-trip
- APPEND/REPLACE/FLUSH policies work correctly
- Error on corrupted payloads

### Step 5: Verify persistence across restart

`persist_functions()` in `function.rs` saves to `{data_dir}/functions.fdb` on every LOAD/DELETE/
FLUSH/RESTORE. On startup, `init.rs` loads from this file. The "debug reload" tests in Redis test
this via `DEBUG RELOAD`, which FrogDB doesn't support. Instead, the tests should:
- Load functions
- Stop the server
- Restart the server
- Verify functions are still registered

---

## Integration Points

### Where function execution checks OOM

```
handle_fcall (connection handler)
  -> ShardMessage::FunctionCall
    -> ShardWorker::handle_function_call (shard/functions.rs)
      -> [NEW] check ALLOW_OOM flag + check_memory_for_write()
      -> executor.execute_function()
```

### Where stale flag is evaluated

```
handle_fcall (connection handler)
  -> ShardMessage::FunctionCall
    -> ShardWorker::handle_function_call (shard/functions.rs)
      -> [NEW] check is_stale_follower() + ALLOW_STALE flag
      -> executor.execute_function()
```

### Standard command OOM check (for comparison)

```
execute_command_inner (shard/execution.rs:60-66)
  -> handler.flags().contains(WRITE) => check_memory_for_write()
```

---

## FrogDB Adaptations

### Stale Raft Followers vs Redis Stale Replicas

| Aspect | Redis | FrogDB |
|--------|-------|--------|
| Replication model | Async streaming replication | Raft consensus |
| "Stale" condition | Replica disconnected from master | Follower lost leader heartbeat |
| Config knob | `replica-serve-stale-data yes/no` | Raft election timeout (implicit) |
| Error message | `MASTERDOWN Link with MASTER is down...` | Same message for compat |
| `allow-stale` semantics | Bypass stale-data rejection | Bypass stale-follower rejection |

FrogDB adaptation: The `allow-stale` flag permits function execution on a Raft follower that has
lost its leader, analogous to Redis allowing stale data reads on disconnected replicas.

### RocksDB Function Persistence vs RDB

| Aspect | Redis | FrogDB |
|--------|-------|--------|
| Storage format | Inside RDB file | Separate `functions.fdb` file |
| Serialization | RDB-specific encoding | Custom binary (FDBF magic, FNV-1a checksum) |
| Trigger | RDB save / BGSAVE | Every LOAD/DELETE/FLUSH/RESTORE |
| Startup restore | RDB load | `load_from_file()` in `init.rs` |
| DEBUG RELOAD | Supported | Not supported (use server restart) |

The DUMP/RESTORE wire format is FrogDB's own (not Redis-compatible). This is intentional — FrogDB
clients should use FrogDB's format. Cross-system migration would require a conversion tool.

---

## Test List

### OOM enforcement (2 tests)

Reject function execution when OOM (except read-only or `allow-oom` functions).

- `FUNCTION - deny oom` — *adapt*: use `CONFIG SET maxmemory` to trigger OOM, call a write function, expect OOM error
- `FUNCTION - deny oom on no-writes function` — *adapt*: same OOM setup, call a `no-writes` function, expect success (reads bypass OOM)

### Stale replica (1 test — *adapt*)

Allow `allow-stale` flagged functions on stale Raft followers.

- `FUNCTION - allow stale` — *adapt*: simulate stale follower state, verify function with `allow-stale` flag executes while function without it is rejected with MASTERDOWN

### DUMP/RESTORE (1 test — *adapt*)

Verify FrogDB serialization format round-trips correctly.

- `FUNCTION - test function dump and restore` — *adapt*: load libraries, DUMP, FLUSH, RESTORE, verify functions work; test APPEND/REPLACE/FLUSH policies

### Server restart (2 tests — *adapt*)

Verify functions persist across RocksDB restart (FrogDB equivalent of DEBUG RELOAD).

- `FUNCTION - test debug reload different options` — *adapt*: load functions, restart server, verify functions survived; test with different persistence configs
- `FUNCTION - test debug reload with nosave and noflush` — *adapt*: load functions, restart server with nosave equivalent, verify behavior matches expectations

---

## Verification

1. **OOM tests**: `just test frogdb-server "function.*deny_oom"`
   - Requires test harness support for `CONFIG SET maxmemory` to constrain memory
   - Load a write function and a `no-writes` function
   - Fill memory to trigger OOM
   - Verify write function is rejected, read function succeeds

2. **Stale tests**: `just test frogdb-server "function.*allow_stale"`
   - Requires test harness support for simulating a stale Raft follower
   - Load functions with and without `allow-stale` flag
   - Trigger stale state
   - Verify `allow-stale` function runs, other is rejected

3. **DUMP/RESTORE tests**: `just test frogdb-server "function.*dump"`
   - Already partially tested in `scripting/src/persistence.rs` unit tests
   - Integration test: FUNCTION LOAD -> FUNCTION DUMP -> FUNCTION FLUSH -> FUNCTION RESTORE -> FCALL

4. **Persistence/restart tests**: `just test frogdb-server "function.*reload"`
   - Use `TestServer` restart capability
   - FUNCTION LOAD -> server stop -> server start -> FCALL succeeds

5. **Existing regression tests**: `just test frogdb-redis-regression functions_tcl`
   - All existing tests in `functions_tcl.rs` must continue to pass
