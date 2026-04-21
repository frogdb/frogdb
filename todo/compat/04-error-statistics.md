# 4. Error Statistics (12 tests — `info_tcl.rs`)

**Status**: Not implemented (latency stats exist, error stats do not)

**Scope**: Implement per-error-type tracking in `INFO stats` (`errorstat_*` fields, `rejected_calls`,
`failed_calls`, `total_error_replies`) and per-command `rejected_calls`/`failed_calls` in
`INFO commandstats`.

**Source files (primary)**:
- `frogdb-server/crates/server/src/connection/dispatch.rs` — command routing and pre-execution pipeline
- `frogdb-server/crates/server/src/connection/guards.rs` — pre-checks (auth, ACL, arity, OOM, pub/sub mode)
- `frogdb-server/crates/server/src/connection/handlers/transaction.rs` — MULTI/EXEC queuing errors
- `frogdb-server/crates/server/src/connection/handlers/scripting/eval.rs` — script execution errors
- `frogdb-server/crates/server/src/connection/handlers/scatter.rs` — `handle_info` patching and aggregation
- `frogdb-server/crates/server/src/commands/info.rs` — `build_stats_info()` hardcoded values
- `frogdb-server/crates/core/src/client_registry/mod.rs` — server-wide stats aggregation
- `frogdb-server/crates/core/src/shard/execution.rs` — shard-level command execution (OOM check, handler errors)
- `frogdb-server/crates/core/src/shard/eviction.rs` — `CommandError::OutOfMemory` generation
- `frogdb-server/crates/types/src/error.rs` — `CommandError` enum (error prefixes defined here)

---

## Architecture

### Error classification (rejected vs failed)

Redis distinguishes two error categories:

| Category | Meaning | When | Examples |
|----------|---------|------|----------|
| **Rejected** | Command refused *before* execution | Pre-dispatch checks | Unknown command, wrong arity, OOM, ACL NOPERM |
| **Failed** | Command accepted but *execution* returned an error | During `Command::execute()` | WRONGTYPE, NOGROUP, NOSCRIPT, AUTH failure |

In FrogDB's pipeline (`route_and_execute_with_transaction`):
- **Rejected** = errors returned from `run_pre_checks()`, arity validation (line 512-519), `queue_command()` validation in MULTI, unknown command in shard `execute_command_inner()`
- **Failed** = errors returned from `handler.execute()` (line 94-96 in `execution.rs`), script errors (`ScriptError::NoScript`, runtime errors), `CommandError::OutOfMemory` during write execution

### Error prefix extraction

Each error response in Redis starts with a prefix token before the first space:
- `"ERR wrong number..."` → prefix `ERR`
- `"WRONGTYPE Operation..."` → prefix `WRONGTYPE`
- `"NOSCRIPT No matching..."` → prefix `NOSCRIPT`
- `"OOM command not allowed..."` → prefix `OOM`
- `"NOPERM this user..."` → prefix `NOPERM`
- `"NOGROUP No such..."` → prefix `NOGROUP`

The error prefix is extracted from the `Response::Error(bytes)` payload: everything before the first
space (or the whole string if no space).

### Counter data structures

```rust
// In frogdb-server/crates/core/src/client_registry/mod.rs (or a new stats module)

/// Server-wide error statistics, shared across all connections via Arc.
pub struct ErrorStats {
    /// Total error replies sent (rejected + failed).
    pub total_error_replies: AtomicU64,
    /// Commands rejected before execution.
    pub rejected_calls: AtomicU64,
    /// Commands that failed during execution.
    pub failed_calls: AtomicU64,
    /// Per-error-prefix counters: "ERR" -> count, "WRONGTYPE" -> count, etc.
    /// Protected by RwLock; capped at MAX_ERROR_TYPES entries.
    pub error_type_counts: RwLock<HashMap<String, u64>>,
}
```

### Growth cap strategy

Redis caps at 128 distinct error prefixes. FrogDB should use the same cap. When the map reaches
128 entries, new prefixes are silently dropped (the counter for `total_error_replies` and
`rejected_calls`/`failed_calls` still increments, but no new `errorstat_*` key is created).

This prevents unbounded memory growth from adversarial clients sending gibberish commands that
produce unique error prefixes.

---

## Implementation Steps

### Step 1: Add `ErrorStats` struct

**File**: `frogdb-server/crates/core/src/client_registry/mod.rs` (add to existing module, alongside
`command_call_counts`)

```rust
use std::sync::atomic::AtomicU64;

/// Maximum distinct error prefixes tracked (matches Redis 7.x cap of 128).
const MAX_ERROR_TYPES: usize = 128;

/// Server-wide error statistics.
#[derive(Debug, Default)]
pub struct ErrorStats {
    pub total_error_replies: AtomicU64,
    pub rejected_calls: AtomicU64,
    pub failed_calls: AtomicU64,
    /// Maps error prefix (e.g., "ERR", "WRONGTYPE") to occurrence count.
    error_type_counts: RwLock<HashMap<String, u64>>,
}

impl ErrorStats {
    pub fn new() -> Self { Self::default() }

    /// Record a rejected command error.
    pub fn record_rejected(&self, error_prefix: &str) {
        self.rejected_calls.fetch_add(1, Ordering::Relaxed);
        self.total_error_replies.fetch_add(1, Ordering::Relaxed);
        self.record_error_type(error_prefix);
    }

    /// Record a failed command error.
    pub fn record_failed(&self, error_prefix: &str) {
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
        self.total_error_replies.fetch_add(1, Ordering::Relaxed);
        self.record_error_type(error_prefix);
    }

    /// Increment the per-prefix counter (capped at MAX_ERROR_TYPES).
    fn record_error_type(&self, prefix: &str) {
        let mut map = self.error_type_counts.write().unwrap();
        if let Some(count) = map.get_mut(prefix) {
            *count += 1;
        } else if map.len() < MAX_ERROR_TYPES {
            map.insert(prefix.to_string(), 1);
        }
        // else: silently drop (cap reached)
    }

    /// Snapshot of per-prefix error counts for INFO output.
    pub fn error_type_snapshot(&self) -> HashMap<String, u64> {
        self.error_type_counts.read().unwrap().clone()
    }

    /// Reset all error stats (CONFIG RESETSTAT).
    pub fn reset(&self) {
        self.total_error_replies.store(0, Ordering::Relaxed);
        self.rejected_calls.store(0, Ordering::Relaxed);
        self.failed_calls.store(0, Ordering::Relaxed);
        self.error_type_counts.write().unwrap().clear();
    }
}
```

Add an `error_stats: Arc<ErrorStats>` field to `ClientRegistry`.

### Step 2: Add error prefix extraction helper

**File**: `frogdb-server/crates/core/src/client_registry/mod.rs` (or a shared util)

```rust
/// Extract error prefix from a RESP error message.
/// "ERR wrong number..." -> "ERR"
/// "WRONGTYPE Operation..." -> "WRONGTYPE"
pub fn extract_error_prefix(error_bytes: &[u8]) -> &str {
    let s = std::str::from_utf8(error_bytes).unwrap_or("ERR");
    s.split_once(' ').map(|(prefix, _)| prefix).unwrap_or(s)
}
```

### Step 3: Hook error counting into the connection dispatch pipeline

**File**: `frogdb-server/crates/server/src/connection/dispatch.rs`

The main dispatch method `route_and_execute_with_transaction` (line 441) is the single point
through which all client commands flow. After computing the response, inspect it:

**Rejected call sites** (errors returned BEFORE execution):
1. `run_pre_checks()` returns error → line 462 (auth, ACL NOPERM, pub/sub mode, replica READONLY)
2. Arity validation → line 512-519 (`ERR wrong number of arguments...`)
3. Unknown command in `queue_command()` → transaction.rs line 443-455
4. Arity failure in `queue_command()` → transaction.rs line 458-468
5. OOM rejection in shard `execute_command_inner` → eviction.rs line 58 (this is a pre-execution
   memory check, before `handler.execute()` runs)

**Failed call sites** (errors returned DURING execution):
1. `handler.execute()` returns `Err(CommandError)` → execution.rs line 94-96
2. Script errors (NOSCRIPT, runtime) → returned from `handle_eval`/`handle_evalsha`
3. Errors inside MULTI/EXEC results (individual responses in the array)
4. Blocking command timeout errors

**Integration approach**: Add a method to `ConnectionHandler`:

```rust
/// Classify and record an error response.
fn record_error_response(&self, response: &Response, is_rejected: bool) {
    if let Response::Error(ref bytes) = response {
        let prefix = extract_error_prefix(bytes);
        let error_stats = &self.admin.client_registry.error_stats;
        if is_rejected {
            error_stats.record_rejected(prefix);
        } else {
            error_stats.record_failed(prefix);
        }
    }
}
```

Call this at each error return point in `route_and_execute_with_transaction`:
- After `run_pre_checks` (rejected)
- After arity check (rejected)
- After `route_and_execute` returns (failed, if response is Error)
- Inside `handle_exec` when iterating transaction results (each Error in the array)

For scripting: script errors (NOSCRIPT, runtime errors that surface as `Response::Error`) should
be recorded as failed. The response comes back from `handle_eval`/`handle_evalsha` which return
`Response::Error(...)` directly.

### Step 4: Hook into MULTI/EXEC error tracking

**File**: `frogdb-server/crates/server/src/connection/handlers/transaction.rs`

Two sites:
1. **queue_command** rejection (unknown cmd, wrong arity) — these are *rejected* calls. The error
   is returned immediately during queuing. Record at lines 443-468.
2. **EXEC results** — iterate the response array after execution. Each `Response::Error` in the
   array is a *failed* call (the command was accepted into the queue but failed during execution).

### Step 5: Hook into Lua/scripting error tracking

**File**: `frogdb-server/crates/server/src/connection/handlers/scripting/eval.rs`

Script execution errors (`NOSCRIPT`, runtime errors from `pcall`) surface as `Response::Error`
from `execute_single_shard_script` / `execute_single_shard_script_sha`. The dispatch pipeline
(Step 3) handles these automatically since they flow through `route_and_execute_with_transaction`.

However, errors *within* a Lua script that are caught by `pcall` and not re-raised do NOT
count — only errors that make it to the client response count. This matches Redis behavior.

### Step 6: Expose in INFO stats section

**File**: `frogdb-server/crates/server/src/commands/info.rs` — `build_stats_info()`

Replace the hardcoded values:
```
total_error_replies:0
```
with a placeholder that the scatter handler can patch (same pattern as `evicted_keys`).

**File**: `frogdb-server/crates/server/src/connection/handlers/scatter.rs` — `handle_info()`

Add patching logic (same pattern as the existing `evicted_keys`/`connected_clients` patching):
```rust
// Patch error stats
let error_stats = &self.admin.client_registry.error_stats;
let total_errors = error_stats.total_error_replies.load(Ordering::Relaxed);
let rejected = error_stats.rejected_calls.load(Ordering::Relaxed);
let failed = error_stats.failed_calls.load(Ordering::Relaxed);

patched = patched
    .replace("total_error_replies:0", &format!("total_error_replies:{total_errors}"))
    .replace("rejected_calls:0", &format!("..."));

// Append errorstat section
let error_types = error_stats.error_type_snapshot();
if !error_types.is_empty() {
    // Append after Stats section (or as new Errorstats section)
    // Format: errorstat_ERR:count=5
    //         errorstat_WRONGTYPE:count=2
}
```

Also patch per-command `rejected_calls` and `failed_calls` in the commandstats section (currently
hardcoded to 0 at line 511).

### Step 7: Add errorstats section to INFO

**File**: `frogdb-server/crates/server/src/commands/info.rs`

Add `b"errorstats"` to `EXTRA_SECTIONS` (or `DEFAULT_SECTIONS` — Redis includes it in `all`).
Add a `build_errorstats_info()` function that emits a placeholder header:
```rust
fn build_errorstats_info() -> String {
    "# Errorstats\r\n\r\n".to_string()
}
```

The scatter handler patches in the real data (same pattern as commandstats).

### Step 8: Reset error stats on CONFIG RESETSTAT

**File**: `frogdb-server/crates/server/src/connection/handlers/config.rs`

In `handle_config_resetstat()`, add:
```rust
self.admin.client_registry.error_stats.reset();
```

### Step 9: Per-command rejected/failed counts

Redis tracks `rejected_calls` and `failed_calls` per command in `INFO commandstats`. This requires
extending the `command_call_counts: HashMap<String, u64>` in `ClientRegistry` to also track per-command
error counts:

```rust
pub struct CommandStats {
    pub calls: u64,
    pub rejected_calls: u64,
    pub failed_calls: u64,
}
```

Replace `command_call_counts: RwLock<HashMap<String, u64>>` with
`command_stats: RwLock<HashMap<String, CommandStats>>`.

Update the scatter handler's commandstats rendering to emit real values instead of the current
hardcoded `rejected_calls=0,failed_calls=0`.

---

## Integration Points

| Location | Error Type | Classification |
|----------|-----------|----------------|
| `dispatch.rs:462` — `run_pre_checks()` returns Some | NOAUTH, NOPERM, READONLY, CLUSTERDOWN, pub/sub mode | Rejected |
| `dispatch.rs:515` — arity check | ERR (wrong number of arguments) | Rejected |
| `guards.rs:62-66` — rate limit check | ERR (rate limit exceeded) | Rejected |
| `transaction.rs:443-455` — unknown cmd in MULTI | ERR (unknown command) | Rejected |
| `transaction.rs:458-468` — arity in MULTI | ERR (wrong number of arguments) | Rejected |
| `execution.rs:64` — OOM pre-check | OOM | Rejected |
| `execution.rs:94-96` — `handler.execute()` Err | WRONGTYPE, NOGROUP, ERR, etc. | Failed |
| `eval.rs` — NOSCRIPT from evalsha | NOSCRIPT | Failed |
| `scripting/executor.rs` — Lua runtime errors | ERR (script error) | Failed |
| AUTH failure (wrong password) | ERR (invalid password) | Failed |

---

## FrogDB Adaptations

This feature is pure observability with no behavioral impact. No Raft-specific error types are
needed because error stats are local-node metrics (each node tracks its own errors independently).

One FrogDB-specific consideration: the IOERR from WAL rollback (`execution.rs:180`) should be
classified as *failed* (the command was accepted and attempted but the persistence layer rejected it).
This is a FrogDB-specific error type that has no Redis equivalent.

---

## Tests

- `errorstats: failed call authentication error`
- `errorstats: failed call NOSCRIPT error`
- `errorstats: failed call NOGROUP error`
- `errorstats: failed call within LUA`
- `errorstats: failed call within MULTI/EXEC`
- `errorstats: rejected call unknown command`
- `errorstats: rejected call within MULTI/EXEC`
- `errorstats: rejected call due to wrong arity`
- `errorstats: rejected call by OOM error`
- `errorstats: rejected call by authorization error`
- `errorstats: blocking commands`
- `errorstats: limit errors will not increase indefinitely`

---

## Verification

1. **Unit tests**: Test `ErrorStats` struct directly (record, cap, reset, prefix extraction).
2. **Integration tests**: For each of the 12 tests above, send the triggering command sequence
   and verify `INFO stats` / `INFO errorstats` contains the expected values.
3. **Test pattern** (example for "rejected call unknown command"):
   ```
   CONFIG RESETSTAT          # clear baseline
   FOOBAR                    # unknown command → rejected
   INFO stats                # verify: total_error_replies:1, rejected_calls:1 (not added to stats section)
   INFO errorstats           # verify: errorstat_ERR:count=1
   INFO commandstats         # verify: cmdstat_foobar doesn't exist (unknown commands aren't tracked)
   ```
4. **Growth cap test** ("limit errors will not increase indefinitely"):
   ```
   CONFIG RESETSTAT
   # Send 200 commands that produce 200 distinct error prefixes
   # (e.g., custom script errors with unique prefixes)
   INFO errorstats           # verify: at most 128 errorstat_* lines
   ```
5. **CONFIG RESETSTAT clears all**: Verify all counters return to 0 after reset.
