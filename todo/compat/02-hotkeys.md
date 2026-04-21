# 2. HOTKEYS Subsystem (43 tests -- `hotkeys_tcl.rs`)

**Status**: Not implemented
**Test file**: `frogdb-server/crates/redis-regression/tests/hotkeys_tcl.rs`
**Upstream source**: Redis 8.6.0 `unit/hotkeys.tcl`
**Scope**: Implement `HOTKEYS START/STOP/RESET/GET` subcommands for key access frequency sampling.

---

## Architecture

### Overview

HOTKEYS is a server-wide observability feature that tracks which keys are accessed most frequently
during a sampling session. It operates at the connection handler level (like LATENCY and SLOWLOG)
because it needs access to cross-shard coordination without modifying per-shard execution paths.

Key design decisions:
- **Session is server-global**: One active session at a time (shared state via `Arc<Mutex<..>>`)
- **Interception point**: After command execution, record key accesses from the command's `keys()`
  method using existing routing infrastructure
- **Sampling**: Probabilistic (1-in-N) sampling configurable via SAMPLE parameter
- **Slot filtering**: Only track keys whose hash slot is in the selected set
- **Metrics**: Track CPU time (command execution duration) and/or NET bytes (request/response size)

### Session State Machine

```
    [Idle] --START--> [Active] --STOP--> [Stopped]
      ^                                       |
      |                RESET                  |
      +---------------------------------------+
```

- `Idle`: No session exists. GET returns nil. START allowed.
- `Active`: Sampling in progress. STOP allowed. RESET returns error.
- `Stopped`: Session data retained for GET queries. RESET returns to Idle.

### Key Access Tracking Flow

1. Client sends command (e.g., `GET mykey`)
2. Connection handler resolves command, extracts keys via `handler.keys(&args)`
3. After execution, if a hotkey session is active:
   a. Compute hash slot for each key via `slot_for_key()`
   b. Check if slot is in selected set (skip if not)
   c. Apply sampling ratio (skip if random check fails)
   d. Record access: increment count, accumulate CPU time / net bytes

---

## Implementation Steps

### Step 1: Define session types (`frogdb-server/crates/core/src/hotkeys.rs`)

New module in the `core` crate since the session state is shared server-wide.

```rust
/// Which metrics to track during a hotkey session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotkeyMetric {
    Cpu,
    Net,
}

/// Configuration for a hotkey session (from START parameters).
#[derive(Debug, Clone)]
pub struct HotkeySessionConfig {
    pub metrics: Vec<HotkeyMetric>,     // 1 or 2 metrics
    pub count: usize,                    // top-N keys to report (default: 10)
    pub duration: Option<Duration>,      // auto-stop after duration
    pub sample_ratio: u64,              // 1 = every access, N = 1-in-N
    pub selected_slots: Option<Vec<u16>>, // None = all slots
}

/// Per-key accumulated statistics.
#[derive(Debug, Clone, Default)]
pub struct HotkeyEntry {
    pub access_count: u64,
    pub total_cpu_us: u64,      // microseconds of command execution time
    pub total_net_bytes: u64,   // request + response bytes
}

/// Active or stopped hotkey session.
#[derive(Debug)]
pub struct HotkeySession {
    pub config: HotkeySessionConfig,
    pub started_at: Instant,
    pub stopped_at: Option<Instant>,
    pub entries: HashMap<Bytes, HotkeyEntry>,
    pub total_samples: u64,
    pub total_accesses: u64,
}

/// Thread-safe session holder.
pub type SharedHotkeySession = Arc<Mutex<Option<HotkeySession>>>;
```

### Step 2: Register HOTKEYS command (`frogdb-server/crates/server/src/commands/hotkeys.rs`)

Follow the same pattern as `latency.rs` -- a stub `Command` impl that declares
`ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)`:

```rust
pub struct HotkeysCommand;

impl Command for HotkeysCommand {
    fn name(&self) -> &'static str { "HOTKEYS" }
    fn arity(&self) -> Arity { Arity::AtLeast(1) }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NOSCRIPT | CommandFlags::LOADING | CommandFlags::STALE
    }
    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
    }
    fn execute(&self, _ctx: &mut CommandContext, _args: &[Bytes]) -> Result<Response, CommandError> {
        Err(CommandError::InvalidArgument {
            message: "HOTKEYS command should be handled by connection handler".to_string(),
        })
    }
    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> { vec![] }
}
```

Registration changes:
- Add `pub mod hotkeys;` and `pub use hotkeys::*;` in `commands/mod.rs`
- Register `HotkeysCommand` in the command registry (same place as `LatencyCommand`)
- Add `"HOTKEYS"` to `CONTAINER_COMMANDS` in `connection/util.rs`

### Step 3: Route HOTKEYS to connection handler

Files to modify:
- `frogdb-server/crates/server/src/connection/dispatch.rs`:
  - Add `"HOTKEYS" => ConnectionLevelHandler::Hotkeys` in `refine_handler` under `Admin`
  - Add dispatch arm: `ConnectionLevelHandler::Hotkeys => Some(vec![self.handle_hotkeys_command(args).await])`
- `frogdb-server/crates/server/src/connection/router.rs`:
  - Add `Hotkeys` variant to `ConnectionLevelHandler` enum

### Step 4: Implement handler (`frogdb-server/crates/server/src/connection/handlers/hotkeys.rs`)

New handler file following the LATENCY pattern. Methods on `ConnectionHandler`:

```rust
impl ConnectionHandler {
    pub(crate) async fn handle_hotkeys_command(&self, args: &[Bytes]) -> Response {
        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"START" => self.handle_hotkeys_start(&args[1..]).await,
            b"STOP"  => self.handle_hotkeys_stop().await,
            b"RESET" => self.handle_hotkeys_reset().await,
            b"GET"   => self.handle_hotkeys_get().await,
            _ => Response::error(format!("ERR unknown subcommand '{}'. Try HOTKEYS START|STOP|RESET|GET.", ...)),
        }
    }

    async fn handle_hotkeys_start(&self, args: &[Bytes]) -> Response { ... }
    async fn handle_hotkeys_stop(&self) -> Response { ... }
    async fn handle_hotkeys_reset(&self) -> Response { ... }
    async fn handle_hotkeys_get(&self) -> Response { ... }
}
```

**START parsing**: Parse `METRICS <count> <metric>...`, `COUNT <n>`, `DURATION <secs>`,
`SAMPLE <ratio>`, `SLOTS <count> <slot>...` with all error cases from the test list.

**GET response format**:
- RESP2: flat array of alternating field/value pairs
- RESP3: Map type with keys like `"metrics"`, `"count"`, `"sample-ratio"`,
  `"selected-slots"`, `"hotkeys"` (array of `[key, access_count, cpu_us, net_bytes]`)

Use `self.state.protocol_version.is_resp3()` to select format.

### Step 5: Wire session into ConnectionHandler

The `SharedHotkeySession` needs to be accessible from `ConnectionHandler`. Options:

1. **Add to `ObservabilityDeps`** (preferred -- mirrors how `metrics_recorder` is shared):
   - Add `pub hotkey_session: SharedHotkeySession` field to `ObservabilityDeps`
   - Initialize in server startup, pass through `ConnectionHandlerBuilder`
   - Access in handler as `self.observability.hotkey_session`

2. Store in `ConnectionHandler` directly via a new field on the struct (in
   `connection.rs` `ConnectionHandler` definition).

Option 1 is better since the session is shared across all connections.

### Step 6: Key access interception

The interception point is in `ConnectionHandler::route_and_execute_with_transaction()` in
`dispatch.rs`. After a command executes successfully:

```rust
// After: vec![self.handle_internal_action(response).await]
// Add hotkey tracking:
if let Some(session) = self.observability.hotkey_session.as_ref() {
    self.record_hotkey_access(cmd, cmd_name, &response, execution_duration, request_bytes, response_bytes);
}
```

The `record_hotkey_access` method:
1. Look up command in registry to get `keys()` extraction
2. For each key, compute `slot_for_key(key)` (from `frogdb_core::shard::helpers`)
3. Check slot membership in session's `selected_slots`
4. Apply sampling (use `fastrand::u64(0..ratio)` == 0)
5. Lock session mutex, update `HotkeyEntry`

For **MULTI/EXEC**: track keys from each command in the transaction after execution
(the transaction handler already executes all commands sequentially).

For **EVAL/scripting**: the script executor already resolves keys via the KEYS argument;
track those declared keys at the connection level after script execution.

### Step 7: Cluster-mode slot filtering

FrogDB uses `slot_for_key()` from `frogdb-server/crates/core/src/shard/helpers.rs`:

```rust
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % 16384
}
```

**SLOTS parameter validation** (in `handle_hotkeys_start`):
- In non-cluster mode (`!self.cluster.cluster_state.is_some()`): `SLOTS` is an error
- In cluster mode: validate each slot is handled by this node using
  `cluster_state.slot_owner(slot) == self.cluster.node_id`
- Duplicates: track seen slots in a `HashSet`, error on repeat
- Range: 0..=16383

When no SLOTS specified:
- Standalone: implicitly all 16384 slots (set `selected_slots = None`)
- Cluster: only slots owned by this node (from `ClusterState::local_slots()`)

---

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Command stub | `server/src/commands/hotkeys.rs` | New file |
| Command registration | `server/src/commands/mod.rs` | Add module |
| Container commands | `server/src/connection/util.rs` | Add `"HOTKEYS"` |
| Router enum | `server/src/connection/router.rs` | Add `Hotkeys` variant |
| Dispatch routing | `server/src/connection/dispatch.rs` | Route to handler |
| Handler impl | `server/src/connection/handlers/hotkeys.rs` | New file |
| Handler mod | `server/src/connection/handlers/mod.rs` | Add module |
| Session types | `core/src/hotkeys.rs` | New file |
| Core lib | `core/src/lib.rs` | `pub mod hotkeys;` |
| Deps struct | `server/src/connection/deps.rs` (or wherever `ObservabilityDeps` lives) | Add session field |
| Key interception | `server/src/connection/dispatch.rs` | Post-execution hook |
| Slot helpers | `core/src/shard/helpers.rs` | Already has `slot_for_key` (no change) |

---

## FrogDB Adaptations

### Sharding Model Difference

Redis 8.6 runs HOTKEYS on a single-threaded server. FrogDB has multiple shards. Key differences:

1. **Key access happens at shard level** but HOTKEYS is connection-level. The interception
   must happen in the connection handler *before* routing to shards (we already know the keys
   from registry lookup) or *after* getting the response back. Doing it in the connection
   handler (after execution) is simpler and avoids modifying shard worker code.

2. **Cross-shard commands (MGET, MSET)**: The connection handler already resolves all keys
   for scatter-gather. Track all keys from the command regardless of which shard they route to.

3. **Slot ownership in cluster mode**: FrogDB maps slot -> shard via `slot % num_shards` for
   local sharding, but cluster-mode slot ownership is tracked separately in `ClusterState`.
   Use `ClusterState` for the SLOTS validation, not the local shard mapping.

### CPU Time Measurement

Redis tracks per-command CPU time internally. FrogDB can measure this in the connection handler
as `Instant::now()` before/after `route_and_execute`. This captures end-to-end including
shard message passing, which is slightly different from Redis's pure CPU time, but is the
best available proxy without modifying shard workers.

### NET Bytes Measurement

Request bytes: available from the parsed command frame size.
Response bytes: compute from the serialized response frame size, or estimate from `Response`
structure. For accuracy, measure at the frame-IO layer. Initially, a rough estimate
(serialized response length) is acceptable.

---

## Tests

- `HOTKEYS START - METRICS required`
- `HOTKEYS START - METRICS with CPU only`
- `HOTKEYS START - METRICS with NET only`
- `HOTKEYS START - METRICS with both CPU and NET`
- `HOTKEYS START - with COUNT parameter`
- `HOTKEYS START - with DURATION parameter`
- `HOTKEYS START - with SAMPLE parameter`
- `HOTKEYS START - with SLOTS parameter in cluster mode`
- `HOTKEYS START - Error: session already started`
- `HOTKEYS START - Error: invalid METRICS count`
- `HOTKEYS START - Error: METRICS count mismatch`
- `HOTKEYS START - Error: METRICS invalid metrics`
- `HOTKEYS START - Error: METRICS same parameter`
- `HOTKEYS START - Error: COUNT out of range`
- `HOTKEYS START - Error: SAMPLE ratio invalid`
- `HOTKEYS START - Error: SLOTS not allowed in non-cluster mode`
- `HOTKEYS START - Error: SLOTS count mismatch`
- `HOTKEYS START - Error: SLOTS already specified`
- `HOTKEYS START - Error: duplicate slots`
- `HOTKEYS START - Error: invalid slot - negative value`
- `HOTKEYS START - Error: invalid slot - out of range`
- `HOTKEYS START - Error: invalid slot - non-integer`
- `HOTKEYS START - Error: slot not handled by this node`
- `HOTKEYS STOP - basic functionality`
- `HOTKEYS RESET - basic functionality`
- `HOTKEYS RESET - Error: session in progress`
- `HOTKEYS GET - returns nil when not started`
- `HOTKEYS GET - sample-ratio field`
- `HOTKEYS GET - no conditional fields without selected slots`
- `HOTKEYS GET - no conditional fields with sample_ratio = 1`
- `HOTKEYS GET - conditional fields with sample_ratio > 1 and selected slots`
- `HOTKEYS GET - selected-slots field with individual slots`
- `HOTKEYS GET - selected-slots with unordered input slots are sorted`
- `HOTKEYS GET - selected-slots returns full range in non-cluster mode`
- `HOTKEYS GET - selected-slots returns node's slot ranges when no SLOTS specified in cluster mode`
- `HOTKEYS GET - selected-slots returns each node's slot ranges in multi-node cluster`
- `HOTKEYS GET - RESP3 returns map with flat array values for hotkeys`
- `HOTKEYS - nested commands`
- `HOTKEYS - commands inside MULTI/EXEC`
- `HOTKEYS - EVAL inside MULTI/EXEC with nested calls`
- `HOTKEYS - tracks only keys in selected slots`
- `HOTKEYS - multiple selected slots`
- `HOTKEYS detection with biased key access, sample ratio = $sample_ratio`

---

## Verification

### Unit tests (in handler module)

- Parse all START parameter combinations and verify config struct
- Validate all error conditions (mismatch, duplicates, out of range, etc.)
- Session state machine transitions (Idle -> Active -> Stopped -> Idle)
- GET response format for RESP2 vs RESP3

### Integration tests (in redis-regression)

Move tests from exclusion list to `#[tokio::test]` in `hotkeys_tcl.rs`:
- Use the test harness `TestServer` to send HOTKEYS commands
- For cluster tests, use `ClusterTestHarness`
- For biased key detection: send N accesses to "hot" key and M accesses spread across
  "cold" keys, verify "hot" appears in top-K results

### Manual verification

```bash
just test frogdb-server hotkeys
```

After implementation, update `hotkeys_tcl.rs` to remove intentional-incompatibility comments
and add real test implementations. Update `website/src/data/compat-exclusions.json` to remove
the 43 HOTKEYS entries.
