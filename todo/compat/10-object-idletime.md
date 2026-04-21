# 10. OBJECT IDLETIME & Access Tracking

| Field          | Value |
|----------------|-------|
| Tests          | 6 (5 from `introspection2_tcl.rs`, 1 from `maxmemory_tcl.rs`) |
| Source files   | `frogdb-server/crates/redis-regression/tests/introspection2_tcl.rs`, `frogdb-server/crates/redis-regression/tests/maxmemory_tcl.rs` |
| Status         | **Partially implemented** -- OBJECT IDLETIME/FREQ commands exist and read from `KeyMetadata`; `CLIENT NO-TOUCH` sets the flag in the registry; but the no-touch flag is never propagated to the shard to suppress `touch()` calls, and the LFU counter is never incremented on key access |

## Architecture

### Access time tracking

`KeyMetadata` (in `frogdb-server/crates/types/src/types/mod.rs:1350`) stores:

```rust
pub struct KeyMetadata {
    pub expires_at: Option<Instant>,
    pub last_access: Instant,      // updated by touch()
    pub lfu_counter: u8,           // LFU frequency (starts at 5)
    pub memory_size: usize,
}
```

`KeyMetadata::touch()` sets `last_access = Instant::now()`.

### Where access time is updated today

The store's `get_with_expiry_check()` and `get_mut()` in `HashMapStore` call `entry.metadata.touch()` after reading. These are called from most read/write command handlers via `ctx.store.get_with_expiry_check(key)`.

Commands that intentionally skip access time updates (using `contains()`, `get_expiry()`, or `key_type()` which never call `touch()`):
- **TTL/PTTL** -- uses `ctx.store.contains()` + `ctx.store.get_expiry()`
- **TYPE** -- uses `ctx.store.key_type()`
- **EXISTS** -- uses `ctx.store.contains()`

The **TOUCH** command explicitly calls `ctx.store.touch(key)` in `HashMapStore::touch()` which does `entry.metadata.touch()`.

### NO-TOUCH flag propagation (the gap)

`CLIENT NO-TOUCH ON` sets `ClientFlags::NO_TOUCH` in the `ClientRegistry` (connection-level state in `frogdb-server/crates/core/src/client_registry/mod.rs`). However:

1. The `ShardMessage::Execute` includes `conn_id` but not a `no_touch` flag.
2. The shard worker's `execute_command_inner()` builds `CommandContext` without any no-touch awareness.
3. `get_with_expiry_check()` unconditionally calls `metadata.touch()`.

The shard has no way to know the calling client has NO_TOUCH set, so access time is always updated regardless of the flag.

### LFU counter (the gap)

The `lfu_counter` field exists and the eviction subsystem reads it (`sample_for_eviction_lfu`, `get_lfu_value`), but **no command access path increments it**. `update_lfu_counter()` is defined on the store but never called during normal command execution. Redis increments the LFU counter on every key access (alongside the LRU clock update).

### OBJECT IDLETIME/FREQ commands (working)

`ObjectCommand` in `frogdb-server/crates/commands/src/generic.rs` handles both subcommands:
- `IDLETIME` -- reads `meta.last_access.elapsed().as_secs()`
- `FREQ` -- reads `meta.lfu_counter as i64`

Both use `ctx.store.get_metadata(key)` which does NOT call `touch()` (read-only metadata access). This is correct -- OBJECT IDLETIME itself must not update access time.

## Implementation Steps

### Step 1: Propagate no-touch flag to shard

**File**: `frogdb-server/crates/core/src/shard/message.rs`

Add a `no_touch: bool` field to `ShardMessage::Execute`:

```rust
Execute {
    command: Arc<ParsedCommand>,
    conn_id: u64,
    txid: Option<u64>,
    protocol_version: ProtocolVersion,
    track_reads: bool,
    no_touch: bool,  // NEW
    response_tx: oneshot::Sender<Response>,
},
```

**File**: `frogdb-server/crates/server/src/connection/routing.rs`

When building the `ShardMessage::Execute`, look up the client's NO_TOUCH flag from the `ClientRegistry` (available via `self.admin.client_registry`):

```rust
let no_touch = self.admin.client_registry
    .get(self.state.id)
    .map(|info| info.flags.contains(ClientFlags::NO_TOUCH))
    .unwrap_or(false);
```

Alternatively, cache the no_touch state in `ConnectionHandler.state` (cheaper -- avoids registry lookup on every command). Update it when `CLIENT NO-TOUCH ON/OFF` is called.

### Step 2: Thread no-touch into command execution

**File**: `frogdb-server/crates/core/src/shard/execution.rs`

Pass `no_touch` to `execute_command_inner()` and into `CommandContext`:

```rust
fn execute_command_inner(
    &mut self,
    command: &ParsedCommand,
    conn_id: u64,
    protocol_version: ProtocolVersion,
    track_reads: bool,
    no_touch: bool,  // NEW
) -> (Response, Option<WriteCommandMeta>) { ... }
```

**File**: `frogdb-server/crates/core/src/command.rs`

Add `no_touch: bool` field to `CommandContext`.

### Step 3: Make `get_with_expiry_check` respect no-touch

**Option A (simpler)**: Add a `get_with_expiry_check_no_touch()` method to the `Store` trait that skips the `touch()` call, then have command handlers use `ctx.no_touch` to choose between the two.

**Option B (preferred)**: Add a `suppress_touch: bool` parameter to `get_with_expiry_check` in `HashMapStore`:

Since every call site would need changing, the cleanest approach is:

**File**: `frogdb-server/crates/core/src/store/hashmap.rs`

In `get_with_expiry_check()`, accept the flag from a store-level field:

```rust
impl HashMapStore {
    /// Set whether the next access should suppress touch updates.
    pub fn set_suppress_touch(&mut self, suppress: bool) {
        self.suppress_touch = suppress;
    }
}
```

Then in `get_with_expiry_check()`:
```rust
fn get_with_expiry_check(&mut self, key: &[u8]) -> Option<Arc<Value>> {
    if self.check_and_delete_expired(key) { return None; }
    if let Some(entry) = self.data.get_mut(key) {
        if !self.suppress_touch {
            entry.metadata.touch();
        }
        ...
    }
}
```

The shard sets `store.set_suppress_touch(ctx.no_touch)` before calling `execute_command_inner` and resets it after.

Similarly for `get_mut()`.

### Step 4: TOUCH command must bypass no-touch suppression

Per Redis semantics, `TOUCH` always updates access time even when `CLIENT NO-TOUCH ON` is active. The TOUCH command calls `ctx.store.touch(key)` directly (not `get_with_expiry_check`), and `HashMapStore::touch()` unconditionally calls `entry.metadata.touch()`. This path does NOT go through `suppress_touch`, so no change needed -- it already works correctly.

For the **scatter-gather** TOUCH path in `execute_scatter_part` (`ScatterOp::Touch`), it calls `self.store.touch(key)` directly -- also correct.

### Step 5: TOUCH from Lua scripts must bypass no-touch

Per the test `Operations in no-touch mode TOUCH from script alters the last access time of a key`, redis.call("TOUCH", key) inside a Lua script should still update access time even though the calling connection has NO_TOUCH set.

The Lua scripting path (`frogdb-server/crates/core/src/scripting/bindings.rs`) routes TOUCH commands through normal command dispatch. Since TOUCH calls `store.touch()` (not `get_with_expiry_check()`), it naturally bypasses the suppress_touch flag. No special handling needed.

### Step 6: Increment LFU counter on key access

**File**: `frogdb-server/crates/core/src/store/hashmap.rs`

In `get_with_expiry_check()` and `get_mut()`, after the `touch()` call (when not suppressed), also increment the LFU counter:

```rust
if !self.suppress_touch {
    entry.metadata.touch();
    entry.metadata.lfu_counter = crate::eviction::lfu_log_incr(
        entry.metadata.lfu_counter,
        self.lfu_log_factor,
    );
}
```

Add `lfu_log_factor: u8` field to `HashMapStore` (initialized from `EvictionConfig::lfu_log_factor`, default 10). Update it via a `set_lfu_log_factor()` method called when config changes.

This matches Redis behavior: LFU counter is bumped on every access, and the probabilistic algorithm prevents it from saturating too quickly.

### Step 7: OBJECT FREQ returns the decayed value

The existing `OBJECT FREQ` handler reads `meta.lfu_counter as i64`. Redis returns the raw counter (not decayed). Looking at the test (`lru/lfu value of the key just added`), a freshly-created key should have `OBJECT FREQ` return 5 (the initial value). The current implementation returns `meta.lfu_counter` directly, which will be 5 for new keys. This is correct.

However, verify the test expects the counter after an access (which would be >= 5 due to probabilistic increment). If the test does `SET key val` then immediately `OBJECT FREQ key`, Redis returns the counter after the SET operation's access. Since SET uses `store.set()` (which creates fresh metadata with lfu_counter=5, no increment), the result should be 5.

## Integration Points

### Commands that DO update access time (call get_with_expiry_check / get_mut)
- GET, GETEX, GETRANGE, SUBSTR, STRLEN, APPEND
- INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- HGET, HMGET, HGETALL, HSET, HDEL, etc.
- LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, etc.
- SADD, SREM, SMEMBERS, SISMEMBER, etc.
- ZADD, ZREM, ZSCORE, ZRANGE, etc.
- XADD, XREAD, XRANGE, etc.

### Commands that DO NOT update access time (correct behavior, no changes needed)
- TTL, PTTL -- uses `contains()` + `get_expiry()`
- TYPE -- uses `key_type()`
- EXISTS -- uses `contains()`
- OBJECT IDLETIME/FREQ/ENCODING -- uses `get_metadata()` / `get()` (non-touching)
- PERSIST, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT -- uses `set_expiry()` / `get_expiry()`

### Commands that ALWAYS update access time (bypass no-touch)
- TOUCH -- calls `store.touch()` directly, not subject to suppress_touch

### Where no-touch flag is checked
1. `ConnectionHandler` caches the flag locally (set by `CLIENT NO-TOUCH ON/OFF`)
2. Propagated to shard via `ShardMessage::Execute { no_touch: bool }`
3. Shard sets `store.set_suppress_touch(no_touch)` before command execution

## FrogDB Adaptations

### RocksDB compaction interaction

Access time (`last_access`) and LFU counter are stored in `KeyMetadata` which lives in-memory with the key. They are NOT persisted to RocksDB for hot keys. For warm-tier keys, metadata is serialized during demotion (`serialize(value, metadata)`) and restored during promotion. After promotion, the key's `last_access` is stale (reflects time of demotion), but this is acceptable -- the promotion itself constitutes an access and should call `touch()`.

### Raft-aware access tracking

Access time tracking is purely local and not replicated. Each node tracks its own access patterns independently. This is correct because:
- Eviction is per-node (each node evicts based on local access patterns)
- OBJECT IDLETIME reports local idle time
- LFU counters are local to each node

No Raft changes needed.

### Persistence of LFU counters

When keys are persisted to WAL or snapshots via `serialize()`, `KeyMetadata` (including `lfu_counter`) is included. On recovery, the counter is restored. This means access frequency survives restarts, matching Redis behavior with RDB persistence.

## Tests

### introspection2_tcl.rs (5 tests)

- `TTL, TYPE and EXISTS do not alter the last access time of a key`
- `TOUCH alters the last access time of a key`
- `Operations in no-touch mode do not alter the last access time of a key`
- `Operations in no-touch mode TOUCH alters the last access time of a key`
- `Operations in no-touch mode TOUCH from script alters the last access time of a key`

### maxmemory_tcl.rs (1 test)

- `lru/lfu value of the key just added` -- implement `OBJECT IDLETIME`/`FREQ` tracking

## Verification

1. **Unit test** (store level): Create key, sleep 1s, call `idle_time()` -- verify >= 1s. Call `get_with_expiry_check()` with suppress_touch=false, verify idle time resets. Repeat with suppress_touch=true, verify idle time does NOT reset.

2. **Integration test** (CLIENT NO-TOUCH):
   ```
   SET mykey val
   # sleep 2 seconds
   CLIENT NO-TOUCH ON
   GET mykey
   OBJECT IDLETIME mykey  --> should be >= 2 (access time not updated)
   CLIENT NO-TOUCH OFF
   GET mykey
   OBJECT IDLETIME mykey  --> should be 0 (access time updated)
   ```

3. **Integration test** (TOUCH bypasses no-touch):
   ```
   SET mykey val
   # sleep 2 seconds
   CLIENT NO-TOUCH ON
   TOUCH mykey
   OBJECT IDLETIME mykey  --> should be 0 (TOUCH always updates)
   ```

4. **Integration test** (LFU counter):
   ```
   CONFIG SET maxmemory-policy allkeys-lfu
   SET mykey val
   OBJECT FREQ mykey  --> 5 (initial value)
   GET mykey
   OBJECT FREQ mykey  --> >= 5 (probabilistically incremented)
   ```

5. **Regression tests**: Enable the 6 excluded tests in `introspection2_tcl.rs` and `maxmemory_tcl.rs`, run `just test frogdb-server/crates/redis-regression`.
