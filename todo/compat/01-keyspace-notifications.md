# 1. Keyspace Notifications (25 tests)

**Status**: Not implemented
**Test file**: `frogdb-server/crates/redis-regression/tests/pubsub_tcl.rs`
**Blocking reason**: Needs `notify-keyspace-events` config parameter and event emission infrastructure

## Source Files (primary modification targets)

| File | Role |
|------|------|
| `frogdb-server/crates/server/src/runtime_config.rs` | Add `notify-keyspace-events` parameter to `ConfigManager` |
| `frogdb-server/crates/config/src/params.rs` | Register parameter in `config_param_registry()` |
| `frogdb-server/crates/core/src/shard/pipeline.rs` | Hook notification emission into `run_post_execution()` |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | Emit `expired` event in `run_active_expiry()` |
| `frogdb-server/crates/core/src/shard/eviction.rs` | Emit `evicted` event in `delete_for_eviction()` |
| `frogdb-server/crates/core/src/pubsub.rs` | Reuse existing `ShardSubscriptions::publish()` |
| `frogdb-server/crates/core/src/shard/worker.rs` | Add `notify_keyspace_events` flags field |
| `frogdb-server/crates/core/src/shard/types.rs` | Possibly extend `ShardObservability` or add dedicated sub-struct |

## Architecture

### Key Design Decisions

1. **Reuse existing pub/sub infrastructure** -- Keyspace notifications are just PUBLISH to special
   channels (`__keyevent@0__:*` and `__keyspace@0__:*`). The shard already owns a
   `ShardSubscriptions` that implements `publish()` with channel + pattern matching. No new message
   delivery path is needed.

2. **Emit from the shard worker, not the command** -- Commands should not be aware of keyspace
   notifications. The post-execution pipeline already has hooks for search indexing and client
   tracking invalidation. Keyspace notifications slot in at the same level.

3. **Shared atomic flag for event mask** -- The `notify-keyspace-events` config string is parsed
   into a bitfield. Store it as an `Arc<AtomicU32>` in `ConfigManager` (same pattern as
   `lua_time_limit` / `wal_failure_policy`). Each shard worker reads the flag on every write without
   locking.

4. **DB number is always 0** -- FrogDB is single-database. Channel format is
   `__keyspace@0__:<key>` and `__keyevent@0__:<event>`.

5. **No-alloc fast path** -- When `notify_keyspace_events == 0` (disabled), the hook is a single
   atomic load + branch. Zero overhead in the common case.

### Data Flow

```
Command executes (shard thread)
  --> run_post_execution() / run_post_execution_after_wal()
      --> emit_keyspace_notification(key, event_type, cmd_name)
          |
          | (1) Load atomic event_flags
          | (2) Check if event_type matches flags (bitwise AND)
          | (3) If K flag set: publish to "__keyspace@0__:<key>" with payload=event_name
          | (4) If E flag set: publish to "__keyevent@0__:<event_name>" with payload=key
          |
          --> self.subscriptions.publish(channel, payload)
              --> Delivers to subscribers via existing PubSubSender channels

Active expiry (shard event loop tick)
  --> run_active_expiry()
      --> For each deleted key: emit_keyspace_notification(key, EXPIRED, "expired")

Eviction (memory pressure during write)
  --> check_memory_for_write() --> evict_one() --> delete_for_eviction()
      --> emit_keyspace_notification(key, EVICTED, "evicted")
```

## Implementation Steps

### Step 1: Define event type flags

Create `frogdb-server/crates/core/src/keyspace_event.rs`:

```rust
use bitflags::bitflags;

bitflags! {
    /// Keyspace notification event type flags.
    /// Matches Redis flag characters: K, E, g, $, l, s, h, z, x, e, t, m, d, A, n, o, c
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct KeyspaceEventFlags: u32 {
        /// K -- Keyspace events, published in __keyspace@<db>__ channel prefix.
        const KEYSPACE    = 0b0000_0000_0000_0001;
        /// E -- Keyevent events, published in __keyevent@<db>__ channel prefix.
        const KEYEVENT    = 0b0000_0000_0000_0010;
        /// g -- Generic commands (non-type specific): DEL, EXPIRE, RENAME, etc.
        const GENERIC     = 0b0000_0000_0000_0100;
        /// $ -- String commands.
        const STRING      = 0b0000_0000_0000_1000;
        /// l -- List commands.
        const LIST        = 0b0000_0000_0001_0000;
        /// s -- Set commands.
        const SET         = 0b0000_0000_0010_0000;
        /// h -- Hash commands.
        const HASH        = 0b0000_0000_0100_0000;
        /// z -- Sorted set commands.
        const ZSET        = 0b0000_0000_1000_0000;
        /// x -- Expired events (key TTL reached).
        const EXPIRED     = 0b0000_0001_0000_0000;
        /// e -- Evicted events (maxmemory policy).
        const EVICTED     = 0b0000_0010_0000_0000;
        /// t -- Stream commands.
        const STREAM      = 0b0000_0100_0000_0000;
        /// m -- Key miss events (access to non-existing key).
        const MISS        = 0b0000_1000_0000_0000;
        /// d -- Module key type events (unused in FrogDB).
        const MODULE      = 0b0001_0000_0000_0000;
        /// n -- New key events (key created for the first time).
        const NEW         = 0b0010_0000_0000_0000;
        /// o -- Overwritten events (key value replaced).
        const OVERWRITTEN = 0b0100_0000_0000_0000;
        /// c -- Type changed events (key type changed, e.g. hash -> string).
        const TYPE_CHANGED = 0b1000_0000_0000_0000;
        /// A -- Alias for "g$lshzxet" (all event types).
        const ALL_TYPES   = Self::GENERIC.bits() | Self::STRING.bits() | Self::LIST.bits()
                          | Self::SET.bits() | Self::HASH.bits() | Self::ZSET.bits()
                          | Self::EXPIRED.bits() | Self::EVICTED.bits() | Self::STREAM.bits();
    }
}

impl KeyspaceEventFlags {
    /// Parse a Redis-style flag string into bitflags.
    ///
    /// Returns `None` if the string contains invalid characters.
    pub fn from_flag_string(s: &str) -> Option<Self> {
        let mut flags = Self::empty();
        for ch in s.chars() {
            match ch {
                'K' => flags |= Self::KEYSPACE,
                'E' => flags |= Self::KEYEVENT,
                'g' => flags |= Self::GENERIC,
                '$' => flags |= Self::STRING,
                'l' => flags |= Self::LIST,
                's' => flags |= Self::SET,
                'h' => flags |= Self::HASH,
                'z' => flags |= Self::ZSET,
                'x' => flags |= Self::EXPIRED,
                'e' => flags |= Self::EVICTED,
                't' => flags |= Self::STREAM,
                'm' => flags |= Self::MISS,
                'd' => flags |= Self::MODULE,
                'n' => flags |= Self::NEW,
                'o' => flags |= Self::OVERWRITTEN,
                'c' => flags |= Self::TYPE_CHANGED,
                'A' => flags |= Self::ALL_TYPES,
                _ => return None,
            }
        }
        // If any event type is set but neither K nor E, implicitly enable both
        if flags.intersects(Self::ALL_TYPES | Self::MISS | Self::NEW | Self::OVERWRITTEN | Self::TYPE_CHANGED)
            && !flags.intersects(Self::KEYSPACE | Self::KEYEVENT)
        {
            flags |= Self::KEYSPACE | Self::KEYEVENT;
        }
        Some(flags)
    }

    /// Convert back to a Redis-style flag string.
    pub fn to_flag_string(self) -> String {
        let mut s = String::new();
        if self.contains(Self::KEYSPACE) { s.push('K'); }
        if self.contains(Self::KEYEVENT) { s.push('E'); }
        if self.contains(Self::GENERIC) { s.push('g'); }
        if self.contains(Self::STRING) { s.push('$'); }
        if self.contains(Self::LIST) { s.push('l'); }
        if self.contains(Self::SET) { s.push('s'); }
        if self.contains(Self::HASH) { s.push('h'); }
        if self.contains(Self::ZSET) { s.push('z'); }
        if self.contains(Self::EXPIRED) { s.push('x'); }
        if self.contains(Self::EVICTED) { s.push('e'); }
        if self.contains(Self::STREAM) { s.push('t'); }
        if self.contains(Self::MISS) { s.push('m'); }
        if self.contains(Self::NEW) { s.push('n'); }
        if self.contains(Self::OVERWRITTEN) { s.push('o'); }
        if self.contains(Self::TYPE_CHANGED) { s.push('c'); }
        s
    }

    /// Check if notifications are enabled at all (any type flag set + at least one delivery channel).
    pub fn is_active(self) -> bool {
        let has_type = self.intersects(
            Self::ALL_TYPES | Self::MISS | Self::NEW | Self::OVERWRITTEN | Self::TYPE_CHANGED
        );
        let has_channel = self.intersects(Self::KEYSPACE | Self::KEYEVENT);
        has_type && has_channel
    }
}
```

### Step 2: Add event classification to commands

Add a new trait method to `Command` (in `frogdb-server/crates/core/src/command.rs`):

```rust
/// Which keyspace event category this command belongs to.
///
/// Defaults to `None` (no notification). Override on commands that
/// should emit keyspace notifications.
fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
    None
}
```

Then annotate each command implementation. The mapping from command to event type is:

| Flag | Commands |
|------|----------|
| `GENERIC` | DEL, UNLINK, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, RENAME, RENAMENX, PERSIST, COPY, RESTORE, MOVE |
| `STRING` | SET, SETEX, PSETEX, SETNX, GETSET, GETDEL, GETEX, MSET, MSETNX, APPEND, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, SETRANGE |
| `LIST` | LPUSH, RPUSH, LPOP, RPOP, LINSERT, LSET, LTRIM, RPOPLPUSH, LMOVE, BLPOP, BRPOP, BLMOVE |
| `SET` | SADD, SREM, SPOP, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE |
| `HASH` | HSET, HSETNX, HMSET, HDEL, HINCRBY, HINCRBYFLOAT, HSETEX |
| `ZSET` | ZADD, ZREM, ZINCRBY, ZPOPMIN, ZPOPMAX, ZRANGESTORE, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, BZPOPMIN, BZPOPMAX |
| `STREAM` | XADD, XDEL, XTRIM, XGROUP CREATE, XGROUP DELCONSUMER, XGROUP DESTROY |

### Step 3: Add shared atomic config to `ConfigManager`

In `frogdb-server/crates/server/src/runtime_config.rs`:

```rust
// In ConfigManager struct:
/// Shared keyspace event flags (readable by shard workers without locking).
notify_keyspace_events: Arc<AtomicU32>,

// In ConfigManager::new():
notify_keyspace_events: Arc::new(AtomicU32::new(0)),  // Disabled by default

// Add ParamMeta entry in param_registry():
ParamMeta {
    name: "notify-keyspace-events",
    mutable: true,
    noop: false,
    getter: |mgr| {
        let bits = mgr.notify_keyspace_events.load(Ordering::Relaxed);
        KeyspaceEventFlags::from_bits_truncate(bits).to_flag_string()
    },
    setter: Some(|mgr, val| {
        let flags = KeyspaceEventFlags::from_flag_string(val).ok_or_else(|| {
            ConfigError::InvalidValue {
                param: "notify-keyspace-events".to_string(),
                message: "invalid flag characters".to_string(),
            }
        })?;
        mgr.notify_keyspace_events.store(flags.bits(), Ordering::Relaxed);
        Ok(())
    }),
},

// Public accessor:
pub fn notify_keyspace_events_flags(&self) -> Arc<AtomicU32> {
    self.notify_keyspace_events.clone()
}
```

Also add to `config_param_registry()` in `frogdb-server/crates/config/src/params.rs`:

```rust
ConfigParamInfo {
    name: "notify-keyspace-events",
    section: None,
    field: None,
    mutable: true,
    noop: false,
},
```

### Step 4: Pass event flags to shard workers

In `ShardWorker` (`frogdb-server/crates/core/src/shard/worker.rs`), add:

```rust
/// Shared keyspace notification event flags (from CONFIG).
pub(crate) notify_keyspace_events: Arc<AtomicU32>,
```

Wire this through the shard builder (`frogdb-server/crates/core/src/shard/builder.rs`) from the
`ConfigManager::notify_keyspace_events_flags()`.

### Step 5: Implement notification emission on ShardWorker

Create `frogdb-server/crates/core/src/shard/keyspace_notify.rs`:

```rust
use std::sync::atomic::Ordering;
use bytes::Bytes;
use crate::keyspace_event::KeyspaceEventFlags;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Emit a keyspace notification if the event mask matches.
    ///
    /// This is the core emission point. Called from:
    /// - Post-execution pipeline (write commands)
    /// - Active expiry loop
    /// - Eviction path
    ///
    /// Cost when disabled: one atomic load + branch (< 1ns).
    pub(crate) fn emit_keyspace_notification(
        &self,
        key: &[u8],
        event_name: &str,
        event_type: KeyspaceEventFlags,
    ) {
        let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
        if flags_bits == 0 {
            return; // Fast path: notifications disabled
        }

        let flags = KeyspaceEventFlags::from_bits_truncate(flags_bits);

        // Check if this event type is enabled
        if !flags.intersects(event_type) {
            return;
        }

        let event_bytes = Bytes::from(event_name.to_string());

        // __keyspace@0__:<key> -> event_name
        if flags.contains(KeyspaceEventFlags::KEYSPACE) {
            let channel = Bytes::from(format!(
                "__keyspace@0__:{}",
                String::from_utf8_lossy(key)
            ));
            self.subscriptions.publish(&channel, &event_bytes);
        }

        // __keyevent@0__:<event_name> -> key
        if flags.contains(KeyspaceEventFlags::KEYEVENT) {
            let channel = Bytes::from(format!("__keyevent@0__:{}", event_name));
            let key_bytes = Bytes::copy_from_slice(key);
            self.subscriptions.publish(&channel, &key_bytes);
        }
    }

    /// Emit "new" notification if key did not exist before the command.
    pub(crate) fn emit_new_key_notification(&self, key: &[u8]) {
        self.emit_keyspace_notification(key, "new", KeyspaceEventFlags::NEW);
    }

    /// Emit "overwritten" notification if key existed and was replaced.
    pub(crate) fn emit_overwritten_notification(&self, key: &[u8]) {
        self.emit_keyspace_notification(key, "overwritten", KeyspaceEventFlags::OVERWRITTEN);
    }

    /// Emit "type_changed" notification if key type changed.
    pub(crate) fn emit_type_changed_notification(&self, key: &[u8]) {
        self.emit_keyspace_notification(key, "type_changed", KeyspaceEventFlags::TYPE_CHANGED);
    }
}
```

### Step 6: Hook into the post-execution pipeline

In `frogdb-server/crates/core/src/shard/pipeline.rs`, add a step after version increment
(step 2) and before WAL persistence (step 5):

```rust
// In run_post_execution(), after step 2.5 (client tracking):

// 2.7. Keyspace notifications
self.emit_keyspace_notifications_for_command(handler, args);
```

The helper method determines the event name from the command name and emits accordingly:

```rust
/// Emit keyspace notifications after a write command executes.
fn emit_keyspace_notifications_for_command(&self, handler: &dyn Command, args: &[Bytes]) {
    // Fast path: skip if notifications are disabled entirely
    let flags_bits = self.notify_keyspace_events.load(Ordering::Relaxed);
    if flags_bits == 0 {
        return;
    }

    // Get event type from command trait
    let Some(event_type) = handler.keyspace_event_type() else {
        return;
    };

    let keys = handler.keys(args);
    let event_name = command_to_event_name(handler.name());

    for key in &keys {
        self.emit_keyspace_notification(key, event_name, event_type);
    }
}
```

The `command_to_event_name()` function maps command names to their Redis event names (e.g.,
`SET` -> `"set"`, `LPUSH` -> `"lpush"`, `DEL` -> `"del"`). In most cases it is simply
`handler.name().to_ascii_lowercase()`.

### Step 7: Hook into active expiry

In `frogdb-server/crates/core/src/shard/event_loop.rs`, within `run_active_expiry()`, after
each key is deleted:

```rust
// After: if self.store.delete(&key) {
//   deleted_count += 1;
//   ...existing tracking/search code...

// Emit expired keyspace notification
self.emit_keyspace_notification(&key, "expired", KeyspaceEventFlags::EXPIRED);
```

### Step 8: Hook into eviction

In `frogdb-server/crates/core/src/shard/eviction.rs`, within `delete_for_eviction()`, after
the successful delete:

```rust
// After: if self.store.delete(key) {
//   ...existing metrics code...

// Emit evicted keyspace notification
self.emit_keyspace_notification(key, "evicted", KeyspaceEventFlags::EVICTED);
```

### Step 9: Handle "new", "overwritten", and "type_changed" events

These require checking pre-mutation state. Two approaches:

**Option A (preferred): Track in CommandContext.**
Add fields to `CommandContext`:
```rust
/// Whether the primary key existed before command execution.
pub key_existed_before: bool,
/// The type of the primary key before command execution (if it existed).
pub key_type_before: Option<ValueTypeTag>,
```

Set these in `execute_command_inner()` before calling `handler.execute()`:
```rust
if is_write && !args.is_empty() {
    let key = &args[0]; // Most commands: first arg is the key
    ctx.key_existed_before = self.store.contains(key);
    ctx.key_type_before = self.store.get(key).map(|v| v.type_tag());
}
```

Then in the post-execution notification hook, compare:
- If `!key_existed_before` and key now exists: emit "new"
- If `key_existed_before` and key now exists: emit "overwritten"
- If `key_existed_before` and type changed: emit "type_changed"

**Option B: Snapshot in pipeline.**
Similar to the rollback snapshot, capture minimal state (exists + type) before execution and
compare after. This avoids adding fields to `CommandContext` but requires the same kind of
pre-execution check.

## Integration Points

| Location | What to add |
|----------|-------------|
| `pipeline.rs::run_post_execution()` (after step 2.5) | Call `emit_keyspace_notifications_for_command()` |
| `pipeline.rs::run_post_execution_after_wal()` (same position) | Same call |
| `pipeline.rs::run_transaction_post_execution()` (after step 2) | Loop over write_infos, emit for each |
| `event_loop.rs::run_active_expiry()` (after key delete) | `emit_keyspace_notification(&key, "expired", EXPIRED)` |
| `eviction.rs::delete_for_eviction()` (after key delete) | `emit_keyspace_notification(key, "evicted", EVICTED)` |
| `eviction.rs::demote_for_eviction()` (after demote) | `emit_keyspace_notification(key, "evicted", EVICTED)` |
| `runtime_config.rs` | New `notify-keyspace-events` parameter with `Arc<AtomicU32>` |
| `config/src/params.rs` | Register in `config_param_registry()` |
| `shard/worker.rs` | Add `notify_keyspace_events: Arc<AtomicU32>` field |
| `command.rs` | Add `fn keyspace_event_type()` default method |
| Each command implementation | Override `keyspace_event_type()` returning appropriate flag |

## FrogDB Adaptations

### Expired events

Redis has two expiry mechanisms:
1. **Lazy expiry** -- checked on access (Redis calls this "triggered expire")
2. **Active expiry** -- periodic background sweep (Redis calls this "background expire")

FrogDB works similarly:
- **Passive expiry**: The `HashMapStore` checks TTL on access (`get()`/`contains()`) and returns
  `None` for expired keys (lazy deletion happens on subsequent write or active sweep).
- **Active expiry**: The `run_active_expiry()` tick every 100ms sweeps and deletes expired keys.

For keyspace notifications:
- Emit `"expired"` from `run_active_expiry()` when keys are actively deleted.
- For passive/lazy expiry, emit `"expired"` from the store's access path when a key is found to be
  expired and removed. This requires a callback or return value from `store.get()` indicating a key
  was lazily expired. The simplest approach: have `HashMapStore::get()` return
  `GetResult::Expired(key)` instead of `None` when it lazily expires, and bubble that up to the
  shard to emit the notification.

### Evicted events

Redis emits `evicted` only when a key is deleted due to `maxmemory` policy. FrogDB has the same
concept in `check_memory_for_write() -> evict_one() -> delete_for_eviction()`. Additionally,
FrogDB has `TieredLru`/`TieredLfu` policies that demote keys to warm storage rather than deleting.
Demotions should also emit `"evicted"` for compatibility (the key is gone from the hot tier,
which is what clients observe).

### No RDB/AOF events

Redis emits events for BGSAVE completion, AOF rewrite, etc. FrogDB uses RocksDB + WAL, so these
events do not apply. No `BGSAVE`-related notifications are needed.

### Single database

All channel names use `@0__` since FrogDB does not support multiple databases (SELECT is a no-op
for db 0).

## Tests

- `Keyspace notifications: we receive keyspace notifications`
- `Keyspace notifications: we receive keyevent notifications`
- `Keyspace notifications: we can receive both kind of events`
- `Keyspace notifications: we are able to mask events`
- `Keyspace notifications: general events test`
- `Keyspace notifications: list events test`
- `Keyspace notifications: set events test`
- `Keyspace notifications: zset events test`
- `Keyspace notifications: hash events test ($type)`
- `Keyspace notifications: stream events test`
- `Keyspace notifications:FXX/FNX with HSETEX cmd`
- `Keyspace notifications: expired events (triggered expire)`
- `Keyspace notifications: expired events (background expire)`
- `Keyspace notifications: evicted events`
- `Keyspace notifications: test CONFIG GET/SET of event flags`
- `Keyspace notifications: new key test`
- `Keyspace notifications: overwritten events - string to string`
- `Keyspace notifications: type_changed events - hash to string`
- `Keyspace notifications: both overwritten and type_changed events`
- `Keyspace notifications: configuration flags work correctly`
- `Keyspace notifications: RESTORE REPLACE different type - restore, overwritten and type_changed events`
- `Keyspace notifications: SET on existing string key - overwritten event`
- `Keyspace notifications: setKey on existing different type key - overwritten and type_changed events`
- `Keyspace notifications: overwritten and type_changed events for RENAME and COPY commands`
- `Keyspace notifications: overwritten and type_changed for *STORE* commands`

## Verification

### Unit tests

1. `KeyspaceEventFlags::from_flag_string()` -- parse/roundtrip for all flag characters, "A" alias,
   implicit K+E addition, invalid chars return `None`, empty string returns empty flags.
2. `emit_keyspace_notification()` -- mock subscriptions, verify correct channel name formatting and
   event delivery with various flag combinations.
3. `command_to_event_name()` -- spot-check mapping for ~10 commands.

### Integration tests

1. Run the 25 regression tests from `pubsub_tcl.rs` (currently excluded as
   `intentional-incompatibility:config`). Remove the exclusion tag after implementation.
2. Verify that with `notify-keyspace-events ""` (disabled), no messages are published even when
   subscribers exist on `__keyspace@0__:*` patterns.
3. Verify selective masking: set `Kl` and confirm only list-related keyspace events fire (no
   keyevent, no string events).
4. Verify expired events: set a key with 100ms TTL, subscribe, confirm `"expired"` event arrives
   within ~200ms.
5. Verify evicted events: set `maxmemory 1` + `maxmemory-policy allkeys-lru`, write keys, confirm
   `"evicted"` events.

### Performance validation

Benchmark with notifications disabled (default) to confirm zero overhead:
```bash
just bench -- --test keyspace-overhead
```
Compare throughput with `notify-keyspace-events ""` vs `notify-keyspace-events "KEA"` under load
to quantify the cost of emission when enabled.
