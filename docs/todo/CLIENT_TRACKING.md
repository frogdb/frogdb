# Client Tracking (Client-Side Cache Invalidation)

Implementation spec for Redis-compatible [CLIENT TRACKING](https://redis.io/docs/latest/commands/client-tracking/).

**Status**: Not yet implemented (stubs exist for `CLIENT TRACKINGINFO`, `CLIENT GETREDIR`,
`CLIENT CACHING`).

---

## Overview

Client tracking enables server-assisted client-side caching. When a client reads a key, the
server records the association. When that key is later modified, the server sends an invalidation
message so the client can evict its local cache entry. This dramatically reduces read latency and
server load for read-heavy workloads.

**Wire format** — Invalidation messages are delivered as RESP3 Push frames:

```
>2
$10
invalidate
*1
$3
foo
```

RESP2 clients receive invalidations through a dedicated Pub/Sub channel `__redis__:invalidate`
on a second connection (via `REDIRECT`).

---

## Redis Compatibility

### `CLIENT TRACKING` Syntax

```
CLIENT TRACKING <ON|OFF> [REDIRECT client-id] [PREFIX prefix [...]]
                         [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
```

### Modes

| Mode | Description | Implementation Priority |
|------|-------------|------------------------|
| **Default** | Track keys that a connection reads; invalidate when any client modifies them | Phase 1 |
| **BCAST** | Broadcast invalidations for all keys matching registered prefixes (no per-read tracking) | Phase 2 |
| **OPTIN** | Only track keys after `CLIENT CACHING YES` (default: don't track) | Phase 1 |
| **OPTOUT** | Track all reads except after `CLIENT CACHING NO` | Phase 1 |
| **REDIRECT** | Send invalidations to a different connection (for RESP2 clients) | Phase 2 |
| **NOLOOP** | Don't send invalidations to the connection that performed the write | Phase 1 |

### Related Commands

| Command | Current State | Change Needed |
|---------|--------------|---------------|
| `CLIENT TRACKING ON/OFF` | Returns `ERR unknown subcommand` | Full implementation |
| `CLIENT CACHING YES/NO` | Stub, accepts and ignores | Wire to tracking state |
| `CLIENT TRACKINGINFO` | Stub, returns "off" | Read from real state |
| `CLIENT GETREDIR` | Stub, returns -1 | Read from real state |

---

## Architecture

### Per-Shard Tracking Table

Each `ShardWorker` owns a `TrackingTable` that maps keys to sets of connection IDs. This is the
natural fit because:

1. **No cross-shard locking** — writes modify only the local shard's table.
2. **Write hook is local** — `execute_command()` already has the key list and conn_id.
3. **Bounded memory** — each shard independently enforces its LRU limit.

```
ShardWorker                          ConnectionHandler
┌─────────────────────────┐          ┌──────────────────────┐
│  store: HashMapStore    │          │  state.tracking:     │
│  tracking: TrackingTable│──────────│    TrackingState      │
│                         │          │  invalidation_tx     │
│  On write:              │          │                      │
│   1. execute command    │  Push    │  On recv:             │
│   2. lookup key in      │ ──────> │   send >invalidate   │
│      tracking table     │  (mpsc) │   to client           │
│   3. send invalidation  │          │                      │
│      to each conn_id    │          └──────────────────────┘
└─────────────────────────┘
```

### Data Structures

```rust
// crates/core/src/tracking.rs (new file)

/// Per-shard tracking table mapping keys → interested connections.
pub struct TrackingTable {
    /// key → set of (conn_id, noloop: bool)
    key_to_clients: HashMap<Bytes, HashSet<TrackedClient>>,

    /// LRU eviction: when table exceeds max_keys, evict oldest entries
    /// and send bulk invalidation (key=None → "flush all").
    lru_order: VecDeque<Bytes>,

    /// Maximum tracked keys (configurable, default 1M).
    max_keys: usize,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct TrackedClient {
    conn_id: u64,
    noloop: bool,
}

/// Per-shard sender registry: conn_id → invalidation sender.
/// Populated when a connection enables tracking.
pub struct InvalidationRegistry {
    senders: HashMap<u64, InvalidationSender>,
}

/// Sender handle for delivering invalidation messages to a connection.
pub type InvalidationSender = mpsc::UnboundedSender<InvalidationMessage>;

/// Message delivered to a connection for cache invalidation.
#[derive(Debug, Clone)]
pub enum InvalidationMessage {
    /// Invalidate specific keys.
    Invalidate(Vec<Bytes>),
    /// Flush all — sent when LRU eviction can't enumerate individual keys,
    /// or when the connection disables tracking.
    FlushAll,
}
```

### Connection-Level State

```rust
// Added to ConnectionState (crates/server/src/connection/state.rs)

pub struct TrackingState {
    /// Whether tracking is enabled.
    pub enabled: bool,

    /// Tracking mode.
    pub mode: TrackingMode,

    /// REDIRECT target connection ID (0 = self).
    pub redirect: u64,

    /// BCAST prefix filters (empty = all keys).
    pub prefixes: Vec<Bytes>,

    /// NOLOOP flag.
    pub noloop: bool,

    /// OPTIN/OPTOUT per-command override.
    /// Set by CLIENT CACHING YES/NO, consumed and reset after next read command.
    pub caching_override: Option<bool>,
}

pub enum TrackingMode {
    /// Default: track keys that this connection reads.
    Default,
    /// BCAST: receive invalidations for all keys matching prefixes.
    Broadcast,
    /// OPTIN: only track after CLIENT CACHING YES.
    OptIn,
    /// OPTOUT: track everything except after CLIENT CACHING NO.
    OptOut,
}
```

### Message Flow

#### Default Mode (Phase 1)

```
Client A                    Shard                      Client B
   │                          │                           │
   │  GET foo ───────────────>│                           │
   │  <─── "bar"              │                           │
   │                          │ record: foo → {A}         │
   │                          │                           │
   │                          │<──────────── SET foo baz  │
   │                          │  execute_command()        │
   │                          │  lookup foo in tracking   │
   │                          │  send Invalidate([foo])   │
   │  <── >invalidate [foo]   │  to conn A                │
   │  (RESP3 Push)            │  remove foo → {A}         │
   │                          │                           │
```

#### BCAST Mode (Phase 2)

```
Client A (BCAST PREFIX user:)      Shard
   │                                 │
   │  CLIENT TRACKING ON BCAST       │
   │    PREFIX user: ───────────────>│
   │  <─── OK                        │ register: prefix "user:" → {A}
   │                                 │
   │                          Client B SET user:42 "new" ──>│
   │                                 │  "user:42" matches prefix "user:"
   │  <── >invalidate [user:42]      │  send to A
   │                                 │
```

#### REDIRECT Mode (Phase 2)

```
Client A (RESP2)     Client B (RESP2, subscribed to __redis__:invalidate)
   │                    │
   │  CLIENT TRACKING ON REDIRECT <B_id> ──> Shard
   │  <─── OK                                  │
   │                                           │
   │  GET foo ────────────────────────────────>│
   │  <─── "bar"                               │ record: foo → {A, redirect=B}
   │                                           │
   │                     Client C SET foo ────>│
   │                                           │ lookup foo → redirect to B
   │                    <── PUBLISH __redis__:invalidate [foo]
   │                    (received via SUBSCRIBE)
```

### Write Hook Integration

The invalidation hook goes in `ShardWorker::execute_command()` at
`crates/core/src/shard/execution.rs`, inside the existing `if is_write` block (after line 103).
This is the same location that already handles:
- Version increment (line 104)
- Dirty tracking (lines 109-116)
- Blocking waiter satisfaction (lines 119-143)
- WAL persistence (line 146)
- Replication broadcast (lines 151-154)

```rust
// After version increment, before WAL persistence:
if is_write {
    self.increment_version();
    // ... dirty tracking ...

    // NEW: Invalidate tracked keys
    let keys = handler.keys(&command.args);
    self.invalidate_tracked_keys(&keys, conn_id);

    // ... blocking waiters, WAL, replication (unchanged) ...
}
```

### Read Hook Integration

Read tracking goes in `execute_command()` after command execution (after line 79), for read
commands in Default/OptIn/OptOut modes:

```rust
// After command execution, before keyspace hit/miss tracking:
if !is_write && self.tracking_table.is_some() {
    let keys = handler.keys(&command.args);
    self.record_tracked_read(&keys, conn_id);
}
```

### Connection Lifecycle

When a connection closes (`ShardMessage::ConnectionClosed`), the shard must remove all entries
for that conn_id from the tracking table and invalidation registry. This mirrors the existing
pub/sub cleanup path.

---

## Key Design Decisions

### Why Per-Shard (Not Global)

A global tracking table would require cross-shard synchronization on every write. Since keys
are already partitioned across shards, each shard can independently track and invalidate its own
keys. This is the same pattern used for pub/sub subscriptions
(`ShardWorker::subscriptions: ShardSubscriptions`).

### Why a Dedicated Channel (Not Pub/Sub)

Invalidation delivery uses a dedicated `mpsc::UnboundedSender<InvalidationMessage>` per
connection rather than the existing pub/sub infrastructure because:

1. **Different semantics** — invalidation messages use RESP3 Push `>invalidate`, not pub/sub
   message framing.
2. **No subscription management** — invalidation delivery is controlled by `CLIENT TRACKING`,
   not SUBSCRIBE/UNSUBSCRIBE.
3. **Per-key routing** — default mode routes to specific connections that read specific keys,
   unlike pub/sub's channel-based fan-out.
4. **Simpler cleanup** — on `CLIENT TRACKING OFF`, just drop the sender and flush the table
   entries for that connection.

For RESP2 REDIRECT mode, invalidations *are* delivered through pub/sub on
`__redis__:invalidate`, reusing the existing pub/sub infrastructure.

### Bounded LRU Table

Redis caps the tracking table at a configurable number of keys (default: no limit, but memory
pressure causes eviction). FrogDB should cap per-shard at `tracking_table_max_keys` (default 1M
keys). When the table is full and a new key is tracked:

1. Evict the oldest key from the LRU.
2. Send an invalidation for that key to all registered connections.

If mass eviction occurs (e.g., table shrinks by >50%), send a flush-all invalidation
(`invalidate` with a null key array) to force clients to clear their entire cache.

### FLUSHDB / FLUSHALL Handling

These commands invalidate *all* keys. Rather than iterating the tracking table, send a single
flush-all invalidation message to every connection with tracking enabled. This matches Redis
behavior.

---

## Implementation Phases

### Phase 1: Default Mode + OPTIN/OPTOUT + NOLOOP

Core tracking with the most common mode. Sufficient for most client libraries
(`redis-py`, `lettuce`, `Jedis`).

#### Step 1: Tracking Data Structures

Create `crates/core/src/tracking.rs`:
- `TrackingTable` with `record_read()`, `invalidate_keys()`, `remove_connection()`,
  `evict_lru()`.
- `InvalidationRegistry` with `register()`, `unregister()`, `send()`.
- `InvalidationMessage` enum.
- Unit tests for table operations, LRU eviction, connection cleanup.

Register the module in `crates/core/src/lib.rs`.

#### Step 2: Connection State

Add `TrackingState` to `ConnectionState` in `crates/server/src/connection/state.rs`:
- `tracking: TrackingState` field with `Default` impl (disabled).
- Add `invalidation_tx: Option<InvalidationSender>` to `ConnectionHandler` in
  `crates/server/src/connection.rs` (alongside existing `pubsub_tx`).

#### Step 3: `CLIENT TRACKING ON/OFF`

Implement `handle_client_tracking()` in `crates/server/src/connection/handlers/client.rs`:
- Parse ON/OFF and option flags (NOLOOP, OPTIN, OPTOUT).
- ON: set `state.tracking`, create `invalidation_tx`/`invalidation_rx` channel pair,
  register sender in each shard's `InvalidationRegistry` via a new `ShardMessage::TrackingRegister`.
- OFF: set `state.tracking.enabled = false`, send `ShardMessage::TrackingUnregister` to
  all shards, drop sender.
- Add `"TRACKING"` case to the `match` in `handle_client_command()` (line 101).

#### Step 4: Wire Up `CLIENT CACHING`, `CLIENT TRACKINGINFO`, `CLIENT GETREDIR`

Replace stubs with real implementations:
- `handle_client_caching()`: set `state.tracking.caching_override`.
- `handle_client_trackinginfo()`: read from `state.tracking`.
- `handle_client_getredir()`: return `state.tracking.redirect` (or -1 if no redirect).

#### Step 5: ShardMessage Variants

Add to `ShardMessage` in `crates/core/src/shard/message.rs`:

```rust
// Client tracking messages
TrackingRegister {
    conn_id: ConnId,
    sender: InvalidationSender,
    noloop: bool,
},
TrackingUnregister {
    conn_id: ConnId,
},
```

Update the shard event loop in `crates/core/src/shard/event_loop.rs` to process
these variants.

#### Step 6: Write Hook

In `ShardWorker::execute_command()` (`crates/core/src/shard/execution.rs`):
- After `self.increment_version()` (line 104), call `self.invalidate_tracked_keys()`.
- This method looks up each written key in `TrackingTable`, sends `InvalidationMessage`
  to each registered conn_id (skipping the writer if NOLOOP), then removes the entries.

#### Step 7: Read Hook

In `ShardWorker::execute_command()`:
- After command execution (line 79), for non-write commands, call
  `self.record_tracked_read()`.
- This records `(key, conn_id)` in the tracking table if the connection has
  tracking enabled and the OPTIN/OPTOUT override allows it.

Note: the shard needs to know whether a connection has tracking enabled and in what mode.
The `TrackingRegister` message carries this info, and the shard stores it in the
`InvalidationRegistry`.

#### Step 8: Invalidation Delivery Loop

In `ConnectionHandler::run()` (`crates/server/src/connection.rs`):
- Add `invalidation_rx` to the main `tokio::select!` loop (alongside `pubsub_rx`).
- On receiving `InvalidationMessage::Invalidate(keys)`:
  - RESP3: send `Response::Push([bulk("invalidate"), Array(keys)])`.
  - RESP2 with REDIRECT: N/A in phase 1.
- On receiving `InvalidationMessage::FlushAll`:
  - Send `Response::Push([bulk("invalidate"), Null])`.

#### Step 9: Connection Cleanup

Extend the existing `ConnectionClosed` handler to also call
`tracking_table.remove_connection(conn_id)` and
`invalidation_registry.unregister(conn_id)`.

### Phase 2: BCAST + REDIRECT

#### Step 10: BCAST Mode

- Add a `BroadcastTable` to `TrackingTable`: `prefix → set of conn_id`.
- On write, check all registered prefixes against the written key.
- No per-read tracking needed — all matching keys trigger invalidation.
- Parse `PREFIX` arguments in `CLIENT TRACKING ON BCAST PREFIX ...`.

#### Step 11: REDIRECT Mode

- Parse `REDIRECT client-id` in `CLIENT TRACKING ON`.
- Validate the target connection exists in the `ClientRegistry`.
- On invalidation, deliver to the redirect target via the pub/sub channel
  `__redis__:invalidate` (using the existing pub/sub infrastructure at shard 0).
- Handle the case where the redirect target disconnects (disable tracking for the
  source connection or queue invalidations).

### Phase 3: Observability & Configuration

#### Step 12: Metrics

- `frogdb_tracking_keys_total` gauge (per shard)
- `frogdb_tracking_invalidations_total` counter (per shard)
- `frogdb_tracking_connections_total` gauge
- `frogdb_tracking_lru_evictions_total` counter (per shard)

#### Step 13: Configuration

Add to server config (`crates/server/src/config/mod.rs`):
- `tracking_table_max_keys: usize` (default 1,000,000)
- `tracking_enabled: bool` (default true, allows disabling globally)

---

## File Change Summary

| File | Change | Phase |
|------|--------|-------|
| `crates/core/src/tracking.rs` | **New** — TrackingTable, InvalidationRegistry, InvalidationMessage | 1 |
| `crates/core/src/lib.rs` | Add `pub mod tracking` | 1 |
| `crates/core/src/shard/worker.rs` | Add `tracking: TrackingTable` and `invalidation_registry: InvalidationRegistry` fields | 1 |
| `crates/core/src/shard/message.rs` | Add `TrackingRegister`, `TrackingUnregister` variants | 1 |
| `crates/core/src/shard/execution.rs` | Write hook (`invalidate_tracked_keys`) and read hook (`record_tracked_read`) | 1 |
| `crates/core/src/shard/event_loop.rs` | Handle new `ShardMessage` variants | 1 |
| `crates/core/src/shard/builder.rs` | Initialize tracking table in builder | 1 |
| `crates/server/src/connection/state.rs` | Add `TrackingState`, `TrackingMode` | 1 |
| `crates/server/src/connection.rs` | Add `invalidation_tx`/`invalidation_rx`, delivery loop in `select!` | 1 |
| `crates/server/src/connection/handlers/client.rs` | Implement `handle_client_tracking()`, update stubs | 1 |
| `crates/server/src/connection/dispatch.rs` | No change needed (CLIENT subcommands already dispatch through `handle_client_command`) | — |
| `crates/protocol/src/response.rs` | No change needed (`WireResponse::Push` already exists) | — |
| `crates/core/src/pubsub.rs` | No change needed (REDIRECT uses existing pub/sub in Phase 2) | — |
| `crates/server/src/config/mod.rs` | Add `tracking_table_max_keys`, `tracking_enabled` to `Config` | 3 |
| `crates/redis-regression/tests/tracking_regression.rs` | Expand: verify invalidation delivery, OPTIN/OPTOUT, NOLOOP | 1 |
| `crates/server/tests/integration_client.rs` | Add integration tests for tracking lifecycle | 1 |
| `docs/spec/PROTOCOL.md` | Update "Deferred" → "Implemented" in status tables | 1 |
| `docs/spec/COMPATIBILITY.md` | Update tracking status | 1 |
| `docs/todo/NOT_YET_IMPLEMENTED.md` | Remove tracking entry | 1 |

---

## Verification Plan

### Unit Tests (`crates/core/src/tracking.rs`)

- `record_read` adds entry, `invalidate_keys` removes and returns connections
- LRU eviction triggers invalidation of oldest key
- `remove_connection` clears all entries for a conn_id
- Duplicate reads for same key+conn_id are idempotent
- NOLOOP: writer conn_id excluded from invalidation set

### Integration Tests (`crates/server/tests/integration_client.rs`)

1. **Basic invalidation** — client A enables tracking, GETs key, client B SETs key,
   verify client A receives `>invalidate [key]`.
2. **OPTIN** — tracking ON OPTIN, GET without CACHING YES → no invalidation.
   CLIENT CACHING YES then GET → invalidation on write.
3. **OPTOUT** — tracking ON OPTOUT, GET → invalidation. CLIENT CACHING NO then
   GET → no invalidation.
4. **NOLOOP** — client writes key it previously read → no self-invalidation.
5. **Multi-key** — MGET tracks all keys, verify each invalidated independently.
6. **Disable tracking** — CLIENT TRACKING OFF, verify no more invalidations.
7. **Connection disconnect** — enable tracking, disconnect, verify no shard leak.
8. **FLUSHDB** — verify flush-all invalidation sent.

### Regression Tests (`crates/redis-regression/tests/tracking_regression.rs`)

- Expand existing `client_tracking_on_off` test to verify OK response.
- CLIENT TRACKINGINFO returns correct mode/flags after enable.

### Build Verification

```bash
just check                           # type-check
just lint                            # clippy clean
just test frogdb-core                # unit tests
just test frogdb-server              # integration tests
just test frogdb-redis-regression    # regression tests
```

---

## Cross-References

- [PROTOCOL.md](../spec/PROTOCOL.md) — RESP3 types, Push frame format, client tracking status
- [COMPATIBILITY.md](../spec/COMPATIBILITY.md) — feature status table
- [PUBSUB.md](../spec/PUBSUB.md) — pub/sub infrastructure (reused for REDIRECT mode)
- [NOT_YET_IMPLEMENTED.md](NOT_YET_IMPLEMENTED.md) — deferred features list
