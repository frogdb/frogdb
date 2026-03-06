# FrogDB Blocking Commands

This document details the design for blocking commands (BLPOP, BRPOP, BLMOVE, etc.) in FrogDB's shared-nothing architecture.


## Industry Comparison

| Aspect | Redis/Valkey | DragonflyDB | FrogDB |
|--------|--------------|-------------|--------|
| Architecture | Single-threaded | Multi-threaded, shared-nothing | Multi-threaded, shared-nothing |
| Blocking mechanism | `beforeSleep()` phase rechecks | Per-thread wait queues | Per-shard wait queues |
| Multi-key ordering | First non-empty key in BLPOP order | Non-deterministic (parallel exec) | First non-empty key in order |
| Cross-shard blocking | N/A (single thread) | Keys must be on same thread | Keys must be on same shard |
| Fairness | FIFO by blocking time | FIFO by blocking time | FIFO by blocking time |

### Redis Implementation Details

Redis handles blocking in the event loop's `beforeSleep()` phase:

1. When `LPUSH` adds data, Redis marks clients blocked on that key as "ready"
2. Before going idle, Redis rechecks all ready clients
3. Clients are served in FIFO order (longest-waiting first)
4. If multiple keys listed in BLPOP, the **order in the command** determines priority

> *"After the writes occur, Redis reprocesses the blocking command for the client and pops from the first non-empty key in the key list."* - [Redis BLPOP Docs](https://redis.io/docs/latest/commands/blpop/)

### DragonflyDB Differences

DragonflyDB's multi-threaded architecture introduces non-determinism:

> *"Dragonfly can potentially parallelize the execution of a single MULTI/EXEC transaction or script, which makes it impossible to determine what key will receive a new element first."*

- With `multi_exec_squash=true` (default): Key ordering in BLPOP may not match data arrival order
- With `multi_exec_squash=false`: Redis-compatible ordering restored

### FrogDB Approach

FrogDB follows the **Redis model** for semantics but with **DragonflyDB-style architecture**:

- Per-shard wait queues (similar to DragonflyDB's per-thread model)
- Cross-shard blocking rejected with `-CROSSSLOT` error (use hash tags for colocation)
- FIFO fairness within each shard
- Key ordering in BLPOP command respected (no parallel execution of blocking checks)

---

## Overview

Blocking commands allow clients to wait for data to become available:

| Command | Description |
|---------|-------------|
| BLPOP | Blocking left pop from list |
| BRPOP | Blocking right pop from list |
| BLMOVE | Blocking list move |
| BRPOPLPUSH | Blocking pop + push (deprecated) |
| BZPOPMIN | Blocking pop minimum from sorted set |
| BZPOPMAX | Blocking pop maximum from sorted set |
| BLMPOP | Blocking pop from multiple lists |
| BZMPOP | Blocking pop from multiple sorted sets |

---

## Design Considerations

### Per-Connection Blocking State

```rust
struct BlockedConnection {
    keys: Vec<Bytes>,           // Keys being waited on
    timeout: Option<Instant>,   // When to unblock
    shard_id: usize,            // Owning shard
    op: BlockingOp,             // Operation type
    response_tx: oneshot::Sender<Response>,
}

enum BlockingOp {
    BLPop,
    BRPop,
    BLMove { dest: Bytes, src_dir: Direction, dest_dir: Direction },
    BZPopMin,
    BZPopMax,
}
```

### Challenges in Shared-Nothing Architecture

1. **Shard Mismatch**: Keys may be on different shards than the blocking connection's home thread
2. **Timeout Management**: Distributed timeout handling across threads
3. **Cross-Shard Notification**: Notify blocking connections when keys become available
4. **Fairness**: Multiple clients waiting on same key

---

## Architecture

### Single-Shard Blocking

```
Client sends: BLPOP mylist 0
         │
         ▼
    Connection Handler (Thread N)
         │
         ├── 1. Validate keys on same shard
         │
         ├── 2. Check if data exists → return immediately if so
         │
         ├── 3. Register with target shard's wait queue
         │      └── Send BlockWait message to Shard M
         │
         ├── 4. Wait on response channel
         │      └── Either: data arrives, timeout, or disconnect
         │
         └── 5. Return result to client
```

### Wait Queue per Shard

```rust
struct ShardWaitQueue {
    /// Keys with waiting connections, FIFO order per key
    waiters: HashMap<Bytes, VecDeque<WaitEntry>>,
}

struct WaitEntry {
    conn_id: u64,
    keys: Vec<Bytes>,           // All keys this connection is waiting on
    op: BlockingOp,
    response_tx: oneshot::Sender<Response>,
    deadline: Option<Instant>,
}
```

### Registration Flow (Detailed)

The complete flow for registering a blocked connection with a shard:

```
Connection Handler                           Target Shard
       │                                           │
       │  1. Receive BLPOP key1 key2 timeout       │
       │                                           │
       ├──► 2. Validate all keys hash to same shard
       │                                           │
       ├──► 3. Check if any key has data           │
       │     └── If yes: pop and return immediately
       │                                           │
       │  4. Create oneshot channel (tx, rx)       │
       │                                           │
       │  5. Send BlockWait message ──────────────►│
       │     BlockWait {                           │
       │       conn_id,                            │
       │       keys: [key1, key2],                 │
       │       op: BLPop,                          │
       │       response_tx: tx,                    │
       │       deadline: Some(Instant),            │
       │     }                                     │
       │                                           │
       │                                    6. Shard adds WaitEntry
       │                                       to wait queue for EACH key
       │                                           │
       ├──► 7. Await rx with timeout_at(deadline)  │
       │                                           │
       │     ... time passes ...                   │
       │                                           │
       │  8a. Data arrives (LPUSH) ◄───────────────│
       │      Response sent via tx                 │
       │      Shard removes entry from all keys    │
       │                                           │
       │  8b. OR timeout fires                     │
       │      Connection sends UnregisterWait ────►│
       │      Shard removes entry from all keys    │
       │      Return nil to client                 │
       │                                           │
       │  8c. OR connection closes                 │
       │      Connection handler drops             │
       │      Shard detects closed tx, cleans up   │
```

**Message Types:**

```rust
enum ShardMessage {
    // ... existing variants ...

    /// Register a blocking wait
    BlockWait {
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        response_tx: oneshot::Sender<Response>,
        deadline: Option<Instant>,
    },

    /// Cancel a blocking wait (timeout or disconnect)
    UnregisterWait {
        conn_id: u64,
    },
}
```

**Error Handling:**

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Shard channel full | `send()` returns `Err` | Return `-BUSY` error to client |
| Invalid shard | Key hash doesn't match | Return `-MOVED` error |
| Shard shutdown | Channel closed | Return `-SHUTDOWN` error |

### Notification Flow

```
Client sends: LPUSH mylist "value"
         │
         ▼
    Shard M executes LPUSH
         │
         ├── 1. Push value to list
         │
         ├── 2. Check wait queue for "mylist"
         │
         ├── 3. If waiter exists:
         │      └── Pop value, send to oldest waiter
         │
         └── 4. Return push count to pushing client
```

---

## Cross-Shard Blocking

**Not supported.** Blocking on keys from different shards would require distributed coordination that defeats the shared-nothing model.

```
BLPOP key1 key2 0  # Fails if key1 and key2 on different shards
```

**Error:** `-CROSSSLOT Keys in request don't hash to the same slot`

**Solution:** Use hash tags for multi-key blocking:

```
BLPOP {queue}:high {queue}:low 0
```

---

## Timeout Handling

### Per-Connection Timeout

```rust
async fn handle_blocking_command(
    conn: &mut Connection,
    keys: &[Bytes],
    timeout: Duration,
) -> Response {
    let deadline = if timeout.is_zero() {
        None  // Wait forever
    } else {
        Some(Instant::now() + timeout)
    };

    let (tx, rx) = oneshot::channel();

    // Register with shard
    shard.register_waiter(keys, tx, deadline).await;

    // Wait with timeout
    match timeout_at(deadline, rx).await {
        Ok(response) => response,
        Err(_) => {
            // Timeout - unregister and return nil
            shard.unregister_waiter(conn.id).await;
            Response::Null
        }
    }
}
```

### Cleanup on Disconnect

When a connection closes while blocked:
1. Connection handler detects close
2. Sends unregister message to all shards with wait entries
3. Shards remove entries from wait queues

---

## Fairness

Wait queues are processed FIFO:

1. Oldest waiter gets data first
2. If multiple keys waited on, first key with data wins
3. Timeout ordering is independent (each waiter has own deadline)

---

## Command-Specific Behavior

### BLPOP / BRPOP

```
BLPOP key [key ...] timeout
```

- Pop from first non-empty list
- Return: `[key, value]` or nil on timeout
- Keys checked in order provided

### BLMOVE

```
BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| source | Key to pop from (must be list) |
| destination | Key to push to (must be list or non-existent) |
| wherefrom | `LEFT` or `RIGHT` - which end to pop from |
| whereto | `LEFT` or `RIGHT` - which end to push to |
| timeout | Seconds to wait (0 = forever), can be decimal |

**Behavior:**

| Aspect | Behavior |
|--------|----------|
| Returns | Element moved, or nil on timeout |
| Atomicity | Pop and push are atomic (no interleaving) |
| Same key | Allowed - rotates elements within list |
| Type mismatch (dest) | `-WRONGTYPE` error |
| Cross-shard | `-CROSSSLOT` error (use hash tags) |

**Direction Enum:**

```rust
pub enum Direction {
    Left,
    Right,
}
```

**Example:**

```
# Move from right of source to left of destination
BLMOVE myqueue processing RIGHT LEFT 10

# Rotate within same list (move tail to head)
BLMOVE mylist mylist RIGHT LEFT 0
```

### BZPOPMIN / BZPOPMAX

```
BZPOPMIN key [key ...] timeout
BZPOPMAX key [key ...] timeout
```

- Pop element with min/max score from first non-empty sorted set
- Return: `[key, member, score]` or nil on timeout
- Keys checked in order provided
- All keys must be on same shard

### BLMPOP

```
BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| timeout | Seconds to wait (0 = forever), can be decimal |
| numkeys | Number of keys to check |
| key | One or more list keys |
| LEFT\|RIGHT | Which end to pop from |
| COUNT | Number of elements to pop (default: 1) |

**Behavior:**

| Aspect | Behavior |
|--------|----------|
| Returns | `[key, [element, ...]]` or nil on timeout |
| Key order | Checked in order provided, first non-empty wins |
| COUNT | Pops up to COUNT elements from winning key |
| Empty lists | Skipped (not deleted) |
| Cross-shard | `-CROSSSLOT` error (use hash tags) |

**Example:**

```
# Pop up to 3 elements from left of first non-empty list
BLMPOP 10 2 queue:high queue:low LEFT COUNT 3

# Returns: ["queue:high", ["task1", "task2"]]
```

**Comparison with BLPOP:**

| Aspect | BLPOP | BLMPOP |
|--------|-------|--------|
| Multi-element | No (1 element) | Yes (COUNT option) |
| Direction | Left only | LEFT or RIGHT |
| Timeout position | Last argument | First argument |

### BZMPOP

```
BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| timeout | Seconds to wait (0 = forever), can be decimal |
| numkeys | Number of keys to check |
| key | One or more sorted set keys |
| MIN\|MAX | Pop minimum or maximum score elements |
| COUNT | Number of elements to pop (default: 1) |

**Behavior:**

| Aspect | Behavior |
|--------|----------|
| Returns | `[key, [[member, score], ...]]` or nil on timeout |
| Key order | Checked in order provided, first non-empty wins |
| COUNT | Pops up to COUNT elements from winning key |
| Cross-shard | `-CROSSSLOT` error (use hash tags) |

**Example:**

```
# Pop 2 elements with minimum scores
BZMPOP 5 2 leaderboard:daily leaderboard:weekly MIN COUNT 2

# Returns: ["leaderboard:daily", [["player1", "100"], ["player2", "150"]]]
```

### BRPOPLPUSH (Deprecated)

```
BRPOPLPUSH source destination timeout
```

**Status:** Deprecated in favor of BLMOVE.

**Behavior:** Equivalent to `BLMOVE source destination RIGHT LEFT timeout`

**Recommendation:** Use BLMOVE for new code. BRPOPLPUSH remains for backwards compatibility.

---

## Connection State Integration

Blocking state integrates with connection state machine:

```
┌─────────────────┐
│     NORMAL      │
│  (default mode) │
└───────┬─────────┘
        │
        │ BLPOP/BRPOP/etc.
        ▼
 ┌─────────────┐
 │   BLOCKED   │ ←── Connection waits here
 │  (waiting)  │
 └──────┬──────┘
        │
        │ Data arrives / Timeout / Disconnect
        ▼
 ┌─────────────┐
 │   NORMAL    │
 └─────────────┘
```

While blocked:
- Connection cannot execute other commands
- PING is allowed (keepalive)
- QUIT causes disconnect and wait cancellation

---

## Command Interaction Matrix

How blocking commands interact with other FrogDB features:

### Transactions (MULTI/EXEC)

| Scenario | Behavior |
|----------|----------|
| BLPOP inside MULTI | **Error:** `-ERR BLPOP inside MULTI is not allowed` |
| MULTI while blocked | Not possible (connection is blocked) |
| WATCH + BLPOP | Not supported (different connection states) |

**Rationale:** Blocking commands wait indefinitely, which would freeze the transaction queue. Use non-blocking alternatives (LPOP) inside transactions.

### Lua Scripts

| Scenario | Behavior |
|----------|----------|
| `redis.call("BLPOP", ...)` in script | **Error:** `-ERR blocking commands not allowed in scripts` |
| Script after unblock | Normal execution resumes |

**Rationale:** Blocking inside Lua would freeze the entire shard's event loop. Scripts must use non-blocking alternatives.

### Pub/Sub Mode

| Scenario | Behavior |
|----------|----------|
| BLPOP in pub/sub mode | **Error:** `-ERR only SUBSCRIBE, UNSUBSCRIBE, PING allowed in this context` |
| SUBSCRIBE while blocked | Not possible (connection is blocked) |

### Connection Commands

| Command | Behavior While Blocked |
|---------|------------------------|
| PING | Allowed, returns PONG immediately |
| QUIT | Unblocks, closes connection gracefully |
| CLIENT KILL (external) | Unblocks, connection terminated |
| AUTH | Not allowed |
| SELECT | Not allowed |

**PING Implementation:**

```rust
// PING is handled at connection level, not shard level
// It bypasses the blocked state check
fn handle_command_while_blocked(cmd: &str) -> Result<(), Error> {
    match cmd {
        "PING" => Ok(()),  // Allow
        "QUIT" => Ok(()),  // Allow (triggers cleanup)
        _ => Err(Error::Blocked),
    }
}
```

---

## Edge Cases (Redis-Compatible)

FrogDB matches Redis behavior for all blocking command edge cases:

### Key Does Not Exist

| Scenario | Behavior |
|----------|----------|
| BLPOP on non-existent key | Block until key is created and has data |
| Key created as wrong type | `-WRONGTYPE` error on next check |

### Key Deleted While Blocked

| Scenario | Behavior |
|----------|----------|
| DEL key while client blocked on it | **Keep blocking** - wait for key to be recreated |
| RENAME key while client blocked | Keep blocking on original key name |

**Example:**

```
Client A: BLPOP mylist 0      # Blocks
Client B: DEL mylist          # mylist deleted
Client A:                     # Still blocking, waiting for mylist
Client C: LPUSH mylist value  # mylist recreated
Client A: ["mylist", "value"] # Finally unblocked
```

### Type Changes While Blocked

| Scenario | Behavior |
|----------|----------|
| Key becomes string while blocked | `-WRONGTYPE` error returned |
| Key becomes hash while blocked | `-WRONGTYPE` error returned |

**Example:**

```
Client A: BLPOP mylist 0      # Blocks on list
Client B: SET mylist "string" # Type changes to string
Client C: LPUSH mylist value  # Error: WRONGTYPE
Client A:                     # Receives WRONGTYPE on next check
```

**Note:** The WRONGTYPE error is returned when an operation triggers a check, not immediately when the type changes.

### Multiple Clients on Same Key

| Scenario | Behavior |
|----------|----------|
| 3 clients BLPOP same key | FIFO: oldest client served first |
| LPUSH adds multiple values | Each value serves one waiting client |
| LPUSH adds fewer values than waiters | Extra waiters remain blocked |

**Example:**

```
t=0: Client A: BLPOP mylist 0   # Blocks (oldest)
t=1: Client B: BLPOP mylist 0   # Blocks (middle)
t=2: Client C: BLPOP mylist 0   # Blocks (newest)
t=3: Client D: LPUSH mylist v1 v2
     # Result: A gets v1, B gets v2, C still blocking
```

### Timeout Precision

| Aspect | Specification |
|--------|---------------|
| Unit | Seconds (can be decimal, e.g., 0.5) |
| Resolution | Millisecond precision internally |
| Zero timeout | Wait forever |
| Negative timeout | Error: `-ERR timeout is negative` |

### QUIT While Blocked

| Step | Behavior |
|------|----------|
| 1. Client sends QUIT | Received by connection handler |
| 2. Connection handler | Sends UnregisterWait to shard |
| 3. Shard | Removes entries from wait queues |
| 4. Response | `+OK` sent before close |
| 5. Connection | Closed gracefully |

### Connection Timeout vs Block Timeout

| Timeout Type | Applies to Blocked? |
|--------------|---------------------|
| `timeout` config (idle timeout) | **No** - blocked clients exempt |
| `tcp-keepalive` | Yes - TCP keepalives still sent |
| Block command timeout | Yes - unblocks when reached |

**Rationale:** Blocked clients are actively waiting, not idle. The idle timeout should not disconnect them.

### Memory Pressure

| Scenario | Behavior |
|----------|----------|
| OOM during LPUSH | LPUSH fails, blocked clients unaffected |
| Max wait queue reached | New BLPOP returns `-OOM` error |

**Configuration:**

```toml
[blocking]
# Maximum waiters per key (0 = unlimited)
max_waiters_per_key = 10000

# Maximum total blocked connections
max_blocked_connections = 50000
```

---

## Cluster Mode: Slot Migration Interaction

> **[Not Yet Implemented]** The slot migration interaction described in this section is a future design. The `on_slot_migration_key_transferred()` callback and related behaviors for sending `-MOVED` to blocked clients during migration are not yet implemented.

When slots migrate between nodes, blocked clients must be handled correctly.
This fixes [Redis issue #2379](https://github.com/redis/redis/issues/2379) where BLPOP blocks
forever after slot migration.

### Behavior During Slot Migration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Blocked Client During Slot Migration                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Client A: BLPOP mylist 0  (blocked on Source node, slot S)             │
│                                                                          │
│  ┌─ Migration Phase ─────────────────────────────────────────────────┐  │
│  │                                                                    │  │
│  │  Phase 1-2 (MIGRATING, key not yet transferred):                  │  │
│  │    - Block continues at Source                                    │  │
│  │    - If LPUSH arrives at Source → unblock client normally         │  │
│  │    - If LPUSH arrives at Target → -MOVED (client retries Source)  │  │
│  │                                                                    │  │
│  │  Phase 2 (key transferred, slot not finalized):                   │  │
│  │    - Source sends UNBLOCK_MIGRATE to blocked client               │  │
│  │    - Client receives: -MOVED <slot> <target_host>:<port>          │  │
│  │    - Client must re-issue BLPOP at Target                         │  │
│  │    *** THIS FIXES THE REDIS BUG ***                               │  │
│  │                                                                    │  │
│  │  Phase 3 (MIGRATED):                                              │  │
│  │    - All new blocks rejected with -MOVED                          │  │
│  │    - Existing blocks already unblocked in Phase 2                 │  │
│  │                                                                    │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Implementation

**Source Node (during slot migration):**

```rust
fn on_slot_migration_key_transferred(key: &Bytes, target: &NodeAddr) {
    // Check if any clients are blocked on this key
    if let Some(waiters) = wait_queue.get(key) {
        for waiter in waiters {
            // Unblock with -MOVED redirect
            waiter.send_moved(key_slot(key), target);
            metrics.migration_unblocks.inc();
        }
        wait_queue.remove(key);
    }
}

fn handle_blocking_command_during_migration(cmd: BlockingCmd) -> Result<Response> {
    let slot = key_slot(&cmd.keys[0]);

    match slot_state(slot) {
        SlotState::Migrating { target, .. } => {
            // Slot is being migrated - check if keys are already transferred
            if all_keys_transferred(&cmd.keys) {
                // Redirect immediately, don't block
                return Err(Error::Moved { slot, target });
            }
            // Keys not yet transferred - allow blocking
            register_blocking_wait(cmd)
        }
        SlotState::Migrated { target } => {
            // Slot fully migrated - redirect
            Err(Error::Moved { slot, target })
        }
        SlotState::Owned => {
            // Normal case - block
            register_blocking_wait(cmd)
        }
    }
}
```

### Wait Queue Transfer

**Wait queues are NOT transferred** during migration:

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Transfer wait queues | Seamless client experience | Race conditions, complex state transfer | **Rejected** |
| Unblock with -MOVED | Simple, predictable | Client must retry | **Chosen** |

**Rationale:**
- Transferring wait queues would require coordinating state between nodes during a sensitive migration window
- Client libraries already handle -MOVED redirects gracefully
- Matches Redis Cluster semantics (though fixes the "block forever" bug)

### DIFFERS FROM REDIS

| Aspect | Redis (broken) | FrogDB (fixed) |
|--------|----------------|----------------|
| Client blocked when slot migrates | **Blocks forever** - never receives -MOVED | Unblocked with -MOVED |
| Client behavior | Must timeout and retry | Immediate redirect |
| Data safety | May pop from wrong node after migration | Always correct node |

### Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_blocking_migration_unblocks_total` | Clients unblocked due to slot migration |
| `frogdb_blocking_migration_redirects_total` | -MOVED sent to blocked clients |
| `frogdb_blocking_migration_keys_total` | Keys with blocked clients during migration |

### Configuration

```toml
[blocking.cluster]
# Proactively unblock clients when slot enters MIGRATING state
# (vs waiting until key is actually transferred)
aggressive_migration_unblock = false

# Delay before unblocking to allow in-flight operations
migration_unblock_delay_ms = 100
```

---

## Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_blocked_clients` | Current number of blocked connections |
| `frogdb_blocked_keys` | Number of keys with waiters |
| `frogdb_blocked_timeout_total` | Timeouts |
| `frogdb_blocked_satisfied_total` | Successful unblocks |

---

## References

- [CONNECTION.md](CONNECTION.md) - Connection state machine, BlockedState
- [CONCURRENCY.md](CONCURRENCY.md) - Shared-nothing architecture
- [FAILURE_MODES.md](FAILURE_MODES.md) - Blocking command failure handling
