# FrogDB Blocking Commands

This document details the design for blocking commands (BLPOP, BRPOP, BLMOVE, etc.) in FrogDB's shared-nothing architecture.

> **Status:** Non-goal for initial implementation. This document outlines future design considerations.

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

## Proposed Architecture

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
    op: BlockingOp,
    response_tx: oneshot::Sender<Response>,
    deadline: Option<Instant>,
}
```

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

**Error:** `-CROSSSHARD Blocking commands require all keys on same shard`

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

- Atomically pop from source, push to destination
- Both keys must be on same shard

### BZPOPMIN / BZPOPMAX

```
BZPOPMIN key [key ...] timeout
```

- Pop element with min/max score from first non-empty sorted set
- Return: `[key, member, score]` or nil on timeout

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
