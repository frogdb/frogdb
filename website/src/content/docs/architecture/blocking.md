---
title: "Blocking Commands"
description: "Design rationale for blocking commands (BLPOP, BRPOP, BLMOVE, etc.) in FrogDB's shared-nothing architecture."
sidebar:
  order: 10
---
Design rationale for blocking commands (BLPOP, BRPOP, BLMOVE, etc.) in FrogDB's shared-nothing architecture.

## Industry Comparison

| Aspect | Redis/Valkey | DragonflyDB | FrogDB |
|--------|--------------|-------------|--------|
| Architecture | Single-threaded | Multi-threaded, shared-nothing | Multi-threaded, shared-nothing |
| Blocking mechanism | `beforeSleep()` phase rechecks | Per-thread wait queues | Per-shard wait queues |
| Multi-key ordering | First non-empty key in BLPOP order | Non-deterministic (parallel exec) | First non-empty key in order |
| Cross-shard blocking | N/A (single thread) | Keys must be on same thread | Keys must be on same shard |
| Fairness | FIFO by blocking time | FIFO by blocking time | FIFO by blocking time |

FrogDB follows the **Redis model** for semantics with a **shared-nothing per-shard architecture**:
- Per-shard wait queues (DragonflyDB uses a similar per-thread model)
- Cross-shard blocking rejected with `-CROSSSLOT` error (use hash tags for colocation)
- FIFO fairness within each shard
- Key ordering in BLPOP command respected (no parallel execution of blocking checks)

---

## Architecture

### Per-Connection Blocking State

```rust
struct BlockedConnection {
    keys: Vec<Bytes>,
    timeout: Option<Instant>,
    shard_id: usize,
    op: BlockingOp,
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

### Wait Queue per Shard

```rust
struct ShardWaitQueue {
    waiters: HashMap<Bytes, VecDeque<WaitEntry>>,
}

struct WaitEntry {
    conn_id: u64,
    keys: Vec<Bytes>,
    op: BlockingOp,
    response_tx: oneshot::Sender<Response>,
    deadline: Option<Instant>,
}
```

### Registration and Notification Flow

When BLPOP is received:
1. Validate all keys hash to same shard
2. Check if any key has data -- return immediately if so
3. Create oneshot channel and send `BlockWait` message to target shard
4. Shard adds WaitEntry to wait queue for each key
5. Connection awaits response with timeout

When LPUSH adds data:
1. Shard executes LPUSH
2. Check wait queue for the key
3. If waiter exists, pop value and send to oldest waiter
4. Return push count to pushing client

---

## Cross-Shard Blocking

**Not supported.** Blocking on keys from different shards would require distributed coordination that defeats the shared-nothing model.

```
BLPOP key1 key2 0  # Fails if key1 and key2 on different shards
```

**Error:** `-CROSSSLOT Keys in request don't hash to the same slot`

**Solution:** Use hash tags: `BLPOP {queue}:high {queue}:low 0`

---

## Connection State Integration

```
+------------------+
|     NORMAL       |
|  (default mode)  |
+--------+---------+
         |
         | BLPOP/BRPOP/etc.
         v
  +-------------+
  |   BLOCKED   | <-- Connection waits here
  |  (waiting)  |
  +------+------+
         |
         | Data arrives / Timeout / Disconnect
         v
  +-------------+
  |   NORMAL    |
  +-------------+
```

While blocked:
- Connection cannot execute other commands
- PING is allowed (keepalive)
- QUIT causes disconnect and wait cancellation

---

## Command Interaction Matrix

### Transactions (MULTI/EXEC)

| Scenario | Behavior |
|----------|----------|
| BLPOP inside MULTI | **Error:** blocking commands not allowed in transactions |
| MULTI while blocked | Not possible (connection is blocked) |

**Rationale:** Blocking commands wait indefinitely, which would freeze the transaction queue.

### Lua Scripts

| Scenario | Behavior |
|----------|----------|
| `redis.call("BLPOP", ...)` | **Error:** blocking commands not allowed in scripts |

**Rationale:** Blocking inside Lua would freeze the entire shard's event loop.

---

## Edge Cases (Redis-Compatible)

### Key Deleted While Blocked

Client keeps blocking. When key is recreated and has data, client is unblocked.

### Type Changes While Blocked

If key becomes a non-list type while a client is blocked on BLPOP, `-WRONGTYPE` error is returned on next check.

### Multiple Clients on Same Key

FIFO ordering: oldest client served first. Each LPUSH value serves one waiting client.

### Connection Timeout vs Block Timeout

| Timeout Type | Applies to Blocked? |
|--------------|---------------------|
| `timeout` config (idle timeout) | **No** - blocked clients exempt |
| `tcp-keepalive` | Yes - TCP keepalives still sent |
| Block command timeout | Yes - unblocks when reached |

---

## Cluster Mode: Slot Migration Interaction

When slots migrate between nodes, blocked clients are unblocked with `-MOVED` redirect. This fixes a known Redis bug where BLPOP blocks forever after slot migration.

| Aspect | Redis (broken) | FrogDB (fixed) |
|--------|----------------|----------------|
| Client blocked when slot migrates | **Blocks forever** | Unblocked with -MOVED |
| Client behavior | Must timeout and retry | Immediate redirect |
