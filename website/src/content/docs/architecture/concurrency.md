---
title: "Concurrency Model"
description: "Design rationale and details of FrogDB's shared-nothing threading architecture, connection handling, and cross-shard coordination."
sidebar:
  order: 3
---
Design rationale and details of FrogDB's shared-nothing threading architecture, connection handling, and cross-shard coordination.

## Terminology

| Term | Definition |
|------|------------|
| **Shard Worker** | A Tokio task that owns an exclusive data partition and processes commands for that partition |
| **Thread** | An OS thread managed by Tokio's multi-threaded runtime |
| **Tokio Task** | A lightweight async task scheduled by Tokio (many tasks can run on one thread) |
| **Thread-per-core** | Architecture style where work is partitioned to minimize cross-thread coordination |

FrogDB uses Tokio's multi-threaded runtime. Each shard worker is a Tokio task, not a dedicated OS thread. However, the runtime typically pins long-running tasks to threads, achieving thread-per-core characteristics. The critical property is that each shard's data is accessed by exactly one async task (shard worker), eliminating data races without locks.

---

## Thread Architecture

```
Main Thread
    |
    +-- Spawns Acceptor Thread (1)
    |       +-- Accepts TCP connections
    |       +-- Assigns connection to thread via round-robin
    |
    +-- Spawns Shard Workers (N = num_cpus)
            +-- Each owns:
                +-- Exclusive data store (HashMap<Key, Value>)
                +-- Lua VM instance
                +-- mpsc::Receiver for incoming messages
                +-- Connection handlers for assigned clients
```

## Connection Model

Connections are **pinned** to a single thread for their entire lifetime but act as **coordinators** that can access any shard via message-passing:

```
Client Connection
         |
         | Accepted by Acceptor, assigned to Thread 2
         |
         v
    +----------------------------------------------------+
    | Thread 2                                           |
    |  +------------------+    +------------------+     |
    |  | Connection Fiber |    |    Shard 2       |     |
    |  |  (coordinator)   |    |   (local data)   |     |
    |  +--------+---------+    +------------------+     |
    +-----------|----------------------------------------+
                |
                | Client sends: SET user:42 "data"
                | hash("user:42") % num_shards = 0
                |
    +-----------|-----------------------------------------+
    | Thread 0  |                                        |
    |           v                                        |
    |  +------------------+                              |
    |  |    Shard 0       | <-- Execute SET, return OK   |
    |  |   (owns key)     |                              |
    |  +------------------+                              |
    +----------------------------------------------------+
                |
                | Response flows back to Thread 2
                v
         Client receives: +OK\r\n
```

### Key Points

- Connection lifetime is bound to one thread (no migration)
- Connection fiber coordinates commands, messaging other shards as needed
- Local shard access is direct; remote shard access via message-passing
- Each thread handles both I/O (connections) and data (its shard)

---

## Key Hashing

Keys are hashed to determine shard ownership:

```rust
fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let hash = xxhash64(hash_key);
    hash as usize % num_shards
}
```

---

## Hash Tags (Redis-compatible)

Hash tags allow related keys to be colocated on the same shard:

```
Key                    Hash Input      Result
---------------------------------------------
user:1:profile         user:1:profile  hash(full key)
{user:1}:profile       user:1          hash("user:1")
{user:1}:settings      user:1          hash("user:1") -> same shard!
foo{bar}baz            bar             hash("bar")
{}{user:1}             (none)          hash("{}{user:1}") - empty first braces
foo{}bar               (none)          hash("foo{}bar") - empty braces
```

### Rules (matching Redis)

1. Find first `{` in key
2. Find first `}` after that `{`
3. If found and content between is non-empty, use content as hash input
4. Otherwise, hash the entire key

### Full Colocation Guarantee

Hash tags guarantee colocation at **both** levels:
- **Cluster level**: `CRC16(hash_tag) % 16384` determines slot/node
- **Internal level**: `xxhash64(hash_tag) % num_shards` determines thread

This ensures hash-tagged keys are always on the same internal shard, enabling:
- Atomic transactions across multiple keys
- Lua scripts accessing multiple keys
- WATCH with consistent visibility

### Edge Cases

| Key | Parsed Tag | Hash Input | Notes |
|-----|------------|------------|-------|
| `foo` | (none) | `foo` | No braces |
| `{foo}bar` | `foo` | `foo` | Standard usage |
| `{}` | (none) | `{}` | Empty braces - hash entire key |
| `{}{foo}` | (none) | `{}{foo}` | Empty braces - hash entire key |
| `foo{bar}baz{qux}` | `bar` | `bar` | First tag wins |
| `{{foo}}` | `{foo` | `{foo` | First `{` to first `}` after it |

```rust
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close = key[open + 1..].iter().position(|&b| b == b'}')?;
    let tag = &key[open + 1..open + 1 + close];
    if tag.is_empty() {
        None  // Empty braces `{}` - hash entire key (Redis behavior)
    } else {
        Some(tag)
    }
}
```

---

## Message Types

```rust
pub enum ShardMessage {
    /// Execute command on this shard, return via oneshot
    Execute {
        command: ParsedCommand,
        response_tx: oneshot::Sender<Result<Response, Error>>,
    },

    /// Scatter-gather: partial request for multi-key operation
    ScatterRequest {
        request_id: u64,
        keys: Vec<Bytes>,
        operation: ScatterOp,
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// Snapshot: iterate and send batches
    SnapshotRequest {
        batch_tx: mpsc::Sender<SnapshotBatch>,
    },

    /// Shutdown signal
    Shutdown,

    // === VLL Messages ===

    /// Request shard to acquire locks for a VLL transaction
    VllLockRequest {
        txid: u64,
        keys: Vec<Bytes>,
        modes: Vec<LockMode>,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    },

    /// Execute operation after locks acquired
    VllExecute {
        txid: u64,
        command: VllCommand,
        result_tx: oneshot::Sender<VllShardResult>,
    },

    /// Abort transaction (release locks, no rollback)
    VllAbort { txid: u64 },

    /// Request continuation (full shard) lock for Lua/MULTI
    VllContinuationLock {
        txid: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    },
}
```

See [vll.md](/architecture/vll/) for full VLL type definitions.

---

## Channel Configuration

Shard message channels use bounded capacity for backpressure:

| Setting | Default | Description |
|---------|---------|-------------|
| `shard-channel-capacity` | 1024 | Messages buffered per shard |

### Backpressure Behavior

When a shard's channel fills:
1. Sending connection blocks (async await)
2. Connection stops reading new commands
3. Client TCP buffer fills
4. Client experiences latency

This creates natural backpressure from overloaded shards to clients.

---

## Coordinator Assignment

For multi-shard operations, the **receiving shard** (where connection is pinned) acts as coordinator:

```
Client -> Connection (pinned to Shard 0) -> MGET key1 key2 key3
                                              |
                        Shard 0 coordinates scatter-gather:
                        +-- key1 -> Shard 2 (request)
                        +-- key2 -> Shard 0 (local)
                        +-- key3 -> Shard 1 (request)
                                              |
                        Shard 0 assembles results, responds to client
```

**Responsibilities of coordinator:**
- Parse command, determine key->shard mapping
- Send sub-requests to remote shards
- Wait for responses (with timeout)
- Handle failures (abort, return error)
- Assemble and return final response

**Cleanup on coordinator failure:**
- If coordinator shard crashes mid-operation, remote shards release pending locks on timeout
- No orphaned state - VLL intents expire after `scatter-gather-timeout-ms`

---

## Scatter-Gather Flow

For multi-key operations like MGET:

```
Client: MGET key1 key2 key3
         |
         v
    Coordinator (receiving shard)
         |
         +-- Hash keys to shards
         |   key1 -> Shard 0
         |   key2 -> Shard 2
         |   key3 -> Shard 0
         |
         +-- Send ScatterRequest to each unique shard
         |   Shard 0: [key1, key3]
         |   Shard 2: [key2]
         |
         +-- Await all responses (fail-all semantics)
         |   - Configurable timeout (default: 1000ms)
         |   - If any shard fails OR times out, entire operation fails
         |
         +-- Gather results, reorder by original key position
              |
              v
         Response: [val1, val2, val3]
```

### Scatter-Gather Failure Modes

| Scenario | Behavior |
|----------|----------|
| All shards respond | Success, return aggregated results |
| Any shard returns error | Fail entire operation, return error |
| Any shard times out | Fail entire operation, return `-TIMEOUT` |
| Partial responses | Discarded (no partial results ever returned) |

**Rationale:** Fail-all semantics are predictable and easier to reason about than partial results with error markers.

---

## Transaction Ordering (VLL)

FrogDB uses a [VLL](/architecture/vll/)-inspired (Very Lightweight Locking) approach for multi-shard atomicity. This provides atomic execution without mutex contention.

See [vll.md](/architecture/vll/) for the complete VLL design including goals, key concepts, and details.

### Global Transaction IDs

A single atomic counter generates monotonically increasing transaction IDs:

```rust
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

fn acquire_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}
```

**Scalability Analysis:**

| Factor | Impact |
|--------|--------|
| **Operation cost** | ~10-50 cycles on x86-64 (LOCK XADD instruction) |
| **Throughput ceiling** | ~50-100M txids/second per core accessing |
| **Real-world bottleneck** | Unlikely before network I/O becomes limiting |

**Why SeqCst?** Sequential consistency ensures all threads observe txids in the same order, critical for VLL correctness. Weaker orderings could cause ordering anomalies.

**DragonflyDB Comparison:** DragonflyDB uses a similar global counter without reported scaling issues. At 1M ops/second, counter acquisition is ~0.01% of operation latency.

### Per-Shard Transaction Queues

Each shard maintains an ordered queue of pending operations:

```rust
pub struct ShardTransactionQueue {
    pending: BTreeMap<u64, PendingOp>,
    executing: Option<u64>,
}

impl ShardTransactionQueue {
    fn enqueue(&mut self, txid: u64, op: PendingOp) {
        self.pending.insert(txid, op);
        self.maybe_execute_next();
    }

    fn maybe_execute_next(&mut self) {
        if self.executing.is_some() { return; }
        if let Some((&txid, _)) = self.pending.first_key_value() {
            self.executing = Some(txid);
            // Execute operation...
        }
    }
}
```

### VLL Properties

| Property | Guarantee |
|----------|-----------|
| **Execution Atomicity** | No interleaving during multi-shard operations (isolation) |
| **Serializable Order** | Operations with lower txid execute before higher ones globally |
| **No deadlocks** | Lock acquisition in sorted shard order prevents circular waits |
| **No starvation** | BTreeMap ensures FIFO within txid ordering |
| **Durability** | Writes are durable per configured durability mode |
| **No Rollback** | Partial state persists on failure (Redis/DragonflyDB-compatible) |

### Timeout Configuration

FrogDB has two independent timeout types:

| Setting | Default | Scope |
|---------|---------|-------|
| `scatter-gather-timeout-ms` | 5000 | Total multi-shard operation time |
| `client-timeout-ms` | 0 (disabled) | Idle time between client commands |

### Client Error Handling

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Idempotent Operations** | Design operations to be safely retryable | MSET (overwrites are safe) |
| **Hash Tags** | Colocate keys on same shard for atomicity | User profiles, related entities |
| **Application-Level Saga** | Track operation state, implement compensating actions | Complex multi-entity updates |
| **Read-After-Write** | Read keys after failure to determine actual state | Verification before retry |
