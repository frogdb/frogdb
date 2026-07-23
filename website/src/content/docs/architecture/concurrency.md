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

FrogDB uses Tokio's multi-threaded runtime. Each shard worker is a Tokio task,
not a dedicated OS thread. Tokio's multi-threaded runtime is work-stealing and
does not pin a task to a specific thread, so FrogDB does not guarantee strict
thread-per-core placement. The property FrogDB does rely on is ownership, not
placement: each shard's data is accessed by exactly one async task (its shard
worker), so there are no data races and no locks are needed for data access,
regardless of which OS thread runs the task at any moment.

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

Keys are hashed to determine shard ownership. The routing functions live in
`frogdb-server/crates/core/src/shard/partition.rs`:

```rust
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % 16384;
    slot % num_shards
}
```

The internal shard is derived from the key's cluster hash slot (CRC16, the same
XMODEM variant Redis Cluster uses), reduced modulo the shard count — not from a
separate hash function. The `xxhash64` hash elsewhere in the codebase is used
only by probabilistic structures (bloom, cuckoo, count-min, top-k); it is never
used for key routing.

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
- **Internal level**: `CRC16(hash_tag) % 16384 % num_shards` determines thread

Because the internal shard is derived from the hash slot, same-slot keys are automatically
same-shard keys — colocation at the cluster level implies colocation at the thread level.

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

The `ShardMessage` enum has dozens of variants (command execution, pub/sub,
blocking, scripting, VLL, cluster, diagnostics, and more). The excerpt below is
an **illustrative subset**; the full enum lives in
`frogdb-server/crates/core/src/shard/message.rs`.

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
        mode: LockMode,
        operation: ScatterOp,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    },

    /// Execute operation after locks acquired
    VllExecute {
        txid: u64,
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// Abort transaction (release locks, no rollback)
    VllAbort { txid: u64 },

    /// Request continuation (full shard) lock for Lua/MULTI
    VllContinuationLock {
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    },
}
```

See [vll.md](/architecture/vll/) for full VLL type definitions.

---

## Channel Configuration

Shard message channels use bounded capacity for backpressure. The capacity is a
compile-time constant in `frogdb-server/crates/server/src/server/util.rs`, not a
runtime `CONFIG` parameter:

```rust
pub const SHARD_CHANNEL_CAPACITY: usize = 1024;   // messages buffered per shard
pub const NEW_CONN_CHANNEL_CAPACITY: usize = 256; // pending new-connection handoffs
```

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
         |   - Bounded by scatter-gather-timeout-ms (default: 5000ms)
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

Allocating a txid is a single atomic fetch-add. This is cheap relative to the
network and store work each transaction performs; the counter is not expected to
be a contention point before network I/O becomes the limiting factor. DragonflyDB
uses a similar global counter in its transaction machinery.

**Why SeqCst?** Sequential consistency ensures all threads observe txids in the
same total order, which VLL correctness depends on. Weaker orderings could allow
ordering anomalies between shards.

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

The overall multi-shard operation is bounded by a single configurable timeout:

| Setting | Default | Scope |
|---------|---------|-------|
| `scatter-gather-timeout-ms` | 5000 | Total multi-shard operation time |

The finer-grained VLL lock-acquisition timeout is a compile-time constant
(`DEFAULT_LOCK_ACQUISITION_TIMEOUT`, 4000 ms, in
`frogdb-server/crates/vll/src/coordinator.rs`) that nests inside this bound,
smaller than `scatter-gather-timeout-ms`. See [vll.md](/architecture/vll/) for
more on VLL queueing.

### Client Error Handling

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Idempotent Operations** | Design operations to be safely retryable | MSET (overwrites are safe) |
| **Hash Tags** | Colocate keys on same shard for atomicity | User profiles, related entities |
| **Application-Level Saga** | Track operation state, implement compensating actions | Complex multi-entity updates |
| **Read-After-Write** | Read keys after failure to determine actual state | Verification before retry |
