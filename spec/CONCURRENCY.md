# FrogDB Concurrency Model

This document details FrogDB's shared-nothing threading architecture, connection handling, and cross-shard coordination.

## Terminology

| Term | Definition |
|------|------------|
| **Shard Worker** | A Tokio task that owns an exclusive data partition and processes commands for that partition |
| **Thread** | An OS thread managed by Tokio's multi-threaded runtime |
| **Tokio Task** | A lightweight async task scheduled by Tokio (many tasks can run on one thread) |
| **Thread-per-core** | Architecture style where work is partitioned to minimize cross-thread coordination |

**Implementation Note:** FrogDB uses Tokio's multi-threaded runtime. Each shard worker is a Tokio task, not a dedicated OS thread. However, the runtime typically pins long-running tasks to threads, achieving thread-per-core characteristics. The documentation uses "thread" and "shard worker" somewhat interchangeably when describing the logical isolation model.

**Key Point:** The critical property is that each shard's data is accessed by exactly one async task (shard worker), eliminating data races without locks. Whether Tokio schedules this task on one thread or occasionally migrates it is an implementation detail handled by the runtime.

---

## Thread Architecture

```
Main Thread
    │
    ├── Spawns Acceptor Thread (1)
    │       └── Accepts TCP connections
    │       └── Assigns connection to thread via round-robin (see CONNECTION.md)
    │
    └── Spawns Shard Workers (N = num_cpus)
            └── Each owns:
                ├── Exclusive data store (HashMap<Key, FrogValue>)
                ├── Lua VM instance
                ├── mpsc::Receiver for incoming messages
                └── Connection handlers for assigned clients
```

## Connection Model (Dragonfly-style)

Connections are **pinned** to a single thread for their entire lifetime but act as **coordinators** that can access any shard via message-passing:

```
Client Connection
         │
         │ Accepted by Acceptor, assigned to Thread 2
         │
         ▼
    ┌─────────────────────────────────────────────────────┐
    │ Thread 2                                            │
    │  ┌──────────────────┐    ┌──────────────────┐      │
    │  │ Connection Fiber │    │    Shard 2       │      │
    │  │  (coordinator)   │    │   (local data)   │      │
    │  └────────┬─────────┘    └──────────────────┘      │
    └───────────┼─────────────────────────────────────────┘
                │
                │ Client sends: SET user:42 "data"
                │ hash("user:42") % num_shards = 0
                │
                │ Key owned by Shard 0 (Thread 0)
                │
    ┌───────────┼─────────────────────────────────────────┐
    │ Thread 0  │                                         │
    │           ▼                                         │
    │  ┌──────────────────┐                              │
    │  │    Shard 0       │ ◄── Execute SET, return OK   │
    │  │   (owns key)     │                              │
    │  └──────────────────┘                              │
    └─────────────────────────────────────────────────────┘
                │
                │ Response flows back to Thread 2
                ▼
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
    // Support Redis hash tags: {tag}rest -> hash("tag")
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
─────────────────────────────────────────────
user:1:profile         user:1:profile  hash(full key)
{user:1}:profile       user:1          hash("user:1")
{user:1}:settings      user:1          hash("user:1") → same shard!
foo{bar}baz            bar             hash("bar")
{}{user:1}             {               hash("{") - empty tag = first {
foo{}bar               (empty)         hash("") - empty tag
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

### Use Case

Lua scripts and multi-key commands (MGET, MSET) require all keys to be on the same shard. Hash tags guarantee colocation:

```
-- All keys hash to same shard via {user:1} tag
EVAL "..." 3 {user:1}:name {user:1}:email {user:1}:prefs
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
}
```

---

## Channel Configuration

Shard message channels use bounded capacity for backpressure:

```rust
// Per-shard message channel
let (tx, rx) = mpsc::channel::<ShardMessage>(SHARD_CHANNEL_CAPACITY);
```

| Setting | Default | Description |
|---------|---------|-------------|
| `shard_channel_capacity` | 1024 | Messages buffered per shard |

### Backpressure Behavior

When a shard's channel fills:
1. Sending connection blocks (async await)
2. Connection stops reading new commands
3. Client TCP buffer fills
4. Client experiences latency

This creates natural backpressure from overloaded shards to clients.

---

## Scatter-Gather Flow

For multi-key operations like MGET:

```
Client: MGET key1 key2 key3
         │
         ▼
    Coordinator (receiving shard)
         │
         ├── Hash keys to shards
         │   key1 -> Shard 0
         │   key2 -> Shard 2
         │   key3 -> Shard 0
         │
         ├── Send ScatterRequest to each unique shard
         │   Shard 0: [key1, key3]
         │   Shard 2: [key2]
         │
         ├── Await all responses (fail-all semantics)
         │   - Configurable timeout (default: 1000ms)
         │   - If any shard fails OR times out, entire operation fails
         │   - All partial results discarded on failure
         │
         └── Gather results, reorder by original key position
              │
              ▼
         Response: [val1, val2, val3]
```

---

## Scatter-Gather Failure Modes

| Scenario | Behavior |
|----------|----------|
| All shards respond | Success, return aggregated results |
| Any shard returns error | Fail entire operation, return error |
| Any shard times out | Fail entire operation, return `-TIMEOUT` |
| Partial responses | Discarded (no partial results ever returned) |

**Rationale:** Fail-all semantics are predictable and easier to reason about than partial results with error markers.

---

## Transaction Ordering (VLL)

FrogDB uses a VLL-inspired (Very Lightweight Locking) approach for multi-shard atomicity,
similar to DragonflyDB. This provides atomic execution without mutex contention.

### Global Transaction IDs

A single atomic counter generates monotonically increasing transaction IDs:

```rust
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

fn acquire_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}
```

### Per-Shard Transaction Queues

Each shard maintains an ordered queue of pending operations:

```rust
pub struct ShardTransactionQueue {
    /// Pending operations ordered by txid
    pending: BTreeMap<u64, PendingOp>,
    /// Currently executing txid (None if idle)
    executing: Option<u64>,
}

impl ShardTransactionQueue {
    /// Queue operation, execute if it's next in line
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

### Multi-Shard Operation Flow

```
MSET key1 val1 key2 val2 (keys on different shards)
         │
         ├── 1. Acquire txid = 1000 from atomic counter
         │
         ├── 2. Send to each shard with same txid:
         │      Shard 0: Execute(txid=1000, SET key1 val1)
         │      Shard 2: Execute(txid=1000, SET key2 val2)
         │
         ├── 3. Each shard queues by txid, executes in order
         │      - Lower txid operations execute first
         │      - No deadlocks (total ordering)
         │
         ├── 4. Await all responses
         │
         └── 5. All complete → return OK
              Any fail → return error (writes may have committed)
```

### Atomicity Semantics

VLL provides **serializable execution order**, not transactional rollback. Understanding the precise guarantees:

| Scenario | Behavior | Data State |
|----------|----------|------------|
| **Single-shard operation** | True atomicity (all-or-nothing) | Consistent, same as Redis |
| **Multi-shard success** | All shards commit | Consistent, globally ordered |
| **Multi-shard timeout** | Client receives `-TIMEOUT` | **Partial commits may exist** |
| **Coordinator crash mid-op** | Client disconnected | **Partial commits may exist** |
| **Single shard fails** | Client receives error | **Other shards may have committed** |

**Critical Distinction:**
- **Client response**: Fail-all semantics (no partial results returned to client)
- **Server state**: Partial commits persist (no rollback mechanism)

**Example - Partial Commit Scenario:**
```
MSET {a}key1 val1 {b}key2 val2  # Keys on different shards
         │
         ├── Shard A: commits SET {a}key1 val1 ✓
         ├── Shard B: times out or fails ✗
         │
         └── Client receives: -TIMEOUT
             But {a}key1 = val1 persists on Shard A
```

**Recommendation:** For operations requiring strict all-or-nothing guarantees, use **hash tags** to ensure
all keys land on the same shard: `MSET {user:1}:name Alice {user:1}:email alice@example.com`

### VLL Properties

| Property | Guarantee |
|----------|-----------|
| **Serializable Order** | Operations with lower txid execute before higher ones globally |
| **No deadlocks** | Total ordering prevents circular waits |
| **No starvation** | BTreeMap ensures FIFO within txid ordering |
| **Durability** | Individual shard writes are durable per durability mode |

### Why VLL vs Locks

Traditional approaches:
- **Mutexes**: Contention at scale, not suitable for shared-nothing
- **2PC**: Expensive coordinator overhead, blocking
- **Optimistic CC**: Aborts under contention

VLL advantages:
- No mutex contention (each shard is single-threaded)
- No coordinator blocking (async execution)
- Deterministic ordering (txid counter)
- No rollback complexity

### VLL Queue Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `vll_queue_timeout_ms` | 5000 | Max time an operation waits in queue before timeout |
| `vll_max_queue_depth` | 10000 | Max pending operations per shard before rejecting new ones |

**Queue Overflow Behavior:**
When `vll_max_queue_depth` is reached:
- New operations receive `-ERR shard queue full, try again later`
- Existing queued operations continue executing
- Metric `frogdb_vll_queue_rejections_total` incremented

**Timeout Behavior:**
When an operation waits longer than `vll_queue_timeout_ms`:
- Operation is removed from queue
- Client receives `-TIMEOUT operation timed out waiting in VLL queue`
- If operation was part of multi-shard scatter-gather, entire operation fails

---

## References

- [CONNECTION.md](CONNECTION.md) - Connection assignment, ConnectionAssigner trait, connection state
- [LIFECYCLE.md](LIFECYCLE.md) - Server startup/shutdown sequences
- [DESIGN.md](../DESIGN.md) - Overall architecture
