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
                ├── Exclusive data store (HashMap<Key, Value>)
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

### Use Case

Lua scripts and multi-key commands (MGET, MSET) require all keys to be on the same shard. Hash tags guarantee colocation:

```
-- All keys hash to same shard via {user:1} tag
EVAL "..." 3 {user:1}:name {user:1}:email {user:1}:prefs
```

### Edge Cases

Comprehensive hash tag parsing behavior:

| Key | Parsed Tag | Hash Input | Notes |
|-----|------------|------------|-------|
| `foo` | (none) | `foo` | No braces |
| `{foo}bar` | `foo` | `foo` | Standard usage |
| `bar{foo}baz` | `foo` | `foo` | Tag in middle |
| `{foo}` | `foo` | `foo` | Tag is entire key |
| `{}` | (none) | `{}` | Empty braces - hash entire key |
| `foo{}bar` | (none) | `foo{}bar` | Empty braces - hash entire key |
| `{}{foo}` | (none) | `{}{foo}` | Empty braces - hash entire key |
| `foo{bar}baz{qux}` | `bar` | `bar` | First tag wins |
| `foo{bar` | (none) | `foo{bar` | No closing brace |
| `foo}bar` | (none) | `foo}bar` | No opening brace |
| `{foo` | (none) | `{foo` | Unclosed tag |
| `foo}` | (none) | `foo}` | Unmatched close |
| `{{foo}}` | `{foo` | `{foo` | First `{` to first `}` after it |
| `{foo{bar}}` | `foo{bar` | `foo{bar` | Content includes nested `{` |

**Binary Data:**
Hash tags work with arbitrary byte sequences:
```rust
// Binary key: bytes 0x7B ('{''), 0x00, 0x7D ('}')
let key = b"{\\x00}rest";
// Tag is: 0x00 (single null byte)
// Hash input: [0x00]
```

**Implementation:**
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

> **Availability:** Multi-shard VLL operations require `allow_cross_slot_standalone = true` in
> standalone mode. By default, cross-shard operations return `-CROSSSLOT` errors (matching Redis Cluster).
> See [CONSISTENCY.md](CONSISTENCY.md#cross-slot-standalone-mode) for configuration details.

### Global Transaction IDs

A single atomic counter generates monotonically increasing transaction IDs:

```rust
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

fn acquire_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}
```

**Scalability Analysis:**

The global `SeqCst` atomic counter is a theoretical contention point. Analysis:

| Factor | Impact |
|--------|--------|
| **Operation cost** | ~10-50 cycles on x86-64 (LOCK XADD instruction) |
| **Throughput ceiling** | ~50-100M txids/second per core accessing |
| **Real-world bottleneck** | Unlikely before network I/O becomes limiting |

**Why SeqCst?** Sequential consistency ensures all threads observe txids in the same order, critical for VLL correctness. Weaker orderings (Relaxed, Acquire/Release) could cause ordering anomalies.

**DragonflyDB Comparison:** DragonflyDB uses a similar global counter without reported scaling issues. At 1M ops/second, counter acquisition is ~0.01% of operation latency.

**Future Optimization (if needed):**
- Per-thread counter with periodic sync (adds complexity)
- Hybrid logical clocks (HLC) for distributed scenarios
- Batched txid acquisition (amortize atomic operation)

Current design prioritizes simplicity. Counter bottleneck has not been observed in practice at expected workloads.

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
              Any fail → rollback and return error (atomic)
```

### Atomicity Semantics

VLL provides **atomic execution** through ordered multi-shard locking:

1. **Lock Phase:** Acquire write locks on all target shards (sorted order prevents deadlock)
2. **Execute Phase:** Execute writes on each shard sequentially while holding all locks
3. **Release Phase:** Release all locks atomically

| Scenario | Behavior | Data State |
|----------|----------|------------|
| **Single-shard operation** | True atomicity (all-or-nothing) | Consistent |
| **Multi-shard success** | All shards commit atomically | Consistent, globally ordered |
| **Lock acquisition timeout** | Client receives `-TIMEOUT` | **No changes** (no locks acquired) |
| **Failure during execution** | Rollback prior writes | **No partial commits** |
| **Coordinator crash mid-op** | Locks timeout and release | **No partial commits** |

**Key Insight:** Because all locks are acquired **before** any writes execute, partial commits
cannot occur. If a write fails after locks are acquired, prior writes in the batch are rolled
back before locks are released.

**Example - Atomic Execution:**
```
MSET {a}key1 val1 {b}key2 val2  # Keys on different shards
         │
         ├── 1. Acquire lock on Shard A
         ├── 2. Acquire lock on Shard B (sorted order)
         ├── 3. Execute SET {a}key1 val1 (while holding both locks)
         ├── 4. Execute SET {b}key2 val2 (while holding both locks)
         ├── 5. Release both locks
         │
         └── Client receives: +OK (both committed atomically)

If step 4 fails:
         ├── Rollback step 3 (delete {a}key1)
         ├── Release both locks
         └── Client receives: -ERR (no changes persisted)
```

**Note:** Hash tags are still recommended for performance, as same-shard operations avoid
the lock coordination overhead: `MSET {user:1}:name Alice {user:1}:email alice@example.com`

### VLL Properties

| Property | Guarantee |
|----------|-----------|
| **Atomicity** | All-or-nothing for multi-shard operations (via ordered locking) |
| **Serializable Order** | Operations with lower txid execute before higher ones globally |
| **No deadlocks** | Lock acquisition in sorted shard order prevents circular waits |
| **No starvation** | BTreeMap ensures FIFO within txid ordering |
| **Durability** | Writes are durable per configured durability mode |

### Why VLL vs Traditional Approaches

Traditional approaches and their drawbacks:
- **Global Mutexes**: Contention at scale, not suitable for shared-nothing
- **2PC with separate coordinator**: Expensive overhead, blocking protocol
- **Optimistic CC**: High abort rate under contention

VLL advantages:
- Ordered locking with deadlock prevention (no 2PC overhead)
- Minimal lock contention (each shard is single-threaded internally)
- Atomic rollback on failure (no partial commits)
- Deterministic ordering via txid counter

### VLL Queue Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `scatter_gather_timeout_ms` | 5000 | Timeout for entire multi-shard operation (VLL + scatter-gather) |
| `vll_max_queue_depth` | 10000 | Max pending operations per shard before rejecting new ones |

**Queue Overflow Behavior:**
When `vll_max_queue_depth` is reached:
- New operations receive `-ERR shard queue full, try again later`
- Existing queued operations continue executing
- Metric `frogdb_vll_queue_rejections_total` incremented

**Timeout Behavior:**
A single timeout (`scatter_gather_timeout_ms`) bounds the entire multi-shard operation:
- Includes VLL lock acquisition time on all participating shards
- Includes execution time across shards
- Includes result aggregation time
- On timeout: `-TIMEOUT operation timed out`
- **No partial commits occur** (VLL is deadlock-free; timeout indicates slow shard)

> **Why a Single Timeout?** VLL is guaranteed deadlock-free by design (ordered transaction IDs).
> A separate "VLL queue timeout" would add complexity without benefit. A slow shard is a node
> health issue, not a locking issue. This matches DragonflyDB's approach.

### Timeout Configuration

FrogDB has two independent timeout types:

| Setting | Default | Scope |
|---------|---------|-------|
| `scatter_gather_timeout_ms` | 5000 | Total multi-shard operation time |
| `client_timeout_ms` | 0 (disabled) | Idle time between client commands |

**Operation Timeout:**

```
Multi-shard operation bounded by scatter_gather_timeout_ms:
┌─────────────────────────────────────────────────────────────────┐
│                 scatter_gather_timeout_ms (5000ms)               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  VLL Lock Acquisition (ordered by txid)                  │    │
│  │     Shard A ──┬── Shard B ──┬── Shard C                 │    │
│  │               │             │                           │    │
│  │  Execute Commands                                        │    │
│  │     Shard A ──┬── Shard B ──┬── Shard C                 │    │
│  │               │             │                           │    │
│  │  Aggregate Results ─────────┴───────────────────────────│    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Timeout Independence:**
- `scatter_gather_timeout_ms` bounds operation latency (VLL + execution + aggregation)
- `client_timeout_ms` bounds idle connections (unrelated to operations)

### Hash Tag Memory Accounting

Hash tag extraction is stack-allocated and does not contribute to memory metrics:
- `extract_hash_tag()` returns a slice into the original key (no allocation)
- Not counted in per-key, per-shard, or global memory accounting
- Transient: exists only for the duration of the hash calculation

### Client Error Handling

With VLL atomicity, client error handling is straightforward:

**Recovery Strategies:**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Idempotent Operations** | Design operations to be safely retryable | MSET (overwrites are safe) |
| **Hash Tags** | Colocate keys on same shard for atomicity | User profiles, related entities |
| **Application-Level Saga** | Track operation state, implement compensating actions | Complex multi-entity updates |
| **Read-After-Write** | Read keys after failure to determine actual state | Verification before retry |

**Idempotency Guidelines:**

```
// SAFE to retry (idempotent)
MSET key1 val1 key2 val2          // Overwrites are deterministic
DEL key1 key2 key3                // Deleting non-existent key is no-op

// UNSAFE to retry (not idempotent)
INCRBY counter1 10                // Would increment twice
LPUSH list1 value                 // Would add duplicate
ZADD set1 1.0 member              // Safe if NX/XX options used appropriately
```

**Design Recommendation:** For optimal performance:
1. **Prefer hash tags** to avoid cross-shard coordination overhead
2. **Design for idempotency** for safe retries on network errors
3. **Use transactions (MULTI/EXEC)** for multi-key atomic updates

**Note:** FrogDB provides stronger guarantees than Redis Cluster for cross-shard operations.
With VLL, multi-shard MSET/MGET are atomic (all-or-nothing), while Redis Cluster has no
atomicity guarantees across slots.

---

## References

- [CONNECTION.md](CONNECTION.md) - Connection assignment, ConnectionAssigner trait, connection state
- [LIFECYCLE.md](LIFECYCLE.md) - Server startup/shutdown sequences
- [DESIGN.md](INDEX.md) - Overall architecture
