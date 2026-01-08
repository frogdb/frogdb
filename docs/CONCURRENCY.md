# FrogDB Concurrency Model

This document details FrogDB's shared-nothing threading architecture, connection handling, and cross-shard coordination.

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

## References

- [CONNECTION.md](CONNECTION.md) - Connection assignment, ConnectionAssigner trait, connection state
- [LIFECYCLE.md](LIFECYCLE.md) - Server startup/shutdown sequences
- [DESIGN.md](../DESIGN.md) - Overall architecture
