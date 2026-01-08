# FrogDB Design Document

A Redis-compatible, high-performance in-memory database written in Rust.

## Overview

FrogDB is designed to be a fast, memory-safe alternative to Redis, leveraging Rust's ownership model to provide thread-safety without the overhead of garbage collection. The primary use cases are:

- High-throughput caching
- Session storage
- Fast operations on data structures

### Goals

1. **Redis compatibility** - Support RESP2/RESP3 protocols and Redis commands for drop-in replacement
2. **High performance** - Thread-per-core architecture with shared-nothing design
3. **Memory safety** - Leverage Rust's guarantees to prevent data races and memory corruption
4. **Durability** - Configurable persistence with RocksDB backend
5. **Extensibility** - Clean abstractions for adding data types, storage backends, and protocols
6. **Correctness** - Eventually pass Jepsen distributed systems tests

### Non-Goals (Initial)

- Full Redis API compatibility from day one (gradual adoption)
- Clustering (single-node first, abstractions for future)
- RESP3 (RESP2 first with abstraction layer)

---

## Architecture

### High-Level System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FrogDB Server                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                                │
│  │  Acceptor   │  (Single thread, accepts connections)          │
│  └──────┬──────┘                                                │
│         │ Distributes connections by consistent hash            │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Shard Workers                         │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │   │
│  │  │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ │ Shard N │        │   │
│  │  │         │ │         │ │         │ │         │        │   │
│  │  │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │        │   │
│  │  │ │Data │ │ │ │Data │ │ │ │Data │ │ │ │Data │ │        │   │
│  │  │ │Store│ │ │ │Store│ │ │ │Store│ │ │ │Store│ │        │   │
│  │  │ └─────┘ │ │ └─────┘ │ │ └─────┘ │ │ └─────┘ │        │   │
│  │  │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │ │ ┌─────┐ │        │   │
│  │  │ │ Lua │ │ │ │ Lua │ │ │ │ Lua │ │ │ │ Lua │ │        │   │
│  │  │ │ VM  │ │ │ │ VM  │ │ │ │ VM  │ │ │ │ VM  │ │        │   │
│  │  │ └─────┘ │ │ └─────┘ │ │ └─────┘ │ │ └─────┘ │        │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘        │   │
│  │       │           │           │           │              │   │
│  │       └───────────┴─────┬─────┴───────────┘              │   │
│  │                         │ Message Passing (mpsc channels) │   │
│  └─────────────────────────┼───────────────────────────────┘   │
│                            │                                    │
│  ┌─────────────────────────▼───────────────────────────────┐   │
│  │                  Persistence Layer                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │   │
│  │  │   RocksDB    │  │   Snapshot   │  │     WAL      │   │   │
│  │  │   Engine     │  │   Manager    │  │   Writer     │   │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Acceptor** | Accept TCP connections, assign to shards via consistent hashing |
| **Shard Worker** | Own a partition of data, execute commands, manage connections |
| **Data Store** | In-memory key-value storage (HashMap-based) |
| **Lua VM** | Execute Lua scripts atomically within shard |
| **Persistence Layer** | WAL writes, snapshot management, recovery |

---

## Design Decisions

### Decision Log

| Decision | Rationale | Alternatives Considered |
|----------|-----------|------------------------|
| Shared-nothing threading | Avoids lock contention, scales linearly with cores, proven by Dragonfly | Lock-based sharding, Actor model |
| Message-passing between threads | Clean coordination for scatter-gather, easier to reason about | Shared memory with atomics |
| Pinned connections (Dragonfly-style) | Simple lifecycle, no migration complexity | Key-owner handles connection |
| Single RocksDB with column families | Simpler backup/restore, shared WAL | Separate RocksDB per shard |
| In-memory + RocksDB persistence | Fast hot path, durable restarts | Pure in-memory, custom engine |
| Forkless snapshots | Avoids fork() memory spike (2x worst case) | Fork + COW (Redis-style) |
| RESP2 first | Simpler, wider client compatibility | RESP3 first |
| Scatter-gather fail-all + timeout | Predictable semantics, configurable timeout | Partial results with errors |
| Hybrid key expiry (lazy + active) | Matches Redis/Valkey, balances memory and CPU | Lazy only, active only |
| OOM rejects writes | Explicit, predictable behavior | LRU eviction |
| Block-shard Lua execution | Redis-compatible atomicity | Async scripts on snapshot |
| Tokio async runtime | Industry standard, excellent ecosystem | async-std, Glommio |
| Eventual consistency model | Appropriate for caching use cases | Strong consistency |

### Key Tradeoffs

#### 1. Shared-Nothing vs Shared-State

**Chosen: Shared-nothing with message passing**

*Pros:*
- No lock contention on hot paths
- Linear scalability with cores
- Each thread can be pinned to a CPU core
- Simpler reasoning about data ownership

*Cons:*
- Cross-shard operations require coordination
- Memory overhead from per-shard data structures
- Scatter-gather latency for multi-key operations

*Mitigation:*
- Efficient message passing via Tokio mpsc channels
- Support for hash tags to colocate related keys

#### 2. In-Memory vs Disk-Based

**Chosen: In-memory primary with RocksDB for durability**

*Pros:*
- Microsecond latencies for all operations
- Simple data structure implementations
- RocksDB handles persistence complexity

*Cons:*
- Data must fit in RAM
- Crash can lose unflushed writes (configurable)

*Mitigation:*
- Configurable durability modes (async/periodic/sync)
- Clear documentation of durability guarantees

#### 3. Redis Compatibility vs Innovation

**Chosen: Compatibility first, innovate where beneficial**

We prioritize Redis protocol and command compatibility for easy adoption, but diverge where we can provide better guarantees:
- Forkless snapshots (no memory spike)
- Shared-nothing multi-threading (vs Redis's single-threaded model)
- Better eventual consistency documentation

---

## Concurrency Model

### Thread Architecture

```
Main Thread
    │
    ├── Spawns Acceptor Thread (1)
    │       └── Accepts TCP connections
    │       └── Assigns connection to thread via round-robin or least-connections
    │
    └── Spawns Shard Workers (N = num_cpus)
            └── Each owns:
                ├── Exclusive data store (HashMap<Key, FrogValue>)
                ├── Lua VM instance
                ├── mpsc::Receiver for incoming messages
                └── Connection handlers for assigned clients
```

### Connection Model (Dragonfly-style)

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

**Key points:**
- Connection lifetime is bound to one thread (no migration)
- Connection fiber coordinates commands, messaging other shards as needed
- Local shard access is direct; remote shard access via message-passing
- Each thread handles both I/O (connections) and data (its shard)

### Key Hashing

Keys are hashed to determine shard ownership:

```rust
fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    // Support Redis hash tags: {tag}rest -> hash("tag")
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let hash = xxhash64(hash_key);
    hash as usize % num_shards
}
```

### Hash Tags (Redis-compatible)

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

**Rules (matching Redis):**
1. Find first `{` in key
2. Find first `}` after that `{`
3. If found and content between is non-empty, use content as hash input
4. Otherwise, hash the entire key

**Use case:** Lua scripts and multi-key commands (MGET, MSET) require all keys to be on the same shard. Hash tags guarantee colocation:

```
-- All keys hash to same shard via {user:1} tag
EVAL "..." 3 {user:1}:name {user:1}:email {user:1}:prefs
```

### Message Types

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

### Scatter-Gather Flow

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

### Scatter-Gather Failure Modes

| Scenario | Behavior |
|----------|----------|
| All shards respond | Success, return aggregated results |
| Any shard returns error | Fail entire operation, return error |
| Any shard times out | Fail entire operation, return `-TIMEOUT` |
| Partial responses | Discarded (no partial results ever returned) |

**Rationale:** Fail-all semantics are predictable and easier to reason about than partial results with error markers.

---

## Data Structures

### Value Types

```rust
pub enum FrogValue {
    String(FrogString),
    SortedSet(FrogSortedSet),
    // Future: List, Hash, Set, Stream, etc.
}
```

### String Type

Simple byte string with optional metadata:

```rust
pub struct FrogString {
    data: Bytes,
}
```

### Sorted Set Type

Dual-indexed structure for O(log n) operations by score and member:

```rust
pub struct FrogSortedSet {
    // Member -> Score mapping for O(1) score lookup
    members: HashMap<Bytes, f64>,
    // Score-ordered structure for range queries
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}
```

Alternative: Skip list implementation for cache-friendlier traversal.

---

## Key Expiry (TTL)

FrogDB uses a **hybrid expiration model** matching Redis/Valkey behavior:

### Expiration Strategies

1. **Lazy expiration**: On every key access, check if expired. Delete immediately if so.
2. **Active expiration**: Background task runs ~10Hz per shard, samples keys with TTL, deletes expired ones within time budget.

```
┌─────────────────────────────────────────────────────────┐
│                  Per-Shard Expiry                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Access Path (Lazy):                                    │
│    GET key → check expiry → if expired: delete, nil    │
│                                                         │
│  Background Task (Active, ~10Hz):                       │
│    1. Sample N keys from expiry index                  │
│    2. Delete expired ones                              │
│    3. Stop when time budget (~1ms) exhausted           │
│    4. If >25% expired, continue next cycle             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Data Structure

Per-shard expiry index for efficient sampling:

```rust
pub struct ExpiryIndex {
    /// Keys with expiry, sorted by expiration time
    by_time: BTreeMap<(Instant, Bytes), ()>,
    /// Quick lookup: key -> expiration time
    by_key: HashMap<Bytes, Instant>,
}
```

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `active_expiry_hz` | 10 | Background sweep frequency per shard |
| `active_expiry_cycle_ms` | 1 | Max time budget per sweep cycle |
| `active_expiry_sample_size` | 20 | Keys sampled per cycle |

---

## Memory Management

### Memory Limit Behavior

When configured memory limit (`max_memory`) is reached:

| Operation | Behavior |
|-----------|----------|
| Write (SET, ZADD, etc.) | Return `-OOM out of memory` error |
| Read (GET, ZRANGE, etc.) | Continue normally |
| Delete (DEL, EXPIRE) | Continue normally |
| Expiry background task | Continues to reclaim memory |

**Rationale:** Explicit OOM errors are predictable. Users must manage capacity or enable expiry policies.

### Non-Goals (Initial)

- LRU/LFU eviction policies (future enhancement)
- Tiered storage to disk for larger-than-memory datasets

---

## Persistence

### RocksDB Topology

FrogDB uses a **single shared RocksDB instance** with one column family per shard:

```
┌─────────────────────────────────────────────────────────┐
│                    RocksDB Instance                      │
├─────────────────────────────────────────────────────────┤
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌─────────┐ │
│  │  CF: s0   │ │  CF: s1   │ │  CF: s2   │ │ CF: sN  │ │
│  │ (Shard 0) │ │ (Shard 1) │ │ (Shard 2) │ │(Shard N)│ │
│  └───────────┘ └───────────┘ └───────────┘ └─────────┘ │
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Shared WAL                          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

**Benefits:**
- Single backup/restore operation for entire database
- Shared WAL simplifies recovery
- Atomic cross-shard operations possible via WriteBatch

**Trade-off:**
- Potential lock contention on WAL writes (mitigated by batching)

### Write-Ahead Log (WAL)

Every write operation is appended to RocksDB's WAL before acknowledgment:

```
Client Write (SET key value)
         │
         ▼
    Shard Worker
         │
         ├── 1. Apply to in-memory store
         │
         ├── 2. Append to WAL (async batch)
         │      └── RocksDB WriteBatch
         │
         └── 3. Return OK to client
```

### Durability Modes

```rust
pub enum DurabilityMode {
    /// Fastest: WAL write, no fsync (data loss on crash possible)
    Async,

    /// Balanced: fsync every N ms or M writes
    Periodic { interval_ms: u64, write_count: usize },

    /// Safest: fsync every write (slowest)
    Sync,
}
```

### Forkless Snapshot Algorithm

Based on Dragonfly's approach - no fork(), no memory spike:

```
1. Coordinator signals all shards: "Start snapshot epoch N"

2. Each shard:
   a. Sets snapshot_epoch = N
   b. Continues processing commands normally
   c. In background, iterates owned keys:
      - For each key, serialize (key, value, expiry)
      - Send batch to snapshot writer
   d. For writes DURING snapshot:
      - If key not yet visited, serialize OLD value first (COW semantics)
      - Then apply new value

3. Snapshot writer:
   - Receives batches from all shards
   - Writes to RocksDB snapshot column family
   - On completion, updates metadata with epoch N

4. Recovery:
   - Load latest snapshot epoch
   - Replay WAL entries after snapshot LSN
```

---

## Protocol

### RESP2 Implementation

RESP2 is the Redis Serialization Protocol version 2:

```
Simple String: +OK\r\n
Error:         -ERR message\r\n
Integer:       :1000\r\n
Bulk String:   $5\r\nhello\r\n
Array:         *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
Null:          $-1\r\n
```

### Protocol Abstraction

```rust
pub trait Protocol: Send + Sync {
    type Frame: ProtocolFrame;

    fn parse(&self, buf: &mut BytesMut) -> Result<Option<Self::Frame>, ProtocolError>;
    fn encode(&self, response: &Response) -> BytesMut;
}
```

This abstraction allows RESP3 to be added later without changing command implementations.

---

## Command Execution

### Command Flow

```
1. Connection accepted by Acceptor
   └── Assigned to Shard N based on client hash

2. Client sends: SET mykey myvalue
   └── Shard N's event loop receives bytes

3. Protocol parser (RESP2)
   └── Parses into ParsedCommand { name: "SET", args: ["mykey", "myvalue"] }

4. Command lookup
   └── Registry.get("SET") -> SetCommand

5. Key routing check
   └── SetCommand.keys(args) -> ["mykey"]
   └── hash("mykey") % num_shards -> Shard M

6. If M == N (local):
   └── Execute directly on local store

   If M != N (remote):
   └── Send ShardMessage::Execute to Shard M
   └── Await response via oneshot channel

7. Execute command
   └── SetCommand.execute(ctx, args)
   └── ctx.store.set("mykey", FrogString::new("myvalue"))

8. Persistence (async)
   └── Append to WAL batch

9. Encode response
   └── Protocol.encode(Response::Ok) -> "+OK\r\n"

10. Send to client
```

### Command Trait

```rust
pub trait Command: Send + Sync {
    fn name(&self) -> &'static str;
    fn arity(&self) -> Arity;
    fn flags(&self) -> CommandFlags;

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    fn keys(&self, args: &[Bytes]) -> Vec<&[u8]>;
}
```

### Arity

Specifies the expected number of arguments for a command:

```rust
pub enum Arity {
    /// Exactly N arguments (e.g., GET = Fixed(1))
    Fixed(usize),
    /// At least N arguments (e.g., MGET = AtLeast(1))
    AtLeast(usize),
    /// Between min and max arguments inclusive
    Range { min: usize, max: usize },
}
```

### Command Flags

Bitflags describing command behavior for routing and optimization:

```rust
bitflags! {
    pub struct CommandFlags: u32 {
        /// Command modifies data (SET, DEL, ZADD)
        const WRITE = 0b0000_0001;
        /// Command only reads data (GET, ZRANGE)
        const READONLY = 0b0000_0010;
        /// O(1) operation, suitable for latency-sensitive paths
        const FAST = 0b0000_0100;
        /// May block the connection (BLPOP, BRPOP)
        const BLOCKING = 0b0000_1000;
        /// Operates on multiple keys (MGET, MSET, DEL with multiple keys)
        const MULTI_KEY = 0b0001_0000;
        /// Pub/sub command, connection enters pub/sub mode
        const PUBSUB = 0b0010_0000;
        /// Script execution (EVAL, EVALSHA)
        const SCRIPT = 0b0100_0000;
    }
}
```

**Usage:** Flags inform the router about command characteristics:
- `WRITE` commands update the WAL
- `READONLY` commands can be load-balanced to replicas (future)
- `MULTI_KEY` commands may trigger scatter-gather
- `BLOCKING` commands require special timeout handling

---

## Lua Scripting

### Execution Model

Lua scripts execute atomically within a single shard (Redis-compatible):

```
EVAL "return redis.call('GET', KEYS[1])" 1 mykey
         │
         ▼
    Shard (owner of mykey)
         │
         ├── Block shard event loop
         │
         ├── Execute Lua in shard's VM
         │   └── redis.call('GET', 'mykey') -> local store lookup
         │
         └── Unblock, return result
```

### Cross-Shard Scripts

Scripts with keys on multiple shards require all keys to hash to the same shard (use hash tags):

```
-- This works: all keys hash to same shard via {user:1} tag
EVAL "..." 2 {user:1}:name {user:1}:email

-- This fails: keys may be on different shards
EVAL "..." 2 user:1:name user:2:email
```

---

## Pub/Sub

FrogDB supports two pub/sub modes with a unified architecture:

| Mode | Commands | Routing | Use Case |
|------|----------|---------|----------|
| **Broadcast** | SUBSCRIBE, PUBLISH, PSUBSCRIBE | All shards | General pub/sub, patterns |
| **Sharded** | SSUBSCRIBE, SPUBLISH | Hash to owner shard | High-throughput, Redis 7.0+ |

### Architecture

```
┌───────────────────────────────────────────────────────────┐
│                   Per-Shard PubSubHandler                  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  ShardSubscriptions (shared by both modes)          │  │
│  │  • channel_subs: HashMap<Channel, Set<ConnId>>      │  │
│  │  • pattern_subs: Vec<(Pattern, ConnId)>             │  │
│  └─────────────────────────────────────────────────────┘  │
│                          │                                 │
│  ┌───────────────────────┴───────────────────────────┐    │
│  │                  Routing Decision                  │    │
│  │  Broadcast: fan-out to all shards                 │    │
│  │  Sharded: route to hash(channel) % num_shards     │    │
│  └────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Unified data structure**: `ShardSubscriptions` used by both modes
2. **Routing is the only difference**: Broadcast fans out, Sharded routes to owner
3. **~90% code reuse**: Only routing logic differs between modes
4. **Pattern subscriptions**: Broadcast mode only (PSUBSCRIBE)

### Mode Comparison

| Aspect | Broadcast | Sharded |
|--------|-----------|---------|
| Publish cost | O(shards) | O(1) |
| Subscribe cost | O(1) local | O(1) or forward |
| Pattern support | Yes | No |
| Use case | General, low-volume | High-throughput |

### Commands

| Command | Mode | Description |
|---------|------|-------------|
| SUBSCRIBE/UNSUBSCRIBE | Broadcast | Channel subscription |
| PSUBSCRIBE/PUNSUBSCRIBE | Broadcast | Pattern subscription |
| PUBLISH | Broadcast | Publish to all shards |
| SSUBSCRIBE/SUNSUBSCRIBE | Sharded | Sharded channel subscription |
| SPUBLISH | Sharded | Publish to owner shard only |
| PUBSUB CHANNELS/NUMSUB | Both | Introspection |

### Delivery Guarantees

- **At-most-once delivery**: Messages may be lost on disconnect
- **Per-channel FIFO**: Order preserved within a channel
- **No persistence**: Pub/sub messages are not persisted

---

## Testing Strategy

### Integration-First Approach

```rust
#[tokio::test]
async fn test_set_get() {
    let server = TestServer::start().await;
    let client = redis::Client::open(server.url()).unwrap();
    let mut con = client.get_connection().unwrap();

    let _: () = con.set("mykey", "myvalue").unwrap();
    let val: String = con.get("mykey").unwrap();

    assert_eq!(val, "myvalue");
}
```

### Test Categories

1. **Protocol tests** - Verify RESP2 parsing/encoding
2. **Command tests** - Each command via redis-cli/client
3. **Concurrency tests** - Multi-client, multi-key operations
4. **Persistence tests** - Crash recovery, snapshot correctness
5. **Lua tests** - Script execution, atomicity
6. **Stress tests** - High throughput, memory pressure
7. **Future: Jepsen tests** - Distributed correctness

---

## Crate Structure

```
frogdb/
├── Cargo.toml                 # Workspace root
├── DESIGN.md                  # This document
│
├── frogdb-server/             # Main server binary
│   └── src/
│       ├── main.rs            # Entry point, CLI
│       ├── server.rs          # Server lifecycle
│       └── config.rs          # Configuration
│
├── frogdb-core/               # Core data structures & logic
│   └── src/
│       ├── shard/             # Shard worker, executor
│       ├── data/              # Value types (String, SortedSet)
│       ├── store/             # Storage trait + implementations
│       └── command/           # Command trait + implementations
│
├── frogdb-protocol/           # RESP protocol handling
│   └── src/
│       ├── trait.rs           # Protocol abstraction
│       └── resp2/             # RESP2 parser/encoder
│
├── frogdb-lua/                # Lua scripting support
│   └── src/
│       ├── vm.rs              # Lua VM wrapper
│       └── commands.rs        # redis.call() bindings
│
├── frogdb-persistence/        # Persistence layer
│   └── src/
│       ├── snapshot.rs        # Forkless snapshot
│       ├── wal.rs             # Write-ahead log
│       └── recovery.rs        # Startup recovery
│
└── tests/                     # Integration tests
    └── src/
        └── *.rs               # Test files
```

---

## Configuration

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `bind` | `127.0.0.1` | Address to bind to |
| `port` | `6379` | Port to listen on |
| `num_shards` | `num_cpus` | Number of shard workers (threads) |

### Memory Options

| Option | Default | Description |
|--------|---------|-------------|
| `max_memory` | `0` (unlimited) | Memory limit in bytes; 0 = no limit |

### Persistence Options

| Option | Default | Description |
|--------|---------|-------------|
| `durability_mode` | `periodic` | `async`, `periodic`, or `sync` |
| `periodic_sync_ms` | `100` | Sync interval for periodic mode |
| `periodic_sync_writes` | `1000` | Write count trigger for periodic mode |
| `snapshot_interval_s` | `3600` | Seconds between snapshots (0 = disabled) |
| `data_dir` | `./data` | Directory for RocksDB and snapshots |

### Timeout Options

| Option | Default | Description |
|--------|---------|-------------|
| `scatter_gather_timeout_ms` | `1000` | Timeout for multi-shard operations |
| `client_timeout_s` | `0` | Idle client timeout (0 = no timeout) |

### Expiry Options

| Option | Default | Description |
|--------|---------|-------------|
| `active_expiry_hz` | `10` | Background expiry sweep frequency |
| `active_expiry_cycle_ms` | `1` | Max time per sweep cycle |

---

## Roadmap

### Phase 0: Design Document (Current)
- [x] Create DESIGN.md
- [x] Document architecture decisions
- [x] Define abstractions and interfaces

### Phase 1: Foundation
- [ ] Project scaffolding (Cargo workspace)
- [ ] RESP2 protocol parser/encoder
- [ ] Single-threaded server
- [ ] In-memory store with String type
- [ ] Basic commands: PING, SET, GET, DEL, EXISTS, EXPIRE, TTL
- [ ] Integration tests with redis-cli

### Phase 2: Multi-Threading
- [ ] Shard worker implementation
- [ ] Message passing infrastructure
- [ ] Acceptor with connection distribution
- [ ] Key hashing and routing
- [ ] Scatter-gather for MGET/MSET

### Phase 3: Sorted Sets
- [ ] FrogSortedSet data structure
- [ ] ZADD, ZREM, ZSCORE, ZRANK, ZRANGE commands

### Phase 4: Persistence
- [ ] RocksDB integration
- [ ] WAL implementation
- [ ] Forkless snapshot algorithm
- [ ] Recovery on startup

### Phase 5: Lua Scripting
- [ ] Lua VM integration (mlua)
- [ ] EVAL, EVALSHA commands
- [ ] redis.call() bindings

### Phase 6: Production Readiness
- [ ] Metrics/observability
- [ ] Configuration file support
- [ ] Graceful shutdown
- [ ] Resource limits

### Future
- RESP3 protocol
- Additional data types
- Clustering
- Replication
- Jepsen testing

---

## References

### Prior Art

- [Redis](https://redis.io/) - The original in-memory data store
- [DragonflyDB](https://dragonflydb.io/) - Modern Redis alternative with shared-nothing architecture
- [Valkey](https://valkey.io/) - Redis fork by Linux Foundation
- [KeyDB](https://keydb.dev/) - Multi-threaded Redis fork

### Papers & Articles

- [Dragonfly Architecture](https://www.dragonflydb.io/blog/scaling-performance-redis-vs-dragonfly) - Shared-nothing design
- [Redis Persistence](https://redis.io/docs/management/persistence/) - RDB + AOF model
- [RESP Protocol](https://redis.io/docs/reference/protocol-spec/) - Redis serialization protocol

### Rust Ecosystem

- [Tokio](https://tokio.rs/) - Async runtime
- [RocksDB Rust](https://github.com/rust-rocksdb/rust-rocksdb) - RocksDB bindings
- [mlua](https://github.com/khvzak/mlua) - Lua bindings for Rust
- [bytes](https://docs.rs/bytes/) - Zero-copy byte handling

---

## Consistency Model

### Guarantees

FrogDB provides **eventual consistency** within a single node:

1. **Read-your-writes**: A client will always see its own writes
2. **Monotonic reads**: Once a value is seen, older values won't be returned
3. **Total order per key**: All operations on a single key are totally ordered

### Non-Guarantees

1. **Cross-key atomicity**: Multi-key operations (MGET, MSET) are not atomic unless all keys are on the same shard
2. **Linearizability**: Operations may be reordered across shards
3. **Durability by default**: Async durability mode may lose recent writes on crash

### Configuration Impact

| Mode | Durability | Latency |
|------|------------|---------|
| `Async` | Best-effort (may lose data) | ~1-10 μs |
| `Periodic(100ms, 1000)` | Bounded loss (100ms or 1000 writes) | ~1-10 μs |
| `Sync` | Guaranteed (fsync per write) | ~100-500 μs |
