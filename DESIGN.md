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

| Date | Decision | Rationale | Alternatives Considered |
|------|----------|-----------|------------------------|
| 2025-01-07 | Shared-nothing threading | Avoids lock contention, scales linearly with cores, proven by Dragonfly | Lock-based sharding, Actor model (Actix) |
| 2025-01-07 | Message-passing between threads | Clean coordination for scatter-gather, easier to reason about than shared state | Shared memory with atomics, Lock-free data structures |
| 2025-01-07 | In-memory + RocksDB persistence | Fast hot path, durable restarts, proven combination | Pure in-memory (no durability), Custom storage engine |
| 2025-01-07 | Forkless snapshots | Avoids Redis's fork() memory spike (2x worst case), better for large datasets | Fork + COW (Redis-style), Incremental checkpointing |
| 2025-01-07 | RESP2 first | Simpler protocol, wider client compatibility, faster to implement | RESP3 first, Custom protocol |
| 2025-01-07 | Scatter-gather with fail-all | Predictable semantics, easier to reason about failures | Partial results with errors, Timeout + retry |
| 2025-01-07 | Block-shard Lua execution | Redis-compatible semantics, atomic execution guaranteed | Async scripts on snapshot, Parallel script execution |
| 2025-01-07 | Tokio async runtime | Industry standard, excellent ecosystem, well-documented | async-std, Glommio (io_uring, Linux-only) |
| 2025-01-07 | Eventual consistency model | Appropriate for caching/session use cases, simpler implementation | Strong consistency (more complex coordination) |

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
    │       └── Assigns connection to shard via consistent hash of client ID
    │
    └── Spawns Shard Workers (N = num_cpus)
            └── Each owns:
                ├── Exclusive data store (HashMap<Key, FrogValue>)
                ├── Lua VM instance
                ├── RocksDB column family (for persistence)
                ├── mpsc::Receiver for incoming messages
                └── Connection handlers for assigned clients
```

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
         │   - If any shard fails, entire operation fails
         │
         └── Gather results, reorder by original key position
              │
              ▼
         Response: [val1, val2, val3]
```

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

## Persistence

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
| **Broadcast** | SUBSCRIBE, PUBLISH | All shards | General pub/sub, low-medium volume |
| **Sharded** | SSUBSCRIBE, SPUBLISH | Hash to owner shard | High-throughput, Redis 7.0+ compatible |

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Pub/Sub System                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    PubSubRouter                          │   │
│  │  ┌─────────────────┐    ┌─────────────────┐             │   │
│  │  │ Broadcast Mode  │    │  Sharded Mode   │             │   │
│  │  │ (all shards)    │    │ (hash to owner) │             │   │
│  │  └────────┬────────┘    └────────┬────────┘             │   │
│  │           │                      │                       │   │
│  │           └──────────┬───────────┘                       │   │
│  │                      ▼                                   │   │
│  │           ┌─────────────────────┐                       │   │
│  │           │   Routing Decision  │                       │   │
│  │           └─────────────────────┘                       │   │
│  └──────────────────────┼──────────────────────────────────┘   │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Per-Shard PubSubHandler                     │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │            ShardSubscriptions                      │  │   │
│  │  │  • channel_subs: HashMap<Channel, Set<ConnId>>    │  │   │
│  │  │  • pattern_subs: Vec<(Pattern, ConnId)>           │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  │                         │                                │   │
│  │                         ▼                                │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │              deliver_locally()                     │  │   │
│  │  │  • Match exact channels                           │  │   │
│  │  │  • Match patterns (glob)                          │  │   │
│  │  │  • Send to connections                            │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Broadcast vs Sharded Comparison

```
BROADCAST: PUBLISH news:sports "goal"     SHARDED: SPUBLISH news:sports "goal"
              │                                        │
              ▼                                        ▼
         ┌────────┐                              ┌────────┐
         │ Shard  │                              │ Shard  │
         │   0    │                              │   0    │
         └────┬───┘                              └────────┘
              │                                        │
     ┌────────┼────────┐                    hash("news:sports") = 2
     ▼        ▼        ▼                               │
┌────────┐┌────────┐┌────────┐                        ▼
│Shard 0 ││Shard 1 ││Shard 2 │               ┌────────────────┐
│deliver ││deliver ││deliver │               │    Shard 2     │
│locally ││locally ││locally │               │ (owner only)   │
└────────┘└────────┘└────────┘               │ deliver locally│
     │        │        │                      └────────────────┘
     └────────┴────────┘                               │
              │                                        │
              ▼                                        ▼
    Total subscribers: 15                   Subscribers: 5
    (sum from all shards)                   (single shard)
```

### Core Components

```rust
/// Pub/Sub mode determines routing behavior
#[derive(Clone, Copy)]
pub enum PubSubMode {
    /// SUBSCRIBE/PUBLISH - broadcast to all shards
    Broadcast,
    /// SSUBSCRIBE/SPUBLISH - route to channel's owning shard
    Sharded,
}

/// Per-connection pub/sub state (shared by both modes)
pub struct PubSubState {
    /// Exact channel subscriptions: (channel, mode)
    channels: HashMap<Bytes, PubSubMode>,
    /// Pattern subscriptions (broadcast mode only)
    patterns: Vec<Pattern>,
    /// Is this connection in pub/sub mode?
    is_subscribed: bool,
}

/// Per-shard subscription index (shared by both modes)
pub struct ShardSubscriptions {
    /// Channel -> Set of subscribed connection IDs
    /// Used by both modes - broadcast stores all local, sharded stores owned only
    channel_subs: HashMap<Bytes, HashSet<ConnectionId>>,
    /// Pattern subscribers (broadcast mode only, checked on every publish)
    pattern_subs: Vec<(Pattern, ConnectionId)>,
}

/// Unified pub/sub handler per shard
pub struct PubSubHandler {
    shard_id: ShardId,
    num_shards: usize,
    subscriptions: ShardSubscriptions,
}

impl PubSubHandler {
    /// Subscribe to a channel
    pub fn subscribe(
        &mut self,
        channel: Bytes,
        conn: ConnectionId,
        mode: PubSubMode,
    ) -> SubscribeResult {
        match mode {
            PubSubMode::Broadcast => {
                // Always subscribe locally - each shard tracks its own connections
                self.subscriptions.add(channel, conn);
                SubscribeResult::Subscribed
            }
            PubSubMode::Sharded => {
                let owner = self.channel_owner(&channel);
                if owner == self.shard_id {
                    // We own this channel - subscribe locally
                    self.subscriptions.add(channel, conn);
                    SubscribeResult::Subscribed
                } else {
                    // Forward to owning shard
                    SubscribeResult::Forward { shard: owner }
                }
            }
        }
    }

    /// Determine where to route a publish
    pub fn route_publish(&self, channel: &Bytes, mode: PubSubMode) -> PublishRoute {
        match mode {
            PubSubMode::Broadcast => PublishRoute::AllShards,
            PubSubMode::Sharded => {
                let owner = self.channel_owner(channel);
                PublishRoute::SingleShard(owner)
            }
        }
    }

    /// Deliver message to local subscribers (SAME for both modes)
    pub fn deliver_locally(&self, channel: &Bytes, message: &Bytes) -> usize {
        let mut delivered = 0;

        // Exact channel matches
        if let Some(subscribers) = self.subscriptions.channel_subs.get(channel) {
            for conn_id in subscribers {
                self.send_to_connection(*conn_id, channel, message);
                delivered += 1;
            }
        }

        // Pattern matches (broadcast mode subscriptions)
        for (pattern, conn_id) in &self.subscriptions.pattern_subs {
            if pattern.matches(channel) {
                self.send_to_connection(*conn_id, channel, message);
                delivered += 1;
            }
        }

        delivered
    }

    /// Hash channel to owning shard (same algorithm as keys)
    fn channel_owner(&self, channel: &Bytes) -> ShardId {
        let hash = xxhash64(channel);
        (hash as usize % self.num_shards) as ShardId
    }
}

/// Result of subscribe operation
pub enum SubscribeResult {
    Subscribed,
    Forward { shard: ShardId },
}

/// Routing decision for publish
pub enum PublishRoute {
    AllShards,
    SingleShard(ShardId),
}
```

### Message Flow: Broadcast Mode

**SUBSCRIBE (Broadcast):**
```
1. Client sends: SUBSCRIBE news:sports
2. Connection's shard adds channel to LOCAL ShardSubscriptions
3. Connection enters "pub/sub mode"
4. No cross-shard communication needed
5. Response: subscription confirmation
```

**PUBLISH (Broadcast):**
```
1. Client sends: PUBLISH news:sports "goal scored"
2. Receiving shard:
   a. route_publish() -> AllShards
   b. Broadcast PubSubBroadcast message to ALL shards
   c. Each shard calls deliver_locally()
   d. Gather subscriber counts from all shards
3. Response: total subscriber count (sum)
```

**PSUBSCRIBE (Broadcast only):**
```
1. Client sends: PSUBSCRIBE news:*
2. Connection's shard stores pattern in pattern_subs
3. On any publish, pattern is evaluated against channel name
4. Pattern subscriptions are LOCAL only (no cross-shard)
```

### Message Flow: Sharded Mode

**SSUBSCRIBE (Sharded):**
```
1. Client sends: SSUBSCRIBE news:sports
2. Connection's shard calculates: hash("news:sports") % shards -> Shard 2
3. If current shard IS owner (Shard 2):
   a. Add to local ShardSubscriptions
   b. Response: subscription confirmation
4. If current shard is NOT owner:
   a. Forward SubscribeRequest to Shard 2
   b. Shard 2 adds subscription (with reference to original connection)
   c. Response: subscription confirmation
```

**SPUBLISH (Sharded):**
```
1. Client sends: SPUBLISH news:sports "goal scored"
2. Receiving shard:
   a. route_publish() -> SingleShard(2)
   b. If we ARE Shard 2: deliver_locally()
   c. If we are NOT Shard 2: forward to Shard 2
3. Owning shard delivers to its subscribers
4. Response: subscriber count (single shard only)
```

### Inter-Shard Messages

```rust
pub enum ShardMessage {
    // ... existing messages ...

    /// Broadcast pub/sub message to all shards (broadcast mode)
    PubSubBroadcast {
        channel: Bytes,
        message: Bytes,
        /// Sender collects counts via this channel
        count_tx: mpsc::Sender<usize>,
    },

    /// Sharded pub/sub - route to specific shard (sharded mode)
    PubSubSharded {
        channel: Bytes,
        message: Bytes,
        response_tx: oneshot::Sender<usize>,
    },

    /// Forward subscription to owning shard (sharded mode)
    PubSubSubscribe {
        channel: Bytes,
        subscriber: RemoteSubscriber,
        response_tx: oneshot::Sender<SubscribeResult>,
    },
}

/// Reference to a subscriber on another shard
pub struct RemoteSubscriber {
    /// Original connection's shard
    home_shard: ShardId,
    /// Connection ID on home shard
    conn_id: ConnectionId,
}
```

### Sharded Mode: Cross-Shard Delivery

When a subscriber is on a different shard than the channel owner:

```
Shard 0 (subscriber)          Shard 2 (channel owner)
        │                              │
        │  SSUBSCRIBE news:sports      │
        ├─────────────────────────────>│
        │                              │ Store: news:sports -> RemoteSubscriber(0, conn_42)
        │                              │
        │      ... later ...           │
        │                              │
        │                              │ SPUBLISH news:sports "goal"
        │                              │
        │  PubSubDeliver(conn_42, msg) │
        │<─────────────────────────────┤
        │                              │
        │ Deliver to conn_42           │
        │                              │
```

### Commands

| Command | Mode | Description |
|---------|------|-------------|
| SUBSCRIBE | Broadcast | Subscribe to channels (all shards track locally) |
| UNSUBSCRIBE | Broadcast | Unsubscribe from channels |
| PUBLISH | Broadcast | Publish to all shards |
| PSUBSCRIBE | Broadcast | Subscribe to pattern (local only) |
| PUNSUBSCRIBE | Broadcast | Unsubscribe from pattern |
| SSUBSCRIBE | Sharded | Subscribe to channel (routed to owner) |
| SUNSUBSCRIBE | Sharded | Unsubscribe from channel |
| SPUBLISH | Sharded | Publish to owning shard only |
| PUBSUB CHANNELS | Both | List active channels |
| PUBSUB NUMSUB | Both | Get subscriber counts |
| PUBSUB SHARDCHANNELS | Sharded | List channels owned by this shard |
| PUBSUB SHARDNUMSUB | Sharded | Get subscriber counts for owned channels |

### Code Reuse Summary

| Component | Shared? | Notes |
|-----------|---------|-------|
| `ShardSubscriptions` | Yes | Same data structure, different population |
| `PubSubState` | Yes | Tracks mode per subscription |
| `deliver_locally()` | Yes | Identical delivery logic |
| Pattern matching | Yes | Same glob implementation |
| RESP encoding | Yes | Same message format |
| Channel hashing | Yes | Same as key hashing |
| **Routing logic** | No | Mode determines broadcast vs single-shard |
| **Cross-shard subscribe** | Sharded only | RemoteSubscriber forwarding |

### Considerations

| Aspect | Broadcast | Sharded |
|--------|-----------|---------|
| **Publish cost** | O(shards) | O(1) |
| **Subscribe cost** | O(1) local | O(1) or forward |
| **Memory** | Duplicated across shards | Single shard per channel |
| **Latency** | Higher (fan-out) | Lower (direct) |
| **Pattern support** | Yes (PSUBSCRIBE) | No |
| **Use case** | General, low-volume | High-throughput |

### Delivery Guarantees

Both modes provide:
- **At-most-once delivery**: Messages may be lost if client disconnects
- **Per-channel FIFO**: Messages on same channel delivered in publish order
- **No persistence**: Pub/sub messages are not persisted to disk

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
