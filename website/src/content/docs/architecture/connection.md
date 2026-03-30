---
title: "Connection Management"
description: "Connection state machine, rate limiting, lifecycle details, and the pinned connection model."
sidebar:
  order: 13
---
Connection state machine, rate limiting, lifecycle details, and the pinned connection model.

## Connection Model

FrogDB uses a **pinned connection model** where each client connection is assigned to a single thread for its entire lifetime, but can coordinate with any shard via message-passing.

---

## Connection Establishment

### Accept Flow

1. **TCP Accept**: Acceptor receives connection from OS
2. **maxclients Check**: If at limit, close immediately (before TLS)
3. **TLS Handshake** (if enabled): Negotiate encryption
4. **Thread Assignment**: ConnectionAssigner selects target thread
5. **Initialize**: Create ConnectionState, assign ID

maxclients is checked before TLS handshake to avoid CPU-expensive handshakes under resource exhaustion.

### Connection Assignment

```rust
pub trait ConnectionAssigner: Send + Sync {
    fn assign(&self, client_addr: &SocketAddr) -> usize;
}

pub struct RoundRobinAssigner {
    next: AtomicUsize,
    num_threads: usize,
}

impl ConnectionAssigner for RoundRobinAssigner {
    fn assign(&self, _addr: &SocketAddr) -> usize {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        idx % self.num_threads
    }
}
```

---

## Connection State

```rust
pub struct ConnectionState {
    pub id: u64,
    pub name: Option<Bytes>,
    pub addr: SocketAddr,
    pub created_at: Instant,
    pub auth: AuthState,
    pub protocol_version: ProtocolVersion,
    pub tx_queue: Option<Vec<ParsedCommand>>,
    pub watches: HashMap<Bytes, u64>,
    pub subscriptions: HashSet<Bytes>,
    pub patterns: HashSet<Bytes>,
    pub pubsub_mode: bool,
    pub blocked: Option<BlockedState>,
}
```

---

## State Transitions

```
                    +------------------+
                    |     NORMAL       |
                    |  (default mode)  |
                    +--------+---------+
                             |
           +-----------------+-----------------+
           |                 |                 |
           v                 v                 v
    +-------------+  +-------------+  +-------------+
    | TRANSACTION |  |   PUBSUB    |  |   BLOCKED   |
    |  (MULTI)    |  | (SUBSCRIBE) |  |  (BLPOP)    |
    +------+------+  +------+------+  +------+------+
           |                |                 |
           | EXEC/DISCARD   | Unsubscribe    | Timeout/Push
           |                | from all       |
           +----------------+-----------------+
                            |
                            v
                     +-------------+
                     |   NORMAL    |
                     +-------------+
```

These states are mutually exclusive (enforced by debug assertion).

### State Transition Table

| From State | Command/Event | To State |
|------------|---------------|----------|
| NORMAL | MULTI | TRANSACTION |
| NORMAL | SUBSCRIBE | PUBSUB |
| NORMAL | BLPOP/BRPOP/BLMOVE | BLOCKED |
| TRANSACTION | EXEC | NORMAL |
| TRANSACTION | DISCARD | NORMAL |
| PUBSUB | Unsubscribe all | NORMAL |
| BLOCKED | Data received | NORMAL |
| BLOCKED | Timeout | NORMAL |

### Commands Allowed per State

| Command | NORMAL | TRANSACTION | PUBSUB | BLOCKED |
|---------|--------|-------------|--------|---------|
| **All data commands** | Execute | Queue | Error | Error |
| **MULTI** | Enter TX | Error (nested) | Error | Error |
| **EXEC** | Error | Execute TX | Error | Error |
| **SUBSCRIBE** | Enter pubsub | Error | Add sub | Error |
| **PING** | Execute | Queue | Execute | Execute |
| **QUIT** | Disconnect | Discard+disconnect | Unsub+disconnect | Cancel+disconnect |

### Timeout Behavior by State

| State | Idle Timeout Behavior |
|-------|----------------------|
| NORMAL | Disconnect after `timeout` seconds |
| TRANSACTION | Disconnect (queued commands discarded) |
| PUBSUB | **Do not timeout** - waiting for messages is expected |
| BLOCKED | **Do not timeout** - explicit timeout in blocking command |

---

## Output Buffer Limits

Per-client output buffer limits prevent slow clients from consuming unbounded memory.

| Client Type | Default | Rationale |
|-------------|---------|-----------|
| `normal` | Unlimited | Most clients consume responses quickly |
| `pubsub` | 32MB hard, 8MB soft/60s | Subscribers can lag behind publishers |
| `replica` | 256MB hard, 64MB soft/60s | Replication streams are large but critical |

The soft limit timer **resets** when the buffer drops below the soft limit, matching Redis behavior.

---

## Backpressure Strategy

FrogDB uses TCP backpressure as natural rate limiting:

```
Slow client (not reading responses)
    -> TCP send buffer fills (OS-level)
    -> Shard worker blocks on socket write
    -> Shard message channel fills (1024 messages)
    -> Coordinator blocks on channel send
    -> Other clients on same thread experience latency
```

TCP backpressure and ACL rate limiting are complementary: backpressure handles slow consumers, while ACL rate limits enforce per-user throughput policies.

---

## Per-ACL-User Rate Limiting

FrogDB supports per-ACL-user rate limiting using a **token bucket algorithm**.

```
ACL SETUSER alice on >password ~* +@all ratelimit:cps=1000 ratelimit:bps=1048576
```

- Rate limit state is **shared across all connections for the same ACL user**
- Capacity: 1 second of tokens (burst allowance)
- MULTI/EXEC: Tokens consumed atomically at EXEC time
- Exempt commands: AUTH, HELLO, PING, QUIT, RESET
- Admin port connections are exempt
