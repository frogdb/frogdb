# FrogDB Connection Management

This document details connection handling, state management, rate limiting, and the client lifecycle in FrogDB.

## Connection Model

FrogDB uses a **pinned connection model** where each client connection is assigned to a single thread for its entire lifetime, but can coordinate with any shard via message-passing.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Connection Lifecycle                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Accept                                                       │
│     └── Acceptor receives TCP connection                        │
│     └── ConnectionAssigner selects target thread (round-robin)  │
│                                                                  │
│  2. Initialize                                                   │
│     └── Create ConnectionState                                  │
│     └── Assign unique connection ID                             │
│     └── Set default user (if ACL disabled)                      │
│                                                                  │
│  3. Command Loop                                                 │
│     └── Read from socket → Parse RESP → Route → Execute         │
│     └── Connection fiber acts as coordinator                    │
│     └── Can message any shard for key operations                │
│                                                                  │
│  4. Close                                                        │
│     └── Client disconnect or QUIT command                       │
│     └── Clean up watches, subscriptions                         │
│     └── Release blocked state if any                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Connection State

Each connection maintains comprehensive state:

```rust
pub struct ConnectionState {
    /// Unique connection ID (monotonically increasing)
    pub id: u64,

    /// Client metadata
    pub name: Option<Bytes>,           // CLIENT SETNAME
    pub addr: SocketAddr,              // Remote address
    pub created_at: Instant,           // Connection time

    /// Authentication
    pub auth: AuthState,

    /// Transaction state (MULTI/EXEC)
    pub tx_queue: Option<Vec<ParsedCommand>>,
    pub watches: HashMap<Bytes, u64>,  // key -> version at WATCH time

    /// Pub/Sub state
    pub subscriptions: HashSet<Bytes>, // Channels
    pub patterns: HashSet<Bytes>,      // Patterns (PSUBSCRIBE)
    pub pubsub_mode: bool,

    /// Blocking state (future)
    pub blocked: Option<BlockedState>,
}

pub struct AuthState {
    /// The authenticated user (or "default")
    pub user: AuthenticatedUser,
    /// Whether AUTH has been called
    pub authenticated: bool,
}

pub struct BlockedState {
    /// Keys being waited on
    pub keys: Vec<Bytes>,
    /// Operation type (BLPOP, BRPOP, etc.)
    pub op: BlockingOp,
    /// Timeout
    pub deadline: Option<Instant>,
    /// Channel to send result when unblocked
    pub response_tx: oneshot::Sender<Response>,
}
```

> **Note:** `BlockedState` is defined for future compatibility but blocking commands (BLPOP, BRPOP, BLMOVE, etc.) are not yet implemented. Current behavior: these commands return `-ERR not implemented`. See [BLOCKING.md](BLOCKING.md) for planned implementation.

---

## State Transitions

```
                    ┌─────────────────┐
                    │     NORMAL      │
                    │  (default mode) │
                    └───────┬─────────┘
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
          ▼                 ▼                 ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ TRANSACTION │  │   PUBSUB    │  │   BLOCKED   │
   │  (MULTI)    │  │ (SUBSCRIBE) │  │  (BLPOP)    │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                 │
          │ EXEC/DISCARD   │ Unsubscribe    │ Timeout/Push
          │                │ from all       │
          └────────────────┴────────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   NORMAL    │
                    └─────────────┘
```

### Normal Mode

Default state. Can execute any command.

### Transaction Mode

Entered via `MULTI`. Commands are queued, not executed.

| Command | Behavior |
|---------|----------|
| MULTI | Enter transaction mode |
| Commands | Queued, return QUEUED |
| EXEC | Execute all, return results |
| DISCARD | Abort, return to normal |
| WATCH | Set key watches (before MULTI) |

See [TRANSACTIONS.md](TRANSACTIONS.md) for full transaction documentation.

### Pub/Sub Mode

Entered via `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`.

**Allowed commands in pub/sub mode:**
- SUBSCRIBE / UNSUBSCRIBE
- PSUBSCRIBE / PUNSUBSCRIBE
- SSUBSCRIBE / SUNSUBSCRIBE
- PING
- QUIT

All other commands return error.

### Blocked Mode (Future)

Entered via blocking commands (BLPOP, BRPOP, BLMOVE).

- Connection waits for data or timeout
- Unblocked when: data pushed, timeout expires, or client disconnects
- Returns to normal mode after unblocking

See [BLOCKING.md](BLOCKING.md) for blocking command design.

---

## Connection Assignment

### ConnectionAssigner Trait

```rust
/// Abstraction for assigning connections to threads
pub trait ConnectionAssigner: Send + Sync {
    /// Select a thread index for a new connection
    fn assign(&self, client_addr: &SocketAddr) -> usize;
}
```

### Round-Robin (Default)

```rust
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

### Alternative Strategies (Future)

| Strategy | Description |
|----------|-------------|
| Least-Connections | Assign to thread with fewest active connections |
| Consistent Hash | Hash client address for deterministic assignment |
| Weighted | Assign based on thread CPU utilization |

---

## Connection Limits

### Maximum Clients

**Configuration:**
```toml
[server]
maxclients = 10000  # Maximum simultaneous connections (0 = OS limit)
```

**Behavior when limit reached:**
- New connections rejected immediately
- Error: `max number of clients reached`
- `connection_rejected_total` metric increments

### Reserved Connections

FrogDB reserves a small number of connections for admin operations:

```toml
[server]
reserved_connections = 32  # Always allow admin commands
```

Reserved connections ensure operators can always connect for debugging even when `maxclients` is reached.

### TCP Keepalive

| Setting | Default | Description |
|---------|---------|-------------|
| `tcp_keepalive` | 300 | TCP keepalive interval (seconds) |

---

## Client Timeout

Idle clients are disconnected after timeout:

```toml
[server]
timeout = 0  # Seconds of idle before disconnect (0 = never)
```

**Recommendation:** Set `timeout > 0` in production to reclaim abandoned connections.

### Timeout Behavior

- Timer resets on any client activity (commands or responses)
- `PING` commands reset timeout (use for keepalive)
- Blocked commands (BLPOP, etc.) do not count as idle

---

## Output Buffer Limits

Per-client output buffer limits prevent slow clients from consuming unbounded memory.

### Configuration

```toml
[client]
# Normal clients (default)
client_output_buffer_limit_normal = "0 0 0"  # Unlimited

# Pub/Sub clients (can accumulate messages)
client_output_buffer_limit_pubsub = "32mb 8mb 60"  # Hard 32MB, soft 8MB for 60s

# Replica clients (replication stream)
client_output_buffer_limit_replica = "256mb 64mb 60"
```

**Format:** `<hard_limit> <soft_limit> <soft_seconds>`

### Limit Types

| Limit | Behavior |
|-------|----------|
| **Hard limit** | Disconnect immediately when exceeded |
| **Soft limit** | Disconnect if exceeded for `soft_seconds` continuously |
| **No limit (0 0 0)** | Unlimited buffer growth |

### Client Types

| Type | Default | Rationale |
|------|---------|-----------|
| `normal` | Unlimited | Most clients consume responses quickly |
| `pubsub` | 32MB hard, 8MB soft/60s | Subscribers can lag behind publishers |
| `replica` | 256MB hard, 64MB soft/60s | Replication streams are large but critical |

### Behavior on Disconnect

When buffer limit exceeded:
- Connection closed immediately
- Client receives no response (already overloaded)
- `client_output_buffer_limit_disconnections_total` metric increments
- Log entry: `"Client disconnected: output buffer limit exceeded"`

---

## Buffer Management

### Read Buffer

- Initial size: 16KB
- Max size: Configurable (default 1GB for queries)
- Grows as needed for large commands

### Write Buffer

- Ring buffer for responses
- Flushes on: buffer full, command complete, or idle
- Backpressure when write buffer fills

### Backpressure Strategy

FrogDB uses TCP backpressure as natural rate limiting rather than explicit throttling.

**Backpressure Flow:**

```
Slow client (not reading responses)
    │
    ▼
TCP send buffer fills (OS-level)
    │
    ▼
Shard worker blocks on socket write
    │
    ▼
Shard message channel fills (1024 messages)
    │
    ▼
Coordinator blocks on channel send
    │
    ▼
Other clients on same thread experience latency
```

**Why backpressure over rate limiting:**
- Automatically throttles at the source
- No configuration needed
- Works across all command types
- No token bucket state to maintain
- Matches Redis behavior

**Trade-off:** A slow client can affect other clients on the same thread. Mitigations:
- Output buffer limits disconnect slow clients
- Client timeouts reclaim abandoned connections
- Connection distribution spreads load across threads

---

## Command-Level Rate Limiting

FrogDB does not provide built-in command-level rate limiting. Implement at application layer if needed.

### Application-Layer Example

```python
# Python example using ratelimit library
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls=1000, period=1)  # 1000 ops/second
def rate_limited_set(key, value):
    return redis.set(key, value)
```

### Proxy-Layer Example

Use a Redis-compatible proxy with rate limiting:
- **Envoy** with Redis filter and rate limit service
- **Twemproxy** with connection pooling
- **Redis Cluster Proxy** with traffic shaping

### Future Consideration

Per-ACL-user rate limiting is not currently planned but could be added:

```toml
# Hypothetical future syntax
[acl.users.limited_user]
commands_per_second = 1000
bytes_per_second = 10485760  # 10MB/s
```

---

## Client Commands

| Command | Description |
|---------|-------------|
| CLIENT LIST | List all connected clients |
| CLIENT ID | Get current connection ID |
| CLIENT SETNAME name | Set client name |
| CLIENT GETNAME | Get client name |
| CLIENT KILL ID id | Kill connection by ID |
| CLIENT PAUSE ms | Pause all clients |
| CLIENT UNPAUSE | Resume clients |
| CLIENT INFO | Get current client info |

### CLIENT LIST Output

```
id=123 addr=192.168.1.10:54321 fd=5 name=myapp age=100 idle=0 flags=N db=0 ...
```

**Flags:**
| Flag | Meaning |
|------|---------|
| N | Normal client |
| M | Master (replication) |
| S | Slave (replication) |
| b | Blocked |
| t | In transaction |
| P | Pub/sub mode |

---

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_connections_total` | Counter | Total connections accepted |
| `frogdb_connections_current` | Gauge | Current active connections |
| `frogdb_connections_rejected` | Counter | Rejected (maxclients) |
| `frogdb_blocked_clients` | Gauge | Clients in blocked state |
| `frogdb_pubsub_clients` | Gauge | Clients in pub/sub mode |
| `frogdb_client_output_buffer_limit_disconnections_total` | Counter | Buffer limit disconnects |
| `frogdb_client_timeout_disconnections_total` | Counter | Timeout disconnects |

---

## References

- [TRANSACTIONS.md](TRANSACTIONS.md) - Transaction state and MULTI/EXEC
- [BLOCKING.md](BLOCKING.md) - Blocking command design
- [CONCURRENCY.md](CONCURRENCY.md) - Channel backpressure details
- [FAILURE_MODES.md](FAILURE_MODES.md) - Connection and network failures
- [OBSERVABILITY.md](OBSERVABILITY.md) - Full metrics reference
- [OPERATIONS.md](OPERATIONS.md) - Metrics endpoint configuration
