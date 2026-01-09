# FrogDB Connection Management

This document details connection handling, state management, and the client lifecycle in FrogDB.

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

> **Note:** `BlockedState` is defined for future compatibility but blocking commands (BLPOP, BRPOP, BLMOVE, etc.) are not yet implemented. Current behavior: these commands return `-ERR not implemented`. See [FAILURE_MODES.md](FAILURE_MODES.md#blocking-command-queue) for planned implementation.

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

| Setting | Default | Description |
|---------|---------|-------------|
| `maxclients` | 10000 | Maximum simultaneous connections |
| `timeout` | 0 | Idle connection timeout (0 = disabled) |
| `tcp_keepalive` | 300 | TCP keepalive interval (seconds) |

### Exceeding Limits

When `maxclients` is reached:
1. New connections are accepted
2. Server sends `-ERR max number of clients reached`
3. Connection is immediately closed

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

When a client can't consume responses fast enough:
1. Write buffer fills
2. Connection stops reading new commands
3. Shard message channel applies backpressure
4. Eventually, connection times out or catches up

---

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_connections_total` | Counter | Total connections accepted |
| `frogdb_connections_current` | Gauge | Current active connections |
| `frogdb_connections_rejected` | Counter | Rejected (maxclients) |
| `frogdb_blocked_clients` | Gauge | Clients in blocked state |
| `frogdb_pubsub_clients` | Gauge | Clients in pub/sub mode |

See [OPERATIONS.md](OPERATIONS.md) for metrics endpoint configuration.
