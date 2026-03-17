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
│     └── Connection task acts as coordinator                     │
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

## Connection Establishment

### Accept Flow

1. **TCP Accept**: Acceptor receives connection from OS
2. **maxclients Check**: If at limit, close immediately (before TLS)
3. **TLS Handshake** (if enabled): Negotiate encryption
4. **Thread Assignment**: ConnectionAssigner selects target thread
5. **Initialize**: Create ConnectionState, assign ID

**Note:** maxclients is checked before TLS handshake to avoid CPU-expensive handshakes under resource exhaustion. Clients at the limit receive a TCP RST or TLS error, not the Redis error message.

### Admin Port Exception

Connections to the admin port (default 6380) are **not** subject to `maxclients`. This ensures operators can always connect for debugging even when the main port is at capacity.

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

    /// Protocol version (RESP2 or RESP3, negotiated via HELLO)
    pub protocol_version: ProtocolVersion,

    /// Transaction state (MULTI/EXEC)
    pub tx_queue: Option<Vec<ParsedCommand>>,
    pub watches: HashMap<Bytes, u64>,  // key -> version at WATCH time

    /// Pub/Sub state
    pub subscriptions: HashSet<Bytes>, // Channels
    pub patterns: HashSet<Bytes>,      // Patterns (PSUBSCRIBE)
    pub pubsub_mode: bool,

    /// Blocking state (BLPOP, BRPOP, BLMOVE, etc.)
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

> **Note:** `BlockedState` is fully specified. See [BLOCKING.md](BLOCKING.md) for complete blocking command semantics including registration flow, edge cases, and interaction with other connection states.

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

> **Note:** QUIT is never queued. Issuing QUIT during MULTI immediately closes the connection and discards all queued commands. WATCHed keys are automatically unwatched.

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

See [PUBSUB.md](PUBSUB.md) for complete pub/sub architecture.

### Blocked Mode

Entered via blocking commands (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP).

- Connection waits for data or timeout
- Unblocked when: data pushed, timeout expires, or client disconnects
- Returns to normal mode after unblocking

See [BLOCKING.md](BLOCKING.md) for blocking command design.

### State Transition Rules

This section specifies the complete state machine for connection states, including all valid transitions and error handling.

**Complete State Transition Table:**

| From State | Command/Event | To State | Notes |
|------------|---------------|----------|-------|
| NORMAL | MULTI | TRANSACTION | Start transaction |
| NORMAL | SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE | PUBSUB | Enter pub/sub mode |
| NORMAL | BLPOP/BRPOP/BLMOVE | BLOCKED | Enter blocking mode |
| NORMAL | QUIT | CLOSED | Clean disconnect |
| NORMAL | Any other command | NORMAL | Execute and stay in state |
| TRANSACTION | EXEC | NORMAL | Execute transaction |
| TRANSACTION | DISCARD | NORMAL | Abort transaction |
| TRANSACTION | QUIT | CLOSED | Discard queue, disconnect |
| TRANSACTION | WATCH/UNWATCH | TRANSACTION | Modify watches |
| TRANSACTION | Other commands | TRANSACTION | Queue command |
| PUBSUB | UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE (all) | NORMAL | When subscription count = 0 |
| PUBSUB | UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE (partial) | PUBSUB | Reduce subscriptions |
| PUBSUB | SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE | PUBSUB | Add subscriptions |
| PUBSUB | PING | PUBSUB | Allowed, responds PONG |
| PUBSUB | QUIT | CLOSED | Unsubscribe all, disconnect |
| BLOCKED | Timeout | NORMAL | Return nil response |
| BLOCKED | Data received | NORMAL | Return data |
| BLOCKED | QUIT | CLOSED | Cancel block, disconnect |
| BLOCKED | Client disconnect | - | Clean up wait registration |

**Invalid State Transitions:**

| From State | Command | Error Response |
|------------|---------|----------------|
| TRANSACTION | MULTI | `-ERR MULTI calls can not be nested` |
| TRANSACTION | SUBSCRIBE/PSUBSCRIBE | `-ERR SUBSCRIBE not allowed inside MULTI` |
| PUBSUB | MULTI | `-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context` |
| PUBSUB | GET/SET/etc. | `-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context` |
| BLOCKED | MULTI | `-ERR connection is blocked, command not allowed` |
| BLOCKED | GET/SET/etc. | `-ERR connection is blocked, command not allowed` |

**Commands Allowed per State:**

| Command | NORMAL | TRANSACTION | PUBSUB | BLOCKED |
|---------|--------|-------------|--------|---------|
| **All data commands** | Execute | Queue | Error | Error |
| **MULTI** | Enter TX | Error (nested) | Error | Error |
| **EXEC** | Error (no MULTI) | Execute TX | Error | Error |
| **DISCARD** | Error (no MULTI) | Abort TX | Error | Error |
| **WATCH** | Set watches | Queue (no-op*) | Error | Error |
| **UNWATCH** | Clear watches | Clear watches | Error | Error |
| **SUBSCRIBE** | Enter pubsub | Error | Add sub | Error |
| **UNSUBSCRIBE** | No-op | Error | Remove sub | Error |
| **PSUBSCRIBE** | Enter pubsub | Error | Add pattern | Error |
| **PUNSUBSCRIBE** | No-op | Error | Remove pattern | Error |
| **PING** | Execute | Queue | Execute | Execute** |
| **QUIT** | Disconnect | Discard+disconnect | Unsub+disconnect | Cancel+disconnect |
| **CLIENT** | Execute | Queue | Error | Error |
| **DEBUG** | Execute | Queue | Error | Error |

*WATCH inside MULTI is queued but has no effect (watches must be set before MULTI).
**PING during block returns immediately; does not cancel block.

**State Detection:**

```rust
impl ConnectionState {
    pub fn current_mode(&self) -> ConnectionMode {
        if self.blocked.is_some() {
            ConnectionMode::Blocked
        } else if self.pubsub_mode {
            ConnectionMode::PubSub
        } else if self.tx_queue.is_some() {
            ConnectionMode::Transaction
        } else {
            ConnectionMode::Normal
        }
    }

    pub fn can_execute(&self, cmd: &ParsedCommand) -> Result<(), StateError> {
        let mode = self.current_mode();
        let cmd_name = cmd.name.to_ascii_uppercase();

        match (mode, cmd_name.as_ref()) {
            // Always allowed in any state
            (_, b"QUIT") => Ok(()),

            // PING allowed everywhere
            (_, b"PING") => Ok(()),

            // Normal mode - everything allowed
            (ConnectionMode::Normal, _) => Ok(()),

            // Transaction mode
            (ConnectionMode::Transaction, b"MULTI") =>
                Err(StateError::NestedMulti),
            (ConnectionMode::Transaction, b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE") =>
                Err(StateError::SubscribeInMulti),
            (ConnectionMode::Transaction, _) => Ok(()), // Queue it

            // Pub/Sub mode - very restricted
            (ConnectionMode::PubSub, b"SUBSCRIBE" | b"UNSUBSCRIBE" |
                b"PSUBSCRIBE" | b"PUNSUBSCRIBE" |
                b"SSUBSCRIBE" | b"SUNSUBSCRIBE") => Ok(()),
            (ConnectionMode::PubSub, _) =>
                Err(StateError::NotAllowedInPubSub),

            // Blocked mode - very restricted
            (ConnectionMode::Blocked, _) =>
                Err(StateError::ConnectionBlocked),
        }
    }
}
```

**QUIT Behavior by State:**

| State | QUIT Behavior |
|-------|---------------|
| NORMAL | Close connection immediately |
| TRANSACTION | Discard queued commands, clear watches, close connection |
| PUBSUB | Unsubscribe from all channels/patterns, close connection |
| BLOCKED | Cancel blocking operation (return nil), clean up wait registration, close connection |

**QUIT Implementation:**

```rust
async fn handle_quit(&mut self) -> Result<(), ConnectionError> {
    // Clean up based on current state
    match self.state.current_mode() {
        ConnectionMode::Transaction => {
            // Discard queued commands
            self.state.tx_queue = None;
            // Clear watches
            self.state.watches.clear();
        }
        ConnectionMode::PubSub => {
            // Unsubscribe from all channels
            for channel in self.state.subscriptions.drain() {
                self.shard_tx.send(ShardMessage::Unsubscribe {
                    channel,
                    conn_id: self.state.id,
                }).await?;
            }
            // Unsubscribe from all patterns
            for pattern in self.state.patterns.drain() {
                self.shard_tx.send(ShardMessage::Punsubscribe {
                    pattern,
                    conn_id: self.state.id,
                }).await?;
            }
            self.state.pubsub_mode = false;
        }
        ConnectionMode::Blocked => {
            // Cancel blocking operation
            if let Some(blocked) = self.state.blocked.take() {
                // Send nil response to waiting task
                let _ = blocked.response_tx.send(Response::Bulk(None));
                // Unregister from wait queues
                for key in blocked.keys {
                    self.shard_tx.send(ShardMessage::UnregisterBlockedClient {
                        key,
                        conn_id: self.state.id,
                    }).await?;
                }
            }
        }
        ConnectionMode::Normal => {
            // Nothing special to clean up
        }
    }

    // Send +OK before closing
    self.send_response(Response::Simple(Bytes::from_static(b"OK"))).await?;

    // Close connection
    Ok(())
}
```

**Timeout Behavior by State:**

| State | Idle Timeout Behavior |
|-------|----------------------|
| NORMAL | Disconnect after `timeout` seconds of no commands |
| TRANSACTION | Disconnect (queued commands discarded) |
| PUBSUB | **Do not timeout** - waiting for messages is expected |
| BLOCKED | **Do not timeout** - explicit timeout in blocking command |

**Rationale for PubSub/Blocked No-Timeout:**
- Pub/Sub clients may be idle for hours waiting for messages
- Blocked clients have their own timeout (BLPOP with timeout argument)
- Idle timeout would interrupt valid waiting states

**Concurrent State Assertions:**

These states are mutually exclusive:

```rust
// Invariant: only one mode at a time
debug_assert!(
    [
        self.tx_queue.is_some(),
        self.pubsub_mode,
        self.blocked.is_some(),
    ].iter().filter(|&&x| x).count() <= 1,
    "Connection in multiple states simultaneously"
);
```

**Metrics for State Transitions:**

| Metric | Description |
|--------|-------------|
| `frogdb_connection_state_transitions_total` | Total state transitions (by `from`, `to` labels) |
| `frogdb_connection_invalid_transitions_total` | Rejected state transitions |
| `frogdb_connection_quit_by_state_total` | QUIT commands by connection state |

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
- `frogdb_connections_rejected_total` metric increments

### Admin Port

The admin port (`admin_port`, default 6380) provides administrative access with special characteristics.

#### Configuration

```toml
[server]
port = 6379             # Main client port
admin_port = 6380       # Admin port (separate listener)
admin_bind = "127.0.0.1"  # Bind admin to localhost only (security)
maxclients = 10000      # Limit on main port only
```

#### Admin Port vs Main Port

| Property | Main Port (6379) | Admin Port (6380) |
|----------|------------------|-------------------|
| maxclients | Enforced | **Exempt** |
| TLS | If configured | **Always plaintext** |
| ACL | Enforced | Optional (configurable) |
| Protocol | RESP2/RESP3 | RESP2/RESP3 |
| Typical clients | Application clients | Orchestrator, operators |

**Why admin port stays plaintext:**
- Simplifies orchestrator configuration (no client certs needed)
- Admin port should be bound to localhost or private network
- Reduces TLS overhead for high-frequency health checks
- Matches DragonflyDB behavior

#### Admin-Only Commands

These commands only work on the admin port (rejected on main port):

| Command | Description |
|---------|-------------|
| `FROGCLUSTER *` | Cluster management (topology, migration) |
| `DEBUG *` | Debugging commands |
| `CONFIG SET` | Runtime configuration changes |
| `SHUTDOWN` | Server shutdown |
| `FAILOVER` | Trigger manual failover |
| `REPLICAOF` | Change replication configuration |

#### Regular Commands on Admin Port

Regular Redis commands (GET, SET, etc.) are **allowed** on admin port:
- Enables orchestrator health checks (`PING`)
- Allows operator debugging (`DEBUG OBJECT key`)
- Supports administrative scripts

#### ACL on Admin Port

By default, admin port **skips ACL checks** (equivalent to `admin_acl = "skip"`).

```toml
[server]
# Options: "skip" (default), "require", "admin_user"
admin_acl = "skip"         # No authentication on admin port
admin_acl = "require"      # Require normal ACL authentication
admin_acl = "admin_user"   # Require "admin" user specifically
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| `skip` | No auth required | Localhost-only access |
| `require` | Normal ACL applies | Production with network exposure |
| `admin_user` | Only "admin" user can connect | Strict separation of duties |

#### Orchestrator Access

The orchestrator connects via admin port for:
- Health checks (`PING`)
- Topology updates (`FROGCLUSTER CONFIG`)
- Replication setup (`REPLICAOF`)
- Migration coordination (`FROGCLUSTER MIGRATE`)
- Failover commands (`FAILOVER`)

```
Orchestrator ──────────────────────► FrogDB Admin Port (6380)
    │                                       │
    │  PING                                 │  Health check
    │  FROGCLUSTER CONFIG {...}             │  Topology push
    │  FROGCLUSTER MIGRATE ...              │  Slot migration
    │                                       │
```

#### Security Best Practices

**CRITICAL:** Admin port should only bind to localhost or private network. Never expose admin port to public internet.

| Deployment | Recommended `admin_bind` |
|------------|--------------------------|
| Development | `127.0.0.1` |
| Kubernetes (sidecar orchestrator) | `127.0.0.1` |
| Kubernetes (separate orchestrator pod) | Pod network CIDR |
| VPC with orchestrator | VPC private subnet |
| Public cloud | Never expose publicly |

```toml
# Secure configuration example
[server]
port = 6379
bind = "0.0.0.0"           # Main port: accept from anywhere (with TLS)
admin_port = 6380
admin_bind = "127.0.0.1"   # Admin port: localhost only
tls_enabled = true         # TLS on main port
```

#### Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_admin_connections_total` | Total admin port connections |
| `frogdb_admin_connections_current` | Current admin port connections |
| `frogdb_admin_commands_total` | Commands executed on admin port |

**Security:** The admin port should be bound to localhost or protected by network policies. See [CLUSTER.md](CLUSTER.md#orchestrator-security) for admin API security options.

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

### Soft Limit Timer Behavior

The soft limit timer **resets** when the buffer drops below the soft limit:

```
t=0:   Buffer exceeds 8MB soft limit, timer starts
t=30:  Buffer drops to 5MB, timer RESETS
t=35:  Buffer exceeds soft limit again, timer RESTARTS
t=95:  (60s continuous) if still above, disconnect
```

This matches Redis behavior - clients that temporarily spike but recover are not penalized.

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

**Why backpressure as the primary throttling mechanism:**
- Automatically throttles at the source
- No configuration needed
- Works across all command types
- Matches Redis behavior

**Note:** For explicit per-user throttling, see [Per-ACL-User Rate Limiting](#per-acl-user-rate-limiting). TCP backpressure and ACL rate limiting are complementary: backpressure handles slow consumers, while ACL rate limits enforce per-user throughput policies.

**Trade-off:** A slow client can affect other clients on the same thread. Mitigations:
- Output buffer limits disconnect slow clients
- Client timeouts reclaim abandoned connections
- Connection distribution spreads load across threads

---

## Per-ACL-User Rate Limiting

FrogDB supports per-ACL-user rate limiting using a **token bucket algorithm**. Rate limits are configured via ACL rules and enforced at the connection layer before command execution.

### Configuration

Rate limits are set as ACL rules on individual users:

```
ACL SETUSER alice on >password ~* +@all ratelimit:cps=1000 ratelimit:bps=1048576
```

| ACL Rule | Description |
|----------|-------------|
| `ratelimit:cps=N` | Limit to N commands per second |
| `ratelimit:bps=N` | Limit to N bytes per second (raw command bytes) |
| `resetratelimit` | Clear all rate limits for the user |

Either or both limits can be set independently. When unconfigured, there is zero overhead (`Option<Arc<RateLimitState>>` check).

### Token Bucket Algorithm

Each configured limit maintains a token bucket:
- **Capacity:** 1 second of tokens (burst allowance, not configurable)
- **Refill rate:** N tokens per second (as configured)
- **Tokens consumed:** 1 per command (cps) or raw byte count (bps)

Tokens refill continuously. A brief burst up to 1 second of accumulated tokens is permitted, then the client is throttled.

### Enforcement

Rate limit state is **shared across all connections for the same ACL user**. If user `alice` has two connections, both draw from the same token buckets.

**Exempt commands:** These commands are never rate-limited, ensuring clients can always authenticate and perform basic health checks:
- `AUTH`, `HELLO`, `PING`, `QUIT`, `RESET`

**Admin port bypass:** Connections on the admin port are exempt from rate limits entirely, ensuring operator access is never throttled.

### MULTI/EXEC Interaction

Queued commands inside a `MULTI` transaction do not consume tokens individually. At `EXEC` time, the entire batch is checked atomically:
- **cps:** N tokens consumed (one per queued command)
- **bps:** total bytes of all queued commands consumed

If the rate limit would be exceeded at `EXEC` time, the entire transaction is rejected.

### Error Responses

When a rate limit is exceeded, the client receives:

```
-ERR rate limit exceeded: commands per second
-ERR rate limit exceeded: bytes per second
```

### Observability

Rate limit configuration is visible in standard ACL introspection commands:
- `ACL LIST` includes `ratelimit:cps=N` and/or `ratelimit:bps=N` in the rule string
- `ACL GETUSER` returns rate limit fields
- `INFO` includes a `# Ratelimit` section with per-user counters

### Example

```
# Set up a rate-limited application user
ACL SETUSER app on >apppass ~app:* +@read +@write ratelimit:cps=500 ratelimit:bps=524288

# Set up an unrestricted admin user
ACL SETUSER admin on >adminpass ~* +@all

# Remove rate limits from a user
ACL SETUSER app resetratelimit
```

---

## Client Commands

| Command | Description |
|---------|-------------|
| CLIENT LIST | List all connected clients |
| CLIENT ID | Get current connection ID |
| CLIENT SETNAME name | Set client name |
| CLIENT GETNAME | Get client name |
| CLIENT KILL [filter...] | Kill connections matching filters |
| CLIENT PAUSE timeout [WRITE\|ALL] | Pause clients for controlled operations |
| CLIENT UNPAUSE | Resume paused clients |
| CLIENT INFO | Get current client info |

### CLIENT PAUSE

Suspends client command processing for controlled operations like failover.

```
CLIENT PAUSE timeout [WRITE|ALL]
```

| Mode | Behavior |
|------|----------|
| `ALL` (default) | Block all client commands |
| `WRITE` | Block write commands only (recommended for failover) |

**WRITE mode specifics:**
- Read commands continue executing
- EVAL/EVALSHA blocked (may contain writes)
- PUBLISH blocked
- Replication to replicas continues
- Keys are not evicted/expired during pause

**Usage:** Before failover, pause writes to ensure replicas catch up:
```
CLIENT PAUSE 60000 WRITE
# Wait for replica to catch up
# Promote replica
CLIENT UNPAUSE
```

See [CLUSTER.md](CLUSTER.md#manual-failover) for failover procedures.

### CLIENT KILL Filters

Multiple filters can be combined (logical AND):

| Filter | Description |
|--------|-------------|
| `ID client-id` | Kill by connection ID |
| `ADDR ip:port` | Kill by remote address |
| `LADDR ip:port` | Kill by local (bind) address |
| `TYPE normal\|master\|replica\|pubsub` | Kill by client type |
| `USER username` | Kill all connections for ACL user |

**Examples:**
```
CLIENT KILL ID 123                    # Specific connection
CLIENT KILL TYPE pubsub               # All pub/sub clients
CLIENT KILL USER compromised_account  # Security: revoke user
CLIENT KILL ADDR 10.0.0.5:54321 TYPE normal  # Combined filters
```

**Return value:** Number of clients killed (may be 0).

**Note:** Admin port connections cannot be killed from the main port.

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
| `frogdb_connections_rejected_total` | Counter | Rejected (maxclients) |
| `frogdb_blocked_clients` | Gauge | Clients in blocked state |
| `frogdb_pubsub_clients` | Gauge | Clients in pub/sub mode |
| `frogdb_client_output_buffer_bytes` | Gauge | Current output buffer usage (by `type` label: normal, pubsub, replica) |
| `frogdb_client_output_buffer_limit_disconnections_total` | Counter | Buffer limit disconnects |
| `frogdb_client_timeout_disconnections_total` | Counter | Timeout disconnects |

---

## References

- [TRANSACTIONS.md](TRANSACTIONS.md) - Transaction state and MULTI/EXEC
- [BLOCKING.md](BLOCKING.md) - Blocking command design
- [PUBSUB.md](PUBSUB.md) - Pub/sub architecture
- [CONCURRENCY.md](CONCURRENCY.md) - Channel backpressure details
- [LIFECYCLE.md](LIFECYCLE.md) - Server startup and shutdown
- [FAILURE_MODES.md](FAILURE_MODES.md) - Connection and network failures
- [OBSERVABILITY.md](OBSERVABILITY.md) - Full metrics reference
- [CONFIGURATION.md](CONFIGURATION.md) - Metrics endpoint configuration
