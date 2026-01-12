# FrogDB Command Execution

This document details the command execution pipeline, command trait interface, arity validation, and command flags.

## Command Flow

```
1. Connection accepted by Acceptor
   └── Assigned to Thread N via round-robin (connection pinned for I/O)

2. Client sends: SET mykey myvalue
   └── Thread N's event loop receives bytes

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

---

## Command Trait

```rust
pub trait Command: Send + Sync {
    /// Command name (e.g., "GET", "SET", "ZADD")
    fn name(&self) -> &'static str;

    /// Expected argument count
    fn arity(&self) -> Arity;

    /// Command behavior flags
    fn flags(&self) -> CommandFlags;

    /// Execute the command
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Extract key(s) from arguments for routing
    fn keys(&self, args: &[Bytes]) -> Vec<&[u8]>;
}
```

---

## CommandError

Error type returned by command execution. Maps to RESP error responses.

```rust
/// Core error variants for command execution.
/// This enum will expand as more commands are implemented.
#[derive(Debug, Clone)]
pub enum CommandError {
    // === Syntax/Argument Errors ===

    /// Wrong number of arguments for command
    /// Response: "ERR wrong number of arguments for '{command}' command"
    WrongArity { command: &'static str },

    /// Invalid argument value or format
    /// Response: "ERR {message}"
    InvalidArgument { message: String },

    /// General syntax error
    /// Response: "ERR syntax error"
    SyntaxError,

    // === Type Errors ===

    /// Operation against wrong value type
    /// Response: "WRONGTYPE Operation against a key holding the wrong kind of value"
    WrongType,

    /// Value is not an integer or out of range
    /// Response: "ERR value is not an integer or out of range"
    NotInteger,

    /// Value is not a valid float
    /// Response: "ERR value is not a valid float"
    NotFloat,

    // === System Errors ===

    /// Out of memory (when maxmemory reached and no eviction possible)
    /// Response: "OOM command not allowed when used memory > 'maxmemory'"
    OutOfMemory,

    /// Internal server error
    /// Response: "ERR {message}"
    Internal { message: String },
}

impl CommandError {
    /// Convert to RESP error response
    pub fn to_response(&self) -> Response {
        Response::Error(self.to_bytes())
    }

    fn to_bytes(&self) -> Bytes {
        match self {
            Self::WrongArity { command } => {
                Bytes::from(format!("ERR wrong number of arguments for '{}' command", command))
            }
            Self::InvalidArgument { message } => {
                Bytes::from(format!("ERR {}", message))
            }
            Self::SyntaxError => {
                Bytes::from_static(b"ERR syntax error")
            }
            Self::WrongType => {
                Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            Self::NotInteger => {
                Bytes::from_static(b"ERR value is not an integer or out of range")
            }
            Self::NotFloat => {
                Bytes::from_static(b"ERR value is not a valid float")
            }
            Self::OutOfMemory => {
                Bytes::from_static(b"OOM command not allowed when used memory > 'maxmemory'")
            }
            Self::Internal { message } => {
                Bytes::from(format!("ERR {}", message))
            }
        }
    }
}
```

**Note:** This enum will expand during implementation as new error cases are discovered.
Additional variants to add as needed:
- `CrossSlot` - For multi-key operations across hash slots
- `NoAuth` / `NoPerm` - For authentication/authorization
- `ReadOnly` - For write commands on replicas
- `Busy` - For blocking operations

---

## Arity

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

### Examples

| Command | Arity | Description |
|---------|-------|-------------|
| GET | `Fixed(1)` | Exactly 1 argument |
| SET | `Range { min: 2, max: 6 }` | 2-6 arguments (key, value, [EX/PX/NX/XX]) |
| MGET | `AtLeast(1)` | 1 or more keys |
| PING | `Range { min: 0, max: 1 }` | 0 or 1 argument |

---

## Command Flags

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

---

## Flag Usage

Flags inform the router and execution engine about command characteristics:

| Flag | Purpose |
|------|---------|
| `WRITE` | Commands with this flag update the WAL |
| `READONLY` | Commands can be load-balanced to replicas (future) |
| `FAST` | O(1) commands, suitable for hot paths |
| `BLOCKING` | Commands require special timeout handling |
| `MULTI_KEY` | Commands may trigger scatter-gather coordination |
| `PUBSUB` | Connection enters pub/sub mode after execution |
| `SCRIPT` | Lua script execution with atomicity requirements |

### Example Flag Combinations

| Command | Flags |
|---------|-------|
| GET | `READONLY \| FAST` |
| SET | `WRITE \| FAST` |
| MGET | `READONLY \| MULTI_KEY` |
| DEL | `WRITE \| MULTI_KEY` |
| BLPOP | `WRITE \| BLOCKING` |
| EVAL | `WRITE \| SCRIPT` |
| SUBSCRIBE | `PUBSUB` |

---

## Command Registry

Commands are registered at startup in a static hashmap:

```rust
lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Box<dyn Command>> = {
        let mut m = HashMap::new();
        m.insert("GET", Box::new(GetCommand));
        m.insert("SET", Box::new(SetCommand));
        m.insert("DEL", Box::new(DelCommand));
        // ... register all commands
        m
    };
}

fn dispatch(cmd: &ParsedCommand) -> Result<Response, Error> {
    let handler = COMMANDS.get(cmd.name.to_uppercase().as_str())
        .ok_or(Error::UnknownCommand)?;
    handler.execute(cmd)
}
```

---

## Type Handling Rules

FrogDB values are typed (string, list, set, sorted set, hash, stream). Commands interact with types in three distinct ways:

### Type-Checking Commands (Return WRONGTYPE)

Most commands expect a specific type and return an error if the key holds a different type:

```
-WRONGTYPE Operation against a key holding the wrong kind of value
```

| Command Category | Expected Type | Example |
|-----------------|---------------|---------|
| String commands | String | `GET`, `APPEND`, `INCR`, `GETRANGE` |
| List commands | List | `LPUSH`, `RPOP`, `LRANGE`, `LLEN` |
| Set commands | Set | `SADD`, `SMEMBERS`, `SISMEMBER` |
| Sorted set commands | Sorted Set | `ZADD`, `ZRANGE`, `ZSCORE` |
| Hash commands | Hash | `HSET`, `HGET`, `HGETALL` |

### Type-Overwriting Commands (Replace Regardless of Type)

These commands unconditionally overwrite the key, changing its type if necessary:

| Command | Behavior |
|---------|----------|
| `SET` | Overwrites any existing value with a string |
| `GETSET` | Returns old value (any type), replaces with string |
| `SETEX`, `SETNX`, `PSETEX` | Same as SET (type overwrite) |

> **Warning:** `SET` on a non-string key will silently replace it. This is Redis-compatible
> behavior but can lead to data loss if used carelessly.

**Example:**
```
ZADD myzset 1 "member"
(integer) 1
TYPE myzset
zset
SET myzset "now a string"  # Overwrites the sorted set!
OK
TYPE myzset
string
```

### Type-Agnostic Commands (Work on Any Type)

These commands operate on keys regardless of their type:

| Command | Behavior |
|---------|----------|
| `DEL` | Delete key of any type |
| `EXISTS` | Check existence of any type |
| `TYPE` | Return type name |
| `EXPIRE`, `TTL`, `PERSIST` | Manage expiration of any type |
| `RENAME`, `RENAMENX` | Rename key of any type |
| `OBJECT` | Inspect any type |
| `DUMP`, `RESTORE` | Serialize/deserialize any type |

### Implementation

```rust
fn check_type<T: ExpectedType>(value: &FrogValue) -> Result<&T, CommandError> {
    match value.as_type::<T>() {
        Some(v) => Ok(v),
        None => Err(CommandError::WrongType),
    }
}
```

Type-overwriting commands skip this check entirely. Type-agnostic commands don't call it.

---

## End-to-End Client Flow

This section documents the complete client lifecycle from connection to disconnection, showing how all components integrate.

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Client Lifecycle                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   Client                    Acceptor                    Shard Workers        │
│     │                          │                             │               │
│     │───TCP Connect───────────▶│                             │               │
│     │                          │                             │               │
│     │                          │──Socket + Assignment───────▶│ (Shard N)     │
│     │                          │   (via new_conn channel)    │               │
│     │                          │                             │               │
│     │◀─────────────────────────────Connection Ready──────────│               │
│     │                                                        │               │
│     │═══════════════════ Command Loop ═══════════════════════│               │
│     │                                                        │               │
│     │───RESP Command──────────────────────────────────────▶ │               │
│     │                                                        │──Route───▶    │
│     │                                                        │  (if remote)  │
│     │◀──RESP Response─────────────────────────────────────── │◀─Response──   │
│     │                                                        │               │
│     │═══════════════════════════════════════════════════════│               │
│     │                                                        │               │
│     │───QUIT / Disconnect────────────────────────────────▶  │               │
│     │                                                        │──Cleanup──▶   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Phase 1: Connection Acceptance

The Acceptor is a dedicated Tokio task that accepts TCP connections and distributes them to shard workers.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Acceptor Task                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  loop {                                                         │
│      // 1. Accept TCP connection                                │
│      let (socket, addr) = listener.accept().await?;            │
│                                                                  │
│      // 2. Check connection limit (before TLS)                  │
│      if connection_count.load() >= config.maxclients {         │
│          socket.shutdown();                                     │
│          metrics.connections_rejected.inc();                    │
│          continue;                                              │
│      }                                                          │
│                                                                  │
│      // 3. TLS handshake (if enabled)                          │
│      let socket = if tls_enabled {                             │
│          tls_acceptor.accept(socket).await?                    │
│      } else { socket };                                         │
│                                                                  │
│      // 4. Select target shard via round-robin                 │
│      let shard_id = assigner.assign(&addr);                    │
│                                                                  │
│      // 5. Generate connection ID                               │
│      let conn_id = NEXT_CONN_ID.fetch_add(1, SeqCst);          │
│                                                                  │
│      // 6. Send socket to target shard's new_conn channel      │
│      shard_channels[shard_id].new_conn_tx.send(NewConnection { │
│          socket,                                                │
│          addr,                                                  │
│          conn_id,                                               │
│      }).await?;                                                 │
│                                                                  │
│      connection_count.fetch_add(1, SeqCst);                    │
│  }                                                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- Connection ID is assigned by Acceptor (global monotonic counter)
- Socket is transferred via channel, not spawned directly
- maxclients check happens before expensive TLS handshake

---

### Phase 2: Connection Initialization

When a shard worker receives a new connection, it spawns a connection handler task.

```rust
/// Message sent from Acceptor to Shard Worker
pub struct NewConnection {
    pub socket: TcpStream,          // Or TlsStream<TcpStream>
    pub addr: SocketAddr,
    pub conn_id: u64,
}

/// Shard worker's connection acceptance
impl ShardWorker {
    async fn handle_new_connection(&self, new_conn: NewConnection) {
        // Create connection state
        let conn_state = ConnectionState {
            id: new_conn.conn_id,
            addr: new_conn.addr,
            name: None,
            created_at: Instant::now(),
            auth: AuthState::default(),          // Unauthenticated or default user
            protocol_version: ProtocolVersion::Resp2,
            tx_queue: None,
            watches: HashMap::new(),
            subscriptions: HashSet::new(),
            patterns: HashSet::new(),
            pubsub_mode: false,
            blocked: None,
        };

        // Wrap socket with RESP2 codec
        let framed = Framed::new(new_conn.socket, Resp2::default());

        // Spawn connection handler task (stays on this shard's thread)
        let shard_senders = self.shard_senders.clone();
        let local_store = self.store.clone();

        tokio::spawn(async move {
            connection_loop(framed, conn_state, shard_senders, local_store).await
        });
    }
}
```

**Key Points:**
- ConnectionState is created by the shard worker, not Acceptor
- Each connection becomes a Tokio task on its assigned shard
- Connection holds references to all shard senders for routing

---

### Phase 3: Command Loop

The connection handler runs a loop processing commands until disconnect.

```rust
async fn connection_loop(
    mut framed: Framed<TcpStream, Resp2>,
    mut state: ConnectionState,
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    local_shard_id: usize,
    local_store: Arc<RwLock<Store>>,
) {
    loop {
        // 1. Read next frame(s) from client
        //    Codec handles partial reads and pipelining
        let frame = match framed.next().await {
            Some(Ok(frame)) => frame,
            Some(Err(e)) => {
                // Protocol error - send error response, continue
                let _ = framed.send(error_frame(&e)).await;
                continue;
            }
            None => break, // Client disconnected
        };

        // 2. Parse frame into command
        let cmd = match ParsedCommand::try_from(frame) {
            Ok(cmd) => cmd,
            Err(e) => {
                let _ = framed.send(error_frame(&e)).await;
                continue;
            }
        };

        // 3. Check authentication (if required)
        if !state.auth.has_permission(&cmd) {
            let _ = framed.send(noauth_error()).await;
            continue;
        }

        // 4. Lookup command handler
        let handler = match COMMANDS.get(cmd.name.to_ascii_uppercase().as_slice()) {
            Some(h) => h,
            None => {
                let _ = framed.send(unknown_command_error(&cmd.name)).await;
                continue;
            }
        };

        // 5. Validate arity
        if !handler.arity().check(cmd.args.len()) {
            let _ = framed.send(arity_error(handler.name())).await;
            continue;
        }

        // 6. Route and execute
        let response = route_and_execute(
            &cmd,
            handler,
            &mut state,
            &shard_senders,
            local_shard_id,
            &local_store,
        ).await;

        // 7. Encode and send response
        let frame = response.into_frame(state.protocol_version);
        if framed.send(frame).await.is_err() {
            break; // Write failed, client gone
        }

        // 8. Handle QUIT command
        if cmd.name.eq_ignore_ascii_case(b"QUIT") {
            break;
        }
    }

    // Connection ended - cleanup handled in Phase 5
    cleanup_connection(&state).await;
}
```

**Pipelining:** The Tokio codec automatically handles pipelining. Multiple commands in a single TCP read are parsed as separate frames and processed sequentially in the loop. Responses are buffered and flushed together.

---

### Phase 4: Shard Communication

Connections need to communicate with any shard for key operations. This is achieved through a channel topology.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Channel Topology                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Server Startup:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  // Create channels for each shard                                   │   │
│  │  let mut shard_senders = Vec::with_capacity(num_shards);            │   │
│  │  let mut shard_receivers = Vec::with_capacity(num_shards);          │   │
│  │                                                                      │   │
│  │  for _ in 0..num_shards {                                           │   │
│  │      let (tx, rx) = mpsc::channel(SHARD_CHANNEL_CAPACITY);          │   │
│  │      shard_senders.push(tx);                                        │   │
│  │      shard_receivers.push(rx);                                      │   │
│  │  }                                                                   │   │
│  │                                                                      │   │
│  │  let shard_senders = Arc::new(shard_senders); // Shared by all      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Runtime Topology:                                                          │
│                                                                              │
│     ┌──────────────┐                                                        │
│     │   Acceptor   │                                                        │
│     └──────┬───────┘                                                        │
│            │ new_conn channels (one per shard)                              │
│            ▼                                                                │
│     ┌──────────────────────────────────────────────────────────┐           │
│     │              Arc<Vec<Sender<ShardMessage>>>              │           │
│     │         (cloned to every connection handler)             │           │
│     └──────────────────────────────────────────────────────────┘           │
│            │                    │                    │                      │
│            ▼                    ▼                    ▼                      │
│     ┌────────────┐       ┌────────────┐       ┌────────────┐               │
│     │  Shard 0   │       │  Shard 1   │       │  Shard N   │               │
│     │ ┌────────┐ │       │ ┌────────┐ │       │ ┌────────┐ │               │
│     │ │Receiver│ │       │ │Receiver│ │       │ │Receiver│ │               │
│     │ └────────┘ │       │ └────────┘ │       │ └────────┘ │               │
│     │    │       │       │    │       │       │    │       │               │
│     │    ▼       │       │    ▼       │       │    ▼       │               │
│     │  Store     │       │  Store     │       │  Store     │               │
│     └────────────┘       └────────────┘       └────────────┘               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Route and Execute

```rust
async fn route_and_execute(
    cmd: &ParsedCommand,
    handler: &dyn Command,
    state: &mut ConnectionState,
    shard_senders: &Arc<Vec<mpsc::Sender<ShardMessage>>>,
    local_shard_id: usize,
    local_store: &Arc<RwLock<Store>>,
) -> Response {
    // Extract keys to determine routing
    let keys = handler.keys(&cmd.args);

    // Keyless commands (PING, INFO, etc.) execute locally
    if keys.is_empty() {
        return execute_local(cmd, handler, state, local_store).await;
    }

    // Single-key command
    if keys.len() == 1 {
        let target_shard = shard_for_key(keys[0], shard_senders.len());

        if target_shard == local_shard_id {
            // Local execution - direct store access
            execute_local(cmd, handler, state, local_store).await
        } else {
            // Remote execution - send via channel
            execute_remote(cmd, &shard_senders[target_shard]).await
        }
    } else {
        // Multi-key command - scatter-gather
        execute_scatter_gather(cmd, handler, &keys, shard_senders, local_shard_id, local_store).await
    }
}

async fn execute_remote(
    cmd: &ParsedCommand,
    sender: &mpsc::Sender<ShardMessage>,
) -> Response {
    // Create oneshot channel for response
    let (response_tx, response_rx) = oneshot::channel();

    // Send execution request
    let msg = ShardMessage::Execute {
        command: cmd.clone(),
        response_tx,
    };

    if sender.send(msg).await.is_err() {
        return Response::Error(Bytes::from_static(b"ERR shard unavailable"));
    }

    // Await response
    match response_rx.await {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => Response::Error(e.into()),
        Err(_) => Response::Error(Bytes::from_static(b"ERR shard dropped request")),
    }
}
```

#### Shard Worker Message Loop

Each shard worker processes messages from connections:

```rust
impl ShardWorker {
    async fn run(mut self) {
        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages (remote execution requests)
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, response_tx } => {
                            let result = self.execute_command(&command).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::ScatterRequest { request_id, keys, operation, response_tx } => {
                            let result = self.execute_scatter_part(&keys, &operation).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::Shutdown => break,
                    }
                }
            }
        }
    }
}
```

---

### Phase 5: Disconnection

When a connection ends (QUIT, client disconnect, or error), cleanup is performed.

```rust
async fn cleanup_connection(state: &ConnectionState) {
    // 1. Discard transaction queue if in MULTI
    // (state.tx_queue is dropped automatically)

    // 2. Unwatch all watched keys
    // (state.watches is dropped automatically)

    // 3. Unsubscribe from all pub/sub channels
    if !state.subscriptions.is_empty() || !state.patterns.is_empty() {
        // Send unsubscribe to pub/sub manager
        // (handled by PubSubManager, see PUBSUB.md)
    }

    // 4. Cancel any blocked state
    if let Some(blocked) = &state.blocked {
        // Send cancellation signal
        // (handled by BlockingManager, see BLOCKING.md)
    }

    // 5. Decrement connection counter
    CONNECTION_COUNT.fetch_sub(1, SeqCst);

    // 6. Log disconnection
    tracing::debug!(
        conn_id = state.id,
        addr = %state.addr,
        duration_secs = state.created_at.elapsed().as_secs(),
        "client disconnected"
    );
}
```

**QUIT vs Disconnect:**
- `QUIT` command: Client receives `+OK` response before connection closes
- TCP disconnect: Connection handler loop ends when `framed.next()` returns `None`
- Both paths converge at `cleanup_connection()`

---

### CommandContext Definition

The `CommandContext` struct provides commands access to execution state:

```rust
pub struct CommandContext<'a> {
    /// Current connection state (auth, transaction queue, etc.)
    pub conn_state: &'a mut ConnectionState,

    /// Local shard's data store
    pub store: &'a mut Store,

    /// For commands that need to reach other shards (rare)
    pub shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// This shard's ID
    pub shard_id: usize,

    /// Total number of shards (for key routing)
    pub num_shards: usize,

    /// Current timestamp (for TTL, metrics)
    pub now: Instant,

    /// WAL writer for persistence
    pub wal: &'a mut WalWriter,
}
```

**Note:** Most commands only need `conn_state` and `store`. The shard_senders field is used by multi-key commands that need scatter-gather coordination.

---

## Replication Integration

This section documents how replication hooks into the command execution flow. For full replication protocol details, see [CLUSTER.md](CLUSTER.md).

### Replication in Command Flow

```
Client Request
      │
      ▼
┌─────────────────┐
│  Parse Command  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Execute Command │──────────▶ Modify in-memory store
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  WAL Append     │──────────▶ Sequence number assigned here
└────────┬────────┘
         │
         ├─── Async mode ───▶ Response sent immediately to client
         │
         └─── Sync mode ────▶ Block until min_replicas_to_write ACK
                              │
                              ├─── ACK received ───▶ Response sent (+OK)
                              │
                              └─── Timeout ────────▶ Response sent (-NOREPL)
```

### Sequence Number Assignment

Sequence numbers are assigned at WAL append time, not at command execution time. This ensures:
- Monotonically increasing sequences for replication ordering
- Gaps are possible if batched writes fail partially
- Replicas can request resumption from any sequence number

```rust
impl WalWriter {
    /// Append operation to WAL, returning assigned sequence number
    fn append(&mut self, operation: &Operation) -> u64 {
        // RocksDB assigns sequence number during WriteBatch commit
        let batch = WriteBatch::new();
        batch.put(/* key, value encoding */);

        // Sequence assigned atomically by RocksDB
        let seq = self.db.write(batch)?;

        // Notify replication subsystem of new entry
        self.replication_notify.send(ReplicationEntry {
            sequence: seq,
            operation: operation.clone(),
        });

        seq
    }
}
```

### Synchronous Replication Blocking

When `min_replicas_to_write > 0`, write commands block after WAL append until sufficient replicas acknowledge:

```rust
async fn execute_with_sync_replication(
    cmd: &ParsedCommand,
    handler: &dyn Command,
    ctx: &mut CommandContext<'_>,
) -> Response {
    // 1. Execute command (modifies in-memory store)
    let result = handler.execute(ctx, &cmd.args);

    // 2. Append to WAL (assigns sequence number)
    let seq = ctx.wal.append(&result.operation)?;

    // 3. Check if sync replication required
    if ctx.config.min_replicas_to_write == 0 {
        return result.response; // Async mode - return immediately
    }

    // 4. Wait for replica acknowledgments
    let ack_future = ctx.replication_tracker.wait_for_acks(
        seq,
        ctx.config.min_replicas_to_write,
    );

    match tokio::time::timeout(
        ctx.config.replica_ack_timeout,
        ack_future,
    ).await {
        Ok(Ok(ack_count)) => {
            // Sufficient replicas acknowledged
            result.response
        }
        Ok(Err(_)) | Err(_) => {
            // Timeout or not enough replicas
            // Note: Write is ALREADY committed on primary
            Response::Error(Bytes::from_static(b"NOREPL Not enough replicas"))
        }
    }
}
```

**Important:** When `-NOREPL` is returned, the write has already succeeded on the primary and is in the WAL. The error indicates replication durability was not confirmed, not that the write failed. Clients must handle this appropriately (retry may cause duplicates unless using idempotent operations).

### Extended CommandContext for Replication

When replication is enabled, CommandContext includes additional state:

```rust
pub struct CommandContext<'a> {
    // ... existing fields ...

    /// WAL writer for persistence
    pub wal: &'a mut WalWriter,

    /// Replication configuration (None if standalone)
    pub replication: Option<&'a ReplicationConfig>,

    /// Tracker for synchronous replication acknowledgments
    pub replication_tracker: Option<&'a ReplicationTracker>,
}

pub struct ReplicationConfig {
    /// Minimum replicas that must ACK before responding (0 = async)
    pub min_replicas_to_write: u32,

    /// Timeout waiting for replica ACKs
    pub replica_ack_timeout: Duration,

    /// This node's role
    pub role: NodeRole,
}

pub enum NodeRole {
    Primary,
    Replica { primary_addr: SocketAddr },
    Standalone,
}
```

### Replica Command Handling

Replicas handle client commands differently based on type:

| Command Type | Replica Behavior |
|--------------|------------------|
| Read (`READONLY` flag) | Execute locally, may return stale data |
| Write (`WRITE` flag) | Return `-READONLY` error |
| Replication commands | Execute (PSYNC, REPLCONF, etc.) |
| Admin commands | Depends on command (INFO allowed, SHUTDOWN not) |

```rust
fn check_replica_permission(
    handler: &dyn Command,
    role: NodeRole,
) -> Result<(), CommandError> {
    match role {
        NodeRole::Primary | NodeRole::Standalone => Ok(()),
        NodeRole::Replica { .. } => {
            if handler.flags().contains(CommandFlags::WRITE) {
                Err(CommandError::ReadOnly)
            } else {
                Ok(())
            }
        }
    }
}
```

### Replication Metrics

Commands update replication-related metrics:

```rust
/// Metrics updated during command execution
pub struct ReplicationMetrics {
    /// Sequence number of last write
    pub last_write_seq: AtomicU64,

    /// Number of writes waiting for replica ACK
    pub pending_sync_writes: AtomicU64,

    /// Writes that failed due to replica timeout
    pub norepl_errors: Counter,

    /// Time spent waiting for replica ACKs
    pub replica_ack_wait_time: Histogram,
}
```

---

## References

- [CONNECTION.md](CONNECTION.md) - Connection state machine and client limits
- [CONCURRENCY.md](CONCURRENCY.md) - Thread architecture and message types
- [PROTOCOL.md](PROTOCOL.md) - RESP2/RESP3 codec integration
- [STORAGE.md](STORAGE.md) - Store trait and data access
