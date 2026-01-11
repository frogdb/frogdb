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

## Type Conversion and Errors

FrogDB is strictly typed. Attempting wrong-type operations returns:

```
-WRONGTYPE Operation against a key holding the wrong kind of value
```

**Type checking behavior:**

| Scenario | Result |
|----------|--------|
| GET on string | Value returned |
| GET on sorted set | WRONGTYPE error |
| ZADD on string | WRONGTYPE error |
| SET on existing sorted set | Overwrites (type changes to string) |
| DEL | Works on any type |
| TYPE | Returns type name |
| EXISTS | Works on any type |

**Implementation:**

```rust
fn check_type<T: ExpectedType>(value: &FrogValue) -> Result<&T, CommandError> {
    match value.as_type::<T>() {
        Some(v) => Ok(v),
        None => Err(CommandError::WrongType),
    }
}
```

Commands that work on any type (DEL, EXISTS, TYPE, EXPIRE, TTL, RENAME, etc.) skip type checking and operate on the key directly.

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

## References

- [CONNECTION.md](CONNECTION.md) - Connection state machine and client limits
- [CONCURRENCY.md](CONCURRENCY.md) - Thread architecture and message types
- [PROTOCOL.md](PROTOCOL.md) - RESP2/RESP3 codec integration
- [STORAGE.md](STORAGE.md) - Store trait and data access
