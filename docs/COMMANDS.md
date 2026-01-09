# FrogDB Command Reference

Index of command groups and cross-cutting execution concerns.

## Command Groups

| Group | Description | File |
|-------|-------------|------|
| Generic | Key management (DEL, EXISTS, EXPIRE, TTL, TYPE, SCAN) | [GENERIC.md](command-groups/GENERIC.md) |
| String | Binary-safe strings (GET, SET, INCR, APPEND) | [STRING.md](command-groups/STRING.md) |
| Sorted Set | Score-ordered sets (ZADD, ZRANGE, ZSCORE, ZRANK) | [SORTED_SET.md](command-groups/SORTED_SET.md) |
| Hash | Field-value maps (HGET, HSET, HGETALL) | *Future* |
| List | Ordered sequences (LPUSH, RPUSH, LRANGE) | *Future* |
| Set | Unique collections (SADD, SMEMBERS, SINTER) | *Future* |
| Stream | Append-only log (XADD, XREAD, XREADGROUP) | *Future* |
| Pub/Sub | Messaging (SUBSCRIBE, PUBLISH) | *Future* |
| Scripting | Lua scripts (EVAL, EVALSHA) | *Future* |
| Connection | Client management (AUTH, PING, QUIT) | *Future* |
| Server | Administration (INFO, CONFIG, DEBUG) | *Future* |
| Cluster | Distribution (CLUSTER NODES, CLUSTER SLOTS) | *Future* |

---

## Transactions

### MULTI/EXEC Flow

```
Client                     Server (Shard)
   │                            │
   │──── MULTI ────────────────▶│  Enter queuing mode
   │◀─── +OK ──────────────────│
   │                            │
   │──── SET foo bar ──────────▶│  Queue command
   │◀─── +QUEUED ──────────────│
   │                            │
   │──── INCR counter ─────────▶│  Queue command
   │◀─── +QUEUED ──────────────│
   │                            │
   │──── EXEC ─────────────────▶│  Execute all atomically
   │◀─── *2 ───────────────────│  Array of results
       $2 OK
       :1
```

### Connection State

```rust
struct ConnectionState {
    /// Transaction queue (None = not in transaction)
    tx_queue: Option<Vec<ParsedCommand>>,
    /// Watched keys for optimistic locking
    watches: HashMap<Bytes, u64>,  // key -> version
}
```

### WATCH Implementation

```rust
// On WATCH
fn watch(conn: &mut Connection, keys: &[Bytes]) {
    for key in keys {
        let version = shard.key_version(key);
        conn.watches.insert(key.clone(), version);
    }
}

// On any write to key
fn on_key_modified(shard: &mut Shard, key: &Bytes) {
    shard.increment_version(key);
}

// On EXEC
fn exec(conn: &mut Connection) -> Result<Vec<Response>, Error> {
    // Check watches
    for (key, watched_version) in &conn.watches {
        let current_version = shard.key_version(key);
        if current_version != *watched_version {
            conn.watches.clear();
            return Ok(vec![]); // Abort - return nil multi-bulk
        }
    }

    // Execute queued commands
    let results = conn.tx_queue.take()
        .map(|cmds| cmds.iter().map(|c| execute(c)).collect())
        .unwrap_or_default();

    conn.watches.clear();
    Ok(results)
}
```

### Cross-Shard Transactions

FrogDB requires all keys in a transaction to be on the same shard:

```rust
fn validate_transaction(cmds: &[ParsedCommand]) -> Result<(), Error> {
    let shards: HashSet<_> = cmds.iter()
        .flat_map(|c| c.keys())
        .map(|k| shard_for_key(k))
        .collect();

    if shards.len() > 1 {
        return Err(Error::CrossShardTransaction);
    }
    Ok(())
}
```

**Recommendation:** Use hash tags to colocate transaction keys:
```
MULTI
SET {user:123}:name "Alice"
INCR {user:123}:visits
EXEC
```

### Transaction Durability

**WAL Behavior:**
- Commands within MULTI are buffered in memory
- On EXEC: Entire transaction written as single RocksDB WriteBatch
- WriteBatch is atomic - all or nothing

**Durability by Mode:**

| Mode | EXEC Behavior |
|------|---------------|
| `async` | WriteBatch queued, EXEC returns immediately |
| `periodic` | WriteBatch queued, EXEC returns immediately, fsync on next interval |
| `sync` | WriteBatch written + fsync, then EXEC returns |

**Failure Scenarios:**

| Failure Point | Outcome |
|---------------|---------|
| Crash before EXEC | Transaction never applied |
| Crash during EXEC (async) | Transaction may be lost |
| Crash during EXEC (sync) | Transaction either fully applied or not |
| Crash after EXEC returns (sync) | Transaction guaranteed durable |

**Partial Failure:**
- If any command in transaction fails validation, entire EXEC fails
- RocksDB WriteBatch ensures atomicity at storage level

See [CONSISTENCY.md](CONSISTENCY.md) for detailed consistency semantics.

---

## Pipelining

### Protocol-Level Batching

Clients send multiple commands without waiting for responses:

```
Client                     Server
   │                            │
   │──── SET a 1 ──────────────▶│
   │──── SET b 2 ──────────────▶│  Commands processed
   │──── GET a ────────────────▶│  in order received
   │                            │
   │◀─── +OK ──────────────────│
   │◀─── +OK ──────────────────│  Responses sent
   │◀─── $1 1 ─────────────────│  in order
```

### Server Handling

```rust
async fn handle_connection(conn: TcpStream) {
    let (reader, writer) = conn.split();
    let mut responses = VecDeque::new();

    loop {
        // Parse all available commands
        while let Some(cmd) = try_parse(&reader) {
            let result = execute(cmd).await;
            responses.push_back(result);
        }

        // Write all responses
        while let Some(response) = responses.pop_front() {
            write_response(&writer, response).await;
        }
    }
}
```

### Key Differences from Transactions

| Aspect | Pipelining | Transactions |
|--------|------------|--------------|
| Atomicity | No | Yes |
| Interleaving | Other clients may interleave | No interleaving |
| Server state | None | Queue maintained |
| Cross-shard | Works | Rejected |
| Performance | Best | Good |

---

## Command Implementation Notes

### Command Registry

```rust
lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Box<dyn Command>> = {
        let mut m = HashMap::new();
        m.insert("GET", Box::new(GetCommand));
        m.insert("SET", Box::new(SetCommand));
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

### Common Patterns

**Read Command:**
```rust
impl Command for GetCommand {
    fn execute(&self, ctx: &CommandContext, args: &[Bytes]) -> Result<Response> {
        let key = &args[0];
        match ctx.store.get(key)? {
            Some(FrogValue::String(s)) => Ok(Response::Bulk(s.data.clone())),
            Some(_) => Err(Error::WrongType),
            None => Ok(Response::Null),
        }
    }
}
```

**Write Command:**
```rust
impl Command for SetCommand {
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response> {
        let key = args[0].clone();
        let value = args[1].clone();

        // Parse options (EX, PX, NX, XX)
        let opts = parse_set_options(&args[2..])?;

        // Check NX/XX conditions
        if !check_conditions(ctx.store, &key, &opts) {
            return Ok(Response::Null);
        }

        // Set value
        ctx.store.set(key.clone(), FrogValue::String(FrogString { data: value }));

        // Set expiry if specified
        if let Some(ttl) = opts.ttl {
            ctx.store.set_expiry(&key, ttl);
        }

        Ok(Response::Ok)
    }
}
```

---

## Future Commands

### Cluster (Future)

See [CLUSTER.md](CLUSTER.md) for full clustering architecture.

**Topology Commands:**

| Command | Description |
|---------|-------------|
| `CLUSTER NODES` | Full cluster state in Redis format |
| `CLUSTER SLOTS` | Get slot→node mapping (array format) |
| `CLUSTER SHARDS` | Get slot→node mapping (dict format, newer) |
| `CLUSTER INFO` | Cluster status summary |
| `CLUSTER KEYSLOT <key>` | Get slot for key |
| `CLUSTER COUNTKEYSINSLOT <slot>` | Count keys in slot |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | Get keys for migration |

**Client Commands:**

| Command | Description |
|---------|-------------|
| `READONLY` | Enable reads from replica |
| `READWRITE` | Disable replica reads (default) |
| `ASKING` | Allow next command during slot import |

**Failover Commands:**

| Command | Description |
|---------|-------------|
| `CLUSTER FAILOVER [FORCE]` | Manual failover (replica only) |
| `CLUSTER REPLICATE <node-id>` | Make this node replica of another |

**Redirections:**

| Response | Meaning | Client Action |
|----------|---------|---------------|
| `-MOVED <slot> <host>:<port>` | Wrong node | Update slot map, retry |
| `-ASK <slot> <host>:<port>` | Slot migrating | Send ASKING, then retry (once) |

### Modules (Future)

| Command | Description |
|---------|-------------|
| MODULE LOAD | Load module |
| MODULE LIST | List modules |
| MODULE UNLOAD | Unload module |
