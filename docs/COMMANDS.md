# FrogDB Command Reference

This document provides detailed command implementation notes, algorithms, and future features.

## Command Categories

| Category | Examples | Notes |
|----------|----------|-------|
| Generic | DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME | Key management |
| String | GET, SET, INCR, APPEND | Basic operations |
| Hash | HGET, HSET, HGETALL | Field-value maps |
| List | LPUSH, RPUSH, LRANGE | Ordered sequences |
| Set | SADD, SMEMBERS, SINTER | Unique collections |
| Sorted Set | ZADD, ZRANGE, ZSCORE | Score-ordered |
| Pub/Sub | SUBSCRIBE, PUBLISH | Messaging |
| Transactions | MULTI, EXEC, WATCH | Atomicity |
| Scripting | EVAL, EVALSHA | Lua scripts |
| Server | INFO, CONFIG, DEBUG | Administration |
| Connection | AUTH, PING, QUIT | Client management |
| Cluster | (Future) | Distribution |

---

## Key Iteration: SCAN

### Overview

SCAN provides cursor-based iteration over the keyspace without blocking. Unlike KEYS, it returns results incrementally and is safe for production use.

### Cursor Format (Shard-Aware)

FrogDB encodes shard information in the cursor for transparent cross-shard iteration:

```
┌─────────────────────────────────────────────────────────────┐
│                     64-bit Cursor                            │
├─────────────────────────┬───────────────────────────────────┤
│      Shard ID           │      Position within Shard        │
│      (bits 48-63)       │      (bits 0-47)                  │
└─────────────────────────┴───────────────────────────────────┘
```

**Bit allocation:**
- Bits 48-63 (16 bits): Shard ID (supports up to 65,535 shards)
- Bits 0-47 (48 bits): Position within shard (supports very large dictionaries)

### Algorithm

```rust
fn scan(cursor: u64, count: usize, pattern: Option<&str>) -> (u64, Vec<Bytes>) {
    let shard_id = (cursor >> 48) as u16;
    let position = cursor & 0x0000_FFFF_FFFF_FFFF;

    // Determine target shard
    let target_shard = shard_id as usize;
    if target_shard >= num_shards {
        return (0, vec![]); // Invalid cursor
    }

    // Execute SCAN on target shard
    let (new_position, keys) = shards[target_shard].scan_local(position, count, pattern);

    // Determine next cursor
    let next_cursor = if new_position == 0 {
        // Shard exhausted, move to next
        let next_shard = target_shard + 1;
        if next_shard >= num_shards {
            0 // All shards exhausted
        } else {
            (next_shard as u64) << 48 // Start of next shard
        }
    } else {
        // Continue in same shard
        ((target_shard as u64) << 48) | new_position
    };

    (next_cursor, keys)
}
```

### Local Shard SCAN

Each shard implements dictionary iteration using the hash table cursor technique:

```rust
fn scan_local(position: u64, count: usize, pattern: Option<&str>) -> (u64, Vec<Bytes>) {
    let mut keys = Vec::new();
    let mut cursor = position;

    // Iterate hash buckets
    while keys.len() < count {
        let bucket = cursor % table_size;
        for key in bucket_entries(bucket) {
            if pattern.map_or(true, |p| matches(key, p)) {
                keys.push(key.clone());
            }
        }
        cursor = next_cursor(cursor);
        if cursor == 0 {
            break; // Wrapped around
        }
    }

    (cursor, keys)
}

// Reverse bits for cursor advancement (handles table resizing)
fn next_cursor(cursor: u64) -> u64 {
    reverse_bits(reverse_bits(cursor) + 1)
}
```

### Properties

| Property | Guarantee |
|----------|-----------|
| Blocking | No - returns incrementally |
| Completeness | All keys present for entire scan are returned |
| Duplicates | May return duplicates |
| Missing | Keys added/removed during scan may be missed |
| State | Stateless on server - cursor encodes all state |

### Commands

| Command | Description |
|---------|-------------|
| SCAN cursor [MATCH pattern] [COUNT hint] [TYPE type] | Iterate keys |
| SSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Set members |
| HSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Hash fields |
| ZSCAN key cursor [MATCH pattern] [COUNT hint] | Iterate Sorted Set members |

### KEYS Command

**Warning:** KEYS blocks the server while matching. Avoid in production.

```
KEYS pattern
```

Implementation: Scatter to all shards, gather matching keys, return combined result.

---

## Key Eviction

See [EVICTION.md](EVICTION.md) for eviction policies, LRU/LFU implementation, and memory thresholds.

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

| Command | Description |
|---------|-------------|
| CLUSTER NODES | List cluster nodes |
| CLUSTER SLOTS | Get slot assignments |
| CLUSTER KEYSLOT | Get slot for key |
| CLUSTER REPLICATE | Set replication |

### Modules (Future)

| Command | Description |
|---------|-------------|
| MODULE LOAD | Load module |
| MODULE LIST | List modules |
| MODULE UNLOAD | Unload module |
