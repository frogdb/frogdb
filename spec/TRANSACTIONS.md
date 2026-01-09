# FrogDB Transactions

This document details transaction execution (MULTI/EXEC), optimistic locking (WATCH), and pipelining in FrogDB.

## MULTI/EXEC Transactions

Transactions provide atomic command execution with queuing:

1. **MULTI**: Start transaction, server enters queuing mode
2. **Commands**: Queued, server responds QUEUED
3. **EXEC**: Execute all queued commands atomically
4. **DISCARD**: Abort transaction, clear queue

### Command Flow

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

### Key Semantics

| Property | Guarantee |
|----------|-----------|
| **Atomic** | All commands execute without interleaving |
| **NOT rollback** | If one command fails, others still execute |
| **Single-shard** | All keys must hash to same shard |

---

## Connection State

Transaction state is tracked per-connection:

```rust
struct ConnectionState {
    /// Transaction queue (None = not in transaction)
    tx_queue: Option<Vec<ParsedCommand>>,
    /// Watched keys for optimistic locking
    watches: HashMap<Bytes, u64>,  // key -> version
}
```

---

## Cross-Shard Transactions

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

**Cross-shard detection:** When a command is queued that references a key on a different shard than previously queued keys, FrogDB returns `-CROSSSHARD` error immediately (not at EXEC time). This provides early feedback rather than wasting round trips.

**Recommendation:** Use hash tags to colocate transaction keys:

```
MULTI
SET {user:123}:name "Alice"
INCR {user:123}:visits
EXEC
```

---

## WATCH (Optimistic Locking)

WATCH implements optimistic concurrency control:

- `WATCH key [key...]`: Monitor keys for changes
- If watched key modified before EXEC, transaction aborts (returns nil)
- `UNWATCH`: Clear all watches
- **Single-shard requirement**: Watched keys must be on the same shard as transaction keys

### Implementation

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

---

## Transaction Durability

### WAL Behavior

- Commands within MULTI are buffered in memory
- On EXEC: Entire transaction written as single RocksDB WriteBatch
- WriteBatch is atomic - all or nothing

### Durability by Mode

| Mode | EXEC Behavior |
|------|---------------|
| `async` | WriteBatch queued, EXEC returns immediately |
| `periodic` | WriteBatch queued, EXEC returns immediately, fsync on next interval |
| `sync` | WriteBatch written + fsync, then EXEC returns |

### Failure Scenarios

| Failure Point | Outcome |
|---------------|---------|
| Crash before EXEC | Transaction never applied |
| Crash during EXEC (async) | Transaction may be lost |
| Crash during EXEC (sync) | Transaction either fully applied or not |
| Crash after EXEC returns (sync) | Transaction guaranteed durable |

### Partial Failure

- If any command in transaction fails validation, entire EXEC fails
- RocksDB WriteBatch ensures atomicity at storage level

See [CONSISTENCY.md](CONSISTENCY.md) for detailed consistency semantics.

---

## Pipelining

Pipelining is a client-side optimization - batching commands in a single network round-trip.

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

### Pipelining Properties

- **No server state required**
- **Commands may interleave** with other clients
- **NOT atomic** - purely a performance optimization
- **Supported automatically** by RESP protocol
- **Works cross-shard** (unlike transactions)

---

## Pipelining vs Transactions

| Aspect | Pipelining | Transactions |
|--------|------------|--------------|
| Atomicity | No | Yes |
| Interleaving | Other clients may interleave | No interleaving |
| Server state | None | Queue maintained |
| Cross-shard | Works | Rejected |
| Performance | Best | Good |

---

## VLL Multi-Shard Operations

For multi-key commands like MSET/MGET that span shards, FrogDB uses VLL-inspired ordering (not transactions):

| Operation Type | Cross-Shard | Behavior |
|----------------|-------------|----------|
| **MGET, MSET, DEL** (multi-key) | Yes | VLL ordering, fail-all semantics, partial commits possible |
| **MULTI/EXEC** (transactions) | No | `-CROSSSHARD` error, all keys must be on same shard |
| **Lua EVAL** (scripts) | No | `-CROSSSLOT` error, all keys must be on same shard |

See [CONCURRENCY.md](CONCURRENCY.md) for VLL implementation details.

---

## Command Reference

| Command | Description |
|---------|-------------|
| MULTI | Start transaction |
| EXEC | Execute queued commands |
| DISCARD | Abort transaction |
| WATCH | Monitor keys for changes |
| UNWATCH | Clear all watches |

---

## References

- [CONCURRENCY.md](CONCURRENCY.md) - VLL multi-shard atomicity
- [CONSISTENCY.md](CONSISTENCY.md) - Durability guarantees
- [PERSISTENCE.md](PERSISTENCE.md) - WAL and WriteBatch details
- [CONNECTION.md](CONNECTION.md) - Connection state machine
