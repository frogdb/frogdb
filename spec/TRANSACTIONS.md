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

### Transaction Guarantees Summary

FrogDB transactions provide guarantees across three orthogonal dimensions:

| Dimension | FrogDB Guarantee | Notes |
|-----------|------------------|-------|
| **Execution Atomicity** | Yes | All commands execute without interleaving from other clients |
| **Isolation** | Yes | WATCH-based optimistic locking detects conflicts |
| **Durability** | Configurable | Depends on persistence mode (see below) |
| **Rollback on Error** | No | If one command fails, others still execute (Redis-compatible) |
| **Single-Slot Requirement** | Yes | All keys must hash to same slot (use hash tags) |

> **Key Insight: Acknowledgment ≠ Durability**
>
> When EXEC returns successfully, the transaction has been *executed* but not necessarily *persisted*.
> - In `async` mode: Transaction is in memory and WAL queue, may be lost on crash
> - In `periodic` mode: Transaction persists within fsync interval (default 1s)
> - In `sync` mode: Transaction is fsync'd before acknowledgment - fully durable
>
> This matches Redis AOF behavior and is standard for in-memory databases.

**Durability by Configuration:**

| `durability_mode` | Acknowledgment Means | Max Data Loss on Crash |
|-------------------|---------------------|------------------------|
| `async` | In memory + WAL queued | Unbounded (until next fsync) |
| `periodic` | In memory + WAL queued | Up to `interval_ms` (default 1000ms) |
| `sync` | Persisted to disk | None (assuming disk doesn't lie) |

See [Transaction Durability](#transaction-durability) for failure scenarios.

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

## Cross-Slot Transactions

FrogDB requires all keys in a transaction to hash to the same **internal shard** (thread partition).
This is enforced via hash slot validation - keys with the same hash slot will always be on the same internal shard.

```rust
fn validate_transaction(cmds: &[ParsedCommand]) -> Result<(), Error> {
    let slots: HashSet<_> = cmds.iter()
        .flat_map(|c| c.keys())
        .map(|k| hash_slot(k))
        .collect();

    if slots.len() > 1 {
        return Err(Error::CrossSlot);
    }
    Ok(())
}
```

**Cross-slot detection:** When a command is queued that references a key in a different hash slot than previously queued keys, FrogDB returns `-CROSSSLOT` error immediately (not at EXEC time). This provides early feedback rather than wasting round trips.

**Cross-Shard Transactions (Standalone Mode):**

When `allow_cross_slot_standalone = true` is configured in standalone mode, FrogDB supports atomic cross-shard MULTI/EXEC transactions via [VLL coordination](VLL.md):

- Transaction commands are coordinated across multiple shards using continuation locks
- **Execution atomicity**: All commands execute without interleaving from other operations
- **No rollback**: If a command fails mid-transaction, prior commands' effects persist (matches Redis/DragonflyDB behavior)
- See [VLL.md - Continuation Locks](VLL.md#continuation-locks) for implementation details

> **Important:** Cross-shard transactions provide execution atomicity (isolation), not failure atomicity (rollback). If a shard fails during execution, partial state may remain. This follows Redis's design philosophy that "the utility of rollbacks would not outweigh the costs in terms of performance and additional complexity."

**Recommendation:** Use hash tags to colocate transaction keys for best performance:

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

### Key Version Implementation

This section specifies exactly how key versions are generated and managed for WATCH.

**Version Number Type:**

```rust
/// Key version for WATCH tracking
/// u64 provides 18 quintillion versions - overflow is not a practical concern
type KeyVersion = u64;
```

**Version Counter Scope:**

- **Per-shard**: Each shard maintains its own version counter
- **Not global**: Version numbers are NOT unique across shards (not needed for WATCH)
- **In-memory only**: Versions are NOT persisted (reset on restart)

```rust
pub struct Shard {
    /// Monotonically increasing version counter for this shard
    /// Increments on any key modification
    key_version_counter: AtomicU64,

    /// Per-key version tracking (optional optimization)
    /// If None, use global counter for all keys
    per_key_versions: Option<HashMap<Bytes, KeyVersion>>,
}
```

**Version Increment Triggers:**

| Operation | Version Incremented? | Notes |
|-----------|---------------------|-------|
| SET/SETNX/SETEX | Yes | Any value change |
| GET | No | Read-only |
| DEL | Yes | Even if key didn't exist |
| EXPIRE/PEXPIRE | Yes | TTL is a modification |
| PERSIST | Yes | Removing TTL is a modification |
| RENAME src dst | Yes (both keys) | src and dst versions increment |
| INCR/DECR/INCRBY | Yes | Value modification |
| APPEND | Yes | Value modification |
| HSET/HDEL/etc. | Yes | Any hash modification |
| LPUSH/RPUSH/etc. | Yes | Any list modification |
| SADD/SREM/etc. | Yes | Any set modification |
| ZADD/ZREM/etc. | Yes | Any sorted set modification |
| Key expiration | Yes | Background expiry triggers version bump |

**Non-Existent Key Handling:**

```rust
fn key_version(&self, key: &Bytes) -> KeyVersion {
    match self.store.get(key) {
        Some(entry) => entry.version,
        None => 0,  // Non-existent keys have version 0
    }
}

fn watch_check(&self, key: &Bytes, watched_version: KeyVersion) -> bool {
    let current = self.key_version(key);

    // Special case: key didn't exist at WATCH time and still doesn't
    if watched_version == 0 && current == 0 {
        return true; // No change
    }

    // Special case: key didn't exist but now does (was created)
    if watched_version == 0 && current > 0 {
        return false; // Changed
    }

    // Normal case: compare versions
    current == watched_version
}
```

**DEL Behavior:**

When a key is deleted:

```rust
fn delete_key(&mut self, key: &Bytes) -> bool {
    let existed = self.store.remove(key).is_some();

    // Always increment version, even if key didn't exist
    // This ensures WATCH on non-existent key detects if key was
    // created then deleted during the watch period
    self.increment_version(key);

    existed
}
```

**Version Overflow Handling:**

```rust
fn increment_version(&mut self, key: &Bytes) {
    // Use wrapping add for overflow
    // At 1 billion increments/second, overflow takes 584 years
    let new_version = self.key_version_counter.fetch_add(1, Ordering::SeqCst)
        .wrapping_add(1);

    if let Some(entry) = self.store.get_mut(key) {
        entry.version = new_version;
    }
}
```

**Persistence: NOT Persisted:**

Key versions are **intentionally not persisted**. On restart, all versions reset to 0.

**Rationale:**
- WATCH is for short-lived optimistic locking, not durable state
- Matches Redis behavior exactly
- Simplifies persistence (no version metadata in snapshots)
- All connections reset on restart anyway, so watched versions are meaningless

**Behavior After Restart:**

```
Before crash:
  WATCH key1  (version = 12345)
  SET key1 value
  ... server crashes ...

After restart:
  - key1 loaded from snapshot
  - key1 version = 0 (reset)
  - Any WATCH from previous session = invalid (connection gone)
  - New WATCHes start fresh
```

**WATCH on Expired Keys:**

```rust
fn key_version_with_expiry(&self, key: &Bytes) -> KeyVersion {
    match self.store.get(key) {
        Some(entry) if !entry.is_expired() => entry.version,
        Some(_) => 0,  // Expired = treat as non-existent
        None => 0,
    }
}
```

| Scenario | Behavior |
|----------|----------|
| WATCH on expired key | Version = 0 (as if non-existent) |
| Key expires during WATCH | Version changes (background expiry increments) |
| WATCH, key expires, key recreated | Version mismatch detected, transaction aborts |

**Per-Key vs Global Version Counter:**

| Approach | Memory | Precision | Use Case |
|----------|--------|-----------|----------|
| Global counter | O(1) | Low | Simple, most workloads |
| Per-key versions | O(n) | High | High-contention workloads |

FrogDB uses **global per-shard counter** by default:

- Simpler implementation
- Lower memory overhead
- False positives possible (unrelated key modified causes abort)
- Matches Redis behavior

**Configuration:**

```toml
[transactions]
# Use per-key version tracking (more memory, fewer false positive WATCH aborts)
per_key_watch_versions = false
```

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_watch_abort_total` | Total WATCH-triggered transaction aborts |
| `frogdb_watch_false_positive_total` | Aborts from unrelated key changes (if per-key disabled) |
| `frogdb_key_version_counter` | Current version counter value (per shard) |

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

### Atomicity and Failover

**Important:** Transaction atomicity guarantees apply to single-node operation only.
In cluster mode with asynchronous replication (default):

- A transaction acknowledged by the primary may be **lost** during failover
- Data loss bounded by replication lag at time of failure
- For stronger guarantees, use `min_replicas_to_write = 1` (higher latency)

| Replication Mode | Transaction Durability on Failover |
|------------------|-----------------------------------|
| Async (default)  | May lose up to `frogdb_replication_lag_seconds` of transactions |
| Sync (`min_replicas_to_write >= 1`) | Acknowledged transactions survive failover |

See [REPLICATION.md - Synchronous Replication](REPLICATION.md#synchronous-replication) for protocol details and [CLUSTER.md - Failover](CLUSTER.md#failover) for cluster failover behavior.

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

## Cross-Slot Behavior

FrogDB rejects multi-key operations that span different hash slots, matching Redis Cluster behavior:

| Operation Type | Cross-Slot | Behavior |
|----------------|------------|----------|
| **MGET, MSET, DEL** (multi-key) | Rejected | `-CROSSSLOT` error |
| **MULTI/EXEC** (transactions) | Rejected | `-CROSSSLOT` error |
| **Lua EVAL** (scripts) | Rejected | `-CROSSSLOT` error |

**Using Hash Tags:** Colocate related keys using hash tags:
```
MSET {user:1}name Alice {user:1}email alice@example.com  # OK - same slot
MSET user1 Alice user2 Bob                                # ERROR - different slots
```

See [CONSISTENCY.md](CONSISTENCY.md#cross-slot-handling) for details on hash tags.

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

- [CONSISTENCY.md](CONSISTENCY.md) - Consistency and durability guarantees
- [PERSISTENCE.md](PERSISTENCE.md) - WAL and WriteBatch details
- [CONNECTION.md](CONNECTION.md) - Connection state machine
- [CLUSTER.md](CLUSTER.md) - Hash slots and cluster behavior
