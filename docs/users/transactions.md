# Transactions

FrogDB supports MULTI/EXEC transactions with optimistic locking via WATCH, compatible with the Redis transaction model.

## MULTI/EXEC

Transactions provide atomic command execution with queuing:

1. **MULTI**: Start transaction, server enters queuing mode
2. **Commands**: Queued, server responds QUEUED
3. **EXEC**: Execute all queued commands atomically
4. **DISCARD**: Abort transaction, clear queue

### Command Flow

```
Client                     Server
   |                            |
   |---- MULTI ------------------>  Enter queuing mode
   |<--- +OK --------------------|
   |                            |
   |---- SET foo bar ------------>  Queue command
   |<--- +QUEUED ----------------|
   |                            |
   |---- INCR counter ----------->  Queue command
   |<--- +QUEUED ----------------|
   |                            |
   |---- EXEC ------------------>  Execute all atomically
   |<--- *2 --------------------|  Array of results
       $2 OK
       :1
```

### Transaction Guarantees

| Guarantee | Behavior |
|-----------|----------|
| **Execution Atomicity** | All commands execute without interleaving from other clients |
| **Isolation** | WATCH-based optimistic locking detects conflicts |
| **Durability** | Depends on persistence mode (see below) |
| **Rollback on Error** | No -- if one command fails, others still execute (Redis-compatible) |
| **Single-Slot Requirement** | All keys must hash to the same slot (use hash tags) |

### Durability

When EXEC returns successfully, the transaction has been executed but not necessarily persisted to disk. Durability depends on the configured persistence mode:

| Durability Mode | EXEC Behavior | Max Data Loss on Crash |
|-----------------|---------------|------------------------|
| `async` | Returns immediately | Unbounded (until next fsync) |
| `periodic` | Returns immediately, fsync on next interval | Up to `interval_ms` (default 1000ms) |
| `sync` | Fsync before returning | None |

See [Configuration](../operators/configuration.md) for persistence settings.

## Same-Slot Requirement

All keys in a transaction must hash to the same slot. When a command is queued that references a key in a different hash slot than previously queued keys, FrogDB returns a `-CROSSSLOT` error immediately (not at EXEC time).

Use hash tags to colocate transaction keys:

```
MULTI
SET {user:123}:name "Alice"
INCR {user:123}:visits
EXEC
```

In cluster mode, MULTI/EXEC is restricted to a single slot (single node). Cross-node transactions are not supported.

### Cross-Slot Transactions in Standalone Mode

When `allow_cross_slot_standalone = true` is configured in standalone mode, FrogDB supports atomic cross-shard MULTI/EXEC transactions. Locks are acquired in shard order to prevent deadlocks, and all commands execute without interleaving.

Cross-shard transactions provide execution atomicity (isolation), not failure atomicity (rollback). If a shard fails during execution, partial state may remain. This follows Redis's design philosophy.

**Partial Failure Errors:**

For cross-shard transactions, FrogDB provides detailed error information for recovery:

```
-ERR PARTIAL_FAILURE keys_written:[k1,k2] keys_failed:[k3] reason:<reason>
-ERR TIMEOUT keys_written:[k1] keys_unknown:[k2,k3] reason:scatter_gather_timeout
-ERR REJECTED lock_conflict txid:12345
```

## WATCH (Optimistic Locking)

WATCH provides optimistic concurrency control:

- `WATCH key [key...]`: Monitor keys for changes
- If any watched key is modified before EXEC, the transaction aborts (EXEC returns nil)
- `UNWATCH`: Clear all watches

```
WATCH mykey
GET mykey            # Returns "old"
MULTI
SET mykey "new"
EXEC                 # Returns nil if mykey was changed by another client
```

### What Triggers a WATCH Abort

Any write operation on a watched key causes the transaction to abort, including:

- SET, SETNX, SETEX and other string writes
- DEL (even if the key did not exist)
- EXPIRE, PERSIST (TTL changes)
- RENAME (both source and destination)
- INCR, DECR, APPEND
- Hash, List, Set, and Sorted Set modifications
- Key expiration by the background expiry task

Non-existent keys can be watched. If the key is created before EXEC, the transaction aborts.

### WATCH and Expired Keys

- Watching an expired key is equivalent to watching a non-existent key
- If a key expires during the WATCH period, the version changes and the transaction aborts
- If a key expires and is recreated, the transaction aborts

### Configuration

By default, FrogDB uses a global per-shard version counter for WATCH. This is simple and memory-efficient but can produce false-positive aborts when unrelated keys on the same shard are modified.

For high-contention workloads, enable per-key version tracking:

```toml
[transactions]
per_key_watch_versions = false  # default; set to true for fewer false positives
```

## Pipelining

Pipelining is a client-side optimization -- batching multiple commands in a single network round-trip. Unlike transactions, pipelined commands are not atomic and may interleave with commands from other clients.

```
Client                     Server
   |                            |
   |---- SET a 1 ---------------->
   |---- SET b 2 ---------------->  Commands processed
   |---- GET a ------------------>  in order received
   |                            |
   |<--- +OK --------------------|
   |<--- +OK --------------------|  Responses sent
   |<--- $1 1 -------------------|  in order
```

### Pipelining vs Transactions

| Aspect | Pipelining | Transactions |
|--------|------------|--------------|
| Atomicity | No | Yes |
| Interleaving | Other clients may interleave | No interleaving |
| Server state | None | Queue maintained |
| Cross-shard | Works | Rejected (unless standalone cross-slot enabled) |
| Performance | Best | Good |

Pipelining works automatically with any Redis client -- no server-side configuration needed.

## Cross-Slot Behavior Summary

| Operation Type | Cross-Slot | Behavior |
|----------------|------------|----------|
| MGET, MSET, DEL (multi-key) | Rejected | `-CROSSSLOT` error |
| MULTI/EXEC (transactions) | Rejected | `-CROSSSLOT` error |
| Lua EVAL (scripts) | Rejected | `-CROSSSLOT` error |

Use hash tags `{...}` to colocate related keys:
```
MSET {user:1}name Alice {user:1}email alice@example.com  # OK - same slot
```

## Command Reference

| Command | Description |
|---------|-------------|
| MULTI | Start transaction |
| EXEC | Execute queued commands |
| DISCARD | Abort transaction |
| WATCH | Monitor keys for changes |
| UNWATCH | Clear all watches |
