# FrogDB Consistency Model

This document defines FrogDB's consistency guarantees for single-node operation, clustered deployments, and transactions.

## Single-Node Guarantees

These guarantees apply to all deployments:

### Read-Your-Writes
A client always sees its own writes on the **same connection**. After a successful write,
subsequent reads from the same connection return the written value.

**Scope:** This guarantee is per-connection, not per-client. In cluster mode, if a client
reconnects to a different node or the connection is reset, previously written values may
not be immediately visible (due to replication lag or slot routing changes).

### Monotonic Reads
Once a client reads a value, it will never see an older value for that key on the same connection.

### Total Order Per Key
All operations on a single key are totally ordered. Concurrent writes from different clients are serialized.

### Per-Shard Linearizability
Within a single internal shard, operations are linearizable.

### Cross-Shard Atomicity (VLL)
Multi-shard operations (MGET, MSET, DEL with multiple keys) use VLL-style transaction ordering:
- Operations are serialized via global transaction IDs
- Execution order is deterministic across all shards
- Client receives all-or-nothing response (fail-all semantics)

**Important:** This provides serializable ordering, NOT transactional rollback. If a multi-shard
write partially succeeds before failure/timeout, committed portions persist. Use hash tags to
guarantee true atomicity for related keys.

---

## Durability Guarantees

Consistency is tied to the configured durability mode:

| Mode | Write Acknowledged When | Data at Risk on Crash |
|------|------------------------|----------------------|
| `async` | Written to memory | All unflushed writes |
| `periodic` | Written to memory | Up to sync interval |
| `sync` | fsync completes | None |

### Durability Mode Details

**Async Mode:**
- Fastest performance (~1-10 μs writes)
- WAL written but not fsynced
- Risk: Power failure loses all buffered writes
- Use case: Caching, ephemeral data

**Periodic Mode:**
```toml
[persistence]
durability_mode = { periodic = { interval_ms = 100, write_count = 1000 } }
```
- Balanced performance and safety
- fsync every N ms OR M writes (whichever comes first)
- Risk: Up to interval_ms of data loss
- Use case: Most production workloads

**Sync Mode:**
- Safest (~100-500 μs writes)
- Every write fsynced before acknowledgment
- Risk: None for acknowledged writes
- Use case: Financial, audit logs

---

## Cluster Consistency

> **Note:** Clustering is a future capability. This section describes the planned consistency model.

### Replication Model
- **Asynchronous replication** by default
- Replicas receive WAL stream from primary
- Typical lag: milliseconds (depends on network and write rate)

### Eventual Consistency
Replicas converge with primary within bounded lag:
- All acknowledged writes eventually appear on all replicas
- No guarantee of order across different keys on different nodes
- Monitor `frogdb_replication_lag_seconds` for lag

### Guarantees NOT Provided
- **Linearizability**: Cross-node operations are not linearizable
- **Snapshot isolation**: No point-in-time consistency across keys
- **Causal consistency**: Causally related operations may be seen out of order by different clients

### Cross-Shard Operations

| Operation | Consistency |
|-----------|-------------|
| Single key | Linearizable within shard |
| MGET/MSET (same hash tag) | Atomic, linearizable (same internal shard) |
| MGET/MSET (different shards) | Serializable via VLL, fail-all response |
| KEYS, SCAN | Eventually consistent snapshot |

**VLL Ordering:** Cross-shard operations execute in global transaction ID order on each shard.
See [CONCURRENCY.md](CONCURRENCY.md#transaction-ordering-vll) for implementation details.

---

## Failover Consistency

### Data Loss Window
During automatic failover:
1. Primary fails
2. Orchestrator promotes replica
3. Writes acknowledged by primary but not replicated are **lost**

**Loss bounded by:** `frogdb_replication_lag_seconds` at failure time

### Client Experience

| Phase | Behavior |
|-------|----------|
| Primary fails | Writes timeout or error |
| Failover in progress | `-CLUSTERDOWN` or connection errors |
| New primary elected | `-MOVED` redirects |
| Steady state | Normal operation |

### Reducing Data Loss

**Option 1: Synchronous Replication (Future)**
```toml
[cluster]
min_replicas_to_write = 1  # Require N replica acks
```
- Write waits for replica acknowledgment
- Higher latency, stronger durability
- If replicas unavailable, writes fail

**Option 2: Lower Sync Interval**
```toml
[persistence]
durability_mode = { periodic = { interval_ms = 10 } }
```
- More frequent fsync
- Reduces loss window
- Higher disk I/O

---

## Transaction Consistency

### MULTI/EXEC Atomicity

Transactions provide atomicity within a single shard:

```
MULTI
SET key1 value1
INCR key2
EXEC
```

**Guarantees:**
- All commands execute or none execute
- Commands execute in order
- No interleaving with other clients' commands on same keys

**Limitations:**
- Keys must hash to same shard (use hash tags: `{tag}key1`, `{tag}key2`)
- Cross-shard transactions not supported

### Transaction Durability

| Durability Mode | Transaction Behavior |
|-----------------|---------------------|
| `async` | EXEC returns immediately, may lose on crash |
| `periodic` | EXEC returns immediately, durable at next sync |
| `sync` | EXEC blocks until fsync, then returns |

Transaction written as single RocksDB WriteBatch (atomic at storage level).

### WATCH Consistency

WATCH provides optimistic locking:

```
WATCH key1
val = GET key1
MULTI
SET key1 (val + 1)
EXEC
```

- EXEC fails if watched key modified by another client
- No guarantee key wasn't modified and reverted
- Retry logic required in application
- **Same-shard requirement:** Watched keys must be on the same internal shard as transaction keys (use hash tags)

---

## Read Consistency

### Read from Primary (Default)
- Strongly consistent
- Always sees latest committed writes
- Higher latency in cluster (routing overhead)

### Read from Replica
Enable with `READONLY` command:
```
READONLY
GET key
```

**Stale read characteristics:**
- May return older value than primary
- Staleness bounded by replication lag
- Suitable for read scaling when staleness acceptable

---

## Ordering Guarantees

### Within Single Connection
- Operations execute in order sent
- Pipelining preserves order
- Responses return in order

### Across Connections
- No ordering guarantee between different clients
- Use application-level coordination if needed (locks, sequences)

### Pub/Sub Message Ordering
- Messages delivered in publish order per channel
- No ordering across channels
- At-most-once delivery (messages may be lost on reconnect)

---

## Recommendations

### For Highest Consistency
```toml
[persistence]
durability_mode = "sync"

[cluster]
min_replicas_to_write = 1  # When available
```
- Use hash tags for related keys
- Use WATCH for optimistic concurrency
- Avoid cross-shard operations

### For Highest Performance
```toml
[persistence]
durability_mode = "async"
```
- Accept potential data loss
- Use for caching/ephemeral data
- Replicas for read scaling

### For Balanced Production
```toml
[persistence]
durability_mode = { periodic = { interval_ms = 100, write_count = 1000 } }
```
- Bounded data loss (100ms max)
- Good throughput
- Monitor replication lag

---

## References

- [PERSISTENCE.md](PERSISTENCE.md) - Durability modes and WAL configuration
- [CLUSTER.md](CLUSTER.md) - Cluster architecture and replication
- [COMMANDS.md](COMMANDS.md) - Transaction commands (MULTI/EXEC/WATCH)
- [FAILURE_MODES.md](FAILURE_MODES.md) - Failure handling and recovery
