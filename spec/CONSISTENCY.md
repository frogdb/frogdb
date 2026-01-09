# FrogDB Consistency Model

This document defines FrogDB's consistency guarantees for single-node operation, clustered deployments, and transactions.

## Single-Node Guarantees

These guarantees apply to all deployments:

### Read-Your-Writes

A client sees its own writes on the **same connection to the same node**, subject to failover limitations.
After a successful write, subsequent reads from the same connection return the written value.

**Important:** This guarantee does NOT survive failover in async replication mode (the default).

| Scenario | Read-Your-Writes |
|----------|------------------|
| Same connection, no failover | **Guaranteed** |
| Reconnect to same node | **Guaranteed** (data persisted) |
| Failover to replica (async) | **NOT guaranteed** - may lose unreplicated writes |
| Failover to replica (sync) | **Guaranteed** if write was acknowledged |

**Scope:** This guarantee is per-connection AND per-node:
- Connection reset: guarantee resets (fresh connection state)
- Failover occurs: treat new primary as fresh connection
- Writes not yet replicated: may be invisible on new primary

**Mitigation:** For critical writes that must survive failover, use `min_replicas_to_write = 1`.

### Monotonic Reads
Once a client reads a value, it will never see an older value for that key on the same connection.

### Total Order Per Key
All operations on a single key are totally ordered. Concurrent writes from different clients are serialized.

### Per-Shard Linearizability
Within a single internal shard, operations are linearizable.

### Cross-Slot Handling

Multi-key commands (MGET, MSET, DEL) that span multiple hash slots are rejected with a `CROSSSLOT` error:

```
> MSET key1 val1 key2 val2  # Different hash slots
-CROSSSLOT Keys in request don't hash to the same slot
```

**Rationale:** This matches Redis Cluster behavior and provides clear semantics:
- No partial commits - operation fails before execution
- Simpler implementation - no cross-shard coordination
- Predictable client experience - clients already handle this

**Using Hash Tags:** Use hash tags `{tag}` to colocate keys on the same slot:

```
> MSET {user:123}name Alice {user:123}email alice@example.com
+OK  # Both keys hash to same slot based on "user:123"
```

Only the substring between `{` and `}` is hashed, allowing related keys to be colocated.

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

**Normal Operation:** Replicas converge with primary within bounded lag:
- Acknowledged writes eventually appear on all replicas
- Lag is typically milliseconds (monitor `frogdb_replication_lag_seconds`)
- No guarantee of order across different keys on different nodes

**Exception - Split-Brain Data Loss:** During failover, writes may be **permanently lost**, not just delayed:

| Scenario | Data Fate |
|----------|-----------|
| Write replicated before failover | **Preserved** on new primary |
| Write acknowledged but not replicated | **Lost** - bounded by replication lag |
| Write to old primary during split-brain | **Discarded** - logged for manual recovery |

This is NOT eventual consistency in the traditional sense - divergent writes do not converge.

### Split-Brain Window

During failover, there is a window (up to `fencing_timeout_ms`, default 10s) where both old and new
primary may accept writes, creating **divergent histories**.

**Timeline:**
```
T=0:     Primary loses orchestrator connection
T=15s:   Orchestrator detects failure (node_timeout_ms)
T=20s:   Replica promoted (new primary)
T=30s:   Old primary receives demotion (fencing_timeout_ms)

Split-brain window: T=20s to T=30s (up to 10 seconds)
```

**Split-Brain Data Fate:**
- Old primary's divergent writes are **discarded** when it receives demotion topology
- Discarded writes logged to `data/split_brain_discarded.log` for manual recovery
- Clients that wrote to old primary during split-brain will see their writes disappear

**Reducing Split-Brain Risk:**
1. Lower `fencing_timeout_ms` (faster demotion, more false positives)
2. Use `min_replicas_to_write = 1` (old primary blocks without replica ack)
3. Client-side epoch validation (reject responses with stale epoch)

See [CLUSTER.md](CLUSTER.md#split-brain-prevention) for configuration details.

### Guarantees NOT Provided
- **Linearizability**: Cross-node operations are not linearizable
- **Snapshot isolation**: No point-in-time consistency across keys
- **Causal consistency**: Causally related operations may be seen out of order by different clients

### Cross-Slot Operations

| Operation | Behavior |
|-----------|----------|
| Single key | Linearizable within shard |
| MGET/MSET (same hash slot) | Atomic, linearizable |
| MGET/MSET (different hash slots) | `-CROSSSLOT` error - rejected |
| KEYS, SCAN | Eventually consistent snapshot |

**Hash Slot Calculation:** Keys are assigned to one of 16384 hash slots using CRC16.
See [CLUSTER.md](CLUSTER.md#hash-slots) for slot assignment details.

### Same-Slot Multi-Key Operations

When MGET/MSET operates on keys within the same hash slot:

```
MGET {user:1}name {user:1}email {user:1}age  # All hash to same slot
```

**Guarantees:**
- All keys read/written atomically within the shard
- Linearizable with other operations on those keys
- Point-in-time consistent snapshot

**Recommendation:** Always use hash tags to colocate related keys:
```
# User data - all keys share {user:123} tag
SET {user:123}name Alice
SET {user:123}email alice@example.com
MGET {user:123}name {user:123}email  # Atomic read
```

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
