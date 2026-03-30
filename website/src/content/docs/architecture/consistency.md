---
title: "Consistency Model"
description: "Consistency guarantees for single-node operation, clustered deployments, and transactions."
sidebar:
  order: 9
---
Consistency guarantees for single-node operation, clustered deployments, and transactions.

## Single-Node Guarantees

### Read-Your-Writes

A client sees its own writes on the **same connection to the same node**, subject to failover limitations.

| Scenario | Read-Your-Writes |
|----------|------------------|
| Same connection, no failover | **Guaranteed** |
| Reconnect to same node | **Guaranteed** (data persisted) |
| Failover to replica (async) | **NOT guaranteed** - may lose unreplicated writes |
| Failover to replica (sync) | **Guaranteed** if write was acknowledged |

**Mitigation:** For critical writes that must survive failover, use `min_replicas_to_write = 1`.

### Monotonic Reads
Once a client reads a value, it will never see an older value for that key on the same connection.

### Total Order Per Key
All operations on a single key are totally ordered. Concurrent writes from different clients are serialized.

### Per-Shard Linearizability
Within a single internal shard, operations are linearizable.

### Cross-Slot Handling

Multi-key commands spanning multiple hash slots are rejected with a `CROSSSLOT` error before execution. This matches Redis Cluster behavior.

**Using Hash Tags:** Use hash tags `{tag}` to colocate keys on the same slot:
```
MSET {user:123}name Alice {user:123}email alice@example.com
```

### Cross-Slot Standalone Mode

In **standalone mode only**, FrogDB optionally supports atomic cross-shard operations via [VLL](/architecture/vll/):

| Mode | Cross-Shard Behavior | Atomicity |
|------|---------------------|-----------|
| `allow_cross_slot_standalone = false` (default) | `-CROSSSLOT` error | N/A |
| `allow_cross_slot_standalone = true` | VLL coordination | Atomic |
| Cluster mode (any setting) | `-CROSSSLOT` error | N/A |

---

## Durability Guarantees

| Mode | Write Acknowledged When | Data at Risk on Crash |
|------|------------------------|----------------------|
| `async` | Written to memory | All unflushed writes |
| `periodic` | Written to memory | Up to sync interval |
| `sync` | fsync completes | None |

---

## Cluster Consistency

### Replication Model

- **Asynchronous replication** by default
- Replicas receive WAL stream from primary
- Typical lag: milliseconds

### Eventual Consistency

Acknowledged writes eventually appear on all replicas. During failover, writes may be **permanently lost** (not just delayed):

| Scenario | Data Fate |
|----------|-----------|
| Write replicated before failover | **Preserved** on new primary |
| Write acknowledged but not replicated | **Lost** - bounded by replication lag |
| Write to old primary during split-brain | **Discarded** |

### Split-Brain Window

During failover, there is a window (up to `fencing_timeout_ms`, default 10s) where both old and new primary may accept writes. Old primary's divergent writes are **discarded** when it receives demotion topology.

**Reducing Split-Brain Risk:**
1. Lower `fencing_timeout_ms`
2. Use `min_replicas_to_write = 1`
3. Client-side epoch validation

### Guarantees NOT Provided
- **Linearizability**: Cross-node operations are not linearizable
- **Snapshot isolation**: No point-in-time consistency across keys
- **Causal consistency**: Causally related operations may be seen out of order by different clients

---

## Transaction Consistency

### MULTI/EXEC Atomicity

**Guarantees:**
- All commands execute or none execute
- Commands execute in order
- No interleaving with other clients' commands on same keys

**Limitations:**
- Keys must hash to same shard (use hash tags)
- Cross-shard transactions not supported

### Transaction Durability

Transaction written as single RocksDB WriteBatch (atomic at storage level).

| Durability Mode | Transaction Behavior |
|-----------------|---------------------|
| `async` | EXEC returns immediately, may lose on crash |
| `periodic` | EXEC returns immediately, durable at next sync |
| `sync` | EXEC blocks until fsync, then returns |

### WATCH Consistency

WATCH provides optimistic locking. EXEC fails if watched key modified by another client. Watched keys must be on the same internal shard as transaction keys.

---

## Read Consistency

### Read from Primary (Default)
- Strongly consistent, always sees latest committed writes

### Read from Replica
- May return older value than primary
- Staleness bounded by replication lag

---

## Ordering Guarantees

### Within Single Connection
- Operations execute in order sent
- Pipelining preserves order
- Responses return in order

### Across Connections
- No ordering guarantee between different clients

### Pub/Sub Message Ordering
- Messages delivered in publish order per channel
- No ordering across channels
- At-most-once delivery (messages may be lost on reconnect)
