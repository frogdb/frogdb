# FrogDB Clustering

This document covers FrogDB's clustering architecture, including slot-based sharding, replication protocol, node management, failover, and abstractions for future implementation.

## Overview

FrogDB is designed for single-node operation initially, with clustering as a future capability. The architecture uses:

- **Orchestrated control plane** (DragonflyDB-style) rather than gossip
- **16384 hash slots** for Redis Cluster client compatibility
- **Full dataset replication** - replicas copy all data from primary
- **RocksDB WAL streaming** for incremental replication

---

## Architecture

### Terminology

| Term | Definition |
|------|------------|
| **Node** | A FrogDB server instance |
| **Internal Shard** | Thread-per-core partition within a node (N per node) |
| **Slot** | Hash slot 0-16383 (cluster distribution unit) |
| **Primary** | Node owning slots for writes |
| **Replica** | Node replicating from a primary |
| **Orchestrator** | External service managing cluster topology |

### Slot-Based Sharding

FrogDB uses 16384 hash slots for Redis Cluster compatibility:

```
┌─────────────────────────────────────────────────────────────────┐
│                      16384 Hash Slots                            │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   Slots 0-5460  │  Slots 5461-10922  │  Slots 10923-16383       │
│   (Node 1)      │  (Node 2)          │  (Node 3)                │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

**Slot Calculation:**
```
slot = CRC16(key) % 16384
```

**Hash Tags:** Keys containing `{tag}` use only the tag for hashing, ensuring colocation:
```
{user:123}:profile  → slot = CRC16("user:123") % 16384
{user:123}:sessions → slot = CRC16("user:123") % 16384  (same slot)
```

### Internal vs Cluster Sharding

FrogDB has two levels of sharding that are separate concepts:

```
┌─────────────────────────────────────────────────────────────────┐
│                       Cluster Level                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Node 1    │  │   Node 2    │  │   Node 3    │             │
│  │ Slots 0-5460│  │Slots 5461-  │  │Slots 10923- │             │
│  │             │  │   10922     │  │   16383     │             │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
└─────────┼────────────────┼────────────────┼─────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Node Level (Internal Shards)               │
│  Each node has N internal shards (threads):                      │
│                                                                  │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                   │
│  │Shard 0 │ │Shard 1 │ │Shard 2 │ │Shard N │                   │
│  │        │ │        │ │        │ │        │                   │
│  └────────┘ └────────┘ └────────┘ └────────┘                   │
│                                                                  │
│  internal_shard = hash(key) % num_internal_shards               │
└─────────────────────────────────────────────────────────────────┘
```

**Two-Level Routing:**
1. **Cluster routing:** `slot = CRC16(key) % 16384` → determines which node
2. **Internal routing:** `internal_shard = hash(key) % N` → determines which thread within node

---

## Node Roles

### Primary Node

- Owns one or more slot ranges
- Accepts writes for owned slots
- Streams changes to replicas via WAL
- Responds to client requests or sends MOVED redirect
- Participates in slot migration (source or target)

### Replica Node

- Full copy of primary's dataset (not slot-specific)
- Read-only by default (READONLY command enables reads)
- Candidates for failover promotion
- Uses PSYNC-style incremental replication
- Can serve stale reads to scale read throughput

---

## Control Plane

### Orchestrated Model

FrogDB uses an orchestrated control plane (DragonflyDB-style) rather than Redis gossip:

| Aspect | Orchestrated (FrogDB) | Gossip (Redis) |
|--------|----------------------|----------------|
| Topology source | External orchestrator | Node consensus |
| Node discovery | Orchestrator tells nodes | Nodes discover each other |
| Failure detection | Orchestrator monitors | Nodes vote on failures |
| Topology changes | Deterministic, immediate | Eventually consistent |
| Debugging | Explicit state | Derived from gossip |

**Benefits of Orchestrated:**
- Deterministic: No convergence delays
- Simpler: No gossip protocol implementation
- Debuggable: Topology is explicit
- Container-friendly: Stateless nodes

### Topology Push Flow

```
┌─────────────┐                        ┌─────────────────────────┐
│ Orchestrator │                        │     FrogDB Nodes        │
│             │                        │                         │
│ K8s Operator│                        │  ┌─────┐ ┌─────┐ ┌─────┐│
│ etcd        │  POST /admin/cluster   │  │ N1  │ │ N2  │ │ N3  ││
│ Consul      │ ────────────────────▶  │  └─────┘ └─────┘ └─────┘│
│ Custom      │    (topology JSON)     │                         │
└─────────────┘                        │  All nodes update local │
                                       │  slot→node mapping      │
                                       └─────────────────────────┘
```

### Topology Configuration

The orchestrator pushes configuration like:

```json
{
  "cluster_id": "frogdb-prod-1",
  "epoch": 42,
  "nodes": [
    {
      "id": "node-abc123",
      "address": "10.0.0.1:6379",
      "admin_address": "10.0.0.1:6380",
      "role": "primary",
      "slots": [{"start": 0, "end": 5460}]
    },
    {
      "id": "node-def456",
      "address": "10.0.0.2:6379",
      "admin_address": "10.0.0.2:6380",
      "role": "primary",
      "slots": [{"start": 5461, "end": 10922}]
    },
    {
      "id": "node-ghi789",
      "address": "10.0.0.3:6379",
      "admin_address": "10.0.0.3:6380",
      "role": "replica",
      "replicates": "node-abc123"
    }
  ]
}
```

### Node-to-Node Communication

Nodes do NOT gossip with each other. They only connect directly for:

| Purpose | Direction | Protocol |
|---------|-----------|----------|
| Replication | Replica → Primary | PSYNC + WAL stream |
| Slot Migration | Source → Target | MIGRATE protocol |

All topology knowledge comes from the orchestrator.

### Admin API

Each node exposes an admin API on a separate port:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/cluster` | POST | Receive topology update |
| `/admin/cluster` | GET | Return current topology |
| `/admin/health` | GET | Health check |
| `/admin/replication` | GET | Replication status |

---

## Replication Protocol

### Overview

FrogDB replication leverages RocksDB infrastructure:

- **Sequence numbers** instead of Redis replication offset
- **RocksDB checkpoints** for full synchronization
- **WAL tailing** via `GetUpdatesSince()` for incremental sync
- **Replication backlog** for reconnecting replicas

### Replication ID

Each dataset history has a unique identifier:

- **40-character hex string** (like Redis)
- **Changes on failover**: New primary generates new ID
- **Secondary ID**: Promoted replicas remember old primary's ID

```
Primary A (repl_id: abc123...)
    │
    ├── Replica B (tracking abc123...)
    │
    [Primary A fails, B promoted]
    │
Primary B (repl_id: def456..., secondary_id: abc123...)
```

This allows replicas of A to connect to B and continue incrementally.

### Full Synchronization

When incremental sync is not possible:

```
Primary                              Replica
   │                                    │
   │◀──── PSYNC ? -1 ──────────────────│  "I have no data"
   │                                    │
   │  Create RocksDB checkpoint         │
   │                                    │
   │───── FULLRESYNC <id> <seq> ──────▶│
   │                                    │
   │───── [checkpoint files] ─────────▶│  Transfer checkpoint
   │                                    │
   │                                    │  Load checkpoint
   │                                    │
   │◀──── PSYNC <id> <seq> ────────────│  Ready for incremental
   │                                    │
   │───── [WAL stream] ───────────────▶│  Continue streaming
```

### Partial Synchronization (PSYNC)

When replica reconnects with valid state:

```
Primary                              Replica
   │                                    │
   │◀──── PSYNC <repl_id> <seq> ───────│  "I have data up to seq"
   │                                    │
   │  Check: Is seq in WAL retention?   │
   │                                    │
   │───── CONTINUE ───────────────────▶│  "Yes, continuing"
   │                                    │
   │───── [WAL entries from seq] ─────▶│  Stream missing entries
```

### WAL Streaming

Primary streams RocksDB WAL entries to replicas:

```rust
// Conceptual flow
fn stream_to_replica(replica: &mut Connection, from_seq: u64) {
    let mut current_seq = from_seq;

    loop {
        // Get updates since last sequence
        let updates = rocksdb.get_updates_since(current_seq);

        for batch in updates {
            // Convert WriteBatch to replication format
            let repl_data = encode_for_replication(batch);

            // Send to replica
            replica.send(repl_data);

            current_seq = batch.sequence_number();
        }

        // Wait for more WAL entries
        wait_for_wal_update();
    }
}
```

### Replication Backlog

In-memory buffer of recent WAL entries for fast reconnection:

| Setting | Default | Description |
|---------|---------|-------------|
| `repl_backlog_size` | 1MB | Size of backlog buffer |
| `repl_backlog_ttl` | 3600s | How long to keep backlog after last replica disconnects |

---

## Client Protocol

### MOVED Redirection

When client sends command to wrong node:

```
Client                    Node A                    Node B
   │                         │                         │
   │── GET {user}:key ──────▶│                         │
   │                         │ "Not my slot"           │
   │◀─ -MOVED 1234 10.0.0.2:6379 ─│                    │
   │                         │                         │
   │── GET {user}:key ─────────────────────────────────▶│
   │◀─ $5\r\nvalue ────────────────────────────────────│
```

Client updates its slot mapping and retries.

### ASK Redirection

During slot migration (slot partially on both nodes):

```
Client                    Source                    Target
   │                         │                         │
   │── GET key ─────────────▶│                         │
   │                         │ "Key not here, migrating"│
   │◀─ -ASK 1234 10.0.0.3:6379 ─│                       │
   │                         │                         │
   │── ASKING ─────────────────────────────────────────▶│
   │◀─ +OK ────────────────────────────────────────────│
   │── GET key ────────────────────────────────────────▶│
   │◀─ $5\r\nvalue ────────────────────────────────────│
```

**Key difference from MOVED:** Client does NOT update slot mapping.

### READONLY Mode

Enable reads from replicas for scaling:

```
READONLY
+OK

GET mykey
$5
value    (possibly stale)
```

### CLUSTER Commands

| Command | Description |
|---------|-------------|
| `CLUSTER SLOTS` | Get slot→node mapping (array format) |
| `CLUSTER SHARDS` | Get slot→node mapping (newer dict format) |
| `CLUSTER NODES` | Full cluster state in Redis format |
| `CLUSTER INFO` | Cluster status summary |
| `CLUSTER KEYSLOT <key>` | Get slot for key |
| `CLUSTER COUNTKEYSINSLOT <slot>` | Count keys in slot |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | Get keys for migration |
| `CLUSTER FAILOVER [FORCE]` | Manual failover |
| `READONLY` | Enable reads from replica |
| `READWRITE` | Disable replica reads |

---

## Slot Migration

### Overview

Moving slot ownership between nodes with minimal client impact:

- **Atomic migration** (Valkey 9+ style) preferred
- **IMPORTING/MIGRATING states** during transition
- **ASK redirects** for keys not yet migrated

### Migration States

| State | Node | Behavior |
|-------|------|----------|
| MIGRATING | Source | Accept existing keys, ASK redirect for missing |
| IMPORTING | Target | Accept only with ASKING prefix |

### Migration Process

```
Orchestrator              Source (A)              Target (B)
     │                        │                       │
     │  1. Setup migration    │                       │
     │───────────────────────▶│                       │
     │────────────────────────────────────────────────▶│
     │                        │                       │
     │                        │ MIGRATING slot 1234   │
     │                        │                       │ IMPORTING slot 1234
     │                        │                       │
     │  2. Migrate keys       │                       │
     │                        │── MIGRATE keys ──────▶│
     │                        │◀─ OK ─────────────────│
     │                        │                       │
     │  3. Finalize           │                       │
     │───────────────────────▶│                       │
     │────────────────────────────────────────────────▶│
     │                        │                       │
     │                        │ Slot 1234 → B         │
     │                        │                       │ Slot 1234 owned
```

### Atomic Migration (Future)

Valkey 9.0-style server-side migration:

- Batched key migration
- No client ASK handling needed
- Rollback on failure
- Lower end-to-end latency

---

## Failover

### Automatic Failover

Orchestrator-driven failover process:

```
Orchestrator                 Primary (A)              Replica (B)
     │                           │                        │
     │  Health check fails       │ (down)                 │
     │                           │                        │
     │  1. Select best replica   │                        │
     │     (most up-to-date)     │                        │
     │                           │                        │
     │  2. Promote B to primary  │                        │
     │─────────────────────────────────────────────────▶  │
     │                           │                        │ Become primary
     │                           │                        │
     │  3. Update all nodes      │                        │
     │─────────────────────────────────────────────────▶  │
     │                           │                        │
     │  (Optional: Fence A when it returns)               │
```

### Manual Failover

Using CLUSTER FAILOVER command:

| Mode | Behavior |
|------|----------|
| Default (graceful) | Wait for replica to catch up, then switch |
| FORCE | Immediate promotion, may lose data |

### Split-Brain Prevention

- **Quorum required**: Orchestrator needs quorum to make decisions
- **Fencing**: Optional mechanism to prevent old primary from accepting writes
- **Odd voters**: Recommended odd number of orchestrator instances

---

## Persistence Integration

### Per-Node RocksDB

Each node has its own RocksDB instance:

```
Node 1                          Node 2
┌─────────────────────┐        ┌─────────────────────┐
│ RocksDB Instance    │        │ RocksDB Instance    │
│                     │        │                     │
│ ┌─────┐ ┌─────┐    │        │ ┌─────┐ ┌─────┐    │
│ │ CF0 │ │ CF1 │... │        │ │ CF0 │ │ CF1 │... │
│ └─────┘ └─────┘    │        │ └─────┘ └─────┘    │
│ (internal shards)   │        │ (internal shards)   │
│                     │        │                     │
│ ┌─────────────────┐│        │ ┌─────────────────┐│
│ │   Shared WAL    ││        │ │   Shared WAL    ││
│ └─────────────────┘│        │ └─────────────────┘│
└─────────────────────┘        └─────────────────────┘
```

### WAL Dual Purpose

The RocksDB WAL serves two purposes:

1. **Durability**: Local crash recovery
2. **Replication**: Source for replica synchronization

### WAL Retention for Replication

When clustering is enabled, WAL files are retained beyond normal durability needs:

| Setting | Default | Description |
|---------|---------|-------------|
| `wal_retention_size` | 100MB | Keep this much WAL for replicas |
| `wal_retention_time` | 3600s | Keep WAL files for this duration |

This allows replicas that fall behind to catch up without full resync.

### Snapshot Coordination

- Snapshots are per-node, not cluster-wide
- Replica snapshots are independent of primary
- Recovery: Load local snapshot, apply WAL, then catch up from primary

---

## Abstractions Needed Now

These abstractions should be designed into the single-node implementation to avoid future refactoring.

### Core Types

| Type | Description |
|------|-------------|
| **NodeId** | 40-character hex string identifying a node |
| **SlotId** | u16 in range 0-16383 |
| **SlotRange** | Contiguous range of slots (start, end) |
| **ReplicationId** | 40-character hex string for dataset history |
| **SequenceNumber** | u64 WAL sequence number |

### Key Abstractions

| Abstraction | Purpose |
|-------------|---------|
| **ClusterTopology** | Maps slots to nodes, answers "where does key X go?" |
| **ReplicationStream** | Abstracts WAL tailing and network streaming |
| **NodeTransport** | Inter-node communication layer |
| **AdminApi** | Receives topology updates from orchestrator |

### Message Types

| Message | Purpose |
|---------|---------|
| **TopologyUpdate** | Orchestrator → node configuration push |
| **ReplicationRequest** | PSYNC handshake |
| **ReplicationData** | WAL entries being streamed |
| **MigrationStart** | Begin slot migration |
| **MigrationData** | Keys being migrated |
| **MigrationEnd** | Finalize slot migration |

### Configuration Concepts

| Setting | Description |
|---------|-------------|
| `cluster_enabled` | Enable cluster mode |
| `admin_port` | Port for orchestrator communication |
| `node_id` | This node's identifier |
| `repl_backlog_size` | Replication backlog buffer size |
| `wal_retention_*` | WAL retention for replication |

---

## Metrics

### Cluster Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_cluster_known_nodes` | Gauge | Number of nodes in cluster |
| `frogdb_cluster_slots_assigned` | Gauge | Slots assigned to this node |
| `frogdb_cluster_slots_ok` | Gauge | Slots in healthy state |
| `frogdb_cluster_state` | Gauge | 0=fail, 1=ok |
| `frogdb_cluster_messages_sent` | Counter | Inter-node messages sent |
| `frogdb_cluster_messages_received` | Counter | Inter-node messages received |

### Replication Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_replication_offset` | Gauge | Current replication offset (seq number) |
| `frogdb_replication_lag_seconds` | Gauge | Lag behind primary |
| `frogdb_connected_replicas` | Gauge | Number of connected replicas (primary only) |
| `frogdb_sync_full_count` | Counter | Full syncs performed |
| `frogdb_sync_partial_ok` | Counter | Successful partial syncs |
| `frogdb_sync_partial_err` | Counter | Failed partial syncs (triggered full) |

---

## Future Enhancements

| Feature | Description |
|---------|-------------|
| **Gossip Protocol Option** | Self-managing cluster without external orchestrator |
| **WAIT Command** | Synchronous replication with consistency guarantees |
| **Cross-Datacenter** | Multi-region replication with latency-aware routing |
| **Auto-Rebalancing** | Automatic slot redistribution on scale events |
| **Read Replicas** | Non-failover replicas for read scaling |

---

## References

- [Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Valkey Cluster Tutorial](https://valkey.io/topics/cluster-tutorial/)
- [Valkey Replication](https://valkey.io/topics/replication/)
- [Valkey Atomic Slot Migration](https://valkey.io/blog/atomic-slot-migration/)
- [DragonflyDB Cluster Mode](https://www.dragonflydb.io/docs/managing-dragonfly/cluster-mode)
- [RocksDB Replication Helpers](https://github.com/facebook/rocksdb/wiki/Replication-Helpers)
- [Pinterest Rocksplicator](https://github.com/pinterest/rocksplicator)
