---
title: "Clustering Internals"
description: "Contributor-facing documentation for FrogDB's clustering architecture: slot routing internals, orchestrator protocol, Raft consensus, and failover algorithm."
sidebar:
  order: 8
---
Contributor-facing documentation for FrogDB's clustering architecture: slot routing internals, orchestrator protocol, Raft consensus, and failover algorithm.

For operator-facing setup and configuration, see [Operations: Clustering](/operations/clustering/).

---

## Architecture

FrogDB supports both single-node and cluster operation. The architecture uses:

- **Orchestrated control plane** rather than gossip
- **16384 hash slots** for Redis Cluster client compatibility
- **Full dataset replication** -- replicas copy all data from primary
- **RocksDB WAL streaming** for incremental replication

### Terminology

| Term | Definition |
|------|------------|
| **Node** | A FrogDB server instance |
| **Internal Shard** | Thread-per-core partition within a node (N per node) |
| **Slot** | Hash slot 0-16383 (cluster distribution unit) |
| **Primary** | Node owning slots for writes |
| **Replica** | Node replicating from a primary |
| **Orchestrator** | External service managing cluster topology |

### Two-Level Sharding

```
+---------------------------------------------------------+
|                       Cluster Level                      |
|  +-------------+  +-------------+  +-------------+      |
|  |   Node 1    |  |   Node 2    |  |   Node 3    |      |
|  | Slots 0-5460|  |Slots 5461-  |  |Slots 10923- |      |
|  |             |  |   10922     |  |   16383     |      |
|  +------+------+  +------+------+  +------+------+      |
+---------+----------------+----------------+--------------+
          |                |                |
+---------v----------------v----------------v--------------+
|                       Node Level (Internal Shards)       |
|  +--------+ +--------+ +--------+ +--------+            |
|  |Shard 0 | |Shard 1 | |Shard 2 | |Shard N |            |
|  +--------+ +--------+ +--------+ +--------+            |
|                                                          |
|  internal_shard = hash(key) % num_internal_shards        |
+---------------------------------------------------------+
```

**Routing:**
1. **Cluster routing:** `slot = CRC16(key) % 16384` -> determines which node
2. **Internal routing:** `internal_shard = hash(key) % N` -> determines which thread within node

### Hash Tag Full Colocation

Hash tags guarantee colocation at **both** levels:

```rust
fn route_key(key: &[u8]) -> (SlotId, InternalShardId) {
    let hash_input = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16(hash_input) % 16384;
    let shard = xxhash64(hash_input) % num_shards;
    (slot, shard)
}
```

---

## Orchestrated Control Plane

### Design Rationale

| Aspect | Orchestrated (FrogDB) | Gossip (Redis) |
|--------|----------------------|----------------|
| Topology source | External orchestrator | Node consensus |
| Node discovery | Orchestrator tells nodes | Nodes discover each other |
| Failure detection | Orchestrator monitors | Nodes vote on failures |
| Topology changes | Deterministic, immediate | Eventually consistent |
| Debugging | Explicit state | Derived from gossip |

**Benefits:** Deterministic (no convergence delays), simpler (no gossip protocol), debuggable (topology is explicit), container-friendly (stateless nodes).

### Topology Push Flow

The orchestrator pushes topology to all nodes via admin API:

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
    }
  ]
}
```

### Epoch System

The epoch is a monotonic counter incremented on any topology change. Nodes detect staleness by comparing their local epoch against incoming topology updates.

| Detection Method | Description |
|-----------------|-------------|
| Orchestrator push | Receives topology with higher epoch |
| Client redirect | Receives `-MOVED` from another node |
| Replica sync | Primary sends epoch in replication handshake |
| Periodic poll | Node polls orchestrator every `topology-refresh-interval-ms` |

---

## Slot Ownership Validation

Before executing a command, the node validates slot ownership:

```rust
enum SlotValidationResult {
    NoKeys,                                          // Keyless command
    Owned,                                           // Execute locally
    CrossSlot,                                       // Multi-slot error
    Redirect { slot: SlotId, owner: NodeId, owner_addr: SocketAddr },
    Migrating { slot: SlotId, target: NodeId },     // We're source
    Importing { slot: SlotId, source: NodeId },     // We're target
}
```

| Result | Command Action |
|--------|----------------|
| `NoKeys` | Execute locally |
| `Owned` | Execute locally |
| `CrossSlot` | Return `-CROSSSLOT` |
| `Redirect` | Return `-MOVED <slot> <host>:<port>` |
| `Migrating` | Execute if key exists locally, else `-ASK <slot> <target>` |
| `Importing` | Execute only if `ASKING` flag set, else `-MOVED` |

**Validation timing in pipeline:**
Parse -> Lookup -> Arity -> Extract keys -> CROSSSLOT check -> Slot ownership -> Execute

---

## Node Lifecycle

### Node Addition

1. Node starts standalone, registers with orchestrator
2. Orchestrator pushes initial topology
3. Node may receive empty slots (immediately own), importing slots (enter IMPORTING), or replica role (begin PSYNC)

### Node Removal (Graceful)

1. Orchestrator updates topology (remove node's slots)
2. Node enters MIGRATING state for owned slots, transfers keys
3. Final topology pushed, node stops serving cluster traffic

### Node Demotion (Primary -> Replica)

```rust
fn handle_demotion(old_role: Role, new_topology: &Topology) {
    match old_role {
        Role::Primary => {
            self.read_only = true;
            self.wal.flush_sync()?;
            let new_primary = new_topology.find_primary_for(self.old_slots)?;
            self.start_replication(new_primary);
        }
        Role::Replica => {
            let assigned_primary = new_topology.find_my_primary(self.id)?;
            if assigned_primary != self.current_primary {
                self.stop_replication();
                self.start_replication(assigned_primary);
            }
        }
    }
}
```

---

## Node-to-Node Communication

Nodes do NOT gossip with each other. They only connect directly for:

| Purpose | Direction | Protocol |
|---------|-----------|----------|
| Replication | Replica -> Primary | PSYNC + WAL stream |
| Slot Migration | Source -> Target | MIGRATE protocol |

All topology knowledge comes from the orchestrator.

---

## Admin API

Each node exposes an admin API on a separate port:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/cluster` | POST | Receive topology update |
| `/admin/cluster` | GET | Return current topology |
| `/admin/health` | GET | Health check |
| `/admin/replication` | GET | Replication status |
| `/admin/ready` | GET | Node readiness for traffic |

### Health Check Criteria

| Check | Healthy | Unhealthy |
|-------|---------|-----------|
| **Acceptor** | Accepting connections | Not listening |
| **Shard Workers** | All responding within 100ms | Any unresponsive |
| **Memory** | Below critical threshold (95%) | Above critical |
| **Persistence** | Queue depth < high watermark | Blocked > 30s |
| **Cluster State** | Has valid topology | No/stale topology |

---

## Configuration Homogeneity

All nodes in a cluster should have the same `num-shards` configuration. Hash tag colocation is computed per-node using `xxhash64(tag) % num_shards`, so different shard counts change which internal shard keys land on (though colocation is always preserved within each node).

During migration between nodes with different shard counts, the source communicates its shard count and the target redistributes keys to its own internal shards.
