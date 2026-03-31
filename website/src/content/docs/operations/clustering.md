---
title: "Clustering"
description: "FrogDB supports slot-based sharding across multiple nodes for horizontal scaling."
sidebar:
  order: 4
---
FrogDB supports slot-based sharding across multiple nodes for horizontal scaling.

## Overview

- **16384 hash slots** for Redis Cluster client compatibility.
- **Orchestrated control plane** (external orchestrator, not gossip).
- **Full dataset replication** -- replicas copy all data from their primary.

### Terminology

| Term | Definition |
|------|------------|
| **Node** | A FrogDB server instance |
| **Internal Shard** | Thread-per-core partition within a node |
| **Slot** | Hash slot 0-16383 (cluster distribution unit) |
| **Primary** | Node owning slots for writes |
| **Replica** | Node replicating from a primary |
| **Orchestrator** | External service managing cluster topology |

---

## Slot-Based Sharding

FrogDB uses 16384 hash slots:

```
slot = CRC16(key) % 16384
```

Slots are assigned to nodes. Each node owns one or more contiguous slot ranges.

### Hash Tags

Keys containing `{tag}` use only the tag content for hashing, ensuring colocation:

```bash
# These keys hash to the same slot
SET {user:123}:profile "..."
SET {user:123}:settings "..."

# This MGET is guaranteed to be same-slot (fast, atomic)
MGET {user:123}:profile {user:123}:settings
```

### Two-Level Routing

FrogDB has two levels of sharding:

1. **Cluster routing**: `slot = CRC16(key) % 16384` determines which node.
2. **Internal routing**: `internal_shard = hash(key) % N` determines which thread within a node.

Hash tags guarantee colocation at both levels.

---

## Control Plane

### Orchestrated Model

FrogDB uses an external orchestrator rather than Redis-style gossip:

| Aspect | Orchestrated (FrogDB) | Gossip (Redis) |
|--------|----------------------|----------------|
| Topology source | External orchestrator | Node consensus |
| Failure detection | Orchestrator monitors | Nodes vote on failures |
| Topology changes | Deterministic, immediate | Eventually consistent |
| Debugging | Explicit state | Derived from gossip |

### Orchestrator Requirements

The orchestrator is the control plane for cluster operations. **Always deploy 3+ orchestrator instances in production.**

If all orchestrators are unavailable:
- No automatic failover can occur.
- No slot migrations can happen.
- No new nodes can join.
- **Data plane continues** -- existing topology serves traffic normally.

Recommended orchestrator backends: Kubernetes operator with etcd, Consul, or custom Raft-based.

### Topology Configuration

The orchestrator pushes topology as JSON to each node's admin API:

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

---

## Node Roles

### Primary Node

- Owns one or more slot ranges.
- Accepts writes for owned slots.
- Streams changes to replicas via WAL.
- Responds to client requests or sends `-MOVED` redirects.

### Replica Node

- Full copy of primary's dataset.
- Read-only by default (`READONLY` command enables reads).
- Candidates for failover promotion.
- Can serve stale reads to scale read throughput.

---

## Admin API

Each node exposes an admin API on a separate port:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/cluster` | POST | Receive topology update |
| `/admin/cluster` | GET | Return current topology |
| `/admin/health` | GET | Health check |
| `/admin/replication` | GET | Replication status |

### Admin API Security

The admin API must be secured. Options (in order of recommendation):

| Method | Description | Use Case |
|--------|-------------|----------|
| **Network Isolation** | Bind admin port to localhost or private network | Default, simplest |
| **mTLS** | Mutual TLS with client certificates | Production, external access |
| **Bearer Token** | Shared secret in Authorization header | Simple auth for trusted networks |

```toml
[cluster.admin]
bind = "127.0.0.1"  # Bind to localhost only (default)
port = 6380

# For external access with auth:
# bind = "0.0.0.0"
# auth-token = "your-secret-token"
```

### Health Check

The `/admin/health` endpoint returns `200 OK` when healthy or `503 Service Unavailable` when unhealthy.

Health criteria:
- Process is running and accepting connections.
- All shard workers are responsive.
- Memory is below critical threshold (default 95%).
- Persistence is not blocked.
- Cluster topology is valid.

---

## Cluster Configuration

### Node Configuration

All nodes in a cluster should have the same `num-shards` configuration for predictable behavior.

```toml
[cluster]
orchestrator-contact-timeout-ms = 60000  # Max time without orchestrator contact
topology-refresh-interval-ms = 30000     # Periodic topology refresh

[cluster.admin]
bind = "127.0.0.1"
port = 6380

[health]
memory-critical-percent = 95
shard-ping-timeout-ms = 100
```

### Replication Authentication

See [replication.md](/operations/replication/) for replica authentication setup.

---

## Orchestrator Failure Modes

| Scenario | Cluster Behavior |
|----------|------------------|
| Single orchestrator down | Others continue, no impact |
| Orchestrator minority partitioned | Majority continues making decisions |
| Orchestrator majority down | Cluster frozen (no topology changes, no failover) |
| All orchestrators down | Nodes continue with last known topology |

**What works without orchestrator**: All read/write operations, client connections, established replication streams, health endpoint responses.

**What requires orchestrator**: Automatic failover, slot migration, adding/removing nodes, topology updates.

---

## Monitoring

### Key Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_cluster_epoch` | Current local epoch |
| `frogdb_cluster_epoch_stale_detections` | Times stale epoch detected |
| `frogdb_cluster_last_orchestrator_contact` | Timestamp of last orchestrator message |
