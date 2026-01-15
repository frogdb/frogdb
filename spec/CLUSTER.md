# FrogDB Clustering

This document covers FrogDB's clustering architecture, including slot-based sharding, replication protocol, node management, failover, and abstractions for future implementation.

## Overview

FrogDB is designed for single-node operation initially, with clustering as a future capability. The architecture uses:

- **Orchestrated control plane** (DragonflyDB-style) rather than gossip
- **16384 hash slots** for Redis Cluster client compatibility
- **Full dataset replication** - replicas copy all data from primary
- **RocksDB WAL streaming** for incremental replication

For consistency guarantees (single-node and cluster), see [CONSISTENCY.md](CONSISTENCY.md).

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

### Hash Tag Full Colocation

Hash tags (e.g., `{user:1}:profile`) guarantee colocation at **both** levels:

```rust
// For keys with hash tags, both cluster slot AND internal shard
// are computed from the hash tag content, not the full key
fn route_key(key: &[u8]) -> (SlotId, InternalShardId) {
    let hash_input = extract_hash_tag(key).unwrap_or(key);

    let slot = crc16(hash_input) % 16384;           // Cluster slot
    let shard = xxhash64(hash_input) % num_shards;  // Internal shard

    (slot, shard)
}
```

This ensures hash-tagged keys are always on the same internal shard within a node, enabling:
- Atomic MULTI/EXEC transactions
- Lua scripts accessing multiple keys
- WATCH with consistent visibility

### Configuration Homogeneity

**Recommendation:** All nodes in a cluster should have the same `num_shards` configuration.

| Aspect | Same `num_shards` | Different `num_shards` |
|--------|-------------------|------------------------|
| Hash tag colocation | Guaranteed | Guaranteed (per-node) |
| Internal shard distribution | Predictable | Changes after migration |
| Performance characteristics | Uniform | Variable per node |
| Migration complexity | Simple | Adapts via SHARDS_NUM |

**Why it matters:**

Hash tag colocation is computed *per-node* using `xxhash64(tag) % num_shards`. This means:
- Keys with the same hash tag always land on the same internal shard *within* each node
- But with different `num_shards`, the *which* shard differs between nodes

**Example:**
```
Node A (8 shards):  xxhash64("user:1") % 8  = shard 3
Node B (16 shards): xxhash64("user:1") % 16 = shard 11
```

After slot migration from A to B, hash-tagged keys remain colocated (all on shard 11),
but the distribution changes. This is correct but may affect performance predictability.

**Slot Migration with Heterogeneous Configs:**

During migration, the source node communicates its shard count:
```
DFLYMIGRATE INIT [SOURCE_NODE_ID, SHARDS_NUM, SLOT_RANGES]
```

The target adapts by establishing one flow per source shard, then redistributes
keys to its own internal shards based on hash computation.

**Orchestrator Validation (Optional):**

For strict homogeneity, the orchestrator can validate `num_shards` when nodes join:
```json
{
  "cluster_id": "frogdb-prod-1",
  "required_num_shards": 8,
  "nodes": [...]
}
```

Nodes with mismatched `num_shards` would be rejected from joining.

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
- Uses PSYNC-style incremental replication (see [REPLICATION.md](REPLICATION.md))
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

### Orchestrator Requirements

> **CRITICAL: Single Point of Failure**
>
> The orchestrator is the control plane for cluster operations. If ALL orchestrators are unavailable:
> - No automatic failover can occur
> - No slot migrations can happen
> - No new nodes can join
> - Data plane continues (existing topology serves traffic)
>
> **Recommendation:** Always deploy 3+ orchestrator instances in production.

The external orchestrator must satisfy these requirements:

**High Availability:**
- Minimum 3 orchestrator instances recommended
- Odd number required for quorum (3, 5, 7)
- Can use Raft, Paxos, or external consensus (etcd, Consul, ZooKeeper)

**Single Orchestrator Warning:** Running with a single orchestrator is supported for development and testing only. In this configuration:
- Orchestrator failure = no automatic failover possible
- No quorum decisions (all decisions are unilateral)
- Recommended only for non-production environments
- FrogDB logs a warning on startup when cluster mode is enabled with only one orchestrator endpoint configured

**Quorum Semantics:**
- Topology changes require majority agreement
- Health check threshold: 3 consecutive failures before failover
- Orchestrator partition: nodes continue serving existing topology

**Orchestrator Failure Modes:**

| Scenario | Cluster Behavior |
|----------|------------------|
| Single orchestrator down | Others continue, no impact |
| Orchestrator minority partitioned | Majority continues making decisions |
| Orchestrator majority down | Cluster frozen (no topology changes, no failover) |
| All orchestrators down | Nodes continue with last known topology, no failover possible |

**Recovery:**
- Orchestrator state should be persisted (slot assignments, node registry)
- On orchestrator restart: reload state, health check all nodes, resume operations

**Reference Implementations:**
- Kubernetes StatefulSet with etcd
- Consul cluster
- Custom Raft-based orchestrator

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

### Replication Authentication

Replicas authenticate with primaries using dedicated credentials:

| Config | Description |
|--------|-------------|
| `primary_auth` | Password for replica→primary authentication |
| `primary_user` | ACL username for replication (if using ACLs) |

**Configuration:**
```toml
[replication]
primary_auth = "replication-secret"
primary_user = "replicator"  # Optional, for ACL-based auth
max_replicas = 0             # Optional: 0 = unlimited (default), >0 = limit
```

**Behavior:**
- Replica sends AUTH before PSYNC handshake
- If `primary_user` set: `AUTH <user> <password>`
- If only `primary_auth` set: `AUTH <password>`
- Primary rejects PSYNC if authentication fails

**Best Practice:** Set `primary_auth` on all nodes (including primaries) since any node may become a replica after failover.

**PSYNC Handshake Sequence:**

The replica must authenticate before initiating replication:

```
Replica                              Primary
   │                                    │
   │───── AUTH [user] <password> ──────▶│  (if primary_auth configured)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── REPLCONF listening-port 6379 ▶│  (optional, for INFO output)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── REPLCONF capa eof psync2 ────▶│  (announce capabilities)
   │◀──── +OK ──────────────────────────│
   │                                    │
   │───── PSYNC <repl_id> <seq> ───────▶│  (initiate sync)
   │◀──── +FULLRESYNC or +CONTINUE ─────│
```

**Auth Failure Handling:**
- If AUTH fails: Primary returns `-ERR invalid password` and closes connection
- If AUTH required but not sent: Primary returns `-NOAUTH Authentication required` on PSYNC
- Replica retries with exponential backoff on auth failure

### Admin API

Each node exposes an admin API on a separate port:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/cluster` | POST | Receive topology update |
| `/admin/cluster` | GET | Return current topology |
| `/admin/health` | GET | Health check |
| `/admin/replication` | GET | Replication status |

### Health Check Definition

The `/admin/health` endpoint returns `200 OK` when the node is healthy, or `503 Service Unavailable` when unhealthy.

**Health Criteria:**

| Check | Healthy | Unhealthy |
|-------|---------|-----------|
| **Process** | Running | - |
| **Acceptor** | Accepting connections | Not listening |
| **Shard Workers** | All responding to ping within 100ms | Any shard unresponsive |
| **Memory** | Below critical threshold (default: 95%) | Above critical threshold |
| **Persistence** | Not blocked (queue depth < high watermark) | Blocked > 30s |
| **Cluster State** | Has valid topology | No topology or stale epoch |

**Response Format:**
```json
{
  "status": "healthy",
  "checks": {
    "acceptor": "ok",
    "shards": "ok",
    "memory": "ok",
    "persistence": "ok",
    "cluster": "ok"
  },
  "memory_used_bytes": 1234567890,
  "memory_max_bytes": 10737418240,
  "uptime_seconds": 86400
}
```

**Unhealthy Response:**
```json
{
  "status": "unhealthy",
  "checks": {
    "acceptor": "ok",
    "shards": "fail:shard_3_unresponsive",
    "memory": "ok",
    "persistence": "ok",
    "cluster": "ok"
  },
  "reason": "shard_3_unresponsive"
}
```

**Configuration:**
```toml
[health]
memory_critical_percent = 95   # Memory threshold for unhealthy
shard_ping_timeout_ms = 100    # Max time to wait for shard response
persistence_block_timeout_s = 30  # Max persistence queue block time
```

### Epoch Staleness

An epoch is considered **stale** when the node's local epoch is lower than the cluster's current epoch. This indicates the node has missed topology updates.

**Staleness Detection:**

| Condition | Staleness | Action |
|-----------|-----------|--------|
| `local_epoch == cluster_epoch` | Current | Normal operation |
| `local_epoch < cluster_epoch` | Stale | Fetch updated topology from orchestrator |
| `local_epoch > cluster_epoch` | Invalid | Should never happen - indicates corruption or bug |

**How Nodes Detect Staleness:**

1. **Orchestrator push:** Receives topology with higher epoch
2. **Client redirect:** Receives `-MOVED` from another node with higher epoch in response
3. **Replica sync:** Primary sends epoch in replication handshake
4. **Periodic poll:** Node polls orchestrator every `topology_refresh_interval_ms`

**Staleness TTL:**

Topology has no inherent TTL - a node with epoch 42 can operate indefinitely if no topology changes occur. However, nodes are considered unhealthy if:

```toml
[cluster]
# Max time without orchestrator contact before unhealthy
orchestrator_contact_timeout_ms = 60000

# Periodic topology refresh interval
topology_refresh_interval_ms = 30000
```

**Epoch vs Topology:**
- **Epoch:** Monotonic counter, incremented on any topology change
- **Topology:** Full cluster state (nodes, slots, roles)

A node can have a current epoch but stale topology details if:
- Epoch matches but node list is outdated (rare race condition)
- Mitigated by including topology hash in health checks

**Metrics:**
```
frogdb_cluster_epoch                     # Current local epoch
frogdb_cluster_epoch_stale_detections    # Times stale epoch detected
frogdb_cluster_last_orchestrator_contact # Timestamp of last orchestrator message
```

### Orchestrator Security

The orchestrator admin API (`/admin/cluster`, `/admin/acl`) must be secured to prevent unauthorized topology changes.

**Security Options (in order of recommendation):**

| Method | Description | Use Case |
|--------|-------------|----------|
| **Network Isolation** | Bind admin port to localhost or private network only | Default, simplest |
| **mTLS** | Mutual TLS with client certificates | Production, external access |
| **Bearer Token** | Shared secret in Authorization header | Simple auth for trusted networks |

**Configuration:**
```toml
[cluster.admin]
# Bind to localhost only (default, safest)
bind = "127.0.0.1"
port = 6380

# Or bind to all interfaces with auth
# bind = "0.0.0.0"
# port = 6380
# auth_token = "your-secret-token"  # Required if bind != localhost

# mTLS (recommended for production)
# tls_cert = "/path/to/server.crt"
# tls_key = "/path/to/server.key"
# tls_ca = "/path/to/ca.crt"  # Required client certs signed by this CA
```

**Authentication Flow (Bearer Token):**
```
POST /admin/cluster HTTP/1.1
Authorization: Bearer your-secret-token
Content-Type: application/json

{"epoch": 42, "nodes": [...]}
```

**Unauthorized Response:**
```
HTTP/1.1 401 Unauthorized
Content-Type: application/json

{"error": "invalid or missing authentication"}
```

**Redis/Valkey Comparison:**
- Redis/Valkey use ACLs for all commands including `CLUSTER` commands via the Redis protocol
- FrogDB follows DragonflyDB's approach: separate admin HTTP API for orchestration
- ACLs still apply to `CLUSTER` commands sent via Redis protocol

---

## Node Lifecycle Operations

This section documents node-side behavior during cluster membership changes. The orchestrator implementation is out of scope; this documents how nodes respond to orchestrator commands.

### Node Addition

When the orchestrator adds a new node to the cluster:

**Step 1: Node Startup (Standalone)**
```
New Node                          Orchestrator
    │                                  │
    │──── Startup (no cluster) ───────▶│  (node registers or is discovered)
    │                                  │
    │◀─── POST /admin/cluster ─────────│  (initial topology with epoch=N)
    │                                  │
    │     Validate topology            │
    │     Store epoch = N              │
    │     Initialize slot map          │
    │                                  │
    │──── 200 OK ─────────────────────▶│
```

**Step 2: Slot Assignment**

The new node may be assigned slots immediately (if taking over empty slots) or enter IMPORTING state for migration:

| Assignment Type | Node Behavior |
|-----------------|---------------|
| **Empty slots** | Immediately own slots, accept writes |
| **Migrating from another node** | Enter IMPORTING state for assigned slots |
| **Replica role** | No slot ownership, begin PSYNC to primary |

**Node State After Addition:**
```rust
enum NodeJoinState {
    /// Node has topology but no slot ownership yet
    Joined { epoch: u64 },

    /// Node is importing slots from other nodes
    Importing {
        epoch: u64,
        importing_slots: HashMap<SlotId, NodeId>,  // slot -> source node
    },

    /// Node owns slots and is fully operational
    Active {
        epoch: u64,
        owned_slots: Vec<SlotRange>,
    },

    /// Node is a replica
    Replica {
        epoch: u64,
        primary: NodeId,
    },
}
```

**Validation on Topology Receipt:**
- Epoch must be >= current epoch (reject stale topology)
- Node's own ID must be present in topology
- If slots assigned, validate slot ranges are contiguous
- If replica, validate primary exists in topology

### Node Removal

When the orchestrator removes a node from the cluster:

**Graceful Removal (Planned)**
```
Orchestrator               Node (being removed)         Other Nodes
     │                            │                          │
     │  1. Update topology        │                          │
     │  (remove node's slots)     │                          │
     │───────────────────────────▶│                          │
     │────────────────────────────────────────────────────▶  │
     │                            │                          │
     │                            │  Enter MIGRATING         │
     │                            │  for all owned slots     │
     │                            │                          │
     │  2. Migration completes    │                          │
     │                            │── MIGRATE keys ─────────▶│
     │                            │◀─ OK ───────────────────│
     │                            │                          │
     │  3. Final topology         │                          │
     │  (node removed)            │                          │
     │───────────────────────────▶│                          │
     │                            │                          │
     │                            │  Shutdown or             │
     │                            │  become standalone       │
```

**Node Behavior on Removal:**

| Removal Phase | Node Actions |
|---------------|--------------|
| **Slots reassigned** | Enter MIGRATING state, begin key transfer |
| **Migration in progress** | Continue serving MIGRATING slots, redirect missing keys |
| **All slots migrated** | Accept final topology, stop serving cluster traffic |
| **Removed from topology** | Option 1: Shutdown. Option 2: Continue as standalone (requires restart to rejoin) |

**Ungraceful Removal (Node Failure)**

When a node fails unexpectedly:
1. Orchestrator detects via health check failure
2. Orchestrator updates topology (node removed, slots reassigned to replicas or empty)
3. Other nodes receive topology, stop routing to failed node
4. If failed node recovers, it receives new topology and acts accordingly

**Demotion (Primary → Replica)**
```rust
fn handle_demotion(old_role: Role, new_topology: &Topology) {
    match old_role {
        Role::Primary => {
            // Stop accepting writes
            self.read_only = true;

            // Flush pending WAL
            self.wal.flush_sync()?;

            // Begin PSYNC to new primary
            let new_primary = new_topology.find_primary_for(self.old_slots)?;
            self.start_replication(new_primary);
        }
        Role::Replica => {
            // May need to switch primary
            let assigned_primary = new_topology.find_my_primary(self.id)?;
            if assigned_primary != self.current_primary {
                self.stop_replication();
                self.start_replication(assigned_primary);
            }
        }
    }
}
```

### Node Readiness

Before a node accepts client traffic after joining or topology change:

**Readiness Criteria:**
1. Topology received and validated
2. If primary: Owns at least one slot (or explicitly empty)
3. If replica: Connected to primary and initial sync complete (or allowed to serve stale)
4. If importing: At least one IMPORTING slot ready to accept ASKING commands

**Readiness Endpoint:**
```
GET /admin/ready

200 OK  {"ready": true, "role": "primary", "slots_owned": 5461}
503     {"ready": false, "reason": "initial_sync_incomplete"}
```

---

## Replication Protocol

FrogDB uses primary-replica replication with RocksDB WAL streaming for high availability.

**Key concepts:**
- **PSYNC** for partial synchronization, **FULLRESYNC** for new replicas
- **Replication IDs** track dataset history across failovers
- **TCP backpressure** (no buffer limits) avoids "full sync loop" problems
- **Synchronous replication** available via `min_replicas_to_write`

See [REPLICATION.md](REPLICATION.md) for the complete replication protocol specification, including:
- Full and partial synchronization protocols
- WAL streaming format and framing
- Replication backlog and flow control
- Connection management and authentication
- Failure modes and recovery
- Configuration reference and metrics

---

## Synchronous Replication

When `min_replicas_to_write >= 1`, writes wait for replica acknowledgment before responding to client. This reduces data loss on failover but increases write latency.

See [REPLICATION.md - Synchronous Replication](REPLICATION.md#synchronous-replication) for protocol details, ACK format, timeout behavior, and failure handling.

---

## WAIT Command

The `WAIT` command blocks until a write has propagated to N replicas:

```
SET mykey myvalue
WAIT 1 5000
:1
```

`WAIT` is complementary to `min_replicas_to_write`:
- `min_replicas_to_write`: Pre-write gate (reject if insufficient replicas)
- `WAIT`: Post-write confirmation (block until replicated)

See [REPLICATION.md - WAIT Command](REPLICATION.md#wait-command) for details.

---

## Client Protocol

### Slot Ownership Validation

Before executing a command, the node must determine whether it owns the relevant slot(s). This validation happens early in the command pipeline, after key extraction but before execution.

**Validation Algorithm:**

```rust
fn validate_slot_ownership(
    keys: &[&[u8]],
    topology: &ClusterTopology,
    migration_state: &MigrationState,
) -> SlotValidationResult {
    if keys.is_empty() {
        return SlotValidationResult::NoKeys;  // Keyless command, execute locally
    }

    // 1. Calculate slots for all keys
    let slots: HashSet<SlotId> = keys.iter()
        .map(|k| crc16(extract_hash_tag(k)) % 16384)
        .collect();

    // 2. Check for cross-slot violation (multi-key commands)
    if slots.len() > 1 {
        return SlotValidationResult::CrossSlot;
    }

    let slot = *slots.iter().next().unwrap();

    // 3. Check local epoch vs topology epoch
    if topology.epoch > self.local_epoch {
        // Stale topology - should refresh, but continue with local knowledge
        metrics.stale_topology_commands.inc();
    }

    // 4. Check if we own this slot
    let owner = topology.slot_owner(slot);

    if owner == self.node_id {
        // We own this slot - check migration state
        match migration_state.get(slot) {
            Some(MigrationSlotState::Migrating { target }) => {
                SlotValidationResult::Migrating { slot, target }
            }
            None => SlotValidationResult::Owned,
        }
    } else {
        // We don't own this slot
        match migration_state.get(slot) {
            Some(MigrationSlotState::Importing { source }) => {
                SlotValidationResult::Importing { slot, source }
            }
            None => SlotValidationResult::Redirect {
                slot,
                owner,
                owner_addr: topology.node_address(owner),
            }
        }
    }
}

enum SlotValidationResult {
    NoKeys,                                          // Keyless command
    Owned,                                           // Execute locally
    CrossSlot,                                       // Multi-slot error
    Redirect { slot: SlotId, owner: NodeId, owner_addr: SocketAddr },
    Migrating { slot: SlotId, target: NodeId },     // We're source
    Importing { slot: SlotId, source: NodeId },     // We're target
}
```

**Handling Validation Results:**

| Result | Command Action |
|--------|----------------|
| `NoKeys` | Execute locally |
| `Owned` | Execute locally |
| `CrossSlot` | Return `-CROSSSLOT Keys in request don't hash to the same slot` |
| `Redirect` | Return `-MOVED <slot> <host>:<port>` |
| `Migrating` | Execute if key exists locally, else `-ASK <slot> <target>` |
| `Importing` | Execute only if `ASKING` flag set, else `-MOVED <slot> <source>` |

### CROSSSLOT Validation

Multi-key commands must operate on keys that hash to the same slot. CROSSSLOT validation occurs **after key extraction, before routing**.

**Validation Timing in Pipeline:**

```
1. Parse command → ParsedCommand
2. Lookup handler → Command trait
3. Validate arity → Check arg count
4. Extract keys → handler.keys(&args)
5. ★ CROSSSLOT check ★ → All keys same slot?
6. Slot ownership check → MOVED/ASK/execute
7. Execute command
```

**CROSSSLOT Rules (Redis/Valkey Compatible):**

| Scenario | Behavior |
|----------|----------|
| Single-key command | No CROSSSLOT check needed |
| Multi-key, same slot | Execute normally |
| Multi-key, different slots | Return `-CROSSSLOT` error |
| Hash tags ensure same slot | Execute normally |

**Example:**
```
MGET key1 key2 key3
  → slot(key1) = 1234, slot(key2) = 5678, slot(key3) = 1234
  → Slots differ → -CROSSSLOT Keys in request don't hash to the same slot

MGET {user:1}:a {user:1}:b {user:1}:c
  → All hash to slot(user:1) = 4567
  → Execute normally
```

**CROSSSLOT in Transactions (Redis/Valkey Compatible):**

FrogDB follows Redis and Valkey behavior: cross-slot commands within `MULTI/EXEC` cause the entire transaction to abort.

```
MULTI
+OK
SET key1 val1      # slot 1234
+QUEUED
SET key2 val2      # slot 5678 (different slot!)
-CROSSSLOT Keys in request don't hash to the same slot
EXEC
-EXECABORT Transaction discarded because of previous errors.
```

**Key Points:**
- CROSSSLOT error is returned immediately when the command is queued (not at EXEC time)
- Once any command returns an error during queuing, EXEC returns EXECABORT
- The entire transaction is discarded; no commands are executed
- Use hash tags (e.g., `{user:1}:profile`, `{user:1}:settings`) to ensure multi-key operations target the same slot

**Slot Migration During Transaction:**

If slot ownership changes during a transaction (between MULTI and EXEC):
- At EXEC time, the server checks if a failover or slot migration has occurred since queuing
- If the slot moved, EXEC returns `-MOVED` or `-ASK` without executing any commands
- Client must retry the entire transaction on the correct node

### Cross-Node Authentication

When a client receives a `-MOVED` or `-ASK` redirect, it must establish a new connection to the target node. Authentication handling:

**Client Responsibility:**
- After redirect, client opens new connection to target node
- If the cluster requires authentication, client must `AUTH` on new connection
- Connection pooling clients should maintain authenticated connections to all known nodes

**Server Behavior:**
- Target node has no knowledge of client's auth state on source node
- Target node requires fresh `AUTH` if authentication is enabled
- `ASKING` command does NOT require special authentication

**Redirect Flow with Auth:**
```
Client                    Node A                    Node B
   │                         │                         │
   │── AUTH user pass ──────▶│                         │
   │◀─ +OK ─────────────────│                         │
   │                         │                         │
   │── GET key ─────────────▶│                         │
   │◀─ -MOVED 1234 B:6379 ──│                         │
   │                         │                         │
   │── [new connection] ─────────────────────────────▶│
   │── AUTH user pass ───────────────────────────────▶│  (required!)
   │◀─ +OK ──────────────────────────────────────────│
   │── GET key ──────────────────────────────────────▶│
   │◀─ $5\r\nvalue ──────────────────────────────────│
```

**Cluster-Wide Auth Consistency:**
- All nodes should share the same ACL configuration (see ACL Distribution section)
- Orchestrator pushes ACL updates to all nodes to ensure consistency
- Client credentials valid on one node should be valid on all nodes

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

### Slot Migration Algorithm

This section specifies the detailed algorithm for migrating keys during slot rebalancing.

#### Phase 1: Snapshot

When migration starts, the source creates a **point-in-time key iterator** for the slot:

```rust
fn start_migration(slot: u16) -> MigrationState {
    // Create point-in-time snapshot of keys in this slot
    // Iterator sees keys as they existed at migration START
    let key_iterator = store.snapshot_keys_in_slot(slot);

    // Mark slot as MIGRATING
    slot_states.insert(slot, SlotState::Migrating {
        iterator: key_iterator,
        migration_acked: HashSet::new(),  // Keys ACK'd by target
        pending_writes: VecDeque::new(),  // Writes during migration
    });

    MigrationState::new(key_iterator)
}
```

**Key principle:** The iterator captures a consistent snapshot at migration start. New writes during
migration do NOT appear in the iterator (they're handled separately via `pending_writes`).

#### Phase 2: Streaming

Keys are streamed to the target in batches:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Concurrent Write Handling                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Write arrives for key K during migration:                               │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │ Is K in iterator (existed at migration start)?                   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│          │                                    │                          │
│          ▼ YES                                ▼ NO                       │
│  ┌─────────────────────┐            ┌─────────────────────┐             │
│  │ Has K been sent to  │            │ Apply locally       │             │
│  │ target yet?         │            │ Queue for migration │             │
│  └─────────────────────┘            │ (pending_writes)    │             │
│      │           │                  └─────────────────────┘             │
│      ▼ YES       ▼ NO                                                   │
│  Forward to   Apply locally                                             │
│  target via   (COW: old value                                           │
│  ASKING       already captured)                                         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Deletion During Migration:**

Keys are NOT deleted from source until:
1. Target ACKs the batch containing the key
2. Migration is finalized (DFLYMIGRATE FINALIZE received)
3. Topology update confirms target ownership

```rust
fn handle_batch_ack(batch_seq: u64, keys: &[Bytes]) {
    // Mark keys as ACK'd (safe to delete after finalization)
    for key in keys {
        migration_state.migration_acked.insert(key.clone());
    }
    // DO NOT delete yet - wait for finalization
}

fn handle_finalization() {
    // Now safe to delete
    // Schedule async deletion (low priority, doesn't block migration)
    task::spawn_low_priority(async move {
        for key in migration_state.migration_acked.iter() {
            store.delete(key);
        }
    });

    // Keep tombstones for 60s to handle straggler redirects
    slot_states.insert(slot, SlotState::Tombstone {
        expires_at: Instant::now() + Duration::from_secs(60),
    });
}
```

#### Orphaned Key Detection

If migration fails or is aborted, orphaned keys may exist on both source and target:

```rust
fn cleanup_failed_migration(migration_id: Uuid, slot: u16) {
    // On SOURCE:
    // - Keys still exist (never deleted)
    // - Clear MIGRATING state
    // - Migration never happened from source's perspective

    // On TARGET:
    // - Keys received during migration must be cleaned up
    // - Scan slot for keys with migration_id marker
    // - Delete keys that were only partially migrated

    let orphaned_keys = target.scan_slot(slot)
        .filter(|k| k.migration_source_id == Some(migration_id));

    for key in orphaned_keys {
        target.delete(&key);
        metrics.orphaned_keys_cleaned.inc();
    }
}
```

**Orphan Detection Heuristics:**

| Scenario | Detection | Cleanup |
|----------|-----------|---------|
| Target received keys, no FINALIZE | migration_id mismatch with current slot owner | Delete on target |
| Source sent keys, target unreachable | Migration timeout | Keys remain on source (safe) |
| Both nodes crash mid-migration | Orchestrator detects incomplete migration | Orchestrator issues explicit cleanup |

#### Oversized Key Handling

If a single key exceeds `batch_size_bytes`:

```rust
fn create_batch(keys: &mut KeyIterator, config: &MigrationConfig) -> Batch {
    let mut batch = Batch::new();

    while let Some(key) = keys.peek() {
        let dump = serialize_key(key);

        // Special case: single key exceeds batch size
        if batch.is_empty() && dump.len() > config.batch_size_bytes {
            // Send as single-key batch with LARGE_KEY flag
            batch.add_large_key(keys.next().unwrap(), dump);
            batch.flags |= BatchFlags::LARGE_KEY;
            return batch;  // Return immediately, don't add more keys
        }

        // Normal batching logic
        if batch.num_keys >= config.batch_size_keys
           || batch.size_bytes + dump.len() > config.batch_size_bytes {
            break;
        }

        batch.add(keys.next().unwrap(), dump);
    }

    batch
}
```

**Target Handling for Large Keys:**

| Key Size | Target Behavior |
|----------|-----------------|
| ≤ batch_size_bytes | Normal batch processing |
| > batch_size_bytes, ≤ max_key_size | Accept with LARGE_KEY flag |
| > max_key_size | Reject with `-OOM key too large` |

**Configuration:**

```toml
[cluster.migration]
batch_size_bytes = 1048576     # 1MB default batch size
max_key_size = 536870912       # 512MB maximum single key
large_key_timeout_ms = 300000  # 5 min timeout for large keys
```

### Migration Protocol Details

This section specifies the slot migration protocol at a conceptual level.

#### Migration Commands

**DFLYMIGRATE INIT**

Source node initiates migration to target:

```
DFLYMIGRATE INIT <migration_id> <source_node_id> <num_shards> <slot_ranges>
```

| Field | Description |
|-------|-------------|
| migration_id | UUID identifying this migration |
| source_node_id | NodeId of source node |
| num_shards | Source node's internal shard count (for flow setup) |
| slot_ranges | Slot ranges being migrated (e.g., "1234-1234" or "0-1000") |

**Response:**

| Response | Meaning |
|----------|---------|
| `+OK` | Target enters IMPORTING state, ready to receive |
| `-BUSY migration already in progress` | Target busy with another migration |
| `-SLOTS already owned` | Target already owns some slots in range |

**DFLYMIGRATE FLOW**

Source establishes per-shard data flow:

```
DFLYMIGRATE FLOW <migration_id> <shard_id>
```

One FLOW connection per source internal shard. Target spawns receiver coroutine for each.

**DFLYMIGRATE DATA**

Batch of keys sent over FLOW connection:

```
DFLYMIGRATE DATA <migration_id> <shard_id> <batch_seq> <num_keys>
<key1_len> <key1> <dump1_len> <dump1>
<key2_len> <key2> <dump2_len> <dump2>
...
```

| Field | Description |
|-------|-------------|
| batch_seq | Monotonic batch sequence number (for ordering) |
| num_keys | Number of keys in this batch |
| dump | DUMP-format serialized value (includes TTL) |

**Response per batch:**

| Response | Meaning |
|----------|---------|
| `+OK <batch_seq>` | Batch received and persisted |
| `-OOM out of memory` | Target cannot accept more keys |

**DFLYMIGRATE ACK**

Source confirms all data sent for a shard:

```
DFLYMIGRATE ACK <migration_id> <shard_id> <total_keys> <total_bytes>
```

Target verifies counts match received data.

**DFLYMIGRATE FINALIZE**

Orchestrator signals migration complete:

```
DFLYMIGRATE FINALIZE <migration_id>
```

Sent to both source and target. Source clears MIGRATING state, target clears IMPORTING and takes ownership.

#### Key Transfer Batching

Keys are transferred in batches to balance throughput and memory:

```toml
[cluster.migration]
# Maximum keys per batch
batch_size_keys = 100

# Maximum batch size in bytes
batch_size_bytes = 1048576    # 1MB

# Maximum concurrent batches in flight per flow
max_inflight_batches = 4
```

**Batching Algorithm:**

```rust
fn create_batch(keys: &mut KeyIterator, config: &MigrationConfig) -> Batch {
    let mut batch = Batch::new();

    while let Some(key) = keys.peek() {
        let dump = serialize_key(key);

        // Stop if batch would exceed limits
        if batch.num_keys >= config.batch_size_keys
           || batch.size_bytes + dump.len() > config.batch_size_bytes {
            break;
        }

        batch.add(keys.next().unwrap(), dump);
    }

    batch
}
```

**Flow Control:**

- Source tracks in-flight batches per flow
- Waits for ACK before sending more when at `max_inflight_batches`
- TCP backpressure applies if target is slow

#### Migration Progress Reporting

Source reports progress to orchestrator periodically:

```
POST /admin/migration/progress
{
    "migration_id": "uuid",
    "slot_range": "1234-1234",
    "source_node": "node-abc",
    "target_node": "node-def",
    "status": "in_progress",
    "keys_total": 50000,
    "keys_migrated": 25000,
    "bytes_migrated": 104857600,
    "shards_complete": 4,
    "shards_total": 8,
    "started_at": "2024-01-15T10:00:00Z",
    "estimated_remaining_ms": 30000
}
```

**Progress Check Flow:**

```
Source                      Orchestrator
   │                            │
   │── POST /migration/progress ▶│
   │   (every progress_check_interval_ms)
   │                            │
   │                            │ Check:
   │                            │  - Is progress > min_progress_keys?
   │                            │  - Is migration within timeout?
   │                            │
   │◀── 200 OK (continue) ──────│
   │   or                       │
   │◀── 409 ABORT ──────────────│  (timeout or orchestrator decision)
```

**Metrics:**

```
frogdb_migration_keys_sent{migration_id}       # Keys sent (source)
frogdb_migration_keys_received{migration_id}   # Keys received (target)
frogdb_migration_bytes_sent{migration_id}      # Bytes sent
frogdb_migration_batch_latency_ms              # Time per batch
frogdb_migration_flow_active                   # Active flow connections
```

#### Migration Finalization Sequence

Complete handoff sequence:

```
Source (A)                 Orchestrator               Target (B)
   │                            │                         │
   │  [All shards ACK'd]        │                         │
   │── POST /migration/complete ▶│                         │
   │                            │                         │
   │                            │  Verify both nodes ready│
   │                            │                         │
   │◀── DFLYMIGRATE FINALIZE ───│                         │
   │                            │── DFLYMIGRATE FINALIZE ─▶│
   │                            │                         │
   │  Clear MIGRATING           │                         │  Clear IMPORTING
   │  Remove keys locally       │                         │  Accept writes
   │                            │                         │
   │                            │── FROGDB.TOPOLOGY ──────▶│
   │◀── FROGDB.TOPOLOGY ────────│                         │
   │                            │                         │
   │  Update slot map           │                         │  Own slot officially
```

**Finalization Steps (Source):**

1. Receive DFLYMIGRATE FINALIZE
2. Clear MIGRATING state for slot
3. Delete migrated keys locally (async, low priority)
4. Return +OK

**Finalization Steps (Target):**

1. Receive DFLYMIGRATE FINALIZE
2. Clear IMPORTING state for slot
3. Begin accepting direct writes (not just ASKING)
4. Return +OK

**Atomic Guarantee:**

The FINALIZE command is idempotent. If either node crashes:
- On restart, node receives topology update
- Topology is authoritative for slot ownership
- MIGRATING/IMPORTING states are transient (not persisted)

**Rollback on Finalization Failure:**

If orchestrator cannot reach both nodes for FINALIZE:

| Scenario | Recovery |
|----------|----------|
| Source unreachable | Wait for source recovery, retry FINALIZE |
| Target unreachable | Wait for target recovery, retry FINALIZE |
| Both unreachable | Wait for recovery, retry FINALIZE |
| Orchestrator crash | New orchestrator discovers migration state, completes |

Migration state is persisted by orchestrator. Keys already on target are safe. Worst case: some keys exist on both nodes until FINALIZE completes.

### Migration Data Preservation

**TTL Handling:**
- TTL stored as absolute Unix timestamp in value header (`expires_at` field)
- Timestamp preserved exactly during key migration (part of serialized value)
- **Clock skew consideration:** Nodes should use NTP for synchronized clocks
- Skew of ±1 second may cause keys to expire slightly early/late on target node
- For critical TTL accuracy, ensure cluster nodes are time-synchronized

**Value Format During Migration:**
```
MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [KEYS key...]
```
Each key is serialized with its full metadata (type, expiry, value) using DUMP format.

### Internal Shard Distribution

When keys migrate to a new node, they are assigned to internal shards on the target:

- **Hash tag preservation:** Keys with hash tags (`{tag}key`) use the tag for internal shard assignment
- **Internal shard assignment:** `xxhash64(hash_tag_or_key) % num_shards`
- **Configuration homogeneity:** All cluster nodes should use the same `num_shards` setting

**Important:** If source and target nodes have different `num_shards` configurations, hash-tagged keys may land on different internal shards, but colocation within the same hash tag is still preserved on each node.

### Connection State During Migration

Client connection state may be affected during slot migration:

| State | Behavior |
|-------|----------|
| **MULTI/EXEC in progress** | If any key migrates mid-transaction, EXEC returns `-MOVED` or `-ASK` for affected keys. Client must retry. |
| **WATCH** | Watched keys that migrate are implicitly unwatched. EXEC returns nil (transaction aborted). |
| **SUBSCRIBE (global)** | Unaffected - global pub/sub not tied to slots |
| **SSUBSCRIBE (sharded)** | Client receives `-MOVED` and must re-subscribe on new node |
| **Blocking commands** | If key migrates during BLPOP/BRPOP wait, command times out (future feature) |

### Blocking Commands and Failover (Design Notes)

When blocking commands (BLPOP, BRPOP, BLMOVE) are implemented:

| Failover Event | Blocking Client Behavior |
|----------------|-------------------------|
| Primary fails while client blocked | Client times out (no response from dead primary) |
| Slot migrates while blocked | Return `-MOVED`, client must retry on new node |
| Replica promoted while blocked | Blocked state lost, client times out |

**Future: Blocking State Transfer**

For improved UX, blocking state could be transferred during failover:
1. Primary tracks blocked clients with their timeout remaining
2. On graceful failover (CLUSTER FAILOVER): transfer blocked client list to new primary
3. New primary accepts transferred blocked state
4. Blocked clients receive response when key is pushed (seamless)

**Note:** This is a future enhancement. Initial implementation will rely on client timeouts and retries.

**Client responsibility:** Handle `-MOVED` and `-ASK` redirects, retry failed transactions.

### Replica Behavior During Slot Migration

Replicas follow their primary through slot migrations:

| Migration Phase | Replica Behavior |
|-----------------|------------------|
| **MIGRATING on primary** | Replica continues serving READONLY for all keys |
| **Key migrated** | Replica receives DELETE via replication stream |
| **Slot ownership transferred** | Replica removes slot from local mapping |

**READONLY Consistency During Migration:**
- Replicas may briefly serve stale data for migrated keys
- After DELETE replicates, key returns nil
- No `-MOVED` from replicas - they serve from local data

**Slot Ownership Tracking:**
```rust
// Replica receives slot ownership changes via replication
enum ReplicationEvent {
    KeyUpdate { key: Bytes, value: Value },
    KeyDelete { key: Bytes },
    SlotOwnershipChange { slot: u16, new_owner: NodeId },  // Replica updates local mapping
}
```

### Atomic Migration (Future)

Valkey 9.0-style server-side migration:

- Batched key migration
- No client ASK handling needed
- Rollback on failure
- Lower end-to-end latency

### Migration Timeout

Slot migrations have configurable timeouts to prevent indefinite stalls.

**Configuration:**
```toml
[cluster.migration]
# Maximum time for entire slot migration (all keys)
slot_migration_timeout_ms = 3600000    # 1 hour default

# Maximum time for single MIGRATE command batch
migrate_command_timeout_ms = 60000     # 60 seconds default

# Time between migration progress checks
progress_check_interval_ms = 10000     # 10 seconds default

# Minimum keys migrated per progress interval to consider "making progress"
min_progress_keys = 100
```

**Timeout Behavior:**

| Timeout Type | Trigger | Node Behavior |
|--------------|---------|---------------|
| `migrate_command_timeout` | Single MIGRATE batch takes too long | Retry batch with smaller key count |
| `slot_migration_timeout` | Entire slot migration exceeds limit | Abort migration, cleanup orphaned state |
| No progress | No keys migrated in `progress_check_interval` | Log warning, continue (may indicate large keys) |

**Source Node on Timeout:**
```rust
fn handle_migration_timeout(slot: SlotId) {
    // 1. Abort migration - remain slot owner
    self.migration_state.remove(slot);

    // 2. Any keys that were ALREADY migrated remain on target
    // (no automatic rollback - would require distributed coordination)

    // 3. Log for operator intervention
    warn!(slot, "migration timeout - slot remains on source, manual cleanup may be required");

    // 4. Notify orchestrator
    self.notify_orchestrator(MigrationAborted { slot, reason: "timeout" });
}
```

**Target Node on Timeout:**
```rust
fn handle_migration_timeout(slot: SlotId) {
    // 1. Exit IMPORTING state
    self.migration_state.remove(slot);

    // 2. Keep any keys already received (orphaned keys)
    // These will be cleaned up when slot is officially assigned elsewhere

    // 3. Log for operator intervention
    warn!(slot, "migration timeout - received partial keys, awaiting topology update");
}
```

### Migration Failure Recovery

When migration fails (timeout, crash, network partition), recovery procedures vary by failure mode.

**Failure: Source Node Crashes Mid-Migration**

```
State Before Crash:
  - Source: MIGRATING slot 1234, some keys moved to target
  - Target: IMPORTING slot 1234, has partial keys

Recovery:
  1. Orchestrator detects source failure (health check)
  2. If source had replica: promote replica (has all keys including not-yet-migrated)
  3. Orchestrator updates topology: new primary owns slot 1234
  4. Target exits IMPORTING state on topology update
  5. Target's partial keys become orphaned (will be cleaned on next full sync or manual SCAN + DEL)
```

**Failure: Target Node Crashes Mid-Migration**

```
State Before Crash:
  - Source: MIGRATING slot 1234
  - Target: IMPORTING slot 1234, has partial keys (lost on crash)

Recovery:
  1. Orchestrator detects target failure
  2. Source exits MIGRATING state (timeout or orchestrator notification)
  3. Source remains slot owner with all keys
  4. Migration must be restarted to different target or recovered target
```

**Failure: Network Partition Between Source and Target**

```
State During Partition:
  - Source: MIGRATING, MIGRATE commands timing out
  - Target: IMPORTING, no new keys arriving

Recovery:
  1. Source hits migrate_command_timeout, retries
  2. After slot_migration_timeout: source aborts, exits MIGRATING
  3. Source notifies orchestrator of abort
  4. Orchestrator can retry migration after partition heals
  5. Target exits IMPORTING on orchestrator topology update
```

**Orphaned Key Cleanup:**

When migration fails, target may have partial keys that don't belong to it:

```rust
fn cleanup_orphaned_keys(slot: SlotId) {
    // Only run after topology confirms we don't own this slot
    if self.owns_slot(slot) {
        return;  // Not orphaned, we actually own it now
    }

    // Scan and delete keys belonging to orphaned slot
    let keys = self.scan_keys_in_slot(slot);
    for key in keys {
        self.delete_key(key);
    }

    metrics.orphaned_keys_cleaned.inc_by(keys.len());
}
```

**Manual Intervention Commands:**

| Command | Purpose |
|---------|---------|
| `CLUSTER SETSLOT <slot> STABLE` | Clear MIGRATING/IMPORTING state |
| `CLUSTER SETSLOT <slot> NODE <node-id>` | Force slot ownership |
| `CLUSTER COUNTKEYSINSLOT <slot>` | Check for orphaned keys |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | List keys for manual cleanup |

**Metrics:**

```
frogdb_migration_timeout_total       # Migrations that hit timeout
frogdb_migration_abort_total         # Migrations aborted (any reason)
frogdb_migration_orphaned_keys       # Keys orphaned by failed migration
frogdb_migration_recovery_total      # Successful migration recoveries
```

---

## Failover

### Automatic Failover

Orchestrator-driven failover process:

**Step 1: Detect Primary Failure**
- Health check fails N consecutive times (default: 3)
- Or primary explicitly reports FAILING state

**Step 2: Select Best Replica**

Selection criteria (in priority order):
1. **Node priority**: Configurable `replica_priority` (default: 100, lower = more preferred, 0 = never promote)
2. **Replication lag**: Prefer replica with highest `sequence_number` (least data loss) - see [REPLICATION.md - Lag Measurement](REPLICATION.md#replication-lag-measurement)
3. **Connection stability**: Prefer replica with longest continuous connection to primary
4. **Deterministic tiebreaker**: Lexicographic NodeId comparison

```
replica_score =
    (replica_priority == 0 ? INFINITY : replica_priority * 100000) +
    (max_seq - replica_seq) * 1000 +
    (now - connected_since).seconds()
# Lower score = better candidate
# Priority 0 = never promote (excluded from selection)
# Priority 1-99 = high priority, 100 = default, 101+ = low priority
```

**Configuration:**
```toml
[cluster]
replica_priority = 100  # Default. Set to 0 to never promote this replica.
```

**Step 3: Promote Replica**
- Send `ROLE PRIMARY` command to selected replica
- Replica generates new [ReplicationId](REPLICATION.md#replication-id)
- Replica stores previous primary's ReplicationId as secondary_id (enables PSYNC continuity)

**Step 4: Update Topology**
- Push new topology to all nodes
- Epoch number incremented
- Nodes reject commands for old epoch

**Step 5: Fence Old Primary (Recommended)**
- If old primary recovers, it receives topology update
- Sees higher epoch than local → refuses writes
- Becomes replica of new primary

```
Orchestrator                 Primary (A)              Replica (B)
     │                           │                        │
     │  Health check fails x3    │ (down)                 │
     │                           │                        │
     │  Select B (highest seq)   │                        │
     │                           │                        │
     │  ROLE PRIMARY             │                        │
     │─────────────────────────────────────────────────▶  │
     │                           │                        │ Become primary
     │                           │                        │ New ReplicationId
     │  Push topology (epoch+1)  │                        │
     │─────────────────────────────────────────────────▶  │
     │                           │                        │
     │  (A recovers, receives topology, becomes replica)  │
```

### Replica Promotion Timeline

Detailed state transitions when a replica is promoted to primary:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Replica Promotion State Machine                      │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  REPLICA_CONNECTED ────[ROLE PRIMARY cmd]────▶ PROMOTING               │
│       │                                            │                    │
│       │                                            │                    │
│       │                                    1. Stop replication stream   │
│       │                                    2. Flush pending WAL         │
│       │                                    3. Wait for WAL sync         │
│       │                                            │                    │
│       │                                            ▼                    │
│       │                                       BECOMING_PRIMARY          │
│       │                                            │                    │
│       │                                    4. Generate new ReplicationId│
│       │                                    5. Store secondary_id        │
│       │                                    6. Set role = PRIMARY        │
│       │                                            │                    │
│       │                                            ▼                    │
│       │                                       PRIMARY_READY             │
│       │                                            │                    │
│       │                                    7. Accept writes             │
│       │                                    8. Begin accepting replicas  │
│       │                                            │                    │
│       ▼                                            ▼                    │
│  [Error/Timeout] ──────────────────────▶ REPLICA_CONNECTING            │
│                                         (retry connection to old primary)│
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

**Promotion Steps in Detail:**

| Step | Action | Duration | Failure Handling |
|------|--------|----------|------------------|
| 1 | Stop receiving WAL from old primary | Immediate | N/A |
| 2 | Flush in-memory WAL batch to RocksDB | ~1-10ms | Retry until success |
| 3 | Wait for WAL sync to disk | ~1-50ms | Timeout → abort promotion |
| 4 | Generate new ReplicationId | < 1ms | N/A |
| 5 | Store old primary's ID as secondary_id | < 1ms | N/A |
| 6 | Update role to PRIMARY | < 1ms | N/A |
| 7 | Send ROLE_CHANGED ack to orchestrator | < 1ms | N/A |
| 8 | Accept write commands | After topology update | N/A |
| 9 | Listen for replica PSYNC | Immediate | N/A |

### Failover Timing Invariants

**Critical Invariant:** Topology update arrives AFTER ROLE_CHANGED acknowledgment.

```
Failover sequence (timestamped):

T+0ms:   Orchestrator detects primary failure (health check timeout)
T+10ms:  Orchestrator selects replica with lowest lag
T+20ms:  Orchestrator sends ROLE PRIMARY to selected replica
T+30ms:  Replica:
           1. Stops accepting replication stream
           2. Flushes in-memory WAL buffer to RocksDB
           3. Waits for WAL sync to disk
           4. Stores old primary's ID as secondary_id
           5. Transitions to PRIMARY role
           6. Sends ROLE_CHANGED ack to orchestrator
T+50ms:  Orchestrator receives ROLE_CHANGED ack
T+55ms:  Orchestrator updates cluster topology:
           - Marks old primary as FAILED
           - Assigns slots to new primary
           - Broadcasts TOPOLOGY_UPDATE to all nodes
T+60ms:  All nodes apply topology update
T+65ms:  New primary begins accepting writes

INVARIANT: Topology update (T+55ms) occurs AFTER ROLE_CHANGED (T+50ms)
  - Prevents race where writes arrive before replica is ready
  - If topology arrives early (network reorder), node queues until ROLE completes
```

**Handling Network Reordering:**

```rust
fn handle_topology_update(&mut self, update: TopologyUpdate) {
    if self.promotion_in_progress {
        // Queue topology update until promotion completes
        self.pending_topology = Some(update);
        return;
    }

    self.apply_topology(update);
}

fn complete_promotion(&mut self) {
    self.promotion_in_progress = false;
    self.role = Role::Primary;

    // Apply any queued topology update
    if let Some(update) = self.pending_topology.take() {
        self.apply_topology(update);
    }
}
```

**Configuration:**
```toml
[failover]
promotion_wal_sync_timeout_ms = 5000  # Max wait for WAL flush
promotion_total_timeout_ms = 10000    # Max total promotion time
```

**In-Flight PSYNC During Promotion:**

When the new primary was serving as a replica, it may have had other replicas connecting to it (cascading, which FrogDB doesn't support). Since cascading is not supported, this is N/A.

However, if the old primary had in-flight FULLRESYNC to this replica when failover occurred:

| State | Handling |
|-------|----------|
| FULLRESYNC in progress | Abort, checkpoint discarded |
| Partial sync in progress | Promotion waits for pending WAL entries |
| Stream idle (caught up) | Promotion proceeds immediately |

**Metrics:**
```
frogdb_promotion_duration_ms       # Time spent in PROMOTING state
frogdb_promotion_wal_flush_duration_ms  # Time to flush pending WAL
frogdb_promotion_success_total          # Successful promotions
frogdb_promotion_timeout_total          # Promotions that timed out
frogdb_promotion_aborted_total          # Promotions aborted (e.g., orchestrator cancelled)
```

### Manual Failover

Using CLUSTER FAILOVER command:

| Mode | Behavior |
|------|----------|
| Default (graceful) | Wait for replica to catch up, then switch |
| FORCE | Immediate promotion, may lose data |

### Failover Protocol Details

This section specifies the message flow at a conceptual level.

#### Orchestrator→Node Commands

**FROGDB.PROMOTE**

Sent by orchestrator to selected replica to initiate promotion:

```
FROGDB.PROMOTE <epoch> <new_replication_id> <slot_ranges>
```

| Field | Description |
|-------|-------------|
| epoch | New topology epoch number |
| new_replication_id | Pre-generated ReplicationId for new primary |
| slot_ranges | Slots this node is now primary for |

**Response:**

| Response | Meaning |
|----------|---------|
| `+OK` | Promotion initiated |
| `-EPOCH stale epoch` | Node has higher epoch (abort failover) |
| `-REPLICATING still receiving WAL` | Retry after WAL flush |

**FROGDB.DEMOTE**

Sent to old primary (if reachable) to demote it:

```
FROGDB.DEMOTE <epoch> <new_primary_node_id>
```

| Response | Meaning |
|----------|---------|
| `+OK` | Demotion accepted |
| `-EPOCH stale epoch` | Old primary already has higher epoch |

**FROGDB.TOPOLOGY**

Broadcast to all nodes with new cluster topology:

```
FROGDB.TOPOLOGY <epoch> <json_topology>
```

All nodes update their slot mappings when receiving higher epoch.

#### Multi-Orchestrator Coordination

When multiple orchestrator instances are deployed for HA:

| Aspect | Specification |
|--------|---------------|
| Leader election | External (e.g., etcd, ZooKeeper) |
| Failover decision | Only leader initiates failover |
| State synchronization | Shared state in coordination service |
| Split-orchestrator | Follower orchestrators reject writes |

**Note:** Multi-orchestrator coordination is delegated to the deployment infrastructure. FrogDB nodes accept commands from any orchestrator but validate epoch numbers to prevent conflicting decisions.

#### Replica State During Failover

When a primary fails, other replicas of that primary:

| Step | Replica Behavior |
|------|------------------|
| 1 | Detect primary connection loss |
| 2 | Enter `REPLICA_CONNECTING` state |
| 3 | Retry connection with exponential backoff |
| 4 | Receive FROGDB.TOPOLOGY with new primary |
| 5 | Connect to new primary with PSYNC |
| 6 | Resume replication from new primary |

**Message to replicas:**

```
FROGDB.PRIMARY_CHANGED <new_primary_host> <new_primary_port> <epoch>
```

#### Client Redirect After Failover

Clients learn about new primary through:

| Method | Description |
|--------|-------------|
| `-MOVED` response | First command to old primary returns redirect |
| Topology refresh | Client requests CLUSTER SLOTS periodically |
| Connection failure | Client reconnects and discovers new primary |

**Recommended client behavior:**

```
1. Send command to cached primary
2. If -MOVED: update cache, retry on new node
3. If connection error: request CLUSTER SLOTS, update cache
4. Implement exponential backoff for reconnection
```

**In-Flight Commands During Failover:**

| Command State | Behavior |
|---------------|----------|
| Sent to old primary, no response | Client retries (may go to new primary) |
| Response received before failover | Command succeeded on old primary |
| Old primary returns -READONLY | Client retries on new primary |

### Split-Brain Prevention

**Architecture:** Orchestrated topology prevents classic split-brain (no gossip voting).

**Partition Scenarios:**

| Partition | Behavior |
|-----------|----------|
| Orchestrator ↔ Primary | Orchestrator may trigger failover after timeout; primary continues serving until demoted |
| Orchestrator ↔ Replica | Replica continues replicating; not considered for failover |
| Primary ↔ Replica | Replica falls behind; PSYNC will catch up or trigger full sync |
| Primary ↔ Clients (some) | Affected clients timeout; others continue |
| Node ↔ All | Node isolated; orchestrator promotes replica if primary |

**Fencing Mechanism:**

When a primary is demoted (or recovers after partition):
1. Orchestrator pushes topology with higher epoch
2. Node compares `received_epoch > local_epoch`
3. If true: Node transitions to replica role, connects to new primary
4. Writes during partition window are rejected after demotion

**Fencing Limitations:**
- Requires network connectivity to orchestrator
- Brief window where old primary may accept writes before receiving new topology
- **Recommendation:** Use `min_replicas_to_write` to require replica acknowledgment

### Fencing Failure Scenarios

**Permanent Partition:**
If old primary cannot reach orchestrator AND clients can still reach it:

| Duration | Old Primary Behavior | Risk |
|----------|---------------------|------|
| < `self_fence_timeout_ms` | Continues serving | Data divergence |
| >= `self_fence_timeout_ms` | Self-demotes to read-only | Limited divergence |

**Self-Fencing Configuration:**
```toml
[cluster]
# If orchestrator unreachable for this long, self-demote
self_fence_timeout_ms = 30000

# Behavior when self-fenced
self_fence_mode = "readonly"  # "readonly" or "reject_all"
```

**Self-Fencing Flow:**
1. Primary loses orchestrator connection
2. Timer starts: `self_fence_timeout_ms`
3. If timeout expires before reconnection:
   - Log: "Self-fencing: orchestrator unreachable"
   - Transition to `self_fence_mode`
   - Continue serving reads (if readonly) or reject all (if reject_all)
4. On orchestrator reconnection: receive topology, act accordingly

**Self-Fencing Edge Cases:**

| Scenario | Behavior |
|----------|----------|
| Orchestrator returns during self-fence | Receive topology, transition to appropriate role |
| Orchestrator returns, no failover occurred | Resume as primary (epoch unchanged) |
| Orchestrator returns, failover occurred | Become replica of new primary |
| Self-fenced primary still connected to replicas | **Stops streaming** - replicas see disconnect, wait |
| Replica sees primary self-fence | Replica remains connected, waits for orchestrator decision |

**Replication During Self-Fence:**

When a primary self-fences:
1. **Stops accepting writes:** Returns `-READONLY` or closes connections
2. **Stops streaming to replicas:** No new WAL entries sent
3. **Keeps replica connections open:** Replicas remain connected but idle
4. **Replicas detect stall:** No data for `repl_timeout_ms` → log warning, wait

```
Primary (self-fenced)              Replica
   │                                  │
   │  [self-fence triggered]          │
   │  Stop accepting writes           │
   │  Stop WAL streaming              │
   │                                  │
   │        (silence)                 │
   │                                  │  Detect no data for repl_timeout_ms
   │                                  │  Log: "Primary appears stalled"
   │                                  │  Continue waiting (don't disconnect)
   │                                  │
   │  [orchestrator returns]          │
   │  Receive topology                │
   │                                  │
```

**Why replicas don't disconnect:** Self-fence is a temporary safety state. If replica disconnected, it might trigger unnecessary FULLRESYNC when primary resumes.

**Transition Back to Normal:**

| Topology Received | Primary's New Role | Actions |
|-------------------|-------------------|---------|
| Same epoch, still primary | Primary | Clear self-fence, resume writes and replication |
| Higher epoch, still primary | Primary | Update epoch, resume (rare - orchestrator rebooted) |
| Higher epoch, now replica | Replica | Connect to new primary, PSYNC |
| Higher epoch, no longer in cluster | Shutdown | Node removed from cluster |

**Metrics:**
```
frogdb_self_fence_active                  # 1 if currently self-fenced
frogdb_self_fence_events_total            # Times self-fence triggered
frogdb_self_fence_duration_ms        # Time spent in self-fence state
frogdb_self_fence_resumed_as_primary      # Resumed as primary after self-fence
frogdb_self_fence_demoted_to_replica      # Became replica after self-fence
```

**Client-Side Epoch Validation (Defense in Depth):**
Clients can validate responses include expected epoch:
```
CONFIG SET cluster_epoch_check ON
SET key value
+OK epoch=42
```
Client rejects response if epoch < last_known_epoch.

**Configuration:**
```toml
[cluster]
# Require N replicas to acknowledge before write succeeds
# 0 = disabled (async replication, potential data loss)
# 1+ = synchronous to N replicas (higher durability, higher latency)
min_replicas_to_write = 0

# Time before orchestrator considers node unreachable
node_timeout_ms = 15000

# Additional time to wait before triggering failover (debounce)
failover_timeout_ms = 5000

# Time for old primary to receive demotion after failover decision
fencing_timeout_ms = 10000
```

**Fencing Timeline (Worst Case):**

```
T=0:     Primary loses connection to orchestrator
T=15s:   Orchestrator marks primary as unreachable (node_timeout_ms)
T=20s:   Orchestrator initiates failover (failover_timeout_ms)
T=21s:   Replica promoted, receives new epoch
T=30s:   Old primary receives demotion (fencing_timeout_ms)

SPLIT-BRAIN WINDOW: T=21s to T=30s (up to 9 seconds)
```

During the split-brain window:
- Old primary continues accepting writes (no epoch update yet)
- New primary also accepting writes
- Data divergence possible

**Reducing Split-Brain Window:**

| Approach | Configuration | Trade-off |
|----------|---------------|-----------|
| Lower fencing timeout | `fencing_timeout_ms = 5000` | Faster convergence, more aggressive |
| Synchronous replication | `min_replicas_to_write = 1` | No data loss, higher latency |
| Client-side validation | Check epoch on response | Application complexity |

### Split-Brain Data Recovery

When an old primary receives a demotion topology after accepting writes during split-brain:

1. **Compare sequence numbers:** old_primary_seq vs new_primary_seq
2. **If diverged:** Log all operations with seq > last_replicated_seq to `data/split_brain_discarded.log`
3. **Discard divergent data:** Roll back to last_replicated_seq
4. **Connect as replica:** Begin replicating from new primary

**Split-Brain Log Format:**
```
timestamp=2024-01-15T10:30:45Z old_primary=node-abc seq_start=12345 seq_end=12400 ops_lost=55
[MSET key1 value1 key2 value2]
[INCR counter]
...
```

**Manual Recovery:** Operators can replay `split_brain_discarded.log` if business logic permits.

**Metrics:**
- `frogdb_split_brain_events_total`: Counter of split-brain detections
- `frogdb_split_brain_ops_discarded_total`: Counter of discarded operations

### Partition Healing Sequence

When a network partition heals, the cluster must reconcile state. The sequence depends on what happened during the partition:

**Scenario A: No Failover Occurred (Orchestrator Didn't Promote)**

```
T=0:     Partition occurs
T=?:     Orchestrator couldn't reach primary, but no failover (below threshold)
T=heal:  Partition heals

Sequence:
1. Orchestrator resumes health checks → primary healthy
2. No topology change needed
3. Replicas reconnect to primary via PSYNC
4. Normal operation resumes
```

**Scenario B: Failover Occurred, Old Primary Isolated**

```
T=0:     Partition occurs (old primary isolated)
T=21s:   Failover - replica promoted to new primary
T=heal:  Partition heals, old primary can reach orchestrator

Sequence:
1. Old primary receives topology update (higher epoch)
2. Old primary detects divergent writes (if any)
3. Divergent writes logged to split_brain_discarded.log
4. Old primary rolls back to last_replicated_seq
5. Old primary connects as replica to new primary
6. PSYNC or FULLRESYNC based on WAL retention
7. Old primary becomes healthy replica
```

**Scenario C: Orchestrator Was Partitioned From All Nodes**

```
T=0:     Orchestrator loses connectivity to all nodes
T=?:     Nodes continue serving (existing topology)
T=heal:  Orchestrator reconnects

Sequence:
1. Orchestrator pushes current topology to all nodes
2. Nodes compare epochs - should match (no changes during partition)
3. If node self-fenced during partition, clears self-fence
4. Normal operation resumes
```

**Client Topology Invalidation:**

Clients cache cluster topology. After partition healing:

| Client Cache State | Behavior |
|--------------------|----------|
| Matches new topology | Normal operation |
| Points to old primary | Receives `-MOVED`, updates cache |
| Missing new nodes | Discovers on `-MOVED` or periodic refresh |

**Recommended Client Behavior:**
```
on_connection_restored():
    send CLUSTER SLOTS  # Refresh topology
    clear_local_slot_cache()
    rebuild_slot_cache_from_response()
```

**Metrics:**
```
frogdb_partition_heal_events_total        # Partition healing detected
frogdb_partition_heal_no_failover         # Healed without failover
frogdb_partition_heal_with_failover       # Healed after failover occurred
frogdb_partition_heal_duration_ms    # Time partition lasted
```

### Cascade Failure Handling

Complex failure scenarios where multiple components fail simultaneously or in sequence:

**Scenario 1: Multiple Replicas Fail Simultaneously**

| Remaining Replicas | Behavior |
|--------------------|----------|
| >= `min_replicas_to_write` | Writes continue (sync mode meets quorum) |
| < `min_replicas_to_write` | Writes fail with `-NOREPL` until replicas recover |
| 0 replicas (async mode) | Primary continues serving (data loss risk on primary failure) |

**Scenario 2: Primary Fails During Full Resync**

When primary fails while a replica is in FULLRESYNC state:

1. **Replica aborts FULLRESYNC:** Partial checkpoint discarded
2. **Failover proceeds:** Another replica (if available) promoted
3. **Aborted replica:** Must FULLRESYNC from new primary after failover
4. **If no other replicas:** Cluster unavailable until primary recovers or manual intervention

```
Primary                    Replica A (syncing)         Replica B (caught up)
   │                            │                           │
   │── FULLRESYNC checkpoint ──▶│                           │
   │         (in progress)      │                           │
   │ ✕ PRIMARY FAILS            │                           │
   │                            │                           │
   │                            │ Abort, discard partial    │
   │                            │ checkpoint                │
   │                            │                           │
Orchestrator promotes Replica B (has data)
   │                            │                           │
   │                            │◀── FULLRESYNC from B ─────│
```

**Scenario 3: Orchestrator Unreachable During Failover**

If orchestrator becomes unreachable mid-failover:

| Failover Stage | State | Recovery |
|----------------|-------|----------|
| Before replica selected | No promotion | Wait for orchestrator recovery |
| After ROLE PRIMARY sent | New primary active, old topology persists | Orchestrator reconnects, reconciles |
| After topology pushed | Normal operation | None needed |

**Key Risk:** If orchestrator fails between promoting replica and pushing topology, some nodes may still route to old primary. Recovery requires orchestrator to come back and push updated topology.

**Scenario 4: Old Primary Recovers But Can't Reach New Primary**

When old primary recovers after partition but can't reach new primary:

1. **Receives demotion topology:** Knows it's no longer primary
2. **Attempts to connect to new primary:** For PSYNC
3. **Connection fails:** Remains in `REPLICA_CONNECTING` state
4. **Retries with backoff:** `repl_reconnect_base_ms * 2^attempt`
5. **After max retries:** Alerts operator, remains disconnected

```toml
[replication]
repl_reconnect_max_attempts = 10  # Max reconnection attempts before alerting
repl_reconnect_alert_threshold = 5  # Alert after N failures (continues retrying)
```

**Metrics for Cascade Failures:**
- `frogdb_failover_cascade_events_total`: Failures during active failover
- `frogdb_replica_sync_aborted_total{reason="primary_failed"}`: Syncs interrupted by primary failure
- `frogdb_orchestrator_unreachable_during_failover_total`: Orchestrator failures mid-failover

### In-Flight Commands During Failover

When a primary fails, commands in various stages are affected:

| Command State | Outcome |
|---------------|---------|
| **In TCP buffer (client→server)** | Lost. Client times out, should retry. |
| **In shard message queue** | Lost. Server never processes. Client times out. |
| **Mid-execution** | Lost. Result never sent. Client times out. |
| **Executed, in response buffer** | Lost. Client times out despite server success. |
| **Acknowledged to client** | Safe (client received response). |

**Client Behavior:**
```
Client                    Primary                    (failover)
   │                         │                           │
   │── SET key value ───────▶│                           │
   │                         │ (queued)                  │
   │                         │ ✕ PRIMARY FAILS           │
   │                         │                           │
   │  [timeout: no response] │                           │
   │                         │                           │
   │── retry to new primary ─────────────────────────────▶│
   │◀── +OK ──────────────────────────────────────────────│
```

**Idempotency Requirement:** Clients should use idempotent operations or implement deduplication for non-idempotent commands (e.g., INCR) when retrying after failover.

**Monitoring:**
- `frogdb_failover_commands_lost` - Estimated commands in flight during failover
- `frogdb_client_retry_total` - Client-reported retries (if client library supports)

### Client State and Failover

When a client's connection to primary is severed during failover:

| State | Outcome | Client Recovery |
|-------|---------|-----------------|
| **MULTI (queued commands)** | Lost - never executed | Re-send MULTI and commands |
| **WATCH** | Lost - keys unwatched | Re-WATCH before transaction |
| **SUBSCRIBE** | Lost - unsubscribed | Re-SUBSCRIBE on new connection |
| **SSUBSCRIBE** | Lost + may need redirect | Re-SSUBSCRIBE, handle -MOVED |
| **Blocking (BLPOP)** | Lost - timeout on client | Re-issue blocking command |
| **CLIENT REPLY OFF** | Reset to ON | Re-configure if needed |

**Client Recommendations:**

1. **Transaction recovery:** Always wrap MULTI/EXEC in retry loop
2. **Watch recovery:** Re-WATCH and re-read values after reconnect
3. **Pub/Sub recovery:** Implement resubscription logic in client
4. **Idempotency:** Design commands to be safely retriable

**Example Transaction Retry Pattern:**
```python
def execute_with_retry(client, transaction_fn, max_retries=3):
    for attempt in range(max_retries):
        try:
            return transaction_fn(client)
        except (ConnectionError, MovedError) as e:
            client.refresh_cluster_topology()
            if attempt == max_retries - 1:
                raise
            time.sleep(0.1 * (2 ** attempt))  # Exponential backoff
```

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

## Pub/Sub in Cluster Mode

### Broadcast Pub/Sub (SUBSCRIBE/PUBLISH)

In cluster mode, PUBLISH broadcasts to all nodes:

```
Client A (Node 1)           Node 1              Node 2              Node 3
      │                        │                   │                   │
      │── PUBLISH chan msg ──▶│                   │                   │
      │                        │                   │                   │
      │                        │── Forward ───────▶│                   │
      │                        │── Forward ────────────────────────────▶│
      │                        │                   │                   │
      │                        │ Deliver to local  │ Deliver to local  │ Deliver to local
      │                        │ subscribers       │ subscribers       │ subscribers
```

**Cost:** O(nodes) for each PUBLISH

### Sharded Pub/Sub (SSUBSCRIBE/SPUBLISH)

Sharded pub/sub routes by channel name (Redis 7.0+ compatible):

```
Channel "mychan" → slot = CRC16("mychan") % 16384 → Node owning slot
```

**Cost:** O(1) - only the owning node handles the channel

### Sharded Pub/Sub During Slot Migration

When a slot migrates, sharded pub/sub subscriptions are affected:

```
Client                    Source (A)                 Target (B)
   │                          │                          │
   │ SSUBSCRIBE mychan        │                          │
   │─────────────────────────▶│                          │
   │ (subscribed to mychan)   │                          │
   │                          │                          │
   │                          │  [Slot migration starts] │
   │                          │                          │
   │                          │  [Slot migration ends]   │
   │                          │                          │
   │    Server unsubscribes   │                          │
   │◀── -MOVED 1234 B:6379 ───│                          │
   │                          │                          │
   │ Client must resubscribe  │                          │
   │── SSUBSCRIBE mychan ─────────────────────────────────▶│
   │◀── (subscribed) ──────────────────────────────────────│
```

**Server-Side Behavior (matches Redis 7.0+):**
1. When slot migration completes, source node iterates sharded subscriptions
2. For each subscription to a channel in the migrated slot:
   - Server sends `-MOVED slot target:port` to the subscribed client
   - Server removes the subscription from its local state
3. Client must reconnect to target node and resubscribe

**Client Responsibility:**
- Handle `-MOVED` responses in subscription context
- Reconnect to new node and issue `SSUBSCRIBE` again
- Messages published during migration window may be lost

See [PUBSUB.md](PUBSUB.md#cluster-integration) for complete pub/sub cluster behavior.

### Pattern Subscriptions

- PSUBSCRIBE patterns work within broadcast mode only
- Each node evaluates patterns locally
- No pattern support in sharded pub/sub

### Cluster Pub/Sub Commands

| Command | Scope | Behavior |
|---------|-------|----------|
| SUBSCRIBE | Broadcast | Fan-out to all nodes |
| PSUBSCRIBE | Broadcast | Pattern matching, all nodes |
| PUBLISH | Broadcast | Forward to all nodes |
| SSUBSCRIBE | Sharded | Route to slot owner |
| SPUBLISH | Sharded | Route to slot owner |

### Abstraction

The `ClusterPubSubForwarder` trait abstracts cluster pub/sub forwarding, allowing single-node
deployments to use a no-op implementation. See [PUBSUB.md](PUBSUB.md#cluster-mode) for the
full interface definition and message flow diagrams.

---

## ACL in Cluster Mode

### ACL Distribution

ACLs are managed per-node. For consistent authentication across a cluster:

**Recommended Approach:** Orchestrator distributes ACL configuration:

```
Orchestrator
      │
      │── POST /admin/acl ──▶ Node 1
      │── POST /admin/acl ──▶ Node 2
      │── POST /admin/acl ──▶ Node 3
      │
      │  (All nodes receive identical ACL config)
```

### ACL Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/acl` | POST | Receive ACL configuration update |
| `/admin/acl` | GET | Return current ACL configuration |

### Consistency Model

ACL updates are **eventually consistent** across the cluster:
- Orchestrator pushes updates to all nodes
- Nodes apply updates independently
- Brief window where nodes may have different ACL states

**Recommendation:** Update ACL during low-traffic periods or use rolling updates.

### ACL Configuration Format

```json
{
  "version": 1,
  "users": [
    {
      "name": "default",
      "enabled": true,
      "passwords": ["sha256:..."],
      "permissions": {
        "commands": ["+@all"],
        "keys": ["*"],
        "channels": ["*"]
      }
    },
    {
      "name": "readonly",
      "enabled": true,
      "passwords": ["sha256:..."],
      "permissions": {
        "commands": ["+@read", "-@write"],
        "keys": ["prefix:*"],
        "channels": []
      }
    }
  ]
}
```

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
| `frogdb_replication_lag_ms` | Gauge | Lag behind primary |
| `frogdb_replication_lag_bytes` | Gauge | Lag in bytes behind primary |
| `frogdb_connected_replicas` | Gauge | Number of connected replicas (primary only) |
| `frogdb_sync_full_count` | Counter | Full syncs performed |
| `frogdb_sync_partial_ok` | Counter | Successful partial syncs |
| `frogdb_sync_partial_err` | Counter | Failed partial syncs (triggered full) |

### Replication Lag Measurement

**Calculation (matching Redis):**

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `lag_ms` | `now - last_ack_time` | Milliseconds since last REPLCONF ACK from replica |
| `lag_bytes` | `primary_offset - replica_offset` | Bytes behind primary |

Replicas send `REPLCONF ACK <offset>` every `repl_ping_interval_ms` (default: 1000ms).

**Why time-since-ACK?**
- Simple and reliable (no throughput estimation needed)
- Matches Redis behavior (though exposed as milliseconds for consistency)
- Works correctly even when write throughput is zero

**Lag Visibility:**

On primary (via INFO replication):
```
# Replication
role:master
connected_slaves:2
slave0:ip=10.0.0.2,port=6379,state=online,offset=12345678,lag=0
slave1:ip=10.0.0.3,port=6379,state=online,offset=12345600,lag=1
```

On replica:
```
# Replication
role:slave
master_link_status:up
master_last_io_seconds_ago:0
master_sync_in_progress:0
slave_repl_offset:12345678
slave_read_repl_offset:12345678
master_repl_offset:12345700
```

**Alerting Thresholds:**

| Threshold | Status | Action |
|-----------|--------|--------|
| < 1 second | Healthy | Normal operation |
| 1-5 seconds | Elevated | Monitor closely |
| 5-30 seconds | Warning | Investigate primary load or network |
| > 30 seconds | Critical | Risk of data loss on failover |

**Data Loss Bound:**

On failover, maximum data loss = replication lag at time of failure.
With `lag_ms = 5000`, up to 5 seconds of writes may be lost.

**Reducing Lag:**
- Ensure sufficient network bandwidth
- Monitor primary CPU and disk I/O
- Consider dedicated replication network
- Use synchronous replication (`WAIT` command) for critical writes

---

## Future Enhancements

| Feature | Description |
|---------|-------------|
| **Gossip Protocol Option** | Self-managing cluster without external orchestrator |
| **Cross-Datacenter** | Multi-region replication with latency-aware routing |
| **Auto-Rebalancing** | Automatic slot redistribution on scale events |
| **Read Replicas** | Non-failover replicas for read scaling |

### WAIT Command (Planned)

```
WAIT numreplicas timeout
```

Block until write propagated to N replicas or timeout.

| Behavior | Description |
|----------|-------------|
| Returns | Number of replicas that acknowledged |
| Timeout 0 | Block forever |
| numreplicas = 0 | Return immediately with current ack count |

**Example:**
```
SET mykey myvalue
WAIT 1 5000
:1
```

### WAIT vs min_replicas_to_write

These mechanisms are **complementary, not overlapping**:

| Mechanism | When Applied | Purpose |
|-----------|--------------|---------|
| `min_replicas_to_write` | **Before** write | Gate: reject writes if insufficient replicas connected |
| `WAIT` | **After** write | Confirm: block until write replicated to N replicas |

**Interaction:**
- `min_replicas_to_write` checks replica *connectivity* (based on ping lag, pre-write check)
- `WAIT` checks *replication progress* (specific offset acknowledged, post-write)
- Both can be used together for defense-in-depth
- `WAIT` can request more replicas than `min_replicas_to_write` requires

**Example - Combined Usage:**
```toml
[cluster]
min_replicas_to_write = 1  # Ensure at least 1 replica is connected
```
```
SET user:1 data
WAIT 2 5000  # Wait for 2 replicas to acknowledge this specific write
:2
```

This enables applications to selectively wait for stronger replication on critical writes while keeping general writes performant.

---

## References

- [Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Valkey Cluster Tutorial](https://valkey.io/topics/cluster-tutorial/)
- [Valkey Replication](https://valkey.io/topics/replication/)
- [Valkey Atomic Slot Migration](https://valkey.io/blog/atomic-slot-migration/)
- [DragonflyDB Cluster Mode](https://www.dragonflydb.io/docs/managing-dragonfly/cluster-mode)
- [RocksDB Replication Helpers](https://github.com/facebook/rocksdb/wiki/Replication-Helpers)
- [Pinterest Rocksplicator](https://github.com/pinterest/rocksplicator)
