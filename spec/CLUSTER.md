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

### Replica Memory Constraints

**Problem:** Primary dataset may exceed replica memory during full sync.

**Detection:**
- Replica monitors memory usage during checkpoint load
- If `used_memory` exceeds `max_memory * 0.9`, abort sync

**Behavior:**
```
Replica                              Primary
   │                                    │
   │◀──── FULLRESYNC <id> <seq> ───────│
   │                                    │
   │  Loading checkpoint...             │
   │  Memory: 80%... 85%... 90%         │
   │                                    │
   │───── SYNC_ABORTED OOM ───────────▶│
   │                                    │
   │  (Wait, retry with backoff)        │
```

**Recovery Options:**
1. Increase replica `max_memory`
2. Enable eviction on replica (when implemented)
3. Use replica with larger memory
4. Reduce primary dataset size

**Configuration:**
```toml
[replication]
# Abort full sync if memory exceeds this percentage
sync_memory_limit_pct = 90

# Retry sync after this delay (with exponential backoff)
sync_retry_delay_ms = 5000
```

**Metrics:**
- `frogdb_sync_aborted_oom_total`: Counter of OOM-aborted syncs

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

**Client responsibility:** Handle `-MOVED` and `-ASK` redirects, retry failed transactions.

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

**Step 1: Detect Primary Failure**
- Health check fails N consecutive times (default: 3)
- Or primary explicitly reports FAILING state

**Step 2: Select Best Replica**

Selection criteria (in priority order):
1. **Replication lag**: Prefer replica with highest `sequence_number` (least data loss)
2. **Connection stability**: Prefer replica with longest continuous connection to primary
3. **Node priority**: Configurable `replica_priority` (0 = never promote, higher = prefer)
4. **Deterministic tiebreaker**: Lexicographic NodeId comparison

```
replica_score = (max_seq - replica_seq) * 1000 + (now - connected_since).seconds()
# Lower score = better candidate
# replica_priority = 0 excludes from selection
```

**Step 3: Promote Replica**
- Send `ROLE PRIMARY` command to selected replica
- Replica generates new ReplicationId
- Replica stores previous primary's ReplicationId as secondary_id

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

### Manual Failover

Using CLUSTER FAILOVER command:

| Mode | Behavior |
|------|----------|
| Default (graceful) | Wait for replica to catch up, then switch |
| FORCE | Immediate promotion, may lose data |

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
| `frogdb_replication_lag_seconds` | Gauge | Lag behind primary |
| `frogdb_replication_lag_bytes` | Gauge | Lag in bytes behind primary |
| `frogdb_connected_replicas` | Gauge | Number of connected replicas (primary only) |
| `frogdb_sync_full_count` | Counter | Full syncs performed |
| `frogdb_sync_partial_ok` | Counter | Successful partial syncs |
| `frogdb_sync_partial_err` | Counter | Failed partial syncs (triggered full) |

### Replication Lag Measurement

**Calculation:**

```
lag_bytes = primary_offset - replica_offset
lag_seconds = lag_bytes / throughput_bytes_per_second
```

Where:
- `primary_offset`: Current WAL sequence number on primary
- `replica_offset`: Last acknowledged sequence number from replica
- `throughput`: Smoothed moving average of replication throughput

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
With `lag_seconds = 5`, up to 5 seconds of writes may be lost.

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
