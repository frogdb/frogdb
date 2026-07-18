---
title: "Clustering Internals"
description: "Contributor-facing documentation for FrogDB's clustering architecture: slot routing internals, the Raft metadata plane, Config Epoch versioning, and the failover algorithm."
sidebar:
  order: 8
---
Contributor-facing documentation for FrogDB's clustering architecture: slot routing internals, the Raft metadata plane, Config Epoch versioning, and the failover algorithm.

For operator-facing setup and configuration, see [Operations: Clustering](/operations/clustering/).

For the canonical vocabulary, see the [glossary](/architecture/glossary/).

---

## Architecture

FrogDB supports both single-node and cluster operation. The architecture uses:

- **A Raft metadata plane** (embedded openraft) rather than gossip or an external control-plane
  service
- **16384 hash slots** for Redis Cluster client compatibility
- **Full dataset replication** -- replicas copy all data from their primary
- **PSYNC + WAL streaming** for incremental replication

Cluster metadata (topology, slot ownership, node roles, Config Epoch) is self-coordinated by the
nodes themselves through Raft consensus. The data path never goes through Raft: reads, writes,
replication, and slot key-transfer all bypass it. This is the decision recorded in ADR 0001
(*Embedded Raft for cluster metadata*, `frogdb-server/docs/adr/`) — the earlier external
"Orchestrator" control-plane design is superseded.

### Terminology

| Term | Definition |
|------|------------|
| **Node** | A FrogDB server instance |
| **Internal Shard** | Thread-per-core partition within a node (N per node) |
| **Hash Slot** | Hash slot 0-16383 (cluster distribution unit) |
| **Primary** | Node owning slots for writes |
| **Replica** | Node replicating from a primary |
| **Raft Metadata Plane** | The embedded openraft consensus among nodes that owns cluster metadata |
| **Config Epoch** | Monotonic cluster-topology version counter (`ConfigEpoch`, a `u64`) |

### Two-Level Sharding

Keys are partitioned twice: once across nodes (hash slots) and again across the
ShardWorker threads within a node (internal shards).

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
|  internal_shard = slot % num_shards                      |
+---------------------------------------------------------+
```

**Routing** (`frogdb-core`, `shard/partition.rs`):
1. **Cluster routing:** `slot = CRC16(key) % 16384` -> determines which node owns the key
2. **Internal routing:** `internal_shard = slot % num_shards` -> determines which ShardWorker
   thread within the node

Both steps use the XMODEM CRC16 variant, so slot assignment is byte-for-byte compatible with Redis
Cluster. Because the internal shard is derived from the slot (`slot % num_shards`), the strict
slot check strictly implies the shard check: keys that share a slot always share an internal shard.

### Hash Tag Full Colocation

Hash tags guarantee colocation at **both** levels: the `{tag}` substring is hashed instead of the
whole key, so both the slot and the internal shard are computed from the tag alone.

```rust
// frogdb-core: shard/partition.rs
fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % 16384
}

fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % 16384;
    slot % num_shards
}
```

---

## Raft Metadata Plane

Cluster metadata has a single consistent owner: an embedded Raft group formed by the cluster's
nodes. There is no external service to deploy — the nodes self-coordinate. Metadata changes get
real consensus rather than gossip convergence.

### What Raft Owns (Metadata Only)

The replicated state machine (`ClusterStateMachine`, `frogdb-cluster`) holds exactly:

| Metadata (replicated via Raft) | NOT via Raft (data plane) |
|--------------------------------|---------------------------|
| Cluster membership (`nodes`) | Key-value reads and writes |
| Slot ownership (`slot_assignment`) | PSYNC / WAL replication streaming |
| Node roles (Primary / Replica) | Slot key-transfer during migration |
| Config Epoch | |
| Active slot migrations (`migrations`) | |

### Design Rationale

| Aspect | Raft metadata plane (FrogDB) | Gossip (Redis Cluster) |
|--------|------------------------------|------------------------|
| Topology source | Node consensus (Raft log) | Node consensus (gossip) |
| Node discovery | Nodes join the Raft group | Nodes discover each other |
| Failure detection | Leader-only probing (see [Failover](#failover)) | Nodes vote on failures |
| Topology changes | Linearizable, committed by quorum | Eventually consistent |
| Debugging | Explicit replicated state machine | Derived from gossip |

Cluster mode requires an **odd member count (≥3)** for quorum; the operator enforces this and
derives the PodDisruptionBudget `minAvailable` from it (see ADR 0001).

### How a Topology Change Propagates

Metadata mutations are expressed as `ClusterCommand` values (`AddNode`, `RemoveNode`,
`AssignSlots`, `SetRole`, `IncrementEpoch`, `Failover`, `BeginSlotMigration`,
`CompleteSlotMigration`, ...). They flow through Raft, not an external push:

1. The Raft **leader** receives the request and calls `raft.client_write(cmd)`. Only the leader
   can propose; a non-leader returns `NotLeader`.
2. Raft replicates the command as a log entry to the followers and commits it once a quorum has
   persisted it.
3. Every node applies the committed entry through the same validated path
   (`ClusterState::apply_command`), so all replicas converge on identical metadata. Bootstrap
   seeding on a fresh node reuses this exact path via `apply_local`, enforcing the same invariants
   (node-exists, slot-already-assigned, bounds) that Raft-applied commands do.
4. Applying certain commands emits local side-effect events — e.g. a node demoted to Replica
   receives a `DemotionEvent`, and a finished migration fires a `SlotMigrationCompleteEvent` on
   every node.

### Config Epoch

The **Config Epoch** (`ConfigEpoch`, a monotonic `u64`) versions cluster topology. Higher epochs
win conflict resolution. Two levels track it:

- A single cluster-wide `config_epoch` in the replicated state, bumped by `IncrementEpoch` and by
  the composite `Failover` / `MarkNodeFailed` transitions.
- A per-node `config_epoch` recorded in `NodeInfo`, reported in the `CLUSTER NODES` line.

Topology-visibility changes bump the Config Epoch **inside the same state-machine transition** that
makes the change, so peers can never observe new slot ownership or a `FAIL` flag at a stale Config
Epoch. During failover the promoted successor claims the new Config Epoch, mirroring Redis, where a
promoted replica claims a new `configEpoch` and the slot bitmap and epoch propagate together in one
cluster message.

Nodes detect that their local view is stale by:

| Detection method | Description |
|------------------|-------------|
| Raft log apply | A committed entry advances the local state machine |
| Client redirect | A `-MOVED` reply names the current owner of a slot |
| Replication handshake | The primary's Config Epoch travels with the replication stream |

---

## Slot Ownership Routing

Before executing a keyed command, the node routes it against the current slot assignment and any
active migration. The decision logic lives in `frogdb-server`
(`slot_migration/routing.rs`, `RouteDecision`):

```rust
enum RouteDecision {
    LocalServe,                                   // we own the slot, no migration
    LocalServeMigrating,                          // we own it and are the migration source
    AcceptImporting,                              // another owns it; we are the target + ASKING
    Moved { slot: u16, owner: NodeId, addr: Option<SocketAddr> },
    Unassigned { slot: u16 },                     // no owner
}
```

| Decision | Client-visible action |
|----------|-----------------------|
| `LocalServe` | Execute locally |
| `LocalServeMigrating` | Execute locally; a nil reply is converted to `-ASK <slot> <target>` |
| `AcceptImporting` | Execute locally (target has `ASKING`, or the command is `RESTORE`) |
| `Moved` | Return `-MOVED <slot> <host>:<port>` (or `-CLUSTERDOWN` if the owner's address is unknown); a READONLY replica may instead serve a read |
| `Unassigned` | Return `-CLUSTERDOWN` |

Multi-key commands whose keys span more than one hash slot are rejected up front with `-CROSSSLOT`
by `SlotValidator::same_slot`, before routing — matching Redis Cluster. (The same validator's
`same_shard` check enforces the internal-shard colocation rule in standalone mode; because
`shard == slot % num_shards`, the slot check is strictly stronger.)

**Validation timing in the pipeline:**
Parse -> Lookup -> Arity -> Extract keys -> CROSSSLOT check -> Slot routing -> Execute

---

## Slot Migration

A migration is a **two-point ownership swap** recorded in the replicated `migrations` map
(`SlotMigration { slot, source_node, target_node }`); there is no intermediate migration state
machine:

1. `BeginSlotMigration` inserts the record, marking the slot "migrating". Both
   `CLUSTER SETSLOT ... MIGRATING` (on the source) and `... IMPORTING` (on the target) lower to
   this single Raft command.
2. Keys transfer source -> target over the data path (the `MIGRATE` protocol), **not** through
   Raft.
3. `CompleteSlotMigration` flips `slot_assignment` to the target and removes the record;
   `CancelSlotMigration` removes it without transferring ownership.

While a migration is active, a node derives its role for that slot by comparing its own id against
the record: the `source_node` behaves as MIGRATING (`LocalServeMigrating` above), the
`target_node` behaves as IMPORTING (`AcceptImporting`). `RESTORE` is accepted on the importing
target even without `ASKING`.

---

## Failover

Failure detection is **leader-only** (`frogdb-server`, `failure_detector.rs`), like
CockroachDB/FoundationDB rather than peer-to-peer gossip. The Raft leader probes each node with a
TCP connect; after `fail_threshold` consecutive failures it proposes `MarkNodeFailed` through Raft
(which sets the `FAIL` flag and bumps the Config Epoch atomically). Only the leader can write
metadata, so centralizing detection there costs O(N) rather than O(N²) probes.

When automatic failover is enabled (`auto_failover`), the leader then selects a successor among the
failed primary's replicas by lag-based scoring and proposes a single composite `Failover` command:

```rust
enum ClusterCommand {
    // ...
    Failover {
        old_primary_id: NodeId,
        new_primary_id: NodeId,
        force: bool,  // true: remove old primary; false: demote it to a replica
    },
}
```

`Failover` is applied **all-or-nothing** in one replicated transition:

1. Transfer every slot owned by `old_primary_id` to `new_primary_id`.
2. Promote the successor to Primary.
3. Apply the old primary's fate: `force` removes it from the cluster (presumed dead); graceful
   (`force = false`) demotes it to a Replica of the successor.
4. Re-parent the old primary's remaining replicas onto the successor.
5. Bump the Config Epoch and have the successor claim it.

Doing this as one Raft entry means there is never a window where the failed primary's slots are
ownerless — either the whole topology change commits or none of it does. (Auto-failover retries the
`client_write` a few times to survive transient leadership churn, since the `FAIL` latch would
otherwise not re-trigger it.)

A node that finds itself demoted (via `SetRole { role: Replica }` or a graceful `Failover`)
receives a `DemotionEvent` carrying the new primary's id, and switches to streaming from the new
primary via PSYNC.

---

## Node-to-Node Communication

Nodes do NOT gossip topology. They connect directly for:

| Purpose | Direction | Transport |
|---------|-----------|-----------|
| Raft consensus (metadata) | Leader <-> members | Cluster bus (`cluster_addr`) |
| Failure detection | Leader -> members | TCP connect probe |
| Replication | Replica -> Primary | PSYNC + WAL stream |
| Slot key-transfer | Source -> Target | `MIGRATE` protocol |

Only the first row is Raft; the rest are data-path or health-probe connections that never pass
through consensus. All topology knowledge comes from the replicated Raft state machine, not from a
peer's gossip or an external control plane.

---

## HTTP Endpoints

Each node exposes HTTP endpoints (bind address from the `[metrics]` / `[http]` config) used by the
operator and by Kubernetes probes. These are health and status surfaces — topology is **not**
pushed over HTTP; it is owned by the Raft metadata plane.

| Endpoint | Purpose |
|----------|---------|
| `/health/live`, `/healthz` | Liveness probe |
| `/health/ready` | Readiness probe (node ready to serve traffic) |
| `/admin/upgrade-status` | Rolling-upgrade / version-gate status across the cluster |

Admin and debug endpoints can be protected with a bearer token (`[http]` config).

---

## Configuration Homogeneity

All nodes in a cluster should have the same `num-shards` configuration. Internal-shard placement is
computed per-node as `CRC16(key) % 16384 % num_shards`, so different shard counts change which
internal shard a key lands on (colocation within a node is always preserved, since it depends only
on the key's hash tag). During migration between nodes with different shard counts, the source
communicates its shard count and the target redistributes keys to its own internal shards.
