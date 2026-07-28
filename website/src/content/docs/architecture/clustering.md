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
- **PSYNC checkpoint transfer + logical command streaming** for the data path (see
  [Replication Internals](/architecture/replication/))

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

**Routing** (`frogdb-server/crates/core/src/shard/partition.rs`):
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
// frogdb-server/crates/core/src/shard/partition.rs
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

The replicated state machine (`ClusterStateMachine`, `frogdb-server/crates/cluster/src/state.rs`) holds exactly:

| Metadata (replicated via Raft) | NOT via Raft (data plane) |
|--------------------------------|---------------------------|
| Cluster membership (`nodes`) | Key-value reads and writes |
| Slot ownership (`slot_assignment`) | PSYNC checkpoint + command-stream replication |
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

### `CLUSTER INFO`'s current epoch folds in the Raft term

`CLUSTER INFO` reports `cluster_current_epoch` as `max(config_epoch, raft_term)`, where
`config_epoch` is the cluster-wide counter above and `raft_term` is the local Raft leadership term
(`commands/cluster/mod.rs`, `fold_current_epoch`). Every Raft election bumps the term, whether or
not it changes cluster topology, so a leader re-election with no `Failover`/`MarkNodeFailed`/
`IncrementEpoch` committed can push `cluster_current_epoch` strictly above every per-node
`config_epoch` reported by `CLUSTER NODES`.

This is a deliberate **divergence from Redis**: Redis's `currentEpoch` is agreed by gossip and
bumped only by epoch-owning commands (`CLUSTER BUMPEPOCH`, failover votes, `CLUSTER
SET-CONFIG-EPOCH`), never by a leader-election mechanism, because Redis Cluster has none. FrogDB's
consensus plane does have one, and folding its term into the reported epoch means a stale reader
can tell "the cluster's control plane has moved on" (a new Raft term) even when no slot ownership
changed — at the cost of `cluster_current_epoch` no longer being a pure count of committed
topology events.

The relationship this guarantees, and the one regression tests pin
(`frogdb-server/crates/server/tests/integration_cluster.rs`,
`test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change` and
`test_cluster_info_epoch_monotonic_across_failover`), is `cluster_current_epoch >= max(per-node
config_epoch)` — never the reverse. A node's own `config_epoch` is only ever set to a value it
claims from the same counter `cluster_current_epoch` folds in (the `Failover` transition, see
below), so the cluster-wide counter can never trail behind any individual node. Do not assert
`cluster_current_epoch <= max(NODES config_epoch)`: an audit once proposed that bound and it does
not hold — a passing test asserting it would be testing a coincidence, not a guarantee (issue 47).

The fold is lossy in the other direction too, which matters if you are monitoring for topology
changes. `cluster_current_epoch` is monotonic (both inputs to the `max` only ever move forward) but
**not strictly increasing**: a `config_epoch` bump is invisible whenever `raft_term` already
dominates. On a freshly bootstrapped cluster the first election takes `raft_term` to 1 while
`config_epoch` is still 0, so the first `CLUSTER FAILOVER` moves `config_epoch` from 0 to 1 and
leaves `cluster_current_epoch` at 1 before and after. To detect that a topology event happened, read
the raw per-node `config_epoch` from `CLUSTER NODES` rather than `CLUSTER INFO`'s folded value.

### Config Epoch collisions

Two primaries claiming the same nonzero `config_epoch` is the invariant violation worth watching
for — it is what Redis's `redis-cli --cluster check` flags, and it makes slot-ownership arbitration
undecidable, because the epoch is the tiebreaker. Epoch drift or exceedance (the `CLUSTER INFO` fold
above) is not a collision and is not a bug.

**Prevention.** Every epoch FrogDB mints comes from the single Raft-replicated counter, so
`Failover`, `MarkNodeFailed`, and `IncrementEpoch` cannot produce a duplicate. `AddNode` is the one
transition that carries an epoch the counter did not mint — it takes whatever the joining node
reports — so it is the one transition that can introduce a collision: a node rejoining with restored
on-disk state, or two independently bootstrapped sub-clusters merged with `CLUSTER MEET`. It
resolves the claim before recording the node
(`ClusterStateInner::reconcile_incoming_epoch`, `frogdb-server/crates/cluster/src/state.rs`):

| Incoming claim | Resolution |
|----------------|------------|
| `config_epoch == 0` (unassigned) | Recorded as-is. `NodeInfo::new_primary` starts at 0, so bootstrap seeding and self-registration always claim 0; any number of nodes may hold it. If the node is already known at a nonzero epoch, that epoch is **kept** rather than reset — a reset would free the epoch for another node to claim. |
| Nonzero, held by another primary | Collision. The joining node is admitted with a freshly minted epoch (the cluster-wide counter advanced past every epoch currently claimed); the incumbent keeps the contested one. Logged at `warn` with `claimed_epoch` and `assigned_epoch`. |
| Nonzero, uncontested | Recorded as-is, and the cluster-wide counter is raised to at least that value so it keeps dominating every per-node epoch. |

Only primaries are compared, matching Redis's `clusterHandleConfigEpochCollision`, which returns
early unless both nodes are masters: only a primary's epoch arbitrates slot ownership.

This is a deliberate **divergence from Redis's gossip rule**. Redis breaks the tie by node ID (the
node with the lexicographically smaller ID gives up its epoch and re-claims a fresh one), because
under gossip both nodes learn of the collision independently and need a rule they can each apply
without coordination. FrogDB applies `AddNode` in a linearized state-machine transition, where
"which node was already there" is well defined, so the incumbent keeps the epoch and the joiner
takes the fresh one. The outcome is the same — the two nodes end up at distinct epochs, and no
node's epoch ever decreases — with no ID comparison needed. Every replica derives the same
resolution from the same log entry, so nodes cannot disagree about who ended up with which epoch.

**Detection.** `frogctl cluster check` reads `CLUSTER NODES` and reports every group of primaries
sharing a nonzero `config_epoch`, naming the colliding node IDs and addresses, and exits `1` on any
finding (`0` when everything passes). It is structured as a list of independent checks over the
parsed node table (`frogctl/src/commands/cluster.rs`), with epoch collision as the first; further
checks (slot coverage, open migrations) plug into the same list. Detection is deliberately kept
even though the transition above prevents the collision: it catches a collision introduced by a bug
elsewhere, or by an operator editing on-disk state, which prevention at `AddNode` cannot see.

---

## Slot Ownership Routing

Before executing a keyed command, the node routes it against the current slot assignment and any
active migration. The decision logic lives in
`frogdb-server/crates/server/src/slot_migration/routing.rs` (`RouteDecision`):

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

### MULTI/EXEC: validated twice

A queued transaction is routed at two distinct moments, because a slot can change hands between
`MULTI` and `EXEC`.

1. **Queue time.** Each command is routed individually as it is queued. A `-MOVED` / `-ASK` /
   `-CROSSSLOT` here is returned immediately *and* flags the transaction, so the eventual `EXEC`
   replies `-EXECABORT Transaction discarded because of previous errors.` — the client sent
   something the server refused to accept.
2. **EXEC time.** Before any queued command runs, every queued key is folded into a single slot
   set and routed as one unit against **one** `ClusterState` snapshot
   (`route_queued_batch` in `slot_migration/routing.rs`, called from
   `connection/transaction.rs`). If the batch no longer belongs here, `EXEC` answers with the bare
   redirect — `-MOVED`, `-ASK`, `-TRYAGAIN`, `-CROSSSLOT`, or `-CLUSTERDOWN` — and the queue is
   discarded. Not an array, not `-EXECABORT`.

The batch rules mirror the per-command table above, with three whole-batch refinements:

| Batch state | `EXEC` reply |
|-------------|--------------|
| No queued command touches a key | Executes locally (never redirected) |
| Keys span more than one slot | `-CROSSSLOT` |
| Slot owned locally and MIGRATING, all keys present | Executes locally |
| Slot owned locally and MIGRATING, all keys already migrated | `-ASK <slot> <target>` |
| Slot owned locally and MIGRATING, keys split across the boundary | `-TRYAGAIN` |
| We are the importing target and `ASKING` is set | Executes locally (`-TRYAGAIN` if a multi-key batch is still incomplete) |
| Slot owned elsewhere | `-MOVED` (a READONLY connection may serve it if **every** queued command is read-only) |

Two ordering properties this design depends on:

- **Validation is complete before the first command runs.** The shard executes a transaction as
  one batch whose surviving commands propagate to replicas as a single `MULTI`…`EXEC` frame, so a
  mid-batch abort would replicate a partial transaction. The batch is accepted or refused whole.
- **`ASKING` is sticky for the duration of a `MULTI` block.** Outside a transaction the flag is
  one-shot; inside one it survives every queued command and is consumed by `EXEC`, so the
  importing target reaches the same routing arm at `EXEC` that it reached at queue time.

A residual window remains: the snapshot is read once at `EXEC` entry, so a `CompleteSlotMigration`
that commits through Raft microseconds later is not seen by an already-validated batch. This is the
same window every Redis Cluster node has (its own view of ownership is also asynchronously
updated), and it is bounded by Raft apply latency rather than by the client's think time — which
is the property the double validation exists to establish.

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

Failure detection is **leader-only** (`frogdb-server/crates/server/src/failure_detector.rs`), like
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

### Asymmetric partitions and false positives

Because detection is leader-only, the leader's view of a peer's health is the *only* input to
`MarkNodeFailed`. An asymmetric partition — where a primary is cut off from the leader's TCP probe
but stays reachable to clients and keeps Raft quorum through the remaining nodes — therefore
produces a *false positive*: the leader marks a live, serving primary `FAIL`. This is the
deliberate trade-off of leader-centric detection (avoiding O(N²) gossip), and it is bounded so that
the false positive is benign:

- **No spurious slot movement.** `MarkNodeFailed` only sets the `FAIL` flag and bumps the Config
  Epoch; it does not move slots. Auto-failover moves slots only when the failed primary has a
  replica to promote — with none, it is a no-op even when `auto_failover` is enabled, so the
  wrongly-flagged primary keeps its slots and remains the sole owner cluster-wide.
- **No split-brain from the flagged node.** A `FAIL` flag does not stop the flagged primary from
  serving. What stops unsafe writes is the independent self-fence (`self-fence-on-quorum-loss`,
  default on): a primary rejects writes with `CLUSTERDOWN` once *it* loses quorum. A node that keeps quorum (partitioned
  from the leader only, not from the majority) safely continues to accept writes; a node that loses
  quorum fences itself. Either way, a write the flagged primary accepts is not lost, because no
  failover transferred its slots.
- **Automatic recovery.** The leader keeps probing flagged nodes; the first successful probe
  proposes `MarkNodeRecovered`, clearing the flag.

The dangerous case — a promoted replica and the old primary both serving the same slot — requires
both a replica topology and auto-failover racing the old primary's self-fence; that is a separate,
replica-topology concern. The replica-less false-positive contract above is regression-guarded by
`test_cluster_asymmetric_partition_false_failover`
(`frogdb-server/crates/server/tests/simulation.rs`, deterministic turmoil).

### Slot health accounting in `CLUSTER INFO`

`CLUSTER INFO`'s `cluster_slots_ok`, `cluster_slots_pfail` and `cluster_slots_fail` are derived per
slot from the `FAIL`/`PFAIL` flags of the node that currently owns it
(`commands/cluster/mod.rs`, `count_slot_health`). Every assigned slot lands in exactly one bucket,
with `FAIL` taking precedence over `PFAIL`, so `slots_ok + slots_pfail + slots_fail` always equals
`cluster_slots_assigned`. A slot owned by a `FAIL`-flagged primary is reported as failed, not ok.

This matters because those three fields are the standard health signal that cluster-aware clients
and monitoring read; if `slots_ok` always equalled `slots_assigned`, an operator watching them would
see a healthy cluster while a primary was flagged failed. They now agree with the coarser
`cluster_state` field, which reports `fail` whenever any primary carries the `FAIL` flag or the
local node has lost quorum.

`cluster_slots_pfail` reports 0 in practice today, because nothing sets `PFAIL`. `PFAIL` in Redis
means "one node suspects this peer, but the cluster has not agreed yet" — a distinction that only
exists because detection is gossiped among peers. FrogDB's detection is leader-only and its result
goes through Raft, so a probe failure either has not yet crossed `fail_threshold` (nothing is
reported) or has been committed as `MarkNodeFailed` (`FAIL`); there is no intermediate
single-observer state to report. The bucket is still computed rather than hardcoded, so it would
begin reporting real counts if a suspicion phase were ever introduced.

---

## Node-to-Node Communication

Nodes do NOT gossip topology. They connect directly for:

| Purpose | Direction | Transport |
|---------|-----------|-----------|
| Raft consensus (metadata) | Leader <-> members | Cluster bus (`cluster_addr`) |
| Failure detection | Leader -> members | TCP connect probe |
| Replication | Replica -> Primary | PSYNC checkpoint + command stream |
| Slot key-transfer | Source -> Target | `MIGRATE` protocol |

Only the first row is Raft; the rest are data-path or health-probe connections that never pass
through consensus. All topology knowledge comes from the replicated Raft state machine, not from a
peer's gossip or an external control plane.

---

## Node-Local Surfaces: Keyspace Notifications and SCAN

Two client-visible surfaces are scoped to a single node in cluster mode. Both match Redis Cluster,
and both mean a client that wants a cluster-wide view has to fan out across the primaries itself.

### Keyspace notifications are emitted only by the slot-owning primary

A write executes on the primary that owns the key's slot, and that node's shard worker publishes
the `__keyspace@0__:<key>` and `__keyevent@0__:<event>` messages directly into its own subscription
table (`emit_keyspace_notification` in
`frogdb-server/crates/core/src/shard/keyspace_notify.rs`, routed by the
`KeyspaceNotificationCoordinator`). That path never reaches `ClusterPubSubForwarder`, so the event
does not cross the cluster bus.

The distinction matters because ordinary `PUBLISH` *does* cross it: `PUBLISH` broadcasts to every
node so a broadcast-channel subscriber can attach anywhere. A keyspace notification is not routed
that way — it is delivered only to clients subscribed **on the owning primary**. This is Redis's
behavior too, where event emission publishes locally while only the `PUBLISH` command propagates
across the cluster.

Consequences for clients:

- To observe every mutation in the cluster, hold one subscription **per primary**. A single
  connection sees only the slots that node owns.
- After a slot migration or a failover, the emitting node for those slots changes with ownership,
  so a long-lived consumer must re-fan-out on topology change.
- Replicas apply the primary's write stream through the ordinary execution path
  (`ReplicaCommandExecutor` sends each replicated command as a normal shard execute), so a replica
  with notifications enabled emits its own copy locally as it applies the write. Events never cross
  nodes in either direction — each node's subscribers see only what that node executed.

### SCAN iterates one node's keyspace

`SCAN` is a scatter across the node's own [internal shards](#two-level-sharding) with no cluster
fan-out (`handle_scan` in `frogdb-server/crates/server/src/connection/scatter.rs`). It needs no slot
filtering: a node only ever holds keys for the slots it owns, so the keys it can return are already
exactly its own. The cursor encodes an internal shard index plus a position, so a cursor from one
node is meaningless on another and must not be replayed there.

A full-keyspace scan is therefore a client-side fan-out: run `SCAN` to completion against every
primary and union the results. Because slot ownership is exclusive, that union is free of duplicates
and no key ever appears via a non-owning node. `DBSIZE` and `KEYS` are per-node for the same reason.

---

## Rolling Upgrades: Version Gating

A mixed-version cluster must not let a newly upgraded node emit a behavior that older peers cannot
understand. FrogDB gates such behaviors behind an explicit **version gate**
(`frogdb-server/crates/cluster/src/version_gate.rs`).

Each gate is a `VersionGateEntry { name, min_version, description }`. A gate stays **dormant** until
the cluster's finalized `active_version` reaches its `min_version`: `is_gate_active(name,
active_version)` returns true only when `active_version` is set and `>= min_version`. The
`active_version` advances only after every node reports running at least the target version and the
leader commits a `FinalizeUpgrade` Raft command — so a feature never activates while an older node
is still in the cluster. `pending_gates(active_version, target_version)` reports the gates that a
proposed upgrade would unlock but that are still blocked.

The static registry currently declares one gate:

| Gate | `min_version` | Effect once active |
|------|---------------|--------------------|
| `extended_info_fields` | 0.2.0 | Include `active_version` and `cluster_version` in `INFO server` |

See [Operations: Clustering](/operations/clustering/) for the operator-facing rolling-upgrade
procedure.

---

## Observability Endpoints

Each node exposes a **read-only** HTTP observability plane (bind address from the `[http]` config,
`frogdb-server/crates/server/src/observability_server.rs`). These are health and status surfaces —
topology is **not** pushed over HTTP; it is owned by the Raft metadata plane. `/admin/cluster` is
GET-only; there is no POST topology-push endpoint.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/metrics` | GET | Prometheus metrics |
| `/health/live`, `/healthz` | GET | Liveness probe |
| `/health/ready`, `/readyz` | GET | Readiness probe |
| `/status/json` | GET | Node status snapshot |
| `/admin/cluster` | GET | Cluster topology view (read-only) |
| `/admin/role` | GET | This node's role |
| `/admin/nodes` | GET | Known cluster members |
| `/admin/upgrade-status` | GET | Rolling-upgrade / version-gate status |
| `/admin/health` | GET | Admin health detail |
| `/admin/shutdown` | POST | Graceful shutdown |
| `/admin/transfer-leader` | POST | Request Raft leadership transfer |

The `/admin/*` and `/debug/*` routes can be protected with a bearer token (`[http]` `token`
config). Topology mutations are Raft commands, not HTTP verbs. See
[Operations: Diagnostics](/operations/diagnostics/) and the
[Metrics reference](/reference/metrics/).

---

## Configuration Homogeneity

All nodes in a cluster should have the same `num-shards` configuration. Internal-shard placement is
computed per-node as `CRC16(key) % 16384 % num_shards`, so different shard counts change which
internal shard a key lands on (colocation within a node is always preserved, since it depends only
on the key's hash tag). During migration between nodes with different shard counts, the source
communicates its shard count and the target redistributes keys to its own internal shards.
