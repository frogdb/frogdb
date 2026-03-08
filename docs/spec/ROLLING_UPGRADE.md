# Rolling Upgrade

This document specifies FrogDB's rolling upgrade procedure for both replication-mode and
Raft cluster-mode topologies. The design enables zero-downtime upgrades across mixed-version
clusters with explicit finalization semantics.

**Design principles:**

- **Zero downtime** — No client-visible unavailability during the upgrade window
- **N-1 compatibility** — Nodes always support the current wire protocol version and the
  previous one, so a cluster can run at most two adjacent versions simultaneously
- **Explicit finalization** — New-version features remain dormant until the operator (or
  auto-finalization) confirms every node has been upgraded

`[Not Yet Implemented]` — This entire specification is a design document. No upgrade
infrastructure exists in the codebase yet.

For deployment basics, see [DEPLOYMENT.md](DEPLOYMENT.md). For cluster architecture, see
[CLUSTER.md](CLUSTER.md). For replication, see [REPLICATION.md](REPLICATION.md).

---

## Version Model

FrogDB tracks three version dimensions:

| Dimension | Format | Source of Truth | Purpose |
|-----------|--------|-----------------|---------|
| **Node version** | Semver (`0.2.0`) | `Cargo.toml` `[workspace.package].version` | Binary identity; reported by `INFO server`, admin API |
| **Wire protocol version** | Monotonic integer (`1`, `2`, …) | Compiled into binary | Governs Raft RPC, replication frame, and snapshot format compatibility |
| **Cluster version** | Semver (matches a node version) | Raft state (cluster-mode) or primary (replication-mode) | The minimum version the cluster operates at; gates new features |

**Wire protocol version** is independent of semver. A semver bump (e.g. `0.2.0` → `0.3.0`)
may or may not bump the wire protocol version. The wire version increments only when the
binary format of Raft RPCs, replication frames, or snapshots changes.

**Compatibility rule:** A node at wire protocol version W must be able to send and receive
messages at versions W and W-1.

---

## Version Negotiation

### Raft RPCs

The `ClusterRpcRequest` envelope (`crates/cluster/src/network.rs`) gains a version header:

```rust
pub struct RpcEnvelope {
    pub wire_version: u32,
    pub payload: ClusterRpcRequest,
}
```

On receipt, a node checks `wire_version`:
- **Same as own** → process normally
- **Own version minus one** → process using backward-compatible deserialization
- **Anything else** → reject with `ERR INCOMPATIBLE_VERSION`, log warning

### Replication Handshake

The `REPLCONF` capability exchange (`crates/server/src/commands/replication.rs`) is extended
with a `version` capability:

```
REPLCONF capa eof psync2
REPLCONF version 0.2.0 wire 2
```

The primary records the replica's version and wire protocol version. Frame encoding uses
`min(primary_wire_version, replica_wire_version)` so the replica can always decode frames.

### HELLO Command

The existing `HELLO` command (RESP version negotiation) is extended to include
`server-version` and `wire-version` fields in its response map, allowing clients and
monitoring tools to discover version information.

---

## Cluster Version Tracking

### Cluster Mode (Raft)

A new Raft command propagates the cluster version:

```rust
pub enum ClusterCommand {
    // ... existing variants ...
    SetClusterVersion { version: String },
}
```

The `ClusterSnapshot` gains a `cluster_version` field:

```rust
pub struct ClusterSnapshot {
    // ... existing fields ...
    pub cluster_version: Option<String>,
}
```

And `NodeInfo` gains a `node_version` field:

```rust
pub struct NodeInfo {
    // ... existing fields ...
    pub node_version: Option<String>,
}
```

Each node updates its `NodeInfo.node_version` on startup via an `AddNode` or dedicated
`UpdateNodeVersion` command. The Raft leader can compute the minimum version across all
nodes at any time by scanning `ClusterSnapshot.nodes`.

### Replication Mode

The primary tracks replica versions received during the `REPLCONF version` handshake.
The cluster version is the minimum of the primary's own version and all connected replicas'
versions. This is stored in `ReplicationState`:

```rust
pub struct ReplicationState {
    // ... existing fields ...
    pub cluster_version: Option<String>,
}
```

### Admin API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/upgrade-status` | `GET` | Per-node versions, cluster version, finalization state |
| `/admin/upgrade-finalize` | `POST` | Trigger finalization (alternative to `CLUSTER UPGRADE-FINALIZE`) |

Example response for `GET /admin/upgrade-status`:

```json
{
  "cluster_version": "0.1.0",
  "node_versions": {
    "node-1": "0.2.0",
    "node-2": "0.2.0",
    "node-3": "0.1.0"
  },
  "all_nodes_upgraded": false,
  "finalization_eligible": false
}
```

---

## Feature Gating

Features and format changes introduced in version N are gated behind
`cluster_version >= N`. Nodes at version N can *parse* new formats but do not *emit* them
until the cluster version is finalized to N.

### Feature Registry

A compile-time table maps features to the minimum cluster version required to activate them:

```rust
pub struct GatedFeature {
    pub name: &'static str,
    pub min_version: &'static str,
    pub description: &'static str,
}

pub const GATED_FEATURES: &[GatedFeature] = &[
    // Example entries — populated as features are added
    // GatedFeature {
    //     name: "new_frame_flags",
    //     min_version: "0.2.0",
    //     description: "Extended replication frame flag bits",
    // },
];
```

**Gated areas include:**
- New commands or command flags
- New `ClusterCommand` variants in the Raft log
- New replication frame flags or encoding formats
- New configuration options with cluster-wide effects

Runtime checks use the feature registry:

```rust
fn is_feature_active(feature: &str, cluster_version: &str) -> bool {
    GATED_FEATURES.iter()
        .find(|f| f.name == feature)
        .map(|f| semver_gte(cluster_version, f.min_version))
        .unwrap_or(false)
}
```

---

## Upgrade Procedure — Replication Mode

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Replication-Mode Upgrade Flow                      │
│                                                                      │
│  Step 1: Pre-flight checks                                           │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Primary  │────▶│ Replica  │────▶│ Replica  │  All healthy?       │
│  │   v0.1   │     │   v0.1   │     │   v0.1   │  Version compat?   │
│  └──────────┘     └──────────┘     └──────────┘                     │
│                                                                      │
│  Step 2: Upgrade replicas one at a time                              │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Primary  │────▶│ Replica  │     │ Replica  │                     │
│  │   v0.1   │     │ ■ v0.2 ■ │     │   v0.1   │  Stop → swap →    │
│  └──────────┘     └──────────┘     └──────────┘  start → verify    │
│                                                                      │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Primary  │────▶│ Replica  │     │ Replica  │                     │
│  │   v0.1   │     │   v0.2   │     │ ■ v0.2 ■ │                     │
│  └──────────┘     └──────────┘     └──────────┘                     │
│                                                                      │
│  Step 3: Failover to upgraded replica                                │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Replica  │◀────│ Primary  │────▶│ Replica  │  CLUSTER FAILOVER  │
│  │   v0.1   │     │ ■ v0.2 ■ │     │   v0.2   │                     │
│  └──────────┘     └──────────┘     └──────────┘                     │
│                                                                      │
│  Step 4: Upgrade old primary (now replica)                           │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Replica  │◀────│ Primary  │────▶│ Replica  │                     │
│  │ ■ v0.2 ■ │     │   v0.2   │     │   v0.2   │  Stop → swap →    │
│  └──────────┘     └──────────┘     └──────────┘  start → resync    │
│                                                                      │
│  Step 5: Finalize                                                    │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Replica  │◀────│ Primary  │────▶│ Replica  │  CLUSTER            │
│  │   v0.2   │     │   v0.2   │     │   v0.2   │  UPGRADE-FINALIZE  │
│  └──────────┘     └──────────┘     └──────────┘                     │
└──────────────────────────────────────────────────────────────────────┘
```

### Step-by-Step

1. **Pre-flight** — Verify all nodes healthy (`/admin/health`), confirm target version is
   exactly one minor/major version ahead, check wire protocol compatibility.

2. **Upgrade replicas** — For each replica:
   - Stop the replica process
   - Replace the binary with the new version
   - Start the replica
   - Verify it reconnects and resumes replication streaming (`INFO replication`)

3. **Failover** — Run `CLUSTER FAILOVER` on an upgraded replica to promote it to primary.
   The old primary demotes to replica. Clients follow the redirect transparently.

4. **Upgrade old primary** — Stop the now-demoted node, swap binary, restart. It
   reconnects as a replica and re-syncs via PSYNC (or FULLRESYNC if WAL gap is too large).

5. **Finalize** — Run `CLUSTER UPGRADE-FINALIZE` on the primary. The primary verifies all
   replicas report version >= target, then bumps `cluster_version`.

---

## Upgrade Procedure — Cluster Mode (Raft)

```
┌──────────────────────────────────────────────────────────────────────┐
│                    Cluster-Mode Upgrade Flow                         │
│                                                                      │
│  Step 1: Pre-flight                                                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │  Leader  │◀───▶│ Follower │◀───▶│ Follower │  Quorum healthy?   │
│  │   v0.1   │     │   v0.1   │     │   v0.1   │  No migrations?    │
│  └──────────┘     └──────────┘     └──────────┘  Version compat?   │
│                                                                      │
│  Step 2: Upgrade followers one at a time                             │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │  Leader  │◀───▶│ Follower │     │ Follower │                     │
│  │   v0.1   │     │ ■ v0.2 ■ │     │   v0.1   │  Drain → stop →   │
│  └──────────┘     └──────────┘     └──────────┘  swap → start →    │
│                                                    verify            │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │  Leader  │◀───▶│ Follower │     │ Follower │                     │
│  │   v0.1   │     │   v0.2   │     │ ■ v0.2 ■ │                     │
│  └──────────┘     └──────────┘     └──────────┘                     │
│                                                                      │
│  Step 3: Transfer leadership, upgrade old leader                     │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Follower │◀───▶│  Leader  │◀───▶│ Follower │  Leadership        │
│  │ ■ v0.2 ■ │     │ ■ v0.2 ■ │     │   v0.2   │  transfer →       │
│  └──────────┘     └──────────┘     └──────────┘  stop old leader → │
│                                                    swap → restart    │
│  Step 4: Finalize                                                    │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                     │
│  │ Follower │◀───▶│  Leader  │◀───▶│ Follower │  CLUSTER            │
│  │   v0.2   │     │   v0.2   │     │   v0.2   │  UPGRADE-FINALIZE  │
│  └──────────┘     └──────────┘     └──────────┘                     │
└──────────────────────────────────────────────────────────────────────┘
```

### Step-by-Step

1. **Pre-flight** — Verify Raft quorum is healthy, no slot migrations are in progress,
   confirm version compatibility.

2. **Upgrade followers** — For each non-leader node:
   - **Drain**: Stop accepting new client connections via admin API
     (`POST /admin/drain`). Existing connections finish in-flight commands.
   - **Stop** the node process.
   - **Swap** the binary with the new version.
   - **Start** the node. It rejoins the Raft group automatically.
   - **Verify**: Node appears in `CLUSTER NODES` as healthy, Raft log is caught up
     (`/admin/upgrade-status` shows matching version).
   - Proceed to next follower only after the current one is fully caught up.

3. **Upgrade leader** — The leader is upgraded last to minimize election churn:
   - **Transfer leadership**: Run `CLUSTER FAILOVER` targeting an upgraded follower.
     The upgraded follower becomes the new leader.
   - **Stop** the old leader, swap binary, start. It rejoins as a follower.
   - **Verify** it catches up on the Raft log.

4. **Finalize** — Run `CLUSTER UPGRADE-FINALIZE` on any node (it forwards to the leader).
   The leader verifies all nodes report version >= target, then proposes
   `SetClusterVersion` via Raft. Once committed, version-gated features activate.

### Quorum Safety

With a 3-node Raft cluster, upgrading one follower at a time maintains quorum (2 of 3).
For a 5-node cluster, up to two nodes can be down simultaneously. The upgrade procedure
never takes more than one node offline at a time, so quorum is always preserved.

---

## Finalization

### CLUSTER UPGRADE-FINALIZE Command

A new `CLUSTER UPGRADE-FINALIZE` command triggers version finalization.

**Syntax:**
```
CLUSTER UPGRADE-FINALIZE [version]
```

If `version` is omitted, the target version is inferred as the minimum node version across
the cluster.

**Pre-conditions (all must be true):**
- All nodes report `node_version >= target`
- All nodes are healthy (not marked failed)
- No slot migrations in progress
- Raft quorum is available (cluster-mode)

**Effect:**
- Proposes `SetClusterVersion { version }` via Raft (cluster-mode) or updates
  `ReplicationState.cluster_version` directly (replication-mode)
- Once committed, version-gated features for the new version activate across all nodes
- The cluster is no longer in the mixed-version window

**Error responses:**
```
-ERR Not all nodes are at version X.Y.Z (node-3 is at X.Y-1.Z)
-ERR Cannot finalize: node node-2 is marked as failed
-ERR Cannot finalize: slot migration in progress for slot 1234
```

### Auto-Finalization

```toml
[cluster]
auto_finalize_upgrade = false  # default
```

When enabled, the leader periodically checks whether all nodes report the same version and
all pre-conditions are met. If so, it automatically proposes `SetClusterVersion`.

**Safety:** Auto-finalization respects `preserve_downgrade_option` (see Rollback). When
`preserve_downgrade_option` is set, auto-finalization is suppressed regardless of the
`auto_finalize_upgrade` setting.

### Downgrade Window

Until finalization completes:
- The cluster operates at the *old* cluster version
- New-version features are dormant (nodes can parse them but do not emit them)
- Rolling back to the previous version is safe — just reverse the upgrade procedure

After finalization:
- The cluster version advances; new features activate
- On-disk formats may change (Raft snapshots, RocksDB data)
- Rollback to the previous version is **not supported**

---

## Serialization Compatibility

### Raft Snapshots

A version header is prepended to serialized `ClusterSnapshot` data:

```
┌───────────────┬──────────────────────────────────────┐
│ version: u32  │ serialized ClusterSnapshot bytes ... │
└───────────────┴──────────────────────────────────────┘
```

The deserializer reads the version, then dispatches to the correct parser. Nodes support
deserializing snapshots at the current format version and the previous one (N and N-1).

### Raft Log Entries

`ClusterCommand` is a serde-tagged enum. New variants are added at the end. Nodes running
the older version that encounter an unknown variant skip it with a warning log — this is
forward-compatible by design. The skipped entry still advances the Raft log index but
produces no state change on older nodes.

```rust
// Deserialization with forward compatibility
fn apply_command(cmd: ClusterCommand) {
    match cmd {
        ClusterCommand::AddNode { .. } => { /* ... */ },
        ClusterCommand::SetClusterVersion { .. } => { /* ... */ },
        // Unknown variants logged and skipped
        _ => warn!("Unknown cluster command variant, skipping"),
    }
}
```

### Replication Frames

The replication frame header already includes a format byte. During the `REPLCONF version`
handshake, the primary and replica negotiate the frame version. The primary encodes frames
at `min(own_wire_version, replica_wire_version)`, ensuring the replica can always decode.

### RocksDB On-Disk Format

A version marker file (`data/format_version`) records the on-disk format version. On
startup, the node reads this file:

- **Marker matches current version** → proceed normally
- **Marker is older** → run forward migration, update marker
- **Marker is newer** → refuse to start with a clear error (prevents data corruption from
  running an old binary against a migrated data directory)
- **Marker missing** → assume version 1 (initial format), migrate if needed

On-disk migration is a one-way operation. Once a node starts with a new binary and migrates
its data, it cannot be started with an older binary. This is why finalization is the point
of no return.

---

## Rollback

### Before Finalization

Rolling back is safe. The procedure is the reverse of the upgrade:

1. Stop upgraded nodes one at a time
2. Replace binary with the previous version
3. Restart — the node rejoins with the old binary
4. If a failover occurred during upgrade, perform another failover to restore the original
   topology (optional — not strictly required for correctness)

Because the cluster version has not advanced, no new-version features were activated and no
on-disk format changes were made.

### After Finalization

Rollback is **not supported**. On-disk formats may have changed, and the Raft log may
contain entries that older nodes cannot process. Restore from backup if a critical issue is
discovered post-finalization.

### Preserving the Downgrade Option

Inspired by CockroachDB's `cluster.preserve_downgrade_option`:

```toml
[cluster]
preserve_downgrade_option = false  # default
```

When set to `true`:
- Auto-finalization is suppressed
- `CLUSTER UPGRADE-FINALIZE` is rejected with an error
- The cluster remains in the mixed-version window indefinitely

This is useful for:
- Extended testing in a staging environment before committing to the new version
- Canary deployments where a quick rollback must remain possible

Set to `false` (or remove) and then run `CLUSTER UPGRADE-FINALIZE` to proceed.

---

## Admin API & Observability

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/upgrade-status` | `GET` | Cluster version, per-node versions, finalization eligibility |
| `/admin/upgrade-finalize` | `POST` | Trigger finalization (equivalent to `CLUSTER UPGRADE-FINALIZE`) |
| `/admin/drain` | `POST` | Stop accepting new connections (for graceful node shutdown) |

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_cluster_version` | Gauge (label: `version`) | Current cluster version |
| `frogdb_node_version` | Gauge (label: `version`) | This node's version |
| `frogdb_upgrade_in_progress` | Gauge (0 or 1) | 1 when cluster version < min node version |
| `frogdb_wire_protocol_version` | Gauge | This node's wire protocol version |

### Logging

During the mixed-version window, log lines are annotated with `[MIXED-VERSION]` to aid
debugging:

```
[MIXED-VERSION] Received RPC from node-2 at wire version 1 (own: 2), using compat mode
[MIXED-VERSION] Encoding replication frame at wire version 1 for replica node-3
```

---

## Failure Modes

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| Node crash during upgrade | Standard Raft recovery (cluster-mode) or replica reconnect (replication-mode) | Restart node; it catches up automatically |
| Quorum loss during upgrade | Cluster becomes read-only (writes rejected with `CLUSTERDOWN`) | Restore crashed nodes at their current version (old or new) to regain quorum |
| Finalization with unhealthy node | `CLUSTER UPGRADE-FINALIZE` rejected | Restore unhealthy node, then retry finalization |
| Version mismatch on Raft join | Connection rejected with `-ERR INCOMPATIBLE_VERSION` | Upgrade or downgrade the incompatible node |
| Network partition during upgrade | Raft leader on majority side continues; minority side is unavailable | Heal partition; minority nodes catch up on Raft log |
| Finalization partially committed | Raft guarantees atomicity — once committed, all nodes apply | If leader crashes mid-proposal, Raft replays on new leader |

### Upgrade Abort

If an issue is discovered mid-upgrade (before finalization), the operator can abort by
rolling back already-upgraded nodes to the previous version. The cluster continues at the
old version. No special "abort" command is needed — just reverse the binary swap.

---

## Configuration Reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cluster.auto_finalize_upgrade` | `bool` | `false` | Automatically finalize when all nodes match |
| `cluster.preserve_downgrade_option` | `bool` | `false` | Prevent finalization, keep downgrade path open |
| `cluster.upgrade_drain_timeout_secs` | `u64` | `30` | Max seconds to wait for connections to drain before shutdown |
| `cluster.version_check_interval_secs` | `u64` | `10` | How often the leader polls node versions (for auto-finalize) |
