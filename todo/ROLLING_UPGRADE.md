# Rolling Upgrades

Design specification for zero-downtime rolling upgrades across FrogDB's deployment modes.
Covers version management, feature gating, finalization semantics, and the `frog upgrade` CLI.

**Status:** Spec only — implementation not started.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Version Model](#2-version-model)
3. [Feature Gating & Finalization](#3-feature-gating--finalization)
4. [Upgrade Procedure — Standalone](#4-upgrade-procedure--standalone)
5. [Upgrade Procedure — Replication Mode](#5-upgrade-procedure--replication-mode)
6. [Upgrade Procedure — Raft Cluster Mode](#6-upgrade-procedure--raft-cluster-mode)
7. [`frog upgrade` CLI](#7-frog-upgrade-cli)
8. [Server-Side Support Needed](#8-server-side-support-needed)
9. [Observability](#9-observability)
10. [Safety Guarantees](#10-safety-guarantees)
11. [Rollback](#11-rollback)
12. [Prior Art Comparison](#12-prior-art-comparison)

---

## 1. Overview

FrogDB rolling upgrades are **operator-driven** — the system never auto-upgrades. The operator
uses the `frog upgrade` CLI subcommands to plan, execute, and finalize upgrades across a cluster.

Key principles:

- **Mixed-version clusters** are supported within a one-minor-version window (e.g., 0.1.x nodes
  can coexist with 0.2.x nodes during the upgrade window)
- **New features are gated** until the operator explicitly finalizes the upgrade — upgraded nodes
  run new binary code but behave as old-version nodes until finalization
- **Finalization is irreversible** — it marks the point of no rollback, after which new data
  formats and protocol features may be used
- **One node at a time** — at most one node is down during the upgrade, preserving availability

This design draws from [CockroachDB's cluster version and finalization model](https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html)
adapted for FrogDB's orchestrated control plane and simpler topology.

---

## 2. Version Model

### Version Types

| Version | Source | Description |
|---------|--------|-------------|
| **Binary version** | `Cargo.toml` semver (e.g., `0.2.0`) | The version of the `frogdb-server` binary running on a given node |
| **Cluster version** | Derived: minimum binary version across all nodes | The effective version floor — what the cluster can safely assume all nodes support |
| **Active version** | Stored in Raft state (or primary state in replication mode) | The finalized version that determines which features are enabled; only advances via explicit finalization |

### Compatibility Rules

- **N → N+1 minor**: supported (e.g., 0.1.x → 0.2.x)
- **N → N+2 minor**: requires intermediate step (0.1.x → 0.2.x → 0.3.x)
- **Patch versions**: always compatible within the same minor (0.2.0 → 0.2.3 requires no
  finalization — no feature gates between patches)

### Version Advertisement

Version information is communicated through three channels:

| Channel | Mechanism | Details |
|---------|-----------|---------|
| RESP | `INFO server` (existing) | `frogdb_version:0.2.0` field — already present |
| RESP | `FROGDB.VERSION` (new) | Dedicated command returning binary version, cluster version, and active version |
| Raft state | `NodeInfo.version` (new field) | Each node's binary version stored in cluster state, visible to all peers |
| Replication | `REPLCONF frogdb-version <V>` (new) | New replication capability exchanged during handshake |

---

## 3. Feature Gating & Finalization

### Concept

Inspired by CockroachDB's version gates: named features that activate only at a specific active
version. During the mixed-version window, all nodes run the new binary but suppress new behavior
to remain compatible with old-version nodes that may still be in the cluster.

### Version Gate Registry

A compile-time list of `VersionGateEntry` structs:

```rust
struct VersionGateEntry {
    /// Human-readable feature name (e.g., "compact_type_bytes")
    name: &'static str,
    /// Minimum active version that enables this gate
    min_version: Version,
    /// Description shown in `frog upgrade status`
    description: &'static str,
}
```

### What Gets Gated

| Category | Example | Why Gated |
|----------|---------|-----------|
| Serialization | New compact type byte encoding | Old nodes can't deserialize new format |
| Raft commands | New `ClusterCommand` variants | Old nodes would fail to apply unknown log entries |
| Protocol features | New RESP command responses | Old clients/proxies may not expect new response shapes |
| Replication frames | New frame types in replication stream | Old replicas would fail to parse |
| Data structures | New on-disk data type | Old nodes can't read back after rollback |

### Mixed-Version Behavior

During the upgrade window (new binary, old active version):

1. Nodes detect that `active_version < binary_version`
2. All version-gated code paths check `is_gate_active(gate_name)` before executing
3. `is_gate_active` returns `true` only if `active_version >= gate.min_version`
4. New `ClusterCommand` variants are not proposed to Raft — prevents log entries old nodes can't
   deserialize
5. Serialization uses the old format for all writes
6. New RESP commands return `ERR this command requires version finalization` if their behavior
   depends on a gated feature

### Finalization

Finalization is the irreversible step that advances the active version and unlocks gated features.

**Raft cluster mode:**

`FinalizeUpgrade { version: String }` — a new `ClusterCommand` variant (the one exception to
"new variants are gated" — this variant is specifically designed to be parseable by both old and
new code, though it will only be proposed after all nodes are verified at the target version).

Procedure:
1. CLI sends finalization request to the Raft leader
2. Leader validates: every node in `ClusterStateInner.nodes` has `version >= target`
3. If validation passes, leader proposes `FinalizeUpgrade` to the Raft log
4. All nodes apply the command: set `ClusterStateInner.active_version = Some(target)`
5. Gates activate on the next check

**Replication mode:**

No Raft log, so finalization state is stored by the primary and communicated via a new
`FinalizeVersion` replication frame type:

1. CLI sends `FROGDB.FINALIZE <version>` to the primary
2. Primary validates: all connected replicas report `version >= target`
3. Primary stores `active_version` in its local state and persists it
4. Primary sends `FinalizeVersion { version }` frame to all replicas
5. Replicas update their local `active_version`

### `ClusterStateInner` Changes

```rust
pub struct ClusterStateInner {
    // ... existing fields (nodes, slot_assignment, config_epoch, migrations,
    //     last_applied_log, last_membership) ...

    /// The finalized active version. `None` means pre-versioning (original install,
    /// no finalization has ever occurred). Gates check this to decide behavior.
    pub active_version: Option<String>,
}
```

---

## 4. Upgrade Procedure — Standalone

Standalone mode (single node, no replication, no cluster) is trivial:

1. Create a snapshot (`BGSAVE` or `frog backup trigger`)
2. Stop the server (`systemctl stop frogdb` or graceful shutdown)
3. Replace the binary
4. Start the server
5. Verify health (`frog health`)

No finalization is needed — there are no peers to coordinate with. The node begins using new
features immediately upon restart.

---

## 5. Upgrade Procedure — Replication Mode

A primary with one or more replicas, not using Raft cluster mode.

### Prerequisites

- All nodes reachable (`frog health --all <addrs>`)
- Replication healthy — all replicas online and lag within acceptable bounds
  (`frog replication status`)
- Persistence OK — recent snapshot exists
- Optional: create a fresh snapshot on primary (`frog backup trigger`)

### Steps

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Primary    │────▶│  Replica 1  │     │  Replica 2  │
│  v0.1.0     │────▶│  v0.1.0     │     │  v0.1.0     │
└─────────────┘     └─────────────┘     └─────────────┘

Step 1: Upgrade Replica 1
─────────────────────────
  a. Stop Replica 1
  b. Replace binary with v0.2.0
  c. Start Replica 1
  d. Wait for replication catch-up (lag → 0)

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Primary    │────▶│  Replica 1  │     │  Replica 2  │
│  v0.1.0     │────▶│  v0.2.0 ✓   │     │  v0.1.0     │
└─────────────┘     └─────────────┘     └─────────────┘

Step 2: Upgrade Replica 2
─────────────────────────
  (same as Step 1, for Replica 2)

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Primary    │────▶│  Replica 1  │     │  Replica 2  │
│  v0.1.0     │────▶│  v0.2.0 ✓   │     │  v0.2.0 ✓   │
└─────────────┘     └─────────────┘     └─────────────┘

Step 3: Failover primary to an upgraded replica
────────────────────────────────────────────────
  a. Promote Replica 1 to primary (`frog replication promote`)
  b. Reconfigure Replica 2 and old Primary to replicate from new primary
  c. Verify replication healthy

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Replica    │────▶│  Primary    │◀────│  Replica 2  │
│  v0.1.0     │     │  v0.2.0 ✓   │     │  v0.2.0 ✓   │
└─────────────┘     └─────────────┘     └─────────────┘

Step 4: Upgrade old primary (now replica)
─────────────────────────────────────────
  a. Stop old primary
  b. Replace binary with v0.2.0
  c. Start as replica of new primary
  d. Wait for catch-up

┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Replica    │────▶│  Primary    │◀────│  Replica 2  │
│  v0.2.0 ✓   │     │  v0.2.0 ✓   │     │  v0.2.0 ✓   │
└─────────────┘     └─────────────┘     └─────────────┘

Step 5: Verify
──────────────
  frog upgrade status  →  all nodes v0.2.0, cluster version = 0.2.0

Step 6: Finalize (when ready)
─────────────────────────────
  frog upgrade finalize --version 0.2.0
  →  active version advances, gated features unlock
```

---

## 6. Upgrade Procedure — Raft Cluster Mode

A Raft-based cluster with slot-sharded primaries and replicas.

### Prerequisites

- `frog cluster check` passes — all slots covered, quorum OK, no PFAIL/FAIL nodes
- **No active slot migrations** — migrations must be completed or cancelled before starting
- Quorum can survive losing one node (i.e., at least 3 voter nodes)
- Optional: create snapshots on all nodes

### Critical Rule: No Slot Migration During Upgrade

Slot migration involves coordinated state changes across nodes. Running migrations during a
mixed-version window risks incompatible migration protocol behavior. The `frog upgrade check`
command verifies this precondition.

### Steps

```
Raft cluster: 3 voters (n1=leader, n2, n3) + replicas

Step 1: Pre-flight
──────────────────
  frog upgrade check --target-version 0.2.0
  →  Verifies: quorum OK, no migrations, version compatibility, all nodes reachable

Step 2: Upgrade non-leader nodes one at a time
───────────────────────────────────────────────
  For each non-leader Raft voter (n2, n3):
    a. Verify quorum can survive this node going down (N-1 ≥ majority)
    b. Stop node
    c. Replace binary with v0.2.0
    d. Start node
    e. Wait for Raft log catch-up and node to rejoin as voter
    f. Verify cluster health (`frog cluster check`)

  For each replica:
    a. Stop replica
    b. Replace binary
    c. Start replica
    d. Wait for replication catch-up

Step 3: Transfer Raft leadership to an upgraded node
─────────────────────────────────────────────────────
  POST /admin/transfer-leader → target: n2 (or any upgraded voter)
  Wait for leadership transfer to complete

Step 4: Upgrade old leader
──────────────────────────
  a. Stop n1 (no longer leader, safe)
  b. Replace binary with v0.2.0
  c. Start n1
  d. Wait for Raft log catch-up

Step 5: Verify
──────────────
  frog upgrade status
  →  All nodes v0.2.0, cluster version = 0.2.0, no mixed-version state
  frog cluster check
  →  All checks pass

Step 6: Finalize (when ready)
─────────────────────────────
  frog upgrade finalize --version 0.2.0
  →  Leader proposes FinalizeUpgrade to Raft log
  →  All nodes apply: active_version = 0.2.0
  →  Gated features unlock
```

### Raft Log Compatibility

The Raft log must not contain entries that old nodes cannot deserialize. This is enforced by:

1. New `ClusterCommand` variants (other than `FinalizeUpgrade`) are gated behind finalization
2. During the mixed-version window, the leader only proposes commands from the existing variant set
3. `FinalizeUpgrade` itself is designed to be forward-compatible: old nodes that receive it
   in the log can safely ignore it (they will be replaced before finalization completes)

---

## 7. `frog upgrade` CLI

The `frog upgrade` subcommand group provides operator tooling for managing rolling upgrades.
These commands follow the patterns established in [CLI.md](CLI.md): same global connection flags,
output modes (`table`/`json`/`raw`), and exit code conventions.

### Subcommands

#### `frog upgrade plan --target-version <V>`

Analyze the current cluster topology and generate an ordered upgrade plan.

```bash
frog upgrade plan --target-version 0.2.0
# Rolling Upgrade Plan: 0.1.0 → 0.2.0
#
# Mode: Raft Cluster (3 voters, 2 replicas)
# Estimated steps: 6
#
# STEP  ACTION                     NODE              ROLE     NOTES
# 1     Upgrade non-leader voter   10.0.0.2:6379     voter    Quorum: 2/3 → 2/3
# 2     Upgrade non-leader voter   10.0.0.3:6379     voter    Quorum: 2/3 → 2/3
# 3     Upgrade replica            10.0.0.4:6379     replica  Primary: 10.0.0.1
# 4     Upgrade replica            10.0.0.5:6379     replica  Primary: 10.0.0.2
# 5     Transfer leadership        10.0.0.1 → n2     leader   Transfer to upgraded node
# 6     Upgrade old leader         10.0.0.1:6379     voter    Quorum: 2/3 → 2/3
#
# After all nodes upgraded: run `frog upgrade finalize --version 0.2.0`
```

**Checks performed:**
- Detects deployment mode (standalone, replication, Raft cluster)
- Verifies target version is one minor ahead of current
- Computes safe upgrade order (replicas before voters, non-leaders before leader)
- Validates quorum is maintained at each step

**Connection required:** RESP to all nodes, Admin HTTP for Raft topology.

**Exit codes:** `0` = plan generated, `1` = upgrade not possible (version gap too large, quorum
insufficient, etc.)

#### `frog upgrade check --target-version <V>`

Pre-flight compatibility and readiness checks. Run this before starting an upgrade.

```bash
frog upgrade check --target-version 0.2.0
# Pre-flight Check: 0.1.0 → 0.2.0
#
# ✓ All nodes reachable (5/5)
# ✓ Version jump supported (0.1.0 → 0.2.0, one minor)
# ✓ No active slot migrations
# ✓ Raft quorum sufficient (3 voters, can lose 1)
# ✓ All replicas caught up (max lag: 128 bytes)
# ✓ Persistence healthy on all nodes
# ✓ No PFAIL/FAIL nodes
# ✗ Snapshot older than 1h on 10.0.0.3:6379 (WARNING — recommend fresh snapshot)
#
# Result: READY (1 warning)
```

**Connection required:** RESP to all nodes.

**Exit codes:** `0` = ready (warnings OK), `1` = blocking issues found, `2` = nodes unreachable.

#### `frog upgrade status`

Show the current version state of the cluster.

```bash
frog upgrade status
# Cluster Version Status
#
# Active Version:   0.1.0 (finalized)
# Cluster Version:  0.1.0 (minimum across all nodes)
# Mixed-Version:    no
#
# NODE              BINARY VERSION  STATUS
# 10.0.0.1:6379     0.1.0          ok
# 10.0.0.2:6379     0.1.0          ok
# 10.0.0.3:6379     0.1.0          ok
#
# Gated Features (available at 0.2.0):
#   (none pending — all features active for current version)
```

During a mixed-version upgrade:

```bash
frog upgrade status
# Cluster Version Status
#
# Active Version:   0.1.0 (finalized)
# Cluster Version:  0.1.0 (minimum across all nodes)
# Mixed-Version:    YES — upgrade in progress
#
# NODE              BINARY VERSION  STATUS
# 10.0.0.1:6379     0.1.0          pending upgrade
# 10.0.0.2:6379     0.2.0          upgraded
# 10.0.0.3:6379     0.2.0          upgraded
#
# Gated Features (blocked until finalization):
#   compact_type_bytes    Compact serialization format (saves ~15% storage)
#   stream_trim_minid     XTRIM MINID strategy support
```

**Connection required:** RESP to all nodes.

**Exit codes:** `0` = all nodes reachable, `1` = some nodes unreachable.

#### `frog upgrade node <addr>`

Guided single-node upgrade with interactive prompts. Handles pre-checks, graceful shutdown,
and post-upgrade verification for one node.

```bash
frog upgrade node 10.0.0.2:6379
# Upgrading node 10.0.0.2:6379
#
# Pre-checks:
#   ✓ Node reachable
#   ✓ Current version: 0.1.0
#   ✓ Role: voter (non-leader)
#   ✓ Raft quorum OK with this node down (2/3 remaining)
#   ✓ No slots in MIGRATING state on this node
#
# Ready to shut down 10.0.0.2:6379.
# Replace the binary and press Enter to continue (or Ctrl-C to abort)...
#
# [operator replaces binary, presses Enter]
#
# Waiting for 10.0.0.2:6379 to come back online...
#   ✓ Node reachable (took 3.2s)
#   ✓ Binary version: 0.2.0
#   ✓ Raft log caught up
#   ✓ Cluster health OK
#
# Node 10.0.0.2:6379 upgraded successfully.
```

**Procedure:**
1. Connect to node, run pre-checks (version, role, quorum impact)
2. Initiate graceful shutdown via `POST /admin/shutdown`
3. Wait for operator to replace binary (interactive prompt)
4. Poll for node to come back online
5. Verify: correct version, Raft log caught up (cluster mode), replication healthy (replication
   mode)

| Option | Description |
|--------|-------------|
| `--shutdown-timeout <secs>` | Graceful shutdown timeout (default: 30) |
| `--rejoin-timeout <secs>` | Timeout waiting for node to rejoin (default: 120) |
| `--yes` | Skip confirmation prompts |

**Connection required:** RESP + Admin HTTP to target node, RESP to other nodes for health checks.

**Exit codes:** `0` = upgrade successful, `1` = upgrade failed (node didn't rejoin, version
mismatch, etc.)

#### `frog upgrade rollback`

Analyze whether rollback is safe and provide guidance.

```bash
# Before finalization — safe to rollback
frog upgrade rollback
# Rollback Analysis
#
# Active Version:  0.1.0 (not yet finalized to 0.2.0)
# Status:          SAFE TO ROLLBACK
#
# No version-gated features have been activated. You can safely:
#   1. Stop upgraded nodes one at a time
#   2. Replace with v0.1.0 binary
#   3. Restart
#
# Recommended rollback order:
#   1. 10.0.0.2:6379 (voter, non-leader)
#   2. 10.0.0.3:6379 (voter, non-leader)

# After finalization — unsafe
frog upgrade rollback
# Rollback Analysis
#
# Active Version:  0.2.0 (FINALIZED)
# Status:          UNSAFE — ROLLBACK NOT RECOMMENDED
#
# Finalization has occurred. The following gated features are now active:
#   compact_type_bytes  — new serialization format may have been written to disk
#
# Rolling back to v0.1.0 would cause:
#   ✗ Old nodes cannot read data written in compact_type_bytes format
#   ✗ Potential data corruption or read failures
#
# Options:
#   1. Fix forward — address the issue on the current version
#   2. Restore from pre-upgrade backup (data written since finalization will be lost)
```

**Connection required:** RESP to all nodes.

**Exit codes:** `0` = rollback safe, `1` = rollback unsafe (post-finalization).

#### `frog upgrade finalize --version <V>`

Submit the finalization command to enable gated features. **This is irreversible.**

```bash
frog upgrade finalize --version 0.2.0
# Finalize Upgrade to 0.2.0
#
# Pre-checks:
#   ✓ All nodes on version 0.2.0 (5/5)
#   ✓ Cluster healthy
#   ✓ No active slot migrations
#
# WARNING: Finalization is IRREVERSIBLE.
# After finalization, rolling back to v0.1.0 is NOT SAFE.
#
# Features that will be activated:
#   compact_type_bytes    Compact serialization format
#   stream_trim_minid     XTRIM MINID strategy support
#
# Type 'FINALIZE' to confirm: FINALIZE
#
# Submitting FinalizeUpgrade to Raft leader...
#   ✓ FinalizeUpgrade committed (log index 4521)
#   ✓ Active version is now 0.2.0
#   ✓ 2 feature gates activated
#
# Upgrade to 0.2.0 complete.
```

**Procedure:**
1. Verify all nodes are at `--version` (refuse if any node is behind)
2. Verify cluster health (refuse if unhealthy)
3. Display warning and list of features that will activate
4. Require interactive confirmation (type "FINALIZE") unless `--yes` is passed
5. In Raft mode: send `FinalizeUpgrade` command to leader
6. In replication mode: send `FROGDB.FINALIZE` to primary
7. Verify active version advanced on all nodes

| Option | Description |
|--------|-------------|
| `--yes` | Skip interactive confirmation (for automation) |

**Connection required:** RESP to all nodes, Admin HTTP to leader (Raft mode).

**Exit codes:** `0` = finalization successful, `1` = pre-check failure or finalization rejected.

---

## 8. Server-Side Support Needed

These changes are required to implement the rolling upgrade system. They are **not part of this
spec's deliverable** — listed here for implementation planning.

### Cluster State Changes

| Component | Change | Location |
|-----------|--------|----------|
| `NodeInfo` | Add `version: String` field | `frogdb-server/crates/cluster/src/types.rs` |
| `ClusterStateInner` | Add `active_version: Option<String>` field | `frogdb-server/crates/cluster/src/state.rs` |
| `ClusterCommand` | Add `FinalizeUpgrade { version: String }` variant | `frogdb-server/crates/cluster/src/types.rs` |

### Version Gate Infrastructure

| Component | Description |
|-----------|-------------|
| `VersionGateEntry` | Struct: `name`, `min_version`, `description` |
| `VERSION_GATES` | Compile-time `&[VersionGateEntry]` registry |
| `is_gate_active(name)` | Check `active_version >= gate.min_version` |
| Gate check call sites | Serialization write path, new command handlers, new protocol features |

### New RESP Commands

| Command | Description |
|---------|-------------|
| `FROGDB.VERSION` | Returns binary version, cluster version, active version |
| `FROGDB.FINALIZE <version>` | Trigger finalization (replication mode) |

### Admin HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /admin/shutdown` | POST | HTTP-initiated graceful shutdown |
| `POST /admin/transfer-leader` | POST | Raft leadership transfer to specified node |
| `GET /admin/upgrade-status` | GET | Binary version, active version, gated features, node versions |

### Replication Protocol

| Change | Description |
|--------|-------------|
| `REPLCONF frogdb-version <V>` | New capability exchanged during replication handshake |
| `FinalizeVersion { version }` | New replication frame type, sent by primary after finalization |

---

## 9. Observability

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_binary_version` | Info gauge (label: `version`) | Binary version of this node |
| `frogdb_active_version` | Info gauge (label: `version`) | Current active (finalized) version |
| `frogdb_cluster_mixed_version` | Gauge (0 or 1) | 1 if nodes report different binary versions |
| `frogdb_version_gate_active` | Gauge (label: `gate`) | 1 if gate is active, 0 if suppressed |

### Logging

| Event | Level | Description |
|-------|-------|-------------|
| Mixed-version detected | WARN | Logged when nodes with different binary versions are observed |
| Gate suppressed | DEBUG | Logged when a version gate check returns false (feature suppressed) |
| Finalization | INFO | Logged when `FinalizeUpgrade` is applied |
| Version mismatch on join | WARN | Logged when a node joins with a version outside the compatibility window |

### Alerting Recommendations

| Condition | Severity | Rationale |
|-----------|----------|-----------|
| `frogdb_cluster_mixed_version == 1` for >1h | Warning | Upgrade may be stalled |
| `frogdb_cluster_mixed_version == 1` for >24h | Critical | Upgrade likely abandoned — mixed-version state should not persist |

---

## 10. Safety Guarantees

### No Data Loss

- Graceful shutdown flushes the WAL and completes pending writes before exit
  (see LIFECYCLE.md)
- RocksDB data survives restart — persistence is crash-safe by design
  (see PERSISTENCE.md)

### No Incompatible Data During Mixed-Version

- Version gates prevent new serialization formats from being written until finalization
- The Raft log contains only command variants that all nodes in the cluster can deserialize
- Replication stream uses only frame types understood by all connected replicas

### Availability

- At most one node is down at a time during the upgrade
- Raft quorum is validated before each node upgrade — the cluster can serve reads and writes
  with one voter down
- Replication mode: at least one replica remains available during primary failover
- Baseline: >80% availability during upgrade, consistent with the existing Jepsen rolling restart
  test (`testing/jepsen/frogdb/src/jepsen/frogdb/rolling_restart.clj`)

### Quorum Protection

- `frog upgrade plan` and `frog upgrade node` refuse to proceed if taking the target node down
  would break quorum
- For a 3-node Raft cluster: only one node may be down at a time (majority = 2)
- For a 5-node Raft cluster: up to two nodes may be down, but upgrade still proceeds one at a
  time for safety

### Rollback Safety

- Before finalization: fully safe — stop upgraded nodes, replace with old binary, restart
- After finalization: unsafe — new data formats may have been written; the only options are
  fix-forward or restore from pre-upgrade backup

---

## 11. Rollback

### Before Finalization

Rollback is safe and straightforward:

1. Stop upgraded nodes one at a time (reverse order of upgrade)
2. Replace binary with the old version
3. Restart each node
4. Verify cluster health

No data incompatibility exists because version gates prevented new formats from being used.
The cluster returns to its pre-upgrade state with no data loss.

### After Finalization

Rollback is **unsafe**:

- Gated features are now active — new serialization types, Raft command variants, and protocol
  features may have been used
- Data written in new formats cannot be read by old binaries
- Raft log may contain entries old nodes cannot deserialize

**Recovery options after a failed post-finalization upgrade:**

1. **Fix forward**: diagnose and resolve the issue on the new version — this is the recommended
   path since the new binary is already deployed
2. **Restore from backup**: restore from a pre-upgrade snapshot/backup — any data written after
   finalization is lost

### Rollback Decision Matrix

| State | Active Version | Rollback Safety | Action |
|-------|---------------|-----------------|--------|
| No nodes upgraded | N/A | Safe | Nothing to do |
| Some nodes upgraded, not finalized | Old version | Safe | Revert upgraded nodes |
| All nodes upgraded, not finalized | Old version | Safe | Revert all nodes |
| Finalized | New version | **Unsafe** | Fix forward or restore backup |

---

## 12. Prior Art Comparison

| System | Version Model | Feature Gating | Finalization | Version Window | Rollback |
|--------|--------------|----------------|--------------|----------------|----------|
| **FrogDB** (this spec) | Binary + cluster + active version | Compile-time gate registry | Explicit, irreversible (`FinalizeUpgrade`) | One minor (N → N+1) | Safe before finalization; unsafe after |
| **Redis Cluster** | No version tracking | None | None | Any (no guarantees) | Any time (no format changes between versions) |
| **CockroachDB** | Cluster version in system table | Version gates with feature checks | Explicit finalization (irreversible) | One major (e.g., 23.1 → 23.2) | Safe before finalization; unsafe after |
| **Kafka** | `inter.broker.protocol.version` | Protocol version negotiation | Manual config bump | One major | Revert config, then revert binary |

### Key Differences from CockroachDB

- FrogDB's gate registry is compile-time (static list) vs. CockroachDB's runtime registration
- FrogDB uses Raft log for finalization vs. CockroachDB's system table writes
- FrogDB has a simpler topology (fewer node roles) making the upgrade order more predictable
- FrogDB also supports replication-mode upgrades (no Raft), which CockroachDB does not need

### Key Differences from Redis Cluster

- Redis has no concept of version gating or finalization — all changes are immediately live
- Redis provides no tooling for coordinated rolling upgrades
- Redis does not track node versions in cluster state
- FrogDB's approach adds safety guarantees at the cost of the finalization step

---

## Cross-References

- Deployment guide: DEPLOYMENT.md — references this spec for rolling
  upgrade details
- CLI spec: [CLI.md](CLI.md) — `frog upgrade` listed as future extensibility; this spec defines
  the full subcommand group
- Cluster architecture: CLUSTER.md — Raft consensus, `ClusterCommand`,
  `ClusterStateInner`
- Replication protocol: REPLICATION.md — replica promotion, replication
  stream framing
- Lifecycle: LIFECYCLE.md — graceful shutdown behavior
- Failure modes: FAILURE_MODES.md — quorum loss, split-brain handling
- Jepsen rolling restart test: `testing/jepsen/frogdb/src/jepsen/frogdb/rolling_restart.clj` —
  validates >80% availability baseline during sequential node restarts
