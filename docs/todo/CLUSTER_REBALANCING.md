# Cluster Rebalancing

## Overview

FrogDB's cluster has partial slot migration — metadata propagation via Raft, key-by-key MIGRATE with
ASK redirections — but lacks a high-throughput migration mechanism and has no auto-rebalancing.

Every Redis-family system (Redis, Valkey, DragonflyDB) relies on external tooling
(`redis-cli --cluster rebalance`, `redis-cli --cluster reshard`) for rebalancing decisions. Only
CockroachDB and FoundationDB have built-in auto-rebalancing intelligence. FrogDB will be the first
Redis-compatible system to combine atomic slot migration with built-in rebalancing.

This document consolidates the remaining work into two phases:
1. **Atomic Slot Migration** — Replace key-by-key MIGRATE with Valkey 9-style atomic slot transfer
2. **Auto-Rebalancing Intelligence** — Built-in multi-dimensional rebalancing (differentiator vs all
   Redis-family systems)

---

## Current State

### What exists

| Component | Status | Location |
|-----------|--------|----------|
| Slot migration metadata (Raft) | Implemented | `BeginSlotMigration`/`CompleteSlotMigration`/`CancelSlotMigration` via Raft consensus |
| CLUSTER SETSLOT | Implemented | `IMPORTING`/`MIGRATING`/`NODE`/`STABLE` subcommands |
| Key-by-key MIGRATE | Implemented | Standard DUMP/RESTORE protocol |
| ASK redirection | Implemented | Redirects during migration |

### Key files

- `frogdb-server/crates/cluster/src/types.rs` — Cluster type definitions, slot state
- `frogdb-server/crates/server/src/migrate.rs` — Current MIGRATE implementation
- `frogdb-server/crates/server/src/commands/cluster/admin.rs` — CLUSTER admin commands

### Limitations

- **Key-by-key MIGRATE is slow**: Each key requires a full DUMP→network→RESTORE round-trip. For a
  slot with 200K keys, that's 200K RTTs.
- **ASK redirections disrupt clients**: Multi-key operations (`MGET`, `MSET`) fail during migration
  if keys span migrating/non-migrating state.
- **No rebalancing intelligence**: Operators must manually decide which slots to move and where.

---

## Phase 1: Atomic Slot Migration

Replaces the DFLYMIGRATE spec from the former CLUSTER_PLAN.md Phase 5 (now merged into [CLUSTER.md](../spec/CLUSTER.md#implementation-status))
with a Valkey 9-inspired approach adapted for FrogDB's architecture.

### Design

Atomic slot migration models the transfer as a replication-like stream rather than individual key
moves. The entire slot transfers atomically — clients never see ASK redirections, and multi-key
operations continue working throughout.

### Mechanism

```
Source Node                          Target Node
─────────────                        ───────────
1. Create RocksDB checkpoint           │
   for slot data (reuse existing       │
   checkpoint infra from full sync)    │
                                       │
2. Stream slot contents ──────────────→ Store in staging area
   (element-level commands,            │ (isolated; clean rollback
   AOF-style, not bulk DUMP)           │  on failure)
                                       │
3. Track mutations to migrating        │
   slot during streaming               │
                                       │
4. Stream tracked mutations ──────────→ Apply delta
   (delta after snapshot)              │
                                       │
5. Brief pause when in-flight          │
   mutations below threshold           │
                                       │
6. Atomic ownership handover ─────────→ Raft consensus
                                       │
7. Source async-deletes ←──────────────→ Target starts serving
   migrated keys                         slot
```

### Key design decisions

**RocksDB checkpoints instead of fork()**: FrogDB already uses RocksDB checkpoints for full sync.
Reusing this for slot migration avoids the memory-doubling problem of `fork()` (which DragonflyDB's
DFLYMIGRATE relies on). Checkpoints are nearly instant (hard-links SST files) and don't block writes.

**Element-level streaming**: Large collections (sorted sets, hashes, lists with millions of elements)
stream as individual element commands (`HSET`, `ZADD`, `RPUSH`) rather than serialized bulk blobs.
This avoids buffer overflow and memory spikes on both source and target. It also naturally handles
values too large for a single network buffer.

**Staging area on target**: Incoming data lands in a staging area, not the live keyspace. If migration
fails at any point, the target discards the staging area cleanly — no partial state leaks into the
live dataset.

**No ASK redirections**: Clients are completely unaware of ongoing migrations. The source continues
serving the slot until the atomic handover completes. This eliminates the ASK/ASKING dance and means
multi-key operations like `MGET`/`MSET` work uninterrupted.

### New commands

| Command | Description |
|---------|-------------|
| `CLUSTER MIGRATESLOTS SLOTSRANGE <start> <end> NODE <target>` | Initiate atomic migration of a slot range to target node. Runs asynchronously. |
| `CLUSTER GETSLOTMIGRATIONS` | Poll migration status on source or target. Shows in-progress, recently completed, and failed migrations. |
| `CLUSTER CANCELSLOTMIGRATIONS` | Cancel all active slot migrations where this node is the source. |

### Migration states

```rust
pub enum AtomicMigrationState {
    /// Snapshot phase: creating RocksDB checkpoint and streaming contents
    Snapshotting { target: NodeId, slots: Vec<SlotRange>, keys_streamed: u64 },
    /// Catching up: streaming tracked mutations (delta)
    CatchingUp { target: NodeId, slots: Vec<SlotRange>, mutations_pending: u64 },
    /// Paused: brief write pause, finalizing handover
    Finalizing { target: NodeId, slots: Vec<SlotRange> },
    /// Importing: this node is receiving slot data
    Importing { source: NodeId, slots: Vec<SlotRange>, keys_received: u64 },
}
```

### Failure recovery

| Failure | Recovery |
|---------|----------|
| Source crashes mid-stream | Target discards staging area. Promote source replica (has all keys). |
| Target crashes mid-stream | Source aborts, remains slot owner. No data lost. |
| Network timeout | Source aborts, remains owner. Target discards staging area on topology update. |
| `FLUSHDB`/`FLUSHALL` during migration | Migration fails on both nodes (prevents data loss). |

### Files to create/modify

| File | Action | Purpose |
|------|--------|---------|
| `frogdb-server/crates/server/src/slot_migration.rs` | Create | Atomic slot migration coordinator |
| `frogdb-server/crates/core/src/cluster/migration.rs` | Create | Migration state machine (AtomicMigrationState) |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | Modify | Add MIGRATESLOTS/GETSLOTMIGRATIONS/CANCELSLOTMIGRATIONS |
| `frogdb-server/crates/server/src/replication/fullsync.rs` | Modify | Reuse RocksDB checkpoint infra for slot snapshots |
| `frogdb-server/crates/server/src/migrate.rs` | Modify | Integrate with new atomic path; keep legacy MIGRATE for compat |

### Performance expectations

Valkey 9 reports **4.6x–9.5x speedup** over key-by-key migration. FrogDB should see similar or
better gains because RocksDB checkpoints (hard-link SST files) are faster than fork()-based
snapshots.

---

## Phase 2: Auto-Rebalancing Intelligence

Source: [NEW_FEATURES.md — Feature 2: Auto-Rebalancing](NEW_FEATURES.md#feature-2-auto-rebalancing)

### The gap

Redis Cluster rebalances by slot-count only, not by actual load. A node with 1000 slots holding
tiny keys has the same "weight" as a node with 1000 slots holding 100GB of hot data. No open-source
Redis-compatible system has intelligent rebalancing.

### Multi-dimensional SlotWeight

```rust
struct SlotWeight {
    key_count: u64,           // Number of keys in slot
    memory_bytes: u64,        // Total memory usage
    qps: f64,                 // Queries per second (EWMA)
    write_qps: f64,           // Write-heavy slots need different handling
    avg_latency_us: u64,      // Operation complexity proxy
}

fn compute_weight(slot: &Slot) -> f64 {
    // Weighted sum — configurable per deployment
    w1 * normalize(slot.memory_bytes) +
    w2 * normalize(slot.qps) +
    w3 * normalize(slot.write_qps) +
    w4 * normalize(slot.avg_latency_us)
}
```

### Configuration

```toml
[cluster.rebalance]
enabled = true
weight_imbalance_threshold = 0.2    # 20% deviation triggers rebalance
memory_imbalance_threshold = 0.3
qps_imbalance_threshold = 0.25
min_slots_per_move = 10
evaluation_interval_secs = 60       # CockroachDB-inspired: re-evaluate every 60s
```

### Commands

| Command | Description |
|---------|-------------|
| `CLUSTER WEIGHTS` | Show per-node aggregate weights |
| `CLUSTER SLOTS WEIGHTS` | Show per-slot weights |
| `CLUSTER REBALANCE [DRY-RUN]` | Trigger rebalance (DRY-RUN shows plan without executing) |
| `CLUSTER REBALANCE FACTOR qps\|memory\|keys` | Rebalance optimizing for a specific factor |

### Implementation approach

CockroachDB-inspired continuous background rebalancer:
1. Every `evaluation_interval_secs`, compute `SlotWeight` for all slots
2. Aggregate per-node weights; detect imbalance above threshold
3. Generate migration plan: move heaviest slots from overloaded nodes to lightest nodes
4. Execute migrations using Phase 1 atomic slot migration
5. Rate-limit: max N concurrent migrations to avoid overwhelming the cluster

### Files to create/modify

| File | Action | Purpose |
|------|--------|---------|
| `frogdb-server/crates/core/src/cluster/rebalance.rs` | Create | Weight calculation, imbalance detection, migration planning |
| `frogdb-server/crates/core/src/shard.rs` | Modify | Add slot-to-shard mapping for weight tracking |
| `frogdb-server/crates/server/src/commands/cluster/admin.rs` | Modify | WEIGHTS, REBALANCE commands |
| `frogdb-server/crates/server/src/telemetry/` | Modify | Extend per-slot metrics collection |

---

## Prior Art / References

| System | Approach | Notes |
|--------|----------|-------|
| [Valkey 9 Atomic Slot Migration](https://valkey.io/blog/atomic-slot-migration/) | fork()-based snapshot + stream + atomic handover | 4.6–9.5x faster than key-by-key. FrogDB adapts this with RocksDB checkpoints instead of fork(). |
| [Valkey Atomic Slot Migration Docs](https://valkey.io/topics/atomic-slot-migration/) | CLUSTER MIGRATESLOTS / GETSLOTMIGRATIONS / CANCELSLOTMIGRATIONS | Command reference for new migration commands. |
| [CockroachDB Range Rebalancing](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer#rebalancing) | Multi-factor (disk, locality, QPS, range count), continuous background | Best-in-class auto-rebalancing; FrogDB's Phase 2 model. |
| FoundationDB | Centralized coordinator, automatic shard splitting/merging | Fully transparent to clients. |
| Redis Cluster | External tooling (`redis-cli --cluster rebalance`), slot-count only | No load awareness. |
| DragonflyDB | DFLYMIGRATE protocol, fork()-based | Memory-doubling risk from fork(). |

### FrogDB specs

- [CLUSTER.md — Implementation Status](../spec/CLUSTER.md#implementation-status) — Phase status and remaining work (formerly CLUSTER_PLAN.md)
- [NEW_FEATURES.md — Feature 2: Auto-Rebalancing](NEW_FEATURES.md#feature-2-auto-rebalancing) — Original auto-rebalancing design
- [POTENTIAL.md](POTENTIAL.md) — High-level remaining cluster work
