# Server Context

The FrogDB database engine: a Redis 8-compatible, memory-first, thread-per-core database. This
glossary fixes canonical terms and resolves homonyms; deep definitions live in the
[contributor docs](../website/src/content/docs/architecture/) (see the
[glossary](../website/src/content/docs/architecture/glossary.md) for the full reference).

## Language

### Topology & sharding

**Node**:
One FrogDB server instance, standalone or a cluster member.
_Avoid_: peer, instance (in cluster prose)

**Internal Shard**:
A thread-local partition of a node's data, owned by one ShardWorker.
_Avoid_: bare "shard" where cluster-level partitioning could be meant

**Hash Slot**:
The cluster-level partition (0–16383, CRC16), Redis Cluster compatible.
_Avoid_: bare "slot" outside cluster prose; "shard" for this concept

**Hash Tag**:
The `{tag}` key substring that pins slot and internal-shard colocation.

**Raft Metadata Plane**:
The embedded openraft consensus among nodes that owns cluster metadata (topology, slot
ownership, Config Epoch). Data operations never go through Raft.
_Avoid_: **Orchestrator** — the earlier external-control-plane design, superseded; do not
introduce it in new code or docs

**Config Epoch**:
The monotonic cluster-topology version counter (`ConfigEpoch`).
_Avoid_: bare "epoch" (homonym with Snapshot Epoch)

### Storage & persistence

**Store**:
The per-internal-shard key-value abstraction (`Store` trait; `HashMapStore` default impl).
_Avoid_: database, DB, keyspace (as a type name)

**keyspace** (lowercase, conceptual):
The whole dataset of a node or shard, used only in observability vocabulary (keyspace
notifications, keyspace hit/miss stats). Not a type.

**FrogDB WAL**:
The per-shard flush buffer that serializes a key's *current state* into RocksDB write batches —
a redo-of-current-state buffer, not a classic operation log.
_Avoid_: describing it as a sequential operation log

**RocksDB WAL**:
RocksDB's internal write-ahead log — the actual crash-durability mechanism beneath the FrogDB
WAL. Always qualify which WAL is meant.

**Snapshot**:
A forkless point-in-time capture of the dataset: a RocksDB checkpoint cut at a single
sequence number (`RocksStore::create_checkpoint`), with no in-memory buffering of
pre-snapshot values — concurrent writes proceed against the live **Store** untouched while
the checkpoint is cut.

**Checkpoint**:
The on-disk snapshot artifact: a RocksDB checkpoint (plus search-index sidecar and
`metadata.json`). FrogDB does not write RDB files.
_Avoid_: "RDB file", "RDB-compatible file"

**Snapshot Epoch**:
The monotonic generation counter (`SnapshotScheduler`) that numbers and names one snapshot
run (`snapshot_NNNNN` directory, `SnapshotEpoch` metric). Identifies a snapshot; does not
gate or buffer writes.
_Avoid_: bare "epoch"

**Pre-Snapshot Hook**:
The `PreSnapshotHook` callback (`RocksSnapshotCoordinator::set_pre_snapshot_hook`) run
immediately before a **Checkpoint** is cut — e.g. flushing search indexes and persisting the
replication offset alongside the snapshot.

**Warm Tier**:
The per-shard RocksDB column family holding values spilled out of RAM.

**Spill / Unspill**:
Moving a value hot→warm (spill) or warm→hot (unspill). Canonical terms; code uses
`spill_key`/`unspill_key` (renamed from `demote_key`/"promotion" per
`.scratch/naming-cleanup/issues/01-warm-tier-spill-rename.md`).
_Avoid_: demote/promote for tiering (reserved for cluster roles)

**Durability Mode**:
The `async` / `periodic` / `sync` WAL flush policy.

### Execution

**CommandSpec**:
The const, declarative description of a command's mechanical facts (key locations, flags,
WAL strategy, events); the dispatcher derives behavior from it.

**ShardWorker**:
The Tokio task owning one Internal Shard's Store and message queue.

**ConnectionHandler**:
The per-connection task that parses, routes, and replies.
_Avoid_: connection fiber, session

**coordinator shard** (lowercase):
The pinned shard orchestrating a scatter-gather or VLL multi-shard operation. Always qualify
with "shard" — never capitalize (avoids confusion with the retired Orchestrator).

**Scatter-Gather**:
The fan-out/merge execution strategy for multi-shard commands (MGET, DBSIZE, KEYS).

**VLL (Very Lightweight Locking)**:
Intent-based, txid-ordered multi-shard atomicity without mutexes.

**Continuation Lock**:
The VLL full-shard exclusive lock used when a MULTI/Lua key set isn't known upfront.

**txid**:
The global monotonic transaction id ordering VLL intents.

### Replication & roles

**Primary / Replica**:
Node data roles: Primary accepts writes for its slots; Replica streams from a Primary.
`master`/`slave` exist only at the wire-compat boundary (`NodeRole` Display, INFO fields).
_Avoid_: master/slave in any new prose or identifier

**Role Demotion / Role Promotion**:
A node changing role (Primary→Replica / Replica→Primary), e.g. during failover. Always
qualified with "role" now that tiering uses spill/unspill.

**Replication Backlog**:
The circular buffer of recent writes enabling PSYNC partial resync. Implemented by
`ReplicationRingBuffer` (implementation name, not the domain term).

**PSYNC / FULLRESYNC**:
Partial vs full replica synchronization; FULLRESYNC transfers a Checkpoint.

### Observability & ops

**Debug Bundle**:
The server-produced support archive (config section `[debug-bundle]`).
_Avoid_: diagnostic bundle

## Relationships

- A **Node** runs N **ShardWorkers**; each owns one **Internal Shard** with its own **Store**,
  expiry index, and **FrogDB WAL**.
- The cluster partitions keys into 16384 **Hash Slots**; each slot has one **Primary** and
  zero-or-more **Replicas**. A node's local keys are further partitioned into **Internal
  Shards** (two-level sharding).
- The **Raft Metadata Plane** owns slot assignment and role changes, versioned by **Config
  Epoch**; it never touches the data path.
- A write applies to the **Store**, then the **FrogDB WAL** (→ RocksDB write batch → **RocksDB
  WAL**), then optionally streams to **Replicas** via the **Replication Backlog**, per the
  **Durability Mode**.
- A **Snapshot** claims the next **Snapshot Epoch**, runs the **Pre-Snapshot Hook**, then cuts
  a RocksDB **Checkpoint** at the current sequence number; concurrent writes are never
  diverted or buffered — they land in the **Store** and **FrogDB WAL** as normal.
- Cold values **spill** from the **Store** to the **Warm Tier** and **unspill** on access.

## Example dialogue

> **Dev:** "After the failover, does the node spill its slots to the new Primary?"
> **Domain expert:** "No — that's a **role demotion**; the **Raft Metadata Plane** bumps the
> **Config Epoch** and the node starts streaming from the new Primary via **PSYNC**. **Spill**
> only ever means a value moving from the **Store** to the **Warm Tier**."

## Flagged ambiguities

- "epoch" was used for both snapshot coordination and topology versioning — resolved:
  **Snapshot Epoch** vs **Config Epoch**; bare "epoch" avoided.
- "demotion" was used for both role changes and tiering — resolved: **role demotion** vs
  **spill** (code renamed per issue 01).
- "WAL" names two different layers — resolved: always qualify **FrogDB WAL** vs **RocksDB WAL**.
- "Orchestrator" (external control plane) is a superseded design — resolved: the **Raft
  Metadata Plane** is current; website glossary updated, `clustering.md` rewrite tracked in
  issue 06.
- `-CROSSSLOT` is deliberately returned even for internal-shard violations (Redis
  compatibility) — the error string does not imply the check was at slot level.
