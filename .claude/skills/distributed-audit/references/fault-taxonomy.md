# FrogDB Fault Taxonomy

Exhaustive catalog of fault scenarios organized by category. Each entry includes:
description, affected components, FrogDB-specific mitigation, and test coverage.

For the definitive source on failure handling, see `docs/spec/FAILURE_MODES.md`.

---

## Timing Faults

### TF-1: Race Condition in Shard Worker

**Description:** Two operations on the same key interleave within a shard, producing
an impossible intermediate state.

**Affected components:** `crates/core/src/shard/`, `crates/core/src/store/`

**FrogDB mitigation:** Single-task shard worker — commands execute sequentially. No
interleaving possible within a shard.

**Risk when violated:** Data corruption, impossible values visible to clients.

**Test coverage:** `just concurrency` (Shuttle), `just jepsen register`

---

### TF-2: TOCTOU in WATCH/EXEC

**Description:** Between WATCH check and EXEC execution, another client modifies the
watched key. EXEC should fail but doesn't because the check and execution are not atomic.

**Affected components:** `crates/server/src/connection/handlers/`, transaction state

**FrogDB mitigation:** WATCH tracks key modification versions within the shard. EXEC
checks versions atomically within shard worker execution.

**Risk when violated:** Lost updates, violated optimistic locking semantics.

**Test coverage:** `just jepsen transaction`, `just jepsen transaction-crash`

---

### TF-3: Message Reordering Between Shards

**Description:** Messages sent from connection handler to shard workers arrive in a
different order than sent, causing operations to execute out of order.

**Affected components:** `crates/server/src/connection/`, `crates/core/src/shard/`

**FrogDB mitigation:** Tokio `mpsc` channels guarantee FIFO order per sender. A single
connection sends to each shard sequentially, preserving order.

**Risk when violated:** Pipeline responses returned in wrong order.

**Test coverage:** Integration tests with pipelining

---

### TF-4: Stale Reads After Failover

**Description:** Client reads from new primary after failover but sees data from before
the promoted replica's last sync point (older than expected).

**Affected components:** `crates/replication/`, `crates/cluster/src/state.rs`

**FrogDB mitigation:** Replica flushes pending WAL before accepting writes as primary.
Data visible on new primary = data replicated before failover. Unreplicated writes on
old primary are lost (bounded by replication lag).

**Risk when violated:** Clients see time-travel (values revert to earlier state).

**Test coverage:** `just jepsen split-brain`, `just jepsen replication`

---

### TF-5: Lost Wakeup in Blocking Commands

**Description:** BLPOP waiter is registered, LPUSH arrives and pushes a value, but the
waiter is never notified. The value sits in the list while the client blocks indefinitely.

**Affected components:** `crates/core/src/shard/`, blocking command handling

**FrogDB mitigation:** `wakes_waiters()` trait method on write commands that produce
data for blocked clients. Shard worker checks waiters after each such command.

**Risk when violated:** Client hangs, potential resource leak from accumulated blocked connections.

**Test coverage:** `just jepsen blocking`, `just jepsen blocking-crash`

---

### TF-6: Clock Skew Affecting Expiry

**Description:** Node clock jumps forward/backward, causing keys to expire too early
or too late.

**Affected components:** `crates/core/src/store/`, expiry handling

**FrogDB mitigation:** Uses `Instant` (monotonic clock) for runtime expiry checks.
Persisted as Unix timestamps (`i64` ms), converted to `Instant` on recovery.

**Risk when violated:** Keys expire unexpectedly or persist beyond TTL.

**Test coverage:** `just jepsen expiry`, `just jepsen clock-skew`

---

## Crash Faults

### CF-1: Mid-Write Crash

**Description:** Server crashes after applying write to in-memory store but before WAL
entry is durable.

**Affected components:** `crates/persistence/src/wal.rs`, `crates/core/src/store/`

**FrogDB mitigation:** In Async/Periodic mode: write may be lost (by design, matches Redis).
In Sync mode: write not acknowledged until fsync, so client retries.

**Risk when violated:** In Sync mode, acknowledged write lost on crash.

**Test coverage:** `just jepsen crash`, `just jepsen counter-crash`

---

### CF-2: Mid-Snapshot Crash

**Description:** Server crashes during forkless snapshot creation.

**Affected components:** `crates/persistence/src/snapshot.rs`

**FrogDB mitigation:** Snapshot written to temp file, atomically renamed on completion.
On recovery: partial snapshot file ignored, use previous complete snapshot + WAL replay.

**Risk when violated:** Corrupt snapshot loaded, producing invalid database state.

**Test coverage:** `just jepsen append-crash`, `just jepsen append-rapid`

---

### CF-3: Shard Worker Panic

**Description:** Shard worker task panics due to bug, assertion failure, or
unrecoverable error.

**Affected components:** `crates/core/src/shard/`

**FrogDB mitigation:** Tokio runtime catches panic. Affected shard marked unhealthy.
Other shards continue serving. Pending operations on affected shard receive error.

**Risk when violated:** Panic in one shard affects other shards, or server crashes entirely.

**Test coverage:** `just test` (unit tests with panic scenarios)

---

### CF-4: OOM Kill

**Description:** OS OOM killer terminates FrogDB process due to memory exhaustion.

**Affected components:** Entire process

**FrogDB mitigation:** `max_memory` configuration with OOM error responses for writes.
Eviction policies reclaim memory proactively. Recovery via WAL replay on restart.

**Risk when violated:** Data loss bounded by durability mode.

**Test coverage:** `just jepsen memory-pressure`

---

### CF-5: Crash During FULLRESYNC

**Description:** Primary or replica crashes during full synchronization checkpoint transfer.

**Affected components:** `crates/replication/src/primary.rs`, `crates/replication/src/replica.rs`

**FrogDB mitigation:**
- Primary crash: Replica detects disconnect, retries PSYNC with exponential backoff.
- Replica crash: Primary detects disconnect, cleans up streaming state.
- SHA256 checksum validates checkpoint integrity. Checksum mismatch → retry.
- Partial checkpoint files deleted on interrupted transfer.

**Risk when violated:** Replica loads corrupt or partial checkpoint, diverges from primary.

**Test coverage:** `just jepsen replication-chaos`

---

### CF-6: Crash During Slot Migration

**Description:** Node crashes while migrating hash slots between nodes.

**Affected components:** `crates/cluster/src/`, migration state machine

**FrogDB mitigation:** Multi-phase migration protocol. Each phase is durable.
Crash during migration → resume from last completed phase on restart.

**Risk when violated:** Slot owned by no node (data inaccessible) or two nodes
(split-brain for that slot range).

**Test coverage:** `just jepsen slot-migration`, `just jepsen slot-migration-partition`

---

### CF-7: Crash During Raft Apply

**Description:** Node crashes while applying a Raft log entry to the state machine.

**Affected components:** `crates/cluster/src/state.rs`

**FrogDB mitigation:** Raft log is durable. On recovery, re-apply uncommitted entries
from Raft log. Idempotent application: re-applying an already-applied entry is safe.

**Risk when violated:** State machine diverges from Raft log.

**Test coverage:** `just jepsen leader-election`, `just jepsen raft-chaos`

---

### CF-8: Double Crash (Primary + Replica)

**Description:** Both primary and replica crash simultaneously (power failure, rack failure).

**Affected components:** All

**FrogDB mitigation:** Data loss bounded by durability mode of each node. If both use
Sync mode: no data loss. If Async/Periodic: up to sync interval of data loss per node.
Recovery: whichever node has highest WAL sequence becomes primary.

**Risk when violated:** Unrecoverable data loss if no durable copy exists.

**Test coverage:** Manual testing, `just jepsen replication-chaos`

---

## Network Faults

### NF-1: Symmetric Network Partition

**Description:** Two groups of nodes cannot communicate with each other. Both groups
can communicate internally.

**Affected components:** `crates/cluster/src/`, `crates/replication/src/`

**FrogDB mitigation:** Orchestrated topology — orchestrator is single source of truth.
Partition between primary and orchestrator triggers failover after timeout.
Partition between primary and replica: replica falls behind, PSYNC catches up after heal.

**Risk when violated:** Split-brain: two primaries accept writes for same slots.

**Test coverage:** `just jepsen split-brain`, `just jepsen leader-election-partition`

---

### NF-2: Asymmetric Network Partition

**Description:** One-way network failure: A can send to B, but B cannot send to A.

**Affected components:** `crates/replication/src/`

**FrogDB mitigation:** Detected via asymmetric timeout behavior:
- Primary→Replica works, Replica→Primary broken: Primary sees no ACKs, times out
- Replica→Primary works, Primary→Replica broken: Replica sees no data but heartbeats OK;
  `replication_health_check()` detects and reconnects

**Risk when violated:** Indefinitely growing lag without detection, or zombie primary.

**Test coverage:** `just jepsen zombie`

---

### NF-3: Split-Brain Window

**Description:** During failover, both old and new primary accept writes for the same
slot range (window up to `fencing_timeout_ms`).

**Affected components:** `crates/cluster/src/state.rs`

**FrogDB mitigation:**
- Self-fencing: primary self-demotes after `self_fence_timeout_ms` without orchestrator
- Epoch fencing: nodes reject operations with stale epoch
- Divergent writes logged to `data/split_brain_discarded.log`
- `min_replicas_to_write = 1` prevents writes without replica confirmation

**Risk when violated:** Permanent data divergence, silent data loss.

**Test coverage:** `just jepsen split-brain`, `just jepsen zombie`

---

### NF-4: Orchestrator Isolation

**Description:** Orchestrator loses connectivity to all nodes but nodes can still
communicate with each other.

**Affected components:** `crates/cluster/src/`

**FrogDB mitigation:** Nodes continue serving with last-known topology. No failover
triggered (orchestrator cannot detect failure). Self-fencing timer starts on primary
if orchestrator unreachable.

**Risk when violated:** No automatic recovery; manual intervention required if primary fails
during orchestrator isolation.

**Test coverage:** Manual testing, `just jepsen raft-chaos`

---

### NF-5: Slow Network

**Description:** Network latency spikes or packet loss causing timeouts and retries.

**Affected components:** All distributed components

**FrogDB mitigation:** Configurable timeouts at every network boundary.
TCP backpressure for replication (no buffer limits). Scatter-gather timeout for
multi-shard operations.

**Risk when violated:** Cascade of timeouts causing unnecessary failovers or client errors.

**Test coverage:** `just jepsen slow-network`

---

## Resource Exhaustion

### RE-1: Memory OOM

**Description:** `used_memory` exceeds `max_memory`.

**Affected components:** All write paths

**FrogDB mitigation:** Write commands rejected with `-OOM`. Reads, deletes, and admin
commands continue. Eviction policies (LRU/LFU) reclaim space proactively.

**Risk when violated:** Process killed by OS OOM killer, uncontrolled data loss.

**Test coverage:** `just jepsen memory-pressure`, integration tests

---

### RE-2: Disk Full

**Description:** WAL or snapshot write fails due to insufficient disk space.

**Affected components:** `crates/persistence/src/`

**FrogDB mitigation:**
- Async mode: log warning, continue (writes at risk)
- Periodic mode: log error, queue writes, retry next sync
- Sync mode: return `-ERR persistence failed` to client
- Snapshot failure: non-fatal, server continues, uses older snapshot for recovery

**Risk when violated:** Silent data loss in Async mode if disk stays full.

**Test coverage:** `just jepsen disk-failure`

---

### RE-3: Connection Exhaustion

**Description:** Too many client connections exceed `max_connections`.

**Affected components:** `crates/server/src/connection/`

**FrogDB mitigation:** New connections rejected. Existing connections unaffected.
`rejected_connections_total` metric incremented.

**Risk when violated:** Existing connections disrupted, or no connections accepted.

**Test coverage:** Load tests via `/benchmark`

---

### RE-4: Channel Backpressure Saturation

**Description:** Shard message channel is full. Sending connection blocks indefinitely.

**Affected components:** `crates/core/src/shard/`, `crates/server/src/connection/`

**FrogDB mitigation:** Bounded channels create natural backpressure. Clients experience
latency, not errors. VLL queue has `vll_max_queue_depth` with explicit rejection.

**Risk when violated:** If channels become unbounded, OOM under load.

**Test coverage:** Load tests, `just concurrency`

---

### RE-5: VLL Queue Overflow

**Description:** VLL pending operation queue exceeds `vll_max_queue_depth`.

**Affected components:** `crates/vll/src/`

**FrogDB mitigation:** New operations rejected with `-ERR shard queue full, try again later`.
Existing queued operations continue executing.

**Risk when violated:** Unbounded queue growth leading to OOM.

**Test coverage:** Load tests

---

## Protocol Faults

### PF-1: Stale Epoch

**Description:** Node receives a command or topology update with an epoch lower than
its current epoch.

**Affected components:** `crates/cluster/src/state.rs`

**FrogDB mitigation:** Node rejects the stale message. For topology: ignore.
For FROGDB.PROMOTE: return `-EPOCH stale epoch`.

**Risk when violated:** Node reverts to stale topology, potentially becoming primary
when it shouldn't be.

**Test coverage:** `just jepsen leader-election`, `just jepsen leader-election-partition`

---

### PF-2: Replication ID Mismatch

**Description:** Replica sends PSYNC with a replication ID that doesn't match primary's
current or secondary ID.

**Affected components:** `crates/replication/src/primary.rs`

**FrogDB mitigation:** PSYNC fails, triggers FULLRESYNC. Primary sends new replication
ID and full checkpoint.

**Risk when violated:** Replica applies WAL from wrong history, diverging.

**Test coverage:** `just jepsen replication`, integration tests

---

### PF-3: WAL Sequence Gap

**Description:** Replica receives a replication frame with a sequence number that skips
expected values.

**Affected components:** `crates/replication/src/replica.rs`

**FrogDB mitigation:** Replica detects gap, logs warning, requests PSYNC from last good
sequence. May trigger FULLRESYNC if gap exceeds retention.

**Risk when violated:** Replica misses operations, diverges from primary.

**Test coverage:** `just jepsen replication-chaos`

---

### PF-4: MOVED/ASK Redirect Loops

**Description:** Client follows MOVED/ASK redirects indefinitely, bouncing between nodes
without reaching the correct one.

**Affected components:** `crates/server/src/connection/`, `crates/cluster/src/`

**FrogDB mitigation:** Clients should implement redirect limit (typically 5).
Redirect loops indicate stale or inconsistent topology; client should refresh via
CLUSTER SLOTS.

**Risk when violated:** Client hangs, potential connection exhaustion.

**Test coverage:** `just jepsen key-routing`, `just jepsen key-routing-kill`

---

### PF-5: Raft Log Divergence

**Description:** Two Raft replicas have different entries at the same log index.

**Affected components:** `crates/cluster/src/`

**FrogDB mitigation:** Raft protocol guarantees: leader's log is authoritative.
Followers with divergent entries overwrite with leader's entries.
OpenRaft implementation handles this automatically.

**Risk when violated:** State machine divergence across cluster nodes.

**Test coverage:** `just jepsen raft-chaos`

---

## Concurrency Faults

### CO-1: Deadlock (Prevented by VLL Ordering)

**Description:** Two operations hold locks that the other needs, creating a cycle.

**Affected components:** `crates/vll/src/`

**FrogDB mitigation:** VLL acquires locks in sorted shard-ID order. Total ordering
prevents circular waits. This is a **design invariant**, not a runtime check.

**Risk when violated:** System hangs — affected shards stop processing.

**Test coverage:** `just concurrency` (Shuttle explores interleavings)

---

### CO-2: Livelock

**Description:** Operations repeatedly retry but never make progress (e.g., competing
VLL transactions constantly aborting each other).

**Affected components:** `crates/vll/src/`

**FrogDB mitigation:** VLL uses global txid ordering — lower txid always wins. No
retry-based contention. Operations queue and wait rather than abort and retry.

**Risk when violated:** Throughput collapse under contention.

**Test coverage:** `just concurrency`, load tests

---

### CO-3: Mutable Reference Across `.await`

**Description:** Holding a mutable reference to shard data across an `.await` point
allows another task to observe partial state.

**Affected components:** Any async code in shard worker

**FrogDB mitigation:** Rust's borrow checker prevents most cases. However, `unsafe`
code or `Arc<Mutex>` can circumvent this. Single-task shard worker model means this
is unlikely but must be verified for any async refactoring.

**Risk when violated:** Data race, impossible intermediate states.

**Test coverage:** Rust compiler (borrow checker), `just concurrency`

---

### CO-4: spawn_blocking in Hot Path

**Description:** Using `spawn_blocking` for a synchronous operation in the shard worker
hot path. The spawned task runs on a separate thread, violating shared-nothing.

**Affected components:** `crates/core/src/shard/`

**FrogDB mitigation:** Avoid `spawn_blocking` in shard data paths. RocksDB operations
are async-compatible via batching. If blocking I/O is necessary, it should not access
shard data.

**Risk when violated:** Data race (blocking task accesses shard data from different thread),
or thread pool exhaustion.

**Test coverage:** Code review, `just concurrency`

---

### CO-5: Channel Circular Wait

**Description:** Shard A sends to shard B's channel, B sends to A's channel, both
channels are full, both tasks block.

**Affected components:** `crates/core/src/shard/`, inter-shard messaging

**FrogDB mitigation:** VLL ordering prevents this for VLL operations (always acquire
in sorted order). For non-VLL operations (snapshot, etc.), careful design ensures no
circular message patterns.

**Risk when violated:** System deadlock — affected shards permanently blocked.

**Test coverage:** `just concurrency` (Shuttle), code review

---

### CO-6: Starvation

**Description:** Low-priority operations never execute because high-priority operations
continuously preempt them.

**Affected components:** `crates/core/src/shard/`, VLL queue

**FrogDB mitigation:** VLL uses BTreeMap (FIFO within txid ordering). No priority
levels — all operations are equal. Channel processing is FIFO.

**Risk when violated:** Specific operations time out consistently under load.

**Test coverage:** Load tests via `/benchmark`
