---
title: "Consistency Model"
description: "Consistency guarantees for single-node operation, clustered deployments, and transactions — each labeled tested or design intent."
sidebar:
  order: 9
---
Consistency guarantees for single-node operation, clustered deployments, and transactions.

## How to Read This Page

FrogDB ties its consistency claims to tests wherever it can. Every guarantee below carries an evidence tag:

- **[Tested]** — a named checker or test verifies this behavior. The checker is linked.
- **[Design intent]** — the design implies this, but no dedicated test proves it.
- **[Asserted negative]** — a "not provided" boundary. This means "not offered and not tested for," not "proven impossible."

The tests referenced here are described in full on the [Testing methodology](/compatibility/testing-methodology/) page. Two checkers do most of the work: the internal WGL (Wing-Gong-Larus) linearizability checker (`frogdb-server/crates/testing/src/checker.rs`), which never reports a silent pass — when its state search is bounded out it returns an explicit *inconclusive* result rather than "linearizable" — and the [Jepsen](/compatibility/testing-methodology/) suite (Knossos and Elle checkers).

## Single-Node Guarantees

### Total Order Per Key — [Tested]
All operations on a single key are totally ordered; concurrent writes from different clients are serialized. Verified by the WGL checker running against the real server on single-key histories (`test_linearizability_concurrent_writes`, `test_linearizability_single_key_serial` in `frogdb-server/crates/server/tests/simulation.rs`) and by the Jepsen `register` workload (Knossos linearizable CAS-register).

### Per-Shard Linearizability — [Tested]
Within a single internal shard, operations are linearizable. Same evidence as above; all real-server linearizability tests are single-node and single-key.

### Read-Your-Writes — [Design intent]
A client sees its own writes on the **same connection to the same node**. This follows from per-connection in-order execution and single-key linearizability, but there is no dedicated read-your-writes test.

| Scenario | Read-Your-Writes |
|----------|------------------|
| Same connection, no failover | Expected |
| Reconnect to same node | Expected (data persisted per the durability mode) |
| Failover to replica (async) | **Not guaranteed** — unreplicated writes may be lost |

**Mitigation:** for writes that must survive failover, require replica acknowledgment with `min-replicas-to-write` and/or use `WAIT` (see [Cluster Consistency](#cluster-consistency) and [Replication](/architecture/replication/#write-quorum-and-fencing)).

### Monotonic Reads — [Design intent]
On a single connection to a single node, a client will not see an older value for a key after seeing a newer one. Implied by linearizability on the connection; no dedicated test.

### Cross-Slot Handling — [Tested]
Multi-key commands spanning multiple hash slots are rejected with a `CROSSSLOT` error before execution, matching Redis Cluster. Verified by the Jepsen `cross_slot` workload (which also confirms same-slot atomic transfers conserve their invariant). Use hash tags `{tag}` to colocate keys on one slot:

```
MSET {user:123}name Alice {user:123}email alice@example.com
```

### Cross-Slot in Standalone Mode
In **standalone mode only**, FrogDB can optionally serve atomic cross-shard operations via [VLL](/architecture/vll/):

| Setting | Cross-Shard Behavior | Atomicity |
|---------|---------------------|-----------|
| `allow_cross_slot_standalone = false` (default) | `-CROSSSLOT` error | N/A |
| `allow_cross_slot_standalone = true` | VLL coordination | Atomic |
| Cluster mode (any setting) | `-CROSSSLOT` error | N/A |

---

## Durability — [Tested]

Durability follows the configured mode (`DurabilityMode`; default `periodic`, 1000 ms).

| Mode | Write Acknowledged When | Data at Risk on Crash |
|------|------------------------|----------------------|
| `async` | Written to memory | All writes not yet flushed |
| `periodic` | Written to memory | Up to one flush interval (default ~1 s) |
| `sync` | fsync completes | None — every acknowledged write is durable |

The per-mode loss windows are verified by fsync-boundary fault-injection tests (`test_sync_mode_zero_acked_write_loss`, `test_periodic_mode_loss_bounded_by_flush_interval`, and the async-window tests in `frogdb-server/crates/persistence/src/wal/tests.rs`). These drive the real WAL flush thread and durability-mode selection, but substitute the storage backend with a page-cache model that discards committed-but-unfsynced writes on a simulated crash — the fsync boundary the modes exist to control. That is exactly the boundary the ordinary crash-recovery tests (`crash_recovery_tests.rs`) cannot exercise, because they reopen the same directory in-process and the OS page cache flushes unsynced writes anyway. A full end-to-end power-loss test against real storage (VM hard reset or block-device fault injection) remains design intent.

See [Persistence Internals](/architecture/persistence/#durability-modes) for the mechanism.

---

## Cluster Consistency

### Replication Model — [Design intent]
Replication is **asynchronous by default**: replicas re-execute a stream of logical commands from the primary (see [Replication](/architecture/replication/#command-streaming-steady-state)). Acknowledged writes eventually appear on all replicas. Lag magnitude is not a tested bound.

### Eventual Convergence — [Tested]
Replicas converge to the primary's state after a partition heals, with no value regressing on any node. Verified by the Jepsen `replication` and `partition_recovery` workloads (per-node no-regression + final all-nodes-equal convergence). These prove convergence and liveness — **not** that a specific acknowledged write survives a failover.

During failover, writes accepted by the old primary but not yet replicated may be **permanently lost**:

| Scenario | Data Fate |
|----------|-----------|
| Write replicated before failover | Preserved on the new primary |
| Write acknowledged but not replicated | Lost (bounded only by replication lag) — [Design intent] |
| Write to an isolated old primary during split-brain | Discarded and audit-logged — [Tested] |

### Reads from a Replica — [Design intent]
A replica may return a value older than the primary's; staleness is bounded only by replication lag.

---

## Split-Brain Behavior

During a partition there can be a window where an isolated old primary still accepts writes while a successor is promoted. FrogDB handles this two ways:

**Fencing (risk reduction).** Two boolean options cause a primary to stop accepting writes when it loses contact with its replicas or its Raft quorum:
- `self_fence_on_replica_loss` (default `true`) — once a primary has had a streaming replica, it rejects writes with `-CLUSTERDOWN` when no fresh replica has acknowledged within the freshness window.
- `self_fence_on_quorum_loss` (default `true`) — a node that loses its Raft quorum rejects writes with `-CLUSTERDOWN`.

There is **no fencing-timeout config knob**; fencing is governed by these booleans plus the replica-freshness and Raft election timings, not a single named timeout. `WAIT` and `min-replicas-to-write` are additional levers to make individual writes wait for replication.

**Divergent-write discard (post-mortem).** When a demoted former primary re-syncs, any writes it accepted while isolated are discarded and recorded to a `split_brain_discarded_<timestamp>.log` audit file (see [Replication → Split-Brain Handling](/architecture/replication/#split-brain-handling)).

**[Tested]:** the Jepsen `split_brain` workload verifies **at-most-one-master**, **rejection of writes to non-primaries**, and **final convergence**. It does *not* verify that specific acknowledged writes are preserved or lost within a timed failover window — that remains design intent.

---

## Guarantees Not Provided

- **Cross-node linearizability — [Asserted negative].** All linearizability checks are single-node/single-slot; no workload demonstrates (or attempts) a cross-node linearizable history.
- **Snapshot isolation — [Asserted negative].** No point-in-time cross-key consistency is offered, and no checker tests for it.
- **Causal consistency — [Asserted negative].** Causally related operations may be observed out of order by different clients; there is no causal checker.

"Not provided" means the guarantee is neither offered nor tested for — not that its absence is proven.

---

## Transaction Consistency

### MULTI/EXEC Atomicity — [Tested]
- All queued commands execute, or none do; they execute in order with no interleaving from other clients on the same keys.
- Verified by `frogdb-server/crates/server/tests/integration_transactions.rs` (basic EXEC, WATCH success/abort, DISCARD, no-partial-visibility) and by no-partial-visibility / isolation tests in `simulation.rs`.

**Limitations:** all keys in a transaction must resolve to the same internal shard (use hash tags); cross-shard transactions are not supported. There is no rollback — a command that fails at runtime does not undo earlier commands in the transaction (matching Redis).

### Transaction Durability — [Tested]
A transaction is applied as a single RocksDB `WriteBatch` (atomic at the storage layer), so its durability follows the [durability mode](#durability--tested): `async`/`periodic` return before the batch is durable, `sync` blocks until fsync.

### WATCH — [Tested]
`WATCH` provides optimistic locking: `EXEC` aborts (returns nil) if a watched key was modified by another client. Watched keys must be on the same internal shard as the transaction's keys. Verified in `integration_transactions.rs` (`test_watch_exec_success`, `test_watch_exec_abort`, and the PFADD watch-version regression tests).

### Checkpoint (BGSAVE) Cross-Shard Cut — [Tested]
A `BGSAVE` checkpoint is a single RocksDB `Checkpoint` over one shared database, so it captures an atomic point-in-time image at one RocksDB sequence number across every internal shard's column family. Before the cut, `BGSAVE` drains every shard's WAL flush engine into RocksDB, so the image is a committed prefix of each shard's history rather than a partial mid-flush state.

- **Single-shard transactions are never torn by the cut.** A single-shard `MULTI`/`EXEC` runs as one shard event-loop step that enqueues all of its writes before the pre-snapshot drain runs, and RocksDB commits in sequence order, so a checkpoint captures either all of a transaction's writes or none. This matters because a torn single-shard transaction in a recovery image would resurrect a half-applied transaction that no client ever observed.
- **Cross-shard writes may be torn by the cut.** A cross-shard `MSET`/scatter dispatches independent per-shard writes, and the drain is likewise per-shard, so a checkpoint can capture one shard's half of a cross-shard write and not another's. This matches FrogDB's cross-shard model — execution atomicity via locking, without failure/durability atomicity (see [Cross-Slot in Standalone Mode](#cross-slot-in-standalone-mode) and the cross-shard transaction framing tracked for the durability phase). Within any one shard, the subset of a cross-shard write that lands on that shard is still applied atomically, so a shard's own portion is never itself torn.

Verified in `integration_persistence.rs` (`test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave`, `test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave`, and `test_concurrent_bgsave_stress_restores_cleanly`), which hammer transactions and cross-shard `MSET`s concurrently with repeated `BGSAVE`, then restore the resulting checkpoint and assert the cut preserved single-shard and per-shard atomicity.

---

## Ordering Guarantees

### Within a Single Connection — [Design intent]
Commands execute in the order sent; pipelining preserves order; responses return in order.

### Across Connections — [Asserted negative]
No ordering is guaranteed between different clients beyond per-key serialization.

### Pub/Sub Message Ordering — [Design intent]
Messages are delivered in publish order per channel, with no ordering across channels and at-most-once delivery (messages may be lost on reconnect).
