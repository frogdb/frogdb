---
title: "Replication Internals"
description: "Contributor-facing documentation for FrogDB's primary-replica replication: the PSYNC handshake, RocksDB checkpoint transfer, logical command streaming, and write fencing."
sidebar:
  order: 7
---
Contributor-facing documentation for FrogDB's primary-replica replication: the PSYNC handshake, checkpoint transfer, logical command streaming, and write fencing.

For operator-facing setup and failover procedures, see [Operations: Replication](/operations/replication/).

---

## Overview

A primary accepts writes; each replica first receives a full copy of the dataset, then a continuous stream of replicated commands. Two distinct mechanisms are involved:

- **Initial sync is a RocksDB checkpoint transfer** — the primary cuts a checkpoint and streams its files to the replica.
- **Steady-state replication is logical command replication** — each write is re-serialized to RESP and re-executed on the replica.

Roles and offsets are tracked per connection. This page covers the data-plane mechanics; [Clustering Internals](/architecture/clustering/) covers how a Raft-coordinated cluster reuses this same data path, and [Operations: Replication](/operations/replication/) covers `REPLICAOF`/`ROLE` and runbooks.

---

## Replication Identity

Each dataset history has a unique **replication ID**: 20 random bytes rendered as 40 hex characters (`generate_replication_id`, `REPLICATION_ID_LEN = 40`, in `frogdb-server/crates/replication/src/state.rs`). A validity check requires exactly 40 ASCII hex digits.

On promotion, a replica generates a *new* replication ID and remembers the old one as a **secondary ID** so the former primary's other replicas can re-attach without a full resync:

```
Primary A (replication_id: abc123...)
    |
    +-- Replica B (tracking abc123...)
    |
    [Primary A fails, B promoted]
    |
Primary B (replication_id: def456..., secondary_id: abc123...)
```

`new_replication_id()` captures `secondary_id: Option<String>` (the old ID) and `secondary_offset: i64` (the offset at promotion; `-1` sentinel when unset), then regenerates `replication_id`. A reconnecting replica is eligible for a partial (offset-resume) sync when its requested `(id, offset)` falls inside the primary's window — `window_contains(requested_id, requested_offset, current_offset)` returns true when the ID matches the current replication ID and the requested offset is not ahead of the live offset, or matches the secondary ID within `secondary_offset`. Otherwise a full resync is required.

---

## PSYNC Handshake

The replica drives the handshake (`frogdb-server/crates/replication/src/replica/connection.rs`):

```
Replica                              Primary
   |                                    |
   |-- REPLCONF listening-port <port> ->| (for INFO output)
   |<- +OK ----------------------------|
   |                                    |
   |-- REPLCONF capa eof capa psync2 ->| (announce capabilities)
   |<- +OK ----------------------------|
   |                                    |
   |-- REPLCONF frogdb-version <ver> ->|
   |<- +OK ----------------------------|
   |                                    |
   |-- PSYNC <repl_id> <offset> ------>| (fresh replica sends PSYNC ? -1)
   |<- +FULLRESYNC <id> <off>  --------|  or  +CONTINUE <id>
```

Notes verified against source:
- The capability line sends **two** `capa` tokens: `capa eof capa psync2`.
- There is **no AUTH step** in this handshake code path.
- `+FULLRESYNC <id> <offset>` triggers a checkpoint transfer; `+CONTINUE <id>` resumes streaming from the replica's offset.

---

## Full Synchronization (Checkpoint Transfer)

A full sync happens for a new replica, or when a replica has fallen outside the primary's replay window. The primary (`handle_full` in `replica_session.rs`) creates a **per-replica** RocksDB checkpoint directory (`fullsync_<replica_id>`) — there is no shared, amortized checkpoint across concurrent replicas — and streams it. The primary's session moves through a `Phase` state machine: `Connecting → PreparingCheckpoint → StreamingCheckpoint → Streaming → Disconnecting`.

### Wire framing

The stream (`CheckpointStreamCodec`, `fullsync.rs`) is:

```
1. Prelude:      $FROGDB_CHECKPOINT\r\n <file_count>\r\n
2. Per file (sorted by name):
                 <file header: name, size>
                 <raw file bytes>
3. Metadata trailer:
                 <rdb_size>:<checksum_hex>:<replication_id>:<replication_offset>
```

### Integrity

The checksum is a **hash of hashes**, not a hash of the concatenated bytes: the primary SHA256s each file, then folds `SHA256(name_0 || hash_0 || name_1 || hash_1 || …)` in wire (name-sorted) order into a final 32-byte digest (64 hex chars). The replica recomputes it while receiving; on mismatch it deletes the incoming directory and errors (a fresh full sync happens on the next reconnect — there is no inline retry). On success the checkpoint is staged and committed by an atomic rename; the replica applies it on restart.

When the store is not RocksDB-backed, an RDB fallback path is used instead of a checkpoint.

---

## Command Streaming (Steady State)

After the initial sync, replication is **logical command replication**. Each write on the primary is serialized to a RESP2 array (`serialize_command_to_resp`), wrapped in a `ReplicationFrame` (magic `FRPL`, version 2, 20-byte header carrying `shard_id` and a monotonic `sequence`), and published to a bounded tokio broadcast channel (`broadcast::channel(10000)` in `primary/mod.rs`). An `OffsetCoordinator` assigns the offset that advances `master_repl_offset`.

Replicas (`replica/streaming.rs`) decode each frame and **re-execute the command** against their own store, routing it to the origin shard tagged on the frame and running it under a reserved internal connection id (`REPLICA_INTERNAL_CONN_ID`) so the replayed write is not itself re-broadcast. MULTI/EXEC groups apply atomically.

**Determinism.** Because replicas re-execute the literal command, correctness depends on the commands replicating deterministically as written. FrogDB does **not** rewrite nondeterministic commands to a canonical form before broadcast (there is no `SPOP`→`SREM` or `EXPIRE`→`PEXPIREAT` rewriting layer). The primary broadcasts the executed verb and arguments verbatim; a command that cannot replicate safely is instead tagged with the `NO_PROPAGATE` flag, which suppresses its replication entirely rather than rewriting it.

---

## Backpressure and Lag

Replication buffers are **bounded**, and a replica that cannot keep up is disconnected rather than allowed to stall the primary indefinitely:

- The write-stream is served from a shared broadcast channel with a fixed capacity (`10000` frames). A replica whose consumer falls behind receives `RecvError::Lagged` and its session **breaks** — it must reconnect, and full-resync if its offset has fallen out of the replay window.
- An optional per-write `write_timeout_ms` disconnects a replica whose socket write stalls beyond the timeout.

This is the same "resync a fallen-behind replica" model as Redis; FrogDB does not attempt unbounded buffering.

---

## Write Quorum and Fencing

Two independent mechanisms let a primary care about replica acknowledgment:

**`WAIT numreplicas timeout`** blocks until at least `numreplicas` replicas have acknowledged the connection's last write, then returns the acknowledged count. It is served by `wait_for_acks(sequence, min_replicas) -> u32` (`tracker.rs`), which returns a plain ack count and **never errors on timeout** — the timeout is imposed by the caller and, on expiry, `WAIT` simply returns however many replicas had acked. Config `min-replicas-to-write` (default `0`) and `min-replicas-timeout-ms` (default `5000`) express the same intent declaratively.

**Quorum fencing** is separate. `ReplicationQuorumChecker` (`frogdb-server/crates/server/src/replication_quorum.rs`) arms the first time a replica reaches the streaming phase and stays armed thereafter. Once armed, if no fresh streaming replica has acked within the freshness window, writes are rejected with `-CLUSTERDOWN` ("quorum lost, writes rejected"). This is gated by `self-fence-on-replica-loss` (default `true`). There is no `-NOREPL` error in FrogDB.

---

## Split-Brain Handling

When a partition heals, a demoted former primary may hold **divergent writes** it accepted while isolated. Those writes are captured in a bounded ring buffer (`primary/ring_buffer.rs`: `ReplicationRingBuffer`, a `VecDeque` with FIFO eviction; default enabled, `max_entries` 10000, `max_bytes` 64 MiB). On demotion and re-sync they are **discarded** and recorded to an audit file `split_brain_discarded_<timestamp>.log` (`split_brain_log.rs`). The header records `old_primary`, `new_primary`, `epoch_old`, `epoch_new`, the divergent sequence range, and `ops_discarded`, followed by the raw RESP bytes of the discarded operations.

This is a post-mortem audit artifact, not a hot-path fence. See [Consistency Model](/architecture/consistency/) for the guarantee-level discussion of what a split brain can and cannot lose.

---

## Observability

Replication state is exposed through the `INFO replication` section (`frogdb-server/crates/server/src/commands/info.rs`), whose fields mirror Redis: `role`, `connected_slaves`, per-slave `slave<i>:ip=…,port=…,state=…,offset=…,lag=…` lines, `master_failover_state`, `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, and the `repl_backlog_*` fields. See [Operations: Replication](/operations/replication/) for monitoring guidance and the [Metrics reference](/reference/metrics/) for exported metrics.
