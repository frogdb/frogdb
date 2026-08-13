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
                                        |  or  -ERR PSYNC refused (version gate)
```

Notes verified against source:
- The capability line sends **two** `capa` tokens: `capa eof capa psync2`.
- There is **no AUTH step** in this handshake code path.
- `+FULLRESYNC <id> <offset>` triggers a checkpoint transfer; `+CONTINUE <id>` resumes streaming from the replica's offset.

### Version compatibility gate

The announced `frogdb-version` is checked at the `PSYNC`, not at the `REPLCONF` that carries it — a replica is free to ignore an error on an option and send `PSYNC` anyway, and `PSYNC` is the point where the primary would otherwise commit a session, a resync counter and possibly a checkpoint directory. The rule lives in `frogdb-server/crates/replication/src/version_compat.rs`:

- **Different major version: refused.** The primary answers `-ERR PSYNC refused - …`, naming both versions and both majors, and closes the handshake. A FrogDB replica logs the same text, so the reason is readable from either node. FrogDB's checkpoint carries a RocksDB column-family layout rather than a self-describing RDB, so a pair that cannot agree on a major version cannot be made to work by transferring the data and finding out.
- **Same major, different minor: served, with one warning per replica session** on the primary naming both versions. This is the normal state part-way through a rolling upgrade, so it does not block replication, but it is the signal that the rollout is not finished.
- **Patch-level differences: served silently.**
- **No announced version, or one that does not parse: served, with a warning.** A pre-option replica and a non-FrogDB peer both look like this, and refusing them would drop connections on a suspicion rather than on a proven incompatibility. The gate refuses only what it can prove.

Versions compare on their numeric core, so a pre-release or build suffix (`2.0.0-rc1`) is compared as `2.0`.

---

## Full Synchronization (Checkpoint Transfer)

A full sync happens for a new replica, when a replica has fallen outside the primary's replay window, or when the window has been freed because no replica was connected for `repl-backlog-ttl` seconds (default 3600, `0` disables; the same idle-expiry Redis runs from `replicationCron`). Freeing the backlog moves no offset and no replication id — it only withdraws the cheap resume path, so the next `PSYNC` is answered `+FULLRESYNC`. The primary (`handle_full` in `replica_session.rs`) creates a **per-replica** RocksDB checkpoint directory (`fullsync_<replica_id>`) — there is no shared, amortized checkpoint across concurrent replicas — and streams it. The primary's session moves through a `Phase` state machine: `Connecting → PreparingCheckpoint → StreamingCheckpoint → Streaming → Disconnecting`.

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

**A failed apply ends the history it happened on.** If a replicated group is rejected by the shard, the replica's keyspace no longer matches the primary's at that offset, so the failure is latched on the history (`apply.rs` → `ReplicaApplyStint::admit_divergence`) rather than logged and stepped over. Every later frame of that history is dropped without reaching a shard, the applied offset stops advancing (so the ACK keeps reporting the last offset that really applied and `WAIT` times out instead of lying), and the connection drops its link and rewinds so the reconnect is answered `+FULLRESYNC`. Installing that dataset clears the latch and the replica resumes. Reads keep being served during the window, as in Redis's `replica-serve-stale-data yes` default.

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

**`WAIT numreplicas timeout`** blocks until at least `numreplicas` replicas have acknowledged the connection's last write, then returns the acknowledged count. The replication decision — offset snapshot, immediate quorum check, `REPLCONF GETACK` solicitation, quorum-or-deadline wait — belongs to `WaitCoordinator` (`wait_coordinator.rs`); the connection handler owns only argument validation, the replica rejection, and the `CLIENT UNBLOCK` race. A timeout is **not** an error: `WAIT` returns however many replicas had acked. The one error verdict is a role change — a wait parked across a demotion is released with `-UNBLOCKED … instance state changed (master -> replica?)`, because a count would describe a replication history the node no longer heads. Config `min-replicas-to-write` (default `0`) and `min-replicas-timeout-ms` (default `5000`) express a related intent declaratively, but enforce it differently: they are a *pre*-dispatch gate in `run_pre_checks`, rejecting a write with `-NOREPLICAS` when too few replicas have ACKed within the freshness window, rather than waiting after the fact.

**What an acknowledgment means.** A replica acks the offset it has *applied*, never the offset it has merely received. When `WAIT` counts a replica at offset `N`, every frame at or below `N` has been executed against that replica's shards and is readable there — a replica with frames still queued for its apply loop acks the lower, already-applied offset instead. This costs a little latency (a replica answers `REPLCONF GETACK` once the frames it holds have landed, and otherwise reports progress on its own `replication.ack-interval-ms` tick, so an ack can trail the apply by up to one tick) and buys the guarantee that a `WAIT`-confirmed write is not sitting in a queue that a crash would discard. Acknowledgment does **not** imply the replica has persisted the write: the applied write is in the replica's store, subject to the same durability settings as any local write.

`WAIT` is **per-node and keyless in every mode**: it counts the replicas attached to the node that received it and never redirects. Cluster mode adds no special case — see [Clustering Internals: `WAIT` semantics](/architecture/clustering/#wait-semantics) for the full contract, including why a cluster-wide guarantee is a client-side fan-out.

**Quorum fencing** is separate. `ReplicationQuorumChecker` (`frogdb-server/crates/replication-runtime/src/quorum.rs`) arms lazily on the write path: each write's quorum check (`arm_if_streaming`, called only from `has_quorum`) latches the fence the first time it observes a streaming replica — a replica that reaches streaming and disconnects before any write is checked never arms it. Once armed, if no fresh streaming replica has acked within the freshness window, writes are rejected with `-SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)`. The error names both the mechanism and the knob that turns it off, `self-fence-on-replica-loss` (default `true`), so an operator can act on it without tracing the write path; the Raft-mode `-CLUSTERDOWN` belongs to the separate cluster fence. There is no `-NOREPL` error in FrogDB.

The latch is dropped when the last streaming replica leaves **cleanly** — an orderly connection close, `REPLICAOF NO ONE` on the replica, a primary-initiated teardown. A replica that is decommissioned on purpose therefore leaves the primary writable, while one that is killed, partitioned away, or disconnected for lag leaves it fenced. A replica that is still attached but has stopped acking has *not* departed, and keeps the fence engaged: that partition is what the fence exists for.

Both knobs are live: the toggle and `replica-freshness-timeout-ms` are atomics read at each write pre-check, so `CONFIG SET` applies from the next command without a restart. Two consequences worth planning around:

- **Enabling fences immediately, not after a grace period.** Arming is latched independently of the toggle, so a primary that has already served a replica and since lost it starts rejecting writes on the *next* command after `CONFIG SET replication-self-fence-on-replica-loss yes` — the same immediacy `min-replicas-to-write` has. The transition logs a `warn!` naming the reason when it fences on the spot.
- **An engaged fence is attributable.** While writes are being fenced, `/status` (and `STATUS`) report `cluster.write_fence` — `"replica quorum lost"` for this fence, `"cluster quorum lost"` for the Raft-mode `cluster-self-fence-on-quorum-loss` gate. The field is absent whenever writes are not fenced, so it never has to be interpreted against a healthy node.

Demoting a primary (`REPLICAOF host port`) un-latches arming, so a later re-promotion does not inherit a fence from replica sessions that are no longer this node's.

**Seams exist on every role.** The replica tracker, the primary-side replication handler, and the quorum checker are constructed at boot regardless of the configured role; behavior is gated at the point of use on the live role flag (`RoleManager`'s single process-wide atomic), not on whether an object exists. A node promoted at runtime therefore enforces `min-replicas-to-write`, `self-fence-on-replica-loss`, `replica-freshness-timeout-ms`, and the lag thresholds — including values `CONFIG SET` while it was still a replica — and serves `PSYNC` to downstream replicas, with nothing rebuilt and no publication that has to reach already-open connections. While a node is a replica the same seams are inert: its tracker is empty (so the fence is permissive), `PSYNC` is refused, `INFO replication` renders from the replica handler, and the primary handler never writes the shared replication state file.

---

## Split-Brain Handling

When a partition heals, a demoted former primary may hold **divergent writes** it accepted while isolated. Those writes are captured in a bounded ring buffer (`primary/ring_buffer.rs`: `ReplicationRingBuffer`, a `VecDeque` with FIFO eviction; default enabled, `max_entries` 10000, `max_bytes` 64 MiB). On demotion and re-sync they are **discarded** and recorded to an audit file `split_brain_discarded_<timestamp>.log` (`split_brain_log.rs`). The header records `old_primary`, `new_primary`, `epoch_old`, `epoch_new`, the divergent sequence range, and `ops_discarded`, followed by the raw RESP bytes of the discarded operations.

This is a post-mortem audit artifact, not a hot-path fence. See [Consistency Model](/architecture/consistency/) for the guarantee-level discussion of what a split brain can and cannot lose.

---

## Observability

Replication state is exposed through the `INFO replication` section (`frogdb-server/crates/server/src/commands/info.rs`), whose fields mirror Redis: `role`, `connected_slaves`, per-slave `slave<i>:ip=…,port=…,state=…,offset=…,lag=…` lines, `master_failover_state`, `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, and the `repl_backlog_*` fields. See [Operations: Replication](/operations/replication/) for monitoring guidance and the [Metrics reference](/reference/metrics/) for exported metrics.
