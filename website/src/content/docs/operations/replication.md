---
title: "Replication"
description: "Set up FrogDB primary-replica replication, configure write quorum, and monitor replication state."
---

FrogDB implements asynchronous primary-replica replication using the Redis PSYNC2 protocol. The standard replication commands (`REPLICAOF`, `ROLE`, `INFO replication`) behave as documented by the [Redis replication docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/); this page covers setup, write quorum, the sync mechanism, and monitoring. Cluster metadata and failover coordination live one layer up in [Clustering](/operations/clustering/); replication is the data plane beneath it.

## How FrogDB differs from Redis replication

- **Raft-coordinated or manual failover.** FrogDB has no Sentinel and no external orchestrator. In cluster mode a Raft leader can promote a replica automatically (`auto-failover`, see [Clustering](/operations/clustering/)); otherwise promotion is manual via `CLUSTER FAILOVER`.
- **RocksDB checkpoints for full resync** instead of an RDB transfer. The primary streams a consistent RocksDB checkpoint rather than serializing an RDB file.
- **SHA256 integrity verification.** The checkpoint payload carries a SHA256 checksum that the replica verifies on receipt.
- **Replicas do not evict independently.** Eviction decisions are made on the primary and applied on replicas through the replicated write stream, not recomputed locally.
- **Memory-aware full sync.** A full resync is bounded by a fixed internal buffering cap; a `FULLRESYNC` is rejected when the buffered payload would exceed that limit rather than risking an out-of-memory condition.
- **No chained replication (replica-of-replica).** Redis allows `REPLICAOF` to target another replica, fanning the write stream out transitively. FrogDB does not support this: `PSYNC` is served only by a node whose live role is primary, whether it booted as one, runs standalone, or was promoted. Pointing `REPLICAOF` at a node that is currently a replica is accepted at the command layer — the sub-replica's role flips and it keeps retrying — but every `PSYNC` attempt against that target is rejected with `-ERR PSYNC not supported - server is not running as primary`, so `master_link_status` never reaches `up` and no data flows. This keeps every replica's write stream one hop from the authoritative history, so a mid-chain node cannot stall or reorder replication for the nodes behind it. Promoting the middle node with `REPLICAOF NO ONE` makes it serve `PSYNC` immediately (see [Failover](#failover)), but it stops following anything at that point, so the result is a second primary rather than a chain. Point every replica directly at the primary.

  The same rule governs cluster mode, where the roles come from Raft instead of `REPLICAOF`: `CLUSTER REPLICATE` onto a node that is itself a replica produces the same never-linking sub-replica, and a node promoted by `CLUSTER FAILOVER` or auto-failover serves `PSYNC` to its own replicas from the moment the promotion reaches its data path. See [Clustering Internals: Data Replication and `WAIT`](/architecture/clustering/#data-replication-and-wait).

## Configuration

Replication is configured under the `[replication]` section. A minimal replica points at its primary:

```toml
[replication]
role = "replica"                 # standalone | primary | replica
primary-host = "primary.internal"
primary-port = 6379
```

A primary that requires acknowledged replicas before returning success to writers:

```toml
[replication]
role = "primary"
min-replicas-to-write = 1
min-replicas-timeout-ms = 5000
```

`REPLICAOF <host> <port>` (and `REPLICAOF NO ONE`) also changes the role at runtime without a restart.

The full key set and defaults:

| Key | Default | Purpose |
|---|---|---|
| `role` | `standalone` | `standalone`, `primary`, or `replica`. |
| `primary-host` | `""` | Primary to connect to; required when `role = "replica"`. |
| `primary-port` | `6379` | Primary port for a replica. |
| `min-replicas-to-write` | `0` | Replicas that must acknowledge a write before it returns success (`0` disables). |
| `min-replicas-timeout-ms` | `5000` | How long a write waits for those acknowledgements. |
| `ack-interval-ms` | `1000` | How often a replica sends `REPLCONF ACK`. |
| `state-file` | `replication_state.json` | Persists the replication ID and offset for partial-sync recovery. |
| `connect-timeout-ms` | `5000` | Replica-to-primary connect timeout. |
| `handshake-timeout-ms` | `10000` | Replication handshake timeout. |
| `reconnect-backoff-initial-ms` | `100` | Initial reconnect backoff. |
| `reconnect-backoff-max-ms` | `30000` | Maximum reconnect backoff. |
| `replication-lag-threshold-bytes` | `0` | Proactively disconnect a replica lagging by this many bytes (`0` disables). |
| `replication-lag-threshold-secs` | `0` | Proactively disconnect a replica whose last ACK is this old (`0` disables). |
| `fullresync-cooldown-secs` | `60` | Cooldown after a lag-triggered disconnect before another is allowed. |
| `self-fence-on-replica-loss` | `true` | Reject writes when the primary loses ACK freshness from all replicas. |
| `replica-freshness-timeout-ms` | `3000` | ACK-freshness window for self-fencing; the build warns if this is below `3 × ack-interval-ms`. |
| `replica-write-timeout-ms` | `5000` | Write timeout when streaming to a replica (`0` disables); forces a disconnect when packets are silently dropped. |
| `split-brain-log-enabled` | `true` | Log divergent writes from a demoted primary before resync (logging only). |
| `split-brain-buffer-size` | `10000` | Recent commands buffered for split-brain detection. |
| `split-brain-buffer-max-mb` | `64` | Memory cap for the split-brain buffer. |

See the [Configuration reference](/reference/configuration/) for the authoritative, generated list.

## Write quorum (`min-replicas-to-write`)

When `min-replicas-to-write` is greater than zero, a write on the primary waits for that many replicas to acknowledge before it returns success. The wait is bounded by `min-replicas-timeout-ms`. This is a best-effort durability signal, not a hard quorum: if the timeout elapses before enough replicas acknowledge, **the write still proceeds** and returns with however many acknowledgements it received. Use it to reduce the window of unreplicated writes, not as a guarantee that a write reached N replicas.

Self-fencing is a separate, stricter guard. With `self-fence-on-replica-loss` enabled (the default), a primary that stops receiving fresh ACKs from all of its replicas within `replica-freshness-timeout-ms` rejects further writes rather than accepting writes it cannot replicate — the case that would otherwise produce a diverging "zombie" primary during a partition. Keep `replica-freshness-timeout-ms` at least `3 × ack-interval-ms`; the server logs a warning at startup otherwise, because a tighter window causes spurious rejections.

## Full sync and partial sync

A replica that cannot resume from its saved offset performs a **full sync**. The primary produces a consistent RocksDB checkpoint and streams it in 64 KB chunks; a SHA256 checksum computed over the payload is sent with the trailing metadata and verified by the replica before the data is accepted. Full sync is memory-bounded by a fixed internal buffering cap — if servicing the `FULLRESYNC` would exceed that cap, the request is rejected rather than risking an out-of-memory condition. Checkpoint internals are covered in [Persistence & durability](/operations/persistence/).

A replica that is only briefly behind performs a **partial sync**: it sends `PSYNC <replication-id> <offset>`, and if the primary still holds the required history it replies `CONTINUE` and resumes streaming write frames from that offset. The replication ID and offset are persisted in `state-file` so a replica can attempt partial sync across a restart.

### Replication IDs across a promotion

A promoted node mints a fresh `master_replid` and keeps the history it inherited in `master_replid2`, with `second_repl_offset` recording the offset at which the hand-off happened. A replica that was following the old primary can therefore present the *old* ID in its `PSYNC`, and the promoted node still answers `CONTINUE` as long as the requested offset falls inside the inherited window and its backlog still holds the bytes. Without that window every surviving replica would have to full-sync after a failover, which is exactly the load spike a failover can least afford.

The window is closed on the offset itself: an offset up to and including `second_repl_offset` is served from the inherited history, and anything past it is a diverged position that gets `FULLRESYNC`. (Redis stores this field as the first offset of the *new* history — one greater — so compare the two values with that off-by-one in mind when reading `INFO` from a mixed fleet.)

A node drops its own secondary window the moment it adopts someone else's history — on a `FULLRESYNC` or a checkpoint install — because at that point it no longer holds the old stream's data and a `CONTINUE` against the old ID would ship a tail from the new history. `master_replid2` therefore never outlives the history it describes; when there is no window, `INFO` reports an all-zero `master_replid2` and `second_repl_offset:-1`.

## Failover

FrogDB does not ship an external failover controller. Promotion happens one of two ways:

- **Raft-coordinated** — in cluster mode with `auto-failover = true`, the Raft leader promotes a replica after a primary is marked failed, choosing among candidates by `replica-priority`. See [Clustering](/operations/clustering/) for the control-plane configuration and failure-mode details.
- **Manual** — `CLUSTER FAILOVER` promotes a replica on demand.

In both cases the promoted node becomes a primary with a fresh replication history and serves `PSYNC` to the surviving replicas. Point them at it with `REPLICAOF <new-primary-host> <new-primary-port>`; each one is resumed with `CONTINUE` when it is still inside the inherited window described in [Replication IDs across a promotion](#replication-ids-across-a-promotion), and falls back to a full sync when it is not — because it was ahead of the promoted node, or because the bytes it needs have already aged out of the backlog. The fallback is automatic; no operator step distinguishes the two paths.

## Monitoring

Query replication state with:

```bash
redis-cli INFO replication
```

A primary reports `role:master`, `connected_slaves`, `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, and the `repl_backlog_*` fields. A replica additionally reports `master_host`, `master_port`, and `master_link_status`.

To measure lag, compare `master_repl_offset` on the primary with `master_repl_offset` reported by the replica — **both sides expose the offset under the same field name** (`master_repl_offset`); there is no `slave_repl_offset` field. An offset that falls persistently behind the primary's is the signal to act on, alongside the primary's `connected_slaves` count and any write rejections from self-fencing.

`master_link_status` reports `up` once the replica has completed the handshake and is actively streaming from the primary, and `down` at any other time — including before the initial connection completes, during a resync, and after the link drops (the replica keeps retrying with backoff; `up` returns once it reconnects and resumes streaming). It's a connectivity signal, not a data-freshness one: `down` means no data is currently arriving over the link, but doesn't by itself say how stale the replica's data is — `master_repl_offset` and the offset gap are what answer that. Alert on `master_link_status:down` alongside the offset gap and the primary-side indicators, not as a standalone signal.

| Symptom | Likely cause | What to check |
|---|---|---|
| Repeated full resyncs | Replica offset falls outside retained history, or lag guards trip | Review `replication-lag-threshold-bytes` / `-secs` and `fullresync-cooldown-secs`; check network stability between primary and replica. |
| Writes rejected on the primary | Self-fencing after loss of replica ACK freshness | Confirm replicas are connected and ACKing; check `replica-freshness-timeout-ms` relative to `ack-interval-ms`. |
| `FULLRESYNC` rejected | Full-sync payload exceeds the internal buffering cap | Provision more replica memory, or reduce the primary's dataset footprint. |
| Replica out of memory during sync | Dataset exceeds replica capacity | Provision the replica with headroom above the primary's footprint. |

The [Metrics reference](/reference/metrics/) lists the exported Prometheus metrics.

## See also

- [Clustering](/operations/clustering/) — Raft control plane and failover.
- [Persistence & durability](/operations/persistence/) — checkpoint and recovery internals.
- [Configuration reference](/reference/configuration/) — every config parameter.
- [Security](/operations/security/) — encrypting replication traffic (`tls-replication`).
