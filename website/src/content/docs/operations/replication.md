---
title: "Replication"
description: "Set up FrogDB primary-replica replication, configure write quorum, and monitor replication state."
sidebar:
  order: 3
---

FrogDB implements asynchronous primary-replica replication using the Redis PSYNC2 protocol. The standard replication commands (`REPLICAOF`, `ROLE`, `INFO replication`) behave as documented by the [Redis replication docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/); this page covers setup, write quorum, the sync mechanism, and monitoring. Cluster metadata and failover coordination live one layer up in [Clustering](/operations/clustering/); replication is the data plane beneath it.

## How FrogDB differs from Redis replication

- **Raft-coordinated or manual failover.** FrogDB has no Sentinel and no external orchestrator. In cluster mode a Raft leader can promote a replica automatically (`auto-failover`, see [Clustering](/operations/clustering/)); otherwise promotion is manual via `CLUSTER FAILOVER`.
- **RocksDB checkpoints for full resync** instead of an RDB transfer. The primary streams a consistent RocksDB checkpoint rather than serializing an RDB file.
- **SHA256 integrity verification.** The checkpoint payload carries a SHA256 checksum that the replica verifies on receipt.
- **Replicas do not evict independently.** Eviction decisions are made on the primary and applied on replicas through the replicated write stream, not recomputed locally.
- **Memory-aware full sync.** A full resync is bounded by `fullsync-max-memory-mb`; a `FULLRESYNC` is rejected when the buffered payload would exceed that limit.

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
| `fullsync-timeout-secs` | `300` | Maximum duration of a full sync. |
| `fullsync-max-memory-mb` | `512` | Buffering cap for full sync; a `FULLRESYNC` over this is rejected. |
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

A replica that cannot resume from its saved offset performs a **full sync**. The primary produces a consistent RocksDB checkpoint and streams it in 64 KB chunks; a SHA256 checksum computed over the payload is sent with the trailing metadata and verified by the replica before the data is accepted. Full sync is memory-bounded by `fullsync-max-memory-mb` — if servicing the `FULLRESYNC` would exceed that cap, the request is rejected rather than risking an out-of-memory condition. Checkpoint internals are covered in [Persistence & durability](/operations/persistence/).

A replica that is only briefly behind performs a **partial sync**: it sends `PSYNC <replication-id> <offset>`, and if the primary still holds the required history it replies `CONTINUE` and resumes streaming write frames from that offset. The replication ID and offset are persisted in `state-file` so a replica can attempt partial sync across a restart.

On promotion a new primary begins a new replication history and other replicas re-synchronize against it. FrogDB tracks a secondary replication ID field for compatibility, but the promotion hand-off should be treated qualitatively — do not rely on a specific secondary-ID continuation behavior.

## Failover

FrogDB does not ship an external failover controller. Promotion happens one of two ways:

- **Raft-coordinated** — in cluster mode with `auto-failover = true`, the Raft leader promotes a replica after a primary is marked failed, choosing among candidates by `replica-priority`. See [Clustering](/operations/clustering/) for the control-plane configuration and failure-mode details.
- **Manual** — `CLUSTER FAILOVER` promotes a replica on demand.

In both cases the promoted node becomes a primary with a fresh replication history, and surviving replicas reconnect to it via PSYNC.

## Monitoring

Query replication state with:

```bash
redis-cli INFO replication
```

A primary reports `role:master`, `connected_slaves`, `master_replid`, `master_replid2`, `master_repl_offset`, `second_repl_offset`, and the `repl_backlog_*` fields. A replica additionally reports `master_host`, `master_port`, and `master_link_status`.

To measure lag, compare `master_repl_offset` on the primary with `master_repl_offset` reported by the replica — **both sides expose the offset under the same field name** (`master_repl_offset`); there is no `slave_repl_offset` field. An offset that falls persistently behind the primary's is the signal to act on, alongside the primary's `connected_slaves` count and any write rejections from self-fencing. Note that `master_link_status` currently always reports `up` and is not yet a usable health signal — do not alert on it; rely on the offset gap and the primary-side indicators instead.

| Symptom | Likely cause | What to check |
|---|---|---|
| Repeated full resyncs | Replica offset falls outside retained history, or lag guards trip | Review `replication-lag-threshold-bytes` / `-secs` and `fullresync-cooldown-secs`; check network stability between primary and replica. |
| Writes rejected on the primary | Self-fencing after loss of replica ACK freshness | Confirm replicas are connected and ACKing; check `replica-freshness-timeout-ms` relative to `ack-interval-ms`. |
| `FULLRESYNC` rejected | Full-sync payload exceeds `fullsync-max-memory-mb` | Raise `fullsync-max-memory-mb`, or provision more replica memory. |
| Replica out of memory during sync | Dataset exceeds replica capacity | Provision the replica with headroom above the primary's footprint. |

The [Metrics reference](/reference/metrics/) lists the exported Prometheus metrics.

## See also

- [Clustering](/operations/clustering/) — Raft control plane and failover.
- [Persistence & durability](/operations/persistence/) — checkpoint and recovery internals.
- [Configuration reference](/reference/configuration/) — every config parameter.
- [Security](/operations/security/) — encrypting replication traffic (`tls-replication`).
