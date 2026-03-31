---
title: "Replication"
description: "FrogDB primary-replica replication for high availability."
sidebar:
  order: 3
---

FrogDB supports primary-replica replication using an orchestrated model (not gossip). Standard Redis replication commands (`REPLICAOF`, `ROLE`, `INFO replication`) work as expected — see [Redis replication docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/).

## How FrogDB Differs from Redis Replication

- **Orchestrated failover** — an external orchestrator manages topology, not Sentinel or gossip
- **RocksDB checkpoints** for full resync instead of RDB transfer
- **SHA256 integrity verification** during checkpoint streaming
- **Replicas do not evict independently** — all eviction decisions come from the primary via replicated DEL commands
- **Memory-aware sync** — replicas monitor memory during full sync and can pause/abort if limits are exceeded

## Configuration

```toml
[replication]
role = "replica"                          # standalone, primary, replica
primary-host = "primary.example.com"
primary-port = 6379
min-replicas-to-write = 1                 # Write quorum (0 = disabled)
min-replicas-timeout-ms = 5000
```

### WAL Retention

WAL retention determines how long replicas can be disconnected before requiring full resync:

```toml
[rocksdb]
min-wal-retention-secs = 3600   # Keep WAL for 1 hour
min-wal-files-to-keep = 10
```

Increase if replicas may be offline for extended periods.

## Replication ID

Each dataset history has a unique replication ID. On failover:

1. Promoted replica generates a new replication ID
2. Promoted replica remembers the old primary's ID (secondary ID)
3. Other replicas can continue with partial sync using either ID

## Failover

Managed by the external orchestrator:

1. Orchestrator detects primary failure via health checks
2. Orchestrator selects a replica for promotion
3. Promoted replica generates new replication ID, begins accepting writes
4. Other replicas reconnect to new primary (PSYNC using secondary ID)
5. Clients receive `-MOVED` redirections

## Memory Considerations

- Provision replicas with 10-20% more memory than the primary (replication buffers)
- When a promoted replica becomes primary, `maxmemory` becomes active and eviction begins immediately if over limit
- Full sync memory is configurable:

```toml
[replication]
fullsync-max-memory-mb = 512    # Max memory for full sync buffering
```

## Monitoring

```
INFO replication
```

Compare `master_repl_offset` (primary) with `slave_repl_offset` (replica) to measure lag.

| Symptom | Likely Cause | Fix |
|---|---|---|
| Frequent full resyncs | WAL retention too short | Increase `min-wal-retention-secs` |
| Replica OOM during sync | Primary dataset exceeds replica memory | Increase replica memory |
