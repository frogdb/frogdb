---
title: "Backup and Restore"
description: "FrogDB backup strategies and recovery procedures."
sidebar:
  order: 6
---

FrogDB uses RocksDB snapshots (not RDB files) for backup and recovery.

## Snapshot-Based Backup

```toml
[snapshot]
snapshot-dir = "/var/lib/frogdb/snapshots"
snapshot-interval-secs = 3600   # Hourly snapshots
max-snapshots = 5
```

Trigger a manual snapshot: `BGSAVE`

Copy snapshots off-host for disaster recovery:

```bash
aws s3 sync /var/lib/frogdb/snapshots/ s3://my-backups/frogdb/$(date +%Y%m%d)/
```

## Replica-Based Backup

A replica serves as a live backup. On primary failure, the orchestrator promotes the replica. See [Replication](/operations/replication/).

For offline backups: stop a dedicated backup replica, copy its data directory, restart it. The replica catches up via partial sync if downtime is within WAL retention.

## Point-in-Time Recovery

Requires snapshot + WAL retention:

```toml
[snapshot]
snapshot-interval-secs = 3600   # Hourly

[rocksdb]
min-wal-retention-secs = 86400  # 24-hour WAL retention
```

Recovery replays WAL entries from the nearest snapshot up to the target sequence number. With hourly snapshots, maximum WAL replay is 1 hour.

## Restore Procedure

```bash
# 1. Stop server
systemctl stop frogdb

# 2. Replace data with backup
rm -rf /var/lib/frogdb/data/*
cp -r /path/to/backup/snapshot/* /var/lib/frogdb/data/
chown -R frogdb:frogdb /var/lib/frogdb/data/

# 3. Restart (WAL replay happens automatically)
systemctl start frogdb

# 4. Verify
redis-cli -p 6379 ping && redis-cli -p 6379 dbsize
```

## Durability and Backup Interaction

| Durability Mode | Backup Consistency |
|---|---|
| `async` | Snapshot may be ahead of WAL on disk |
| `periodic` | Snapshot + WAL within `sync-interval-ms` of actual state |
| `sync` | Snapshot + WAL fully consistent |

For zero-loss backup, use `sync` durability mode with replica-based backup.
