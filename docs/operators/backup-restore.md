# Backup and Restore

FrogDB supports backup and recovery through snapshots, WAL files, and replica-based strategies.

## Backup Strategies

| Strategy | RPO (Data Loss) | RTO (Recovery Time) | Complexity |
|----------|----------------|---------------------|------------|
| Periodic snapshots | Up to snapshot interval | Fast (load snapshot + WAL replay) | Low |
| Replica-based backup | Near-zero (async replication) | Medium (promote replica) | Medium |
| Snapshot + WAL archival | Configurable (point-in-time) | Medium (snapshot + WAL replay) | Higher |

---

## Snapshot-Based Backup

FrogDB creates periodic point-in-time snapshots that can serve as backups.

### Enabling Snapshots

```toml
[persistence]
enabled = true
data_dir = "/var/lib/frogdb"

[persistence.snapshot]
enabled = true
interval_s = 3600  # Snapshot every hour
```

### Where Snapshots Are Stored

Snapshots are stored in the data directory as RocksDB checkpoints. Each snapshot is a consistent view of the full dataset at a point in time.

### Manual Snapshot

Trigger a manual snapshot (when available):
```
BGSAVE
```

### Copying Snapshots Off-Host

For disaster recovery, regularly copy snapshots to remote storage:

```bash
# Example: copy latest snapshot to S3
aws s3 sync /var/lib/frogdb/snapshots/ s3://my-backups/frogdb/$(date +%Y%m%d)/
```

Schedule this via cron or a systemd timer to maintain off-host backups.

---

## Replica-Based Backup

A replica serves as a live backup of the primary. This is the recommended approach for production high availability.

### Setup

See [replication.md](replication.md) for configuring replicas.

### Using a Replica as Backup

1. The replica continuously receives updates from the primary.
2. On primary failure, the orchestrator promotes the replica.
3. For offline backups, stop the replica and copy its data directory.

### Dedicated Backup Replica

For workloads where you need a backup without affecting production read performance:

1. Configure a replica that does not serve client traffic.
2. Periodically stop the replica, copy its data directory, then restart it.
3. The replica will catch up via partial sync (PSYNC) if the downtime is within WAL retention.

---

## Point-in-Time Recovery

Point-in-time recovery uses a snapshot plus WAL replay to restore to a specific moment.

### How It Works

1. Restore the most recent snapshot before the target time.
2. Replay WAL entries from the snapshot's sequence number up to the target sequence.
3. Server recovers to the exact state at that sequence number.

### Configuration for PITR

Ensure WAL retention is sufficient for your recovery window:

```toml
[rocksdb]
min_wal_retention_secs = 86400  # Keep WAL files for 24 hours
```

Combine with snapshot intervals for bounded recovery time:

```toml
[persistence.snapshot]
enabled = true
interval_s = 3600  # Hourly snapshots
```

With hourly snapshots and 24-hour WAL retention, you can recover to any point within the last 24 hours. The maximum WAL replay needed is 1 hour (from the nearest snapshot).

---

## Restore Procedure

### From Snapshot

```bash
# 1. Stop server
systemctl stop frogdb

# 2. Clear current data (if corrupted)
rm -rf /var/lib/frogdb/data/*

# 3. Copy backup snapshot into data directory
cp -r /path/to/backup/snapshot/* /var/lib/frogdb/data/

# 4. Fix ownership
chown -R frogdb:frogdb /var/lib/frogdb/data/

# 5. Start server (WAL replay happens automatically)
systemctl start frogdb

# 6. Verify
redis-cli -p 6379 ping
redis-cli -p 6379 dbsize
```

### From Replica

If the primary is lost but a replica is available:

1. Promote the replica to primary (via orchestrator or `REPLICAOF NO ONE`).
2. Verify data integrity.
3. Set up new replicas from the promoted primary.

### From Corrupted State

```bash
# 1. Stop server
systemctl stop frogdb

# 2. Check for backup snapshots
ls -la /var/lib/frogdb/backups/

# 3. Restore from latest backup
# (copy backup data into data directory)

# 4. Restart
systemctl start frogdb
```

If WAL corruption is detected on startup, see [persistence.md](persistence.md) for WAL corruption recovery options.

---

## Backup Verification

Periodically verify that backups can be restored:

```bash
# 1. Copy backup to a test directory
cp -r /path/to/backup /tmp/frogdb-test/

# 2. Start FrogDB with test config pointing to backup data
frogdb-server --data-dir /tmp/frogdb-test/ --port 6380

# 3. Verify data
redis-cli -p 6380 dbsize
redis-cli -p 6380 get sample-key

# 4. Clean up
kill %1
rm -rf /tmp/frogdb-test/
```

---

## Durability and Backup Interaction

| Durability Mode | Snapshot Backup | Replica Backup |
|-----------------|----------------|----------------|
| `async` | Snapshot may be ahead of WAL on disk | Replica may be ahead of primary's durability |
| `periodic` | Snapshot + WAL replay within 1s of actual state | Replica within 1s |
| `sync` | Snapshot + WAL fully consistent | Replica fully consistent |

For critical data requiring zero-loss backup, use `sync` durability mode combined with replica-based backup.
