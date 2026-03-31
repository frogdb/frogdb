---
title: "Persistence"
description: "FrogDB persists data using RocksDB for durability. This document covers durability modes, snapshot configuration, and recovery procedures for operators."
sidebar:
  order: 2
---
FrogDB persists data using RocksDB for durability. This document covers durability modes, snapshot configuration, and recovery procedures for operators.

## Durability Modes

FrogDB supports three durability modes controlling the trade-off between write performance and data safety:

| Mode | Durability | Latency | Data at Risk on Crash |
|------|------------|---------|----------------------|
| `async` | Best-effort | ~1-10 us | All unflushed writes (unbounded) |
| `periodic` | Bounded loss | ~1-10 us | Up to `fsync-interval-ms` of writes (default 1s) |
| `sync` | Guaranteed | ~100-500 us | None (acknowledged = durable) |

### Choosing a Durability Mode

- **`async`**: Use for caching workloads where data loss is acceptable. Highest throughput.
- **`periodic`** (recommended): Balanced option. Bounded loss window of 1 second by default. Matches Redis `appendfsync everysec` behavior.
- **`sync`**: Use for critical data that must not be lost. Every acknowledged write is fsynced to disk before the client receives OK.

### Write Visibility

In `async` and `periodic` modes, writes are visible to other clients before they are fsynced to disk. A successful `GET` after `SET` does not guarantee the value will survive a crash. This matches standard Redis behavior.

### Configuration

```toml
[persistence]
enabled = true
data-dir = "/var/lib/frogdb"
durability-mode = "periodic"  # async, periodic, sync
sync-interval-ms = 1000       # Fsync every 1 second (periodic mode)

[snapshot]
snapshot-interval-secs = 3600  # Snapshot every hour
max-snapshots = 5
```

---

## Snapshots

FrogDB creates periodic point-in-time snapshots for faster recovery. Snapshots use a forkless algorithm that does not cause memory spikes.

### How Snapshots Work

- Each shard captures a logical point-in-time view using epoch-based versioning.
- The server continues processing commands during the snapshot.
- Copy-on-write semantics capture old values for keys modified during the snapshot.
- No 2x memory spike (unlike Redis fork-based snapshots).

### Snapshot Configuration

```toml
[snapshot]
snapshot-dir = "/var/lib/frogdb/snapshots"
snapshot-interval-secs = 3600   # Snapshot every hour
max-snapshots = 5               # Retain up to 5 snapshots
```

### Memory During Snapshots

| Scenario | Additional Memory |
|----------|-------------------|
| Low write rate | Minimal (~COW buffer size) |
| High write rate, many key overwrites | Up to COW buffer per shard |
| Pathological: every key overwritten | ~dataset size (worst case) |

---

## Recovery

On startup, if data exists, FrogDB recovers state automatically:

1. Check for snapshots and find the latest by epoch number.
2. Load snapshot (for each shard: load key-value pairs into memory, rebuild expiry index).
3. Replay WAL entries after the snapshot's sequence number.
4. Verify integrity and log recovery statistics.

### Recovery Scenarios

| Scenario | Behavior |
|----------|----------|
| Clean shutdown | Load snapshot + minimal WAL replay |
| Crash | Load snapshot + full WAL replay from snapshot point |
| No snapshot | Full WAL replay from beginning |
| No data | Fresh start |
| Corrupted WAL | Recover up to corruption point, log error |

### Recovery Time Estimates

| Dataset Size | Approximate Recovery Time |
|--------------|---------------------------|
| 1 GB | 10-30 seconds |
| 10 GB | 1-5 minutes |
| 100 GB | 10-30 minutes |
| 1 TB | 1-3 hours |

Times depend on disk speed, data structure complexity, and available CPU.

### WAL Corruption Policy

```toml
[persistence]
# Policy when WAL corruption is detected
# "truncate" - Discard corrupted entry and all subsequent entries (default)
# "fail" - Abort startup, require manual intervention
wal-corruption-policy = "truncate"
```

- **`truncate`** (default): Prioritizes returning to service. Operators can inspect logs to assess data loss.
- **`fail`**: Requires manual intervention. Use for critical data where any data loss must be investigated.

Manual recovery when `fail` policy triggers startup abort:

```bash
# 1. Inspect WAL state
frog debug wal-inspect --data-dir /var/lib/frogdb/data/

# 2. Force truncation if acceptable
frog debug wal-truncate --data-dir /var/lib/frogdb/data/ --at-sequence <seq>

# 3. Restart server
systemctl start frogdb
```

### WAL Failure Policy

Controls behavior when a WAL write fails after a command executes in memory:

| Policy | Behavior |
|--------|----------|
| `continue` (default) | Log error, return success to client. Write is visible but may be lost on restart. |
| `rollback` | Undo in-memory state, return `IOERR` to client. Adds latency due to synchronous disk I/O. |

```toml
[persistence]
wal-failure-policy = "continue"  # or "rollback"
```

Runtime toggle: `CONFIG SET wal-failure-policy rollback`

---

## Disk Space Management

### What Consumes Disk

- **WAL files**: Append-only log of all writes.
- **SST files**: RocksDB sorted string table files (compacted data).
- **Snapshots**: Point-in-time copies of the full dataset.

### WAL Retention

WAL files are retained to support replica reconnection via partial sync (PSYNC):

```toml
[rocksdb]
min-wal-retention-secs = 3600  # Keep WAL files for at least 1 hour
min-wal-files-to-keep = 10
```

Larger retention values allow replicas to recover from longer disconnections without requiring a full resync.

### Monitoring Disk Usage

Key metrics to watch:

| Metric | Description |
|--------|-------------|
| `frogdb_wal_bytes_total` | WAL bytes written |
| `frogdb_snapshot_size_bytes` | Last snapshot size |
| `frogdb_persistence_errors_total` | Persistence errors (disk full, I/O) |

Alert on disk usage approaching capacity. WAL write failures in `sync` mode return errors to clients; in `async`/`periodic` modes, data may be silently lost.

---

## Key Metrics

See [Metrics Reference](/reference/metrics/) for the full list. Key persistence metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_wal_writes_total` | Counter | WAL writes |
| `frogdb_wal_bytes_total` | Counter | WAL bytes written |
| `frogdb_wal_flush_duration_seconds` | Histogram | WAL flush latency |
| `frogdb_wal_durability_lag_ms` | Gauge | Durability lag |
| `frogdb_persistence_errors_total` | Counter | Persistence errors |
| `frogdb_snapshot_in_progress` | Gauge | 1 if snapshot running |
| `frogdb_snapshot_duration_seconds` | Histogram | Snapshot duration |
| `frogdb_snapshot_size_bytes` | Gauge | Last snapshot size |
