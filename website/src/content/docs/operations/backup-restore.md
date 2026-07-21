---
title: "Backup and Restore"
description: "FrogDB backup strategies and recovery procedures."
sidebar:
  order: 6
---

FrogDB backs up by copying RocksDB snapshots off-host, or by keeping a dedicated replica as a
live backup. Restoring stages the backup as a checkpoint next to the live data directory and
lets FrogDB's normal recovery path install it on startup. See
[Persistence](/operations/persistence/) for durability-mode and snapshot internals.

## Snapshot-based backup

Configure automatic snapshots:

```toml
[snapshot]
snapshot-dir = "/var/lib/frogdb/snapshots"
snapshot-interval-secs = 3600   # Hourly snapshots (0 = disabled)
max-snapshots = 5               # Retain up to 5 snapshots locally
```

Each completed snapshot is a directory `snapshot_NNNNN/` under `snapshot-dir`, containing:

- `checkpoint/` — a hard-linked RocksDB checkpoint (the actual data)
- `search/` — a copy of the search-index sidecar, if search indexes exist
- `metadata.json` — epoch, sequence number, and size

A `latest` symlink always points at the newest complete snapshot. This is not a flat set of DB
files — treat the whole `snapshot_NNNNN/` directory as the backup unit.

Trigger a manual snapshot instead of waiting for the interval:

```bash
frogctl backup trigger
# or, over the Redis protocol:
redis-cli BGSAVE
```

Both issue the same background snapshot. See [Reference → frogctl](/reference/frogctl/) for the
full CLI.

Copy completed snapshot directories off-host for disaster recovery:

```bash
aws s3 sync /var/lib/frogdb/snapshots/ s3://my-backups/frogdb/$(date +%Y%m%d)/
```

## Replica-based backup

A replica is a live, continuously-updated copy of the dataset — see
[Replication](/operations/replication/) for setup. On primary failure, promotion is Raft-driven
automatic failover or a manual `CLUSTER FAILOVER`, not an external orchestrator flipping roles.

For an offline backup: stop a dedicated backup replica, copy its `persistence.data-dir`, and
restart it. On reconnect it catches up via partial sync, or a full checkpoint-based resync if
it fell too far behind — see [Replication](/operations/replication/).

## Restore

FrogDB's `persistence.data-dir` (default `./frogdb-data`) **is** the RocksDB directory — it is
opened directly, with no intermediate copy step. Restoring a snapshot means installing its
`checkpoint/` contents as the new `data-dir`, then letting RocksDB replay its own internal WAL
on open.

Do this by staging the snapshot as a `checkpoint_ready` directory **next to** `data-dir` (i.e.
in its parent directory) before starting the server. This reuses the same staged-checkpoint
install path FrogDB already runs on every boot for replica full sync
(`frogdb-server/crates/persistence/src/rocks/checkpoint.rs`,
`frogdb-server/crates/server/src/recovery/checkpoint.rs`): on startup, before opening RocksDB,
FrogDB looks for `checkpoint_ready` beside `data-dir`, verifies it's a complete database, moves
any existing `data-dir` aside to `<data-dir>_backup_<unix-timestamp>` (never deletes it), and
renames `checkpoint_ready` into place as the new `data-dir`.

```bash
# Assume persistence.data-dir = /var/lib/frogdb/frogdb-data
DATA_DIR=/var/lib/frogdb/frogdb-data
SNAPSHOT=/var/lib/frogdb/snapshots/snapshot_00042   # or the "latest" symlink target

# 1. Stop the server
systemctl stop frogdb

# 2. Stage the snapshot's checkpoint as checkpoint_ready, a sibling of data-dir.
#    Copy the CONTENTS of checkpoint/ (not the directory itself) plus the
#    search sidecar, if present, so the staged layout matches a live data-dir.
mkdir -p "$(dirname "$DATA_DIR")/checkpoint_ready"
cp -a "$SNAPSHOT/checkpoint/." "$(dirname "$DATA_DIR")/checkpoint_ready/"
if [ -d "$SNAPSHOT/search" ]; then
  cp -a "$SNAPSHOT/search" "$(dirname "$DATA_DIR")/checkpoint_ready/search"
fi
chown -R frogdb:frogdb "$(dirname "$DATA_DIR")/checkpoint_ready"

# 3. Restart — recovery installs the staged checkpoint before opening RocksDB,
#    moving any existing data-dir aside rather than deleting it
systemctl start frogdb

# 4. Verify
redis-cli -p 6379 ping && redis-cli -p 6379 dbsize
```

Do **not** `rm -rf` the existing `data-dir` first — the install step already moves it aside as a
timestamped backup (one generation is kept automatically), which is your rollback path if the
restore turns out to be wrong.

## Durability mode and backup consistency

How closely a snapshot matches the true last-acknowledged state depends on `persistence.durability-mode`:

| Durability mode | Backup consistency |
|---|---|
| `async` | Snapshot may be ahead of what's durable on disk |
| `periodic` | Snapshot + WAL within `sync-interval-ms` of actual state |
| `sync` | Snapshot + WAL fully consistent with acknowledged writes |

For the strongest backup guarantee, use `sync` durability mode, or pair snapshot backups with a
replica-based backup. See [Persistence](/operations/persistence/) for the full durability-mode
trade-offs.

## See also

- [Persistence](/operations/persistence/) — durability modes, snapshot internals, recovery
- [Replication](/operations/replication/) — replica setup and failover
- [Configuration Reference → Snapshot](/reference/configuration/#snapshot)
- [Reference → frogctl](/reference/frogctl/)
