---
title: "Backup and Restore"
description: "FrogDB backup strategies and recovery procedures."
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

`checkpoint/` also carries `frogdb_payload.json`: every file in the payload with its size and
checksum, written and fsynced before the snapshot is published. It travels *inside* the
checkpoint so it survives the copy that matters — the restore below copies `checkpoint/`
and nothing else. Check any snapshot, on any host, without a running server:

```bash
frogctl backup checkpoint-verify /var/lib/frogdb/snapshots/latest/checkpoint
```

That reads every byte and compares checksums (`--quick` checks sizes only, which is what the
boot-time install does). Worth running on a schedule against off-host copies: bit-rot on backup
media is otherwise discovered during the restore.

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

`persistence.data-dir` (default `./frogdb-data`) holds the live RocksDB database at
`<data-dir>/db`, alongside the directories the install path needs — `<data-dir>/staging`,
`<data-dir>/staging.incoming`, and `<data-dir>/backup` — and the identity marker
`<data-dir>/frogdb_data_dir`. Everything FrogDB writes stays **inside** `data-dir`, so the whole
directory can be a mount point. Restoring a snapshot means installing its `checkpoint/` contents
as the new `<data-dir>/db`, then letting RocksDB replay its own internal WAL on open.

Do this by staging the snapshot as `<data-dir>/staging` before starting the server. This reuses
the same staged-checkpoint install path FrogDB already runs on every boot for replica full sync
(`frogdb-server/crates/persistence/src/rocks/checkpoint.rs`,
`frogdb-server/crates/recovery/src/checkpoint.rs`): on startup, before opening RocksDB,
FrogDB looks for `<data-dir>/staging`, verifies it's a complete database, moves any existing
`<data-dir>/db` aside to `<data-dir>/backup/db_backup_<unix-timestamp>` (never deletes it), and
renames `staging` into place as the new `db`.

That verification runs **before** anything moves, and it is three checks, not one: `CURRENT` must
resolve to a `MANIFEST` that exists; every file listed in the payload's own manifest
(`frogdb_payload.json`, written by the snapshot stager) must be present at its recorded size; and
RocksDB must open the staged directory read-only. A staged directory that fails any of them is
refused with the live `data-dir` untouched and still in place — a bad copy costs you a failed
start, not your database.

```bash
# Assume persistence.data-dir = /var/lib/frogdb/frogdb-data
DATA_DIR=/var/lib/frogdb/frogdb-data
SNAPSHOT=/var/lib/frogdb/snapshots/snapshot_00042   # or the "latest" symlink target

# 1. Stop the server
systemctl stop frogdb

# 2. Stage the snapshot's checkpoint as <data-dir>/staging.
#    Copy the CONTENTS of checkpoint/ (not the directory itself) plus the
#    search sidecar, if present, so the staged layout matches a live database.
mkdir -p "$DATA_DIR/staging"
cp -a "$SNAPSHOT/checkpoint/." "$DATA_DIR/staging/"
if [ -d "$SNAPSHOT/search" ]; then
  cp -a "$SNAPSHOT/search" "$DATA_DIR/staging/search"
fi
chown -R frogdb:frogdb "$DATA_DIR/staging"

# 2b. Check the copy before betting the restart on it. Exits non-zero if the
#     payload is incomplete or corrupt; --quick skips the checksum pass.
frogctl backup checkpoint-verify "$DATA_DIR/staging" || exit 1

# 3. Restart — recovery installs the staged checkpoint before opening RocksDB,
#    moving any existing <data-dir>/db aside rather than deleting it
systemctl start frogdb

# 4. Verify
redis-cli -p 6379 ping && redis-cli -p 6379 dbsize
```

Do **not** `rm -rf` the existing `<data-dir>/db` first — the install step already moves it aside
into `<data-dir>/backup` as a timestamped backup (one generation is kept automatically), which is
your rollback path if the restore turns out to be wrong.

The staged path above needs no extra flags: FrogDB stamps the installed directory with its
identity marker as part of recovery. Assembling a `data-dir` by hand is what needs attention — a
snapshot's `checkpoint/` contents copied straight into `<data-dir>/db`, or a directory carried over
from a version that predates the marker, holds files FrogDB did not stamp, and that fails the
boot rather than being written over. Start once with `--force-fresh-data-dir` to adopt such a
directory; a whole-directory copy of a live `data-dir` (the offline-replica backup above) brings
the marker with it and starts normally. See
[Data directory identity](/operations/persistence/#data-directory-identity).

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
