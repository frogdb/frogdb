---
title: "Persistence"
description: "FrogDB's durability modes, WAL failure policy, snapshots, startup recovery, and tiered storage."
---
FrogDB persists every shard to its own RocksDB instance. This page covers the three
durability modes, the WAL failure policy, how forkless snapshots work and what triggers
them, what startup recovery actually does, and the tiered (hot/warm) storage tier. Copying
data off-host is covered separately in [Backup & restore](/operations/backup-restore/);
how replicas bootstrap from a checkpoint is covered in
[Replication](/operations/replication/).

## Durability modes

FrogDB's "WAL" is RocksDB's own write-ahead log — FrogDB does not maintain a separate log
file. Writes are batched on a per-shard flush thread, and the `durability-mode` setting
(`[persistence]` in the TOML file) controls how that thread commits each batch:

- **`sync`** — every batch write is issued with RocksDB's `sync` write option set, which
  fsyncs the write before the call returns. An acknowledged write is durable immediately.
- **`async`** — writes are not synced; the OS decides when buffered data reaches disk.
  There is no background sync task in this mode.
- **`periodic`** (default) — writes are not synced individually either, but a background
  task flushes each shard's RocksDB memtable to disk on a fixed interval
  (`sync-interval-ms`, default `1000`), bounding how much recently-written data can be
  lost if the process crashes between flushes.

Choose based on the data-loss window you can tolerate, not on measured latency (FrogDB
does not publish latency numbers for these modes): `async` favors throughput for
data you can afford to lose, `sync` favors safety for data you cannot, and `periodic`
is the default middle ground, comparable to Redis's `appendfsync everysec`.

```toml
[persistence]
durability-mode = "periodic"  # "async" | "periodic" | "sync"
sync-interval-ms = 1000       # periodic mode only
```

The registry (`frogdb-server/crates/config/src/params.rs`) marks both `durability-mode`
and `sync-interval-ms` as runtime-mutable, and `CONFIG SET` accepts changes to either. In
practice, the periodic-sync background task captures its mode and interval once at
startup (`frogdb-server/crates/server/src/server/startup.rs`) and is not restarted when
these values change, so a runtime change to either does not take effect until the server
restarts. Treat them as effectively startup-only despite being marked mutable.

### Write visibility

In `async` and `periodic` modes, a write is visible to other clients (a `GET` after a
`SET` succeeds) before it is durable on disk — a successful read does not guarantee the
value survives a crash. This matches standard Redis behavior for the equivalent
`appendfsync` settings.

## WAL failure policy

`wal-failure-policy` (`[persistence]`, default `continue`) controls what happens when a
WAL write fails after a command has already executed in memory:

- **`continue`** — logs the failure and returns success to the client. The write stays
  visible in memory but may be lost on restart.
- **`rollback`** — undoes the in-memory change and returns an error to the client instead.

Unlike `durability-mode`/`sync-interval-ms` above, `wal-failure-policy` is genuinely live:
it is stored as a shared atomic that the flush thread reads on every write, so
`CONFIG SET wal-failure-policy rollback` takes effect immediately. One caveat: `rollback`
only catches failures raised by the flush thread itself in non-`sync` modes — document
this as the current behavior rather than a general corruption guarantee.

## Snapshots

FrogDB snapshots are forkless: rather than forking the process (as Redis does for RDB),
a snapshot is a RocksDB checkpoint — RocksDB hard-links the current SST files into a new
directory at the filesystem level instead of copying them
(`frogdb-server/crates/persistence/src/rocks/checkpoint.rs`,
`frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs`). Each snapshot
directory also gets a `metadata.json` and a copy of the Tantivy search indexes if any
exist. Because the low overhead comes from hard-links rather than an in-memory
copy-on-write buffer, there is no memory spike from taking a snapshot; the mechanism does
mean a snapshot's marginal disk cost grows over time as later compaction rewrites the
hard-linked SSTs out from under it.

```toml
[snapshot]
snapshot-dir = "./snapshots"      # default; sibling of the data directory
snapshot-interval-secs = 3600     # 0 disables automatic snapshots
max-snapshots = 5                 # 0 keeps every snapshot
```

`BGSAVE` (and `BGSAVE SCHEDULE`) trigger a snapshot on demand in addition to the automatic
interval.

## Recovery

On startup, FrogDB (`frogdb-server/crates/server/src/server/startup.rs` and
`frogdb-server/crates/persistence/src/recovery.rs`):

1. Installs a staged replica checkpoint if a `checkpoint_ready` sibling directory is
   present next to the data directory; otherwise this step is a no-op.
2. Opens RocksDB at `data-dir`. RocksDB replays its own internal WAL as part of opening.
3. If data exists, iterates every key in every column family (hot tier, then warm tier)
   and restores it into the in-memory store, skipping keys whose TTL already expired and
   counting (and logging) any values that fail to deserialize. Recovery continues past
   deserialize failures rather than aborting — this is the only corruption-handling
   behavior that exists. A warm-tier key is only restored if no hot-tier copy of the same
   key was already loaded (a hot copy always wins).

There is no snapshot-load step at startup: the `data-dir` directory *is* the live RocksDB
instance, not a directory that gets reconstructed from a snapshot on every boot.
Snapshots in `snapshot-dir` are a separate, standalone copy of the data used for backup
and replica bootstrap (see [Backup & restore](/operations/backup-restore/) and
[Replication](/operations/replication/)) — primary recovery never reads them.

## Tiered storage

FrogDB has an optional warm storage tier: cold values, instead of being evicted outright,
can be demoted into a second RocksDB column family per shard. This is enabled with a
single flag:

```toml
[tiered-storage]
enabled = false
```

Enabling it makes two additional `maxmemory-policy` values available, `tiered-lru` and
`tiered-lfu`, which demote (rather than delete) least-recently/least-frequently-used
values into the warm tier (`frogdb-server/crates/core/src/eviction/policy.rs`). Warm-tier
entries are recovered at startup the same way hot-tier entries are (see
[Recovery](#recovery) above). There are no additional tuning knobs beyond the `enabled`
flag today — see [Configuration](/operations/configuration/) and
[Configuration reference](/reference/configuration/) for `maxmemory-policy` and the rest
of the `[memory]` section.

## Persistence metrics

The full metric catalog is in [Metrics reference](/reference/metrics/). The persistence-
relevant subset:

| Metric | Type | Description |
|---|---|---|
| `frogdb_wal_writes_total` | Counter | WAL writes issued |
| `frogdb_wal_bytes_total` | Counter | WAL bytes written |
| `frogdb_wal_flush_duration_seconds` | Histogram | Flush-thread batch duration |
| `frogdb_wal_durability_lag_ms` | Gauge | How far behind durable state is from in-memory state |
| `frogdb_persistence_errors_total` | Counter | Persistence errors (disk full, I/O failures) |
| `frogdb_snapshot_in_progress` | Gauge | 1 while a snapshot is running |
| `frogdb_snapshot_duration_seconds` | Histogram | Snapshot duration |
| `frogdb_snapshot_size_bytes` | Gauge | Size of the last snapshot |

## See also

- [Backup & restore](/operations/backup-restore/) — copying data off-host and restoring it.
- [Replication](/operations/replication/) — how replicas use checkpoints for full sync.
- [Configuration](/operations/configuration/) — the four configuration surfaces and
  runtime mutability in general.
- Architecture → [Storage](/architecture/storage/) and
  [Persistence](/architecture/persistence/) — the on-disk format and storage engine design.
