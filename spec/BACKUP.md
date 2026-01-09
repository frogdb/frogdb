# FrogDB Backup and Restore

Online backup procedures, point-in-time recovery, and restore operations.

## BGSAVE Command

Trigger a background snapshot without blocking the server:

```
BGSAVE
```

**Behavior:**
1. Returns `+Background saving started` immediately
2. Initiates forkless snapshot algorithm (see [PERSISTENCE.md](PERSISTENCE.md#forkless-snapshot-algorithm))
3. Server continues processing commands
4. On completion, updates `last_save_time` and `rdb_changes_since_last_save`

**Monitoring:**
```
INFO persistence

# Persistence
rdb_changes_since_last_save:1234
rdb_bgsave_in_progress:1
rdb_last_save_time:1704825600
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:12
```

## LASTSAVE Command

Return timestamp of last successful snapshot:

```
LASTSAVE
```

Returns Unix timestamp (integer): `:1704825600`

---

## Backup Procedure

**Recommended Process:**

1. **Initiate snapshot:**
   ```
   BGSAVE
   ```

2. **Wait for completion:**
   ```bash
   while redis-cli INFO persistence | grep -q "rdb_bgsave_in_progress:1"; do
       sleep 1
   done
   ```

3. **Verify success:**
   ```
   redis-cli INFO persistence | grep rdb_last_bgsave_status
   # Should return: rdb_last_bgsave_status:ok
   ```

4. **Copy snapshot file:**
   ```bash
   cp /var/lib/frogdb/rocksdb/snapshot_latest /backup/frogdb_$(date +%Y%m%d).rdb
   ```

**Snapshot File Properties:**
- Immutable after completion (renamed atomically on finish)
- Contains all shard data at snapshot epoch
- Self-contained (no WAL needed for consistent restore)
- Portable across compatible FrogDB versions

---

## Point-in-Time Recovery

For recovery to specific point beyond snapshot:

1. **Restore snapshot** (provides base state)
2. **Replay WAL** up to desired timestamp/sequence

**Configuration:**
```toml
[persistence]
# Keep WAL files for point-in-time recovery
wal_retention_time_s = 86400  # 24 hours
```

**Recovery Command (future):**
```
RESTORE-PIT <timestamp_ms>
```

---

## Backup Consistency

| Backup Type | Consistency | Notes |
|-------------|-------------|-------|
| **Snapshot only** | Epoch point-in-time | Keys deleted during snapshot may be absent |
| **Snapshot + WAL** | True point-in-time | Full consistency with WAL replay |
| **WAL only** | Full history | Requires snapshot base for recovery |

See [PERSISTENCE.md](PERSISTENCE.md#forkless-snapshot-algorithm) for snapshot consistency semantics.

---

## Restore Procedure

1. **Stop FrogDB server**

2. **Replace data directory:**
   ```bash
   rm -rf /var/lib/frogdb/rocksdb
   cp -r /backup/snapshot /var/lib/frogdb/rocksdb
   ```

3. **Start server:**
   - Detects snapshot automatically
   - Replays any WAL files present
   - Becomes available after recovery

**Important:** Ensure sufficient disk space for both backup and running database during restore.

---

## Automated Backups

### Scheduled Snapshots

Configure automatic background saves:

```toml
[persistence]
# Trigger BGSAVE after N seconds if at least M changes
save_intervals = [
    { seconds = 900, changes = 1 },      # 15 min if >= 1 change
    { seconds = 300, changes = 10 },     # 5 min if >= 10 changes
    { seconds = 60, changes = 10000 },   # 1 min if >= 10000 changes
]
```

### External Backup Tools

FrogDB snapshots are RocksDB-compatible. Use standard tools:

```bash
# Using RocksDB's backup tool
ldb backup --db=/var/lib/frogdb/rocksdb --backup_dir=/backup/

# Using filesystem snapshots (ZFS, LVM)
zfs snapshot tank/frogdb@backup-$(date +%Y%m%d)
```

---

## References

- [PERSISTENCE.md](PERSISTENCE.md) - WAL, snapshots, durability modes
- [OPERATIONS.md](OPERATIONS.md) - Server administration
- [FAILURE_MODES.md](FAILURE_MODES.md) - Recovery from failures
