# Replication

FrogDB supports primary-replica replication for high availability and read scaling.

## Overview

- **Orchestrated model**: An external orchestrator manages topology (not gossip).
- **Full dataset replication**: Replicas copy the entire primary dataset.
- **PSYNC protocol**: Redis-compatible partial sync when possible.
- **Two sync types**: Full resync (new replica or large gap) and partial sync (reconnection within WAL retention).

```
Primary: Write -> In-Memory -> WAL -> Replication Stream -> Replicas
```

---

## Setting Up Replication

### Replication Authentication

Replicas authenticate with primaries using dedicated credentials:

```toml
[replication]
primary_auth = "replication-secret"
primary_user = "replicator"  # Optional, for ACL-based auth
max_replicas = 0             # 0 = unlimited (default)
```

Set `primary_auth` on all nodes, since any node may become a replica after failover.

### Starting a Replica

Use the `REPLICAOF` command or configure via TOML:

```
REPLICAOF <primary-host> <primary-port>
```

The replica will:
1. Authenticate with the primary (if `primary_auth` is set).
2. Attempt partial sync (PSYNC) using the last known replication ID and offset.
3. If partial sync is not possible, perform a full resync.

### Stopping Replication

```
REPLICAOF NO ONE
```

The replica becomes a standalone server and accepts writes.

---

## Sync Types

### Full Resync (FULLRESYNC)

Triggered when:
- A new replica connects with no prior data.
- The replica's WAL offset is too old (outside WAL retention).

During full resync:
1. Primary creates a RocksDB checkpoint.
2. Checkpoint files are streamed to the replica with SHA256 integrity verification.
3. After loading the checkpoint, the replica transitions to incremental WAL streaming.

If multiple replicas request full resync simultaneously, a single checkpoint is created and shared across all requesting replicas.

### Partial Sync (PSYNC)

When a replica reconnects and its last offset is within the primary's WAL retention:

1. Replica sends `PSYNC <replication_id> <offset>`.
2. Primary responds with `+CONTINUE` and streams missing WAL entries.

### WAL Retention Configuration

WAL retention determines how long replicas can be disconnected before requiring full resync:

```toml
[rocksdb]
min_wal_retention_secs = 3600  # Keep WAL files for at least 1 hour
min_wal_files_to_keep = 10
```

Increase these values if replicas may be offline for extended periods.

---

## Replication ID

Each dataset history has a unique 40-character hex identifier (replication ID).

- **Changes on failover**: A promoted replica generates a new replication ID.
- **Secondary ID**: Promoted replicas remember the old primary's ID, allowing other replicas to continue incrementally.

```
Primary A (repl_id: abc123...)
    |
    +-- Replica B (tracking abc123...)
    |
    [Primary A fails, B promoted]
    |
Primary B (repl_id: def456..., secondary_id: abc123...)
```

Replicas of A can connect to B and continue with partial sync using either ID.

---

## Failover

Failover is managed by the external orchestrator:

1. Orchestrator detects primary failure (health checks).
2. Orchestrator selects a replica for promotion.
3. Orchestrator pushes new topology to all nodes.
4. Promoted replica generates a new replication ID and begins accepting writes.
5. Other replicas reconnect to the new primary (PSYNC using secondary ID).

Clients receive `-MOVED` redirections and should update their connection targets.

---

## Memory Considerations

### Replica Memory

Replicas do NOT evict independently. All eviction decisions come from the primary via replicated `DEL` commands. Replicas may use more memory than the primary due to replication buffers.

**Recommendations:**
- Provision replicas with 10-20% more memory than the primary.
- Monitor replica memory usage separately.
- Alert when replica memory approaches system limits.

### Memory During Full Sync

The replica monitors memory during checkpoint loading:

```toml
[replication]
# Abort full sync if memory exceeds this percentage
sync_memory_limit_pct = 90

# Pause WAL consumption when memory exceeds this percentage
sync_memory_pause_pct = 85

# Resume WAL consumption when memory drops below this percentage
sync_memory_resume_pct = 80
```

If the replica runs out of memory during full sync, it aborts and retries with exponential backoff.

### Replica Promotion and Eviction

When a replica is promoted to primary:
1. `maxmemory` becomes active.
2. If over limit, eviction begins immediately.
3. Eviction `DEL` commands are replicated to new replicas.

---

## Configuration Reference

```toml
[replication]
primary_auth = ""                        # Password for replica->primary authentication
primary_user = ""                        # ACL username for replication
max_replicas = 0                         # Max replicas (0 = unlimited)
checkpoint_transfer_timeout_ms = 300000  # Total FULLRESYNC timeout (5 min)
checkpoint_file_timeout_ms = 60000       # Per-file transfer timeout
sync_memory_limit_pct = 90              # Abort sync at this memory percentage
sync_memory_pause_pct = 85              # Pause WAL consumption threshold
sync_memory_resume_pct = 80             # Resume WAL consumption threshold
sync_retry_delay_ms = 5000              # Retry delay after failed sync

[rocksdb]
min_wal_retention_secs = 3600           # WAL retention for partial sync
min_wal_files_to_keep = 10
```

---

## Monitoring

### INFO replication

```
INFO replication
```

On the primary, shows connected replicas and replication offset. On replicas, shows the primary connection status and offset lag.

### Key Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_sync_memory_pauses_total` | Times streaming paused for memory |
| `frogdb_sync_aborted_memory_total` | Syncs aborted due to memory |

### Monitoring Replication Lag

Compare `master_repl_offset` (primary) with `slave_repl_offset` (replica) in `INFO replication` output. The difference in bytes indicates how far behind the replica is.

### Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Replica falling behind | Network latency or slow replica | Check network, scale replica hardware |
| Frequent full resyncs | WAL retention too short | Increase `min_wal_retention_secs` |
| Replica OOM during sync | Primary dataset exceeds replica memory | Increase replica memory |
| Replica disconnecting | Replication timeout too short | Increase `repl_timeout_ms` |
