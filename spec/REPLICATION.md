# Replication

## Overview

FrogDB supports primary-replica replication for high availability and read scaling.

## Architecture

- **Orchestrated model** - External orchestrator manages topology (not gossip)
- **Full dataset replication** - Replicas copy entire primary, not slot-specific
- **RocksDB WAL streaming** - Uses `GetUpdatesSince()` for incremental sync
- **PSYNC protocol** - Redis-compatible partial sync when possible

## Sync Types

| Type | Trigger | Mechanism |
|------|---------|-----------|
| FULLRESYNC | New replica, WAL gap too large | RocksDB checkpoint transfer |
| PSYNC | Reconnection within backlog | Resume from last sequence number |

## Data Flow

```
Primary: Write → In-Memory → WAL → Replication Stream → Replicas
                              ↓
                        WAL Archive (disk)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `primary_auth` | - | Password for replica→primary auth |
| `primary_user` | - | ACL user for replication (optional) |
| `repl_backlog_size` | 1MB | In-memory buffer for partial sync |
| `wal_retention_size` | 100MB | Disk WAL for slower catch-up |
| `repl_timeout_ms` | 60000 | Connection timeout |
| `min_replicas_to_write` | 0 | Sync replication quorum |

## Flow Control

FrogDB streams WAL directly to replica sockets (DragonflyDB model):
- No output buffer limit - TCP backpressure handles flow control
- Replica disconnected only on network failure or timeout
- No "full sync loop" problem from buffer limits

## Cascading Replication

**Not supported.** Replicas must connect directly to primaries.

Rationale: Simpler implementation, avoids propagation delay compounding.

## See Also

- [CLUSTER.md](CLUSTER.md) - Cluster topology and failover
- [CONSISTENCY.md](CONSISTENCY.md) - Consistency guarantees
- [PERSISTENCE.md](PERSISTENCE.md) - WAL and snapshot details
