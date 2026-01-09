# FrogDB Server Lifecycle

This document details FrogDB's server startup, shutdown, and recovery procedures.

## Startup Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│                     FrogDB Startup                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Parse Configuration                                          │
│     └── Figment: CLI args > env vars > config file > defaults   │
│                                                                  │
│  2. Initialize Logging                                           │
│     └── tracing subscriber (pretty or JSON format)              │
│                                                                  │
│  3. Initialize Metrics                                           │
│     └── Prometheus endpoint on metrics port                     │
│                                                                  │
│  4. Open RocksDB                                                 │
│     └── Create or open at data_dir                              │
│     └── One column family per internal shard                    │
│                                                                  │
│  5. Recovery (if data exists)                                    │
│     └── Load latest snapshot                                    │
│     └── Replay WAL entries after snapshot LSN                   │
│                                                                  │
│  6. Initialize Security                                          │
│     └── Load ACL file (if configured)                           │
│     └── Initialize AclManager                                   │
│                                                                  │
│  7. Spawn Shard Workers                                          │
│     └── N Tokio tasks (one per internal shard)                  │
│     └── Each owns: Store, ExpiryIndex, Lua VM                   │
│     └── Create bounded mpsc channels for messages               │
│                                                                  │
│  8. Start Acceptor                                               │
│     └── Bind to port                                            │
│     └── Begin accepting connections                              │
│                                                                  │
│  9. Signal Ready                                                 │
│     └── Log startup complete                                    │
│     └── Update health check status                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Startup Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `./data` | RocksDB and snapshot directory |
| `num_shards` | `num_cpus` | Number of internal shard workers |
| `bind` | `127.0.0.1` | Address to bind |
| `port` | `6379` | Port to listen on |

---

## Shutdown Sequence

```
┌─────────────────────────────────────────────────────────────────┐
│                     FrogDB Shutdown                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Stop Accepting Connections                                   │
│     └── Close listener socket                                   │
│     └── New connections rejected                                │
│                                                                  │
│  2. Signal Shutdown to Shard Workers                             │
│     └── Send Shutdown message to all shards                     │
│     └── Workers stop accepting new commands                     │
│                                                                  │
│  3. Drain In-Flight Commands                                     │
│     └── Wait for pending commands to complete                   │
│     └── Timeout after drain_timeout_s (default: 30s)            │
│                                                                  │
│  4. Close Client Connections                                     │
│     └── Send error to blocked clients                           │
│     └── Close all TCP connections                               │
│                                                                  │
│  5. Flush WAL                                                    │
│     └── fsync to ensure durability                              │
│                                                                  │
│  6. Final Snapshot (Optional)                                    │
│     └── If shutdown_snapshot = true                             │
│     └── Create point-in-time snapshot                           │
│                                                                  │
│  7. Close RocksDB                                                │
│     └── Flush memtables                                         │
│     └── Close file handles                                      │
│                                                                  │
│  8. Shutdown Observability                                       │
│     └── Flush tracing spans                                     │
│     └── Close metrics endpoint                                  │
│                                                                  │
│  9. Exit                                                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Graceful Shutdown Triggers

| Signal | Behavior |
|--------|----------|
| SIGTERM | Graceful shutdown |
| SIGINT (Ctrl+C) | Graceful shutdown |
| SIGQUIT | Immediate exit (no drain) |

### Shutdown Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `drain_timeout_s` | `30` | Max time to wait for commands to complete |
| `shutdown_snapshot` | `false` | Create snapshot on shutdown |

---

## Recovery Process

On startup, if data exists, FrogDB recovers state:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Recovery Flow                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Check for Snapshots                                          │
│     └── List snapshot directories                               │
│     └── Find latest by epoch number                             │
│                                                                  │
│  2. Load Snapshot (if exists)                                    │
│     └── For each column family (internal shard):                │
│         └── Load key-value pairs into memory                    │
│         └── Rebuild expiry index                                │
│     └── Record snapshot LSN                                     │
│                                                                  │
│  3. Replay WAL                                                   │
│     └── GetUpdatesSince(snapshot_lsn)                           │
│     └── For each WAL entry:                                     │
│         └── Parse operation (PUT/DELETE)                        │
│         └── Apply to appropriate shard                          │
│     └── Continue until end of WAL                               │
│                                                                  │
│  4. Verify Integrity                                             │
│     └── Count keys per shard                                    │
│     └── Log recovery statistics                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Recovery Scenarios

| Scenario | Behavior |
|----------|----------|
| Clean shutdown | Load snapshot + minimal WAL replay |
| Crash | Load snapshot + full WAL replay from snapshot LSN |
| No snapshot | Full WAL replay from beginning |
| No data | Fresh start |
| Corrupted WAL | Recover up to corruption point, log error |

### Data Structure Recovery Details

**SortedSet Recovery:**
```
1. Load from RocksDB: [(score1, member1), (score2, member2), ...]
2. Rebuild HashMap (member → score): O(n) insertions
3. Rebuild BTreeMap (score, member) → (): O(n log n) insertions
4. Total complexity: O(n log n) per sorted set
```

**Expiry Index Rebuild:**
```
1. For each key loaded from RocksDB:
2.   If expires_at > 0:
3.     Parse timestamp from value header
4.     Insert into ExpiryIndex.by_expiry (BTreeMap)
5.     Insert into ExpiryIndex.key_expiry (HashMap)
```

**Memory Tracking Rebuild:**
- Recalculate `memory_size` for each key during load
- Aggregate per-shard totals
- Compare with pre-crash metrics for validation

**Recovery Time Estimates:**

| Dataset Size | Approximate Recovery Time |
|--------------|---------------------------|
| 1 GB | 10-30 seconds |
| 10 GB | 1-5 minutes |
| 100 GB | 10-30 minutes |
| 1 TB | 1-3 hours |

*Times depend on disk speed, sorted set count, and available CPU.*

---

## Startup Failures

| Failure | Cause | Resolution |
|---------|-------|------------|
| Port already in use | Another process on same port | Kill other process or change `port` config |
| Permission denied (port) | Can't bind port <1024 as non-root | Run as root, use higher port, or use `setcap` |
| Data directory not writable | Permission issue | `chown`/`chmod` the directory |
| RocksDB open failed | Lock held or corruption | Check for other instances; see [FAILURE_MODES.md](FAILURE_MODES.md#rocksdb-corruption) |
| Snapshot load failed | Corrupted snapshot | Delete snapshot, rely on WAL replay |
| Out of memory during recovery | Dataset larger than RAM | Increase memory or reduce dataset |
| ACL file parse error | Invalid ACL syntax | Fix ACL file and restart |

---

## Shutdown Edge Cases

| Scenario | Behavior |
|----------|----------|
| SIGTERM during startup | Abort startup, exit immediately |
| SIGKILL anytime | Immediate exit, no cleanup, potential data loss |
| Drain timeout exceeded | Force close connections, may lose in-flight commands |
| In-flight transaction | Transaction aborted, client sees error |
| Pipelined commands | Commands in send buffer may be lost |
| Mid-snapshot shutdown | Snapshot aborted, previous snapshot retained |

For detailed failure handling and recovery procedures, see [FAILURE_MODES.md](FAILURE_MODES.md).

---

## Health Checks

### Liveness

```
PING
# Returns: PONG
```

The server is alive if it responds to PING.

### Readiness

```
INFO server
```

Check `loading` field - server is ready when loading is complete.

### Health Endpoint (Metrics Port)

| Endpoint | Response |
|----------|----------|
| `/health` | `200 OK` if healthy |
| `/ready` | `200 OK` if ready to accept commands |

---

## Operational States

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ STARTING │───▶│ LOADING │───▶│  READY  │───▶│DRAINING │
└─────────┘    └─────────┘    └─────────┘    └────┬────┘
                                                   │
                                                   ▼
                                             ┌─────────┐
                                             │ STOPPED │
                                             └─────────┘
```

| State | Description | Accepts Commands |
|-------|-------------|------------------|
| STARTING | Initializing components | No |
| LOADING | Recovering from snapshot/WAL | No |
| READY | Normal operation | Yes |
| DRAINING | Shutdown in progress | In-flight only |
| STOPPED | Server stopped | No |

---

## Configuration Reference

```toml
[server]
bind = "127.0.0.1"
port = 6379
num_shards = 0  # 0 = auto-detect CPU cores

[lifecycle]
drain_timeout_s = 30
shutdown_snapshot = false

[persistence]
data_dir = "./data"
# ... see PERSISTENCE.md for full options
```

See [OPERATIONS.md](OPERATIONS.md) for complete configuration guide.
