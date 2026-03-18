# Server Lifecycle

FrogDB's startup sequence, graceful shutdown, and recovery procedures.

## Startup Sequence

```
1. Parse Configuration
   Figment: CLI args > env vars > config file > defaults

2. Initialize Logging
   tracing subscriber (pretty or JSON format)

3. Initialize Metrics
   Prometheus endpoint on metrics port

4. Open RocksDB
   Create or open at data_dir
   One column family per internal shard

5. Recovery (if data exists)
   Load latest snapshot
   Replay WAL entries after snapshot sequence number

6. Initialize Security
   Load ACL file (if configured)

7. Spawn Shard Workers
   N Tokio tasks (one per internal shard)

8. Start Acceptor
   Bind to port, begin accepting connections

9. Signal Ready
   Log startup complete, update health check status
```

### Startup Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `./data` | RocksDB and snapshot directory |
| `num_shards` | `num_cpus` | Number of internal shard workers |
| `bind` | `127.0.0.1` | Address to bind |
| `port` | `6379` | Port to listen on |

---

## Graceful Shutdown

```
1. Stop Accepting Connections
   Close listener socket, new connections rejected

2. Signal Shutdown to Shard Workers
   Workers stop accepting new commands

3. Drain In-Flight Commands
   Wait for pending commands to complete
   Timeout after drain_timeout_s (default: 30s)

4. Close Client Connections
   Send error to blocked clients, close all TCP connections

5. Flush WAL
   fsync to ensure durability

6. Final Snapshot (Optional)
   If shutdown_snapshot = true, create point-in-time snapshot

7. Close RocksDB
   Flush memtables, close file handles

8. Shutdown Observability
   Flush tracing spans, close metrics endpoint

9. Exit
```

### Shutdown Triggers

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

```toml
[lifecycle]
drain_timeout_s = 30
shutdown_snapshot = false
```

---

## Recovery

On startup with existing data, FrogDB recovers state automatically:

```
1. Check for Snapshots
   Find latest by epoch number

2. Load Snapshot (if exists)
   For each shard: load key-value pairs into memory, rebuild expiry index
   Record snapshot sequence number

3. Replay WAL
   Apply all entries after snapshot sequence number
   Continue until end of WAL

4. Verify Integrity
   Count keys per shard, log recovery statistics
```

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

---

## Operational States

```
STARTING --> LOADING --> READY --> DRAINING --> STOPPED
```

| State | Description | Accepts Commands |
|-------|-------------|------------------|
| STARTING | Initializing components | No |
| LOADING | Recovering from snapshot/WAL | No |
| READY | Normal operation | Yes |
| DRAINING | Shutdown in progress | In-flight only |
| STOPPED | Server stopped | No |

---

## Startup Failures

| Failure | Cause | Resolution |
|---------|-------|------------|
| Port already in use | Another process on same port | Kill other process or change `port` |
| Permission denied (port) | Can't bind port <1024 as non-root | Use higher port or `setcap` |
| Data directory not writable | Permission issue | `chown`/`chmod` the directory |
| RocksDB open failed | Lock held or corruption | Check for other instances; restore from backup |
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

---

## Health Checks

### Liveness

```
PING          # Returns PONG if alive
GET /health/live   # HTTP 200 if process is running
```

### Readiness

```
INFO server   # Check "loading" field
GET /health/ready  # HTTP 200 if accepting commands
```

### Kubernetes Integration

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

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
durability_mode = "periodic"
```

See [configuration.md](configuration.md) for the complete configuration guide.
