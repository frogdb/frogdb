# FrogDB Failure Modes

How FrogDB handles and recovers from various failure scenarios. This document covers error responses, failure detection, recovery procedures, and client recommendations.

## Error Response Format

FrogDB uses Redis-compatible error responses in RESP2 format:

```
-ERR <message>\r\n
```

### Standard Error Codes

| Error | Format | Cause |
|-------|--------|-------|
| Generic | `-ERR <message>` | General errors (syntax, invalid arguments) |
| Wrong Type | `-WRONGTYPE Operation against a key holding the wrong kind of value` | Type mismatch (e.g., GET on a sorted set) |
| Out of Memory | `-OOM command not allowed when used memory > 'maxmemory'` | Memory limit exceeded |
| Busy | `-BUSY <message>` | Server busy (e.g., script running too long) |
| No Auth | `-NOAUTH Authentication required` | Command requires authentication |
| No Perm | `-NOPERM <message>` | ACL permission denied |
| Cross Slot | `-CROSSSLOT Keys in request don't hash to the same slot` | Multi-key operation spans shards |
| Loading | `-LOADING FrogDB is loading the dataset in memory` | Server starting up |
| Readonly | `-READONLY You can't write against a read only replica` | Write to replica |
| Cluster Down | `-CLUSTERDOWN The cluster is down` | Cluster unavailable |
| Moved | `-MOVED <slot> <host>:<port>` | Key on different node (cluster) |
| Ask | `-ASK <slot> <host>:<port>` | Slot migrating (cluster) |

---

## Memory Failures

### Out of Memory (OOM)

**Trigger:** `used_memory` exceeds configured `max_memory` limit.

**Behavior:**

| Operation Type | Response |
|----------------|----------|
| Write commands (SET, ZADD, etc.) | `-OOM command not allowed when used memory > 'maxmemory'` |
| Read commands (GET, ZRANGE, etc.) | Execute normally |
| Delete commands (DEL, EXPIRE) | Execute normally |
| Admin commands (INFO, CONFIG) | Execute normally |

**Detection:**
```rust
fn check_memory_limit(&self) -> Result<(), Error> {
    if self.config.max_memory > 0 && self.memory_used() > self.config.max_memory {
        return Err(Error::OOM);
    }
    Ok(())
}
```

**Recovery:**
1. Delete keys to free memory
2. Wait for TTL expiration to reclaim space
3. Increase `max_memory` configuration
4. Enable eviction policy (future feature, see [EVICTION.md](EVICTION.md))

**Client Handling:**
- Retry write operations after delay
- Implement exponential backoff
- Consider circuit breaker pattern for sustained OOM

### OOM Edge Cases

Commands that allocate during execution have nuanced behavior:

| Scenario | Behavior |
|----------|----------|
| **Read with temporary allocation** (SORT, SMEMBERS) | Executed; may allocate temporarily |
| **SORT with STORE** | Rejected if result would exceed limit |
| **Lua script allocations** | Continues until `lua_max_memory` exceeded, then `-ENOMEM` |
| **Protocol buffers** | Separate limit (`client_output_buffer_limit`) |
| **Internal overhead** | HashMap resize may temporarily exceed limit |

**Internal Overhead Tolerance:**
FrogDB may temporarily exceed `max_memory` during internal operations (hash table growth). This prevents thrashing at the limit boundary.

### Memory Fragmentation

**Trigger:** Allocator fragmentation causes `used_memory_rss` >> `used_memory`.

**Calculation:**
```
fragmentation_ratio = used_memory_rss / used_memory
```

Where:
- `used_memory_rss`: Resident Set Size (actual physical memory from OS)
- `used_memory`: Logical memory tracked by FrogDB

**Thresholds:**

| Ratio | Status | Action |
|-------|--------|--------|
| < 1.0 | **Swapping** | Critical: System is swapping, add RAM immediately |
| 1.0 - 1.4 | **Healthy** | Normal operation |
| 1.4 - 1.5 | **Elevated** | Monitor closely |
| 1.5 - 2.0 | **High** | Schedule maintenance restart |
| > 2.0 | **Critical** | Restart soon; significant memory waste |

**Detection:**
```
INFO memory

# Memory
used_memory:1073741824
used_memory_rss:1610612736
mem_fragmentation_ratio:1.50
```

**Mitigation:**
- Restart during maintenance window (most effective)
- Use jemalloc allocator (default) for better fragmentation handling
- Reduce key churn (frequent create/delete causes fragmentation)
- Consider `MEMORY PURGE` command (if implemented)

**Alerting:**
- Warn at `fragmentation_ratio > 1.5`
- Critical at `fragmentation_ratio > 2.0` or `< 1.0`

### Shard Memory Imbalance

**Trigger:** Uneven key distribution causes some shards to hit memory limits before others.

**Detection:** Compare `used_memory` across shards via `/metrics` endpoint.

**Mitigation:**
- Use hash tags to control key distribution
- Review key naming patterns
- Consider increasing total `max_memory`

---

## Disk Failures

### WAL Write Failure

**Trigger:** RocksDB cannot write to WAL (disk full, I/O error, permission denied).

**Behavior by Durability Mode:**

| Mode | On WAL Failure |
|------|----------------|
| `async` | Log warning, continue (data loss risk) |
| `periodic` | Log error, queue writes, retry on next sync |
| `sync` | Return `-ERR persistence failed` to client |

**Detection:**
- `persistence_errors_total` metric increments
- Error logged: `"WAL write failed: {error}"` at ERROR level

**Recovery:**
1. Check disk space (`df -h`)
2. Verify RocksDB directory permissions
3. Check for I/O errors in system logs (`dmesg`)
4. If disk failed, restore from replica or backup

**Client Handling (sync mode):**
- Retry with exponential backoff
- Fail over to replica if available
- Alert operations team

### Snapshot Write Failure

**Trigger:** Cannot write snapshot to disk.

**Behavior:** Snapshot operation fails, server continues operating.

**Detection:**
- `snapshot_failures_total` metric increments
- Log: `"Snapshot failed: {error}"` at ERROR level

**Impact:**
- Recovery will use older snapshot + WAL replay
- Longer recovery time if server crashes
- No immediate client impact

**Recovery:**
1. Resolve disk issue (space, permissions)
2. Trigger manual snapshot via `BGSAVE` (when implemented)
3. Verify snapshot directory writable

### RocksDB Corruption

**Trigger:** Data corruption in RocksDB files (hardware failure, incomplete write, bug).

**Detection:** RocksDB returns corruption error on read/write.

**Behavior:**
- Startup: Server refuses to start, logs corruption details
- Runtime: Affected shard returns errors, other shards continue

**Recovery:**
1. **Prefer:** Restore from replica with intact data
2. **Fallback:** Restore from last known good snapshot
3. **Last resort:** Delete corrupted column family, lose that shard's data

**Prevention:**
- Use ECC memory
- Enable RocksDB checksums (default)
- Regular backup verification
- Monitor disk health (SMART)

---

## Network Failures

### Client Connection Timeout

**Trigger:** Client doesn't send data within `client_timeout_s` seconds.

**Behavior:**
1. Connection closed by server
2. Any in-progress transaction aborted
3. `connected_clients` metric decrements

**Configuration:**
```toml
[server]
client_timeout_s = 300  # 5 minutes, 0 = no timeout
```

**Client Handling:**
- Implement connection keepalive (TCP or `PING` commands)
- Reconnect on disconnect
- Re-authenticate after reconnect

### Scatter-Gather Timeout

**Trigger:** Multi-shard operation (MGET, MSET) doesn't complete within timeout.

**Behavior:**
```
Client: MGET key1 key2 key3
        │
        ▼ (scatter to shards)
    Shard 0: responds
    Shard 1: responds
    Shard 2: timeout
        │
        ▼
Response: -ERR operation timed out
```

**All-or-nothing semantics:** If any shard times out, entire operation fails.

**Configuration:**
```toml
[server]
scatter_gather_timeout_ms = 1000  # 1 second
```

**Detection:**
- `scatter_gather_timeouts_total` metric increments
- Slow log entry if enabled

**Client Handling:**
- Retry with exponential backoff
- Consider using hash tags to avoid cross-shard operations
- Break large MGET into smaller batches

### Inter-Shard Channel Backpressure

**Trigger:** Internal message channels between shards are full.

**Behavior:**
- Sending shard blocks until channel has space
- Increased latency for cross-shard operations
- May contribute to scatter-gather timeouts

**Detection:**
- `channel_backpressure_events_total` metric
- Increased p99 latency

**Mitigation:**
- Increase channel capacity (requires code change)
- Reduce cross-shard operations
- Scale vertically (faster CPU)

### TCP Connection Exhaustion

**Trigger:** Too many client connections.

**Behavior:**
- New connections rejected with connection refused
- `connection_errors_total` metric increments

**Configuration:**
```toml
[server]
max_connections = 10000  # 0 = unlimited (OS limit)
```

**Client Handling:**
- Use connection pooling
- Implement connection limits per client
- Consider using UNIX sockets for local connections

### Replication Failures

For all replication-related failure scenarios including:
- Replica streaming stall
- Replication stream corruption detection
- Primary OOM during checkpoint
- Graceful shutdown during replication
- Checkpoint I/O errors
- CPU exhaustion effects on replication
- Asymmetric network failures

See [REPLICATION.md - Failure Modes](REPLICATION.md#failure-modes).

---

## Shard Worker Failures

### Shard Worker Panic

**Trigger:** Tokio task (shard worker) panics due to bug, assertion failure, or unrecoverable error.

**Detection:**
- Panic handler logs stack trace at ERROR level
- `shard_panic_total` metric increments
- Connections on affected shard receive errors

**Behavior:**

```
Shard 2 Worker panics
         │
         ├── 1. Tokio runtime catches panic
         │
         ├── 2. Shard marked unhealthy
         │       └── Health check returns degraded
         │
         ├── 3. Pending operations on Shard 2:
         │       └── Return -ERR shard unavailable
         │
         ├── 4. Connections with in-flight requests to Shard 2:
         │       └── Receive error response
         │
         └── 5. Other shards continue serving
```

**Recovery:**

| Strategy | Description | Trade-off |
|----------|-------------|-----------|
| **Manual restart** | Operator restarts server | Full recovery, downtime |
| **Automatic shard restart** | Shard worker respawned from WAL | Faster, but complex |
| **Graceful degradation** | Shard offline, others continue | Partial service |

**Automatic Shard Recovery (if implemented):**

```rust
async fn shard_supervisor(shard_id: usize) {
    loop {
        let result = std::panic::catch_unwind(|| {
            run_shard_worker(shard_id)
        });

        match result {
            Ok(_) => break, // Clean shutdown
            Err(panic) => {
                error!("Shard {} panicked: {:?}", shard_id, panic);
                metrics.shard_panic_total.inc();

                // Exponential backoff before restart
                let delay = backoff.next_delay();
                if delay > MAX_RESTART_DELAY {
                    error!("Shard {} exceeded restart limit, staying down", shard_id);
                    break;
                }
                tokio::time::sleep(delay).await;

                // Reload shard state from RocksDB
                info!("Restarting shard {}", shard_id);
            }
        }
    }
}
```

**Client Handling:**
- Retry operations that received `-ERR shard unavailable`
- Use circuit breaker pattern to avoid hammering unhealthy shards
- Consider `READONLY` flag to fall back to replica (in cluster mode)

### Shard Worker Deadlock

**Trigger:** Async task blocks on operation that never completes (rare, indicates bug).

**Detection:**
- Watchdog timer: If shard doesn't process any message for `watchdog_timeout_s`, considered stuck
- `shard_watchdog_timeouts_total` metric

**Configuration:**
```toml
[server]
watchdog_timeout_s = 60  # 0 = disabled
```

**Behavior (if watchdog enabled):**
1. Log error with shard state
2. Mark shard unhealthy
3. Optionally abort and restart shard (implementation-dependent)

**Prevention:**
- All shard operations must be non-blocking
- Use timeouts on all channel operations
- Avoid `block_on` within async context

---

## Node Failures (Cluster Mode)

*Note: Cluster mode is partially implemented (Phases 1 and 3 complete; Phases 4-6 pending).*

### Primary Unavailable

**Trigger:** Primary node crashes, network partition, or overloaded.

**Detection:**
- Orchestrator health checks fail
- Replicas detect replication stream disconnect

**Behavior:**
1. Orchestrator initiates failover
2. Promote replica to primary
3. Update cluster topology
4. Clients receive `-MOVED` redirections

**Client Handling:**
- Cache cluster topology locally
- Update topology on `-MOVED` response
- Retry failed operations on new primary

### Replica Unavailable

**Trigger:** Replica node crashes or disconnected.

**Detection:** Orchestrator health checks fail.

**Behavior:**
- Read traffic redistributed to other replicas or primary
- Reduced read capacity until replica recovers

**Impact:**
- No data loss (replica is copy of primary)
- Reduced availability for `READONLY` clients

### Split-Brain Prevention

**Design:** FrogDB uses orchestrated topology (no gossip), preventing split-brain.

**Mechanism:**
- Single orchestrator is source of truth
- Nodes only accept topology from authenticated orchestrator
- Fencing: Old primary rejects writes after demotion

### Split-Brain Recovery Procedure

Despite prevention mechanisms, a split-brain window exists during failover. When detected:

**Automatic Actions:**
1. Old primary receives demotion topology with higher epoch
2. Compares local sequence vs last replicated sequence
3. Divergent writes logged to `data/split_brain_discarded.log`
4. Local data rolled back to last replicated state
5. Node connects as replica to new primary

**Log File Format:**
```
# data/split_brain_discarded.log
# Header
timestamp=2024-01-15T10:30:45Z
old_primary=node-abc
new_primary=node-def
epoch_old=41
epoch_new=42
seq_diverge_start=12345
seq_diverge_end=12400
ops_discarded=55

# Discarded operations (RESP format)
*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n
*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n
...
```

**Manual Recovery Options:**

| Option | When to Use | Procedure |
|--------|-------------|-----------|
| **Discard** | Data not critical, conflicts likely | Delete log file |
| **Review & Replay** | Data critical, conflicts resolvable | Parse log, apply via CLI |
| **Merge** | Business logic can resolve conflicts | Custom script with conflict resolution |

**Replay Tool (Future):**
```bash
# Dry-run to show what would be applied
frogdb-admin split-brain-replay --dry-run data/split_brain_discarded.log

# Apply with conflict handling
frogdb-admin split-brain-replay --on-conflict=skip data/split_brain_discarded.log
```

**Monitoring:**
```
frogdb_split_brain_events_total           # Split-brain detections
frogdb_split_brain_ops_discarded_total    # Operations lost
frogdb_split_brain_recovery_pending       # 1 if log file exists unprocessed
```

### Cascading Failure Matrix

When multiple components fail simultaneously, recovery becomes complex. This matrix documents behavior for common multi-component failures.

**Two-Component Failures:**

| Component A | Component B | Behavior | Recovery |
|-------------|-------------|----------|----------|
| Primary OOM | Replica OOM | Both refuse FULLRESYNC | Add memory to one, restart |
| Primary disk | Replica disk | No persistence anywhere | Restore from backup |
| Network (Pri↔Rep) | Network (Rep↔Orch) | Replica isolated | Wait for network heal |
| Primary crash | Orchestrator down | No automatic failover | Manual promotion |
| Replica crash | WAL retention exceeded | FULLRESYNC required on recover | Increase retention |

**Three-Component Failures:**

| Failure Scenario | Behavior | Recovery Path |
|------------------|----------|---------------|
| Primary + all replicas crash | **Data loss possible** if WAL not synced | Restore from backup |
| Primary + Orchestrator + Network | Replicas orphaned, stale data | Restore orchestrator first |
| Primary disk + Replica OOM + Network | No valid data source | Wait for network, clear replica memory |
| CPU exhaustion + Disk full + Memory pressure | Complete service degradation | Shed load: reject connections, clear temp files |

**Recovery Priority Order:**

When multiple failures exist, recover in this order:

1. **Network** - Restore connectivity first (enables coordination)
2. **Orchestrator** - Required for topology decisions
3. **Primary storage** - Data durability
4. **Primary memory** - Write capability
5. **Replica storage** - Redundancy
6. **Replica memory** - Read scaling

**Cascading Failure Prevention:**

| Configuration | Purpose |
|---------------|---------|
| `max_memory` with headroom | Prevent OOM cascade |
| `disk_usage_warning_percent = 80` | Alert before disk full |
| `orchestrator_replicas = 3` | Orchestrator redundancy |
| `min_replicas_to_write = 1` | Ensure at least one replica confirms |
| `wal_retention_size = 200MB` | Buffer for replica recovery |

**Metrics for Cascade Detection:**

```
frogdb_component_failures_total{component="disk"}
frogdb_component_failures_total{component="memory"}
frogdb_component_failures_total{component="network"}
frogdb_simultaneous_failures_total  # Multiple components failing together
```

**Alert Rules:**

```yaml
# Alert on multiple simultaneous failures
- alert: CascadingFailure
  expr: sum(frogdb_component_failures_total) > 1
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Multiple component failures detected"
```

---

## Graceful Degradation

How FrogDB behaves under overload and partial failure conditions.

### Overload Response

When system resources are exhausted, FrogDB degrades gracefully rather than failing completely:

| Resource | Behavior | Client Impact |
|----------|----------|---------------|
| **Memory (OOM)** | Reject writes, allow reads/deletes | Write errors, reads succeed |
| **CPU (high load)** | Increased latency, backpressure | Slower responses |
| **Disk I/O** | Persistence backpressure | Higher write latency |
| **Connections** | Reject new, serve existing | New clients fail to connect |
| **Single shard down** | Other shards continue | Errors for affected keys only |

### Partial Availability

FrogDB prioritizes partial availability over total failure:

```
Shard 0: Healthy ✓
Shard 1: Healthy ✓
Shard 2: FAILED ✗
Shard 3: Healthy ✓
```

**Behavior:**
- Commands for keys on shards 0, 1, 3: Execute normally
- Commands for keys on shard 2: Return `-ERR shard unavailable`
- Multi-key commands touching shard 2: Fail (fail-all semantics)

### Adaptive Load Shedding

Under extreme load, FrogDB sheds load progressively:

**Level 1 - Backpressure:**
- Channel buffers fill, senders block
- Natural slowdown, no errors

**Level 2 - Queue rejection:**
- [VLL](VLL.md) queue exceeds `vll_max_queue_depth`
- New operations rejected: `-ERR shard queue full, try again later`

**Level 3 - Connection rejection:**
- `maxclients` exceeded
- New connections refused

**Level 4 - OOM:**
- Memory limit exceeded
- Writes rejected, reads continue

### Health Endpoints

```
GET /health          # Returns 200 if any shard healthy, 503 if all down
GET /health/ready    # Returns 200 if fully operational, 503 if degraded
GET /health/live     # Returns 200 if process alive
```

**Response format:**
```json
{
  "status": "degraded",
  "shards": {
    "healthy": 3,
    "unhealthy": 1,
    "total": 4
  },
  "memory_pressure": false,
  "accepting_writes": true
}
```

### Circuit Breaker Pattern (Client-Side)

FrogDB does not implement server-side circuit breakers. Clients should implement:

```python
# Client-side circuit breaker example
class FrogDBCircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=30):
        self.failures = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = None

    def call(self, operation):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
            else:
                raise CircuitOpenError()

        try:
            result = operation()
            if self.state == "half-open":
                self.state = "closed"
                self.failures = 0
            return result
        except FrogDBError as e:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "open"
            raise
```

### Priority (Future)

Command priority is not currently implemented. Future consideration:

```toml
# Hypothetical priority configuration
[server]
priority_queues = true
admin_priority = "high"      # CONFIG, INFO, DEBUG
read_priority = "normal"     # GET, MGET, SCAN
write_priority = "normal"    # SET, DEL
bulk_priority = "low"        # KEYS, FLUSHDB
```

---

## Crash Recovery

### Process Crash

**Causes:**
- Panic (bug)
- OOM killer
- SIGKILL
- Hardware failure

**Data Safety:**

| Durability Mode | Data at Risk |
|-----------------|--------------|
| `async` | All unflushed writes (unbounded) |
| `periodic` | Writes in current period (up to `periodic_sync_ms`) |
| `sync` | None (all acknowledged writes persisted) |

**Recovery Sequence:**
1. Process restarts (via supervisor, systemd, k8s)
2. Load configuration
3. Open RocksDB (automatic WAL replay)
4. Restore snapshot if exists
5. Replay WAL entries after snapshot
6. Accept client connections

### Mid-Write Crash

**Scenario:** Server crashes during command execution.

**RocksDB Guarantees:**
- Atomic writes: Either fully applied or not at all
- WAL provides durability for acknowledged writes

**Outcome:**
- Writes acknowledged before crash: Recovered
- Write in progress at crash: Lost (client sees timeout)

### Mid-Snapshot Crash

**Scenario:** Server crashes during snapshot creation.

**Outcome:**
- Partial snapshot file on disk
- On restart: Partial snapshot ignored, use previous complete snapshot + WAL

**Implementation:**
```rust
// Snapshot written to temp file, atomically renamed on completion
fn create_snapshot(&self) -> Result<()> {
    let temp_path = format!("{}.tmp", self.snapshot_path);
    self.write_snapshot(&temp_path)?;
    std::fs::rename(&temp_path, &self.snapshot_path)?;  // Atomic on POSIX
    Ok(())
}
```

### Startup Failures

| Failure | Cause | Recovery |
|---------|-------|----------|
| Port in use | Another process using port | Kill other process or change port |
| Permission denied | Can't bind port <1024 | Run as root or use higher port |
| RocksDB open failed | Corruption or lock held | Check for other instances, restore from backup |
| Snapshot load failed | Corrupted snapshot | Delete snapshot, rely on WAL |
| Configuration invalid | Bad TOML syntax | Fix configuration file |

---

## Overload Handling

### Too Many Connections

**Trigger:** Connection count exceeds `max_connections`.

**Behavior:** New connections rejected, existing connections unaffected.

**Metrics:**
- `connected_clients` (current)
- `rejected_connections_total` (counter)

### Too Many Requests (Rate Limiting)

**Current Behavior:** Server processes all requests (no rate limiting).

**Mitigation:**
- Client-side rate limiting
- Proxy-based rate limiting (e.g., envoy, nginx)
- Cluster mode distributes load

### Blocking Command Queue

**Design:** Blocked clients tracked separately, don't consume shard resources.

**Timeout:** Blocking commands have mandatory timeout parameter.

---

## Command Timeouts

### Slow Commands

**Detection:** Commands exceeding threshold logged to slow log.

**Configuration:**
```toml
[server]
slowlog_log_slower_than_us = 10000  # 10ms
slowlog_max_len = 128
```

**Common Causes:**
- `KEYS *` (scans all keys)
- Large `ZRANGE` results
- Lua scripts without time limits
- Cross-shard operations

### Lua Script Timeout

**Trigger:** Lua script runs longer than configured limit.

**Behavior:**
1. Script continues to timeout threshold
2. After threshold: Script receives kill flag
3. Script checks flag and may terminate
4. Server may return `-BUSY` to other clients

**Configuration:**
```toml
[scripting]
lua_time_limit_ms = 5000  # 5 seconds
```

**Client Handling:**
- Optimize scripts
- Break large operations into batches
- Use `redis.replicate_commands()` for read-heavy scripts

---

## Client Recommendations

### Connection Management

```
1. Use connection pooling
2. Implement health checks (PING)
3. Handle reconnection gracefully
4. Re-authenticate after reconnect
5. Resubscribe to pub/sub channels after reconnect
```

### Retry Strategy

```
Operation failed?
    │
    ├─ OOM → Backoff and retry (memory may be freed)
    ├─ BUSY → Wait and retry (operation in progress)
    ├─ Timeout → Retry with same or idempotent operation
    ├─ MOVED → Update topology, retry on correct node
    ├─ READONLY → Retry on primary
    ├─ LOADING → Wait and retry (server starting)
    └─ Other errors → Evaluate if retriable
```

### Circuit Breaker Pattern

For sustained failures, implement circuit breaker:

```
CLOSED (normal)
    │ failure count exceeds threshold
    ▼
OPEN (failing fast)
    │ after timeout period
    ▼
HALF-OPEN (testing)
    │ success → CLOSED
    │ failure → OPEN
```

### Idempotency

For operations that may need retry:
- Use `SET key value NX` instead of `SET key value` for create-only
- Use `ZADD key NX score member` for create-only sorted set adds
- Track request IDs client-side for deduplication

---

## Monitoring and Alerting

### Key Metrics for Failure Detection

| Metric | Alert Threshold | Meaning |
|--------|-----------------|---------|
| `used_memory / max_memory` | > 0.9 | Near OOM |
| `persistence_errors_total` | Any increase | Disk issues |
| `rejected_connections_total` | Any increase | Connection exhaustion |
| `scatter_gather_timeouts_total` | > 0.1% of ops | Cross-shard issues |
| `connected_clients` | > 80% of max | Connection pressure |
| `command_latency_p99` | > 10x baseline | Performance degradation |

### Log Messages to Alert On

```
ERROR "WAL write failed"         → Disk failure
ERROR "Snapshot failed"          → Disk failure
ERROR "RocksDB error"            → Storage corruption
ERROR "OOM"                      → Memory exhaustion
WARN  "Channel backpressure"     → Internal overload
WARN  "Slow command"             → Performance issue
```

---

## References

- [PERSISTENCE.md](PERSISTENCE.md) - Durability configuration
- [LIFECYCLE.md](LIFECYCLE.md) - Startup/shutdown procedures
- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics and logging
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration system
- [CLUSTER.md](CLUSTER.md) - Cluster architecture (partially implemented)
