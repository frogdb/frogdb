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
4. Enable eviction policy (when implemented)

**Client Handling:**
- Retry write operations after delay
- Implement exponential backoff
- Consider circuit breaker pattern for sustained OOM

### Memory Fragmentation

**Trigger:** Allocator fragmentation causes `used_memory_rss` >> `used_memory`.

**Detection:** Monitor `memory_fragmentation_ratio` metric:
- Ratio > 1.5: Moderate fragmentation
- Ratio > 2.0: High fragmentation, consider restart

**Mitigation:**
- Restart during maintenance window
- Use jemalloc allocator (default) for better fragmentation handling
- Consider `MEMORY PURGE` command (if implemented)

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

---

## Node Failures (Cluster Mode)

*Note: Cluster mode is planned for future implementation.*

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

*Note: Built-in rate limiting is planned for future implementation.*

**Current Behavior:** Server processes all requests (no rate limiting).

**Mitigation:**
- Client-side rate limiting
- Proxy-based rate limiting (e.g., envoy, nginx)
- Cluster mode distributes load

### Blocking Command Queue

*Note: Blocking commands (BLPOP, etc.) planned for future implementation.*

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
- [OPERATIONS.md](OPERATIONS.md) - Operational configuration
- [CLUSTER.md](CLUSTER.md) - Cluster architecture (future)
