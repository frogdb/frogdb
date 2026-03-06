# FrogDB Troubleshooting Guide

Step-by-step diagnosis for common issues. For debugging tools and techniques, see [DEBUGGING.md](DEBUGGING.md). For failure recovery procedures, see [FAILURE_MODES.md](FAILURE_MODES.md). For passive observability, see [OBSERVABILITY.md](OBSERVABILITY.md).

## Feature Labels

Throughout this document:
- **Unmarked items** apply to any Redis-compatible database
- **`[FrogDB]`** indicates FrogDB-specific diagnostics

---

## High Latency

### Symptoms
- Slow responses from clients
- p99 latency spikes in metrics
- Client timeouts
- Application performance degradation

### Quick Diagnosis

```bash
# 1. Check for slow commands
SLOWLOG GET 10

# 2. Get automated latency analysis
LATENCY DOCTOR

# 3. Check throughput
INFO stats
# Look at: instantaneous_ops_per_sec

# 4. Test system baseline (run on server, not client)
frogdb-cli --intrinsic-latency 10
```

### Decision Tree

```
High latency detected
├── SLOWLOG shows expensive commands?
│   ├── KEYS * → Use SCAN instead
│   ├── Large SMEMBERS/ZRANGE → Paginate or limit
│   └── Large DEL → Use UNLINK for async delete
├── LATENCY DOCTOR reports expire-cycle?
│   └── Many keys expiring simultaneously → Spread TTLs
├── Intrinsic latency > 1ms?
│   └── System issue → Check OS, hypervisor, hardware
├── [FrogDB] INFO frogdb shows shard imbalance?
│   └── One shard high queue_depth → Redistribute keys
└── Memory pressure (evictions > 0)?
    └── Increase max_memory or reduce dataset
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| O(N) commands | `KEYS *`, `SMEMBERS` on large sets in SLOWLOG | Use SCAN, paginate results |
| Thundering herd expiry | `expire-cycle` in LATENCY DOCTOR | Spread TTLs with jitter |
| System latency | `--intrinsic-latency` > 1ms | Check THP, CPU throttling, VM |
| Memory pressure | High eviction rate | Increase `max_memory` |
| Shard hotspot `[FrogDB]` | High `queue_depth` in INFO frogdb | Use hash tags to redistribute |
| Cross-shard operations `[FrogDB]` | `scatter-gather` in LATENCY DOCTOR | Colocate related keys |
| Blocking Lua scripts | BUSY errors | Optimize scripts, add yields |

### Additional Diagnostics

**Check transparent huge pages (Linux):**
```bash
cat /sys/kernel/mm/transparent_hugepage/enabled
# Should show [never], not [always]
# Fix: echo never > /sys/kernel/mm/transparent_hugepage/enabled
```

**Check CPU frequency scaling:**
```bash
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
# Should show "performance", not "powersave"
```

---

## Memory Growth

### Symptoms
- RSS memory growing over time
- OOM errors (`-OOM command not allowed`)
- Increasing eviction rate
- Server killed by OOM killer

### Quick Diagnosis

```bash
# 1. Compare dataset vs allocated
MEMORY STATS
# Look at: dataset.bytes, peak.allocated

# 2. Get memory recommendations
MEMORY DOCTOR

# 3. Check fragmentation
INFO memory
# Look at: mem_fragmentation_ratio

# 4. Find large keys
frogdb-cli --bigkeys
```

### Decision Tree

```
Memory growing
├── Keys growing? (INFO keyspace)
│   ├── No TTLs set → Add expiration to keys
│   └── TTLs too long → Reduce TTL values
├── Fragmentation ratio > 1.5?
│   └── Schedule restart during maintenance
├── Big keys found?
│   └── Split into smaller collections
├── [FrogDB] Shard imbalance in memory?
│   └── Review key naming, use hash tags
└── Peak >> current?
    └── Restart to reclaim fragmented memory
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Key growth | `keys_total` increasing in INFO | Add TTLs, review retention |
| Big keys | `--bigkeys` shows MB+ keys | Split large collections |
| Memory fragmentation | `fragmentation_ratio > 1.5` | Restart server |
| Shard imbalance `[FrogDB]` | Uneven `shard_X_memory` in INFO frogdb | Redistribute with hash tags |
| Connection buffers | High `omem` in CLIENT LIST | Fix slow clients |
| Lua memory | High script memory | Optimize scripts |

### Memory Thresholds

| Fragmentation Ratio | Status | Action |
|--------------------|--------|--------|
| < 1.0 | **Swapping** | Critical: Add RAM immediately |
| 1.0 - 1.4 | Healthy | Normal operation |
| 1.4 - 1.5 | Elevated | Monitor closely |
| 1.5 - 2.0 | High | Schedule restart |
| > 2.0 | Critical | Restart soon |

### Reducing Memory Usage

```bash
# Check memory for specific key
MEMORY USAGE mykey SAMPLES 5

# Analyze key type distribution
DEBUG OBJECT mykey

# Force lazy-free of deleted keys
CONFIG SET lazyfree-lazy-expire yes
CONFIG SET lazyfree-lazy-server-del yes
```

---

## Connection Issues

### Symptoms
- Connection refused errors
- Client connection timeouts
- Connections being dropped
- `max clients reached` errors

### Quick Diagnosis

```bash
# 1. Check connection state
CLIENT LIST

# 2. Compare against limits
INFO clients
# Look at: connected_clients, blocked_clients

# 3. Check socket state
netstat -an | grep 6379 | head -20

# 4. Check for rejected connections
INFO stats
# Look at: rejected_connections
```

### Decision Tree

```
Connection issues
├── rejected_connections > 0?
│   └── Increase max_clients
├── Many TIME_WAIT sockets?
│   └── Enable tcp_tw_reuse, use persistent connections
├── High idle connections?
│   └── Configure connection pool properly
├── High omem in CLIENT LIST?
│   └── Slow clients backing up → Fix consumer performance
└── blocked_clients high?
    └── Review blocking command timeouts
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Max clients | `rejected_connections` > 0 | Increase `max_clients` config |
| Slow clients | High `omem` in CLIENT LIST | Fix client read performance |
| Connection leaks | `connected_clients` growing | Fix client pooling |
| TCP exhaustion | Many TIME_WAIT sockets | Enable connection reuse |
| Network issues | Sporadic timeouts | Check network path |

### CLIENT LIST Analysis

```
CLIENT LIST
id=1 addr=192.168.1.10:54321 ... idle=0 omem=0 cmd=get      # Healthy
id=2 addr=192.168.1.11:54322 ... idle=300 omem=0 cmd=null   # Idle - possible leak
id=3 addr=192.168.1.12:54323 ... idle=0 omem=1048576 cmd=get # Slow consumer
```

**Red flags:**
- `idle > 300` for non-pub/sub clients → Connection leak
- `omem > 1MB` → Slow client backing up
- Many clients with same `addr` → Pool misconfiguration

### Connection Limits

```toml
[server]
max_clients = 10000          # Adjust based on needs
client_timeout_s = 300       # Close idle connections after 5 min
tcp_keepalive = 300          # TCP keepalive interval
```

---

## Shard Failures `[FrogDB]`

### Symptoms
- Partial errors (some keys work, some don't)
- `-ERR shard unavailable` responses
- Degraded health endpoint
- Shard panic in logs

### Quick Diagnosis

```bash
# 1. Check per-shard status
INFO frogdb
# Look for missing or zero values

# 2. Check health endpoint
curl http://localhost:9090/health
# Look at: shards.healthy, shards.unhealthy

# 3. Check logs for panics
grep -i panic /var/log/frogdb/frogdb.log | tail -20

# 4. Check VLL queue state (see VLL.md)
DEBUG DUMP-VLL-QUEUE <shard_id>
```

### Decision Tree

```
Shard unavailable
├── Panic in logs?
│   ├── Stack trace → File bug report with trace
│   └── OOM in shard → Increase memory, check big keys
├── VLL queue full?
│   └── Increase vll_max_queue_depth or reduce load
├── Watchdog timeout?
│   └── Shard stuck → Check for blocking operations
└── Partial responses?
    └── Some shards healthy → Use hash tags for resilience
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Shard panic | Stack trace in logs | File bug, restart server |
| VLL queue overflow | `-ERR shard queue full` | Increase queue depth |
| Shard deadlock | Watchdog timeout in logs | Restart, file bug |
| Memory exhaustion | OOM in shard worker | Reduce per-shard load |

### Identifying Affected Keys

```bash
# Find which shard a key maps to
DEBUG HASHING mykey
# key:mykey hash:0x... shard:3

# If shard 3 is down, keys hashing to shard 3 are affected
```

### Recovery Steps

1. **Check if auto-recovery is configured:**
   ```toml
   [server]
   shard_auto_restart = true
   shard_restart_backoff_ms = 1000
   ```

2. **Manual recovery:**
   ```bash
   # Restart entire server if shard won't recover
   systemctl restart frogdb
   ```

3. **See [FAILURE_MODES.md](FAILURE_MODES.md#shard-worker-failures) for detailed procedures**

---

## Persistence Issues

### Symptoms
- Data loss after restart
- Slow startup
- Write errors in logs
- Disk full alerts

### Quick Diagnosis

```bash
# 1. Check persistence status
INFO persistence
# Look at: rdb_last_bgsave_status, aof_last_write_status

# 2. Check disk space
df -h /var/lib/frogdb

# 3. Check for RocksDB errors in logs
grep -i "rocksdb\|persistence" /var/log/frogdb/frogdb.log | tail -20

# 4. Check write latency
LATENCY HISTORY persistence-write
```

### Decision Tree

```
Persistence issues
├── rdb_last_bgsave_status = err?
│   ├── Disk full → Free space or expand disk
│   └── Permission denied → Fix directory permissions
├── Slow startup?
│   ├── Large dataset → Expected, monitor progress
│   └── Many WAL files → Increase snapshot frequency
├── Write errors in logs?
│   ├── I/O error → Check disk health (SMART)
│   └── Corruption → Restore from backup
└── Data missing after restart?
    └── Check durability mode (async loses data on crash)
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Disk full | `df` shows full disk | Free space, add capacity |
| Permission denied | "permission denied" in logs | `chown -R frogdb:frogdb /var/lib/frogdb` |
| Disk failure | I/O errors in dmesg | Replace disk, restore from backup |
| Corruption | RocksDB corruption error | Restore from replica or backup |
| Wrong durability | Data loss on crash | Use `periodic` or `sync` mode |

### Durability Modes

| Mode | Data at Risk on Crash | Use Case |
|------|----------------------|----------|
| `async` | All unflushed writes | Cache-only, acceptable loss |
| `periodic` | Up to `periodic_sync_ms` | Balanced (recommended) |
| `sync` | None (acknowledged = durable) | Critical data, lower throughput |

### Recovery from Corruption

```bash
# 1. Stop server
systemctl stop frogdb

# 2. Check for backup
ls -la /var/lib/frogdb/backups/

# 3. Restore from latest backup
frogdb-admin restore --from /var/lib/frogdb/backups/latest

# 4. Restart
systemctl start frogdb
```

See [FAILURE_MODES.md](FAILURE_MODES.md#disk-failures) for detailed recovery procedures.

---

## Cross-Shard Operation Failures `[FrogDB]`

### Symptoms
- MGET/MSET/DEL with multiple keys timing out
- `-TIMEOUT` errors on multi-key operations
- Scatter-gather timeout metrics increasing
- Partial results (should never happen - FrogDB uses fail-all)

### Quick Diagnosis

```bash
# 1. Enable scatter tracing
CONFIG SET loglevel debug

# 2. Check timeout metrics
# Via Prometheus: frogdb_scatter_gather_timeouts_total

# 3. Check per-shard queue depths
INFO frogdb
# Look for high queue_depth on specific shards

# 4. Check VLL contention
DEBUG DUMP-VLL-QUEUE <suspected_shard>
```

### Decision Tree

```
Cross-shard timeouts
├── One shard consistently slow?
│   ├── High queue_depth → Hotspot, redistribute keys
│   └── High command latency → Check that shard's workload
├── All shards slow?
│   ├── Overall load too high → Scale or reduce load
│   └── System resource issue → Check CPU, memory, disk
├── Sporadic timeouts?
│   ├── Network issues → Check network stability
│   └── GC pauses → Shouldn't happen (Rust), file bug
└── Specific operations fail?
    └── Very large MGET/MSET → Batch into smaller chunks
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Shard hotspot | One shard high `queue_depth` | Redistribute keys with hash tags |
| VLL contention | Long queue in DEBUG DUMP-VLL-QUEUE | Increase `vll_queue_timeout_ms` |
| Large operations | Timeouts on large MGET/MSET | Batch into smaller chunks |
| Network issues | Sporadic failures | Check network stability |

### Avoiding Cross-Shard Operations

Use hash tags to colocate related keys:

```bash
# These keys hash to the same shard
SET {user:123}:profile "..."
SET {user:123}:settings "..."
SET {user:123}:session "..."

# This MGET is shard-local (fast, atomic)
MGET {user:123}:profile {user:123}:settings {user:123}:session
```

### Tuning Scatter-Gather

```toml
[server]
scatter_gather_timeout_ms = 1000    # Default: 1 second
vll_queue_timeout_ms = 5000         # Per-shard queue timeout
vll_max_queue_depth = 10000         # Max pending ops per shard
```

---

## Replication Issues

### Symptoms
- Replica falling behind
- Replication lag alerts
- Replica disconnecting and reconnecting
- Stale reads from replicas

### Quick Diagnosis

```bash
# 1. Check replication status
INFO replication
# Look at: role, connected_slaves, master_repl_offset

# 2. Check replica lag
# On replica:
INFO replication
# Compare: master_repl_offset vs slave_repl_offset

# 3. Check for disconnects in logs
grep -i "replica\|replication" /var/log/frogdb/frogdb.log | tail -20
```

### Causes & Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Network latency | High replication lag | Check network path |
| Slow replica | Replica can't keep up | Scale replica hardware |
| Large write burst | Temporary lag spike | Expected, should recover |
| Disk I/O on replica | Replica persistence slow | Use faster storage |

### Monitoring Replication Lag

```bash
# Primary: check connected replicas
INFO replication

# Replica: check offset difference
# master_repl_offset - slave_repl_offset = bytes behind
```

---

## Performance Baseline Checklist

Before deploying to production, verify:

### System Configuration

- [ ] **Transparent huge pages disabled**
  ```bash
  echo never > /sys/kernel/mm/transparent_hugepage/enabled
  ```

- [ ] **Overcommit memory enabled** (if not using swap)
  ```bash
  sysctl vm.overcommit_memory=1
  ```

- [ ] **Sufficient file descriptors**
  ```bash
  ulimit -n 65535
  ```

- [ ] **CPU frequency scaling disabled**
  ```bash
  cpupower frequency-set -g performance
  ```

### FrogDB Configuration

- [ ] **max_memory set appropriately**
  ```toml
  [server]
  max_memory = "8GB"  # ~80% of available RAM
  ```

- [ ] **Durability mode appropriate for use case**
  ```toml
  [persistence]
  durability_mode = "periodic"  # or "sync" for critical data
  ```

- [ ] **Latency monitoring enabled**
  ```toml
  [latency]
  monitor_threshold_ms = 100
  ```

- [ ] **Metrics endpoint accessible**
  ```bash
  curl http://localhost:9090/metrics | head
  ```

### Baseline Tests

- [ ] **Intrinsic latency < 1ms**
  ```bash
  frogdb-cli --intrinsic-latency 10
  ```

- [ ] **Benchmark baseline established**
  ```bash
  redis-benchmark -h localhost -p 6379 -t set,get -n 100000 -c 50
  ```

- [ ] **Memory fragmentation < 1.5**
  ```bash
  frogdb-cli INFO memory | grep fragmentation
  ```

---

## Getting Help

### Information to Collect

When reporting issues, include:

1. **FrogDB version:** `INFO server | grep version`
2. **OS and kernel:** `uname -a`
3. **Configuration:** `CONFIG GET *` (sanitize passwords)
4. **Relevant logs:** Last 100 lines around the issue
5. **Metrics snapshot:** `INFO all`
6. **Memory state:** `MEMORY DOCTOR`
7. **Latency state:** `LATENCY DOCTOR`

### Diagnostic Bundle `[FrogDB]`

```bash
# Generate diagnostic bundle (future feature)
frogdb-admin diagnostic-bundle --output /tmp/frogdb-diag.tar.gz
```

### References

- [DEBUGGING.md](DEBUGGING.md) - Tools and techniques
- [FAILURE_MODES.md](FAILURE_MODES.md) - Recovery procedures
- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics reference
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration guide
