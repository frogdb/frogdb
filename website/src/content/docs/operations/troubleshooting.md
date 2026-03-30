---
title: "Troubleshooting"
description: "Symptom-driven diagnosis guide for common FrogDB operational issues."
sidebar:
  order: 11
---
Symptom-driven diagnosis guide for common FrogDB operational issues.

## High Latency

### Symptoms
- Slow responses from clients
- p99 latency spikes in metrics
- Client timeouts

### Quick Diagnosis

```bash
# 1. Check for slow commands
SLOWLOG GET 10

# 2. Check throughput
INFO stats
# Look at: instantaneous_ops_per_sec

# 3. Test system baseline (run on server, not client)
frogdb-cli --intrinsic-latency 10
```

### Decision Tree

```
High latency detected
+-- SLOWLOG shows expensive commands?
|   +-- KEYS * -> Use SCAN instead
|   +-- Large SMEMBERS/ZRANGE -> Paginate or limit
|   +-- Large DEL -> Use UNLINK for async delete
+-- Intrinsic latency > 1ms?
|   +-- System issue -> Check OS, hypervisor, hardware
+-- INFO frogdb shows shard imbalance?
|   +-- One shard high queue_depth -> Redistribute keys
+-- Memory pressure (evictions > 0)?
    +-- Increase max_memory or reduce dataset
```

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| O(N) commands | `KEYS *`, `SMEMBERS` on large sets in SLOWLOG | Use SCAN, paginate results |
| Thundering herd expiry | Many keys expiring simultaneously | Spread TTLs with jitter |
| System latency | `--intrinsic-latency` > 1ms | Check THP, CPU throttling, VM settings |
| Memory pressure | High eviction rate | Increase `maxmemory` |
| Shard hotspot | High `queue_depth` in INFO frogdb | Use hash tags to redistribute |
| Cross-shard operations | `scatter-gather` timeouts | Colocate related keys with hash tags |

### System Checks

```bash
# Transparent huge pages (should be disabled)
cat /sys/kernel/mm/transparent_hugepage/enabled
# Fix: echo never > /sys/kernel/mm/transparent_hugepage/enabled

# CPU frequency scaling (should be "performance")
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

---

## Memory Growth

### Symptoms
- RSS memory growing over time
- OOM errors (`-OOM command not allowed`)
- Server killed by OOM killer

### Quick Diagnosis

```bash
# 1. Check memory stats
MEMORY STATS
# Look at: dataset.bytes, peak.allocated

# 2. Get recommendations
MEMORY DOCTOR

# 3. Check fragmentation
INFO memory
# Look at: mem_fragmentation_ratio

# 4. Find large keys
frogdb-cli --bigkeys
```

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Key growth | `keys_total` increasing | Add TTLs, review retention |
| Big keys | `--bigkeys` shows MB+ keys | Split large collections |
| Memory fragmentation | `fragmentation_ratio > 1.5` | Restart server |
| Shard imbalance | Uneven `shard_X_memory` in INFO frogdb | Redistribute with hash tags |
| Connection buffers | High `omem` in CLIENT LIST | Fix slow clients |

### Fragmentation Thresholds

| Ratio | Status | Action |
|-------|--------|--------|
| < 1.0 | **Swapping** | Critical -- add RAM immediately |
| 1.0 - 1.4 | Healthy | Normal operation |
| 1.5 - 2.0 | High | Schedule restart |
| > 2.0 | Critical | Restart soon |

---

## Connection Issues

### Symptoms
- Connection refused errors
- `max clients reached` errors
- Connections being dropped

### Quick Diagnosis

```bash
# 1. Check connections
CLIENT LIST

# 2. Check limits
INFO clients
# Look at: connected_clients, blocked_clients, rejected_connections

# 3. Check socket state
netstat -an | grep 6379 | head -20
```

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Max clients reached | `rejected_connections` > 0 | Increase `maxclients` |
| Slow clients | High `omem` in CLIENT LIST | Fix client read performance |
| Connection leaks | `connected_clients` growing | Fix client pooling |
| TCP exhaustion | Many TIME_WAIT sockets | Enable connection reuse |

### CLIENT LIST Analysis

```
id=1 addr=192.168.1.10:54321 ... idle=0 omem=0 cmd=get      # Healthy
id=2 addr=192.168.1.11:54322 ... idle=300 omem=0 cmd=null   # Idle - possible leak
id=3 addr=192.168.1.12:54323 ... idle=0 omem=1048576 cmd=get # Slow consumer
```

---

## Shard Failures

### Symptoms
- Partial errors (some keys work, others return errors)
- `-ERR shard unavailable` responses
- Degraded health endpoint

### Quick Diagnosis

```bash
# 1. Check per-shard status
INFO frogdb

# 2. Check health endpoint
curl http://localhost:9090/health

# 3. Check logs for panics
grep -i panic /var/log/frogdb/frogdb.log | tail -20
```

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Shard panic | Stack trace in logs | File bug, restart server |
| VLL queue overflow | `-ERR shard queue full` | Increase `vll_max_queue_depth` |
| Shard deadlock | Watchdog timeout in logs | Restart, file bug |

### Identifying Affected Keys

```bash
# Find which shard a key maps to
DEBUG HASHING mykey
# key:mykey hash:0x... shard:3
```

---

## Persistence Issues

### Symptoms
- Data loss after restart
- Slow startup
- Write errors in logs

### Quick Diagnosis

```bash
# 1. Check persistence status
INFO persistence

# 2. Check disk space
df -h /var/lib/frogdb

# 3. Check for errors in logs
grep -i "rocksdb\|persistence" /var/log/frogdb/frogdb.log | tail -20
```

### Causes and Fixes

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
| `periodic` | Up to `fsync_interval_ms` | Balanced (recommended) |
| `sync` | None | Critical data |

### Recovery from Corruption

```bash
# 1. Stop server
systemctl stop frogdb

# 2. Restore from backup
ls -la /var/lib/frogdb/backups/

# 3. Restart
systemctl start frogdb
```

See [persistence.md](/operations/persistence/) for WAL corruption recovery options.

---

## Cross-Shard Operation Failures

### Symptoms
- MGET/MSET/DEL with multiple keys timing out
- `-TIMEOUT` errors on multi-key operations

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Shard hotspot | One shard high `queue_depth` | Redistribute keys with hash tags |
| Large operations | Timeouts on large MGET/MSET | Batch into smaller chunks |
| VLL contention | Long queue depth | Increase `scatter_gather_timeout_ms` |

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

### Tuning

```toml
[server]
scatter_gather_timeout_ms = 1000
vll_max_queue_depth = 10000
```

---

## Replication Issues

### Symptoms
- Replica falling behind
- Replica disconnecting and reconnecting
- Stale reads from replicas

### Quick Diagnosis

```bash
# On primary
INFO replication
# Look at: connected_slaves, master_repl_offset

# On replica
INFO replication
# Compare master_repl_offset vs slave_repl_offset
```

### Causes and Fixes

| Cause | Evidence | Fix |
|-------|----------|-----|
| Network latency | High replication lag | Check network path |
| Slow replica | Replica cannot keep up | Scale replica hardware |
| Write burst | Temporary lag spike | Expected, should recover |
| WAL retention too short | Frequent full resyncs | Increase `min_wal_retention_secs` |

---

## Performance Baseline Checklist

Before deploying to production, verify:

- [ ] **Transparent huge pages disabled**: `echo never > /sys/kernel/mm/transparent_hugepage/enabled`
- [ ] **Overcommit memory enabled**: `sysctl vm.overcommit_memory=1`
- [ ] **Sufficient file descriptors**: `ulimit -n 65535`
- [ ] **`maxmemory` set**: ~80% of available RAM
- [ ] **Durability mode appropriate**: `periodic` for most workloads, `sync` for critical data
- [ ] **Metrics endpoint accessible**: `curl http://localhost:9090/metrics`
- [ ] **Intrinsic latency < 1ms**: `frogdb-cli --intrinsic-latency 10`
- [ ] **Fragmentation < 1.5**: `INFO memory`

---

## Error Response Reference

| Error | Cause |
|-------|-------|
| `-ERR <message>` | General error (syntax, invalid arguments) |
| `-WRONGTYPE ...` | Type mismatch (e.g., GET on a sorted set) |
| `-OOM ...` | Memory limit exceeded |
| `-BUSY <message>` | Server busy (script running) |
| `-NOAUTH ...` | Authentication required |
| `-NOPERM <message>` | ACL permission denied |
| `-CROSSSLOT ...` | Multi-key operation spans slots |
| `-LOADING ...` | Server starting up |
| `-READONLY ...` | Write to replica |
| `-MOVED <slot> <host>:<port>` | Key on different node (cluster) |

---

## Getting Help

When reporting issues, collect:

1. **FrogDB version**: `INFO server | grep version`
2. **OS and kernel**: `uname -a`
3. **Configuration**: `CONFIG GET *` (sanitize passwords)
4. **Relevant logs**: Last 100 lines around the issue
5. **Metrics snapshot**: `INFO all`
6. **Memory state**: `MEMORY DOCTOR`
