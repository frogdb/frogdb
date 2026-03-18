# Eviction and Memory Management

FrogDB manages memory through eviction policies and optional tiered storage (hot/warm).

## Memory Limit

Set the global memory limit:

```toml
[memory]
max_memory = 0                    # 0 = unlimited (bytes)
# max_memory = "4GB"              # Human-readable also supported
maxmemory_policy = "noeviction"   # Eviction policy
```

`maxmemory` applies to the total memory across all internal shards, not per-shard.

Runtime changes:
```
CONFIG SET maxmemory 8589934592
CONFIG SET maxmemory-policy allkeys-lru
```

---

## Eviction Policies

| Policy | Scope | Description |
|--------|-------|-------------|
| `noeviction` | -- | Return OOM error, reject writes (default) |
| `volatile-lru` | Keys with TTL | Evict least recently used |
| `allkeys-lru` | All keys | Evict least recently used |
| `volatile-lfu` | Keys with TTL | Evict least frequently used |
| `allkeys-lfu` | All keys | Evict least frequently used |
| `volatile-random` | Keys with TTL | Evict random keys |
| `allkeys-random` | All keys | Evict random keys |
| `volatile-ttl` | Keys with TTL | Evict keys with shortest TTL |
| `tiered-lru` | All keys | Demote least recently used to warm tier |
| `tiered-lfu` | All keys | Demote least frequently used to warm tier |

### Choosing a Policy

- **Cache workloads**: Use `allkeys-lru` or `allkeys-lfu` to automatically evict less-useful keys.
- **Mixed workloads**: Use `volatile-lru` or `volatile-lfu` to only evict keys that have a TTL set.
- **Tiered storage**: Use `tiered-lru` or `tiered-lfu` to demote values to disk instead of deleting them.
- **No eviction**: Use `noeviction` when data loss is unacceptable (writes fail with OOM when limit is reached).

### Sampling

FrogDB uses approximate LRU/LFU via sampling (same as Redis). The `maxmemory-samples` setting controls accuracy vs. CPU cost:

| Setting | Accuracy | Use Case |
|---------|----------|----------|
| 3 | ~85% | High-throughput, tolerates eviction variance |
| 5 (default) | ~93% | General purpose |
| 10 | ~98% | Latency-sensitive, predictable eviction |

```
CONFIG SET maxmemory-samples 10
```

### LFU Tuning

For LFU policies:

| Setting | Default | Description |
|---------|---------|-------------|
| `lfu-log-factor` | 10 | Counter growth rate (higher = slower growth) |
| `lfu-decay-time` | 1 | Decay period in minutes |

---

## Eviction and Replication

- **Primary drives all eviction**: Evicted keys generate synthetic `DEL` commands replicated to all replicas.
- **Replicas do NOT evict independently**: Replicas ignore `maxmemory`. All eviction decisions come from the primary.
- **Replicas may use more memory** than the primary (due to replication buffers). Provision replicas with 10-20% extra memory.
- **On replica promotion**: `maxmemory` becomes active. If over limit, eviction begins immediately.

---

## Post-Recovery Eviction Behavior

After server restart:

- **LRU**: All keys appear "fresh" (idle time = 0). Eviction accuracy self-corrects within minutes as keys are accessed.
- **LFU**: Counters are persisted, so accuracy is maintained across restarts. LFU policies provide better accuracy after recovery.

**Recommendation**: For production with eviction enabled, LFU policies (`allkeys-lfu`, `volatile-lfu`) provide better behavior after restarts.

---

## Tiered Storage (Hot/Warm)

When tiered storage is enabled, evicted values are demoted to local SSD (RocksDB) instead of deleted. Values are transparently promoted back to RAM on access.

```
HOT (RAM)                    WARM (Disk/SSD)
+------------------+        +------------------+
| keys + metadata  | demote | demoted values   |
| + hot values     | -----> | (RocksDB CF)     |
+------------------+        +------------------+
        ^                          |
        |         promote          |
        +--------------------------+
```

- Keys and metadata always remain in RAM (~80 bytes per key).
- Only values are demoted to disk.
- EXISTS, SCAN, TTL checks remain fast (metadata is always in RAM).

### Tiered Storage Configuration

```toml
[tiered_storage]
enabled = false

[tiered_storage.hot]
maxmemory = "1gb"              # Hot tier memory limit

[tiered_storage.warm]
column_family = "tiered_warm"
compression = "lz4"           # Heavier compression for warm data

[tiered_storage.policy]
eviction_policy = "tiered-lru"  # or "tiered-lfu"
```

### Latency Characteristics

| Operation | Hot Key | Warm Key |
|-----------|---------|----------|
| GET | < 1 us | ~50-200 us (NVMe read) |
| EXISTS | < 1 us | < 1 us (key always in RAM) |
| TTL | < 1 us | < 1 us (metadata always in RAM) |
| SCAN | < 1 us/key | < 1 us/key (keys always in RAM) |

### Tiered Storage Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_tiered_keys` | Keys in each tier (labels: tier=hot/warm, shard) |
| `frogdb_tiered_promotions_total` | Warm to hot promotion count |
| `frogdb_tiered_demotions_total` | Hot to warm demotion count |
| `frogdb_tiered_warm_read_seconds` | Warm tier read latency |

---

## Memory Monitoring

### Key Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_memory_used_bytes` | Total memory used by data |
| `frogdb_memory_max_bytes` | Configured max memory |
| `frogdb_memory_fragmentation_ratio` | RSS / used memory ratio |
| `frogdb_keys_evicted_total` | Total keys evicted (by policy) |
| `frogdb_eviction_oom_total` | OOM errors despite eviction attempts |

### Memory Fragmentation

| Ratio | Status | Action |
|-------|--------|--------|
| < 1.0 | **Swapping** | Critical -- add RAM immediately |
| 1.0 - 1.4 | Healthy | Normal operation |
| 1.5 - 2.0 | High | Schedule maintenance restart |
| > 2.0 | Critical | Restart soon |

### Alerting

| Alert | Condition | Severity |
|-------|-----------|----------|
| High memory | `used / max > 0.8` | Warning |
| Critical memory | `used / max > 0.95` | Critical |
| High eviction rate | `evicted_keys_rate > 100/s` | Warning |
| High fragmentation | `fragmentation_ratio > 1.5` | Warning |

---

## Configuration Summary

| Setting | Default | Description |
|---------|---------|-------------|
| `maxmemory` | 0 (unlimited) | Memory limit in bytes |
| `maxmemory-policy` | `noeviction` | Eviction policy |
| `maxmemory-samples` | 5 | Keys to sample for LRU/LFU |
| `lfu-log-factor` | 10 | LFU counter growth rate |
| `lfu-decay-time` | 1 | LFU decay period (minutes) |
