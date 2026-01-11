# FrogDB Key Eviction

> **Status: PLANNED** - This document describes the future eviction system design. Current behavior is `noeviction` only (OOM rejects writes).

This document covers key eviction policies, algorithms, and memory threshold handling.

## Overview

When `max_memory` is exceeded, FrogDB must decide how to handle new writes:

- **Default behavior**: `noeviction` - Return OOM error, reject writes
- **With eviction policy**: Automatically remove keys to make room

---

## Supported Policies

| Policy | Scope | Description |
|--------|-------|-------------|
| noeviction | - | Return OOM error (default) |
| volatile-lru | Keys with TTL | Evict least recently used |
| allkeys-lru | All keys | Evict least recently used |
| volatile-lfu | Keys with TTL | Evict least frequently used |
| allkeys-lfu | All keys | Evict least frequently used |
| volatile-random | Keys with TTL | Evict random keys |
| allkeys-random | All keys | Evict random keys |
| volatile-ttl | Keys with TTL | Evict keys with shortest TTL |

---

## LRU Implementation

Redis-style approximate LRU using sampling:

```rust
struct EvictionPool {
    candidates: [Option<EvictionCandidate>; 16],
}

struct EvictionCandidate {
    key: Bytes,
    idle_time: u64,  // Time since last access
}

fn evict_lru(shard: &mut Shard, samples: usize) -> Option<Bytes> {
    // Sample random keys
    for _ in 0..samples {
        let key = shard.random_key();
        let idle = shard.idle_time(&key);
        // Insert into eviction pool if worse than current
        pool.maybe_insert(key, idle);
    }
    // Evict worst candidate
    pool.pop_worst()
}
```

**Configuration:**
- `maxmemory-samples`: Keys to sample (default: 5, more = more accurate)

---

## LFU Implementation

Logarithmic counter with decay:

```rust
// 8-bit logarithmic counter stored in key metadata
fn lfu_log_incr(counter: u8) -> u8 {
    if counter == 255 { return 255; }
    let r: f64 = random();
    let p = 1.0 / ((counter as f64) * LFU_LOG_FACTOR + 1.0);
    if r < p { counter + 1 } else { counter }
}

fn lfu_decay(counter: u8, minutes_since_access: u64) -> u8 {
    let decay = minutes_since_access / DECAY_FACTOR;
    counter.saturating_sub(decay as u8)
}
```

**Configuration:**
- `lfu-log-factor`: Counter growth rate (default: 10)
- `lfu-decay-time`: Decay period in minutes (default: 1)

---

## DragonflyDB LFRU (Future)

Hybrid LFU + LRU with zero per-key overhead:

```
LFRU Score = α * LFU_Score + (1-α) * LRU_Score
```

- Tracks both recency and frequency
- Adapts to traffic pattern changes
- No additional metadata per key

---

## Memory Thresholds

Eviction is triggered on write operations when memory exceeds `max_memory`:

```rust
fn check_memory_on_write(shard: &mut Shard) -> Result<(), Error> {
    let used = shard.memory_used();
    let max = config.max_memory;

    if max == 0 || used < max {
        return Ok(());
    }

    match config.maxmemory_policy {
        Policy::NoEviction => Err(Error::Oom),
        policy => {
            // Try to free memory
            let freed = shard.evict(policy, used - max);
            if freed < (used - max) {
                Err(Error::Oom)
            } else {
                Ok(())
            }
        }
    }
}
```

---

## Per-Shard Memory Behavior

FrogDB tracks memory per-shard, not globally. This has implications for multi-key operations:

### Memory Distribution

```
Total max_memory: 10GB
Shards: 4
Per-shard limit: ~2.5GB (approximate, not strictly enforced)
```

**Note:** Memory tracking is approximate per-shard. Shards may vary in actual usage.

### Multi-Key Operation OOM

For operations like MSET that touch multiple shards:

| Scenario | Behavior |
|----------|----------|
| All shards have space | Operation succeeds |
| One shard at limit | Entire operation fails with OOM |
| Partial failure (pre-VLL) | Not possible - VLL ensures atomic check |

**VLL Guarantee:** With VLL transaction ordering, multi-key writes pre-check memory on all target shards before execution. Either all keys are written, or the entire operation returns `-OOM`.

### Configuration

Memory is configured globally but distributed across shards:

```toml
[memory]
max_memory = "10gb"              # Total memory limit
max_memory_policy = "noeviction" # or eviction policy
```

**Recommendation:** When using per-shard eviction, ensure sufficient headroom for write bursts to avoid frequent cross-shard OOM failures.

---

## Configuration Summary

| Setting | Default | Description |
|---------|---------|-------------|
| `maxmemory` | 0 (unlimited) | Memory limit in bytes |
| `maxmemory-policy` | noeviction | Eviction policy |
| `maxmemory-samples` | 5 | Keys to sample for LRU/LFU |
| `lfu-log-factor` | 10 | LFU counter growth rate |
| `lfu-decay-time` | 1 | LFU decay period (minutes) |

See [CONFIGURATION.md](CONFIGURATION.md) for configuration details.

---

## Post-Recovery Eviction Behavior

After server restart or recovery from persistence, eviction metadata has specific characteristics:

### LRU After Recovery

**All keys appear "fresh" (idle time = 0)** immediately after recovery because `last_access` timestamps
are NOT persisted. This matches Redis behavior ([GitHub Issue #1261](https://github.com/redis/redis/issues/1261)).

**Impact:**
- `volatile-lru` and `allkeys-lru` policies will evict essentially at random immediately after restart
- Eviction accuracy **self-corrects within minutes** as keys are accessed during normal operation
- The more traffic the server receives, the faster accuracy restores

**Workaround for migration tools:** The `RESTORE` command supports `IDLETIME seconds` modifier
to explicitly set idle time during data migration.

### LFU After Recovery

**LFU counters ARE persisted** with each key, so `volatile-lfu` and `allkeys-lfu` policies
maintain accuracy across restarts. Keys with historically high access counts will have
appropriately high `lfu_counter` values after recovery.

**Note:** LFU decay is applied lazily on access, so counters may be stale until keys are first accessed after recovery.

### Recommendation

For production deployments with eviction enabled:
- **LFU policies** (`volatile-lfu`, `allkeys-lfu`) provide better accuracy after recovery
- **LRU policies** require warm-up period; consider a brief grace period before enabling strict memory limits
- Monitor `frogdb_eviction_keys_total` metric to track eviction behavior
