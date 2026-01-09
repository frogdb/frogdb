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

## Configuration Summary

| Setting | Default | Description |
|---------|---------|-------------|
| `maxmemory` | 0 (unlimited) | Memory limit in bytes |
| `maxmemory-policy` | noeviction | Eviction policy |
| `maxmemory-samples` | 5 | Keys to sample for LRU/LFU |
| `lfu-log-factor` | 10 | LFU counter growth rate |
| `lfu-decay-time` | 1 | LFU decay period (minutes) |

See [OPERATIONS.md](OPERATIONS.md) for configuration details.
