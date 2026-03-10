# FrogDB Key Eviction

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

### Industry Comparison

| System | Algorithm | Per-Key Overhead | Approach |
|--------|-----------|------------------|----------|
| **Redis/Valkey** | Sampling LRU | ~24 bits (timestamp) | Sample N keys, evict oldest |
| **DragonflyDB** | 2Q LFRU | Zero | Probationary→Protected buffers, integrated with Dashtable |
| **FrogDB** | Sampling LRU | ~24 bits (timestamp) | Redis-compatible approach |

**Why FrogDB uses Redis-style sampling:**
- Proven algorithm with well-understood accuracy characteristics
- Simple implementation without custom data structures
- ~95% accuracy with 5 samples, ~98% with 10 samples
- See [POTENTIAL.md](../todo/POTENTIAL.md) for DragonflyDB's 2Q approach (requires Dashtable)

### Algorithm Details

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

### Eviction Pool Parameters

The eviction pool improves sampling efficiency by maintaining candidates across eviction rounds:

**Pool Size:**

```rust
/// Fixed-size pool of eviction candidates
/// Size 16 provides good accuracy without significant memory overhead
const EVICTION_POOL_SIZE: usize = 16;

struct EvictionPool {
    /// Candidates sorted by idle time (worst first)
    candidates: [Option<EvictionCandidate>; EVICTION_POOL_SIZE],

    /// Number of valid candidates currently in pool
    count: usize,
}
```

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Pool size | 16 | Matches Redis; larger pools have diminishing returns |
| Pool scope | Per-shard | No cross-shard coordination for eviction |
| Candidate lifetime | Until evicted or key deleted | Pool persists across eviction rounds |

**Sample Selection Strategy:**

```rust
impl Shard {
    /// Select a random key for eviction sampling
    fn random_key(&self) -> Option<Bytes> {
        // Strategy: Random bucket, then random entry in bucket
        //
        // 1. Select random bucket from hash table
        let bucket_idx = self.rng.gen_range(0..self.table.bucket_count());
        let bucket = &self.table.buckets[bucket_idx];

        // 2. If bucket empty, try next few buckets (avoid full rescan)
        // Linear probe up to 5 adjacent buckets
        for offset in 0..5 {
            let try_idx = (bucket_idx + offset) % self.table.bucket_count();
            if let Some(entry) = self.table.buckets[try_idx].random_entry(&mut self.rng) {
                return Some(entry.key.clone());
            }
        }

        // 3. If still empty, fall back to iterator (rare, mostly empty shard)
        self.table.iter().next().map(|(k, _)| k.clone())
    }
}
```

**Why random bucket selection:**
- O(1) average case (no iteration needed)
- Uniform distribution across keyspace
- Handles sparse tables gracefully with linear probe

**Pool Insertion and Eviction:**

```rust
impl EvictionPool {
    /// Insert candidate if it qualifies for eviction
    fn maybe_insert(&mut self, key: Bytes, idle_time: u64) {
        // Find insertion position (sorted by idle_time descending)
        // Pool keeps worst candidates (highest idle time)

        // Don't insert if better than all current candidates and pool is full
        if self.count == EVICTION_POOL_SIZE {
            if let Some(best) = self.candidates[self.count - 1].as_ref() {
                if idle_time <= best.idle_time {
                    return; // This key is not worse than current worst
                }
            }
        }

        // Check for duplicates (same key already in pool)
        for candidate in self.candidates.iter().flatten() {
            if candidate.key == key {
                return; // Key already tracked
            }
        }

        // Binary search for insertion position
        let pos = self.candidates[..self.count]
            .binary_search_by(|c| {
                c.as_ref()
                    .map(|c| idle_time.cmp(&c.idle_time))
                    .unwrap_or(std::cmp::Ordering::Less)
            })
            .unwrap_or_else(|e| e);

        // Shift elements to make room
        if self.count < EVICTION_POOL_SIZE {
            self.count += 1;
        }
        for i in (pos + 1..self.count).rev() {
            self.candidates[i] = self.candidates[i - 1].take();
        }

        // Insert new candidate
        self.candidates[pos] = Some(EvictionCandidate { key, idle_time });
    }

    /// Remove and return the worst candidate (highest idle time)
    fn pop_worst(&mut self) -> Option<Bytes> {
        if self.count == 0 {
            return None;
        }

        let candidate = self.candidates[0].take()?;

        // Shift remaining candidates
        for i in 0..self.count - 1 {
            self.candidates[i] = self.candidates[i + 1].take();
        }
        self.count -= 1;

        Some(candidate.key)
    }

    /// Remove key from pool if present (called when key is deleted externally)
    fn remove(&mut self, key: &Bytes) {
        if let Some(pos) = self.candidates.iter().position(|c| {
            c.as_ref().map_or(false, |c| &c.key == key)
        }) {
            for i in pos..self.count - 1 {
                self.candidates[i] = self.candidates[i + 1].take();
            }
            self.count -= 1;
        }
    }
}
```

**Pool Refresh Behavior:**

| Event | Pool Behavior |
|-------|---------------|
| Key deleted (DEL, UNLINK) | Remove from pool if present |
| Key expired (TTL) | Remove from pool if present |
| Key accessed (GET, SET) | Pool NOT updated (lazy) |
| Eviction triggered | Sample more keys, add to pool, evict worst |
| Server restart | Pool reset (empty) |

### maxmemory-samples Effects

The `maxmemory-samples` setting controls eviction accuracy vs. CPU cost:

| Setting | Accuracy | CPU Cost | Use Case |
|---------|----------|----------|----------|
| 1 | ~50% | Lowest | Testing only |
| 3 | ~85% | Very low | High-throughput, tolerates eviction variance |
| 5 (default) | ~93% | Low | General purpose |
| 10 | ~98% | Medium | Latency-sensitive, predictable eviction |
| 20 | ~99%+ | Higher | Rarely needed, diminishing returns |

**Accuracy measurement:** Percentage of time the actual LRU key is evicted (vs. true LRU).

```
Eviction Accuracy vs. Sample Size:

Accuracy
   ▲
99%├───────────────────────────────●───●
98%├─────────────────────────●
95%├──────────────────●
90%├────────────●
85%├───────●
80%├───●
   │
   └──┼──┼──┼──┼──┼──┼──┼──┼──┼──┼──▶ Samples
      1  2  3  4  5  6  7  8  9  10
```

**Recommendation:**
- Default (5) is appropriate for most workloads
- Increase to 10 if eviction "wrong key" is causing issues
- Never set below 3 in production

### Per-Shard vs. Global Eviction

FrogDB performs eviction at the **shard level**, not globally:

```
Per-Shard Eviction Model:

┌─────────────────────────────────────────────────────────────────┐
│                       Global Memory Check                        │
│              (total_used > maxmemory triggers eviction)         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────────┐
              │             │                 │
              ▼             ▼                 ▼
        ┌──────────┐  ┌──────────┐      ┌──────────┐
        │ Shard 0  │  │ Shard 1  │ ...  │ Shard N  │
        │          │  │          │      │          │
        │ [Pool 0] │  │ [Pool 1] │      │ [Pool N] │
        │   ↓      │  │   ↓      │      │   ↓      │
        │ Evict    │  │ Evict    │      │ Evict    │
        └──────────┘  └──────────┘      └──────────┘
              │             │                 │
              └─────────────┼─────────────────┘
                            ▼
                    Memory Reclaimed
```

**Eviction Decision Flow:**

```rust
fn trigger_eviction(config: &Config, shards: &[Shard]) -> Result<(), OomError> {
    let total_used: usize = shards.iter().map(|s| s.memory_used()).sum();

    if total_used <= config.maxmemory {
        return Ok(()); // No eviction needed
    }

    let to_free = total_used - config.maxmemory;
    let mut freed = 0usize;

    // Eviction strategy: Round-robin across shards
    // Each shard evicts one key per round until target reached
    let mut round = 0;
    while freed < to_free {
        let mut made_progress = false;

        for shard in shards.iter_mut() {
            if let Some(key_size) = shard.evict_one(config.maxmemory_policy) {
                freed += key_size;
                made_progress = true;

                if freed >= to_free {
                    break;
                }
            }
        }

        // Prevent infinite loop if no keys can be evicted
        if !made_progress {
            return Err(OomError::CannotEvict);
        }

        round += 1;
        if round > MAX_EVICTION_ROUNDS {
            return Err(OomError::EvictionTimeout);
        }
    }

    Ok(())
}
```

**Why per-shard eviction:**

| Aspect | Per-Shard | Global |
|--------|-----------|--------|
| Lock contention | None (shard-local) | Requires global lock |
| LRU accuracy | Per-shard accurate | Would need global time sync |
| Hot shard handling | May evict cold keys from hot shard | Better at balancing |
| Implementation | Simple | Complex coordination |

**Imbalanced Shard Scenario:**

When shards have unequal memory usage:

```
Shard 0: 3GB (hot)    → Has most keys, evicts most
Shard 1: 1GB (warm)   → Evicts proportionally less
Shard 2: 0.5GB (cold) → May have no evictable keys
```

Round-robin eviction naturally balances by attempting eviction from all shards, but shards with no evictable keys (all volatile-* policies with no TTL keys) are skipped.

### Eviction Metrics

| Metric | Description |
|--------|-------------|
| `frogdb_eviction_keys_total` | Total keys evicted (by policy, shard) |
| `frogdb_eviction_bytes_total` | Total memory freed by eviction |
| `frogdb_eviction_pool_size` | Current candidates in eviction pool (per shard) |
| `frogdb_eviction_samples_total` | Total keys sampled for eviction |
| `frogdb_eviction_oom_total` | OOM errors despite eviction attempts |
| `frogdb_eviction_latency_ms` | Time spent in eviction (histogram) |

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

See [POTENTIAL.md](../todo/POTENTIAL.md) for DragonflyDB's 2Q LFRU algorithm notes (requires Dashtable).

---

## Memory Thresholds

### Global Memory Limit

`maxmemory` applies to the **TOTAL memory across all internal shards**, not per-shard:

```
Total used = Σ(shard[i].used_memory) for i in 0..num_shards
OOM condition: Total used > maxmemory
```

### Enforcement Mechanism

1. **Periodic aggregation:** Background task sums shard memory every 100ms
2. **Write-time check:** Before each write, check global counter
3. **Eviction trigger:** If over limit, trigger eviction on heaviest shard(s)

```rust
fn check_memory_on_write(shards: &[Shard]) -> Result<(), Error> {
    // Global memory check across all shards
    let total_used: usize = shards.iter().map(|s| s.memory_used()).sum();
    let max = config.max_memory;

    if max == 0 || total_used < max {
        return Ok(());
    }

    match config.maxmemory_policy {
        Policy::NoEviction => Err(Error::Oom),
        policy => {
            // Evict from heaviest shards first
            let to_free = total_used - max;
            trigger_eviction(shards, policy, to_free)
        }
    }
}
```

### Per-Shard vs Global Metrics

| Metric | Scope | Use |
|--------|-------|-----|
| `used_memory` | Global (sum) | OOM decisions, INFO memory |
| `shard_used_memory` | Per-shard | Eviction targeting |
| `maxmemory` | Global config | Total limit |

### Imbalanced Shards

If one shard holds 80% of data and another holds 20%:
- Global limit still enforced on the sum
- Eviction targets the heavy shard first
- Keys evicted until global total < maxmemory

```
Example: maxmemory = 10GB
  Shard 0: 8GB (80%)  → Primary eviction target
  Shard 1: 2GB (20%)  → Eviction only if Shard 0 exhausted

Total: 10GB → at limit → evict from Shard 0
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

**[VLL](VLL.md) Guarantee:** With VLL transaction ordering, multi-key writes pre-check memory on all target shards before execution. Either all keys are written, or the entire operation returns `-OOM`.

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

## Eviction and Replication

This section specifies how eviction interacts with primary-replica replication.

### Primary Behavior

When the primary evicts a key due to memory pressure:
1. Key is removed from local storage
2. Synthetic `DEL key` command is generated and replicated to all replicas
3. Eviction counts toward replication backlog

```rust
fn evict_key(key: &Bytes, replication: &mut ReplicationStream) {
    // 1. Remove from local storage
    self.store.delete(key);

    // 2. Generate DEL for replication
    let del_cmd = Command::Del { keys: vec![key.clone()] };
    replication.append(del_cmd);

    // 3. Update metrics
    metrics.eviction_keys_total.inc();
    metrics.eviction_bytes_total.inc_by(key_size);
}
```

### Replica Behavior

**Replicas do NOT evict independently:**
- Replicas **ignore** the `maxmemory` setting
- All eviction decisions come from primary via replicated `DEL` commands
- Replicas may use MORE memory than primary (due to replication buffers, copy-on-write overhead)

```
# Primary memory (at limit, evicting)
used_memory: 10737418240  (10GB)
maxmemory:   10737418240  (10GB)

# Replica memory (may exceed primary)
used_memory: 11811160064  (11GB - 10% higher)
maxmemory:   10737418240  (ignored on replica)
```

### Why Replicas Don't Evict

If replicas evicted independently:
1. **Inconsistency:** Replica could have different keys than primary
2. **Clock skew:** Different eviction timing due to clock drift
3. **Read anomalies:** Client reads different data depending on which replica

By having the primary drive all eviction decisions:
- Primary and replicas stay consistent
- Eviction is deterministic and replayable
- Failover preserves expected data

### Memory Monitoring for Replicas

Since replicas don't respect `maxmemory`, operators must:
1. **Provision replicas with extra memory** (10-20% more than primary)
2. **Monitor replica memory usage** separately
3. **Alert on replica memory approaching system limits**

```
# Recommended alerting thresholds
replica_memory_warning:  80% of system RAM
replica_memory_critical: 90% of system RAM
```

### Replica Promotion and Eviction

When a replica is promoted to primary:
1. `maxmemory` setting becomes active
2. If over limit, eviction begins immediately
3. New replicas receive eviction via replication

```
Replica (11GB used, maxmemory ignored)
           │
           ▼  [FAILOVER - promoted to primary]
           │
Primary (11GB used, maxmemory=10GB active)
           │
           ▼  [Eviction triggered - 1GB over limit]
           │
Primary (10GB used, eviction DELs sent to new replicas)
```

### References

This behavior matches Redis: [Redis Replication Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)

> "By default, a replica will ignore maxmemory. This means that the eviction of keys will be handled by the master, sending the DEL commands to the replica as keys evict in the master side."

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

**RESTORE Command LFU Handling:**
- `RESTORE key ... FREQ count` sets explicit LFU counter
- `RESTORE key ...` (without FREQ) uses default counter = 5 (same as newly created keys)
- `RESTORE key ... IDLETIME seconds` affects LRU timestamp, not LFU counter

### Recommendation

For production deployments with eviction enabled:
- **LFU policies** (`volatile-lfu`, `allkeys-lfu`) provide better accuracy after recovery
- **LRU policies** require warm-up period; consider a brief grace period before enabling strict memory limits
- Monitor `frogdb_eviction_keys_total` metric to track eviction behavior
