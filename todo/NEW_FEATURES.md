# FrogDB New Features & Competitive Analysis

This document captures competitive research and potential new features for FrogDB, organized for the
development team.

## Competitor Landscape

### Primary Competitors

| Database        | Language | License                | Threading Model                    | Production Status          |
| --------------- | -------- | ---------------------- | ---------------------------------- | -------------------------- |
| **Redis**       | C        | AGPLv3                 | Single-threaded core + I/O threads | Mature                     |
| **Valkey**      | C        | BSD 3-clause           | Single-threaded + async I/O        | Growing (Linux Foundation) |
| **DragonflyDB** | C++      | BSL (source-available) | Shared-nothing multi-threaded      | Production                 |
| **Garnet**      | C#/.NET  | MIT                    | Shared-memory multi-threaded       | Early production           |
| **KeyDB**       | C++      | BSD                    | Multi-threaded                     | **Stale (1.5 years)**      |
| **Skytable**    | Rust     | Open source            | Multi-threaded                     | Early stage                |

---

## Major Pain Points in the Ecosystem

### 1. CROSSSLOT Limitations (Critical)

```
CROSSSLOT Keys in request don't hash to the same slot
```

- Multi-key operations (MGET, MSET, DEL) fail across slots
- Lua scripts must be redesigned for cluster
- Transactions (MULTI/EXEC) only work within single slots
- Rate limiting libraries universally broken in cluster mode

---

## Potential New Features

### Feature Priority Summary

| Feature              | Impact    | Effort      | Priority | Notes                       |
| -------------------- | --------- | ----------- | -------- | --------------------------- |
| **Migration System** | Very High | Medium      | 1        | Critical differentiator     |
| **Auto-Rebalancing** | High      | High        | 3        | Requires cluster mode first |
| **Feature Store**    | Medium    | Medium      | 4        | Niche but growing market    |
| **Vector Search**    | Medium    | Medium-High | 5        | Competitive but crowded     |

---

## Feature 1: Tiered Storage (Hot/Warm/Cold)

**The Gap:** All open-source competitors are RAM-only or RAM+snapshot. Redis Enterprise has tiering
but is proprietary.

### Architecture Design

```
┌─────────────────────────────────────────────────────────┐
│  HOT TIER (RAM)                                         │
│  - In-memory HashMap per shard                          │
│  - Sub-millisecond access                               │
│  - Automatic for frequently accessed keys               │
└─────────────────────────────────────────────────────────┘
                           ↕ LRU promotion/demotion
┌─────────────────────────────────────────────────────────┐
│  WARM TIER (Local SSD via RocksDB)                      │
│  - LSM-tree structure                                   │
│  - 1-5ms access latency                                 │
│  - Compressed storage                                   │
└─────────────────────────────────────────────────────────┘
```

### Proposed Commands

```
# Per-namespace tiering policy
TIER.POLICY namespace RAM_RATIO 0.2 COLD_AFTER 7d

# Per-key temperature hints
SET key value TEMP hot|warm|cold

# Force tiering operations
TIER.DEMOTE key      # Move to colder tier
TIER.PROMOTE key     # Bring to hotter tier

# Tiering statistics
TIER.INFO [namespace]  # Show tier distribution, costs
```

### Tiering Algorithm Options

| Algorithm | Description                                 | Best For                     |
| --------- | ------------------------------------------- | ---------------------------- |
| **LRU**   | Evicts based on last access time            | Temporal locality            |
| **LFU**   | Evicts based on access count                | Identifying truly "hot" data |
| **LRFU**  | Weighted combination of recency + frequency | Better overall performance   |
| **LFUDA** | LFU with dynamic aging                      | Handles popularity shifts    |

**Recommended:** Hybrid LRFU + Time-Based

```rust
struct TieringMetrics {
    last_access: Instant,      // For LRU component
    access_count: u32,         // For LFU component
    created_at: Instant,       // For age-based policies
    value_size: u64,           // For cost-weighted decisions
}

fn tier_score(key: &Key) -> f64 {
    let recency = time_since(key.last_access);
    let frequency = key.access_count;
    // LRFU formula with configurable weight
    alpha * recency_score(recency) + (1-alpha) * frequency_score(frequency)
}
```

### Implementation Tasks

- [ ] Extend `KeyMetadata` in `crates/core/src/store.rs` with access tracking
- [ ] Use RocksDB `last_level_temperature` option for warm tier
- [ ] Create `crates/server/src/commands/tier.rs` for new commands
- [ ] Add tiering configuration to TOML config

---

## Feature 2: Auto-Rebalancing

**The Gap:** Redis Cluster rebalances by slot-count only, not by actual load. No open-source
solution has intelligent rebalancing.

### Current State

**Redis Cluster:**
- Goal: Equal number of slots per node
- Does NOT consider: key sizes, access patterns, operation costs
- Result: Uneven load despite "balanced" slots

**CockroachDB (Best-in-Class):**
- Multi-factor evaluation: disk fullness, locality, range count, QPS
- Load-based splitting: hot ranges split based on QPS, not just size
- Priority queue of hottest ranges, updated every 60 seconds

### Multi-Dimensional Weight Design

```rust
struct SlotWeight {
    key_count: u64,           // Number of keys
    memory_bytes: u64,        // Total memory usage
    qps: f64,                 // Queries per second (EWMA)
    write_qps: f64,           // Write-heavy slots need different handling
    avg_latency_us: u64,      // Operation complexity proxy
}

fn compute_weight(slot: &Slot) -> f64 {
    // Weighted sum - configurable
    w1 * normalize(slot.memory_bytes) +
    w2 * normalize(slot.qps) +
    w3 * normalize(slot.write_qps) +
    w4 * normalize(slot.avg_latency_us)
}
```

### Configuration

```toml
[cluster.rebalance]
weight_imbalance_threshold = 0.2  # 20% deviation triggers rebalance
memory_imbalance_threshold = 0.3
qps_imbalance_threshold = 0.25
min_slots_per_move = 10
```

### Proposed Commands

```
CLUSTER WEIGHTS                    # Show per-node weights
CLUSTER SLOTS WEIGHTS              # Show per-slot weights
CLUSTER REBALANCE [DRY-RUN]        # Trigger rebalance
CLUSTER REBALANCE FACTOR qps|memory|keys  # Rebalance by specific factor
```

### Implementation Tasks

- [ ] Add slot-to-shard mapping in `crates/core/src/shard.rs`
- [ ] Extend metrics collection with per-slot metrics
- [ ] Create `crates/core/src/cluster/rebalance.rs` for weight calculation
- [ ] Leverage existing `ScatterOp` for slot migration
- [ ] Extend `crates/server/src/commands/cluster.rs` with new commands

---

## Feature 3: Native Feature Store

**The Gap:** Feast + Redis is bolted-on, not native. Requires multiple round trips for batch
inference.

### How Feast Uses Redis Today

Feast uses Redis as a two-level hash map:
- **Level 1 (Redis Key)**: `project_name + entity_key` (protobuf-serialized)
- **Level 2 (Hash Field)**: `Murmur3_32(table_name:feature_name)` → feature value
- **Timestamps**: Stored under `_ts:table_name` field

### Proposed Commands

```
# Feature View Registration (metadata)
FS.REGISTER view_name entity_name [SCHEMA json] [TTL seconds]
FS.VIEWS [PATTERN pattern]

# Single Entity Feature Retrieval (online serving)
FS.GET entity_key feature1 [feature2 ...] [AS_OF timestamp]

# Batch Feature Retrieval (critical for inference - reduces round trips)
FS.MGET entity_key1 entity_key2 ... FEATURES feature1 [feature2 ...]

# Feature Materialization (write from offline store)
FS.WRITE entity_key feature_view {feature: value, ...} [TIMESTAMP ts] [TTL seconds]

# Point-in-Time Correctness (for training data)
FS.HISTORY entity_key feature_view [FROM ts] [TO ts]

# Feature Freshness Metadata
FS.FRESHNESS feature_view  # Returns last update time, staleness
```

### Key Differentiators from Feast+Redis

1. **Native batch retrieval** - Single command for multi-entity, multi-feature fetches
2. **Built-in point-in-time support** - Critical for avoiding data leakage in training
3. **Feature freshness tracking** - Know when features went stale
4. **Atomic multi-feature writes** - Leverage VLL for consistency

### Data Type Design

```rust
pub struct FeatureSetValue {
    pub entity_key: Bytes,
    pub features: HashMap<String, FeatureValue>,
    pub timestamps: HashMap<String, u64>,  // Per-feature freshness
    pub view_name: String,
}

pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(Bytes),
    Vector(Vec<f32>),
    List(Vec<FeatureValue>),
}
```

### Implementation Tasks

- [ ] Add `FeatureSet` data type in `crates/core/src/types.rs` (TYPE_FEATURESET = 11)
- [ ] Add serialization in `crates/core/src/persistence/serialization.rs`
- [ ] Create `crates/server/src/commands/featurestore.rs`
- [ ] Implement Feast compatibility layer with `FS.FEAST.GET`

---

## Feature 4: Vector Search

**The Gap:** Redis has vector search (RediSearch), but purpose-built vector DBs often outperform at
scale.

### Competitor Comparison

| Database     | Language | Indexing           | Filtering      | Scale          |
| ------------ | -------- | ------------------ | -------------- | -------------- |
| **Milvus**   | Go/C++   | HNSW, IVF, DiskANN | Pre/Post       | Billions       |
| **Qdrant**   | Rust     | Filterable HNSW    | Native         | 100M+          |
| **Weaviate** | Go       | HNSW               | Pre/Post       | ~50M optimal   |
| **Redis**    | C        | HNSW only          | Via RediSearch | Memory-limited |

### Proposed Commands

```
# Index creation
VEC.CREATE index_name DIM 768 METRIC cosine [ALGORITHM hnsw] [M 16] [EF_CONSTRUCTION 200]

# Vector insertion
VEC.ADD index_name key vector [metadata_json]

# Similarity search
VEC.SEARCH index_name vector K 10 [FILTER condition] [EF_RUNTIME 100]

# Batch operations (critical for ML inference)
VEC.MSEARCH index_name vectors... K 10
```

### FrogDB Differentiators

1. **Tiered vector storage** - Hot vectors in RAM, warm on SSD (using RocksDB)
2. **Filterable HNSW** - Learn from Qdrant's approach
3. **Native batch search** - Critical for ML inference
4. **Rust performance** - Qdrant proves Rust is excellent for vector workloads

### Implementation Tasks

- [ ] Add `Vector` data type in `crates/core/src/types.rs` (TYPE_VECTOR = 12)
- [ ] Create `crates/core/src/vector/hnsw.rs` using `hnsw_rs` or `instant-distance` crate
- [ ] Add index persistence to RocksDB
- [ ] Create `crates/server/src/commands/vector.rs`
- [ ] Implement tiered vector storage (RAM + SSD)

---

## Feature 6: Migration System

**The Gap:** No competitor has solved the migration experience. This is the #1 barrier to adoption.

### Architecture

```rust
pub struct ProxyConfig {
    pub upstream_addresses: Vec<String>,
    pub mode: ProxyMode,
}

pub enum ProxyMode {
    Forward,     // All commands go to upstream
    DualWrite,   // Write to both, read from FrogDB
    Shadow,      // Write to both, compare results
}
```

### Proposed Commands

```
MIGRATE.START source_url [DUAL_WRITE]
MIGRATE.STATUS
MIGRATE.ANALYZE              # Detect CROSSSLOT usage, unsupported commands
MIGRATE.BACKFILL [COUNT n]   # SCAN + DUMP/RESTORE for bulk transfer
MIGRATE.SWITCHOVER           # Cutover to FrogDB
```

### Migration Issue Detection

```rust
pub struct MigrationIssue {
    pub command: String,
    pub severity: Severity,  // Warning, Error, Critical
    pub description: String,
    pub recommendation: String,
}
```

Detects: CROSSSLOT usage, unsupported commands, Lua scripts with issues.

### Implementation Tasks

- [ ] Create `crates/server/src/proxy.rs` for proxy mode
- [ ] Create `crates/server/src/migration/analyzer.rs` for issue detection
- [ ] Create `crates/server/src/migration/backfill.rs` for bulk transfer
- [ ] Implement MIGRATE commands

---

## References

### Competitor Analysis
- [Valkey vs Redis Comparison 2025](https://www.dragonflydb.io/guides/valkey-vs-redis)
- [Redis 8.0 vs Valkey 8.1 Technical
  Comparison](https://www.dragonflydb.io/blog/redis-8-0-vs-valkey-8-1-a-technical-comparison)
- [DragonflyDB Known
  Limitations](https://www.dragonflydb.io/docs/managing-dragonfly/known-limitations)
- [Microsoft Garnet
  Introduction](https://www.microsoft.com/en-us/research/blog/introducing-garnet-an-open-source-next-generation-faster-cache-store-for-accelerating-applications-and-services/)
- [Redis Cluster Limitations](https://www.dragonflydb.io/faq/limitations-of-redis-cluster)
- [Redis Cluster Slot
  Migration](https://severalnines.com/blog/hash-slot-resharding-and-rebalancing-redis-cluster/)

### Event Sourcing
- [Why Kafka is Not Ideal for Event
  Sourcing](https://dcassisi.com/2023/05/06/why-is-kafka-not-ideal-for-event-sourcing/)
- [Event Sourcing Database
  Architecture](https://www.redpanda.com/guides/event-stream-processing-event-sourcing-database)
- [EloqKV - ACID Redis Alternative](https://github.com/eloqdata/eloqkv)

### ML Feature Stores
- [Feature Stores for Real-time
  AI/ML](https://redis.io/blog/feature-stores-for-real-time-artificial-intelligence-and-machine-learning/)
- [Feast with Redis
  Tutorial](https://redis.io/blog/building-feature-stores-with-redis-introduction-to-feast-with-redis/)
- [Feast Online Store Format
  Spec](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md)
- [Adding a New Online Store to
  Feast](https://docs.feast.dev/how-to-guides/customizing-feast/adding-support-for-a-new-online-store)
- [Lyft Feature Store
  Architecture](https://eng.lyft.com/lyfts-feature-store-architecture-optimization-and-evolution-7835f8962b99)

### Tiered Storage
- [Redis Auto Tiering](https://redis.io/auto-tiering/)
- [AWS ElastiCache Data
  Tiering](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/data-tiering.html)
- [RocksDB Tiered Storage
  (Experimental)](https://github.com/facebook/rocksdb/wiki/Tiered-Storage-(Experimental))
- [RocksDB-Cloud for S3](https://github.com/rockset/rocksdb-cloud)
- [The Shift to S3 - Database
  Architecture](https://wesql.io/blog/every-database-will-be-rearchitectured-to-use-s3)

### Tiering Algorithms
- [LFU vs LRU -
  Redis](https://redis.io/blog/lfu-vs-lru-how-to-choose-the-right-cache-eviction-policy/)
- [LRFU Algorithm Paper](https://dl.acm.org/doi/10.1109/TC.2001.970573)
- [TinyLFU Paper](https://arxiv.org/pdf/1512.00727)

### Auto-Rebalancing
- [CockroachDB Rebalancing Tech
  Notes](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/rebalancing.md)
- [CockroachDB Automated
  Rebalance](https://www.cockroachlabs.com/blog/automated-rebalance-and-repair/)
- [Redis Cluster Slot Metrics Proposal](https://github.com/redis/redis/issues/10472)

### Vector Search
- [Redis Vector Search Concepts](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/)
- [Qdrant Benchmarks](https://qdrant.tech/benchmarks/)
- [Reddit Vector DB Comparison (Milvus vs
  Qdrant)](https://milvus.io/blog/choosing-a-vector-database-for-ann-search-at-reddit.md)
