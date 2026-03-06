# FrogDB Design References

This document contains external references used during FrogDB's design process. These sources document how Redis, DragonflyDB, and other systems handle similar problems.

---

## DragonflyDB

| Topic | URL | Notes |
|-------|-----|-------|
| Transactions & VLL | https://www.dragonflydb.io/blog/transactions-in-dragonfly | VLL algorithm, multi-key atomicity |
| Snapshotting | https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting | Epoch-based forkless snapshots |
| Cluster Mode | https://www.dragonflydb.io/docs/managing-dragonfly/cluster-mode | Orchestrated topology, slot migration |
| Configuration Flags | https://www.dragonflydb.io/docs/managing-dragonfly/flags | Admin port, runtime options |
| Lua Scripting | https://www.dragonflydb.io/docs/managing-dragonfly/scripting | Key validation, atomicity |
| Lua Blog | https://www.dragonflydb.io/blog/leveraging-power-of-lua-scripting | Atomic script execution |
| Threading Model | https://www.dragonflydb.io/blog/redis-analysis-part-1-threading-model | Message bus, IO threads |
| Replication | https://www.dragonflydb.io/blog/replication-for-high-availability | HA replication design |
| Operator Auth | https://www.dragonflydb.io/docs/managing-dragonfly/operator/authentication | Admin access patterns |

---

## Redis

| Topic | URL | Notes |
|-------|-----|-------|
| Replication | https://redis.io/docs/latest/operate/oss_and_stack/management/replication/ | Primary-replica sync, expiry handling |
| Cluster Specification | https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ | Slot distribution, MOVED/ASK |
| Key Eviction | https://redis.io/docs/latest/develop/reference/eviction/ | LRU/LFU algorithms |
| Persistence | https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ | RDB snapshots, AOF |
| RESP Protocol | https://redis.io/docs/latest/develop/reference/protocol-spec/ | RESP2/RESP3 wire format |
| Transactions | https://redis.io/docs/latest/develop/using-commands/transactions/ | WATCH, MULTI/EXEC |
| Scripting | https://redis.io/docs/latest/develop/programmability/eval-intro/ | Lua evaluation |
| Pub/Sub | https://redis.io/docs/latest/develop/pubsub/ | Channel messaging |
| BLPOP | https://redis.io/docs/latest/commands/blpop/ | Blocking list operations |
| Client Handling | https://redis.io/docs/latest/develop/reference/clients/ | Connection management |
| Security | https://redis.io/docs/latest/operate/oss_and_stack/management/security/ | ACL, authentication |
| Sentinel | https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/ | HA management |
| Latency | https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/ | Performance metrics |

---

## Valkey

| Topic | URL | Notes |
|-------|-----|-------|
| Replication | https://valkey.io/topics/replication/ | PSYNC protocol |
| Security | https://valkey.io/topics/security/ | ACL system |
| LRU Cache | https://valkey.io/topics/lru-cache/ | Eviction behavior |
| Cluster Spec | https://valkey.io/topics/cluster-spec/ | Hash slots |
| Atomic Slot Migration | https://valkey.io/blog/atomic-slot-migration/ | Cluster atomicity improvements |
| FAILOVER Command | https://valkey.io/commands/failover/ | Graceful failover |

---

## Redis Known Issues & PRs

| Topic | URL | Notes |
|-------|-----|-------|
| BLPOP + Slot Migration | https://github.com/redis/redis/issues/2379 | Blocked clients during migration |
| Replica Eviction KB | https://redis.io/kb/doc/1oc8qbinn7/do-replicas-apply-eviction-policies-if-maxmemory-is-set | Replicas ignore maxmemory |
| Atomic Slot Migration | https://github.com/redis/redis/issues/10933 | Proposed migration improvements |
| LRU Timestamps RDB | https://github.com/redis/redis/issues/1261 | Not persisted in RDB |
| WATCH Expiry Bug | https://github.com/redis/redis/issues/7918 | Fixed in 6.0.9 |
| Sharded Pub/Sub PR | https://github.com/redis/redis/pull/8621 | Channel slot hashing |
| Monotonic Clock PR | https://github.com/redis/redis/pull/7644 | rdtsc/cntvcnt usage |
| Cascading Replication | https://github.com/redis/redis/issues/10977 | Chain replication issues |
| PSYNC2 Blog | https://antirez.com/news/115 | Replication improvements |
| Sync Replication Blog | https://antirez.com/news/58 | WAIT command design |
| No Rollback Blog | https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/ | Design philosophy |

---

## RocksDB

| Topic | URL | Notes |
|-------|-----|-------|
| Known Issues | https://github.com/facebook/rocksdb/wiki/Known-Issues | Column family behavior |
| Crash Recovery | https://rocksdb.org/blog/2022/10/05/lost-buffered-write-recovery.html | WAL replay guarantees |

---

## Academic Papers & Theory

| Topic | URL | Notes |
|-------|-----|-------|
| VLL Paper | https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf | Original VLL algorithm |
| VLL (VLDB) | https://www.vldb.org/pvldb/vol6/p901-ren.pdf | Lock manager redesign |
| Serializability & Linearizability | https://aphyr.com/posts/333-serializability-linearizability-and-locality | Consistency model definitions |
| CAP Theorem Critique | https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html | Nuanced consistency discussion |

---

## Testing & Verification Tools

| Tool | URL | Notes |
|------|-----|-------|
| Jepsen | https://jepsen.io/ | Distributed systems verification |
| Maelstrom | https://github.com/jepsen-io/maelstrom | Jepsen learning workbench |
| Loom | https://github.com/tokio-rs/loom | Concurrency permutation testing |
| Shuttle | https://github.com/awslabs/shuttle | Randomized concurrency testing |
| MadSim | https://github.com/madsim-rs/madsim | Deterministic simulation for Rust |
| FoundationDB Testing | https://apple.github.io/foundationdb/testing.html | Simulation testing approach |
| TigerBeetle Safety | https://docs.tigerbeetle.com/concepts/safety/ | Safety-first design |
| Antithesis DST | https://antithesis.com/resources/deterministic_simulation_testing/ | Deterministic testing |

---

## Cloud Provider Documentation

| Provider | URL | Notes |
|----------|-----|-------|
| AWS ElastiCache | https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Replication.Redis.Groups.html | Managed Redis replication |
| AWS ElastiCache OOM | https://repost.aws/knowledge-center/oom-command-not-allowed-redis | OOM error handling |
| AWS CROSSSLOT | https://repost.aws/knowledge-center/elasticache-crossslot-keys-error-redis | Cluster key routing |
| Google Memorystore | https://cloud.google.com/memorystore/docs/redis/about-read-replicas | Read replica design |

---

## Monitoring & Operations

| Topic | URL | Notes |
|-------|-----|-------|
| Redis Metrics | https://www.datadoghq.com/blog/how-to-monitor-redis-performance-metrics/ | Fragmentation ratio |
| Redis Lag Metrics | https://last9.io/blog/redis-metrics-monitoring/ | Replication lag |
| Redis COW Memory | https://engineering.zalando.com/posts/2019/05/understanding-redis-background-memory-usage.html | Fork-based COW |
| Lua Timeout | https://scalegrid.io/blog/redis-transactions-long-running-lua-scripts/ | Script timeout behavior |

---

## How to Use These References

When implementing a feature or debugging behavior:

1. **Check the relevant section** in this document for prior art
2. **Read the source material** to understand trade-offs
3. **Document deviations** from Redis/DragonflyDB behavior in the relevant spec file
4. **Update this file** when adding new external references

---

## FrogDB-Specific Design Decisions

Where FrogDB intentionally differs from Redis or DragonflyDB:

| Feature | Redis Behavior | FrogDB Behavior | Rationale |
|---------|----------------|-----------------|-----------|
| Cluster topology | Gossip protocol | External orchestrator | Simpler, deterministic |
| BLPOP + migration | Blocks forever (bug) | Returns -MOVED | Predictable client behavior |
| Replica expiry | Waits for DEL from primary | Local expiry + receives DEL | Lower replica memory |
| Cross-shard MULTI | N/A (single-threaded) | VLL coordination | Multi-core utilization |
