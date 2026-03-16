# FrogDB Redis Compatibility

This document describes how FrogDB differs from Redis and what to consider when migrating from Redis or using existing Redis client libraries.

## Compatibility Goals

FrogDB aims to be **wire-protocol compatible** with Redis v6+ via RESP2 and RESP3. Most Redis clients should work without modification. However, FrogDB makes intentional design decisions that differ from Redis, and some features are not yet implemented.

This document is organized into:
1. **Intentional Incompatibilities** - Permanent design decisions
2. **Not Yet Implemented** - Features not currently available

---

## Intentional Incompatibilities

These are deliberate design choices that will not change to match Redis behavior.

### Database Model

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Database selection | SELECT 0-15 | Not supported | Use separate instances for logical separation |
| Cross-slot multi-key ops | Allowed (standalone) | Always rejected | Use hash tags `{tag}` to colocate keys |

**SELECT Command**

FrogDB does not support multiple databases per instance. The `SELECT` command is not implemented. Each FrogDB instance is a single logical database.

```
> SELECT 1
-ERR SELECT is not supported. FrogDB uses a single database per instance.
```

**Workaround:** Run separate FrogDB instances for logical separation, or use key prefixes.

**Cross-Slot Operations**

FrogDB rejects multi-key operations (MGET, MSET, DEL, EXISTS, etc.) when keys hash to different slots, even in standalone mode:

```
> MSET foo 1 bar 2
-CROSSSLOT Keys in request don't hash to the same slot
```

**Rationale:** This provides consistent behavior whether running standalone or clustered, avoiding surprises when scaling.

**Workaround:** Use hash tags to colocate related keys:
```
> MSET {user:1}name Alice {user:1}email alice@example.com
OK
```

---

### Configuration

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Config format | redis.conf | TOML | Different syntax |
| CONFIG REWRITE | Persists changes | Not supported | Restart required for permanent changes |
| Environment variables | Limited | Native FROGDB_ prefix | Different naming |

**Configuration Format**

FrogDB uses TOML configuration files instead of redis.conf format:

```toml
# frogdb.toml
[server]
bind = "0.0.0.0"
port = 6379

[persistence]
durability_mode = { periodic = { interval_ms = 100 } }
```

**CONFIG REWRITE**

The `CONFIG REWRITE` command is not supported. Changes made via `CONFIG SET` are transient and lost on restart.

```
> CONFIG SET maxclients 1000
OK
> CONFIG REWRITE
-ERR CONFIG REWRITE is not supported
```

**Workaround:** Edit `frogdb.toml` directly and restart the server.

**Environment Variables**

FrogDB uses the `FROGDB_` prefix with double-underscore nesting:
```bash
FROGDB_SERVER__PORT=6380
FROGDB_PERSISTENCE__DURABILITY_MODE=sync
```

---

### Persistence

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Snapshot mechanism | Fork-based (RDB) | Forkless epoch-based | Different point-in-time semantics |
| LRU/LFU metadata | Persisted | Not persisted | Keys appear "fresh" after restart |

**Forkless Snapshots**

FrogDB uses forkless, epoch-based snapshots instead of Redis's fork-based approach. This avoids the 2x memory spike of forking but has subtly different semantics:

- Keys deleted during snapshot iteration may or may not appear in the snapshot
- The snapshot represents a logical point-in-time, not a physical one
- Consistency is ensured via WAL replay during recovery

**LRU/LFU Metadata**

Access frequency (`lfu_counter`) and last access time (`last_access`) are not persisted across restarts. After recovery, all keys appear "fresh" from an eviction perspective. This matches Redis behavior and self-corrects within minutes of normal operation.

---

### Clustering

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Cluster coordination | Gossip protocol | Orchestrated control plane | External orchestrator required |
| Replica data | Slot-specific | Full dataset | Higher replica memory usage |
| Cascading replication | Supported | Not supported | No replica-of-replica |
| Split-brain handling | Last-write-wins (merge) | Discard divergent writes | Potential data loss |

**Orchestrated Control Plane**

FrogDB uses an external orchestrator (3+ instances for HA) instead of Redis's peer-to-peer gossip protocol. This provides:
- Stronger consistency guarantees
- Simpler node implementation
- External dependency requirement

**Full Dataset Replication**

FrogDB replicas hold the entire dataset, not just their assigned hash slots. This simplifies implementation but increases memory usage on replicas.

**Split-Brain Data Loss**

During a network partition, writes to an old primary during the split-brain window (up to `fencing_timeout_ms`, default 10s) are **discarded** when the partition heals:

- Discarded writes logged to `data/split_brain_discarded.log`
- Clients may see acknowledged writes disappear
- This differs from Redis's last-write-wins approach

**Mitigation:** Use `min_replicas_to_write = 1` to prevent writes without replica acknowledgment.

---

### Pub/Sub

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Message ordering | Deterministic (single-threaded) | Not guaranteed across shards | Out-of-order delivery possible |

**Message Ordering**

FrogDB's multi-threaded architecture means pub/sub messages may be delivered out of order to different subscribers on different shards. Within a single subscriber connection, ordering is preserved.

**Impact:** Applications relying on global message ordering across all subscribers should implement application-level sequencing.

---

### Scripting

| Feature | Redis | FrogDB | Impact |
|---------|-------|--------|--------|
| Key validation | Keys should be in KEYS | Keys must be in KEYS | Scripts accessing undeclared keys will error |

**Strict Key Validation**

FrogDB enforces strict key validation for Lua scripts (DragonflyDB-style). All keys accessed by a script **must** be declared in the KEYS array:

```
> EVAL "return redis.call('GET', 'mykey')" 0
-ERR Script attempted to access key 'mykey' not in KEYS array
```

**Correct usage:**
```
> EVAL "return redis.call('GET', KEYS[1])" 1 mykey
"value"
```

**Rationale:** This enables safe parallel execution and cross-shard routing validation.

---

### Consistency Guarantees

FrogDB provides different consistency guarantees than Redis:

| Guarantee | Redis (Single) | Redis (Cluster) | FrogDB |
|-----------|----------------|-----------------|--------|
| Per-key linearizability | Yes | Yes | Yes |
| Cross-key linearizability | Yes | No | No (cross-shard) |
| Snapshot isolation | Yes (single-threaded) | No | No |
| Causal consistency | Yes | No | No |

**Implications:**
- Reads from different shards may see inconsistent snapshots
- Causally related operations may appear out of order to different clients
- Use hash tags and transactions for operations requiring atomicity

---

### Other Differences

**BITFIELD Limitations**

Unsigned integers in BITFIELD are limited to 1-63 bits. u64 is not supported:
```
> BITFIELD key SET u64 0 12345
-ERR BITFIELD: unsigned integers limited to 63 bits
```

**GEO Precision**

GEO commands use geohash encoding with ~0.6mm precision. Retrieved coordinates may differ slightly from stored values:
```
> GEOADD places 13.361389 52.519444 "Berlin"
> GEOPOS places Berlin
1) "13.36138933897018433"
2) "52.51944333774275188"
```

---

## Not Yet Implemented

These features are planned but not available in the current implementation. Check the [ROADMAP.md](ROADMAP.md) for current status.

### Commands

| Command Category | Status | Notes |
|------------------|--------|-------|
| DEBUG (unsafe) | Rejected | DEBUG SEGFAULT, RELOAD, CRASH-AND-RECOVER, SET-ACTIVE-EXPIRE, OOM, PANIC return explicit errors |
| MONITOR | Not planned | Diagnostic command not prioritized |
| MODULE commands | Not planned | No modular architecture |

### Features

| Feature | Status | Notes |
|---------|--------|-------|
| Client tracking | Phase 1 | Default/OPTIN/OPTOUT/NOLOOP modes implemented; BCAST/REDIRECT/PREFIX deferred to Phase 2 |
| Circuit breakers | Not planned | Clients should implement |
| Clustering (full) | In progress | Phases 1 and 3 complete; slot migration and chaos testing remaining |

---

## Command Compatibility Reference

Quick reference for commands with compatibility notes:

| Command | Status | Compatibility Notes |
|---------|--------|---------------------|
| SELECT | Not supported | Single database only |
| CONFIG REWRITE | Not supported | Changes are transient |
| MGET/MSET | Partial | Requires same hash slot |
| DEL/EXISTS (multi) | Partial | Requires same hash slot |
| BLPOP/BRPOP | Full | Blocking list operations |
| XADD/XREAD | Full | Basic stream operations |
| CLUSTER * | Partial | Phases 1 and 3 implemented |
| EVAL/EVALSHA | Partial | Strict key validation |
| PUBLISH | Full | No cross-shard ordering |
| BGSAVE | Full | Forkless semantics |
| HELLO | Full | RESP3 protocol negotiation |
| MEMORY * | Full | DOCTOR, STATS, USAGE, PURGE |
| LATENCY * | Full | DOCTOR, GRAPH, HISTOGRAM, HISTORY |

---

## Migration Guide

### Using Hash Tags

Colocate related keys using hash tags for multi-key operations:

```
# Instead of:
MGET user:1:name user:1:email  # May fail with CROSSSLOT

# Use:
MGET {user:1}name {user:1}email  # Always colocated
```

### Configuration Mapping

| redis.conf | frogdb.toml |
|------------|-------------|
| `bind 0.0.0.0` | `server.bind = "0.0.0.0"` |
| `port 6379` | `server.port = 6379` |
| `maxclients 10000` | `server.max_clients = 10000` |
| `appendonly yes` | `persistence.enabled = true` |
| `appendfsync everysec` | `persistence.durability_mode = { periodic = { interval_ms = 1000 } }` |
| `save 900 1` | (snapshots configured separately) |

### Client Library Considerations

Most Redis clients work without modification. Consider:

1. **Connection pooling:** Works as expected
2. **Pipelining:** Fully supported
3. **Transactions:** Supported with same-slot requirement
4. **Pub/Sub:** Supported; ordering not guaranteed across shards
5. **Lua scripts:** Update to use KEYS array for all key access
6. **Cluster mode:** Use FrogDB's orchestrator instead of Redis Cluster protocol

### Testing Migration

1. Run FrogDB alongside Redis
2. Shadow traffic to FrogDB
3. Compare responses for discrepancies
4. Address CROSSSLOT errors by adding hash tags
5. Update scripts for strict key validation
6. Migrate configuration to TOML format

---

## References

- [INDEX.md](INDEX.md) - Architecture overview
- [ROADMAP.md](ROADMAP.md) - Implementation phases
- [CONSISTENCY.md](CONSISTENCY.md) - Consistency guarantees
- [CLUSTER.md](CLUSTER.md) - Clustering architecture
- [COMMANDS.md](COMMANDS.md) - Command reference
