---
title: "Redis Compatibility"
description: "FrogDB is wire-protocol compatible with Redis v6+ via RESP2 and RESP3. Most Redis clients work without modification. This page documents intentional differen..."
sidebar:
  order: 3
---
FrogDB is wire-protocol compatible with Redis v6+ via RESP2 and RESP3. Most Redis clients work without modification. This page documents intentional differences and features not yet available.

## Intentional Incompatibilities

These are deliberate design choices that will not change.

### Database Model

| Feature | Redis | FrogDB |
|---------|-------|--------|
| Database selection | SELECT 0-15 | Not supported |
| Cross-slot multi-key ops | Allowed (standalone) | Always rejected |

FrogDB uses a single database per instance. `SELECT 0` is accepted as a no-op; any other index returns an error. `MOVE` and `SWAPDB` are not supported.

**Workaround:** Run separate FrogDB instances for logical separation, or use key prefixes.

Multi-key operations (MGET, MSET, DEL, EXISTS, etc.) require all keys to hash to the same slot, even in standalone mode. Use hash tags to colocate related keys:

```
> MSET {user:1}name Alice {user:1}email alice@example.com
OK
```

### Configuration

| Feature | Redis | FrogDB |
|---------|-------|--------|
| Config format | redis.conf | TOML |
| CONFIG REWRITE | Persists changes | Not supported |
| Environment variables | Limited | Native FROGDB_ prefix |

FrogDB uses TOML configuration files. Changes made via `CONFIG SET` are transient and lost on restart. Edit `frogdb.toml` directly for permanent changes.

Environment variables use the `FROGDB_` prefix with double-underscore nesting:
```bash
FROGDB_SERVER__PORT=6380
FROGDB_PERSISTENCE__DURABILITY_MODE=sync
```

### Persistence

| Feature | Redis | FrogDB |
|---------|-------|--------|
| Snapshot mechanism | Fork-based (RDB) | Forkless epoch-based |
| LRU/LFU metadata | Persisted | Not persisted |

FrogDB uses forkless, epoch-based snapshots instead of Redis's fork-based approach. This avoids the 2x memory spike of forking. Consistency is ensured via WAL replay during recovery.

Access frequency and last access time are not persisted across restarts. After recovery, all keys appear "fresh" from an eviction perspective. This self-corrects within minutes of normal operation.

### Persistence Commands

| Command | Redis | FrogDB |
|---------|-------|--------|
| SAVE | Blocking snapshot | Not supported (use BGSAVE) |
| BGREWRITEAOF | Rewrites AOF file | Not supported (no AOF; RocksDB manages WAL) |
| SYNC | Legacy replication | Not supported (use PSYNC) |

### Clustering

| Feature | Redis | FrogDB |
|---------|-------|--------|
| Cluster coordination | Gossip protocol | Orchestrated control plane |
| Replica data | Slot-specific | Full dataset |
| Cascading replication | Supported | Not supported |
| Split-brain handling | Last-write-wins | Discard divergent writes |

FrogDB uses an external orchestrator instead of peer-to-peer gossip. Replicas hold the entire dataset. During split-brain scenarios, writes to the old primary are discarded when the partition heals.

**Mitigation:** Use `min-replicas-to-write = 1` to prevent writes without replica acknowledgment.

### Pub/Sub

Message ordering is not guaranteed across shards due to FrogDB's multi-threaded architecture. Within a single subscriber connection, ordering is preserved.

### Scripting

FrogDB enforces strict key validation for Lua scripts. All keys accessed by a script must be declared in the KEYS array:

```
> EVAL "return redis.call('GET', 'mykey')" 0
-ERR Script attempted to access key 'mykey' not in KEYS array
```

See [Lua Scripting](/guides/lua-scripting/) for a compatibility mode option.

### Consistency Guarantees

| Guarantee | Redis (Single) | Redis (Cluster) | FrogDB |
|-----------|----------------|-----------------|--------|
| Per-key linearizability | Yes | Yes | Yes |
| Cross-key linearizability | Yes | No | No (cross-shard) |
| Snapshot isolation | Yes | No | No |
| Causal consistency | Yes | No | No |

Use hash tags and transactions for operations requiring atomicity across multiple keys.

### Other Differences

**BITFIELD:** Unsigned integers limited to 1-63 bits. u64 is not supported.

**GEO precision:** GEO commands use geohash encoding with ~0.6mm precision. Retrieved coordinates may differ slightly from stored values.

## Not Yet Available

| Feature | Notes |
|---------|-------|
| WAITAOF | Depends on WAL acknowledgment tracking |
| DEBUG (unsafe subcommands) | SEGFAULT, RELOAD, CRASH-AND-RECOVER, etc. return errors |
| MONITOR | Not available |
| MODULE commands | No module architecture |

## Command Compatibility Reference

| Command | Status | Notes |
|---------|--------|-------|
| SELECT (non-zero) | Not supported | Single database only; SELECT 0 accepted |
| MOVE, SWAPDB | Not supported | Single database only |
| CONFIG REWRITE | Not supported | Changes are transient |
| SAVE | Not supported | Use BGSAVE |
| BGREWRITEAOF | Not supported | No AOF |
| SYNC | Not supported | Use PSYNC |
| MODULE * | Not supported | No module architecture |
| MONITOR | Not supported | Not available |
| WAITAOF | Not available | Stub |
| MGET/MSET | Supported | Requires same hash slot |
| DEL/EXISTS (multi) | Supported | Requires same hash slot |
| EVAL/EVALSHA | Supported | Strict key validation |
| BLPOP/BRPOP | Supported | Full compatibility |
| XADD/XREAD | Supported | Full compatibility |
| PUBLISH | Supported | No cross-shard ordering |
| BGSAVE | Supported | Forkless semantics |
| HELLO | Supported | RESP3 negotiation |
| MEMORY * | Supported | DOCTOR, STATS, USAGE, PURGE |
| LATENCY * | Supported | DOCTOR, GRAPH, HISTOGRAM, HISTORY |

## Migration Guide

### Configuration Mapping

| redis.conf | frogdb.toml |
|------------|-------------|
| `bind 0.0.0.0` | `server.bind = "0.0.0.0"` |
| `port 6379` | `server.port = 6379` |
| `maxclients 10000` | `server.max-clients = 10000` |
| `appendonly yes` | `persistence.enabled = true` |
| `appendfsync everysec` | `persistence.durability-mode = { periodic = { interval-ms = 1000 } }` |

### Client Library Considerations

Most Redis clients work without modification. Key points:

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
