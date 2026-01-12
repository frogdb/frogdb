# FrogDB Command Reference

Index of command groups and cross-cutting execution concerns.

## Command Groups

| Group | Description | File |
|-------|-------------|------|
| Generic | Key management (DEL, EXISTS, EXPIRE, TTL, TYPE, SCAN, DUMP, RESTORE) | [GENERIC.md](types/GENERIC.md) |
| String | Binary-safe strings (GET, SET, INCR, APPEND) | [STRING.md](types/STRING.md) |
| Sorted Set | Score-ordered sets (ZADD, ZRANGE, ZSCORE, ZRANK) | [SORTED_SET.md](types/SORTED_SET.md) |
| Hash | Field-value maps (HGET, HSET, HGETALL) | [HASH.md](types/HASH.md) |
| List | Ordered sequences (LPUSH, RPUSH, LRANGE) | [LIST.md](types/LIST.md) |
| Set | Unique collections (SADD, SMEMBERS, SINTER) | [SET.md](types/SET.md) |
| Stream | Append-only log (XADD, XREAD, XREADGROUP) | [STREAM.md](types/STREAM.md) |
| Server | Administration (CONFIG, INFO, DEBUG) | [SERVER.md](types/SERVER.md) |
| Pub/Sub | Messaging (SUBSCRIBE, PUBLISH) | [PUBSUB.md](PUBSUB.md) |
| Scripting | Lua scripts (EVAL, EVALSHA) | [SCRIPTING.md](SCRIPTING.md) |
| Connection | Client management (AUTH, PING, QUIT) | [CONNECTION.md](CONNECTION.md) |
| Cluster | Distribution (CLUSTER NODES, CLUSTER SLOTS) | [CLUSTER.md](CLUSTER.md) |

---

## Size Limits

FrogDB enforces Redis-compatible size limits on command input. See [LIMITS.md](LIMITS.md) for:

- Max key/value sizes (512 MB default)
- Collection element limits
- Protocol-level enforcement

---

## Transactions

FrogDB supports MULTI/EXEC transactions with optimistic locking via WATCH. See [TRANSACTIONS.md](TRANSACTIONS.md) for:

- MULTI/EXEC command flow
- Cross-shard restrictions
- WATCH implementation
- Pipelining vs transactions

---

## References

- [EXECUTION.md](EXECUTION.md) - Command trait, arity, flags, type checking
- [STORAGE.md](STORAGE.md) - Value enum and supported data types
- [LIMITS.md](LIMITS.md) - Size limits and enforcement
- [TRANSACTIONS.md](TRANSACTIONS.md) - MULTI/EXEC and pipelining
- [CONFIGURATION.md](CONFIGURATION.md) - CONFIG commands and runtime configuration
