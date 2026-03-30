---
title: "Command Reference"
description: "FrogDB implements the Redis command set and is wire-compatible with Redis v6+ via RESP2 and RESP3. Most Redis clients work without modification."
sidebar:
  order: 2
---
FrogDB implements the Redis command set and is wire-compatible with Redis v6+ via RESP2 and RESP3. Most Redis clients work without modification.

For full documentation of individual commands, see the [Redis command reference](https://redis.io/docs/latest/commands/).

## Supported Command Groups

| Group | Examples | Redis Docs |
|-------|----------|------------|
| Generic | DEL, EXISTS, EXPIRE, TTL, TYPE, SCAN, DUMP, RESTORE | [redis.io/commands#generic](https://redis.io/docs/latest/commands/?group=generic) |
| String | GET, SET, INCR, APPEND, MGET, MSET | [redis.io/commands#string](https://redis.io/docs/latest/commands/?group=string) |
| Hash | HGET, HSET, HGETALL, HDEL | [redis.io/commands#hash](https://redis.io/docs/latest/commands/?group=hash) |
| List | LPUSH, RPUSH, LRANGE, BLPOP, BRPOP | [redis.io/commands#list](https://redis.io/docs/latest/commands/?group=list) |
| Set | SADD, SMEMBERS, SINTER, SUNION | [redis.io/commands#set](https://redis.io/docs/latest/commands/?group=set) |
| Sorted Set | ZADD, ZRANGE, ZSCORE, ZRANK | [redis.io/commands#sorted-set](https://redis.io/docs/latest/commands/?group=sorted_set) |
| Stream | XADD, XREAD, XREADGROUP, XLEN | [redis.io/commands#stream](https://redis.io/docs/latest/commands/?group=stream) |
| Pub/Sub | SUBSCRIBE, PUBLISH, SSUBSCRIBE, SPUBLISH | [redis.io/commands#pubsub](https://redis.io/docs/latest/commands/?group=pubsub) |
| Scripting | EVAL, EVALSHA, SCRIPT LOAD | [redis.io/commands#scripting](https://redis.io/docs/latest/commands/?group=scripting) |
| Transactions | MULTI, EXEC, DISCARD, WATCH | [redis.io/commands#transactions](https://redis.io/docs/latest/commands/?group=transactions) |
| Server | CONFIG, INFO, BGSAVE, MEMORY, LATENCY | [redis.io/commands#server](https://redis.io/docs/latest/commands/?group=server) |
| Connection | AUTH, PING, QUIT, HELLO | [redis.io/commands#connection](https://redis.io/docs/latest/commands/?group=connection) |
| Cluster | CLUSTER NODES, CLUSTER SLOTS, CLUSTER INFO | [redis.io/commands#cluster](https://redis.io/docs/latest/commands/?group=cluster) |

## FrogDB Extensions

FrogDB provides commands not available in Redis, Valkey, or DragonflyDB.

### Event Sourcing (ES.*)

First-class event sourcing primitives built on Redis Streams with optimistic concurrency control, version-based reads, and snapshot-accelerated replay.

| Command | Description |
|---------|-------------|
| ES.APPEND | Append event with OCC version check |
| ES.READ | Read events by version range |
| ES.REPLAY | Replay events, optionally from snapshot |
| ES.INFO | Stream metadata (version, entries, etc.) |
| ES.SNAPSHOT | Store a snapshot at a version |
| ES.ALL | Read global $all stream (scatter-gather) |

See [Event Sourcing](/guides/event-sourcing/) for full documentation.

## Behavioral Differences

Some commands behave differently in FrogDB compared to Redis:

- **Multi-key operations** (MGET, MSET, DEL with multiple keys) require all keys to hash to the same slot. Use hash tags `{tag}` to colocate keys.
- **EVAL/EVALSHA** enforce strict key validation -- all accessed keys must be declared in the KEYS array.
- **SELECT** only accepts database 0 (single-database model).
- **Pub/Sub** message ordering is not guaranteed across shards.

See [Compatibility](/guides/compatibility/) for the full list of differences.

## Unsupported Commands

The following commands are not available:

| Command | Reason |
|---------|--------|
| SELECT (non-zero) | Single database per instance |
| MOVE, SWAPDB | Single database per instance |
| CONFIG REWRITE | Changes via CONFIG SET are transient |
| SAVE | Use BGSAVE; continuous WAL makes SAVE redundant |
| BGREWRITEAOF | No AOF; RocksDB manages WAL lifecycle |
| SYNC | Legacy protocol; use PSYNC |
| MODULE * | No module architecture |
| MONITOR | Not available |

## Size Limits

FrogDB enforces Redis-compatible size limits on command input. See [Limits](/guides/limits/) for details on max key/value sizes, collection element limits, and protocol-level enforcement.
