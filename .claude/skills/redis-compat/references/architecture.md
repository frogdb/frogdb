# FrogDB vs Redis: Architecture Differences Affecting Compatibility

This supplements `~/.claude/skills/shared/frogdb-rules.md` (general architectural invariants)
with details specifically relevant to redis-compat test failures.

## Multi-Shard Threading

FrogDB uses a shared-nothing, multi-shard architecture where each shard owns its data
exclusively. Redis is single-threaded.

### Cross-slot errors

All multi-key operations require keys to hash to the same slot, even in standalone mode:

```
> MGET foo bar
-CROSSSLOT Keys in request don't hash to the same slot
```

**Affected suites**: Nearly all suites with multi-key operations that don't use hash tags.
The TCL test suite uses `--singledb` and `--ignore-encoding` flags which help, but many
tests still use unrelated key names.

**Mitigation**: The `--singledb` flag in the test runner helps with SELECT-related tests.
Cross-slot failures in data command tests are usually genuine incompatibilities that need
hash tag workarounds in FrogDB or skips.

### Blocking command atomicity gaps

Redis processes blocking commands atomically in its event loop. FrogDB's multi-shard
architecture creates gaps:

- LPUSH on shard A notifies a BLPOP waiter, but DEL on the same key (or a concurrent
  operation) may execute before the waiter reads the value
- MULTI/EXEC is not isolated from the perspective of blocking commands on other shards
- BLMOVE does not correctly wake clients blocked on the destination list

**Affected tests** (all skipped in `skiplist.txt`):
- `unit/type/list#BLPOP, LPUSH + DEL should not awake blocked client`
- `unit/type/list#MULTI/EXEC is isolated from the point of view of BLPOP`
- `unit/type/list#BRPOPLPUSH with multiple blocked clients`
- `unit/type/list#Linked LMOVEs`
- `unit/type/list#Circular BRPOPLPUSH`
- And more — see the "BLOCKED LIST CORRECTNESS" section in `skiplist.txt`

### Blocked client tracking

FrogDB does not update `blocked_clients` in CLIENT LIST or INFO output. Tests that poll
for blocked state will hang until timeout.

**Affected suites**: `unit/type/zset` (BZPOPMIN/BZMPOP), `unit/pause`, `unit/multi`

### Pub/Sub fan-out

Broadcast pub/sub (SUBSCRIBE/PUBLISH) uses shard 0 as coordinator. Message ordering is
not guaranteed across shards. Keyspace notifications have different delivery semantics.

**Affected tests** (skipped):
- `unit/pubsub#PubSub messages with CLIENT REPLY OFF`
- `unit/pubsub#/after UNSUBSCRIBE without arguments`
- `unit/pubsub#/Keyspace notifications`
- And more — see "PUBSUB / NOTIFICATIONS" in `skiplist.txt`

## RocksDB Persistence

FrogDB uses RocksDB for persistence instead of Redis's AOF and RDB files.

### No AOF/RDB format

All tests related to AOF file format, RDB encoding, corrupt dump handling, and
format conversion are permanently skipped:

- `integration/aof`, `integration/aof-race`, `integration/aof-multi-part`
- `unit/aofrw`
- `integration/rdb`, `integration/corrupt-dump`, `integration/corrupt-dump-fuzzer`
- `integration/convert-zipmap-hash-on-load`
- `integration/convert-ziplist-hash-on-load`
- `integration/convert-ziplist-zset-on-load`

### No fork-based snapshots

FrogDB uses forkless, epoch-based snapshots. BGSAVE works but with different semantics:
- No child process (no `wait3()`, no `info persistence` child PID)
- No 2x memory spike
- Snapshot may not represent an exact point-in-time

### OBJECT ENCODING differences

FrogDB does not use Redis's internal encodings (ziplist, listpack, intset, hashtable, etc.).
The `--ignore-encoding` flag in the test runner suppresses most encoding-related checks,
but some tests explicitly verify encoding transitions.

## Single-DB Model

FrogDB supports only one database per instance. There is no `SELECT` command.

### Affected commands

| Command | FrogDB Behavior |
|---------|-----------------|
| SELECT | Returns error |
| SWAPDB | Not implemented |
| MOVE | Not implemented |
| CONFIG SET databases | Ignored |

### Affected suites

- `unit/introspection` — Uses SELECT in many tests
- Any test that switches databases for isolation

## Strict Script Key Validation

FrogDB requires all keys accessed by Lua scripts to be declared in the KEYS array
(DragonflyDB-style). Redis treats this as a should, not a must.

```
> EVAL "return redis.call('GET', 'mykey')" 0
-ERR Script attempted to access key 'mykey' not in KEYS array
```

**Affected suites**: `unit/scripting` (entire suite skipped)

## Unsupported Commands

Commands in `stub.rs` return `-ERR <COMMAND> is not implemented` instead of the expected
behavior. The full list is in `frogdb-server/crates/server/src/commands/stub.rs`.

Key categories:
- **MODULE** commands — Module API not supported
- **WAITAOF** — AOF-specific replication
- **DEBUG** subcommands — Marked with `needs:debug` tag, excluded via `DEFAULT_EXCLUDE_TAGS`

## HyperLogLog

FrogDB stores HyperLogLog as a separate data type, not as a string with a special header.

### Differences

- No sparse/dense encoding (always uses dense-equivalent representation)
- `OBJECT ENCODING <hll-key>` returns a different value
- Corruption tests don't apply (different storage format)
- Sparse-to-dense promotion tests don't apply
- `hll-sparse-max-bytes` config has no effect

**Skipped tests** (see "HLL STORAGE" in `skiplist.txt`):
- `unit/hyperloglog#/Corrupted sparse HyperLogLogs`
- `unit/hyperloglog#HyperLogLog self test passes`
- `unit/hyperloglog#/HyperLogLogs are promote from sparse to dense`
- And more

## Hash Ordering

FrogDB uses `HashMap` (unordered) for hash fields. Redis preserves insertion order
in ziplist/listpack encodings for small hashes.

**Affected tests**:
- `unit/type/hash#/Hash ziplist of various encodings` — Checks insertion order

Tests that check hash field values (not order) should still pass.

## BITFIELD u63 Limit

FrogDB limits unsigned BITFIELD integers to 63 bits (u1-u63). Redis supports u1-u64.

```
> BITFIELD key SET u64 0 12345
-ERR BITFIELD: unsigned integers limited to 63 bits
```

**Affected suites**: `unit/bitfield` (entire suite skipped)

## CONFIG REWRITE / RESETSTAT

- `CONFIG REWRITE` returns an error (not supported)
- `CONFIG RESETSTAT` returns OK but is a no-op

**Affected suites**: `unit/introspection-2`, `unit/lazyfree`, `unit/latency-monitor`

## Cluster and Replication

FrogDB uses an orchestrated control plane (Raft) instead of Redis's gossip protocol.
Replication uses a checkpoint-based approach instead of PSYNC.

All cluster gossip tests, replication stream tests, and failover tests are permanently
skipped when running in external server mode (they require starting/stopping multiple
server instances which the TCL test harness controls).

**Skipped suites** (marked as "External Mode" in STATUS.md):
- `unit/cluster/*` (except those testing client-facing cluster commands)
- `integration/replication*`
- `integration/psync*`
- `integration/failover`
- `integration/block-repl`

## Script KILL / FUNCTION KILL

FrogDB is multi-threaded, so long-running scripts don't block other clients the way they
do in single-threaded Redis. `SCRIPT KILL` and `FUNCTION KILL` have different semantics.

**Skipped tests**:
- `unit/functions#FUNCTION - test function kill`
- `unit/functions#FUNCTION - test script kill not working on function`
- `unit/functions#FUNCTION - test function kill not working on eval`

## LIBRARIES Error Handling

FUNCTION LOAD failures in the LIBRARIES context produce malformed error responses that
corrupt the connection state, causing cascading "Bad protocol" errors in subsequent tests.

**Skipped tests**: `unit/functions#/^LIBRARIES -`

## Sorted Sets with Regular Sets

ZUNIONSTORE/ZINTERSTORE with mixed set types (sorted set + regular set) and weights
has edge cases that differ from Redis.

**Skipped tests**: `unit/type/zset#/with a regular set and weights`

## Performance Timeouts

Some Redis tests are performance-sensitive and may exceed the suite timeout on slower
hardware or under load:

- `unit/type/set#SDIFF fuzzing`
- `unit/type/zset#/ZDIFF fuzzing`
- `unit/sort#/SORT speed.*directly`
- `unit/other#/PIPELINING stresser`
- `unit/hyperloglog#/PFCOUNT multiple-keys merge returns cardinality of union`

These are skipped by default. If investigating performance, increase `--suite-timeout`.
