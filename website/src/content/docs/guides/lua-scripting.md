---
title: "Lua Scripting"
description: "FrogDB supports Lua scripting via EVAL and EVALSHA, compatible with the Redis scripting model. Scripts execute atomically within a single shard."
sidebar:
  order: 5
---
FrogDB supports Lua scripting via EVAL and EVALSHA, compatible with the Redis scripting model. Scripts execute atomically within a single shard.

## Basic Usage

```
EVAL "return redis.call('GET', KEYS[1])" 1 mykey
```

Scripts interact with FrogDB via `redis.call()` and `redis.pcall()`:

```lua
-- Direct call (errors propagate to the client)
local value = redis.call('GET', KEYS[1])

-- Protected call (errors returned as a table)
local result = redis.pcall('SET', KEYS[1], ARGV[1])
if result.err then
    -- handle error
end
```

### Key Characteristics

- **Atomic**: Script execution is not interruptible -- no other commands on the same shard execute during the script
- **Blocking**: The shard cannot process other commands while a script runs
- **No cross-shard access**: Scripts can only access keys owned by the executing shard (by default)

## Strict Key Validation

FrogDB enforces that scripts only access keys declared in the KEYS array. This differs from Redis, which treats undeclared key access as a guideline rather than an error.

```
> EVAL "return redis.call('GET', 'undeclared')" 0
-ERR script tried accessing undeclared key: undeclared
```

All key access must go through the KEYS array:

```lua
-- This FAILS: 'other_key' not in KEYS array
local other = redis.call('GET', 'other_key')

-- This WORKS: use KEYS array for all key access
local other = redis.call('GET', KEYS[2])
```

### Compatibility Mode

For migrating legacy Redis scripts that access undeclared keys, a compatibility flag is available:

```bash
frogdb-server --default_lua_flags=allow-undeclared-keys
```

This mode locks the entire datastore for each script execution, severely impacting performance. Use only for migration, not production workloads.

| Mode | Key Access | Performance |
|------|------------|-------------|
| Strict (default) | KEYS array only | Full multi-threading |
| Compatibility | Any key | Global lock, significantly slower |

**Migration path:**
1. Enable compatibility mode temporarily
2. Identify scripts using undeclared keys (check the `frogdb_script_undeclared_key_access` metric)
3. Rewrite scripts to declare all keys
4. Disable compatibility mode

## Multi-Key Scripts

Scripts accessing multiple keys require all keys to be on the same shard. Use hash tags to guarantee colocation:

```
-- This works: all keys hash to same shard via {user:1} tag
EVAL "..." 2 {user:1}:name {user:1}:email

-- This fails: keys may be on different shards
EVAL "..." 2 user:1:name user:2:email
```

When `allow_cross_slot_standalone = true` is configured in standalone mode, cross-shard scripts are supported, though at the cost of locking all participating shards during execution. Use hash tags for best performance.

## Available Commands in Scripts

All single-key commands that operate on the local shard are available inside scripts:
- String: GET, SET, INCR, APPEND, etc.
- Hash: HGET, HSET, HGETALL, etc.
- List: LPUSH, RPOP, LRANGE, etc.
- Set: SADD, SMEMBERS, etc.
- Sorted Set: ZADD, ZRANGE, ZSCORE, etc.

### Restricted Commands

The following are not available inside scripts:
- Multi-key commands that might span shards
- Administrative commands (CONFIG, DEBUG, etc.)
- Blocking commands (BLPOP, BRPOP)
- Transaction commands (MULTI, EXEC, DISCARD, WATCH)

Transaction commands are forbidden because scripts already execute atomically -- they are effectively an implicit transaction.

## Resource Limits

### Time Limit

```toml
[scripting]
lua_time_limit_ms = 5000  # 5 seconds (default), 0 = unlimited
```

When exceeded, the script is forcibly terminated and returns `-ERR ETIME script timeout`. Partial writes may have occurred (no rollback).

While a long-running script blocks a shard, other commands to that shard return:
```
-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.
```

Use `SCRIPT KILL` to manually terminate a running script (only works if the script has not performed any writes).

### Memory Limit

```toml
[scripting]
lua_heap_limit_mb = 256  # 256 MB per VM (default), 0 = unlimited
```

When exceeded, the script returns `-ERR ENOMEM script out of memory`.

### Timeout Recommendations

Set `client_timeout_ms >= lua_time_limit_ms + 100` to avoid client disconnection during legitimate long-running scripts.

## Script Caching

Scripts are cached by SHA1 hash for efficient re-execution via EVALSHA:

```
> SCRIPT LOAD "return redis.call('GET', KEYS[1])"
"a42059b3..."

> EVALSHA a42059b3... 1 mykey
"value"
```

If a cached script is not found, the server returns `-NOSCRIPT`. Most Redis client libraries handle this automatically by falling back to EVAL.

### Cache Properties

| Property | Behavior |
|----------|----------|
| Scope | Per-node (each node has independent cache) |
| Persistence | Volatile (lost on restart) |
| Eviction | LRU when cache exceeds size limits |

### Cache Configuration

```toml
[scripting]
lua_script_cache_max_size = 10000       # Max cached scripts (default)
lua_script_cache_max_bytes = 104857600  # Max total bytecode, 100MB (default)
```

## Script State Isolation

Global variables are reset between script executions (Redis-compatible):

```lua
-- Script A
_G.my_counter = 1
return "set"

-- Script B (same shard, later)
return _G.my_counter  -- Returns nil, NOT 1
```

Cached bytecode (EVALSHA) is preserved between executions, but all other state is reset.

## Determinism

FrogDB replicates script effects (the resulting writes), not the script source. This means scripts can safely use `redis.call('TIME')` for time access.

The following functions are forbidden to preserve determinism:
- `os.time()`, `os.clock()`, `os.date()`
- `math.randomseed()`
- `loadfile()`, `dofile()`

`math.random()` is available but seeded deterministically per-execution.

## Scripts Inside Transactions

EVAL and EVALSHA can be queued inside a MULTI block:

```
MULTI
SET key1 val1
EVAL "return redis.call('GET', KEYS[1])" 1 key1
SET key2 val2
EXEC
```

The script executes in order with the other queued commands. If the script errors, other commands in the transaction still execute (Redis-compatible).

WATCH works with scripts as expected -- if a watched key is modified by another client before EXEC, the transaction (including the script) is aborted.

## Cluster Considerations

In cluster mode, the script cache is per-node. If a key routes to a different node than where the script was loaded, you may get a `-NOSCRIPT` error. Most client libraries handle this automatically.

For guaranteed EVALSHA success, pre-load scripts to all nodes at startup:

```python
for node in cluster.master_nodes():
    node.script_load(my_script)
```

## Command Reference

| Command | Description |
|---------|-------------|
| `EVAL script numkeys [key ...] [arg ...]` | Execute Lua script |
| `EVALSHA sha1 numkeys [key ...] [arg ...]` | Execute cached script by SHA1 |
| `SCRIPT LOAD script` | Load script, return SHA1 |
| `SCRIPT EXISTS sha1 [sha1...]` | Check if scripts are cached |
| `SCRIPT FLUSH [ASYNC\|SYNC]` | Clear all cached scripts |
| `SCRIPT KILL` | Terminate running script (if no writes) |
