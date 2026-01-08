# FrogDB Lua Scripting

This document details Lua script execution in FrogDB, including the execution model, cross-shard requirements, and atomicity guarantees.

## Execution Model

Lua scripts execute atomically within a single shard (Redis-compatible):

```
EVAL "return redis.call('GET', KEYS[1])" 1 mykey
         │
         ▼
    Shard (owner of mykey)
         │
         ├── Block shard event loop
         │
         ├── Execute Lua in shard's VM
         │   └── redis.call('GET', 'mykey') -> local store lookup
         │
         └── Unblock, return result
```

### Key Characteristics

- **Atomic**: Script execution is not interruptible
- **Blocking**: Shard cannot process other commands during script execution
- **Per-shard VM**: Each shard has its own Lua VM instance
- **No cross-shard access**: Scripts can only access keys owned by the executing shard

---

## Cross-Shard Scripts

Scripts with keys on multiple shards require all keys to hash to the same shard using hash tags:

```
-- This works: all keys hash to same shard via {user:1} tag
EVAL "..." 2 {user:1}:name {user:1}:email

-- This fails: keys may be on different shards
EVAL "..." 2 user:1:name user:2:email
```

### Validation

Before script execution, FrogDB validates that all keys hash to the same shard:

```rust
fn validate_script_keys(keys: &[Bytes]) -> Result<ShardId, Error> {
    let shards: HashSet<_> = keys.iter()
        .map(|k| shard_for_key(k))
        .collect();

    if shards.len() > 1 {
        return Err(Error::CrossShardScript);
    }

    Ok(shards.into_iter().next().unwrap_or(0))
}
```

---

## Commands

| Command | Description |
|---------|-------------|
| EVAL | Execute Lua script with arguments |
| EVALSHA | Execute cached script by SHA1 hash |
| SCRIPT LOAD | Load script into cache |
| SCRIPT EXISTS | Check if script is cached |
| SCRIPT FLUSH | Clear script cache |

---

## redis.call() Bindings

Scripts interact with FrogDB via the `redis.call()` and `redis.pcall()` functions:

```lua
-- Direct call (errors propagate)
local value = redis.call('GET', KEYS[1])

-- Protected call (errors returned as table)
local result = redis.pcall('SET', KEYS[1], ARGV[1])
if result.err then
    -- handle error
end
```

### Available Commands

All single-key commands that operate on the local shard are available:
- String: GET, SET, INCR, APPEND, etc.
- Hash: HGET, HSET, HGETALL, etc.
- List: LPUSH, RPOP, LRANGE, etc.
- Set: SADD, SMEMBERS, SINTER (single key), etc.
- Sorted Set: ZADD, ZRANGE, ZSCORE, etc.

### Restricted Commands

- Multi-key commands that might span shards
- Administrative commands (CONFIG, DEBUG, etc.)
- Blocking commands (BLPOP, BRPOP)

---

## Script Caching (Future)

Scripts can be cached by their SHA1 hash for efficient re-execution:

```rust
struct ScriptCache {
    /// SHA1 -> compiled Lua bytecode
    scripts: HashMap<[u8; 20], CompiledScript>,
}
```

The `EVALSHA` command executes a cached script without re-transmitting the source.
