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

## Multi-Key Scripts (Same-Shard Requirement)

Scripts accessing multiple keys **require all keys to be on the same shard**. Use hash tags to guarantee colocation:

```
-- This works: all keys hash to same shard via {user:1} tag
EVAL "..." 2 {user:1}:name {user:1}:email

-- This fails: keys may be on different shards
EVAL "..." 2 user:1:name user:2:email
```

**Why no cross-shard scripts?**
- Lua scripts execute atomically within a single shard's event loop
- Cross-shard access would require distributed locking (defeats multi-threading)
- VLL transaction ordering only guarantees atomicity within scatter-gather, not Lua execution

**Solution:** Use hash tags `{tag}` to colocate related keys on the same shard.
See [CONCURRENCY.md](CONCURRENCY.md#hash-tags-redis-compatible) for hash tag semantics.

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

## Undeclared Key Validation (DragonflyDB-style)

FrogDB **strictly validates** that scripts only access keys declared in the KEYS array.
This matches DragonflyDB's default behavior and is required for correct multi-threaded execution.

### Default Behavior: Strict

```
> EVAL "return redis.call('GET', 'undeclared')" 0
-ERR script tried accessing undeclared key: undeclared
```

Scripts that compute key names dynamically will fail:

```lua
-- This FAILS: 'other_key' not in KEYS array
local other = redis.call('GET', 'other_key')

-- This WORKS: use KEYS array for all key access
local other = redis.call('GET', KEYS[2])
```

### Rationale

In a shared-nothing architecture, each shard operates independently. If scripts could access
arbitrary keys:
1. The script might need to access keys on different shards (deadlock risk)
2. Global locking would be required (defeats multi-threading benefits)
3. Transaction ordering (VLL) couldn't guarantee atomicity

### Compatibility Mode (Optional)

For legacy Redis scripts that access undeclared keys, FrogDB provides a compatibility flag:

```bash
frogdb-server --default_lua_flags=allow-undeclared-keys
```

**Warning:** This mode locks the **entire datastore** for each script execution, significantly
impacting performance. Use only for migration purposes, not production workloads.

| Mode | Key Access | Performance | Atomicity |
|------|------------|-------------|-----------|
| Strict (default) | KEYS array only | Full multi-threading | Per-shard |
| Compatibility | Any key | Global lock, slow | Global |

### Script Flags

Scripts can be executed with specific flags:

| Flag | Description |
|------|-------------|
| `allow-undeclared-keys` | Allow accessing keys not in KEYS array (global lock) |
| `disable-atomicity` | Allow non-atomic execution (not recommended) |

Example:
```
> EVAL_RO "..." 0 FLAGS allow-undeclared-keys
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

## Resource Limits

Lua scripts can consume significant resources. FrogDB provides configuration options to prevent runaway scripts:

### Time Limit

Maximum execution time before script is terminated:

```toml
[scripting]
lua_time_limit_ms = 5000  # 5 seconds (default), 0 = unlimited
```

When exceeded:
- Script is forcibly terminated
- Error returned: `-ERR ETIME script timeout`
- Partial writes may have occurred (no rollback)

**Warning:** Setting to 0 (unlimited) risks blocking a shard indefinitely.

### Timeout Behavior Details

**During Timeout:**

| Phase | Behavior |
|-------|----------|
| Script running | Other commands to same shard queue (shard blocked) |
| Timeout exceeded | Script continues briefly while FrogDB attempts graceful stop |
| Script doesn't yield | Forcibly terminated after grace period |
| After termination | Shard resumes processing queued commands |

**SCRIPT KILL Command:**

Manually terminate a running script:

```
SCRIPT KILL
```

| Scenario | Result |
|----------|--------|
| Script has no writes | Script terminated, `+OK` returned |
| Script has writes | `-UNKILLABLE script has performed writes` |
| No script running | `-NOTBUSY No scripts in execution right now` |

**Why scripts with writes can't be killed:**
- No rollback mechanism exists
- Partial writes are already committed
- Only `SHUTDOWN NOSAVE` can stop (loses all data)

**VM State After Timeout/Kill:**

| State | Preserved | Notes |
|-------|-----------|-------|
| Cached scripts | Yes | EVALSHA still works |
| Global variables | Reset | VM is reset to clean state |
| Pending operations | Discarded | In-flight commands fail |
| Written data | Committed | No rollback |

**BUSY Response:**

While a long script runs, other commands return:

```
-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.
```

This matches Redis behavior and prevents client confusion about command ordering.

### Memory Limit

Maximum heap memory per Lua VM:

```toml
[scripting]
lua_heap_limit_mb = 256  # 256 MB per VM (default), 0 = unlimited
```

When exceeded:
- Lua allocation fails
- Script returns error: `-ERR ENOMEM script out of memory`
- VM state is reset (cached scripts remain)

### Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `lua_time_limit_ms` | `5000` | Max script execution time (0 = unlimited) |
| `lua_heap_limit_mb` | `256` | Max memory per Lua VM (0 = unlimited) |
| `lua_replicate_commands` | `true` | Replicate script effects (not script itself) |

### Per-VM Isolation

Each internal shard has its own Lua VM:
- Memory limits apply per-VM (not global)
- One shard hitting memory limit doesn't affect others
- Script state (global variables) not shared across VMs

---

## Script Caching

Scripts are cached by their SHA1 hash for efficient re-execution:

```rust
struct ScriptCache {
    /// SHA1 -> compiled Lua bytecode
    scripts: HashMap<[u8; 20], CompiledScript>,
}
```

The `EVALSHA` command executes a cached script without re-transmitting the source.

### Cache Characteristics

| Property | Behavior |
|----------|----------|
| Scope | Per-node (not shared across cluster) |
| Persistence | Volatile (lost on restart) |
| Replication | NOT replicated to replicas |
| Propagation | NOT propagated to other cluster nodes |

### NOSCRIPT Error

When `EVALSHA` references a script not in the local cache:

```
> EVALSHA abc123... 1 mykey
-NOSCRIPT No matching script. Please use EVAL.
```

---

## Script Caching in Cluster Mode

In cluster mode, the script cache is **per-node** (Redis/Valkey compatible).

### Behavior

```
Client                    Node A                    Node B
   │                         │                         │
   │── SCRIPT LOAD script ──▶│                         │
   │◀── "abc123..." ─────────│                         │
   │                         │  (script cached)        │
   │                         │                         │
   │  [Key routes to Node B] │                         │
   │                         │                         │
   │── EVALSHA abc123... ────────────────────────────▶│
   │◀── -NOSCRIPT ───────────────────────────────────│
   │                         │                         │  (not cached here)
```

### Client Responsibility

Clients must handle NOSCRIPT errors. The standard pattern:

```
1. Try EVALSHA <sha> <numkeys> <keys...> <args...>
2. If -NOSCRIPT error:
   a. Execute EVAL <script> <numkeys> <keys...> <args...>
   b. Script is now cached on that node
3. Future EVALSHA calls to that node succeed
```

Most Redis client libraries (Lettuce, StackExchange.Redis, etc.) implement this automatically.

### Pre-loading Scripts

For guaranteed EVALSHA success, pre-load scripts to all nodes at startup:

```python
# Load to all master nodes
for node in cluster.master_nodes():
    node.script_load(my_script)
```

### Replica Considerations

Replicas maintain their own script cache, NOT synchronized with primary:

- Scripts executed on primary are NOT propagated to replicas
- `READONLY` + `EVALSHA` may return NOSCRIPT even if primary has the script
- Either pre-load scripts to replicas, or use `EVAL` for replica reads

### Slot Migration and EVALSHA

During slot migration, scripts may fail in various ways:

**Scenario 1: Script execution during migration**
```
Client: EVALSHA abc123 1 {slot:123}:key
        │
        └── Slot 123 migrating from Node A → Node B
```

| Migration Phase | Behavior |
|-----------------|----------|
| **MIGRATING state** | Node A returns `-ASK` redirect |
| **IMPORTING state** | Node B requires `ASKING` first |
| **Script accesses migrated key** | `-TRYAGAIN` if key mid-transfer |
| **Completed** | Normal routing to Node B |

**Scenario 2: Multi-key script spanning migration**

If a script's keys span a slot being migrated:
- Keys may be on different nodes during migration
- Script receives `-CROSSSLOT` (if keys on different nodes)
- Use hash tags to ensure colocation

**Scenario 3: Script cache on new node**

After migration completes:
- Scripts cached on old node (A) are NOT transferred
- New node (B) returns `-NOSCRIPT` on `EVALSHA`
- Client must re-load script via `EVAL`

**Client Handling:**

```python
def execute_script_safe(cluster, sha, keys, args):
    while True:
        try:
            return cluster.evalsha(sha, keys, args)
        except MovedError as e:
            cluster.update_slot_map()
        except AskError as e:
            node = cluster.get_node(e.host, e.port)
            node.asking()
            return node.evalsha(sha, keys, args)
        except NoScriptError:
            return cluster.eval(SCRIPT_SOURCE, keys, args)
        except TryAgainError:
            time.sleep(0.1)  # Retry after brief delay
```

**Best Practice:** Pre-load scripts to all nodes, re-load after topology changes.
