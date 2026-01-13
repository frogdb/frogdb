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
- [VLL](VLL.md) transaction ordering only guarantees atomicity within scatter-gather, not Lua execution

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

### Compatibility Mode Performance Impact

When compatibility mode is enabled (`--default_lua_flags=allow-undeclared-keys`), performance degrades significantly due to global locking.

**Performance Comparison:**

| Metric | Strict Mode | Compatibility Mode |
|--------|-------------|-------------------|
| Concurrent scripts | `num_shards` parallel | 1 at a time |
| Latency (p50) | ~1ms | ~10ms (10x worse) |
| Latency (p99) | ~5ms | ~100ms (20x worse) |
| Throughput | Linear with cores | Single-core bound |
| Lock scope | Per-shard (local) | Global (entire node) |

**Why Global Lock?**

Without knowing which keys a script will access:
1. Script could read/write any key on any shard
2. No way to determine which shards to lock
3. Only safe option: lock entire datastore

```rust
fn execute_script_compatibility_mode(script: &Script) {
    // Must acquire global lock - blocks ALL other operations
    let _global_guard = GLOBAL_LOCK.write().await;

    // Now script can safely access any key
    let result = vm.execute(script);

    // Release lock - other operations can proceed
    drop(_global_guard);
}
```

**Benchmarks:**

```
8-core node, 8 shards, 1000 concurrent clients:

Strict Mode:
  Operations/sec: 850,000
  Avg latency:    0.9ms
  CPU utilization: 95% (all cores)

Compatibility Mode:
  Operations/sec: 45,000
  Avg latency:    22ms
  CPU utilization: 12% (single core bottleneck)
```

**When to Use Compatibility Mode:**

| Use Case | Recommendation |
|----------|----------------|
| Migration from Redis | Temporary only, migrate scripts |
| Development/testing | Acceptable for low-traffic testing |
| Production workloads | **Never** - rewrite scripts |

**Migration Path:**

1. Enable compatibility mode temporarily
2. Identify scripts using undeclared keys (`frogdb_script_undeclared_key_access` metric)
3. Rewrite scripts to declare all keys
4. Disable compatibility mode

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

### Timeout Grace Period

When a script exceeds `lua_time_limit_ms`, FrogDB attempts a graceful stop before forcible termination.

**Timeout Sequence:**
```
T=0:           Script starts executing
T=5000ms:      Time limit exceeded (default 5s)
T=5000ms:      Set "soft stop" flag
T=5000-5100ms: Script has 100ms grace period to yield
               - If script checks yield points: stops gracefully
               - If script is in tight loop: continues
T=5100ms:      Grace period exceeded → forcible termination
               - Lua VM is aborted mid-execution
               - Script returns -ERR ETIME
               - Partial writes remain committed
```

**Grace Period Configuration:**
```toml
[scripting]
lua_time_limit_ms = 5000        # Time limit
lua_timeout_grace_ms = 100      # Grace period (default 100ms)
```

**Yield Points:**
Scripts naturally yield at:
- `redis.call()` / `redis.pcall()` invocations
- Certain Lua library calls (io, os)

Scripts in tight loops (pure computation) may not yield:
```lua
-- This script may require forcible termination
local sum = 0
for i = 1, 1000000000 do
    sum = sum + i  -- No yield points
end
return sum
```

**After Forcible Termination:**
- VM is reset to clean state
- Cached scripts (bytecode) remain valid
- Shard resumes normal operation

### Script Timeout vs Client Timeout Interaction

Script timeout (`lua_time_limit_ms`) and client idle timeout (`client_timeout_ms`) are independent:

| Timeout | What it bounds | Effect on other |
|---------|----------------|-----------------|
| `lua_time_limit_ms` | Script execution time | Does NOT reset client idle timer |
| `client_timeout_ms` | Time between client commands | Does NOT affect running scripts |

**Example:** If `lua_time_limit_ms = 5000` and `client_timeout_ms = 3000`:
- A script running for 4 seconds will be terminated (script timeout)
- A client waiting for a 4-second script result may be disconnected (client timeout)
- These timeouts can trigger independently

**Recommendation:** Set `client_timeout_ms >= lua_time_limit_ms + grace period` to avoid client
disconnection during legitimate long-running scripts.

### Cross-Script State Isolation

Lua global variables are **reset between script executions** (Redis-compatible).

**Isolation Behavior:**
```lua
-- Script A: Sets a global
_G.my_counter = 1
return "set"

-- Script B (same shard, later): Global doesn't exist
return _G.my_counter  -- Returns nil, NOT 1
```

**Why Reset Globals?**
1. **Determinism:** Scripts produce same output for same input
2. **Replication:** Effects replicate correctly without global state
3. **Security:** Scripts can't leak data to other scripts
4. **Simplicity:** No need to track cross-script dependencies

**What Is Preserved Between Executions:**

| State | Preserved | Notes |
|-------|-----------|-------|
| Global variables (`_G.*`) | No | Reset to clean environment |
| Loaded modules (`require`) | No | Must re-require each execution |
| Local variables | No | Local scope ends with script |
| Cached bytecode (`EVALSHA`) | Yes | Compiled script retained |
| VM instance | Yes | Same VM, reset environment |

**Redis Compatibility:**

This matches Redis 7.0+ behavior where `redis.setresp()`, `redis.set_repl()`, and user-defined globals do NOT persist across script calls.

### Script Determinism for Replication

Scripts must produce deterministic results for correct replication. FrogDB ensures this by replicating **effects**, not the script itself.

**Replication Model:**
```
Primary:                          Replica:
EVAL "INCR counter" 1 counter
  → counter = 1
  → WAL: SET counter 1            → Receives: SET counter 1
  → +OK                           → counter = 1
```

**Why Effects, Not Scripts?**
- Scripts may use non-deterministic functions (time, random)
- Replica would get different results re-executing same script
- Effect replication ensures exact state synchronization

**Handled Non-Deterministic Sources:**

| Source | Handling |
|--------|----------|
| `redis.call('TIME')` | Returns current time (allowed - effects replicated) |
| `os.time()` | **Forbidden** - returns error |
| `os.clock()` | **Forbidden** - returns error |
| `math.random()` | Seeded deterministically per-execution |
| `math.randomseed()` | **Forbidden** - returns error |

**Random Number Handling:**
```lua
-- math.random() is available but seeded deterministically
-- Seed = hash(script_sha + sequence_number)
local val = math.random(1, 100)  -- Same result given same inputs
```

**Forbidden Functions:**

Calling these functions returns an error:
```
-ERR This script uses a non-deterministic function. Please check your script.
```

Forbidden: `os.time()`, `os.clock()`, `os.date()`, `math.randomseed()`, `loadfile()`, `dofile()`

**Allowed Time Access:**
```lua
-- Use redis.call to get time (effects will be replicated)
local time = redis.call('TIME')  -- Returns {seconds, microseconds}
```

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
| Scope | Per-node (each node has independent cache) |
| Persistence | Volatile (lost on restart) |
| Auto-sync | None (scripts don't auto-propagate between nodes) |
| Replica caches | Independent (replicas build their own cache via direct client EVAL/SCRIPT LOAD calls) |

**Clarification:** "Not replicated" means scripts are not automatically synchronized between nodes.
Replicas CAN and DO cache scripts when clients execute `EVAL` or `SCRIPT LOAD` on them directly.
The primary's script cache is NOT copied to replicas via replication.

### NOSCRIPT Error

When `EVALSHA` references a script not in the local cache:

```
> EVALSHA abc123... 1 mykey
-NOSCRIPT No matching script. Please use EVAL.
```

### Script Cache Management

The script cache has size limits and eviction policies to prevent unbounded memory growth.

**Configuration:**
```toml
[scripting]
# Maximum number of cached scripts
lua_script_cache_max_size = 10000       # 10,000 scripts (default)

# Maximum total bytecode size
lua_script_cache_max_bytes = 104857600  # 100MB (default)
```

**Eviction Policy:**

When the cache exceeds limits, scripts are evicted using LRU (Least Recently Used):

```rust
struct ScriptCache {
    scripts: LinkedHashMap<[u8; 20], CachedScript>,  // LRU order
    total_bytes: usize,
    max_scripts: usize,
    max_bytes: usize,
}

impl ScriptCache {
    fn insert(&mut self, sha: [u8; 20], script: CachedScript) {
        let script_size = script.bytecode.len();

        // Evict until we have room
        while self.scripts.len() >= self.max_scripts
            || self.total_bytes + script_size > self.max_bytes
        {
            if let Some((_, evicted)) = self.scripts.pop_front() {
                self.total_bytes -= evicted.bytecode.len();
                metrics.script_cache_evictions.inc();
            }
        }

        self.scripts.insert(sha, script);
        self.total_bytes += script_size;
    }

    fn get(&mut self, sha: &[u8; 20]) -> Option<&CachedScript> {
        // Move to back (most recently used)
        self.scripts.get_refresh(sha)
    }
}
```

**SCRIPT FLUSH Semantics:**

```
SCRIPT FLUSH [ASYNC | SYNC]
```

| Mode | Behavior |
|------|----------|
| `SYNC` (default) | Blocks until all scripts removed |
| `ASYNC` | Returns immediately, eviction in background |

**SCRIPT FLUSH Flow:**
```rust
fn script_flush(mode: FlushMode) {
    match mode {
        FlushMode::Sync => {
            // Block and clear all scripts
            for shard in shards.iter_mut() {
                shard.script_cache.clear();
            }
        }
        FlushMode::Async => {
            // Schedule background cleanup
            for shard in shards.iter() {
                shard.schedule_task(ScriptCacheClear);
            }
        }
    }
}
```

**Monitoring:**
```
frogdb_script_cache_size         # Current number of cached scripts
frogdb_script_cache_bytes        # Total bytecode size
frogdb_script_cache_evictions    # Scripts evicted due to limits
frogdb_script_cache_hits         # EVALSHA cache hits
frogdb_script_cache_misses       # EVALSHA cache misses (NOSCRIPT)
```

### SCRIPT Commands Reference

| Command | Description |
|---------|-------------|
| `SCRIPT LOAD <script>` | Load script, return SHA1 |
| `SCRIPT EXISTS <sha1> [sha1...]` | Check if scripts are cached |
| `SCRIPT FLUSH [ASYNC\|SYNC]` | Clear all cached scripts |
| `SCRIPT KILL` | Terminate running script (if no writes) |
| `SCRIPT DEBUG <YES\|SYNC\|NO>` | Set debug mode (if enabled) |

---

## Scripts and Transactions (MULTI/EXEC)

Scripts interact with MULTI/EXEC transactions in specific ways.

### EVAL Inside MULTI

`EVAL` and `EVALSHA` can be queued inside a `MULTI` block (Redis-compatible):

```
MULTI
+OK
SET key1 val1
+QUEUED
EVAL "return redis.call('GET', KEYS[1])" 1 key1
+QUEUED
SET key2 val2
+QUEUED
EXEC
*3
+OK
$4
val1
+OK
```

**Behavior:**
- Script is queued like any other command
- At EXEC time, commands execute in order
- Script executes atomically within the transaction
- All commands (including script) commit together

### MULTI Forbidden Inside Scripts

Scripts cannot start their own transactions:

```lua
-- This will ERROR:
redis.call('MULTI')    -- -ERR MULTI calls not allowed inside scripts
redis.call('SET', KEYS[1], 'val')
redis.call('EXEC')
```

**Why?**
- Scripts already execute atomically (implicit transaction)
- Nested transactions are undefined behavior
- Would complicate transaction ordering

**Workaround:**

Scripts are already atomic. Use the script itself as the transaction boundary:

```lua
-- All operations here are atomic
redis.call('SET', KEYS[1], 'val1')
redis.call('SET', KEYS[2], 'val2')
redis.call('SET', KEYS[3], 'val3')
-- Entire script is atomic, no MULTI needed
```

### Script Errors in Transaction

If a script errors during EXEC:

```
MULTI
SET key1 val1
EVAL "return redis.call('INVALID_CMD')" 0
SET key2 val2
EXEC
*3
+OK
-ERR Unknown command 'INVALID_CMD'
+OK
```

**Key Point:** Other commands in the transaction still execute (Redis-compatible). Script errors do not abort the entire transaction.

### WATCH and Scripts

`WATCH` works with scripts as expected:

```
WATCH mykey
GET mykey        # Returns "old"
MULTI
EVAL "return redis.call('SET', KEYS[1], 'new')" 1 mykey
EXEC
```

| Scenario | Result |
|----------|--------|
| mykey not modified by others | Script executes, returns result |
| mykey modified by others | EXEC returns nil, script not executed |

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
