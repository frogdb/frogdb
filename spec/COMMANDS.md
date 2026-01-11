# FrogDB Command Reference

Index of command groups and cross-cutting execution concerns.

## Command Groups

| Group | Description | File |
|-------|-------------|------|
| Generic | Key management (DEL, EXISTS, EXPIRE, TTL, TYPE, SCAN) | [GENERIC.md](command-groups/GENERIC.md) |
| String | Binary-safe strings (GET, SET, INCR, APPEND) | [STRING.md](command-groups/STRING.md) |
| Sorted Set | Score-ordered sets (ZADD, ZRANGE, ZSCORE, ZRANK) | [SORTED_SET.md](command-groups/SORTED_SET.md) |
| Hash | Field-value maps (HGET, HSET, HGETALL) | *Future* |
| List | Ordered sequences (LPUSH, RPUSH, LRANGE) | *Future* |
| Set | Unique collections (SADD, SMEMBERS, SINTER) | *Future* |
| Stream | Append-only log (XADD, XREAD, XREADGROUP) | [STREAM.md](command-groups/STREAM.md) |
| Pub/Sub | Messaging (SUBSCRIBE, PUBLISH) | [PUBSUB.md](PUBSUB.md) |
| Scripting | Lua scripts (EVAL, EVALSHA) | [SCRIPTING.md](SCRIPTING.md) |
| Connection | Client management (AUTH, PING, QUIT) | [CONNECTION.md](CONNECTION.md) |
| Server | Administration (INFO, CONFIG, DEBUG) | [OPERATIONS.md](OPERATIONS.md) |
| Cluster | Distribution (CLUSTER NODES, CLUSTER SLOTS) | [CLUSTER.md](CLUSTER.md) |

---

## Size Limits

FrogDB enforces Redis-compatible size limits on command input. See [LIMITS.md](LIMITS.md) for complete limit documentation including:

- Max key/value sizes (512 MB default)
- Collection element limits
- Protocol-level enforcement
- Error messages

---

## Transactions

FrogDB supports MULTI/EXEC transactions with optimistic locking via WATCH. See [TRANSACTIONS.md](TRANSACTIONS.md) for:

- MULTI/EXEC command flow
- Cross-shard restrictions
- WATCH implementation
- Transaction durability by mode
- Pipelining vs transactions comparison

---

## DUMP and RESTORE

The DUMP and RESTORE commands serialize/deserialize keys for migration:

### DUMP

```
DUMP key
```

Returns a serialized representation of the value including type, TTL, and LFU counter.
Returns nil if key doesn't exist.

### RESTORE

```
RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
```

Deserialize a value (from DUMP) and store it at key.

| Argument | Description |
|----------|-------------|
| `key` | Destination key |
| `ttl` | Time-to-live in milliseconds (0 = no expiry) |
| `serialized-value` | Output from DUMP command |
| `REPLACE` | Overwrite if key exists (default: error if exists) |
| `ABSTTL` | Interpret `ttl` as absolute Unix timestamp (ms) |
| `IDLETIME seconds` | Set initial idle time for LRU eviction |
| `FREQ frequency` | Set initial LFU counter (0-255) |

**LRU/LFU Modifiers (Redis 5.0+):**

Since LRU timestamps are not persisted, migration tools can use `IDLETIME` and `FREQ`
to preserve eviction metadata:

```
# Restore with 60 seconds idle time (for LRU)
RESTORE mykey 0 "\x00..." REPLACE IDLETIME 60

# Restore with LFU frequency counter
RESTORE mykey 0 "\x00..." REPLACE FREQ 100
```

**Use Cases:**
- Cluster slot migration (keys retain eviction metadata)
- Backup/restore tools that preserve eviction behavior
- Manual data migration between instances

See [EVICTION.md](EVICTION.md#post-recovery-eviction-behavior) for how eviction metadata behaves after recovery.

---

## Server Commands

### CONFIG

The CONFIG command manages server configuration at runtime.

| Subcommand | Description |
|------------|-------------|
| `CONFIG GET pattern` | Get parameters matching glob pattern |
| `CONFIG SET param value` | Set parameter at runtime (mutable only) |
| `CONFIG RESETSTAT` | Reset server statistics (future) |
| `CONFIG HELP` | Show CONFIG subcommands (future) |

**Note:** `CONFIG REWRITE` is not supported. Runtime changes are transient and lost on restart.

#### CONFIG GET

```
CONFIG GET *              # All parameters
CONFIG GET max*           # Params starting with "max"
CONFIG GET *memory*       # Params containing "memory"
```

Returns array of alternating parameter names and values. Both mutable and immutable parameters are returned.

#### CONFIG SET

```
CONFIG SET maxmemory 8GB
CONFIG SET maxmemory-policy allkeys-lru
CONFIG SET loglevel debug
```

Only mutable parameters can be changed. Immutable parameters (bind, port, num_shards, etc.) return an error.

**Errors:**
- `ERR Unknown configuration parameter 'foo'`
- `ERR CONFIG SET parameter 'bind' is immutable`
- `ERR Invalid argument 'xyz' for CONFIG SET 'maxmemory'`

See [CONFIGURATION.md](CONFIGURATION.md) for complete parameter list with mutability.

### INFO

Returns server statistics and information.

```
INFO [section]
```

**Sections:** server, clients, memory, persistence, stats, replication, cpu, cluster, keyspace, all (default)

See [OPERATIONS.md](OPERATIONS.md#info-command) for output format.

### DEBUG

Administrative commands for debugging. Requires admin privileges.

| Command | Description |
|---------|-------------|
| `DEBUG SLEEP <seconds>` | Sleep for duration |
| `DEBUG OBJECT <key>` | Inspect key internals |
| `DEBUG STRUCTSIZE` | Show struct sizes |
| `DEBUG RELOAD` | Reload server |

**Warning:** DEBUG commands are for development/debugging only.

---

## Command Implementation Notes

### Command Registry

```rust
lazy_static! {
    static ref COMMANDS: HashMap<&'static str, Box<dyn Command>> = {
        let mut m = HashMap::new();
        m.insert("GET", Box::new(GetCommand));
        m.insert("SET", Box::new(SetCommand));
        // ... register all commands
        m
    };
}

fn dispatch(cmd: &ParsedCommand) -> Result<Response, Error> {
    let handler = COMMANDS.get(cmd.name.to_uppercase().as_str())
        .ok_or(Error::UnknownCommand)?;
    handler.execute(cmd)
}
```

### Common Patterns

**Read Command:**
```rust
impl Command for GetCommand {
    fn execute(&self, ctx: &CommandContext, args: &[Bytes]) -> Result<Response> {
        let key = &args[0];
        match ctx.store.get(key)? {
            Some(FrogValue::String(s)) => Ok(Response::Bulk(s.data.clone())),
            Some(_) => Err(Error::WrongType),
            None => Ok(Response::Null),
        }
    }
}
```

**Write Command:**
```rust
impl Command for SetCommand {
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response> {
        let key = args[0].clone();
        let value = args[1].clone();

        // Parse options (EX, PX, NX, XX)
        let opts = parse_set_options(&args[2..])?;

        // Check NX/XX conditions
        if !check_conditions(ctx.store, &key, &opts) {
            return Ok(Response::Null);
        }

        // Set value
        ctx.store.set(key.clone(), FrogValue::String(FrogString { data: value }));

        // Set expiry if specified
        if let Some(ttl) = opts.ttl {
            ctx.store.set_expiry(&key, ttl);
        }

        Ok(Response::Ok)
    }
}
```

---

## Future Commands

### Cluster (Future)

See [CLUSTER.md](CLUSTER.md) for full clustering architecture.

**Topology Commands:**

| Command | Description |
|---------|-------------|
| `CLUSTER NODES` | Full cluster state in Redis format |
| `CLUSTER SLOTS` | Get slot→node mapping (array format) |
| `CLUSTER SHARDS` | Get slot→node mapping (dict format, newer) |
| `CLUSTER INFO` | Cluster status summary |
| `CLUSTER KEYSLOT <key>` | Get slot for key |
| `CLUSTER COUNTKEYSINSLOT <slot>` | Count keys in slot |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | Get keys for migration |

**Client Commands:**

| Command | Description |
|---------|-------------|
| `READONLY` | Enable reads from replica |
| `READWRITE` | Disable replica reads (default) |
| `ASKING` | Allow next command during slot import |

**Failover Commands:**

| Command | Description |
|---------|-------------|
| `CLUSTER FAILOVER [FORCE]` | Manual failover (replica only) |
| `CLUSTER REPLICATE <node-id>` | Make this node replica of another |

**Redirections:**

| Response | Meaning | Client Action |
|----------|---------|---------------|
| `-MOVED <slot> <host>:<port>` | Wrong node | Update slot map, retry |
| `-ASK <slot> <host>:<port>` | Slot migrating | Send ASKING, then retry (once) |

### Modules (Future)

| Command | Description |
|---------|-------------|
| MODULE LOAD | Load module |
| MODULE LIST | List modules |
| MODULE UNLOAD | Unload module |

---

## References

- [EXECUTION.md](EXECUTION.md) - Command trait, arity, flags
- [DATA_TYPES.md](DATA_TYPES.md) - Supported data types
- [LIMITS.md](LIMITS.md) - Size limits and enforcement
- [TRANSACTIONS.md](TRANSACTIONS.md) - MULTI/EXEC and pipelining
- [CONFIGURATION.md](CONFIGURATION.md) - CONFIG commands and runtime configuration
- [command-groups/](command-groups/) - Type-specific commands
