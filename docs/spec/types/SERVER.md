# FrogDB Server Commands

Server administration commands for configuration, monitoring, and debugging. These commands operate at the server level rather than on specific keys.

## Data Structure

N/A - Server commands do not operate on data types.

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| CONFIG GET | O(N) | Get configuration parameters |
| CONFIG SET | O(1) | Set configuration parameter |
| CONFIG RESETSTAT | O(1) | Reset server statistics |
| CONFIG HELP | O(1) | Show CONFIG subcommands |
| INFO | O(1) | Get server information |
| BGSAVE | O(1) | Trigger background snapshot |
| LASTSAVE | O(1) | Get timestamp of last successful save |
| DEBUG SLEEP | O(1) | Sleep for duration |
| DEBUG OBJECT | O(1) | Inspect key internals |
| DEBUG STRUCTSIZE | O(1) | Show struct sizes |
| DEBUG RELOAD | O(N) | Reload server **[Not Planned]** (intentionally omitted) |

---

## Command Details

### CONFIG

The CONFIG command manages server configuration at runtime.

| Subcommand | Description |
|------------|-------------|
| `CONFIG GET pattern` | Get parameters matching glob pattern |
| `CONFIG SET param value` | Set parameter at runtime (mutable only) |
| `CONFIG RESETSTAT` | Reset server statistics |
| `CONFIG HELP` | Show CONFIG subcommands |

**Note:** `CONFIG REWRITE` is not supported. Runtime changes are transient and lost on restart.

#### CONFIG GET

Get configuration parameters matching a glob pattern.

```
CONFIG GET pattern
```

**Examples:**
```
CONFIG GET *              # All parameters
CONFIG GET max*           # Params starting with "max"
CONFIG GET *memory*       # Params containing "memory"
```

Returns array of alternating parameter names and values. Both mutable and immutable parameters are returned.

#### CONFIG SET

Set a mutable configuration parameter at runtime.

```
CONFIG SET param value
```

**Examples:**
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

See [CONFIGURATION.md](../CONFIGURATION.md) for complete parameter list with mutability.

#### CONFIG RESETSTAT

Reset server statistics counters.

```
CONFIG RESETSTAT
```

Returns: `OK`

Resets statistics like commands processed, connections received, etc.

---

### INFO

Returns server statistics and information.

```
INFO [section]
```

**Sections:** server, clients, memory, persistence, stats, replication, cpu, cluster, keyspace, all (default)

**Example:**
```
> INFO server
# Server
redis_version:7.0.0
frogdb_version:0.1.0
os:Linux 5.15.0 x86_64
arch_bits:64
process_id:1234
tcp_port:6379
uptime_in_seconds:3600
uptime_in_days:0
```

| Section | Information |
|---------|-------------|
| server | Version, uptime, process info |
| clients | Connected clients, blocked clients |
| memory | Used memory, peak memory, fragmentation |
| persistence | RDB/AOF status, last save time |
| stats | Total connections, commands processed |
| replication | Role, connected replicas, repl offset |
| cpu | System and user CPU usage |
| cluster | Cluster enabled, known nodes |
| keyspace | Per-database key counts and expirations |

See [OBSERVABILITY.md](../OBSERVABILITY.md#info) for detailed output format.

---

### DEBUG

Administrative commands for debugging. **Warning:** DEBUG commands are for development/debugging only.

| Command | Description |
|---------|-------------|
| `DEBUG SLEEP <seconds>` | Sleep for duration (blocks the connection) |
| `DEBUG OBJECT <key>` | Inspect key internals |
| `DEBUG STRUCTSIZE` | Show struct sizes |
| `DEBUG RELOAD` | Reload server **[Not Planned]** (intentionally omitted) |

#### DEBUG SLEEP

Block the connection for a specified duration.

```
DEBUG SLEEP <seconds>
```

**Example:**
```
DEBUG SLEEP 5
```

Returns: `OK` after sleeping.

#### DEBUG OBJECT

Inspect internal details of a key.

```
DEBUG OBJECT <key>
```

**Example:**
```
> DEBUG OBJECT mykey
Value at:0x7f... refcount:1 encoding:embstr serializedlength:6 lru:12345 lru_seconds_idle:10
```

Returns information about:
- Memory address
- Reference count
- Internal encoding
- Serialized length
- LRU clock value
- Idle time

#### DEBUG STRUCTSIZE

Show sizes of internal data structures.

```
DEBUG STRUCTSIZE
```

Returns: Various struct sizes for memory debugging.

#### DEBUG RELOAD

Reload the server (re-read configuration, flush data if configured).

```
DEBUG RELOAD
```

**Warning:** This may cause data loss depending on configuration.

---

## Cross-Shard Behavior

Server commands operate at the server/node level, not on individual shards:

| Command | Behavior |
|---------|----------|
| CONFIG GET/SET | Affects server-wide configuration |
| INFO | Aggregates stats across all shards |
| DEBUG | Server-level debugging |

---

## Persistence

Server commands do not directly affect persistence. CONFIG changes are transient (not persisted to config file).

---

## Security

Server commands may require authentication and authorization:

| Command | ACL Category |
|---------|--------------|
| CONFIG GET | `@admin @slow @dangerous` |
| CONFIG SET | `@admin @slow @dangerous` |
| INFO | `@slow @dangerous` |
| DEBUG | `@admin @slow @dangerous` |

See [AUTH.md](../AUTH.md) for ACL configuration.
