---
title: "Configuration"
description: "FrogDB uses a layered configuration approach with TOML files, environment variables, CLI arguments, and runtime CONFIG commands."
sidebar:
  order: 1
---
FrogDB uses a layered configuration approach with TOML files, environment variables, CLI arguments, and runtime CONFIG commands.

## Configuration Priority

Configuration sources are applied in this order (highest to lowest priority):

1. **Command-line arguments** -- `--port 6379`
2. **Environment variables** -- `FROGDB_SERVER__PORT=6379`
3. **Configuration file** -- `frogdb.toml`
4. **Built-in defaults**

## Configuration File

Default location: `./frogdb.toml` or specified via `--config path/to/config.toml`

```toml
# frogdb.toml

[server]
bind = "127.0.0.1"
port = 6379
num-shards = 1  # Number of shards (increase for multi-core throughput)
allow-cross-slot-standalone = false  # Enable atomic cross-shard operations

[memory]
max-memory = 0  # 0 = unlimited (bytes)
# max-memory = "4GB"  # Human-readable also supported
maxmemory-policy = "noeviction"

[persistence]
enabled = true
data-dir = "./data"           # Directory for data (WAL, SST files)
snapshot-dir = "./snapshots"  # Directory for point-in-time snapshots
durability-mode = "periodic"  # async, periodic, sync

[persistence.periodic]
fsync-interval-ms = 1000  # Fsync on fixed wall-clock schedule

[persistence.snapshot]
enabled = true
interval-s = 3600  # 1 hour

[timeouts]
client-idle-s = 0       # 0 = no timeout
tcp-keepalive-s = 300
scatter-gather-timeout-ms = 5000  # Timeout for multi-shard operations

[logging]
level = "info"  # trace, debug, info, warn, error
format = "pretty"  # pretty, json
slowlog-log-slower-than = 10000  # microseconds
slowlog-max-len = 128

[security]
requirepass = ""  # Empty = no auth required

[http]
enabled = true
bind = "127.0.0.1"
port = 9090
# token = "my-secret-token"

[tls]
enabled = false
cert-file = ""
key-file = ""
ca-file = ""
require-client-cert = false
```

## Environment Variables

All settings can be overridden via environment variables with `FROGDB_` prefix. Use double underscores (`__`) to separate nested keys.

```bash
# Server
FROGDB_SERVER__BIND=127.0.0.1
FROGDB_SERVER__PORT=6379
FROGDB_SERVER__NUM_SHARDS=8

# Memory
FROGDB_MEMORY__MAX_MEMORY=4294967296
FROGDB_MEMORY__MAXMEMORY_POLICY=allkeys-lru

# Persistence
FROGDB_PERSISTENCE__ENABLED=true
FROGDB_PERSISTENCE__DATA_DIR=/var/lib/frogdb

# Logging
FROGDB_LOGGING__LEVEL=debug
```

## Command-Line Arguments

```bash
frogdb-server [OPTIONS]

Options:
  -c, --config <FILE>    Configuration file path
  -b, --bind <ADDR>      Bind address [default: 127.0.0.1]
  -p, --port <PORT>      Listen port [default: 6379]
  -s, --shards <N>       Number of shards [default: 1]
  -m, --max-memory <N>   Memory limit in bytes
  -d, --data-dir <PATH>  Data directory
  -l, --log-level <LVL>  Log level (trace/debug/info/warn/error)
  -h, --help             Print help
  -V, --version          Print version
```

## Naming Conventions

| Context | Convention | Example |
|---------|------------|---------|
| TOML files | `kebab-case` | `max-memory`, `sync-interval-ms` |
| CONFIG GET/SET | Redis-compatible names | `maxmemory`, `slowlog-log-slower-than` |
| Environment vars | `SCREAMING_SNAKE_CASE` | `FROGDB_MEMORY__MAX_MEMORY` |
| CLI args | `kebab-case` | `--max-memory`, `--log-level` |

---

## Runtime Configuration (CONFIG Commands)

FrogDB supports Redis-compatible `CONFIG` commands for runtime changes.

| Command | Description |
|---------|-------------|
| `CONFIG GET pattern` | Get configuration parameters matching glob pattern |
| `CONFIG SET param value` | Set configuration parameter at runtime |
| `CONFIG RESETSTAT` | Reset statistics |
| `CONFIG HELP` | Show CONFIG subcommands |

`CONFIG REWRITE` is not supported. Runtime changes are transient and lost on restart.

### CONFIG GET

Returns all configuration parameters matching the glob pattern.

```
CONFIG GET *           # All parameters
CONFIG GET max*        # Parameters starting with "max"
CONFIG GET *memory*    # Parameters containing "memory"
```

### CONFIG SET

Sets a configuration parameter at runtime. Only mutable parameters can be changed.

```
CONFIG SET maxmemory 8589934592
CONFIG SET maxmemory-policy allkeys-lru
CONFIG SET loglevel debug
```

Errors:

| Error | Meaning |
|-------|---------|
| `ERR Unknown configuration parameter 'foo'` | Parameter does not exist |
| `ERR CONFIG SET parameter 'bind' is immutable` | Parameter cannot be changed at runtime |
| `ERR Invalid argument 'xyz' for CONFIG SET 'maxmemory'` | Value failed validation |

---

## Parameter Mutability

### Immutable Parameters (Require Restart)

| Parameter | Description |
|-----------|-------------|
| `bind` | Network bind address |
| `port` | Network listen port |
| `unixsocket` | Unix socket path |
| `num-shards` | Number of internal shards |
| `allow-cross-slot-standalone` | Enable atomic cross-shard operations |
| `dir` | Data directory path |
| `dbfilename` | Dump filename |
| `tls-cert-file` | TLS certificate file path |
| `tls-key-file` | TLS private key file path |
| `tls-ca-cert-file` | TLS CA certificate file path |

### Mutable Parameters (CONFIG SET)

| Parameter | Type | Description |
|-----------|------|-------------|
| `maxmemory` | bytes | Memory limit (0 = unlimited) |
| `maxmemory-policy` | enum | Eviction policy |
| `maxmemory-samples` | int | Keys to sample for eviction |
| `timeout` | seconds | Client idle timeout (0 = disabled) |
| `tcp-keepalive` | seconds | TCP keepalive interval |
| `loglevel` | enum | Log level (debug, info, warn, error) |
| `slowlog-log-slower-than` | microseconds | Slow log threshold |
| `slowlog-max-len` | int | Max slow log entries |
| `maxclients` | int | Max concurrent connections |
| `requirepass` | string | Authentication password |

### Complete Parameter Reference

**Server Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `bind` | No | `127.0.0.1` | N/A -- requires restart |
| `port` | No | `6379` | N/A -- requires restart |
| `admin-port` | No | `6380` | N/A -- requires restart |
| `maxclients` | Yes | `10000` | Immediate -- new connections rejected if over limit |
| `timeout` | Yes | `0` | Immediate -- applies to existing idle connections |
| `tcp-keepalive` | Yes | `300` | New connections only |
| `tcp-backlog` | No | `511` | N/A -- requires restart |

**Memory Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `maxmemory` | Yes | `0` | Immediate -- triggers eviction if exceeded |
| `maxmemory-policy` | Yes | `noeviction` | Immediate -- next eviction uses new policy |
| `maxmemory-samples` | Yes | `5` | Immediate -- next eviction uses new sample size |

**Persistence Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `dir` | No | `./data` | N/A -- requires restart |
| `durability-mode` | Yes | `periodic` | Immediate -- affects next write |
| `snapshot-interval-s` | Yes | `3600` | Reschedules next snapshot |

**Replication Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `repl-timeout-ms` | Yes | `60000` | Immediate -- applies to existing connections |
| `repl-backlog-size` | Yes | `1048576` | Immediate -- backlog resized |
| `min-replicas-to-write` | Yes | `0` | Immediate -- affects next write |

**Logging Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `loglevel` | Yes | `info` | Immediate -- affects all logging |
| `slowlog-log-slower-than` | Yes | `10000` | Immediate |
| `slowlog-max-len` | Yes | `128` | Immediate -- truncates if reduced |

**TLS Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `tls-cert-file` | No | `""` | N/A -- requires restart |
| `tls-key-file` | No | `""` | N/A -- requires restart |
| `tls-ca-cert-file` | No | `""` | N/A -- requires restart |
| `tls-port` | No | `0` | N/A -- requires restart |

### CONFIG SET Side Effects

When mutable parameters change, FrogDB takes immediate action:

- **`maxmemory` reduced**: If current usage exceeds new limit, triggers immediate eviction cycle.
- **`maxclients` reduced**: Existing connections are not killed, but new connections are rejected until under limit.
- **`slowlog-max-len` reduced**: Slow log is truncated immediately.
- **`repl-backlog-size` changed**: Ring buffer is resized (may truncate oldest entries if reduced).

### CONFIG Name Mapping

CONFIG GET/SET uses Redis-compatible parameter names, mapped to the TOML structure internally:

| CONFIG name | TOML path | Mutable |
|-------------|-----------|---------|
| `bind` | `server.bind` | No |
| `port` | `server.port` | No |
| `maxmemory` | `memory.max-memory` | Yes |
| `maxmemory-policy` | `memory.maxmemory-policy` | Yes |
| `timeout` | `timeouts.client-idle-s` | Yes |
| `slowlog-log-slower-than` | `logging.slowlog-log-slower-than` | Yes |
| `loglevel` | `logging.level` | Yes |
| `dir` | `persistence.data-dir` | No |

---

## Timeout Reference

### Connection Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Client idle | `timeout` | `0` (disabled) | Disconnect idle clients |
| TCP keepalive | `tcp-keepalive` | `300s` | OS-level keepalive interval |

### Operation Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Scatter-gather | `scatter-gather-timeout-ms` | `5000` | Total multi-shard operation time |
| Lua script | `lua-time-limit-ms` | `5000` | Max script execution time |
| Blocking command | Command argument | varies | BLPOP/BRPOP timeout |

### Persistence Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| WAL batch | `wal-batch-timeout-ms` | `10` | Max delay before WAL flush |

### Replication Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Replication connection | `repl-timeout-ms` | `60000` | Disconnect on no data |
| Replication ping | `repl-ping-interval-ms` | `10000` | Heartbeat frequency |
| Checkpoint transfer | `checkpoint-transfer-timeout-ms` | `300000` | Total FULLRESYNC time |
| Sync ACK | `sync-timeout-ms` | `1000` | Wait for replica ACK |

### Cluster Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Cluster bus | `cluster-bus-timeout-ms` | `5000` | Inter-node communication |
| Node timeout | `cluster-node-timeout-ms` | `15000` | Mark node as failing |
| Failover timeout | `cluster-failover-timeout-ms` | `5000` | Complete failover |

### Timeout Relationships

```
Client perspective:
  client-timeout (application)
       |
       +-- Must be > scatter-gather-timeout-ms
       |   (client should wait longer than server operation)
       |
       +-- Consider repl-timeout-ms for sync writes
           (WAIT command may take longer)

Operation perspective:
  scatter-gather-timeout-ms (5000ms)
       |
       +-- vll-lock-acquisition-timeout-ms (4000ms)
       |   Should be < scatter-gather to leave execution time
       |
       +-- Execution time (remaining ~1000ms)

Replication perspective:
  repl-timeout-ms (60000ms)
       |
       +-- repl-ping-interval-ms (10000ms)
       |   Should be < repl-timeout
       |
       +-- sync-timeout-ms (1000ms)
           Per-write ACK wait, much smaller than repl-timeout
```

### Timeout Units

| Suffix | Unit |
|--------|------|
| `-ms` | Milliseconds |
| `-s` | Seconds |
| No suffix | Seconds (legacy Redis compatibility) |
