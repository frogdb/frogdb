# FrogDB Operations Guide

This document covers configuration, observability, security, and debugging for FrogDB.

## Configuration

### Overview

FrogDB uses [Figment](https://docs.rs/figment) for hierarchical configuration with the following priority (highest to lowest):

1. Command-line arguments
2. Environment variables (prefix: `FROGDB_`)
3. Configuration file (TOML)
4. Built-in defaults

### Configuration File

Default location: `./frogdb.toml` or specified via `--config path/to/config.toml`

**Example configuration:**

```toml
# frogdb.toml

[server]
bind = "0.0.0.0"
port = 6379
num_shards = 0  # 0 = auto-detect CPU cores

[memory]
max_memory = 0  # 0 = unlimited (bytes)
# max_memory = "4GB"  # Human-readable also supported

[persistence]
enabled = true
data_dir = "./data"
durability_mode = "periodic"  # async, periodic, sync

[persistence.periodic]
sync_ms = 100
sync_writes = 1000

[persistence.snapshot]
enabled = true
interval_s = 3600  # 1 hour

[expiry]
active_hz = 10
cycle_ms = 1

[timeouts]
scatter_gather_ms = 1000
client_idle_s = 0  # 0 = no timeout

[logging]
level = "info"  # trace, debug, info, warn, error
format = "pretty"  # pretty, json
file = ""  # Empty = stdout only

[metrics]
enabled = true
bind = "0.0.0.0"
port = 9090

[tracing]
enabled = false
otlp_endpoint = "http://localhost:4317"
service_name = "frogdb"
```

### Environment Variables

All settings can be overridden via environment variables:

```bash
# Server
FROGDB_SERVER__BIND=0.0.0.0
FROGDB_SERVER__PORT=6379
FROGDB_SERVER__NUM_SHARDS=8

# Memory
FROGDB_MEMORY__MAX_MEMORY=4294967296

# Persistence
FROGDB_PERSISTENCE__ENABLED=true
FROGDB_PERSISTENCE__DATA_DIR=/var/lib/frogdb
FROGDB_PERSISTENCE__DURABILITY_MODE=sync

# Logging
FROGDB_LOGGING__LEVEL=debug
FROGDB_LOGGING__FORMAT=json
```

### Command-Line Arguments

```bash
frogdb-server [OPTIONS]

Options:
  -c, --config <FILE>    Configuration file path
  -b, --bind <ADDR>      Bind address [default: 127.0.0.1]
  -p, --port <PORT>      Listen port [default: 6379]
  -s, --shards <N>       Number of shards [default: num_cpus]
  -m, --max-memory <N>   Memory limit in bytes
  -d, --data-dir <PATH>  Data directory
  -l, --log-level <LVL>  Log level (trace/debug/info/warn/error)
  -h, --help             Print help
  -V, --version          Print version
```

### Configuration Struct

```rust
use figment::{Figment, providers::{Format, Toml, Env, Serialized}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    pub persistence: PersistenceConfig,
    pub expiry: ExpiryConfig,
    pub timeouts: TimeoutConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub tracing: TracingConfig,
}

impl Config {
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(Toml::file("frogdb.toml"))
            .merge(Env::prefixed("FROGDB_").split("__"))
            .extract()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: "127.0.0.1".into(),
                port: 6379,
                num_shards: 0,
            },
            memory: MemoryConfig {
                max_memory: 0,
            },
            // ... other defaults
        }
    }
}
```

---

## Observability

### Metrics (Prometheus)

FrogDB exposes Prometheus-compatible metrics at `/metrics` endpoint.

#### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_connections_total` | Counter | Total connections accepted |
| `frogdb_connections_current` | Gauge | Current active connections |
| `frogdb_connections_rejected` | Counter | Rejected connections (max clients) |
| `frogdb_blocked_clients` | Gauge | Clients blocked on BLPOP etc. |

#### Memory Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_memory_used_bytes` | Gauge | Memory used by data |
| `frogdb_memory_peak_bytes` | Gauge | Peak memory usage |
| `frogdb_memory_rss_bytes` | Gauge | Resident set size |
| `frogdb_memory_fragmentation_ratio` | Gauge | RSS / used ratio |

#### Command Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_commands_total` | Counter | Total commands processed |
| `frogdb_commands_duration_seconds` | Histogram | Command latency |
| `frogdb_commands_by_type` | Counter | Commands per type (GET, SET, etc.) |

#### Keyspace Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_keys_total` | Gauge | Total keys in database |
| `frogdb_keys_with_expiry` | Gauge | Keys with TTL set |
| `frogdb_keyspace_hits` | Counter | Successful key lookups |
| `frogdb_keyspace_misses` | Counter | Failed key lookups |
| `frogdb_expired_keys` | Counter | Keys expired (lazy + active) |
| `frogdb_evicted_keys` | Counter | Keys evicted (memory pressure) |

#### Per-Shard Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_shard_keys` | Gauge | `shard` | Keys per shard |
| `frogdb_shard_memory_bytes` | Gauge | `shard` | Memory per shard |
| `frogdb_shard_commands` | Counter | `shard` | Commands per shard |

#### Persistence Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_wal_writes_total` | Counter | WAL entries written |
| `frogdb_wal_bytes_total` | Counter | WAL bytes written |
| `frogdb_snapshot_in_progress` | Gauge | Snapshot currently running |
| `frogdb_snapshot_duration_seconds` | Histogram | Snapshot duration |
| `frogdb_last_snapshot_timestamp` | Gauge | Unix timestamp of last snapshot |

### Tracing (OpenTelemetry)

FrogDB integrates with OpenTelemetry for distributed tracing.

#### Setup

```rust
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::prelude::*;

fn init_tracing(config: &TracingConfig) {
    if !config.enabled {
        return;
    }

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&config.otlp_endpoint)
        )
        .with_trace_config(
            opentelemetry::sdk::trace::config()
                .with_resource(Resource::new(vec![
                    KeyValue::new("service.name", config.service_name.clone())
                ]))
        )
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Failed to initialize tracer");

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(telemetry)
        .init();
}
```

#### Span Structure

```
frogdb.command (root span)
├── name: "SET"
├── attributes:
│   ├── db.system: "frogdb"
│   ├── db.operation: "SET"
│   ├── db.key: "user:123"  (if enabled)
│   └── frogdb.shard: 3
│
└── frogdb.shard.execute (child span, if cross-shard)
    ├── attributes:
    │   ├── frogdb.shard: 5
    │   └── frogdb.message_type: "Execute"
    └── ...
```

### Logging

FrogDB uses structured logging via the `tracing` crate.

#### Log Levels

| Level | Description |
|-------|-------------|
| ERROR | Errors that may require intervention |
| WARN | Degraded operation, but recoverable |
| INFO | Significant events (startup, shutdown, snapshots) |
| DEBUG | Detailed operation (commands, connections) |
| TRACE | Very detailed (protocol bytes, internal state) |

#### Log Format

**Pretty (development):**
```
2025-01-07T12:34:56.789Z  INFO frogdb::server: Server listening on 0.0.0.0:6379
2025-01-07T12:34:57.123Z DEBUG frogdb::connection: New connection from 192.168.1.10:54321
```

**JSON (production):**
```json
{"timestamp":"2025-01-07T12:34:56.789Z","level":"INFO","target":"frogdb::server","message":"Server listening","bind":"0.0.0.0","port":6379}
```

---

## Security

### Authentication

#### Simple Password (requirepass)

```toml
[security]
requirepass = "supersecret"
```

Clients must issue AUTH before other commands:
```
AUTH supersecret
+OK
```

#### ACL System (Future)

Redis 6+ compatible access control lists.

**User Management:**
```
ACL SETUSER alice on >password123 ~user:* +@read +@write -@dangerous
ACL SETUSER readonly on >readpass ~* +@read -@write
ACL DELUSER alice
ACL LIST
```

**Rule Syntax:**
| Rule | Description |
|------|-------------|
| `on` / `off` | Enable/disable user |
| `>password` | Add password (hashed with SHA256) |
| `nopass` | Allow passwordless auth |
| `~pattern` | Allow key pattern |
| `%R~pattern` | Read-only key pattern |
| `%W~pattern` | Write-only key pattern |
| `+command` | Allow command |
| `-command` | Deny command |
| `+@category` | Allow command category |
| `-@category` | Deny command category |
| `&pattern` | Allow pub/sub channel pattern |

**Command Categories:**
- `@read` - Read commands
- `@write` - Write commands
- `@fast` - O(1) commands
- `@slow` - O(N) commands
- `@dangerous` - Admin commands (DEBUG, CONFIG, etc.)
- `@pubsub` - Pub/Sub commands
- `@scripting` - Lua scripting

**AUTH Command:**
```
# Legacy (password only, authenticates as 'default')
AUTH password

# Redis 6+ (username and password)
AUTH username password
```

**Storage:**
- Passwords stored as SHA256 hashes
- ACL rules persisted to `aclfile` (configurable)
- Runtime changes via ACL SAVE / ACL LOAD

### TLS (Future)

```toml
[tls]
enabled = true
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
ca_file = "/path/to/ca.pem"  # For client cert verification
```

### Network Security

- Bind to localhost by default (`127.0.0.1`)
- Use firewall rules to restrict access
- Consider VPN or SSH tunneling for remote access

---

## Debugging

### INFO Command

Returns server statistics:

```
INFO [section]
```

**Sections:**
- `server` - Version, uptime, process info
- `clients` - Connection statistics
- `memory` - Memory usage details
- `persistence` - RDB/AOF status
- `stats` - Command statistics
- `replication` - Replication status (future)
- `cpu` - CPU usage
- `cluster` - Cluster status (future)
- `keyspace` - Database statistics
- `all` - All sections (default)

**Example output:**
```
# Server
frogdb_version:0.1.0
uptime_in_seconds:3600
uptime_in_days:0
process_id:12345

# Clients
connected_clients:42
blocked_clients:0

# Memory
used_memory:104857600
used_memory_human:100.00M
used_memory_peak:157286400
used_memory_peak_human:150.00M

# Stats
total_connections_received:1000
total_commands_processed:500000
keyspace_hits:450000
keyspace_misses:50000
```

### SLOWLOG

Records commands exceeding a time threshold.

```
# Configure threshold (microseconds)
CONFIG SET slowlog-log-slower-than 10000  # 10ms

# Configure max entries
CONFIG SET slowlog-max-len 128

# View slow log
SLOWLOG GET [count]

# Get log length
SLOWLOG LEN

# Clear log
SLOWLOG RESET
```

**Output:**
```
1) 1) (integer) 14            # ID
   2) (integer) 1638360000    # Timestamp
   3) (integer) 15234         # Execution time (μs)
   4) 1) "KEYS"               # Command
      2) "*"
   5) "192.168.1.10:54321"    # Client
   6) ""                       # Client name
```

### CLIENT Commands

```
# List connected clients
CLIENT LIST

# Kill client
CLIENT KILL ID 123

# Set client name
CLIENT SETNAME myapp

# Get client name
CLIENT GETNAME

# Get client ID
CLIENT ID

# Pause all clients
CLIENT PAUSE 5000  # 5 seconds

# Resume clients
CLIENT UNPAUSE
```

### DEBUG Commands

**Warning:** These commands are for debugging only. Not for production use.

| Command | Description |
|---------|-------------|
| `DEBUG SLEEP <seconds>` | Sleep for specified duration |
| `DEBUG SEGFAULT` | Crash the server |
| `DEBUG OBJECT <key>` | Inspect key internals |
| `DEBUG STRUCTSIZE` | Show struct sizes |
| `DEBUG RELOAD` | Reload server |

### MEMORY Commands

```
# Overall memory report
MEMORY STATS

# Memory for specific key
MEMORY USAGE <key> [SAMPLES count]

# Memory recommendations
MEMORY DOCTOR

# Memory allocator stats
MEMORY MALLOC-SIZE <size>
```

### LATENCY Commands (Future)

```
# Enable latency monitoring
CONFIG SET latency-monitor-threshold 100  # 100ms

# View latency history
LATENCY HISTORY <event>

# View latest latencies
LATENCY LATEST

# Reset latency data
LATENCY RESET

# Generate latency report
LATENCY DOCTOR
```

**Event Types:**
- `command` - Command execution
- `fork` - Fork operations (if used)
- `expire-cycle` - Expiry sweep
- `eviction-cycle` - Eviction sweep
- `aof-write` - AOF writes
- `rdb-save` - RDB saves

### Health Checks

**Liveness:**
```
PING
# Returns: PONG
```

**Readiness:**
```
# Check if server is ready to accept commands
INFO server
# Check loading status, cluster state, etc.
```

---

## Operational Best Practices

### Memory

1. Set `max_memory` to 80% of available RAM
2. Monitor `memory_fragmentation_ratio` - values > 1.5 indicate fragmentation
3. Enable eviction policies in production
4. Use `MEMORY DOCTOR` for recommendations

### Persistence

1. Use `periodic` durability for balance of safety and performance
2. Schedule snapshots during low-traffic periods
3. Monitor snapshot duration and size
4. Keep at least 2x disk space for snapshots

### Monitoring

1. Alert on `keyspace_misses / (keyspace_hits + keyspace_misses)` > 0.1
2. Alert on `memory_used / max_memory` > 0.9
3. Alert on p99 latency spikes
4. Monitor connection count trends

### Backup

1. Use snapshots for point-in-time recovery
2. Stream WAL to remote storage for continuous backup
3. Test restore procedures regularly
