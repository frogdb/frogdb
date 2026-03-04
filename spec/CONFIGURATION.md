# FrogDB Configuration

This document covers FrogDB's configuration system, including startup configuration, runtime configuration via CONFIG commands, and future TLS certificate hot-reloading.

## Overview

FrogDB uses a layered configuration approach:

1. **Startup configuration** via Figment (CLI > env vars > TOML file > defaults)
2. **Runtime configuration** via `CONFIG SET/GET` commands (Redis-compatible)
3. **TLS certificate hot-reloading** via file watching and signals (future)

### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Keep TOML format | Modern, structured, good Rust ecosystem support via Figment |
| Native env var support | `FROGDB_` prefix - more like DragonflyDB than Redis/Valkey |
| CONFIG SET/GET | Full Redis compatibility for runtime changes |
| No CONFIG REWRITE | Simpler implementation; runtime changes are transient |
| TLS hot-reload (future) | Differentiating feature for operational ease |

### Comparison with Prior Art

| Method | Redis | Valkey | DragonflyDB | FrogDB |
|--------|-------|--------|-------------|--------|
| Config file | `redis.conf` | `valkey.conf` | gflags | TOML |
| CLI args | `--port 6379` | `--port 6379` | `--port=6379` | `--port 6379` |
| Env vars | None (native) | None (native) | `DFLY_` prefix | `FROGDB_` prefix |
| CONFIG SET | Yes | Yes | Limited | Yes |
| CONFIG REWRITE | Yes | Yes | No | No |
| TLS hot-reload | No | No | No | Planned |

---

## Startup Configuration

### Configuration Priority

FrogDB uses [Figment](https://docs.rs/figment) for hierarchical configuration (highest to lowest priority):

1. **Command-line arguments** - `--port 6379`
2. **Environment variables** - `FROGDB_SERVER__PORT=6379`
3. **Configuration file** - `frogdb.toml`
4. **Built-in defaults**

### Configuration File

Default location: `./frogdb.toml` or specified via `--config path/to/config.toml`

```toml
# frogdb.toml

[server]
bind = "0.0.0.0"
port = 6379
num_shards = 0  # 0 = auto-detect CPU cores
allow_cross_slot_standalone = false  # Enable atomic cross-shard operations via VLL (see VLL.md)
# Note: This option only applies in standalone mode.
# In cluster mode, cross-slot operations always return -CROSSSLOT.

[memory]
max_memory = 0  # 0 = unlimited (bytes)
# max_memory = "4GB"  # Human-readable also supported
maxmemory_policy = "noeviction"

[persistence]
enabled = true
data_dir = "./data"           # Directory for RocksDB data (WAL, SST files)
snapshot_dir = "./snapshots"  # Directory for point-in-time snapshots
durability_mode = "periodic"  # async, periodic, sync

[persistence.periodic]
fsync_interval_ms = 1000  # Fsync on fixed wall-clock schedule (matches Redis appendfsync everysec)

[persistence.snapshot]
enabled = true
interval_s = 3600  # 1 hour

[timeouts]
client_idle_s = 0       # 0 = no timeout
tcp_keepalive_s = 300
scatter_gather_timeout_ms = 5000  # Timeout for all multi-shard operations (VLL + scatter-gather)

[logging]
level = "info"  # trace, debug, info, warn, error
format = "pretty"  # pretty, json
slowlog_log_slower_than = 10000  # microseconds
slowlog_max_len = 128

[security]
requirepass = ""  # Empty = no auth required

[metrics]
enabled = true
bind = "0.0.0.0"
port = 9090

[tls]
enabled = false
cert_file = ""
key_file = ""
ca_file = ""
require_client_cert = false
```

### Environment Variables

All settings can be overridden via environment variables with `FROGDB_` prefix:

```bash
# Server
FROGDB_SERVER__BIND=0.0.0.0
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

**Note:** Use double underscores (`__`) to separate nested keys.

### Naming Conventions

| Context | Convention | Example |
|---------|------------|---------|
| TOML files | `snake_case` | `max_memory`, `sync_interval_ms` |
| CONFIG GET/SET | Redis-compatible names | `maxmemory`, `slowlog-log-slower-than` |
| Environment vars | `SCREAMING_SNAKE_CASE` | `FROGDB_MEMORY__MAX_MEMORY` |
| CLI args | `kebab-case` | `--max-memory`, `--log-level` |

**Rationale:**
- CONFIG commands use Redis-compatible names for drop-in compatibility
- TOML uses idiomatic Rust/TOML `snake_case`
- Environment variables follow standard shell conventions

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

---

## Runtime Configuration

FrogDB supports Redis-compatible `CONFIG` commands for runtime configuration changes.

### CONFIG Commands

| Command | Description |
|---------|-------------|
| `CONFIG GET pattern` | Get configuration parameters matching glob pattern |
| `CONFIG SET param value` | Set configuration parameter at runtime |
| `CONFIG RESETSTAT` | Reset statistics |
| `CONFIG HELP` | Show CONFIG subcommands |

**Note:** `CONFIG REWRITE` is not supported. Runtime changes are transient and lost on restart.

### CONFIG GET

Returns all configuration parameters matching the glob pattern.

```
CONFIG GET *           # All parameters
CONFIG GET max*        # Parameters starting with "max"
CONFIG GET *memory*    # Parameters containing "memory"
```

**Response:** Array of alternating parameter names and values.

```
1) "maxmemory"
2) "4294967296"
3) "maxmemory-policy"
4) "noeviction"
```

CONFIG GET returns both mutable and immutable parameters. Immutable parameters are read-only.

### CONFIG SET

Sets a configuration parameter at runtime. Only mutable parameters can be changed.

```
CONFIG SET maxmemory 8589934592
CONFIG SET maxmemory-policy allkeys-lru
CONFIG SET loglevel debug
```

**Response:** `OK` on success, error otherwise.

**Errors:**

| Error | Meaning |
|-------|---------|
| `ERR Unknown configuration parameter 'foo'` | Parameter doesn't exist |
| `ERR CONFIG SET parameter 'bind' is immutable` | Parameter cannot be changed at runtime |
| `ERR Invalid argument 'xyz' for CONFIG SET 'maxmemory'` | Value failed validation |

---

## Parameter Mutability

### Immutable Parameters (Require Restart)

These parameters are fixed at startup and cannot be changed via CONFIG SET:

| Parameter | Description |
|-----------|-------------|
| `bind` | Network bind address |
| `port` | Network listen port |
| `unixsocket` | Unix socket path |
| `num_shards` | Number of internal shards |
| `allow_cross_slot_standalone` | Enable atomic cross-shard operations via VLL |
| `dir` | Data directory path |
| `dbfilename` | RDB dump filename |
| `tls-cert-file` | TLS certificate file path |
| `tls-key-file` | TLS private key file path |
| `tls-ca-cert-file` | TLS CA certificate file path |

### Mutable Parameters (CONFIG SET)

These parameters can be changed at runtime:

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

This table lists ALL configuration parameters with mutability and side effects.

**Server Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `bind` | No | `127.0.0.1` | N/A - requires restart |
| `port` | No | `6379` | N/A - requires restart |
| `admin_port` | No | `6380` | N/A - requires restart |
| `unixsocket` | No | `""` | N/A - requires restart |
| `maxclients` | Yes | `10000` | Immediate - new connections rejected if over limit |
| `timeout` | Yes | `0` | Immediate - applies to existing idle connections |
| `tcp-keepalive` | Yes | `300` | New connections only |
| `tcp-backlog` | No | `511` | N/A - requires restart |

**Memory Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `maxmemory` | Yes | `0` | Immediate - triggers eviction if exceeded |
| `maxmemory-policy` | Yes | `noeviction` | Immediate - next eviction uses new policy |
| `maxmemory-samples` | Yes | `5` | Immediate - next eviction uses new sample size |

**Persistence Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `dir` | No | `./data` | N/A - requires restart |
| `dbfilename` | No | `dump.fdb` | N/A - requires restart |
| `durability_mode` | Yes | `periodic` | Immediate - affects next write |
| `wal_batch_timeout_ms` | Yes | `10` | Immediate |
| `snapshot_interval_s` | Yes | `3600` | Reschedules next snapshot |

**Concurrency Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `num_shards` | No | `0` (auto) | N/A - requires restart |
| `allow_cross_slot_standalone` | No | `false` | N/A - requires restart |
| `scatter_gather_timeout_ms` | Yes | `5000` | Immediate - affects new operations |
| `vll_max_queue_depth` | Yes | `10000` | Immediate |

**Replication Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `repl_timeout_ms` | Yes | `60000` | Immediate - applies to existing connections |
| `repl_backlog_size` | Yes | `1048576` | Immediate - backlog resized |
| `min_replicas_to_write` | Yes | `0` | Immediate - affects next write |
| `sync_timeout_ms` | Yes | `1000` | Immediate |

**Logging Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `loglevel` | Yes | `info` | Immediate - affects all logging |
| `slowlog-log-slower-than` | Yes | `10000` | Immediate |
| `slowlog-max-len` | Yes | `128` | Immediate - truncates if reduced |

**TLS Parameters:**

| Parameter | Mutable | Default | Side Effects on Change |
|-----------|---------|---------|------------------------|
| `tls-cert-file` | No | `""` | N/A - requires restart |
| `tls-key-file` | No | `""` | N/A - requires restart |
| `tls-ca-cert-file` | No | `""` | N/A - requires restart |
| `tls-port` | No | `0` | N/A - requires restart |

### Immutability Rationale

| Parameter | Why Immutable |
|-----------|---------------|
| `bind`, `port` | Changing network addresses requires new socket bindings |
| `num_shards` | Data distribution is fixed at startup; changing requires data migration |
| `allow_cross_slot_standalone` | Affects command routing logic deeply embedded in execution |
| `dir`, `dbfilename` | Changing paths mid-operation would orphan existing data |
| `tls-*` | TLS contexts are expensive to recreate; requires connection termination |
| `tcp-backlog` | OS socket option set at bind time |
| `unixsocket` | Unix socket created at startup |

### CONFIG SET Side Effects

When mutable parameters change, FrogDB takes immediate action:

**`maxmemory` Reduced:**

```rust
fn on_maxmemory_change(new_value: u64) {
    let current_usage = memory_usage();
    if current_usage > new_value {
        // Trigger immediate eviction
        trigger_eviction_cycle();
    }
}
```

**`maxclients` Reduced:**

```rust
fn on_maxclients_change(new_value: u32) {
    // Don't kill existing connections
    // Just reject new connections until under limit
    // Log warning if currently over limit
    let current = connection_count();
    if current > new_value {
        warn!(
            "maxclients reduced to {} but {} connections active",
            new_value, current
        );
    }
}
```

**`slowlog-max-len` Reduced:**

```rust
fn on_slowlog_maxlen_change(new_value: u32) {
    // Truncate slowlog immediately
    while slowlog.len() > new_value as usize {
        slowlog.pop_back();
    }
}
```

**`repl_backlog_size` Changed:**

```rust
fn on_backlog_size_change(new_value: u64) {
    // Resize ring buffer
    // May truncate oldest entries if reduced
    repl_backlog.resize(new_value);
}
```

### Runtime Validation

CONFIG SET validates parameters before applying:

```rust
fn validate_config_set(param: &str, value: &str) -> Result<(), ConfigError> {
    match param {
        "maxmemory" => {
            let bytes = parse_bytes(value)?;
            if bytes > 0 && bytes < MIN_MAXMEMORY {
                return Err(ConfigError::Invalid(
                    format!("maxmemory must be >= {} or 0", MIN_MAXMEMORY)
                ));
            }
        }
        "maxmemory-policy" => {
            if !VALID_POLICIES.contains(&value) {
                return Err(ConfigError::Invalid(
                    format!("unknown eviction policy: {}", value)
                ));
            }
        }
        "loglevel" => {
            if !["debug", "info", "warn", "error"].contains(&value) {
                return Err(ConfigError::Invalid(
                    format!("unknown log level: {}", value)
                ));
            }
        }
        // ... other validations
        _ => {}
    }
    Ok(())
}
```

### Parameter Naming

CONFIG GET/SET uses Redis-compatible parameter names, mapped to TOML structure internally:

| CONFIG name | TOML path | Mutable |
|-------------|-----------|---------|
| `bind` | `server.bind` | No |
| `port` | `server.port` | No |
| `maxmemory` | `memory.max_memory` | Yes |
| `maxmemory-policy` | `memory.maxmemory_policy` | Yes |
| `timeout` | `timeouts.client_idle_s` | Yes |
| `slowlog-log-slower-than` | `logging.slowlog_log_slower_than` | Yes |
| `loglevel` | `logging.level` | Yes |
| `dir` | `persistence.data_dir` | No |
| `dbfilename` | `persistence.dbfilename` | No |

---

## Architecture

### Configuration System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration System                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    startup    ┌──────────────────────┐   │
│  │  frogdb.toml │ ──────────▶   │  StaticConfig        │   │
│  │  (Figment)   │               │  (immutable params)  │   │
│  └──────────────┘               └──────────────────────┘   │
│                                            │                 │
│  ┌──────────────┐                          ▼                │
│  │ CLI args     │ ──────────▶   ┌──────────────────────┐   │
│  │ Env vars     │               │  RuntimeConfig       │   │
│  └──────────────┘               │  (Arc<RwLock<_>>)    │   │
│                                 │  - mutable params    │   │
│                                 │  - CONFIG SET target │   │
│                                 └──────────────────────┘   │
│                                            │                 │
│                                            ▼                │
│                                 ┌──────────────────────┐   │
│                                 │  ConfigSnapshot      │   │
│                                 │  (cheap clone for    │   │
│                                 │   command handlers)  │   │
│                                 └──────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

#### Parameter Metadata Registry

Each config parameter has metadata defining its behavior:

```rust
pub struct ParamMeta {
    /// Parameter name (Redis-compatible, e.g., "maxmemory")
    pub name: &'static str,

    /// Can be changed at runtime via CONFIG SET
    pub mutable: bool,

    /// Parameter type for parsing/validation
    pub param_type: ParamType,

    /// Optional validator function
    pub validator: Option<fn(&str) -> Result<(), String>>,

    /// Help text for CONFIG HELP
    pub description: &'static str,
}

pub enum ParamType {
    String,
    Integer,
    Float,
    Bool,
    Bytes,      // "4GB", "512MB" etc.
    Duration,   // "100ms", "5s" etc.
    Enum(Vec<&'static str>),  // Restricted values
}
```

#### Split Configuration Structs

```rust
/// Immutable configuration - set at startup, never changes
pub struct StaticConfig {
    pub bind: String,
    pub port: u16,
    pub num_shards: usize,
    pub data_dir: PathBuf,
    pub tls: Option<TlsConfig>,
}

/// Mutable configuration - can be changed via CONFIG SET
pub struct RuntimeConfig {
    pub max_memory: u64,
    pub maxmemory_policy: EvictionPolicy,
    pub timeout: Duration,
    pub slowlog_log_slower_than: u64,
    pub slowlog_max_len: usize,
    pub loglevel: LogLevel,
    // ... other mutable params
}

/// Thread-safe wrapper for runtime config
pub struct ConfigManager {
    static_config: StaticConfig,
    runtime_config: Arc<RwLock<RuntimeConfig>>,
    param_registry: HashMap<&'static str, ParamMeta>,
}
```

#### ConfigManager Implementation

```rust
impl ConfigManager {
    /// CONFIG GET pattern - returns all matching params
    pub fn get(&self, pattern: &str) -> Vec<(String, String)> {
        let mut results = vec![];

        for (name, meta) in &self.param_registry {
            if glob_match(pattern, name) {
                let value = self.get_value(name);
                results.push((name.to_string(), value));
            }
        }
        results
    }

    /// CONFIG SET param value - returns error if immutable
    pub fn set(&self, param: &str, value: &str) -> Result<(), ConfigError> {
        let meta = self.param_registry.get(param)
            .ok_or(ConfigError::UnknownParameter(param.into()))?;

        if !meta.mutable {
            return Err(ConfigError::Immutable(param.into()));
        }

        // Validate
        if let Some(validator) = meta.validator {
            validator(value)?;
        }

        // Parse and apply
        let mut config = self.runtime_config.write().unwrap();
        self.apply_value(&mut config, param, value)?;

        Ok(())
    }

    /// Get a snapshot for command handlers (cheap clone)
    pub fn snapshot(&self) -> ConfigSnapshot {
        ConfigSnapshot {
            static_config: self.static_config.clone(),
            runtime_config: self.runtime_config.read().unwrap().clone(),
        }
    }
}
```

### Thread Safety

1. **Read-heavy workload**: Use `RwLock` for runtime config
2. **Snapshot pattern**: Command handlers get immutable snapshot, avoid holding locks
3. **Atomic updates**: Consider `arc-swap` crate for lock-free reads if contention is high
4. **Shard distribution**: Each shard can cache a local snapshot, refresh periodically or on change notification

### Error Handling

```rust
pub enum ConfigError {
    /// Parameter doesn't exist
    UnknownParameter(String),

    /// Parameter cannot be changed at runtime
    Immutable(String),

    /// Value failed validation
    InvalidValue { param: String, reason: String },

    /// Type mismatch
    TypeMismatch { param: String, expected: ParamType },
}

impl ConfigError {
    pub fn to_resp_error(&self) -> String {
        match self {
            Self::UnknownParameter(p) =>
                format!("ERR Unknown configuration parameter '{}'", p),
            Self::Immutable(p) =>
                format!("ERR CONFIG SET parameter '{}' is immutable", p),
            Self::InvalidValue { param, reason } =>
                format!("ERR Invalid argument '{}' for CONFIG SET '{}'", reason, param),
            Self::TypeMismatch { param, expected } =>
                format!("ERR Invalid type for '{}': expected {:?}", param, expected),
        }
    }
}
```

---

## TLS Certificate Hot-Reloading

### Motivation

Certificates often have short expiration (90 days, 30 days) due to security policies. Hot-reloading avoids downtime during certificate rotation.

**Note:** This is a differentiating feature - neither Redis, Valkey, nor DragonflyDB support TLS hot-reloading natively.

### Implementation Approaches

#### 1. File Watching (inotify/kqueue)

- Watch cert/key files for modification
- On change: validate new certs, atomically swap TLS context
- **Pros:** Automatic, no operator action needed
- **Cons:** Platform-specific, may miss rapid changes

#### 2. Signal-Based (SIGUSR1)

- Send signal to trigger cert reload
- **Pros:** Explicit, works with any deployment
- **Cons:** Requires operator/automation to trigger

#### 3. Command-Based

- Add `DEBUG RELOAD-CERTS` or similar command
- **Pros:** Can be called via redis-cli
- **Cons:** Requires connection, ACL considerations

### Recommended Design

Support both file watching (default) and signal-based reload:

```rust
pub struct TlsManager {
    /// Current TLS acceptor (swapped atomically)
    acceptor: Arc<ArcSwap<TlsAcceptor>>,

    /// Watcher for cert file changes (optional)
    watcher: Option<notify::RecommendedWatcher>,

    /// Paths to cert files
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl TlsManager {
    /// Reload certificates from configured paths
    pub fn reload_certs(&self) -> Result<(), TlsError> {
        let new_config = load_tls_config(&self.cert_path, &self.key_path)?;
        let new_acceptor = TlsAcceptor::from(new_config);
        self.acceptor.store(Arc::new(new_acceptor));
        info!("TLS certificates reloaded successfully");
        Ok(())
    }
}
```

### Configuration

```toml
[tls]
enabled = true
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"

# Hot-reload options (future)
watch_certs = true          # Enable file watching
reload_signal = "SIGUSR1"   # Signal to trigger reload
```

---

## Config Observer Pattern (Future)

For future features that need to react to configuration changes:

```rust
pub trait ConfigObserver: Send + Sync {
    /// Called when a config parameter changes
    fn on_config_change(&self, param: &str, old: &str, new: &str);
}

impl ConfigManager {
    /// Register observer for config changes
    pub fn add_observer(&self, observer: Arc<dyn ConfigObserver>) {
        // Enables future features like:
        // - Metrics updates on maxmemory change
        // - Log level hot-switching
        // - Connection limit enforcement
    }
}
```

---

## Timeout Reference

This section consolidates all timeout values across FrogDB for easy reference.

### Connection Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Client idle | `timeout` | `0` (disabled) | Disconnect idle clients |
| TCP keepalive | `tcp_keepalive` | `300s` | OS-level keepalive interval |
| Protocol frame | `frame_timeout_ms` | `30000` | Max time for incomplete frame |

### Operation Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Scatter-gather | `scatter_gather_timeout_ms` | `5000` | Total multi-shard operation time |
| VLL lock acquisition | `vll_lock_acquisition_timeout_ms` | `4000` | Time to acquire locks |
| Per-shard lock | `vll_per_shard_lock_timeout_ms` | `2000` | Per-shard queue wait time |
| Lua script | `lua_time_limit_ms` | `5000` | Max script execution time |
| Blocking command | Command argument | varies | BLPOP/BRPOP timeout |

### Persistence Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| WAL batch | `wal_batch_timeout_ms` | `10` | Max delay before WAL flush |
| Snapshot | N/A | No timeout | Snapshot runs to completion |

### Replication Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Replication connection | `repl_timeout_ms` | `60000` | Disconnect on no data |
| Replication ping | `repl_ping_interval_ms` | `10000` | Heartbeat frequency |
| Checkpoint transfer | `checkpoint_transfer_timeout_ms` | `300000` | Total FULLRESYNC time |
| Checkpoint file | `checkpoint_file_timeout_ms` | `60000` | Per-file transfer time |
| Sync ACK | `sync_timeout_ms` | `1000` | Wait for replica ACK |
| Reconnect base | `repl_reconnect_base_ms` | `1000` | Initial reconnect delay |
| Reconnect max | `repl_reconnect_max_ms` | `30000` | Maximum reconnect delay |
| Data timeout | `repl_data_timeout_ms` | `30000` | Time without data before reconnect |

### Cluster Timeouts

| Timeout | Config Key | Default | Description |
|---------|------------|---------|-------------|
| Cluster bus | `cluster_bus_timeout_ms` | `5000` | Inter-node communication |
| Node timeout | `cluster_node_timeout_ms` | `15000` | Mark node as failing |
| Failover timeout | `cluster_failover_timeout_ms` | `5000` | Complete failover |

### Timeout Relationships

```
┌─────────────────────────────────────────────────────────────────┐
│                    Timeout Hierarchy                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Client perspective:                                             │
│    client_timeout (application)                                  │
│         │                                                         │
│         ├── Must be > scatter_gather_timeout_ms                  │
│         │   (client should wait longer than server operation)    │
│         │                                                         │
│         └── Consider repl_timeout_ms for sync writes             │
│             (WAIT command may take longer)                       │
│                                                                   │
│  Operation perspective:                                          │
│    scatter_gather_timeout_ms (5000ms)                           │
│         │                                                         │
│         ├── vll_lock_acquisition_timeout_ms (4000ms)            │
│         │   └── Should be < scatter_gather to leave execution   │
│         │                                                         │
│         └── Execution time (remaining ~1000ms)                   │
│                                                                   │
│  Replication perspective:                                        │
│    repl_timeout_ms (60000ms)                                    │
│         │                                                         │
│         ├── repl_ping_interval_ms (10000ms)                     │
│         │   └── Should be < repl_timeout                        │
│         │                                                         │
│         └── sync_timeout_ms (1000ms)                            │
│             └── Per-write ACK wait, << repl_timeout             │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Recommended Relationships

| Relationship | Constraint | Reason |
|--------------|------------|--------|
| `vll_lock_acquisition_timeout_ms` < `scatter_gather_timeout_ms` | 80% or less | Leave time for execution |
| `repl_ping_interval_ms` < `repl_timeout_ms` | 10% or less | Multiple heartbeats before timeout |
| `sync_timeout_ms` < `repl_timeout_ms` | Much smaller | Per-write vs connection timeout |
| Client timeout > `scatter_gather_timeout_ms` | At least 2x | Account for network latency |
| `cluster_node_timeout_ms` > `cluster_bus_timeout_ms` | At least 2x | Allow retries before marking failed |

### Timeout Units

All timeout configuration keys follow this convention:

| Suffix | Unit |
|--------|------|
| `_ms` | Milliseconds |
| `_s` | Seconds |
| No suffix | Seconds (legacy Redis compatibility) |

---

## References

- [Redis Configuration](https://redis.io/docs/latest/operate/oss_and_stack/management/config/)
- [Valkey Configuration](https://valkey.io/topics/valkey.conf/)
- [DragonflyDB Flags](https://www.dragonflydb.io/docs/managing-dragonfly/flags)
- [Figment Crate](https://docs.rs/figment)
- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics, logging, tracing
- [LIFECYCLE.md](LIFECYCLE.md) - Startup/shutdown procedures
