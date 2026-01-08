# FrogDB Observability

This document covers logging, tracing, metrics, and debugging capabilities for operating FrogDB in production.

## Overview

FrogDB integrates with standard observability tools:
- **Logging**: Structured logs via `tracing` crate
- **Tracing**: Distributed tracing via OpenTelemetry (OTLP)
- **Metrics**: Prometheus endpoint + OTLP export
- **Debugging**: Redis-compatible INFO, SLOWLOG, CLIENT, MEMORY commands

---

## Logging

### Configuration

```rust
struct LogConfig {
    /// Log level filter
    level: LogLevel,            // error, warn, info, debug, trace
    /// Output format
    format: LogFormat,          // json, pretty
    /// Output destination
    output: LogOutput,          // stdout, stderr, file
    /// File path (if output = file)
    file_path: Option<PathBuf>,
    /// Log rotation settings
    rotation: Option<RotationConfig>,
}

struct RotationConfig {
    max_size_mb: u64,           // Rotate when file exceeds size
    max_files: u32,             // Keep N rotated files
    compress: bool,             // Compress rotated files
}
```

### Config File

```toml
[logging]
level = "info"
format = "json"                 # "json" or "pretty"
output = "stdout"               # "stdout", "stderr", or "file"
# file_path = "/var/log/frogdb/frogdb.log"

[logging.rotation]
max_size_mb = 100
max_files = 5
compress = true
```

### Structured Fields

All log entries include:
- `timestamp` - ISO 8601 timestamp
- `level` - Log level (ERROR, WARN, INFO, DEBUG, TRACE)
- `target` - Module path
- `message` - Log message

Context-dependent fields:
- `connection_id` - Client connection identifier
- `command` - Redis command name
- `key` - Key being operated on (if applicable)
- `shard_id` - Internal shard handling the request
- `request_id` - Unique request identifier for correlation

### Log Events

| Event | Level | Fields |
|-------|-------|--------|
| Server started | INFO | version, config_path, num_shards |
| Server shutdown | INFO | reason, uptime |
| Connection accepted | DEBUG | connection_id, peer_addr |
| Connection closed | DEBUG | connection_id, reason |
| Command received | TRACE | connection_id, command |
| Command executed | TRACE | connection_id, command, duration_us |
| Slow command | WARN | connection_id, command, duration_ms |
| Command error | DEBUG | connection_id, command, error |
| Auth success | INFO | connection_id, username |
| Auth failure | WARN | connection_id, username, reason |
| Key expired | TRACE | shard_id, key |
| Key evicted | DEBUG | shard_id, key, policy |
| Snapshot started | INFO | shard_id |
| Snapshot completed | INFO | shard_id, duration_ms, keys |
| Persistence error | ERROR | error, context |

### Example Log Output

**JSON format:**
```json
{"timestamp":"2024-01-15T10:30:45.123Z","level":"INFO","target":"frogdb::server","message":"Server started","version":"0.1.0","shards":8,"port":6379}
{"timestamp":"2024-01-15T10:30:46.456Z","level":"DEBUG","target":"frogdb::connection","message":"Connection accepted","connection_id":1,"peer_addr":"127.0.0.1:54321"}
{"timestamp":"2024-01-15T10:30:46.789Z","level":"WARN","target":"frogdb::command","message":"Slow command","connection_id":1,"command":"KEYS","duration_ms":150}
```

**Pretty format:**
```
2024-01-15T10:30:45.123Z  INFO frogdb::server: Server started version=0.1.0 shards=8 port=6379
2024-01-15T10:30:46.456Z DEBUG frogdb::connection: Connection accepted connection_id=1 peer_addr=127.0.0.1:54321
2024-01-15T10:30:46.789Z  WARN frogdb::command: Slow command connection_id=1 command=KEYS duration_ms=150
```

### Crate

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
```

---

## Distributed Tracing

FrogDB supports distributed tracing via OpenTelemetry for request flow visualization across services.

### Configuration

```rust
struct TracingConfig {
    /// Enable distributed tracing
    enabled: bool,
    /// OTLP endpoint
    endpoint: String,           // e.g., "http://localhost:4317"
    /// Sampling rate (0.0 to 1.0)
    sampling_rate: f64,
    /// Service name in traces
    service_name: String,       // Default: "frogdb"
    /// Trace context propagation
    propagation: Propagation,   // W3C TraceContext
}
```

### Config File

```toml
[tracing]
enabled = true
endpoint = "http://localhost:4317"
sampling_rate = 0.1             # Sample 10% of requests
service_name = "frogdb"
```

### Span Hierarchy

```
request (root)
├── parse_command
├── route_to_shard
│   └── shard_execute
│       ├── store_operation (get/set/delete)
│       └── persistence_write (if applicable)
└── encode_response
```

For scatter-gather operations (MGET, MSET, DEL with multiple keys):
```
request (root)
├── parse_command
├── scatter
│   ├── shard_0_execute
│   ├── shard_2_execute
│   └── shard_5_execute
├── gather
└── encode_response
```

### Semantic Attributes

Following [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/):

| Attribute | Value | Description |
|-----------|-------|-------------|
| `db.system` | `frogdb` | Database system identifier |
| `db.operation` | `GET`, `SET`, etc. | Command name |
| `db.statement` | Full command | Complete command (if enabled) |
| `db.redis.database_index` | `0` | Database number |
| `net.peer.ip` | Client IP | Client IP address |
| `net.peer.port` | Client port | Client port number |
| `frogdb.connection_id` | Connection ID | Internal connection identifier |
| `frogdb.shard_id` | Shard ID | Internal shard handling request |
| `frogdb.keys_count` | Count | Number of keys in multi-key op |

### Crates

```toml
opentelemetry = "0.21"
opentelemetry-otlp = "0.14"
tracing-opentelemetry = "0.22"
```

---

## Metrics

FrogDB exports metrics in two formats:
1. **Prometheus**: HTTP endpoint for scraping
2. **OTLP**: Push to OpenTelemetry collector

### Configuration

```rust
struct MetricsConfig {
    /// Enable Prometheus endpoint
    prometheus_enabled: bool,
    /// Prometheus endpoint port
    prometheus_port: u16,       // Default: 9090
    /// Enable OTLP metrics export
    otlp_enabled: bool,
    /// OTLP endpoint (shared with tracing)
    otlp_endpoint: String,
    /// OTLP export interval
    export_interval_secs: u64,  // Default: 15
}
```

### Config File

```toml
[metrics]
prometheus_enabled = true
prometheus_port = 9090
otlp_enabled = false
# otlp_endpoint = "http://localhost:4317"
# export_interval_secs = 15
```

### System Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_uptime_seconds` | Gauge | Server uptime |
| `frogdb_cpu_usage_percent` | Gauge | Process CPU usage |
| `frogdb_memory_rss_bytes` | Gauge | Resident set size |
| `frogdb_memory_used_bytes` | Gauge | Memory used by data |
| `frogdb_memory_peak_bytes` | Gauge | Peak memory usage |
| `frogdb_memory_fragmentation_ratio` | Gauge | Memory fragmentation |

### Connection Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_connections_total` | Counter | | Total connections accepted |
| `frogdb_connections_current` | Gauge | | Current open connections |
| `frogdb_connections_rejected_total` | Counter | `reason` | Rejected connections |

Rejection reasons: `max_clients`, `auth_required`, `acl_denied`

### Command Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_commands_total` | Counter | `command` | Commands processed |
| `frogdb_commands_duration_seconds` | Histogram | `command` | Command latency |
| `frogdb_commands_errors_total` | Counter | `command`, `error` | Command errors |

Error types: `wrong_type`, `syntax`, `auth`, `timeout`, `internal`

### Data Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_keys_total` | Gauge | `shard` | Total keys |
| `frogdb_keys_with_expiry` | Gauge | `shard` | Keys with TTL set |
| `frogdb_keys_expired_total` | Counter | `shard` | Keys expired |
| `frogdb_keys_evicted_total` | Counter | `shard`, `policy` | Keys evicted |
| `frogdb_keyspace_hits_total` | Counter | | Cache hits |
| `frogdb_keyspace_misses_total` | Counter | | Cache misses |

Eviction policies: `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `allkeys-random`, `volatile-random`, `volatile-ttl`

### Shard Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_shard_memory_bytes` | Gauge | `shard` | Per-shard memory usage |
| `frogdb_shard_keys` | Gauge | `shard` | Per-shard key count |
| `frogdb_shard_queue_depth` | Gauge | `shard` | Message queue depth |
| `frogdb_shard_queue_latency_seconds` | Histogram | `shard` | Queue wait time |

### Persistence Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `frogdb_persistence_writes_total` | Counter | `shard` | WAL writes |
| `frogdb_persistence_write_bytes_total` | Counter | `shard` | WAL bytes written |
| `frogdb_persistence_write_duration_seconds` | Histogram | | Write latency |
| `frogdb_persistence_errors_total` | Counter | `type` | Persistence errors |
| `frogdb_snapshot_in_progress` | Gauge | `shard` | 1 if snapshot running |
| `frogdb_snapshot_duration_seconds` | Histogram | | Snapshot duration |
| `frogdb_snapshot_size_bytes` | Gauge | `shard` | Last snapshot size |

### Pub/Sub Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_pubsub_channels` | Gauge | Active channels |
| `frogdb_pubsub_patterns` | Gauge | Active pattern subscriptions |
| `frogdb_pubsub_subscribers` | Gauge | Total subscribers |
| `frogdb_pubsub_messages_total` | Counter | Messages published |

### Network Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_network_bytes_received_total` | Counter | Bytes received |
| `frogdb_network_bytes_sent_total` | Counter | Bytes sent |

### Crates

```toml
metrics = "0.21"
metrics-exporter-prometheus = "0.12"
```

### MetricsRecorder Trait

Abstraction for swappable metrics backends:

```rust
pub trait MetricsRecorder: Send + Sync {
    fn increment_counter(&self, name: &str, labels: &[(&str, &str)]);
    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);
    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);
}
```

Implementations:
- `PrometheusRecorder` - Prometheus `/metrics` endpoint
- `OtlpRecorder` - OTLP push export
- `NoopRecorder` - Disabled metrics (zero overhead)

---

## Server Commands

### INFO

Redis-compatible INFO command with FrogDB extensions.

```
INFO [section]
```

Sections: `server`, `clients`, `memory`, `persistence`, `stats`, `replication`, `cpu`, `keyspace`, `frogdb`

**Example output:**

```
# Server
redis_version:7.0.0
frogdb_version:0.1.0
os:Linux 5.15.0 x86_64
arch_bits:64
process_id:12345
tcp_port:6379
uptime_in_seconds:3600
uptime_in_days:0

# Clients
connected_clients:10
blocked_clients:0
max_clients:10000

# Memory
used_memory:1048576
used_memory_human:1.00M
used_memory_peak:2097152
used_memory_peak_human:2.00M
used_memory_rss:4194304
used_memory_rss_human:4.00M
mem_fragmentation_ratio:4.00
maxmemory:0
maxmemory_human:0B
maxmemory_policy:noeviction

# Persistence
rdb_last_save_time:1705312245
rdb_last_bgsave_status:ok
aof_enabled:1
aof_last_write_status:ok

# Stats
total_connections_received:100
total_commands_processed:5000
instantaneous_ops_per_sec:150
keyspace_hits:4000
keyspace_misses:1000
expired_keys:50
evicted_keys:0

# Replication
role:master
connected_slaves:0

# CPU
used_cpu_sys:1.500000
used_cpu_user:3.200000

# Keyspace
db0:keys=1000,expires=50,avg_ttl=3600000

# FrogDB
frogdb_shards:8
frogdb_shard_0_keys:125
frogdb_shard_0_memory:131072
frogdb_shard_1_keys:130
frogdb_shard_1_memory:138240
...
```

### SLOWLOG

Track slow commands for performance analysis.

```
SLOWLOG GET [count]     # Get recent slow queries (default: 10)
SLOWLOG LEN             # Count of entries in slow log
SLOWLOG RESET           # Clear the slow log
```

**Configuration:**

```toml
[slowlog]
log_slower_than = 10000     # Microseconds (10ms), 0 = log all, -1 = disable
max_len = 128               # Max entries to keep
```

**Example output:**

```
> SLOWLOG GET 2
1) 1) (integer) 1              # Entry ID
   2) (integer) 1705312245     # Unix timestamp
   3) (integer) 15000          # Duration in microseconds
   4) 1) "KEYS"                # Command
      2) "*"
   5) "127.0.0.1:54321"        # Client address
   6) ""                       # Client name
2) 1) (integer) 0
   2) (integer) 1705312240
   3) (integer) 12000
   4) 1) "SMEMBERS"
      2) "large_set"
   5) "127.0.0.1:54322"
   6) "worker-1"
```

### CLIENT

Connection management commands.

```
CLIENT LIST                 # List all connections
CLIENT GETNAME              # Get connection name
CLIENT SETNAME <name>       # Set connection name
CLIENT ID                   # Get connection ID
CLIENT INFO                 # Get current connection info
CLIENT KILL <addr>          # Kill connection by address
CLIENT KILL ID <id>         # Kill connection by ID
```

**CLIENT LIST output:**

```
id=1 addr=127.0.0.1:54321 fd=5 name=worker-1 age=100 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 cmd=get
id=2 addr=127.0.0.1:54322 fd=6 name= age=50 idle=5 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 cmd=ping
```

**Flags:**
- `N` - Normal client
- `M` - Master (replication)
- `S` - Replica (replication)
- `P` - Pub/Sub subscriber
- `x` - In MULTI/EXEC transaction
- `b` - Blocked (BLPOP, etc.)

### MEMORY

Memory introspection commands.

```
MEMORY USAGE <key> [SAMPLES count]  # Memory used by key
MEMORY STATS                        # Detailed memory statistics
MEMORY DOCTOR                       # Memory issues diagnosis
MEMORY MALLOC-SIZE <size>           # Allocator size for given size
```

**MEMORY STATS output:**

```
peak.allocated:2097152
total.allocated:1048576
startup.allocated:524288
replication.backlog:0
clients.normal:32768
clients.slaves:0
aof.buffer:0
keys.count:1000
keys.bytes-per-key:1024
dataset.bytes:524288
dataset.percentage:50.00
```

**MEMORY DOCTOR output:**

```
Sam, I have a few reports for you:
* High fragmentation: 4.00 ratio. Consider restarting.
* Big keys detected: 3 keys using more than 1MB each.
```

### DBSIZE

Get key count.

```
DBSIZE                      # Returns total keys across all shards
```

### DEBUG Commands (Safe Subset)

Limited DEBUG commands for diagnostics. **Dangerous commands are intentionally not implemented.**

```
DEBUG OBJECT <key>          # Key internal representation
DEBUG STRUCTSIZE            # Size of internal data structures
DEBUG SLEEP <seconds>       # Sleep (testing only, disable in production)
```

**DEBUG OBJECT output:**

```
Value at:0x7f1234567890 refcount:1 encoding:embstr serializedlength:12 lru:1234567 lru_seconds_idle:10
```

**Not implemented (dangerous):**
- `DEBUG SEGFAULT` - Crashes server intentionally
- `DEBUG RELOAD` - Can cause data corruption
- `DEBUG CRASH-AND-RECOVER` - Unsafe crash simulation
- `DEBUG SET-ACTIVE-EXPIRE` - Internal state manipulation

---

## Health Checks

### HTTP Endpoints

If HTTP metrics endpoint is enabled, health endpoints are available:

```
GET /health/live            # Liveness: process is running
GET /health/ready           # Readiness: accepting commands
```

**Responses:**

```
# Healthy
HTTP 200 OK
{"status": "ok"}

# Unhealthy (liveness)
HTTP 503 Service Unavailable
{"status": "error", "reason": "shutting_down"}

# Not ready (readiness)
HTTP 503 Service Unavailable
{"status": "not_ready", "reason": "loading_data"}
```

### Redis Protocol

```
PING                        # Returns PONG if healthy
INFO server                 # Detailed server status
```

### Kubernetes Integration

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

Or using Redis protocol:

```yaml
livenessProbe:
  exec:
    command: ["redis-cli", "ping"]
  initialDelaySeconds: 5
  periodSeconds: 10
```

---

## Alerting Guidelines

Recommended alerts for production deployments:

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| HighMemoryUsage | `used_memory / maxmemory > 0.8` | Warning | Review eviction policy, scale up |
| CriticalMemoryUsage | `used_memory / maxmemory > 0.95` | Critical | Immediate action required |
| HighLatency | `p99_latency > 100ms` | Warning | Check slow log, optimize queries |
| ConnectionSaturation | `connections / max_clients > 0.8` | Warning | Increase max_clients or scale |
| HighEvictionRate | `evicted_keys_rate > 100/s` | Warning | Increase memory or review data |
| PersistenceFailure | `persistence_errors > 0` | Critical | Check disk space, permissions |
| ReplicationLag | `replication_lag > 10s` | Warning | Check network, replica health |
| ShardImbalance | `max_shard_keys / min_shard_keys > 2` | Warning | Review key distribution |

### Example Prometheus Alert Rules

```yaml
groups:
  - name: frogdb
    rules:
      - alert: FrogDBHighMemory
        expr: frogdb_memory_used_bytes / frogdb_memory_max_bytes > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB memory usage high"
          description: "Memory usage is {{ $value | humanizePercentage }}"

      - alert: FrogDBHighLatency
        expr: histogram_quantile(0.99, rate(frogdb_commands_duration_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB p99 latency high"
          description: "p99 latency is {{ $value | humanizeDuration }}"

      - alert: FrogDBPersistenceError
        expr: increase(frogdb_persistence_errors_total[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "FrogDB persistence errors"
          description: "Persistence errors detected"
```

---

## Grafana Dashboard

Key panels for a FrogDB dashboard:

1. **Overview**
   - Uptime
   - Version
   - Connected clients
   - Commands per second

2. **Performance**
   - Command latency (p50, p95, p99)
   - Commands by type
   - Hit/miss ratio
   - Slow log entries

3. **Memory**
   - Used vs max memory
   - Memory by shard
   - Fragmentation ratio
   - Eviction rate

4. **Connections**
   - Current connections
   - Connection rate
   - Rejected connections

5. **Persistence**
   - WAL write rate
   - Snapshot status
   - Disk usage

6. **Shards**
   - Keys per shard
   - Memory per shard
   - Queue depth per shard

---

## References

- [OPERATIONS.md](OPERATIONS.md) - Configuration and deployment
- [DESIGN.md](../DESIGN.md) - Architecture overview
- [EVICTION.md](EVICTION.md) - Eviction policies and memory management
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/database/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
