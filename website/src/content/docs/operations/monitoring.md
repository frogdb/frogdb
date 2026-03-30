---
title: "Monitoring"
description: "FrogDB provides structured logging, distributed tracing, Prometheus metrics, and Redis-compatible introspection commands."
sidebar:
  order: 9
---
FrogDB provides structured logging, distributed tracing, Prometheus metrics, and Redis-compatible introspection commands.

## Logging

### Configuration

```toml
[logging]
level = "info"                  # trace, debug, info, warn, error
format = "json"                 # "json" or "pretty"
output = "stdout"               # "stdout", "stderr", or "none"
per_request_spans = false       # Enable per-request tracing spans (~13% CPU overhead)
# file_path = "/var/log/frogdb/frogdb.log"

# [logging.rotation]
# max_size_mb = 100
# frequency = "daily"           # "daily", "hourly", or "never"
# max_files = 5
```

Runtime change: `CONFIG SET loglevel debug`

### Log Events

| Event | Level | Key Fields |
|-------|-------|------------|
| Server started | INFO | version, num_shards, port |
| Server shutdown | INFO | reason, uptime |
| Connection accepted/closed | DEBUG | connection_id, peer_addr |
| Slow command | WARN | command, duration_ms |
| Auth failure | WARN | username, reason |
| Persistence error | ERROR | error, context |
| Key evicted | DEBUG | shard_id, key, policy |
| Snapshot completed | INFO | duration_ms, keys |

### JSON Log Example

```json
{"timestamp":"2024-01-15T10:30:45.123Z","level":"INFO","target":"frogdb::server","message":"Server started","version":"0.1.0","shards":8,"port":6379}
{"timestamp":"2024-01-15T10:30:46.789Z","level":"WARN","target":"frogdb::command","message":"Slow command","connection_id":1,"command":"KEYS","duration_ms":150}
```

---

## Distributed Tracing

FrogDB supports OpenTelemetry (OTLP) for distributed tracing.

```toml
[tracing]
enabled = true
endpoint = "http://localhost:4317"
sampling_rate = 0.1             # Sample 10% of requests
service_name = "frogdb"
```

Traces follow OpenTelemetry semantic conventions for databases (`db.system = "frogdb"`).

---

## Prometheus Metrics

### Configuration

```toml
[metrics]
prometheus_enabled = true
prometheus_port = 9090
per_command_metrics = true      # Per-command latency histograms
per_shard_metrics = false       # Per-shard metrics (adds cardinality)
```

Scrape endpoint: `GET http://<host>:9090/metrics`

### System Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_uptime_seconds` | Gauge | Server uptime |
| `frogdb_cpu_usage_percent` | Gauge | Process CPU usage |
| `frogdb_memory_rss_bytes` | Gauge | Resident set size |
| `frogdb_memory_used_bytes` | Gauge | Memory used by data |
| `frogdb_memory_max_bytes` | Gauge | Configured max memory |
| `frogdb_memory_fragmentation_ratio` | Gauge | Memory fragmentation |

### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_connections_total` | Counter | Total connections accepted |
| `frogdb_connections_current` | Gauge | Current open connections |
| `frogdb_connections_max` | Gauge | Configured max connections |
| `frogdb_connections_rejected_total` | Counter | Rejected connections (by reason) |

### Command Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_commands_total` | Counter | Commands processed (by command) |
| `frogdb_commands_duration_ms` | Histogram | Command latency (by command) |
| `frogdb_commands_errors_total` | Counter | Command errors (by command, error type) |

### Data Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_keys_total` | Gauge | Total keys (by shard) |
| `frogdb_keys_expired_total` | Counter | Keys expired |
| `frogdb_keys_evicted_total` | Counter | Keys evicted (by policy) |
| `frogdb_keyspace_hits_total` | Counter | Cache hits |
| `frogdb_keyspace_misses_total` | Counter | Cache misses |

### Persistence Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_persistence_writes_total` | Counter | WAL writes |
| `frogdb_persistence_errors_total` | Counter | Persistence errors |
| `frogdb_snapshot_in_progress` | Gauge | 1 if snapshot running |
| `frogdb_snapshot_duration_ms` | Histogram | Snapshot duration |

### Shard Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_shard_keys` | Gauge | Per-shard key count |
| `frogdb_shard_memory_bytes` | Gauge | Per-shard memory usage |
| `frogdb_shard_queue_depth` | Gauge | Message queue depth |
| `frogdb_shard_ops_total` | Counter | Per-shard operations |

### Pub/Sub Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_pubsub_channels` | Gauge | Active channels |
| `frogdb_pubsub_subscribers` | Gauge | Total subscribers |
| `frogdb_pubsub_messages_total` | Counter | Messages published |

---

## Prometheus Integration

### Scrape Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'frogdb'
    static_configs:
      - targets: ['frogdb:9090']
    metrics_path: /metrics
```

### Alerting Rules

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

      - alert: FrogDBHighLatency
        expr: histogram_quantile(0.99, rate(frogdb_commands_duration_ms_bucket[5m])) > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB p99 latency high"

      - alert: FrogDBPersistenceError
        expr: increase(frogdb_persistence_errors_total[5m]) > 0
        labels:
          severity: critical
        annotations:
          summary: "FrogDB persistence errors"

      - alert: FrogDBHotShard
        expr: |
          max(rate(frogdb_shard_ops_total[5m])) /
          avg(rate(frogdb_shard_ops_total[5m])) > 2.0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB hot shard detected"
```

---

## Grafana Dashboard Panels

Recommended panels:

1. **Overview**: Uptime, version, connected clients, commands per second
2. **Performance**: Command latency (p50, p95, p99), hit/miss ratio, slow log entries
3. **Memory**: Used vs max memory, fragmentation ratio, eviction rate
4. **Connections**: Current connections, connection rate, rejected connections
5. **Persistence**: WAL write rate, snapshot status, disk usage
6. **Shards**: Keys per shard, memory per shard, ops per shard, hot shard indicators

---

## Health Checks

### HTTP Endpoints

| Endpoint | Response |
|----------|----------|
| `GET /health/live` | `200 OK` if process is running |
| `GET /health/ready` | `200 OK` if accepting commands |

### Redis Protocol

```
PING    # Returns PONG if healthy
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

---

## Server Commands

### INFO

Redis-compatible INFO command:

```
INFO [section]
```

Sections: `server`, `clients`, `memory`, `persistence`, `stats`, `replication`, `cpu`, `keyspace`, `hotshards`, `frogdb`

### SLOWLOG

Track slow commands:

```
SLOWLOG GET [count]     # Get recent slow queries (default: 10)
SLOWLOG LEN             # Count of entries
SLOWLOG RESET           # Clear the slow log
```

Configuration:
```toml
[slowlog]
log_slower_than = 10000     # Microseconds (10ms), 0 = log all, -1 = disable
max_len = 128
```

### CLIENT LIST

View connected clients:
```
CLIENT LIST
```

Red flags to look for:
- `idle > 300` for non-pub/sub clients -- possible connection leak
- `omem > 1MB` -- slow client backing up
- Many clients with same `addr` -- pool misconfiguration

### MEMORY Commands

```
MEMORY USAGE <key>      # Memory used by a specific key
MEMORY STATS            # Detailed memory statistics
MEMORY DOCTOR           # Memory issues diagnosis
```

### STATUS

Comprehensive server status:
```
STATUS JSON             # Returns full status as JSON
GET /status/json        # HTTP endpoint (same data)
```

### DEBUG HOTSHARDS

Per-shard traffic analysis:
```
DEBUG HOTSHARDS [PERIOD <seconds>]
```

---

## Metrics Cardinality

FrogDB manages metric cardinality to prevent excessive Prometheus memory usage:

```toml
[metrics]
per_command_metrics = true      # Useful for debugging (~200 time series)
per_shard_metrics = false       # Enable if needed (adds N time series per metric)
max_error_message_labels = 50   # Cap error label cardinality
aggregate_client_ips = true     # Prevent IP cardinality explosion
```

High-cardinality values (key names, client IPs, Lua script SHAs) are intentionally NOT used as metric labels.
