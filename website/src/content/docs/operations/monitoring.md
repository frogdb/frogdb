---
title: "Monitoring"
description: "FrogDB logging, metrics, and introspection."
sidebar:
  order: 5
---

## Logging

```toml
[logging]
level = "info"                  # trace, debug, info, warn, error
format = "json"                 # "json" or "pretty"
output = "stdout"               # "stdout", "stderr", or "none"
per-request-spans = false       # Per-request tracing spans (~13% CPU overhead)
# file-path = "/var/log/frogdb/frogdb.log"

# [logging.rotation]
# max-size-mb = 100
# frequency = "daily"           # "daily", "hourly", or "never"
# max-files = 5
```

Runtime change: `CONFIG SET loglevel debug`

## Prometheus Metrics

Endpoint: `GET http://<host>:9090/metrics`

```toml
[http]
enabled = true
bind = "127.0.0.1"
port = 9090
```

See [Metrics Reference](/reference/metrics/) for the complete list of all exported metrics.

### Key Metrics to Monitor

| Metric | Alert When |
|---|---|
| `frogdb_memory_used_bytes / frogdb_memory_maxmemory_bytes` | > 80% for 5 min |
| `histogram_quantile(0.99, frogdb_commands_duration_seconds)` | > 100ms for 5 min |
| `frogdb_persistence_errors_total` | Any increase |
| `frogdb_connections_rejected_total` | Any increase |
| `max(frogdb_shard_keys) / avg(frogdb_shard_keys)` | > 2.0 (hot shard) |

## Distributed Tracing

FrogDB supports OpenTelemetry (OTLP):

```toml
[tracing]
enabled = true
otlp-endpoint = "http://localhost:4317"
sampling-rate = 0.1
service-name = "frogdb"
```

## Health Checks

| Endpoint | Response |
|---|---|
| `GET /health/live` | `200 OK` if process is running |
| `GET /health/ready` | `200 OK` if accepting commands |
| `PING` (Redis protocol) | `PONG` if healthy |

Kubernetes probes:

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 5
readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 5
```

## FrogDB-Specific Introspection

### STATUS

Comprehensive server and shard status:

```
STATUS JSON
GET /status/json    # HTTP endpoint
```

### DEBUG HOTSHARDS

Per-shard traffic analysis to detect imbalanced key distribution:

```
DEBUG HOTSHARDS [PERIOD <seconds>]
```

### INFO Sections

Standard Redis `INFO` sections plus FrogDB extensions:

```
INFO server|clients|memory|persistence|stats|replication|cpu|keyspace|hotshards|frogdb
```
