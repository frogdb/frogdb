# FrogDB Rate Limiting and Connection Control

Connection limits, output buffer controls, and backpressure mechanisms.

## Connection Limits

### Maximum Clients

**Configuration:**
```toml
[server]
maxclients = 10000  # Maximum simultaneous connections (0 = OS limit)
```

**Behavior when limit reached:**
- New connections rejected immediately
- Error: `max number of clients reached`
- `connection_rejected_total` metric increments

### Reserved Connections

FrogDB reserves a small number of connections for admin operations:

```toml
[server]
reserved_connections = 32  # Always allow admin commands
```

Reserved connections ensure operators can always connect for debugging even when `maxclients` is reached.

---

## Output Buffer Limits

Per-client output buffer limits prevent slow clients from consuming unbounded memory.

### Configuration

```toml
[client]
# Normal clients (default)
client_output_buffer_limit_normal = "0 0 0"  # Unlimited

# Pub/Sub clients (can accumulate messages)
client_output_buffer_limit_pubsub = "32mb 8mb 60"  # Hard 32MB, soft 8MB for 60s

# Replica clients (replication stream)
client_output_buffer_limit_replica = "256mb 64mb 60"
```

**Format:** `<hard_limit> <soft_limit> <soft_seconds>`

### Limit Types

| Limit | Behavior |
|-------|----------|
| **Hard limit** | Disconnect immediately when exceeded |
| **Soft limit** | Disconnect if exceeded for `soft_seconds` continuously |
| **No limit (0 0 0)** | Unlimited buffer growth |

### Client Types

| Type | Default | Rationale |
|------|---------|-----------|
| `normal` | Unlimited | Most clients consume responses quickly |
| `pubsub` | 32MB hard, 8MB soft/60s | Subscribers can lag behind publishers |
| `replica` | 256MB hard, 64MB soft/60s | Replication streams are large but critical |

### Behavior on Disconnect

When buffer limit exceeded:
- Connection closed immediately
- Client receives no response (already overloaded)
- `client_output_buffer_limit_disconnections_total` metric increments
- Log entry: `"Client disconnected: output buffer limit exceeded"`

---

## Backpressure

FrogDB uses TCP backpressure as natural rate limiting rather than explicit throttling.

### Flow

```
Slow client (not reading responses)
    │
    ▼
TCP send buffer fills (OS-level)
    │
    ▼
Shard worker blocks on socket write
    │
    ▼
Shard message channel fills (1024 messages)
    │
    ▼
Coordinator blocks on channel send
    │
    ▼
Other clients on same thread experience latency
```

### Design Rationale

**Why backpressure over rate limiting:**
- Automatically throttles at the source
- No configuration needed
- Works across all command types
- No token bucket state to maintain
- Matches Redis behavior

**Trade-off:** A slow client can affect other clients on the same thread. Mitigations:
- Output buffer limits disconnect slow clients
- Client timeouts reclaim abandoned connections
- Connection distribution spreads load across threads

---

## Client Timeout

Idle clients are disconnected after timeout:

```toml
[server]
timeout = 0  # Seconds of idle before disconnect (0 = never)
```

**Recommendation:** Set `timeout > 0` in production to reclaim abandoned connections.

### Timeout Behavior

- Timer resets on any client activity (commands or responses)
- `PING` commands reset timeout (use for keepalive)
- Blocked commands (BLPOP, etc.) do not count as idle

---

## Command-Level Rate Limiting

FrogDB does not provide built-in command-level rate limiting. Implement at application layer if needed.

### Application-Layer Example

```python
# Python example using ratelimit library
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls=1000, period=1)  # 1000 ops/second
def rate_limited_set(key, value):
    return redis.set(key, value)
```

### Proxy-Layer Example

Use a Redis-compatible proxy with rate limiting:
- **Envoy** with Redis filter and rate limit service
- **Twemproxy** with connection pooling
- **Redis Cluster Proxy** with traffic shaping

### Future Consideration

Per-ACL-user rate limiting is not currently planned but could be added:

```toml
# Hypothetical future syntax
[acl.users.limited_user]
commands_per_second = 1000
bytes_per_second = 10485760  # 10MB/s
```

---

## Monitoring

| Metric | Description |
|--------|-------------|
| `frogdb_connected_clients` | Current connection count |
| `frogdb_connection_rejected_total` | Connections rejected (maxclients) |
| `frogdb_client_output_buffer_limit_disconnections_total` | Buffer limit disconnects |
| `frogdb_client_timeout_disconnections_total` | Timeout disconnects |
| `frogdb_blocked_clients` | Clients in blocking commands |

---

## References

- [FAILURE_MODES.md](FAILURE_MODES.md) - Connection and network failures
- [CONCURRENCY.md](CONCURRENCY.md) - Channel backpressure details
- [OBSERVABILITY.md](OBSERVABILITY.md) - Full metrics reference
