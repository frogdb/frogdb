---
title: "Security"
description: "FrogDB provides authentication via ACLs, transport encryption via TLS, and network security controls."
sidebar:
  order: 8
---
FrogDB provides authentication via ACLs, transport encryption via TLS, and network security controls.

## Authentication and ACLs

### Quick Start

To require a password for all connections:

```toml
[security]
requirepass = "your-secure-password"
```

Clients authenticate with:
```
AUTH your-secure-password
```

### ACL System

FrogDB supports Redis-compatible ACLs for fine-grained access control.

#### Creating Users

```
ACL SETUSER alice on >password123 ~user:* +@read +@write -@dangerous
ACL SETUSER readonly on >readpass ~* +@read -@write
ACL SETUSER admin on >adminpass ~* +@all
```

Clients authenticate as a named user:
```
AUTH alice password123
```

#### ACL Rule Syntax

| Syntax | Description |
|--------|-------------|
| `on` / `off` | Enable/disable user |
| `>password` | Add password |
| `<password` | Remove password |
| `nopass` | Allow passwordless auth |
| `~pattern` | Allow key pattern (read+write) |
| `%R~pattern` | Allow key pattern (read only) |
| `%W~pattern` | Allow key pattern (write only) |
| `allkeys` | Allow all keys |
| `&pattern` | Allow pub/sub channel pattern |
| `allchannels` | Allow all channels |
| `+command` | Allow command |
| `-command` | Deny command |
| `+@category` | Allow command category |
| `-@category` | Deny command category |
| `allcommands` | Allow all commands |

#### Command Categories

| Category | Description | Example Commands |
|----------|-------------|------------------|
| `@read` | Read-only commands | GET, HGET, LRANGE |
| `@write` | Write commands | SET, HSET, LPUSH |
| `@fast` | O(1) operations | GET, SET, PING |
| `@slow` | O(N) operations | KEYS, SMEMBERS |
| `@dangerous` | Admin commands | DEBUG, CONFIG, SHUTDOWN |
| `@pubsub` | Pub/sub commands | SUBSCRIBE, PUBLISH |
| `@scripting` | Lua scripts | EVAL, EVALSHA |

#### Per-User Rate Limiting

FrogDB extends ACL rules with per-user rate limiting:

```
# Rate-limited application user: 500 cmd/s, 512 KB/s
ACL SETUSER app on >apppass ~app:* +@read +@write ratelimit:cps=500 ratelimit:bps=524288

# Remove rate limits
ACL SETUSER app resetratelimit
```

Exempt commands: `AUTH`, `HELLO`, `PING`, `QUIT`, `RESET`. Connections on the admin port bypass rate limits.

#### ACL Commands

| Command | Description |
|---------|-------------|
| `ACL SETUSER username [rules]` | Create or modify user |
| `ACL DELUSER username` | Delete user |
| `ACL LIST` | List all users with rules |
| `ACL GETUSER username` | Get user configuration |
| `ACL WHOAMI` | Return current authenticated username |
| `ACL CAT [category]` | List categories or commands in a category |
| `ACL SAVE` | Persist ACLs to file |
| `ACL LOAD` | Reload ACLs from file |
| `ACL LOG [count]` | View recent security events |
| `ACL GENPASS [bits]` | Generate a secure random password |

#### ACL File

ACLs can be persisted in a file (Redis-compatible format):

```
user default on nopass ~* &* +@all
user alice on #e3b0c44298fc1c14... ~app:* +@read +@write -@dangerous
user readonly on >plainpass ~* +@read -@write
```

Use hashed passwords (`#<sha256>`) in files for security.

```toml
[security]
aclfile = "/etc/frogdb/users.acl"
```

#### Inline User Configuration

Users can be defined in the TOML config:

```toml
[[acl.users]]
name = "app"
rules = "on >password ~app:* +@read +@write"

[[acl.users]]
name = "admin"
rules = "on >adminpass ~* +@all"
```

#### ACL Behavior

- Permission changes do not affect existing connections until re-authentication.
- To revoke access immediately: `CLIENT KILL USER <username>`
- Multiple passwords per user are supported for rotation without downtime.

### ACLs in Cluster Mode

In cluster mode, ACLs are per-node and not automatically synchronized. The recommended approach is to have the orchestrator push identical ACL configuration to all nodes.

---

## TLS

FrogDB uses rustls (pure Rust, no OpenSSL dependency) for TLS.

### Enabling TLS

```toml
[tls]
enabled = true
cert_file = "/etc/frogdb/tls/server.crt"
key_file = "/etc/frogdb/tls/server.key"
ca_file = "/etc/frogdb/tls/ca.crt"       # For client cert validation
tls_port = 6380                            # TLS listener port
require_client_cert = "none"               # none, optional, required
protocols = ["TLS1.3", "TLS1.2"]
```

Or via environment variables:

```bash
FROGDB_TLS__ENABLED=true
FROGDB_TLS__CERT_FILE=/etc/frogdb/tls/server.crt
FROGDB_TLS__KEY_FILE=/etc/frogdb/tls/server.key
FROGDB_TLS__TLS_PORT=6380
```

### Port-by-Port TLS Policy

| Port | Default | TLS Behavior |
|------|---------|--------------|
| Main client (`port`, 6379) | Plaintext | Always plaintext; TLS clients use `tls_port` |
| TLS client (`tls_port`, 6380) | Encrypted | Always TLS when enabled |
| Admin RESP (`admin_port`, 6381) | Plaintext | Plaintext by default; inherits TLS if `no_tls_on_admin_port = false` |
| HTTP server (`http.port`, 9090) | Plaintext | Always plaintext; supports bearer token auth for /admin/* and /debug/* |
| Cluster bus (`port + 10000`) | Plaintext | TLS when `tls_cluster = true` |

### Per-Subsystem TLS

```toml
[tls]
tls_replication = false    # Encrypt replication connections
tls_cluster = false        # Encrypt cluster bus
no_tls_on_admin_port = true  # Admin port stays plaintext
```

### Mutual TLS (mTLS)

| Mode | Behavior |
|------|----------|
| `none` (default) | Server does not request client certificates |
| `optional` | Server requests client cert; connection allowed without one |
| `required` | Server requires valid client certificate |

When enabled, client certificates are validated against the CA bundle in `ca_file`.

### Certificate Hot-Reloading

FrogDB supports reloading TLS certificates without restart or connection interruption.

**Reload triggers:**

| Trigger | How |
|---------|-----|
| File watcher | Automatic -- watches cert/key files for changes |
| SIGUSR1 signal | `kill -USR1 <pid>` or `systemctl reload frogdb` |
| CONFIG command | `redis-cli CONFIG RELOAD-CERTS` |

**Reload behavior:**
- Existing connections keep old certificates until disconnect.
- New connections use new certificates immediately.
- Failed reload keeps old certificates and logs an error.

**File watcher debouncing:**

```toml
[tls]
watch_certs = true           # Enable filesystem watching
watch_debounce_ms = 500      # Wait 500ms after last change before reloading
```

### Certificate Rotation Procedure

1. Place new certificate files at configured paths (or update symlinks).
2. Trigger reload (automatic via file watcher, or manual via `SIGUSR1` / `CONFIG RELOAD-CERTS`).
3. Verify: check logs for "TLS certificates reloaded successfully" and `tls_cert_reload_total` metric.
4. Monitor: `tls_cert_expiry_seconds` shows the new expiry date.

**Symlink-based rotation (recommended for Kubernetes/cert-manager):**

```bash
mkdir /etc/frogdb/tls/generation-2/
cp new-server.crt /etc/frogdb/tls/generation-2/server.crt
cp new-server.key /etc/frogdb/tls/generation-2/server.key
ln -sfn ./generation-2/ /etc/frogdb/tls/live
kill -USR1 $(pidof frogdb-server)
```

### Integration with Certificate Managers

| Tool | Integration |
|------|-------------|
| cert-manager (K8s) | Mount Secret as volume, file watcher detects updates |
| HashiCorp Vault | Vault Agent sidecar writes certs, file watcher detects |
| ACME / Let's Encrypt | Renewal script writes certs, sends SIGUSR1 |
| Manual | Replace files, run `CONFIG RELOAD-CERTS` |

### Replication TLS

```toml
[tls]
enabled = true
tls_replication = true

# Optional: separate client cert for outgoing connections
client_cert_file = "/etc/frogdb/tls/client.crt"
client_key_file = "/etc/frogdb/tls/client.key"
```

When enabled, replicas connect to the primary's TLS port instead of the plaintext port.

### TLS Validation Rules

FrogDB validates TLS configuration at startup and fails fast on errors:

| Rule | Error |
|------|-------|
| `enabled = true` requires `cert_file` and `key_file` | `ERR tls-cert-file and tls-key-file are required when TLS is enabled` |
| `require_client_cert != "none"` requires `ca_file` | `ERR tls-ca-file is required when client certificates are requested` |
| `tls_port` must not conflict with other ports | `ERR tls-port conflicts with port/admin-port` |
| Certificate files must be readable PEM | `ERR failed to load certificate: <reason>` |
| Private key must match certificate | `ERR private key does not match certificate` |

---

## TLS Monitoring

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_tls_cert_expiry_seconds` | Gauge | Unix timestamp when server certificate expires |
| `frogdb_tls_cert_reload_total` | Counter | Successful certificate reloads |
| `frogdb_tls_cert_reload_errors_total` | Counter | Failed reload attempts |
| `frogdb_tls_handshake_duration_seconds` | Histogram | TLS handshake time |
| `frogdb_tls_handshake_errors_total` | Counter | Failed TLS handshakes |
| `frogdb_tls_connections_current` | Gauge | Current TLS connections |

### TLS Alerting

```yaml
groups:
  - name: frogdb-tls
    rules:
      - alert: FrogDBCertExpiringSoon
        expr: frogdb_tls_cert_expiry_seconds - time() < 7 * 86400
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: "FrogDB TLS certificate expires in less than 7 days"

      - alert: FrogDBCertReloadFailing
        expr: increase(frogdb_tls_cert_reload_errors_total[1h]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB TLS certificate reload is failing"
```

---

## Network Security

- Bind to `127.0.0.1` by default (not exposed to network).
- Admin port should be bound to localhost or a private network.
- Metrics port is always plaintext -- place behind a firewall or restrict access.
- Use TLS for any traffic crossing network boundaries.
