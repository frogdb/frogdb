---
title: "Security"
description: "FrogDB authentication, TLS, and access control."
sidebar:
  order: 7
---

FrogDB supports [Redis ACLs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/) for authentication and access control, and TLS via rustls.

## Authentication

Simple password:

```toml
[security]
requirepass = "your-secure-password"
```

For fine-grained ACLs, see the [Redis ACL documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/). FrogDB supports the full ACL command set: SETUSER, DELUSER, LIST, GETUSER, USERS, CAT, WHOAMI, GENPASS, LOG, SAVE, LOAD. Read-only (`%R~`), write-only (`%W~`), and read-write (`%RW~`) key permissions are supported.

**Not supported:** ACL selectors (the `(...)` syntax and `clearselectors` from Redis 7.0+). Use separate users instead of selectors to grant different permission sets.

### Per-User Rate Limiting (FrogDB Extension)

FrogDB extends ACL rules with per-user rate limiting:

```
ACL SETUSER app on >apppass ~app:* +@read +@write ratelimit:cps=500 ratelimit:bps=524288
ACL SETUSER app resetratelimit
```

Exempt commands: AUTH, HELLO, PING, QUIT, RESET. Admin port connections bypass rate limits.

### ACLs in Cluster Mode

ACLs are per-node and not automatically synchronized. Have the orchestrator push identical ACL configuration to all nodes.

### ACL File

```toml
[acl]
aclfile = "/etc/frogdb/users.acl"
```

Use hashed passwords (`#<sha256>`) in files for security.

---

## TLS

FrogDB uses rustls (pure Rust, no OpenSSL dependency).

```toml
[tls]
enabled = true
cert-file = "/etc/frogdb/tls/server.crt"
key-file = "/etc/frogdb/tls/server.key"
ca-file = "/etc/frogdb/tls/ca.crt"
tls-port = 6380
require-client-cert = "none"   # none, optional, required
```

### Port-by-Port TLS Policy

| Port | Default TLS | Notes |
|---|---|---|
| Main client (6379) | Plaintext | TLS clients use `tls-port` |
| TLS client (6380) | Encrypted | Always TLS when enabled |
| Admin RESP (6382) | Plaintext | `no-tls-on-admin-port = false` to enable |
| HTTP / metrics (9090) | Plaintext | Use bearer token for `/admin/*` and `/debug/*` |
| Cluster bus (+10000) | Plaintext | `tls-cluster = true` to enable |
| Replication | Plaintext | `tls-replication = true` to enable |

### Certificate Hot-Reloading

Certificates are reloaded without restart or connection interruption:

| Trigger | How |
|---|---|
| File watcher | Automatic — watches cert/key files for changes |
| SIGUSR1 signal | `kill -USR1 <pid>` or `systemctl reload frogdb` |
| CONFIG command | `CONFIG RELOAD-CERTS` |

Existing connections keep old certificates. New connections use new certificates immediately. Failed reload keeps old certificates and logs an error.

```toml
[tls]
watch-certs = true           # Enable filesystem watching
watch-debounce-ms = 500      # Debounce interval
```

### Integration with Certificate Managers

| Tool | Integration |
|---|---|
| cert-manager (K8s) | Mount Secret as volume, file watcher detects updates |
| HashiCorp Vault | Vault Agent sidecar writes certs, file watcher detects |
| ACME / Let's Encrypt | Renewal script writes certs, sends SIGUSR1 |

---

## Network Security

- Binds to `127.0.0.1` by default (not exposed).
- Admin port should be bound to localhost or a private network.
- Metrics port is always plaintext — restrict access via firewall.
- Use TLS for any traffic crossing network boundaries.
