---
title: "Security"
description: "Configure authentication, ACLs, per-user rate limiting, and TLS in FrogDB."
sidebar:
  order: 7
---

FrogDB supports [Redis ACLs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/) for authentication and access control, TLS via rustls, and a FrogDB-specific per-user rate-limit extension. This page covers each and is explicit about the one area that is not yet automated: certificate hot-reload. Base ACL rule syntax is documented by redis.io; this page documents the FrogDB deltas.

## Authentication

The simplest form is a shared password for the default user:

```toml
[security]
requirepass = "your-secure-password"
```

Clients must then `AUTH` before running commands. For per-user credentials and fine-grained permissions, use ACLs.

## ACLs

FrogDB implements the ACL command set: `WHOAMI`, `LIST`, `USERS`, `GETUSER`, `SETUSER`, `DELUSER`, `CAT`, `GENPASS`, `DRYRUN`, `LOG`, `SAVE`, `LOAD`, and `HELP`. Users, rules, and categories follow Redis semantics.

Key-permission prefixes control per-pattern access:

| Prefix | Access |
|---|---|
| `~pattern` | Read and write. |
| `%R~pattern` | Read only. |
| `%W~pattern` | Write only. |
| `%RW~pattern` | Read and write (explicit form of `~`). |

```bash
ACL SETUSER analyst on >secret %R~metrics:* +@read
```

**ACL selectors are not supported.** The Redis 7.0+ selector syntax (`(...)` groups and `clearselectors`) is explicitly rejected with an error. Grant different permission sets by defining separate users instead of attaching selectors to one user.

### ACL file

Point `[acl] aclfile` at a file for `ACL SAVE` and `ACL LOAD`:

```toml
[acl]
aclfile = "/etc/frogdb/users.acl"
```

Store hashed passwords (`#<sha256>`) rather than plaintext in the file.

### Per-user rate limiting

FrogDB extends `ACL SETUSER` with two rate-limit rule tokens, enforced by a per-user token-bucket:

| Token | Limit |
|---|---|
| `ratelimit:cps=<n>` | Commands per second. |
| `ratelimit:bps=<n>` | Bytes per second. |

```bash
ACL SETUSER app on >apppass ~app:* +@read +@write ratelimit:cps=500 ratelimit:bps=524288
```

The `resetratelimit` rule token clears a user's current buckets when applied through `SETUSER`:

```bash
ACL SETUSER app resetratelimit
```

`AUTH`, `HELLO`, `PING`, `QUIT`, and `RESET` are always exempt from rate limiting so a throttled client can still authenticate and negotiate. Connections on the admin port bypass rate limits entirely.

This page is the canonical reference for the rate-limit rule tokens and the exempt list.

### ACLs across nodes

ACLs are per-node and are **not** automatically synchronized across a cluster. Distribute the same `aclfile` to every node through your configuration management (for example, a shared ConfigMap or the same file on each host), and apply changes by reloading — `ACL LOAD` re-reads the file, and a restart reloads it at startup. See [Clustering](/operations/clustering/) for cluster topology.

## TLS

FrogDB terminates TLS with [rustls](https://github.com/rustls/rustls) and the aws-lc-rs cryptographic provider — a pure-Rust stack with no OpenSSL dependency. A minimal configuration:

```toml
[tls]
enabled = true
cert-file = "/etc/frogdb/tls/server.crt"
key-file = "/etc/frogdb/tls/server.key"
ca-file = "/etc/frogdb/tls/ca.crt"
tls-port = 6380
require-client-cert = "none"   # none | optional | required
```

`require-client-cert` selects the client-certificate (mutual TLS) mode: `none` requests no client certificate, `optional` requests but does not require one, and `required` enforces mutual TLS. A `ca-file` is required whenever the mode is not `none`. `protocols` and `ciphersuites` restrict the negotiated TLS versions and cipher suites (empty `ciphersuites` uses the rustls defaults).

### Per-port TLS policy

TLS is applied per listener. The relevant keys and their default posture:

| Listener | Default | Key |
|---|---|---|
| Client (6379) | Plaintext — TLS clients connect on `tls-port` | — |
| TLS client (6380) | Encrypted when `[tls] enabled` | `tls-port` |
| Admin RESP (6382) | Plaintext | `no-tls-on-admin-port` (default `true`) |
| HTTP / metrics (9090) | Plaintext — protect `/admin/*` and `/debug/*` with `[http] token` | `no-tls-on-http` (default `true`) |
| Cluster bus (16379) | Plaintext | `tls-cluster` |
| Replication | Plaintext | `tls-replication` |

Set `no-tls-on-admin-port = false` or `no-tls-on-http = false` to bring those listeners under TLS when it is enabled. `tls-cluster-migration` enables a dual-accept mode (accepting both plaintext and TLS) for rolling a cluster onto encrypted bus traffic without downtime; it requires `tls-cluster`.

### Certificate management

Certificate changes currently require a **restart**. FrogDB holds its TLS configuration behind an atomic swap internally, so the plumbing to replace certificates in place exists, but no runtime mechanism triggers it yet: there is no filesystem watcher, no reload signal, and no `CONFIG` command for certificates. The `watch-certs` and `watch-debounce-ms` keys are present in the config schema but are not yet wired to a watcher.

To rotate certificates today — including with cert-manager, Vault, or ACME renewals — write the new certificate and key to the configured paths and restart the server (for example, a rolling restart across cluster nodes) to pick them up.

## Network security

- FrogDB binds to `127.0.0.1` by default; it is not exposed on external interfaces until you configure a public bind address.
- Keep the admin RESP port and the HTTP/metrics port on localhost or a private network; the HTTP server is plaintext by default, so restrict it with network controls and an `[http] token`.
- Use TLS for any traffic that crosses a trust boundary — client, replication, and cluster-bus connections each have their own toggle above.

## See also

- [Clustering](/operations/clustering/) — admin-API authentication and cluster topology.
- [Observability](/operations/observability/) — metrics-port exposure and scraping.
- [Configuration reference](/reference/configuration/) — every `[security]`, `[acl]`, and `[tls]` parameter.
