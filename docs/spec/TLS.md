# FrogDB TLS

This document specifies TLS support for FrogDB: encrypted client connections, replication, cluster bus, mutual TLS, certificate hot-reloading, and operational certificate rotation.

## Overview

FrogDB uses [rustls](https://github.com/rustls/rustls) as its TLS backend (pure Rust, no OpenSSL dependency). The project already uses rustls transitively via reqwest in the telemetry and browser-test crates.

### Scope

TLS applies to:

| Traffic | TLS Support | Default |
|---------|-------------|---------|
| Client connections | Yes (dedicated `tls_port`) | Off |
| Replication (PSYNC) | Yes (`tls_replication = true`) | Off |
| Cluster bus (Raft RPC) | Yes (`tls_cluster = true`) | Off |
| Admin RESP port | Configurable (`no_tls_on_admin_port`) | Plaintext |
| Metrics HTTP port | No | Always plaintext |

### Design Principles

1. **Dual-port mode** - Plaintext `port` and encrypted `tls_port` run simultaneously, enabling gradual migration
2. **Explicit opt-in per subsystem** - Client TLS, replication TLS, and cluster TLS are independently controlled (following Redis/Valkey pattern)
3. **Hot-reloading** - Certificate rotation without restart or connection interruption (differentiating feature vs Redis/Valkey/DragonflyDB)
4. **rustls only** - No OpenSSL dependency; consistent cross-platform behavior

---

## Configuration

### `[tls]` TOML Section

```toml
[tls]
enabled = false                     # Master TLS switch
cert_file = ""                      # Path to PEM-encoded server certificate chain
key_file = ""                       # Path to PEM-encoded private key
ca_file = ""                        # Path to PEM-encoded CA bundle (for client cert validation)
tls_port = 6380                     # TLS listener port (0 = disabled)
require_client_cert = "none"        # Client cert mode: "none", "optional", "required"
protocols = ["TLS1.3", "TLS1.2"]   # Allowed TLS protocol versions
ciphersuites = []                   # Allowed ciphersuites (empty = rustls defaults)

# Per-subsystem flags
tls_replication = false             # Encrypt replication connections
tls_cluster = false                 # Encrypt cluster bus (Raft RPC)

# Admin port opt-out (DragonflyDB pattern)
no_tls_on_admin_port = true         # When true, admin port stays plaintext even when TLS is enabled

# Outgoing connection certs (for replication/cluster as client)
client_cert_file = ""               # Client cert for outgoing connections (default: use cert_file)
client_key_file = ""                # Client key for outgoing connections (default: use key_file)

# Hot-reload options
watch_certs = true                  # Enable filesystem watching for cert changes
watch_debounce_ms = 500             # Debounce interval for file watcher events

# Handshake limits
handshake_timeout_ms = 10000        # Max time for TLS handshake (slowloris protection)
```

### Environment Variable Overrides

All TLS settings follow the standard `FROGDB_` prefix with double-underscore nesting:

```bash
FROGDB_TLS__ENABLED=true
FROGDB_TLS__CERT_FILE=/etc/frogdb/tls/server.crt
FROGDB_TLS__KEY_FILE=/etc/frogdb/tls/server.key
FROGDB_TLS__CA_FILE=/etc/frogdb/tls/ca.crt
FROGDB_TLS__TLS_PORT=6380
FROGDB_TLS__REQUIRE_CLIENT_CERT=required
FROGDB_TLS__TLS_REPLICATION=true
FROGDB_TLS__TLS_CLUSTER=true
FROGDB_TLS__NO_TLS_ON_ADMIN_PORT=false
```

### CLI Arguments

```bash
frogdb-server \
  --tls-enabled \
  --tls-cert-file /path/to/cert.pem \
  --tls-key-file /path/to/key.pem \
  --tls-ca-file /path/to/ca.pem \
  --tls-port 6380 \
  --tls-require-client-cert required \
  --tls-replication \
  --tls-cluster
```

### CONFIG GET Support

TLS parameters are **immutable** (read-only via CONFIG GET, not settable via CONFIG SET). Certificate changes use hot-reloading instead.

```
CONFIG GET tls-cert-file        → "/etc/frogdb/tls/server.crt"
CONFIG GET tls-port             → "6380"
CONFIG GET tls-require-*        → "required"
```

See [CONFIGURATION.md](CONFIGURATION.md) for the full parameter mutability table.

### Validation Rules

At startup, FrogDB validates TLS configuration and fails fast on errors:

| Rule | Error |
|------|-------|
| `enabled = true` requires `cert_file` and `key_file` | `ERR tls-cert-file and tls-key-file are required when TLS is enabled` |
| `require_client_cert != "none"` requires `ca_file` | `ERR tls-ca-file is required when client certificates are requested` |
| `tls_port` must not conflict with `port` or `admin_port` | `ERR tls-port conflicts with port/admin-port` |
| `tls_replication = true` requires `enabled = true` | `ERR tls-replication requires TLS to be enabled` |
| `tls_cluster = true` requires `enabled = true` | `ERR tls-cluster requires TLS to be enabled` |
| Certificate files must be readable PEM | `ERR failed to load certificate: <reason>` |
| Private key must match certificate | `ERR private key does not match certificate` |
| `protocols` must contain at least one valid version | `ERR at least one TLS protocol version must be specified` |

---

## Port-by-Port TLS Policy

FrogDB exposes up to 5 ports. Each has an independent TLS policy:

| Port | Default | Protocol | TLS Behavior |
|------|---------|----------|--------------|
| Main client (`port`, default 6379) | Plaintext | RESP2/RESP3 | Always plaintext; TLS clients use `tls_port` instead |
| TLS client (`tls_port`, default 6380) | Encrypted | RESP2/RESP3 | Always TLS when `tls.enabled = true` |
| Admin RESP (`admin_port`, default 6381) | Plaintext | RESP2/RESP3 | Plaintext by default; inherits TLS if `no_tls_on_admin_port = false` |
| Metrics HTTP (`metrics.port`, default 9090) | Plaintext | HTTP | Always plaintext (Prometheus scraping convention) |
| Cluster bus (`port + 10000`, default 16379) | Plaintext | Raft RPC | TLS when `tls_cluster = true` |

**Port conflict note:** When `tls.enabled = true`, `tls_port` defaults to 6380. If `admin_port` is also 6380 (its default), FrogDB will fail startup with a port conflict error. Configure non-overlapping ports explicitly.

### Admin Port TLS Rationale

The admin port defaults to plaintext because:
- Simplifies orchestrator configuration (no client certs needed)
- Admin port should be bound to localhost or private network (see [CONNECTION.md](CONNECTION.md#admin-port))
- Reduces TLS overhead for high-frequency health checks
- Matches DragonflyDB's `--no_tls_on_admin_port` behavior

When `no_tls_on_admin_port = false`, the admin port uses the same TLS configuration as the main TLS port, including mTLS if configured.

### Metrics Port Rationale

The metrics port is always plaintext because:
- Prometheus scrapers expect plaintext HTTP by default
- Metrics data is not sensitive (counters, gauges)
- TLS would complicate Prometheus configuration in every deployment
- Standard practice across Redis, DragonflyDB, and most database systems

---

## Connection Flow

### Accept Flow with TLS

```
                    ┌─────────────┐
                    │  TCP Accept │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  maxclients │  (checked BEFORE TLS to avoid
                    │  check      │   expensive handshakes under load)
                    └──────┬──────┘
                           │
                  ┌────────▼────────┐
                  │  TLS or plain?  │  (determined by which port
                  │                 │   accepted the connection)
                  └───┬─────────┬──┘
                      │         │
               Plain  │         │  TLS port
                      │         │
                      │    ┌────▼────────────┐
                      │    │  TLS Handshake   │
                      │    │  (with timeout)  │
                      │    └────┬─────────────┘
                      │         │
                      │    ┌────▼────────────┐
                      │    │  mTLS client    │  (if require_client_cert
                      │    │  cert check     │   != "none")
                      │    └────┬─────────────┘
                      │         │
                  ┌───▼─────────▼──┐
                  │  Protocol       │
                  │  detection      │  (RESP2/RESP3 via HELLO)
                  │  (RESP2/RESP3) │
                  └────────┬───────┘
                           │
                    ┌──────▼──────┐
                    │  Command    │
                    │  loop       │
                    └─────────────┘
```

### Stream Abstraction

The acceptor produces either a plain TCP stream or a TLS-wrapped stream, unified via an enum:

```rust
/// Unified stream type for TLS-optional connections.
/// Implements AsyncRead + AsyncWrite so command processing
/// is agnostic to encryption.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
}
```

Both variants implement `AsyncRead + AsyncWrite`, so the command processing pipeline is unchanged regardless of encryption.

### Turmoil Compatibility

Under `feature = "turmoil"`, TLS is disabled. The `crate::net` abstraction (see `crates/server/src/net.rs`) already swaps Tokio's TCP types for Turmoil's simulated networking. TLS is orthogonal to concurrency simulation and would interfere with Turmoil's network fault injection.

```rust
#[cfg(feature = "turmoil")]
pub type ConnectionStream = turmoil::net::TcpStream;

#[cfg(not(feature = "turmoil"))]
pub type ConnectionStream = MaybeTlsStream;
```

### Handshake Timeout

A TLS handshake timeout (`handshake_timeout_ms`, default 10s) protects against slowloris-style attacks where a client opens a connection but never completes the handshake:

```rust
match tokio::time::timeout(
    Duration::from_millis(config.tls.handshake_timeout_ms),
    tls_acceptor.accept(tcp_stream),
).await {
    Ok(Ok(tls_stream)) => { /* proceed */ }
    Ok(Err(e)) => {
        metrics.tls_handshake_errors.inc();
        // Log and close
    }
    Err(_timeout) => {
        metrics.tls_handshake_timeouts.inc();
        // Close connection
    }
}
```

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_tls_handshake_duration_seconds` | Histogram | Time to complete TLS handshake |
| `frogdb_tls_handshake_errors_total` | Counter | Failed TLS handshakes (by `reason` label) |
| `frogdb_tls_handshake_timeouts_total` | Counter | Handshakes that exceeded timeout |
| `frogdb_tls_connections_current` | Gauge | Current TLS-encrypted connections |

---

## Certificate Hot-Reloading

### Motivation

Certificates often have short lifetimes (90 days for Let's Encrypt, 30 days or less in enterprise PKI). Hot-reloading avoids downtime during certificate rotation.

**This is a differentiating feature** - neither Redis, Valkey, nor DragonflyDB support TLS hot-reloading natively.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      TlsManager                          │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Arc<ArcSwap<rustls::ServerConfig>>              │   │
│  │  ├── Current server TLS config                    │   │
│  │  └── Atomically swappable (lock-free reads)       │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Arc<ArcSwap<rustls::ClientConfig>>              │   │
│  │  ├── Current client TLS config (outgoing conns)   │   │
│  │  └── Used for replication & cluster connections    │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  Reload Triggers:                                        │
│  ├── File watcher (notify crate, debounced)              │
│  ├── SIGUSR1 signal                                      │
│  └── CONFIG RELOAD-CERTS command                         │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### TlsManager

```rust
pub struct TlsManager {
    /// Current server TLS config (swapped atomically on reload)
    server_config: Arc<ArcSwap<Arc<rustls::ServerConfig>>>,

    /// Current client TLS config (for outgoing replication/cluster connections)
    client_config: Arc<ArcSwap<Arc<rustls::ClientConfig>>>,

    /// File watcher (optional, when watch_certs = true)
    _watcher: Option<notify::RecommendedWatcher>,

    /// Certificate file paths
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: Option<PathBuf>,

    /// Metrics
    reload_total: Counter,
    reload_errors: Counter,
    cert_expiry: Gauge,
}
```

### Reload Triggers

Three mechanisms can trigger a certificate reload:

| Trigger | When to Use | How |
|---------|-------------|-----|
| **File watcher** | Automated cert managers (cert-manager, Vault agent) | `notify` crate watches `cert_file` and `key_file` for changes |
| **SIGUSR1 signal** | Manual rotation, scripts, systemd timers | `kill -USR1 <pid>` or `systemctl reload frogdb` |
| **CONFIG RELOAD-CERTS** | Remote management via redis-cli | `redis-cli CONFIG RELOAD-CERTS` |

### Reload Flow

```
Trigger (file change / signal / command)
    │
    ▼
Load new cert + key from disk
    │
    ▼
Parse PEM, validate cert chain
    │
    ├── Failure → Log error, increment reload_errors,
    │             keep old config, return error
    │
    ▼ Success
Build new rustls::ServerConfig
    │
    ▼
Build new rustls::ClientConfig (if client certs configured)
    │
    ▼
Atomic swap via ArcSwap::store()
    │
    ▼
Update cert_expiry_seconds metric
    │
    ▼
Increment reload_total metric
    │
    ▼
Log: "TLS certificates reloaded successfully"
```

### Reload Semantics

| Aspect | Behavior |
|--------|----------|
| Existing connections | Keep old certificates until disconnect |
| New connections | Use new certificates immediately |
| Reload failure | Keep old certificates, log error, increment metric |
| Concurrent reloads | Serialized (Mutex around reload logic) |
| File watcher debounce | 500ms (configurable via `watch_debounce_ms`) to handle atomic file replacements |

### File Watcher Debouncing

Certificate management tools (cert-manager, Vault) often perform multi-step file operations:
1. Write new cert to temp file
2. Rename temp file over old cert (atomic on same filesystem)
3. Update symlink

The file watcher debounces events to avoid reloading partially-written files:

```rust
watcher.watch(&cert_path, RecursiveMode::NonRecursive)?;
watcher.watch(&key_path, RecursiveMode::NonRecursive)?;

// Debounce: wait 500ms after last event before reloading
let debounced = debouncer::new(Duration::from_millis(config.tls.watch_debounce_ms));
```

---

## Certificate Rotation (Operational)

This section covers operational procedures for rotating certificates in production.

### Rotation Workflow

```
1. Place new certificate files
   └── Copy or symlink to configured paths

2. Trigger reload
   └── Automatic (file watcher) or manual (SIGUSR1 / CONFIG RELOAD-CERTS)

3. Verify
   └── Check metrics: tls_cert_reload_total incremented
   └── Check logs: "TLS certificates reloaded successfully"
   └── Test new connection: openssl s_client -connect host:port

4. Monitor
   └── tls_cert_expiry_seconds shows new expiry date
```

### Symlink-Based Rotation

For atomic certificate replacement, use symlinks (recommended for Kubernetes/cert-manager):

```bash
# Initial setup
/etc/frogdb/tls/
├── live -> ./generation-1/
├── generation-1/
│   ├── server.crt
│   └── server.key

# Rotation
mkdir /etc/frogdb/tls/generation-2/
cp new-server.crt /etc/frogdb/tls/generation-2/server.crt
cp new-server.key /etc/frogdb/tls/generation-2/server.key

# Atomic swap
ln -sfn ./generation-2/ /etc/frogdb/tls/live

# Trigger reload (if file watcher doesn't detect symlink change)
kill -USR1 $(pidof frogdb-server)
```

### Integration with Certificate Managers

| Tool | Integration |
|------|-------------|
| **cert-manager (K8s)** | Mount Secret as volume, file watcher detects updates automatically |
| **HashiCorp Vault** | Vault Agent sidecar writes certs to shared volume, file watcher detects |
| **ACME / Let's Encrypt** | Renewal script writes certs, sends SIGUSR1 |
| **Manual** | Operator replaces files, runs `CONFIG RELOAD-CERTS` |

### Monitoring

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_tls_cert_expiry_seconds` | Gauge | Unix timestamp when the current server certificate expires |
| `frogdb_tls_cert_reload_total` | Counter | Total successful certificate reloads |
| `frogdb_tls_cert_reload_errors_total` | Counter | Failed certificate reload attempts (by `reason` label) |
| `frogdb_tls_cert_not_after` | Gauge | Certificate notAfter as Unix timestamp (alias for expiry) |

### Alerting Recommendations

```yaml
# Prometheus alerting rules (example)
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

      - alert: FrogDBCertExpiryCritical
        expr: frogdb_tls_cert_expiry_seconds - time() < 1 * 86400
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "FrogDB TLS certificate expires in less than 24 hours"

      - alert: FrogDBCertReloadFailing
        expr: increase(frogdb_tls_cert_reload_errors_total[1h]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "FrogDB TLS certificate reload is failing"
```

---

## mTLS (Client Certificate Authentication)

### Modes

The `require_client_cert` setting controls client certificate behavior:

| Mode | Behavior |
|------|----------|
| `none` (default) | Server does not request client certificates |
| `optional` | Server requests a client certificate; connection allowed without one |
| `required` | Server requires a valid client certificate; connection rejected without one |

### CA Validation

When `require_client_cert` is `optional` or `required`, client certificates are validated against the CA bundle specified in `ca_file`:

- Certificate must be signed by a CA in the bundle (direct or chain)
- Certificate must not be expired
- Certificate chain must be complete

### Identity Extraction

When a client presents a valid certificate, FrogDB extracts identity information for logging and auditing:

| Field | Source | Usage |
|-------|--------|-------|
| Common Name (CN) | Subject CN | Logged in connection events, available via `CLIENT INFO` |
| Subject Alternative Names (SANs) | SAN extension | DNS names and IP addresses, logged alongside CN |

### CLIENT INFO Extension

When TLS is active, `CLIENT INFO` includes additional fields:

```
id=123 addr=192.168.1.10:54321 ... tls-version=TLSv1.3 tls-cipher=TLS_AES_256_GCM_SHA384 tls-client-cn=app-service tls-client-san=app.example.com
```

| Field | Description |
|-------|-------------|
| `tls-version` | Negotiated TLS protocol version |
| `tls-cipher` | Negotiated cipher suite |
| `tls-client-cn` | Client certificate CN (empty if no client cert) |
| `tls-client-san` | Client certificate SAN (first DNS name, empty if none) |

### Future: CN/SAN to ACL User Mapping

**[Not Yet Implemented]** A future enhancement will allow mapping client certificate identity to ACL users, enabling certificate-based authentication without passwords:

```toml
# Future configuration (not yet implemented)
[tls.cert_auth]
enabled = false
# Map client cert CN to ACL user
cn_user_map = { "app-service" = "app", "admin-tool" = "admin" }
# Or use a pattern: CN becomes the ACL username directly
cn_as_username = false
```

This would allow mTLS to serve as the sole authentication mechanism, with the certificate identity determining the ACL user and permissions. See [AUTH.md](AUTH.md) for the ACL system.

---

## Replication TLS

When `tls_replication = true`, replication traffic between primary and replica is encrypted.

### Configuration

```toml
[tls]
enabled = true
cert_file = "/etc/frogdb/tls/server.crt"
key_file = "/etc/frogdb/tls/server.key"
tls_replication = true

# Optional: separate client cert for outgoing replication connections
# If not set, server cert/key are used for outgoing connections too
client_cert_file = "/etc/frogdb/tls/client.crt"
client_key_file = "/etc/frogdb/tls/client.key"
```

### Behavior

| Aspect | Behavior |
|--------|----------|
| Replica connects to primary | Uses primary's `tls_port` (not plaintext `port`) |
| PSYNC handshake | Performed over TLS connection |
| WAL streaming | Encrypted end-to-end |
| Server cert as client cert | Server certificate doubles as client cert for outgoing connections (default) |
| Separate client certs | `client_cert_file` / `client_key_file` override for outgoing connections (Redis pattern) |
| REPLICAOF command | When issued, checks `tls_replication` to determine whether to connect via TLS |

### Certificate Requirements

For replication TLS:
- Primary must have TLS enabled with valid server certificate
- Replica must trust the primary's CA (via `ca_file` if mTLS, or system trust store)
- If primary requires client certs (`require_client_cert = required`), replica must present a valid client certificate

---

## Cluster Bus TLS

When `tls_cluster = true`, inter-node Raft RPC connections are encrypted.

### Configuration

```toml
[tls]
enabled = true
cert_file = "/etc/frogdb/tls/server.crt"
key_file = "/etc/frogdb/tls/server.key"
tls_cluster = true

# Optional: dedicated cluster certs (default: same as client TLS)
# cluster_cert_file = "/etc/frogdb/tls/cluster.crt"
# cluster_key_file = "/etc/frogdb/tls/cluster.key"
```

### Behavior

| Aspect | Behavior |
|--------|----------|
| Raft RPC | AppendEntries, Vote, InstallSnapshot all encrypted |
| Cluster bus port | Same port (`port + 10000`), now expects TLS |
| Certificate source | Uses server cert/key by default, or dedicated cluster certs |
| Peer validation | Nodes validate each other's certificates against `ca_file` |

### Cluster TLS Requirements

**All nodes must agree on TLS configuration.** A cluster cannot have some nodes using TLS and others using plaintext on the cluster bus (outside of a migration window).

### Mixed-Mode Migration

To migrate a running cluster from plaintext to TLS on the cluster bus:

```
Phase 1: Deploy new certs to all nodes (no TLS yet)
    └── All nodes have cert_file, key_file, ca_file configured
    └── tls_cluster = false

Phase 2: Enable dual-accept on cluster bus (temporary)
    └── tls_cluster_migration = true  (accepts both TLS and plaintext)
    └── Rolling restart all nodes

Phase 3: Enable TLS-only
    └── tls_cluster = true
    └── tls_cluster_migration = false
    └── Rolling restart all nodes

Phase 4: Clean up
    └── Remove tls_cluster_migration from config
```

During Phase 2, each node's cluster bus acceptor tries TLS first; if the handshake fails with a non-TLS client hello, it falls back to plaintext. This is a temporary mode only for migration.

---

## Implementation Architecture

### File Layout

```
crates/server/src/
├── config/
│   └── tls.rs          # TlsConfig struct, validation, parsing
├── tls.rs              # TlsManager, hot-reload, cert loading
└── net.rs              # Extended with MaybeTlsStream
```

### TlsConfig

```rust
/// TLS configuration parsed from frogdb.toml [tls] section.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub tls_port: u16,
    pub require_client_cert: ClientCertMode,
    pub protocols: Vec<TlsProtocol>,
    pub ciphersuites: Vec<String>,
    pub tls_replication: bool,
    pub tls_cluster: bool,
    pub no_tls_on_admin_port: bool,
    pub client_cert_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub watch_certs: bool,
    pub watch_debounce_ms: u64,
    pub handshake_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientCertMode {
    None,
    Optional,
    Required,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum TlsProtocol {
    #[serde(rename = "TLS1.2")]
    Tls12,
    #[serde(rename = "TLS1.3")]
    Tls13,
}
```

### Acceptor Changes

The TCP acceptor is extended to produce `MaybeTlsStream`:

```rust
// In the accept loop for the TLS port:
let tcp_stream = tls_listener.accept().await?;

let tls_config = tls_manager.current_server_config();
let tls_acceptor = TlsAcceptor::from(tls_config);

let tls_stream = tokio::time::timeout(
    handshake_timeout,
    tls_acceptor.accept(tcp_stream),
).await??;

let stream = MaybeTlsStream::Tls(tls_stream);
// Hand off to connection handler (same as plaintext path)
```

### Turmoil Feature Gate

TLS is disabled when building with Turmoil simulation:

```rust
#[cfg(not(feature = "turmoil"))]
mod tls;

#[cfg(not(feature = "turmoil"))]
pub use tls::TlsManager;
```

Concurrency simulation tests don't need encryption - they test data structure correctness, shard coordination, and fault injection, all of which are orthogonal to TLS.

---

## Testing Strategy

### Unit Tests

| Test | Description |
|------|-------------|
| Config validation | Verify all validation rules produce correct errors |
| Cert loading | Load valid and invalid PEM files, check error messages |
| Reload logic | Swap certs via `ArcSwap`, verify new config is returned |
| Protocol filtering | Verify TLS 1.2/1.3 version restrictions work |

### Integration Tests

| Test | Description |
|------|-------------|
| TLS connection | Connect via `tls_port`, execute commands |
| Plaintext connection | Connect via `port`, verify plaintext still works |
| Dual-port mode | Both ports active simultaneously |
| mTLS handshake | Client presents valid cert, connection succeeds |
| mTLS rejection | Client presents no cert when required, connection rejected |
| mTLS invalid cert | Client presents cert signed by wrong CA, connection rejected |
| Hot-reload | Replace cert files, verify new connections use new cert |
| Admin port plaintext | Admin port stays plaintext when `no_tls_on_admin_port = true` |

### Test Certificate Generation

Tests use ephemeral certificates generated at test time via the `rcgen` crate:

```
tests/certs/  (generated by test helpers, not checked in)
├── ca.crt              # Self-signed CA
├── ca.key
├── server.crt          # Server cert signed by CA
├── server.key
├── client.crt          # Client cert signed by CA
├── client.key
├── expired.crt         # Expired server cert (for negative tests)
├── expired.key
└── wrong-ca.crt        # Cert signed by different CA
```

### Test Helper

```rust
/// Builder for creating test servers with TLS.
/// Generates ephemeral certs via rcgen so tests are self-contained.
pub struct TlsTestServer {
    ca: rcgen::Certificate,
    server_cert: rcgen::Certificate,
    // ...
}

impl TlsTestServer {
    pub fn new() -> Self { /* generate CA + server cert */ }
    pub fn with_client_cert(mut self) -> Self { /* generate client cert */ }
    pub fn with_expired_cert(mut self) -> Self { /* generate expired cert */ }
    pub fn start(self) -> TestServerHandle { /* start server with TLS */ }
}
```

### Negative Tests

| Test | Expected Behavior |
|------|-------------------|
| Expired server cert | Client rejects connection |
| Wrong CA | Client rejects connection (cert not trusted) |
| No client cert when required | Server rejects connection |
| Invalid client cert | Server rejects connection |
| Plaintext on TLS port | Server rejects (TLS handshake fails) |
| TLS on plaintext port | Connection fails (not a TLS listener) |
| Handshake timeout | Connection closed after `handshake_timeout_ms` |

### No TLS in Turmoil Tests

TLS is not exercised in Turmoil concurrency tests. TLS is a transport-layer concern orthogonal to the shared-nothing data correctness that Turmoil tests verify.

---

## Comparison with Redis/Valkey/DragonflyDB

| Feature | FrogDB | Redis 7.x | Valkey 8.x | DragonflyDB |
|---------|--------|-----------|------------|-------------|
| TLS backend | rustls | OpenSSL | OpenSSL | OpenSSL |
| Dual-port (plain + TLS) | Yes | Yes (`tls-port`) | Yes | Yes |
| TLS replication flag | `tls_replication` | `tls-replication yes` | `tls-replication yes` | `--tls_replication` |
| TLS cluster bus flag | `tls_cluster` | `tls-cluster yes` | `tls-cluster yes` | N/A (no cluster) |
| Admin port TLS opt-out | `no_tls_on_admin_port` | N/A (no admin port) | N/A | `--no_tls_on_admin_port` |
| mTLS | `require_client_cert` | `tls-auth-clients` | `tls-auth-clients` | `--tls_ca_cert_file` |
| Certificate hot-reload | File watch + signal + command | No (restart required) | No (restart required) | No (restart required) |
| Cert expiry metric | `tls_cert_expiry_seconds` | No | No | No |
| Config format | TOML `[tls]` section | `tls-*` directives | `tls-*` directives | gflags `--tls_*` |
| Protocol control | `protocols` list | `tls-protocols` | `tls-protocols` | `--tls_protocols` |
| Cipher control | `ciphersuites` list | `tls-ciphersuites` | `tls-ciphersuites` | Limited |

**Key differentiators:**
- **Hot-reloading**: FrogDB is the only system supporting cert hot-reload without restart
- **rustls**: No OpenSSL dependency, consistent cross-platform behavior, memory-safe TLS
- **Cert expiry metric**: Built-in monitoring for certificate lifecycle

---

## References

- [CONFIGURATION.md](CONFIGURATION.md) - Configuration system, parameter mutability
- [CONNECTION.md](CONNECTION.md) - Connection lifecycle, admin port
- [AUTH.md](AUTH.md) - ACL system (orthogonal to TLS)
- [CLUSTER.md](CLUSTER.md) - Cluster bus, orchestrator security
- [REPLICATION.md](REPLICATION.md) - Replication protocol (PSYNC)
- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics reference
- [LIFECYCLE.md](LIFECYCLE.md) - Startup sequence (TLS init position)
- [rustls documentation](https://docs.rs/rustls)
- [tokio-rustls documentation](https://docs.rs/tokio-rustls)
- [Redis TLS documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [DragonflyDB TLS documentation](https://www.dragonflydb.io/docs/managing-dragonfly/tls)
