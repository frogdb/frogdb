# TLS Implementation Plan

## Context

FrogDB has a comprehensive TLS spec (`docs/spec/TLS.md`, ~850 lines) covering encrypted client
connections, mTLS, certificate hot-reloading, replication TLS, cluster bus TLS, and mixed-mode
cluster migration. Currently **zero** TLS infrastructure exists — all connections are plain TCP.
This plan implements the full spec.

**Breaking change**: Admin port default moves from 6380 → 6381 to avoid conflicting with `tls_port`
default (6380). Acceptable since FrogDB is pre-production.

---

## Phase 1: Dependencies + TlsConfig

**Goal**: Add crate dependencies and the `[tls]` config section. No behavioral changes.

### 1a. Workspace dependencies (`Cargo.toml`)

Add to `[workspace.dependencies]`:
- `rustls = "0.23"` — TLS implementation
- `tokio-rustls = "0.26"` — async TLS for tokio
- `arc-swap = "1"` — lock-free atomic pointer swaps (hot-reload)
- `notify = "7"` — filesystem watcher (cert hot-reload)
- `rustls-pemfile = "2"` — PEM file parsing
- `x509-parser = "0.16"` — cert inspection (expiry, CN/SAN extraction)
- `webpki-roots = "0.26"` — system CA trust store
- `rcgen = "0.13"` — test cert generation (dev-dependency only)

### 1b. Crate dependencies

- `frogdb-server/crates/server/Cargo.toml`: Add `rustls`, `tokio-rustls`, `arc-swap`, `notify`,
  `rustls-pemfile`, `x509-parser`, `webpki-roots`. Add `rcgen` to `[dev-dependencies]`.
- `frogdb-server/crates/replication/Cargo.toml`: Add `tokio-rustls`, `rustls`.
- Cluster crate (if separate): Add `tokio-rustls`, `arc-swap`.

### 1c. Create `crates/server/src/config/tls.rs`

Define:
```rust
pub struct TlsConfig {
    pub enabled: bool,                         // default: false
    pub cert_file: PathBuf,                    // required when enabled
    pub key_file: PathBuf,                     // required when enabled
    pub ca_file: Option<PathBuf>,              // required for mTLS
    pub tls_port: u16,                         // default: 6380
    pub require_client_cert: ClientCertMode,   // default: None
    pub protocols: Vec<TlsProtocol>,           // default: [TLS1.3, TLS1.2]
    pub ciphersuites: Vec<String>,             // default: [] (rustls defaults)
    pub tls_replication: bool,                 // default: false
    pub tls_cluster: bool,                     // default: false
    pub tls_cluster_migration: bool,           // default: false (dual-accept mode)
    pub no_tls_on_admin_port: bool,            // default: true
    pub client_cert_file: Option<PathBuf>,     // for outgoing replication/cluster
    pub client_key_file: Option<PathBuf>,
    pub watch_certs: bool,                     // default: true
    pub watch_debounce_ms: u64,                // default: 500
    pub handshake_timeout_ms: u64,             // default: 10000
}

pub enum ClientCertMode { None, Optional, Required }
pub enum TlsProtocol { Tls12, Tls13 }
```

Follow existing config patterns: `Serialize`, `Deserialize`, `JsonSchema`, `Default`,
`deny_unknown_fields`, serde default functions.

### 1d. Register in config module

- `crates/server/src/config/mod.rs`: Add `pub mod tls;`, `pub use tls::TlsConfig;`
- Add `#[serde(default)] pub tls: TlsConfig` field to `Config` struct
- Call `self.tls.validate()?` in `Config::validate()`

### 1e. TlsConfig::validate()

Validation rules per spec:
- `enabled = true` requires non-empty `cert_file` and `key_file`
- `require_client_cert != None` requires `ca_file`
- `tls_replication = true` requires `enabled = true`
- `tls_cluster = true` requires `enabled = true`
- `tls_cluster_migration = true` requires `tls_cluster = true`
- `protocols` must have at least one entry
- Cert/key files must exist and parse as valid PEM at startup

### 1f. Port conflict validator

Add `TlsPortConflictValidator` in `crates/server/src/config/validators/network.rs`:
- `tls_port` != `server.port`, `admin.port`, `http.port` when `tls.enabled`

### 1g. Change admin default port

`crates/server/src/config/admin.rs`: Change `DEFAULT_ADMIN_PORT` from `6380` to `6381`.

### 1h. CLI arguments

Add to `Cli` struct in `crates/server/src/main.rs`:
`--tls-enabled`, `--tls-cert-file`, `--tls-key-file`, `--tls-ca-file`, `--tls-port`,
`--tls-require-client-cert`, `--tls-replication`, `--tls-cluster`.
Apply overrides in `Config::load()`.

### 1i. CONFIG GET support

Register read-only TLS parameters in `ConfigManager` (`crates/server/src/runtime_config.rs`).

**Files**: `Cargo.toml`, `crates/server/Cargo.toml`, `crates/replication/Cargo.toml`,
`crates/server/src/config/tls.rs` (new), `crates/server/src/config/mod.rs`,
`crates/server/src/config/admin.rs`, `crates/server/src/config/validators/network.rs`,
`crates/server/src/main.rs`, `crates/server/src/runtime_config.rs`

**Verify**: `just check && just lint && just test frogdb-server`

---

## Phase 2: MaybeTlsStream + ConnectionStream Abstraction

**Goal**: Introduce stream abstraction without changing runtime behavior. Everything still plain TCP.

### 2a. Define MaybeTlsStream in `crates/server/src/net.rs`

```rust
#[cfg(not(feature = "turmoil"))]
pub enum MaybeTlsStream {
    Plain(tokio::net::TcpStream),
    Tls(tokio_rustls::server::TlsStream<tokio::net::TcpStream>),
}
```

Implement `AsyncRead + AsyncWrite` by delegating to inner variant (pin projection).
Add helper methods: `peer_addr()`, `set_nodelay()`, `local_addr()` (delegate to inner TcpStream,
using `get_ref().0` for TLS variant).

### 2b. ConnectionStream type alias

```rust
#[cfg(feature = "turmoil")]
pub type ConnectionStream = turmoil::net::TcpStream;

#[cfg(not(feature = "turmoil"))]
pub type ConnectionStream = MaybeTlsStream;
```

### 2c. Update ConnectionHandler

- `crates/server/src/connection.rs`: Change `framed: Framed<TcpStream, Resp2>` →
  `framed: Framed<ConnectionStream, Resp2>`
- `from_deps()`: Accept `ConnectionStream` instead of `TcpStream`
- PSYNC handoff path: `self.framed.into_inner()` now returns `ConnectionStream`.
  For non-turmoil, add `MaybeTlsStream::into_tcp_stream()` that returns the inner `TcpStream`
  (for `Plain`, unwrap directly; for `Tls`, call `into_inner().0` to get underlying TCP stream).

### 2d. Update Acceptor

- `crates/server/src/acceptor.rs`: After `listener.accept()`, wrap socket:
  `#[cfg(not(feature = "turmoil"))] let socket = MaybeTlsStream::Plain(socket);`
- Pass `ConnectionStream` to `ConnectionHandler::from_deps()`

### 2e. Update dependent types

Search for all uses of `crate::net::TcpStream` in the server crate and update to
`ConnectionStream` where they participate in the connection pipeline.

**Files**: `crates/server/src/net.rs`, `crates/server/src/connection.rs`,
`crates/server/src/connection/deps.rs`, `crates/server/src/acceptor.rs`

**Verify**: `just check && just test frogdb-server && just concurrency`
(turmoil path must still compile and pass)

---

## Phase 3: TlsManager + TLS Acceptor

**Goal**: Core TLS functionality — load certs, accept TLS connections.

### 3a. Create `crates/server/src/tls.rs`

Feature-gated: `#[cfg(not(feature = "turmoil"))]`

```rust
pub struct TlsManager {
    server_config: Arc<ArcSwap<Arc<rustls::ServerConfig>>>,
    client_config: Arc<ArcSwap<Arc<rustls::ClientConfig>>>,
    cert_file: PathBuf,
    key_file: PathBuf,
    ca_file: Option<PathBuf>,
    client_cert_file: Option<PathBuf>,
    client_key_file: Option<PathBuf>,
    tls_config: TlsConfig,  // for rebuild parameters
    reload_mutex: tokio::sync::Mutex<()>,
    metrics: Arc<dyn MetricsRecorder>,
}
```

Key methods:
- `new(config: &TlsConfig, metrics: Arc<dyn MetricsRecorder>) -> Result<Self>` — load certs,
  build initial configs, extract cert expiry
- `current_server_config() -> Arc<rustls::ServerConfig>` — load from ArcSwap
- `current_client_config() -> Arc<rustls::ClientConfig>` — load from ArcSwap
- `reload() -> Result<()>` — reload from disk, rebuild, atomic swap, update metrics

Helper functions:
- `load_certs(path: &Path) -> Result<Vec<CertificateDer>>`
- `load_private_key(path: &Path) -> Result<PrivateKeyDer>`
- `load_ca_certs(path: &Path) -> Result<RootCertStore>`
- `build_server_config(certs, key, ca, client_cert_mode, protocols, ciphersuites) -> Result<ServerConfig>`
- `build_client_config(certs, key, ca_roots) -> Result<ClientConfig>`
- `extract_cert_expiry(cert_der: &[u8]) -> Result<i64>` — using x509-parser

### 3b. Add TlsManager to Server

`crates/server/src/server/mod.rs`:
- Add `tls_manager: Option<Arc<TlsManager>>` and `tls_listener: Option<TcpListener>` to `Server`
- In `with_listeners()`: if `config.tls.enabled`, create `TlsManager` and bind TLS listener
- Add `pub tls: Option<TcpListener>` to `ServerListeners`

### 3c. Extend Acceptor with TLS handshake

`crates/server/src/acceptor.rs`:
- Add fields: `tls_manager: Option<Arc<TlsManager>>`, `tls_handshake_timeout: Duration`
- In accept loop, after `set_nodelay()`:
  - If `tls_manager.is_some()`: get server config, create `tokio_rustls::TlsAcceptor`, perform
    handshake with timeout, wrap in `MaybeTlsStream::Tls(...)`. On error/timeout: log, increment
    metrics, `continue` the loop.
  - Else: wrap in `MaybeTlsStream::Plain(...)`

### 3d. Spawn TLS acceptor in `run_until()`

In `Server::run_until()`, spawn a third acceptor for the TLS listener (same deps as main acceptor
but with `tls_manager` set). Handle admin port TLS: if `!config.tls.no_tls_on_admin_port`, pass
`tls_manager` to the admin acceptor too.

**Files**: `crates/server/src/tls.rs` (new), `crates/server/src/lib.rs`,
`crates/server/src/server/mod.rs`, `crates/server/src/acceptor.rs`

**Verify**: Start server with TLS config + test certs. Connect via
`openssl s_client -connect 127.0.0.1:6380`. Plain port 6379 still works.

---

## Phase 4: Metrics + CLIENT INFO Extensions

**Goal**: TLS-specific metrics and CLIENT INFO output.

### 4a. Metric definitions

In `crates/telemetry/src/` (definitions and metric_names):
- `frogdb_tls_handshake_duration_seconds` (Histogram)
- `frogdb_tls_handshake_errors_total` (Counter, by reason)
- `frogdb_tls_handshake_timeouts_total` (Counter)
- `frogdb_tls_connections_current` (Gauge)
- `frogdb_tls_cert_expiry_seconds` (Gauge)
- `frogdb_tls_cert_reload_total` (Counter)
- `frogdb_tls_cert_reload_errors_total` (Counter, by reason)

### 4b. Instrument handshake in Acceptor

Record duration/errors/timeouts in the TLS handshake code from Phase 3.

### 4c. TLS info on ConnectionState

Add `tls_info: Option<TlsConnectionInfo>` to `ConnectionState`
(`crates/server/src/connection/state.rs`). Populate after successful TLS handshake by inspecting
`ServerConnection` for version, cipher, client cert CN/SAN.

### 4d. CLIENT INFO extension

`crates/server/src/connection/handlers/client.rs`: Append TLS fields when `tls_info.is_some()`:
`tls-version=TLSv1.3 tls-cipher=... tls-client-cn=... tls-client-san=...`

**Files**: `crates/telemetry/src/...`, `crates/server/src/acceptor.rs`,
`crates/server/src/connection/state.rs`, `crates/server/src/connection/handlers/client.rs`

---

## Phase 5: Replication TLS

**Goal**: Encrypted PSYNC between primary and replica.

### 5a. ReplicationStream enum in replication crate

`crates/replication/src/stream.rs` (new):
```rust
pub enum ReplicationStream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}
```
Implement `AsyncRead + AsyncWrite`. This avoids generics proliferating through the crate.

### 5b. Replica outgoing TLS

`crates/replication/src/replica.rs`: `ReplicaConnection` gains an optional `TlsConnector`.
In `connect_and_sync()`:
1. `TcpStream::connect(primary_addr)` (connect to TLS port when TLS enabled)
2. If TLS: `connector.connect(server_name, stream).await` to upgrade
3. Wrap in `ReplicationStream::Tls(...)` or `ReplicationStream::Plain(...)`

Pass `Option<Arc<rustls::ClientConfig>>` from `Server::with_listeners()` to the replica handler.

### 5c. Primary PSYNC handoff

`ConnectionHandler` PSYNC path extracts `ConnectionStream` from `Framed`. Pass the full
`MaybeTlsStream` (or extracted inner stream) to `PrimaryReplicationHandler::handle_psync()`.
The replication handler only needs `AsyncRead + AsyncWrite` — the TLS layer is transparent.

Make `handle_psync` accept a generic `S: AsyncRead + AsyncWrite + Unpin + Send` or accept
`ReplicationStream`. The latter is simpler since the acceptor already knows the variant.

**Files**: `crates/replication/src/stream.rs` (new), `crates/replication/src/replica.rs`,
`crates/replication/src/primary.rs`, `crates/server/src/connection.rs` (PSYNC handoff),
`crates/server/src/server/mod.rs` (pass TLS config to replica handler)

---

## Phase 6: Cluster Bus TLS

**Goal**: Encrypt Raft RPC between cluster nodes.

### 6a. Outgoing cluster connections

`ClusterNetworkFactory` (in cluster crate or `frogdb_core::cluster::network`) gains
`tls_config: Option<Arc<ArcSwap<Arc<rustls::ClientConfig>>>>`. In `send_rpc()`, after
`TcpStream::connect()`, optionally upgrade with `TlsConnector::connect()`.

### 6b. Incoming cluster connections

`crates/server/src/cluster_bus.rs`: If `tls_cluster = true`, perform TLS handshake on accepted
connections before processing Raft RPC. Uses `TlsManager::current_server_config()`.

### 6c. Mixed-mode migration (`tls_cluster_migration`)

When `tls_cluster_migration = true`, the cluster bus acceptor:
1. Peek first byte of connection
2. If `0x16` (TLS ClientHello): perform TLS handshake
3. Otherwise: treat as plaintext
4. This enables rolling TLS upgrades

Implementation: use `TcpStream::peek(&mut buf)` to check the first byte without consuming it.

### 6d. Outgoing cluster TLS upgrade for migration

During migration, outgoing cluster connections also need dual behavior. Try TLS first; if
handshake fails (because peer hasn't upgraded yet), fall back to plaintext with a warning.

**Files**: `frogdb_core::cluster::network` (or wherever `ClusterNetworkFactory` lives),
`crates/server/src/cluster_bus.rs`, `crates/server/src/server/mod.rs`

---

## Phase 7: Certificate Hot-Reloading

**Goal**: Three reload triggers — file watcher, SIGUSR1, CONFIG RELOAD-CERTS.

### 7a. File watcher

In `TlsManager::new()`, if `watch_certs = true`:
- Create `notify::RecommendedWatcher` watching `cert_file`, `key_file`, and `ca_file`
- Route events through a `tokio::sync::mpsc` channel
- Spawn tokio task: debounce events (500ms default), call `self.reload()`
- Store watcher handle on `TlsManager` to keep it alive

### 7b. SIGUSR1 handler

In `Server::run_until()`, spawn a task:
```rust
let mut sig = tokio::signal::unix::signal(SignalKind::user_defined1())?;
loop {
    sig.recv().await;
    tls_manager.reload()?;
}
```

### 7c. CONFIG RELOAD-CERTS command

- Add to `handle_config_command` in `crates/server/src/connection/handlers/config.rs`
- `ConnectionHandler` needs `Option<Arc<TlsManager>>` — add to `AdminDeps` or `CoreDeps` in
  `crates/server/src/connection/deps.rs`
- Pass from `Acceptor` through `ConnectionHandler::from_deps()`

### 7d. Cert expiry metric update

After successful reload, re-extract expiry from new leaf cert and update
`frogdb_tls_cert_expiry_seconds` gauge.

**Files**: `crates/server/src/tls.rs`, `crates/server/src/server/mod.rs`,
`crates/server/src/connection/handlers/config.rs`, `crates/server/src/connection/deps.rs`,
`crates/server/src/acceptor.rs`

---

## Phase 8: Testing

### 8a. Test cert helpers

Create `crates/server/src/test_tls.rs` (or in test-harness) using `rcgen`:
- `generate_ca()` — self-signed CA
- `generate_server_cert(ca)` — server cert signed by CA
- `generate_client_cert(ca)` — client cert for mTLS
- `generate_expired_cert(ca)` — expired cert for rejection tests
- `write_pem_files(dir, cert, key)` — write to temp dir

### 8b. Extend test harness

Add TLS fields to `TestServerConfig` in test-harness. When enabled, generate ephemeral certs,
configure TLS, expose `tls_socket_addr()`.

### 8c. Integration tests (`crates/server/tests/integration_tls.rs`)

1. TLS connection: connect to TLS port, PING, SET/GET
2. Plaintext still works on main port
3. Dual-port simultaneous operation
4. mTLS required: client with cert succeeds, without fails
5. mTLS optional: both succeed
6. Invalid cert (wrong CA) rejected
7. Handshake timeout enforced
8. Admin port plaintext by default
9. Admin port TLS when `no_tls_on_admin_port = false`
10. Hot-reload: replace certs, CONFIG RELOAD-CERTS, new connections use new cert
11. CLIENT INFO shows TLS fields
12. Config validation errors (missing cert, port conflicts, etc.)

### 8d. Config validation unit tests

In `config/tls.rs`: test all validation rules produce correct errors.

---

## Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Stream type | `MaybeTlsStream` enum | Zero-cost for plain path, variant-specific methods |
| Acceptor design | Single `Acceptor` type with `Option<TlsManager>` | Avoids duplicating massive constructor |
| Replication streams | `ReplicationStream` enum in replication crate | Avoids generics proliferating |
| Turmoil compat | `ConnectionStream` type alias, TLS feature-gated out | TLS orthogonal to simulation |
| Hot-reload | `Arc<ArcSwap<Arc<ServerConfig>>>` | Lock-free reads, serialized writes |
| Admin port default | 6381 (changed from 6380) | Avoid conflict with tls_port default |

## Dependency Graph

```
Phase 1 (Config)
    ↓
Phase 2 (Stream abstraction)
    ↓
Phase 3 (TlsManager + Acceptor)
    ↓
  ┌─┴──────────┬──────────┬──────────┐
Phase 4      Phase 5    Phase 6    Phase 7
(Metrics)    (Repl TLS) (Cluster)  (Hot-reload)
  └──────────┴──────────┴──────────┘
    ↓
Phase 8 (Testing — ongoing throughout)
```

## Verification

After each phase: `just check && just lint && just test frogdb-server`
After Phase 2: `just concurrency` (turmoil must still work)
After Phase 3: Manual TLS connection test with `openssl s_client`
After Phase 8: Full integration test suite passes
