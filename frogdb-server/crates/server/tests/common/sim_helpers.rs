//! Helpers for Turmoil-based simulation tests.
//!
//! Extracts the RESP encoding/decoding utilities and FrogDB server
//! bootstrap functions used across simulation tests.

#![allow(dead_code)]

use bytes::{Bytes, BytesMut};

use super::sim_harness::OperationResult;
use frogdb_server::config::{HttpConfig, MetricsConfig, PersistenceConfig, ServerConfig};
use frogdb_server::{Config, Server};

/// Server port used in simulations.
pub const SERVER_PORT: u16 = 6379;

/// Cluster-bus (Raft RPC) port used in cluster simulations. Deliberately
/// `SERVER_PORT + 10000` so `cluster_init`'s client-port derivation
/// (`bus_port - 10000`) recovers exactly `SERVER_PORT`.
pub const CLUSTER_BUS_PORT: u16 = SERVER_PORT + 10000;

/// Server host name in simulations.
pub const SERVER_HOST: &str = "server";

/// Encode a command as a RESP protocol bulk-array.
pub fn encode_command(parts: &[&[u8]]) -> Bytes {
    let mut buf = BytesMut::new();

    // Array header
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());

    // Each part as bulk string
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }

    buf.freeze()
}

/// Parse a simplified RESP response (sufficient for simulation tests).
pub fn parse_simple_response(data: &[u8]) -> OperationResult {
    if data.is_empty() {
        return OperationResult::Error("Empty response".into());
    }

    match data[0] {
        b'+' => {
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            if s == "OK" {
                OperationResult::Ok
            } else if s == "PONG" {
                OperationResult::String(Bytes::from("PONG"))
            } else {
                OperationResult::String(Bytes::from(s))
            }
        }
        b'-' => {
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            OperationResult::Error(s)
        }
        b':' => {
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            let n = s.parse().unwrap_or(0);
            OperationResult::Integer(n)
        }
        b'$' => {
            let s = String::from_utf8_lossy(&data[1..]);
            if s.starts_with("-1") {
                OperationResult::Nil
            } else if let Some(pos) = s.find("\r\n") {
                let after = &data[1 + pos + 2..];
                if let Some(end) = after.iter().position(|&b| b == b'\r') {
                    OperationResult::String(Bytes::copy_from_slice(&after[..end]))
                } else {
                    OperationResult::String(Bytes::copy_from_slice(after))
                }
            } else {
                OperationResult::Error("Invalid bulk string".into())
            }
        }
        b'*' => OperationResult::Array(parse_resp_array(data)),
        _ => OperationResult::Error("Unknown response type".into()),
    }
}

/// Parse a RESP array response into a vector of `OperationResult`.
///
/// Expects `data` to start with `*N\r\n` followed by N bulk-string elements.
fn parse_resp_array(data: &[u8]) -> Vec<OperationResult> {
    let s = String::from_utf8_lossy(&data[1..]);
    let header_end = match s.find("\r\n") {
        Some(pos) => pos,
        None => return vec![],
    };
    let count: usize = match s[..header_end].parse() {
        Ok(n) => n,
        Err(_) => return vec![],
    };

    let mut results = Vec::with_capacity(count);
    // offset into `data` past the `*N\r\n` header
    let mut pos = 1 + header_end + 2;

    for _ in 0..count {
        if pos >= data.len() {
            break;
        }
        match data[pos] {
            b'$' => {
                let rest = String::from_utf8_lossy(&data[pos + 1..]);
                if rest.starts_with("-1") {
                    results.push(OperationResult::Nil);
                    // skip past `$-1\r\n`
                    pos += 5;
                } else if let Some(len_end) = rest.find("\r\n") {
                    let bulk_len: usize = rest[..len_end].parse().unwrap_or(0);
                    let data_start = pos + 1 + len_end + 2;
                    let data_end = data_start + bulk_len;
                    if data_end <= data.len() {
                        results.push(OperationResult::String(Bytes::copy_from_slice(
                            &data[data_start..data_end],
                        )));
                    }
                    // skip past `$N\r\n<data>\r\n`
                    pos = data_end + 2;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    results
}

type BoxError = Box<dyn std::error::Error + 'static>;

/// Start a real FrogDB server inside a Turmoil simulation.
///
/// Metrics are disabled because the HTTP server uses real TCP bindings
/// incompatible with Turmoil's simulated network.
pub async fn real_frogdb_server(num_shards: usize) -> Result<(), BoxError> {
    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server inside turmoil with the deterministic WAL fake
/// (`persistence.mode = "fake"`), replacing today's `enabled = false`. WAL
/// effects are recorded and reachable via `FakeWalRegistry::log(shard_id)`.
///
/// The fake path never opens RocksDB: recovery leaves `rocks_store = None` and
/// the shard spawn selects the in-process fake sink.
///
/// `seed` is the simulation's seed. It seeds the chaos injector's generator so that every
/// random draw the server makes under simulation is a function of the run's seed rather than
/// of process entropy — the injector is inert at these settings, but a config that carries the
/// seed cannot silently become nondeterministic when a caller later enables jitter.
pub async fn real_frogdb_server_fake_persistence(
    num_shards: usize,
    seed: u64,
) -> Result<(), BoxError> {
    // Pin the wall-clock anchor `XADD *` / `XCLAIM TIME` / absolute-expiry replies
    // read (`clock::system_now`) before this host's paused clock advances at all.
    // The anchor is process-global state (see `clock::reset_system_epoch`), and a
    // test process runs several simulated servers back to back — e.g.
    // `determinism::assert_run_is_reproducible` runs the same workload twice —
    // so without resetting it per host, the *second* server would inherit the
    // first's real `SystemTime::now()` reading and mint stream IDs that depend on
    // how much real wall-clock time separated the two runs, not on the
    // deterministic virtual schedule. See
    // `.scratch/concurrency-testing/issues/17-virtual-wall-clock-for-stream-ids.md`.
    frogdb_core::clock::reset_system_epoch(
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
    );

    let config = Config {
        chaos: frogdb_server::config::ChaosConfig::default().with_seed(seed),
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: true,
            mode: "fake".into(),
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server in the primary replication role inside turmoil.
///
/// `data_dir` must be unique per simulated host (the replication state file
/// lives there); persistence itself stays disabled, so a full sync ships the
/// minimal RDB and all data flows through the live command stream.
pub async fn real_frogdb_primary(
    num_shards: usize,
    data_dir: std::path::PathBuf,
) -> Result<(), BoxError> {
    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            data_dir,
            ..Default::default()
        },
        replication: frogdb_server::config::ReplicationConfigSection {
            role: "primary".to_string(),
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server in the replica role inside turmoil, dialing
/// `primary_ip:SERVER_PORT` (resolve the IP with `turmoil::lookup` inside the
/// host closure). Requires the server's turmoil connect-factory wiring in
/// `replication_init.rs` so the dial goes through the simulated network.
pub async fn real_frogdb_replica(
    num_shards: usize,
    primary_ip: std::net::IpAddr,
    data_dir: std::path::PathBuf,
) -> Result<(), BoxError> {
    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            data_dir,
            ..Default::default()
        },
        replication: frogdb_server::config::ReplicationConfigSection {
            role: "replica".to_string(),
            primary_host: primary_ip.to_string(),
            primary_port: SERVER_PORT,
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// One node of a scheduled **replication** topology: a primary or a replica of
/// one, with the knobs the seeded replication arm draws per run
/// (`simulation/replication_scheduler.rs`, replication-correctness issue 12).
///
/// [`real_frogdb_primary`] and [`real_frogdb_replica`] are the fixed-config
/// forms the scripted sims use; this is the same two roles with the handful of
/// settings a schedule varies spelled out, so the arm never has to hand-roll a
/// `Config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationNodeParams {
    /// Data-plane shard count.
    pub num_shards: usize,
    /// `None` boots this node as a primary; `Some(ip)` as a replica of that
    /// address (resolve it with `turmoil::lookup` inside the host closure).
    pub primary_ip: Option<std::net::IpAddr>,
    /// Enable RocksDB data-plane persistence.
    ///
    /// This is the switch that picks the **full-sync payload shape**: with a
    /// RocksDB store the primary stages a checkpoint and ships
    /// `FROGDB_CHECKPOINT`; without one it serializes its live keyspace and
    /// ships `FROGDB_SNAPSHOT` (`replica_session::run_full_sync`). Both shapes
    /// have to be reachable for `FullSyncInterrupt` to mean anything.
    pub persistence: bool,
    /// Entry cap on the replication backlog — the ring a `+CONTINUE` replays
    /// from, and so the knob that decides which side of the partial-sync
    /// boundary a reconnect lands on.
    pub backlog_size: usize,
    /// `min-replicas-to-write`: writes are refused below this many good
    /// replicas.
    pub min_replicas_to_write: u32,
    /// ACK-freshness window `min_replicas_to_write` counts inside
    /// (`min-replicas-max-lag-ms`).
    pub min_replicas_timeout_ms: u64,
    /// Reject writes once no streaming replica is fresh
    /// (`self-fence-on-replica-loss`).
    pub self_fence_on_replica_loss: bool,
    /// Freshness window the self-fence measures against.
    pub replica_freshness_timeout_ms: u64,
    /// Seconds without an ACK before the primary proactively disconnects a
    /// lagging replica. 0 disables it.
    pub replication_lag_threshold_secs: u64,
}

impl Default for ReplicationNodeParams {
    /// A primary with the shipped defaults: no persistence (so a full sync
    /// ships the live dataset), a backlog wide enough that nothing this arm
    /// writes evicts a resume point, and neither the fence nor
    /// `min-replicas-to-write` engaged.
    fn default() -> Self {
        Self {
            num_shards: 1,
            primary_ip: None,
            persistence: false,
            backlog_size: 8192,
            min_replicas_to_write: 0,
            min_replicas_timeout_ms: 10_000,
            self_fence_on_replica_loss: false,
            replica_freshness_timeout_ms: 10_000,
            replication_lag_threshold_secs: 0,
        }
    }
}

/// Start one node of a scheduled replication topology inside turmoil.
///
/// `data_dir` must be unique per simulated host: the replication state file
/// lives there, and so does RocksDB when [`ReplicationNodeParams::persistence`]
/// is set.
pub async fn real_frogdb_replication_node(
    params: ReplicationNodeParams,
    data_dir: std::path::PathBuf,
) -> Result<(), BoxError> {
    let ReplicationNodeParams {
        num_shards,
        primary_ip,
        persistence,
        backlog_size,
        min_replicas_to_write,
        min_replicas_timeout_ms,
        self_fence_on_replica_loss,
        replica_freshness_timeout_ms,
        replication_lag_threshold_secs,
    } = params;

    let (role, primary_host) = match primary_ip {
        Some(ip) => ("replica", ip.to_string()),
        None => ("primary", String::new()),
    };

    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: persistence,
            data_dir,
            ..Default::default()
        },
        replication: frogdb_server::config::ReplicationConfigSection {
            role: role.to_string(),
            primary_host,
            primary_port: SERVER_PORT,
            backlog_size,
            min_replicas_to_write,
            min_replicas_timeout_ms,
            self_fence_on_replica_loss,
            replica_freshness_timeout_ms,
            replication_lag_threshold_secs,
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server as one node of a multi-node Raft cluster inside
/// turmoil.
///
/// Every node runs real openraft consensus over the simulated cluster bus:
/// incoming Raft RPCs are served by `cluster_bus::run` (framed via `new_framed`
/// under turmoil), and outgoing RPCs dial through the turmoil connect factory
/// injected in `cluster_init.rs`. Bootstrap, leader election, slot assignment,
/// and slot migration therefore all execute deterministically for a given seed.
///
/// - `own_ip` is this host's turmoil address (`turmoil::lookup(hostname)`); the
///   client listener advertises `own_ip:SERVER_PORT` and the cluster bus
///   `own_ip:CLUSTER_BUS_PORT`.
/// - `initial_nodes` is the full set of peer cluster-bus addresses (including
///   this node), each `"<ip>:<CLUSTER_BUS_PORT>"`. Node IDs are derived by
///   hashing the bus address (config `node_id = 0`), so every node computes the
///   same ID for every peer — the lowest-ID node bootstraps.
/// - `data_dir` must be unique per host: the RocksDB Raft log/metadata store
///   lives at `<data_dir>/raft`. Persistence of the data plane stays disabled.
/// - `auto_failover` wires `cluster.auto_failover`: when `true`, the leader's
///   failure detector proposes a `Failover` (successor promotion + slot transfer)
///   after latching a peer `FAIL`. Left `false` for scenarios that only need the
///   `MarkNodeFailed` half.
pub async fn real_frogdb_cluster_node(
    num_shards: usize,
    own_ip: std::net::IpAddr,
    initial_nodes: Vec<String>,
    data_dir: std::path::PathBuf,
    auto_failover: bool,
) -> Result<(), BoxError> {
    real_frogdb_cluster_node_with(
        ClusterNodeParams {
            num_shards,
            auto_failover,
            ..ClusterNodeParams::default()
        },
        own_ip,
        initial_nodes,
        data_dir,
    )
    .await
}

/// The per-node knobs [`real_frogdb_cluster_node_with`] varies.
///
/// Split out of the positional argument list because the seeded fault scheduler
/// (`simulation::scheduler`) skews the Raft timers *per node* from its seed:
/// with `election_timeout_max = election_timeout_min + 1` under turmoil (see
/// `cluster_init.rs`), openraft draws no jitter, so a distinct timeout per node
/// is what breaks election ties — deterministically, rather than through an
/// unseeded `thread_rng`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterNodeParams {
    /// Data-plane shard count.
    pub num_shards: usize,
    /// Wires `cluster.auto_failover`.
    pub auto_failover: bool,
    /// Raft election timeout; must stay strictly above `heartbeat_interval_ms`.
    pub election_timeout_ms: u64,
    /// Raft heartbeat interval.
    pub heartbeat_interval_ms: u64,
}

impl Default for ClusterNodeParams {
    /// The values every scripted cluster sim used before the scheduler existed:
    /// fast, simulated-clock timers so elections converge quickly in sim time.
    fn default() -> Self {
        Self {
            num_shards: 1,
            auto_failover: false,
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }
}

/// [`real_frogdb_cluster_node`] with the per-node knobs spelled out.
pub async fn real_frogdb_cluster_node_with(
    params: ClusterNodeParams,
    own_ip: std::net::IpAddr,
    initial_nodes: Vec<String>,
    data_dir: std::path::PathBuf,
) -> Result<(), BoxError> {
    use frogdb_server::config::ClusterConfigSection;

    let ClusterNodeParams {
        num_shards,
        auto_failover,
        election_timeout_ms,
        heartbeat_interval_ms,
    } = params;

    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            data_dir: data_dir.clone(),
            ..Default::default()
        },
        cluster: ClusterConfigSection {
            enabled: true,
            node_id: 0, // auto-derive from cluster_bus_addr hash (stable per peer)
            client_addr: format!("{own_ip}:{SERVER_PORT}"),
            cluster_bus_addr: format!("{own_ip}:{CLUSTER_BUS_PORT}"),
            initial_nodes,
            data_dir,
            // Fast, simulated-clock timers so elections converge quickly in sim
            // time. heartbeat must stay strictly below election_timeout_ms.
            election_timeout_ms,
            heartbeat_interval_ms,
            auto_failover,
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server with a chaos configuration.
///
/// Passes the chaos config through to the server so that failure injection
/// (shard unavailability, error shards, connection resets, delays) takes effect.
pub async fn real_frogdb_server_with_chaos(
    num_shards: usize,
    chaos: frogdb_server::config::ChaosConfig,
) -> Result<(), BoxError> {
    let config = Config {
        server: ServerConfig {
            bind: "0.0.0.0".to_string(),
            port: SERVER_PORT,
            num_shards,
            allow_cross_slot_standalone: true,
            scatter_gather_timeout_ms: 5000,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            ..Default::default()
        },
        http: HttpConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        chaos,
        ..Default::default()
    };

    let server = Server::new(
        config,
        frogdb_server::runtime_config::LogReloadHandle::noop(),
    )
    .await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}
