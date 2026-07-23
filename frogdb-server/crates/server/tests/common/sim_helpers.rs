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
pub async fn real_frogdb_server_fake_persistence(num_shards: usize) -> Result<(), BoxError> {
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
    use frogdb_server::config::ClusterConfigSection;

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
            election_timeout_ms: 300,
            heartbeat_interval_ms: 50,
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
