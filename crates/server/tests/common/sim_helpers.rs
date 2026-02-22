//! Helpers for Turmoil-based simulation tests.
//!
//! Extracts the RESP encoding/decoding utilities and FrogDB server
//! bootstrap functions used across simulation tests.

#![allow(dead_code)]

use bytes::{Bytes, BytesMut};

use super::sim_harness::OperationResult;
use frogdb_server::config::{MetricsConfig, PersistenceConfig, ServerConfig};
use frogdb_server::{Config, Server};

/// Server port used in simulations.
pub const SERVER_PORT: u16 = 6379;

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
        b'*' => OperationResult::Array(vec![]),
        _ => OperationResult::Error("Unknown response type".into()),
    }
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
        },
        persistence: PersistenceConfig {
            enabled: false,
            ..Default::default()
        },
        metrics: MetricsConfig {
            enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };

    let server = Server::new(config).await?;
    server.run_until(std::future::pending::<()>()).await?;

    Ok(())
}

/// Start a real FrogDB server with a chaos configuration.
///
/// Currently delegates to the standard server (chaos hooks are not yet
/// wired into `Server::new`), but this consolidates the two
/// near-duplicate functions in `simulation.rs`.
pub async fn real_frogdb_server_with_chaos(
    num_shards: usize,
    _chaos: frogdb_server::config::ChaosConfig,
) -> Result<(), BoxError> {
    // TODO: Pass chaos config when Server::new_with_chaos is implemented.
    real_frogdb_server(num_shards).await
}
