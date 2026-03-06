//! Helpers for replication integration tests.
//!
//! Extracts the primary+replica startup, WAIT-based sync, and INFO
//! replication parsing patterns that repeat across replication tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use frogdb_protocol::Response;

use super::test_server::{TestServer, TestServerConfig, parse_integer};

/// Start a primary and a replica that connects to it, then wait for
/// the replication handshake to complete.
///
/// Returns `(primary, replica)`.
pub async fn start_primary_replica_pair(config: TestServerConfig) -> (TestServer, TestServer) {
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Allow the replication handshake to finish.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    (primary, replica)
}

/// Issue `WAIT <numreplicas> <timeout_ms>` on `primary` and return the
/// number of replicas that acknowledged.  Falls back to a short sleep when
/// WAIT returns 0 (replication still in progress).
pub async fn wait_for_replication(primary: &TestServer, timeout_ms: u64) -> i64 {
    let response = primary.send("WAIT", &["1", &timeout_ms.to_string()]).await;
    let acked = parse_integer(&response).unwrap_or(0);

    if acked == 0 {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    acked
}

/// Parse `INFO replication` output into key-value pairs.
///
/// Returns `None` when the response is not a bulk string.
pub fn parse_info_replication(response: &Response) -> Option<HashMap<String, String>> {
    let data = match response {
        Response::Bulk(Some(b)) => b,
        _ => return None,
    };

    let text = String::from_utf8_lossy(data);
    let map = text
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    Some(map)
}

/// Extract `(master_replid, master_repl_offset)` from a server's
/// `INFO replication` output.
///
/// Returns `None` when either field is missing.
pub async fn get_replication_state(server: &TestServer) -> Option<(String, i64)> {
    let response = server.send("INFO", &["replication"]).await;
    let info = parse_info_replication(&response)?;

    let replid = info.get("master_replid")?.clone();
    let offset: i64 = info.get("master_repl_offset")?.parse().ok()?;

    Some((replid, offset))
}
