//! Integration tests for maxclients connection limiting.

use crate::common::test_server::{TestServer, TestServerConfig};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Start a server with a specific max_clients value.
async fn start_with_max_clients(max_clients: u32) -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        max_clients: Some(max_clients),
        ..Default::default()
    })
    .await
}

/// Wait until the server reports exactly `expected` connected clients.
///
/// `TestServer::wait_for_ready` probes the port with a connect-and-drop, and
/// the acceptor's slot for that probe is only released when its connection
/// task observes the EOF — asynchronously, after `start` has returned. A test
/// that counts connections against `maxclients` must therefore wait for the
/// registry to drain to a known baseline instead of assuming it; under a
/// loaded test run the probe's cleanup can lag past several accepts.
/// (The registry deregisters before the acceptor's atomic slot count is
/// decremented, with no await point between the two, so once INFO reports the
/// expected count the next accept observes the freed slot.)
async fn wait_for_client_count(
    client: &mut crate::common::test_server::TestClient,
    expected: usize,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let r = client.command(&["INFO", "clients"]).await;
        if let frogdb_protocol::Response::Bulk(Some(data)) = &r {
            let info = String::from_utf8_lossy(data);
            if info.contains(&format!("connected_clients:{expected}\r\n")) {
                return;
            }
            if std::time::Instant::now() > deadline {
                panic!("timed out waiting for connected_clients:{expected}, got: {info}");
            }
        } else {
            panic!("Expected bulk response for INFO clients, got: {r:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Try to connect raw TCP and read any initial error the server sends.
async fn try_raw_connect(port: u16) -> Result<String, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;

    // Read whatever the server sends (error message on rejection)
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buf))
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|e| e.to_string())?;

    buf.truncate(n);
    Ok(String::from_utf8_lossy(&buf).to_string())
}

#[tokio::test]
async fn test_maxclients_rejects_when_limit_reached() {
    let server = start_with_max_clients(2).await;

    // Connect two clients (at the limit). The startup probe's slot must be
    // confirmed released before the second connect, or it counts against us.
    let mut c1 = server.connect().await;
    wait_for_client_count(&mut c1, 1).await;
    let mut c2 = server.connect().await;

    // Verify they work
    let r1 = c1.command(&["PING"]).await;
    assert_eq!(r1, frogdb_protocol::Response::pong());
    let r2 = c2.command(&["PING"]).await;
    assert_eq!(r2, frogdb_protocol::Response::pong());

    // Third connection should get rejected
    let response = try_raw_connect(server.port()).await.unwrap();
    assert!(
        response.contains("max number of clients reached"),
        "Expected rejection message, got: {response}"
    );

    // Drop c1 and try again - should succeed now
    drop(c1);
    wait_for_client_count(&mut c2, 1).await;

    let mut c3 = server.connect().await;
    let r3 = c3.command(&["PING"]).await;
    assert_eq!(r3, frogdb_protocol::Response::pong());

    drop(c2);
    drop(c3);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_unlimited_when_zero() {
    let server = start_with_max_clients(0).await;

    // Should accept many connections when unlimited
    let mut clients = Vec::new();
    for _ in 0..10 {
        let mut c = server.connect().await;
        let r = c.command(&["PING"]).await;
        assert_eq!(r, frogdb_protocol::Response::pong());
        clients.push(c);
    }

    drop(clients);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_admin_port_exempt() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        max_clients: Some(2),
        admin_enabled: true,
        ..Default::default()
    })
    .await;

    // Fill up the regular port limit (waiting out the startup probe's slot
    // first — see `wait_for_client_count`)
    let mut c1 = server.connect().await;
    let r1 = c1.command(&["PING"]).await;
    assert_eq!(r1, frogdb_protocol::Response::pong());
    wait_for_client_count(&mut c1, 1).await;
    let mut c2 = server.connect().await;
    let r2 = c2.command(&["PING"]).await;
    assert_eq!(r2, frogdb_protocol::Response::pong());

    // Regular port should be full
    let response = try_raw_connect(server.port()).await.unwrap();
    assert!(
        response.contains("max number of clients reached"),
        "Expected rejection on regular port, got: {response}"
    );

    // Admin port should still accept despite regular port being full
    let mut admin_client = server.connect_admin().await;
    let r = admin_client.command(&["PING"]).await;
    assert_eq!(r, frogdb_protocol::Response::pong());

    drop(c1);
    drop(c2);
    drop(admin_client);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_config_get_set() {
    let server = start_with_max_clients(5000).await;
    let mut client = server.connect().await;

    // CONFIG GET maxclients should return configured value
    let r = client.command(&["CONFIG", "GET", "maxclients"]).await;
    if let frogdb_protocol::Response::Array(items) = r {
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[1],
            frogdb_protocol::Response::bulk(bytes::Bytes::from("5000"))
        );
    } else {
        panic!("Expected array response for CONFIG GET, got: {:?}", r);
    }

    // CONFIG SET maxclients to a new value
    let r = client.command(&["CONFIG", "SET", "maxclients", "42"]).await;
    assert_eq!(r, frogdb_protocol::Response::ok());

    // CONFIG GET should reflect new value
    let r = client.command(&["CONFIG", "GET", "maxclients"]).await;
    if let frogdb_protocol::Response::Array(items) = r {
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[1],
            frogdb_protocol::Response::bulk(bytes::Bytes::from("42"))
        );
    } else {
        panic!("Expected array response, got: {:?}", r);
    }

    drop(client);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_config_set_live_update() {
    let server = start_with_max_clients(10).await;
    let mut client = server.connect().await;

    // Lower the limit to 1 via CONFIG SET
    let r = client.command(&["CONFIG", "SET", "maxclients", "1"]).await;
    assert_eq!(r, frogdb_protocol::Response::ok());

    // Existing connection should still work
    let r = client.command(&["PING"]).await;
    assert_eq!(r, frogdb_protocol::Response::pong());

    // New connections should be rejected (we already have 1)
    let response = try_raw_connect(server.port()).await.unwrap();
    assert!(
        response.contains("max number of clients reached"),
        "Expected rejection after CONFIG SET, got: {response}"
    );

    drop(client);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_default_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Default maxclients should be 10000
    let r = client.command(&["CONFIG", "GET", "maxclients"]).await;
    if let frogdb_protocol::Response::Array(items) = r {
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[1],
            frogdb_protocol::Response::bulk(bytes::Bytes::from("10000"))
        );
    } else {
        panic!("Expected array response, got: {:?}", r);
    }

    drop(client);
    server.shutdown().await;
}

#[tokio::test]
async fn test_maxclients_info_clients() {
    let server = start_with_max_clients(5000).await;
    let mut client = server.connect().await;

    let r = client.command(&["INFO", "clients"]).await;
    if let frogdb_protocol::Response::Bulk(Some(data)) = r {
        let info = String::from_utf8_lossy(&data);
        assert!(
            info.contains("maxclients:5000"),
            "INFO clients should show maxclients:5000, got: {info}"
        );
    } else {
        panic!("Expected bulk response for INFO clients, got: {:?}", r);
    }

    drop(client);
    server.shutdown().await;
}
