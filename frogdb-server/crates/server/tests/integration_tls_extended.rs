//! Extended TLS integration tests covering cluster bus, replication, and HTTPS.
//!
//! These tests exercise the TLS functionality added for inter-node communication,
//! replica connections, and the HTTP observability server.

use std::sync::Arc;
use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::tls::TlsFixture;

use crate::common::test_server::TestServer;

// ============================================================================
// HTTPS / Observability Server TLS
// ============================================================================

/// Verify that the HTTP metrics endpoint serves over HTTPS when `no_tls_on_http = false`.
#[tokio::test]
async fn test_https_metrics_endpoint() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_https(&fixture).await;

    let resp = server.fetch_https(&fixture, "/metrics").await;
    assert_eq!(resp.status(), 200);

    server.shutdown().await;
}

/// Verify that the health endpoint works over HTTPS.
#[tokio::test]
async fn test_https_health_endpoint() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_https(&fixture).await;

    let resp = server.fetch_https(&fixture, "/health/live").await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

/// By default (`no_tls_on_http = true`), the HTTP server stays plaintext even
/// when TLS is enabled for the RESP port.
#[tokio::test]
async fn test_http_plaintext_default_when_tls_enabled() {
    let fixture = TlsFixture::generate();
    // start_with_tls does NOT set no_tls_on_http=false, so HTTP stays plain
    let server = TestServer::start_with_tls(&fixture).await;

    // Plain HTTP should work
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/health/live",
            server.metrics_port()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // TLS RESP port should also work
    let mut tls_client = server.connect_tls(&fixture).await;
    let response = tls_client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    server.shutdown().await;
}

// ============================================================================
// TLS Configuration
// ============================================================================

/// Verify the new `no_tls_on_http` config field defaults and serializes correctly.
#[test]
fn test_no_tls_on_http_config_defaults() {
    let config = frogdb_server::config::TlsConfig::default();
    assert!(
        config.no_tls_on_http,
        "no_tls_on_http should default to true"
    );
    assert!(
        config.no_tls_on_admin_port,
        "no_tls_on_admin_port should default to true"
    );
}

#[test]
fn test_no_tls_on_http_serde() {
    let json = r#"{"enabled": false, "no-tls-on-http": false}"#;
    let config: frogdb_server::config::TlsConfig = serde_json::from_str(json).unwrap();
    assert!(!config.no_tls_on_http);
}

// ============================================================================
// Replication over TLS
// ============================================================================

/// Primary with TLS port, replica connects over TLS to replicate.
#[tokio::test]
async fn test_replication_over_tls() {
    let fixture = TlsFixture::generate();

    let primary = TestServer::start_primary_with_tls(&fixture).await;
    let replica = TestServer::start_replica_with_tls(&primary, &fixture).await;

    // Give replica time to connect and handshake over TLS
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write to primary
    let mut pclient = primary.connect().await;
    let resp = pclient
        .command(&["SET", "tls_repl_key", "tls_repl_val"])
        .await;
    assert!(matches!(resp, Response::Simple(_)));

    // Wait for replication to propagate
    let _ = pclient.command(&["WAIT", "1", "5000"]).await;

    // Poll replica for the key with retries
    let mut rclient = replica.connect().await;
    let mut found = false;
    for _ in 0..10 {
        let resp = rclient.command(&["GET", "tls_repl_key"]).await;
        if let Response::Bulk(Some(data)) = &resp {
            assert_eq!(data.as_ref(), b"tls_repl_val");
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(found, "Replica should have replicated the key over TLS");

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Cluster Bus TLS
// ============================================================================

/// 3-node cluster with TLS-encrypted cluster bus forms correctly and can
/// elect a leader.
#[tokio::test]
async fn test_cluster_formation_with_tls() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;

    let fixture = Arc::new(TlsFixture::generate());
    let mut harness = ClusterTestHarness::with_tls(fixture);

    harness.start_cluster(3).await.unwrap();
    assert_eq!(harness.node_ids().len(), 3);

    let leader = harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("cluster should elect a leader over TLS bus");
    assert!(harness.node_ids().contains(&leader));

    harness
        .wait_for_cluster_convergence(Duration::from_secs(10))
        .await
        .expect("cluster should converge over TLS bus");

    harness.shutdown_all().await;
}

/// Cluster bus in migration mode accepts both plaintext and TLS connections.
#[tokio::test]
async fn test_cluster_tls_migration_mode() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;

    let fixture = Arc::new(TlsFixture::generate());
    let mut harness = ClusterTestHarness::with_tls_migration(fixture);

    // Migration mode should still allow cluster formation (nodes use TLS
    // outgoing but accept both TLS and plaintext incoming).
    harness.start_cluster(3).await.unwrap();
    assert_eq!(harness.node_ids().len(), 3);

    let leader = harness
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("cluster should elect a leader in migration mode");
    assert!(harness.node_ids().contains(&leader));

    harness.shutdown_all().await;
}

// ============================================================================
// TlsManager unit-level smoke tests
// ============================================================================

/// When `tls_cluster` is set, TlsManager should produce a client connector.
#[test]
fn test_tls_manager_connector_with_cluster_enabled() {
    let fixture = TlsFixture::generate();
    let config = frogdb_server::config::TlsConfig {
        enabled: true,
        cert_file: fixture.server_cert.clone(),
        key_file: fixture.server_key.clone(),
        ca_file: Some(fixture.ca_cert.clone()),
        tls_cluster: true,
        ..Default::default()
    };

    let mgr = frogdb_server::tls::TlsManager::new(&config).unwrap();
    assert!(
        mgr.connector().is_some(),
        "connector should be Some when tls_cluster=true"
    );
}

/// When `tls_replication` is set, TlsManager should produce a client connector.
#[test]
fn test_tls_manager_connector_with_replication_enabled() {
    let fixture = TlsFixture::generate();
    let config = frogdb_server::config::TlsConfig {
        enabled: true,
        cert_file: fixture.server_cert.clone(),
        key_file: fixture.server_key.clone(),
        ca_file: Some(fixture.ca_cert.clone()),
        tls_replication: true,
        ..Default::default()
    };

    let mgr = frogdb_server::tls::TlsManager::new(&config).unwrap();
    assert!(
        mgr.connector().is_some(),
        "connector should be Some when tls_replication=true"
    );
}

/// When neither cluster nor replication TLS is enabled, connector is None.
#[test]
fn test_tls_manager_connector_none_by_default() {
    let fixture = TlsFixture::generate();
    let config = frogdb_server::config::TlsConfig {
        enabled: true,
        cert_file: fixture.server_cert.clone(),
        key_file: fixture.server_key.clone(),
        ..Default::default()
    };

    let mgr = frogdb_server::tls::TlsManager::new(&config).unwrap();
    assert!(
        mgr.connector().is_none(),
        "connector should be None when tls_cluster and tls_replication are both false"
    );
}

/// TlsManager::reload updates certificates without error.
#[test]
fn test_tls_manager_reload() {
    let fixture = TlsFixture::generate();
    let config = frogdb_server::config::TlsConfig {
        enabled: true,
        cert_file: fixture.server_cert.clone(),
        key_file: fixture.server_key.clone(),
        ca_file: Some(fixture.ca_cert.clone()),
        tls_cluster: true,
        ..Default::default()
    };

    let mgr = frogdb_server::tls::TlsManager::new(&config).unwrap();

    // Reload with the same config should succeed
    mgr.reload(&config).unwrap();

    // Connector should still work after reload
    assert!(mgr.connector().is_some());
}
