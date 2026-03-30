//! TLS integration tests.

use frogdb_protocol::Response;
use frogdb_test_harness::server::{TestServer, is_ok};
use frogdb_test_harness::tls::TlsFixture;

#[tokio::test]
async fn test_tls_ping() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_tls(&fixture).await;
    let mut client = server.connect_tls(&fixture).await;

    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_tls_set_get() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_tls(&fixture).await;
    let mut client = server.connect_tls(&fixture).await;

    let response = client.command(&["SET", "key1", "value1"]).await;
    assert!(is_ok(&response));

    let response = client.command(&["GET", "key1"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(bytes::Bytes::from("value1")))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_plaintext_still_works() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_tls(&fixture).await;

    // Plaintext port should still accept connections
    let mut client = server.connect().await;
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_dual_port_simultaneous() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_tls(&fixture).await;

    // Connect both plaintext and TLS
    let mut plain_client = server.connect().await;
    let mut tls_client = server.connect_tls(&fixture).await;

    // Both should work simultaneously
    let plain_resp = plain_client.command(&["SET", "plain_key", "plain_val"]).await;
    assert!(is_ok(&plain_resp));

    let tls_resp = tls_client.command(&["SET", "tls_key", "tls_val"]).await;
    assert!(is_ok(&tls_resp));

    // Cross-read: TLS client reads plaintext-written key
    let response = tls_client.command(&["GET", "plain_key"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(bytes::Bytes::from("plain_val")))
    );

    // Cross-read: plaintext client reads TLS-written key
    let response = plain_client.command(&["GET", "tls_key"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(bytes::Bytes::from("tls_val")))
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_mtls_required_with_cert() {
    let fixture = TlsFixture::generate();
    let server =
        TestServer::start_with_mtls(&fixture, frogdb_server::config::ClientCertMode::Required)
            .await;

    // Client with cert should succeed
    let mut client = server.connect_tls_with_client_cert(&fixture).await;
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_mtls_required_without_cert() {
    let fixture = TlsFixture::generate();
    let server =
        TestServer::start_with_mtls(&fixture, frogdb_server::config::ClientCertMode::Required)
            .await;

    // Client without cert should fail at handshake or first command
    let result = server
        .try_connect_tls(&fixture.ca_cert_der, None, None)
        .await;
    match result {
        Err(_) => {
            // Expected: handshake failure
        }
        Ok(mut client) => {
            // The TLS handshake may succeed but the server sends CertificateRequired
            // alert during the first I/O operation. Try sending a frame directly and
            // check for error.
            use futures::SinkExt;
            let frame = redis_protocol::resp2::types::BytesFrame::Array(vec![
                redis_protocol::resp2::types::BytesFrame::BulkString(bytes::Bytes::from("PING")),
            ]);
            let send_result = client.framed.send(frame).await;
            if send_result.is_ok() {
                use futures::StreamExt;
                let recv_result = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.framed.next(),
                )
                .await;
                // Should be either timeout, None (closed), or Err
                match recv_result {
                    Ok(Some(Ok(_))) => panic!("Expected connection to fail without client cert"),
                    _ => { /* Expected: error, None, or timeout */ }
                }
            }
            // send failure also means the connection was rejected — that's fine
        }
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_mtls_optional_both_paths() {
    let fixture = TlsFixture::generate();
    let server =
        TestServer::start_with_mtls(&fixture, frogdb_server::config::ClientCertMode::Optional)
            .await;

    // With client cert
    let mut client = server.connect_tls_with_client_cert(&fixture).await;
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    // Without client cert (should also succeed for Optional mode)
    let mut client = server.connect_tls(&fixture).await;
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(bytes::Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_invalid_cert_rejected() {
    let fixture = TlsFixture::generate();
    let server = TestServer::start_with_tls(&fixture).await;

    // Generate a different CA (not trusted by the server)
    let wrong_fixture = TlsFixture::generate();

    // Try connecting with a client that trusts the wrong CA
    let result = server
        .try_connect_tls(&wrong_fixture.ca_cert_der, None, None)
        .await;
    assert!(
        result.is_err(),
        "Expected TLS connection to fail with untrusted CA"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_config_validation_errors() {
    use frogdb_server::config::Config;

    // Enabled but no cert file
    let mut config = Config::default();
    config.tls.enabled = true;
    assert!(config.tls.validate().is_err());

    // Enabled with cert but no key
    config.tls.cert_file = "/some/cert.pem".into();
    assert!(config.tls.validate().is_err());

    // Cluster migration without cluster
    let mut config2 = Config::default();
    config2.tls.enabled = true;
    config2.tls.cert_file = "/some/cert.pem".into();
    config2.tls.key_file = "/some/key.pem".into();
    config2.tls.tls_cluster_migration = true;
    config2.tls.tls_cluster = false;
    assert!(config2.tls.validate().is_err());

    // Empty protocols
    let mut config3 = Config::default();
    config3.tls.enabled = true;
    config3.tls.cert_file = "/some/cert.pem".into();
    config3.tls.key_file = "/some/key.pem".into();
    config3.tls.protocols = vec![];
    assert!(config3.tls.validate().is_err());
}
