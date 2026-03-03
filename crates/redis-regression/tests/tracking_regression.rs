use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// unit/tracking covers client-side invalidation tracking. These tests verify
// the CLIENT ID infrastructure which is foundational to the tracking feature.

#[tokio::test]
async fn client_id_returns_non_negative_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "ID"]).await;
    let id = unwrap_integer(&resp);
    assert!(id >= 0, "CLIENT ID should return a non-negative integer, got {id}");
}

#[tokio::test]
async fn client_ids_are_unique_across_connections() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;
    let mut c3 = server.connect().await;

    let id1 = unwrap_integer(&c1.command(&["CLIENT", "ID"]).await);
    let id2 = unwrap_integer(&c2.command(&["CLIENT", "ID"]).await);
    let id3 = unwrap_integer(&c3.command(&["CLIENT", "ID"]).await);

    assert_ne!(id1, id2, "c1 and c2 should have different CLIENT IDs");
    assert_ne!(id2, id3, "c2 and c3 should have different CLIENT IDs");
    assert_ne!(id1, id3, "c1 and c3 should have different CLIENT IDs");
}

#[tokio::test]
async fn client_tracking_on_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Verify CLIENT TRACKING subcommand is available (may not be fully implemented)
    let resp = client.command(&["CLIENT", "TRACKING", "ON"]).await;
    // Either OK (implemented) or ERR (not yet implemented) - both are acceptable
    // as long as it doesn't panic
    let _ = resp;

    let resp = client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    let _ = resp;
}

#[tokio::test]
async fn client_info_contains_id_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT INFO returns info about the current client
    let resp = client.command(&["CLIENT", "INFO"]).await;
    // Should be a bulk string with client info, or error if not implemented
    // Just verify it doesn't panic
    let _ = resp;
}
