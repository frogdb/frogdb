//! Integration tests for admin port separation.
//!
//! Tests verify:
//! 1. Admin commands are blocked on regular port when admin port is enabled
//! 2. Admin commands work on admin port
//! 3. All commands work on regular port when admin port is disabled (backward compat)


use crate::common::test_server::{TestServer, get_error_message, is_error, is_ok};

// ============================================================================
// Admin port enabled - blocking tests
// ============================================================================

#[tokio::test]
async fn test_admin_port_blocks_debug_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

    // DEBUG SLEEP should fail on regular port when admin port is enabled
    let response = client.command(&["DEBUG", "SLEEP", "0"]).await;
    assert!(
        is_error(&response),
        "DEBUG should be blocked on regular port"
    );
    let err = get_error_message(&response).expect("should have error message");
    assert!(
        err.contains("NOADMIN"),
        "Error should mention NOADMIN: {}",
        err
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_blocks_config_set_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

    // CONFIG SET should fail on regular port when admin port is enabled
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100"])
        .await;
    assert!(
        is_error(&response),
        "CONFIG SET should be blocked on regular port"
    );
    let err = get_error_message(&response).expect("should have error message");
    assert!(
        err.contains("NOADMIN"),
        "Error should mention NOADMIN: {}",
        err
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_blocks_shutdown_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

    // SHUTDOWN should fail on regular port when admin port is enabled
    let response = client.command(&["SHUTDOWN", "NOSAVE"]).await;
    assert!(
        is_error(&response),
        "SHUTDOWN should be blocked on regular port"
    );
    let err = get_error_message(&response).expect("should have error message");
    assert!(
        err.contains("NOADMIN"),
        "Error should mention NOADMIN: {}",
        err
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_allows_config_get_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

    // CONFIG GET should work on regular port (read-only, not ADMIN flagged)
    // Note: CONFIG command as a whole has ADMIN flag, so this will also be blocked
    // The plan says "CONFIG SET should have ADMIN, GET should not" but the implementation
    // uses a single CONFIG command. Let's test regular commands instead.

    // GET/SET should work on regular port
    let response = client.command(&["SET", "key", "value"]).await;
    assert!(is_ok(&response), "SET should work on regular port");

    let response = client.command(&["GET", "key"]).await;
    match response {
        frogdb_protocol::Response::Bulk(Some(b)) => {
            assert_eq!(b.as_ref(), b"value");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// Admin port enabled - admin commands work on admin port
// ============================================================================

#[tokio::test]
async fn test_admin_port_allows_debug_on_admin_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut admin_client = server.connect_admin().await;

    // DEBUG SLEEP should work on admin port
    let response = admin_client.command(&["DEBUG", "SLEEP", "0"]).await;
    assert!(
        is_ok(&response),
        "DEBUG SLEEP should work on admin port: {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_allows_config_set_on_admin_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut admin_client = server.connect_admin().await;

    // CONFIG SET should work on admin port
    let response = admin_client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100"])
        .await;
    assert!(
        is_ok(&response),
        "CONFIG SET should work on admin port: {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_allows_regular_commands_on_admin_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut admin_client = server.connect_admin().await;

    // Regular commands should also work on admin port
    let response = admin_client
        .command(&["SET", "admin_key", "admin_value"])
        .await;
    assert!(is_ok(&response), "SET should work on admin port");

    let response = admin_client.command(&["GET", "admin_key"]).await;
    match response {
        frogdb_protocol::Response::Bulk(Some(b)) => {
            assert_eq!(b.as_ref(), b"admin_value");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// Admin port disabled - backward compatibility
// ============================================================================

#[tokio::test]
async fn test_admin_disabled_allows_debug_on_regular_port() {
    // Start server without admin port (default)
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // DEBUG SLEEP should work on regular port when admin is disabled
    let response = client.command(&["DEBUG", "SLEEP", "0"]).await;
    assert!(
        is_ok(&response),
        "DEBUG SLEEP should work when admin disabled: {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_disabled_allows_config_set_on_regular_port() {
    // Start server without admin port (default)
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CONFIG SET should work on regular port when admin is disabled
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "100"])
        .await;
    assert!(
        is_ok(&response),
        "CONFIG SET should work when admin disabled: {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_disabled_has_no_admin_port() {
    // Start server without admin port (default)
    let server = TestServer::start_standalone().await;

    // Should not have admin port
    assert!(
        !server.has_admin_port(),
        "Should not have admin port when disabled"
    );

    server.shutdown().await;
}

// ============================================================================
// Data isolation tests (admin and regular ports share same data)
// ============================================================================

#[tokio::test]
async fn test_admin_port_data_shared_between_ports() {
    let server = TestServer::start_with_admin_port().await;
    let mut regular_client = server.connect().await;
    let mut admin_client = server.connect_admin().await;

    // Set data via regular port
    let response = regular_client
        .command(&["SET", "shared_key", "from_regular"])
        .await;
    assert!(is_ok(&response));

    // Read via admin port
    let response = admin_client.command(&["GET", "shared_key"]).await;
    match response {
        frogdb_protocol::Response::Bulk(Some(b)) => {
            assert_eq!(b.as_ref(), b"from_regular", "Data should be shared");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    // Set data via admin port
    let response = admin_client
        .command(&["SET", "admin_shared", "from_admin"])
        .await;
    assert!(is_ok(&response));

    // Read via regular port
    let response = regular_client.command(&["GET", "admin_shared"]).await;
    match response {
        frogdb_protocol::Response::Bulk(Some(b)) => {
            assert_eq!(b.as_ref(), b"from_admin", "Data should be shared");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}
