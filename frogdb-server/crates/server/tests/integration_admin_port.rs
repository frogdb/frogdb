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

/// Whether a response is the admin-port gate's rejection.
fn is_noadmin(response: &frogdb_protocol::Response) -> bool {
    is_error(response) && get_error_message(response).is_some_and(|err| err.contains("NOADMIN"))
}

/// Assert a container command's admin split: `public` invocations pass the gate
/// on the client port, `admin_only` ones are refused there with `-NOADMIN` and
/// pass the gate on the admin port.
///
/// The assertions are about the *gate*, not about each subcommand's own result,
/// so they check for the absence/presence of `-NOADMIN` rather than for success
/// — several of these subcommands legitimately answer with a different error
/// (no config file to rewrite, no such client to kill).
async fn assert_split(server: &TestServer, public: &[&[&str]], admin_only: &[&[&str]]) {
    let mut client = server.connect().await;
    for args in public {
        let response = client.command(args).await;
        assert!(
            !is_noadmin(&response),
            "{args:?} must be reachable from the client port, got {response:?}"
        );
    }
    for args in admin_only {
        let response = client.command(args).await;
        assert!(
            is_noadmin(&response),
            "{args:?} must be blocked on the client port with NOADMIN, got {response:?}"
        );
    }

    let mut admin_client = server.connect_admin().await;
    for args in admin_only {
        let response = admin_client.command(args).await;
        assert!(
            !is_noadmin(&response),
            "{args:?} must pass the gate on the admin port, got {response:?}"
        );
    }
}

/// `CONFIG` splits per subcommand, and Redis marks `CONFIG GET` `@admin` just
/// like `SET`/`REWRITE`/`RESETSTAT` — reading the configuration is a disclosure
/// surface. Only `CONFIG HELP` is public. (This replaces a test that admitted in
/// its own comment that it never exercised `CONFIG GET`.)
#[tokio::test]
async fn test_admin_port_config_subcommand_split_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;

    assert_split(
        &server,
        &[&["CONFIG", "HELP"]],
        &[
            &["CONFIG", "GET", "maxmemory"],
            &["CONFIG", "RESETSTAT"],
            &["CONFIG", "REWRITE"],
        ],
    )
    .await;

    // A bare `CONFIG`, and a subcommand nobody declared, both fail closed.
    let mut client = server.connect().await;
    for args in [vec!["CONFIG"], vec!["CONFIG", "NOT-A-SUBCOMMAND"]] {
        let response = client.command(&args).await;
        let err = get_error_message(&response).expect("should have error message");
        assert!(
            err.contains("NOADMIN"),
            "{args:?} must fail closed with NOADMIN, got: {err}"
        );
    }

    server.shutdown().await;
}

/// The inverse of the old `test_admin_port_blocks_cluster_discovery_subcommands
/// _on_regular_port` pin: with an admin port configured, the *client* port now
/// serves the read-only discovery surface every cluster-aware client needs to
/// bootstrap its slot map, while the mutating subcommands stay `-NOADMIN`.
/// Closes `.scratch/replication-cluster-rework/issues/done/05-*`.
#[tokio::test]
async fn test_admin_port_allows_cluster_discovery_subcommands_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

    for args in [
        vec!["CLUSTER", "SLOTS"],
        vec!["CLUSTER", "SHARDS"],
        vec!["CLUSTER", "INFO"],
        vec!["CLUSTER", "NODES"],
        vec!["CLUSTER", "MYID"],
    ] {
        let response = client.command(&args).await;
        assert!(
            !is_error(&response),
            "{args:?} must answer on the client port, got {response:?}"
        );
    }

    // …and the mutating half is still refused there.
    for args in [
        vec!["CLUSTER", "SETSLOT", "0", "NODE", "abc"],
        vec!["CLUSTER", "FORGET", "abc"],
        vec!["CLUSTER", "MEET", "127.0.0.1", "1"],
        vec!["CLUSTER", "RESET"],
        vec!["CLUSTER", "FAILOVER"],
        vec!["CLUSTER", "ADDSLOTS", "0"],
        vec!["CLUSTER", "DELSLOTS", "0"],
        // Fail-closed defaults: no subcommand, and an undeclared one.
        vec!["CLUSTER"],
        vec!["CLUSTER", "NOT-A-SUBCOMMAND"],
    ] {
        let response = client.command(&args).await;
        assert!(
            is_error(&response),
            "{args:?} must stay blocked on the client port"
        );
        let err = get_error_message(&response).expect("should have error message");
        assert!(
            err.contains("NOADMIN"),
            "{args:?} must fail with NOADMIN, got: {err}"
        );
    }

    // The discovery subcommands keep answering on the admin port too.
    let mut admin_client = server.connect_admin().await;
    for args in [
        vec!["CLUSTER", "SLOTS"],
        vec!["CLUSTER", "SHARDS"],
        vec!["CLUSTER", "INFO"],
        vec!["CLUSTER", "NODES"],
    ] {
        let response = admin_client.command(&args).await;
        assert!(
            !is_error(&response),
            "{args:?} must succeed on the admin port, got {response:?}"
        );
    }

    server.shutdown().await;
}

/// `ACL`: a client may ask about itself and about the static helpers; anything
/// that reads or edits *other* users' ACLs is admin-only (Redis parity).
#[tokio::test]
async fn test_admin_port_acl_subcommand_split_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;

    assert_split(
        &server,
        &[
            &["ACL", "WHOAMI"],
            &["ACL", "CAT"],
            &["ACL", "GENPASS"],
            &["ACL", "HELP"],
        ],
        &[
            &["ACL", "SETUSER", "issue05user", "on", ">pw"],
            &["ACL", "LIST"],
            &["ACL", "USERS"],
            &["ACL", "GETUSER", "default"],
            &["ACL", "DELUSER", "issue05user"],
        ],
    )
    .await;

    server.shutdown().await;
}

/// `CLIENT`: self-directed connection control is public; observing or
/// disturbing *other* connections is admin-only.
#[tokio::test]
async fn test_admin_port_client_subcommand_split_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;

    assert_split(
        &server,
        &[
            &["CLIENT", "ID"],
            &["CLIENT", "GETNAME"],
            &["CLIENT", "SETNAME", "issue05"],
            &["CLIENT", "INFO"],
            &["CLIENT", "NO-TOUCH", "off"],
            &["CLIENT", "HELP"],
        ],
        &[
            &["CLIENT", "KILL", "ID", "99999999"],
            &["CLIENT", "LIST"],
            &["CLIENT", "UNPAUSE"],
            &["CLIENT", "NO-EVICT", "off"],
        ],
    )
    .await;

    server.shutdown().await;
}

/// `MEMORY` gains an admin surface it did not have before: `PURGE` mutates
/// server state, the introspection subcommands do not.
#[tokio::test]
async fn test_admin_port_memory_subcommand_split_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;

    assert_split(
        &server,
        &[
            &["MEMORY", "STATS"],
            &["MEMORY", "DOCTOR"],
            &["MEMORY", "HELP"],
        ],
        &[&["MEMORY", "PURGE"]],
    )
    .await;

    server.shutdown().await;
}

/// The observability containers get **no** split: Redis marks every `SLOWLOG`
/// and `LATENCY` subcommand `@admin`, and FrogDB's own `HOTKEYS` follows them.
/// Their readers are disclosure surfaces — `SLOWLOG GET` returns the arguments of
/// executed commands, `HOTKEYS GET` returns key names and access frequencies — so
/// the whole container stays admin-only on a client port while the admin port
/// keeps serving all of it.
#[tokio::test]
async fn test_admin_port_blocks_observability_containers_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;

    assert_split(
        &server,
        &[],
        &[
            &["SLOWLOG", "GET"],
            &["SLOWLOG", "LEN"],
            &["SLOWLOG", "HELP"],
            &["SLOWLOG", "RESET"],
            &["LATENCY", "LATEST"],
            &["LATENCY", "HISTORY", "command"],
            &["LATENCY", "DOCTOR"],
            &["LATENCY", "HELP"],
            &["LATENCY", "RESET"],
            &["HOTKEYS", "GET"],
            &["HOTKEYS", "START"],
            &["HOTKEYS", "STOP"],
            &["HOTKEYS", "RESET"],
        ],
    )
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_admin_port_allows_regular_commands_on_regular_port() {
    let server = TestServer::start_with_admin_port().await;
    let mut client = server.connect().await;

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
