//! Integration tests for per-ACL-user rate limiting.

use crate::common::acl_helpers::{create_and_auth_user, start_server_with_admin};
use crate::common::response_helpers::{assert_error_prefix, assert_ok};
use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// Rate Limit Integration Tests
// ============================================================================

#[tokio::test]
async fn test_ratelimit_commands_per_second() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create user with 5 commands/second rate limit
    let mut client = create_and_auth_user(
        &server,
        &mut admin,
        "limited",
        "pass",
        &["+@all", "~*", "ratelimit:cps=5"],
    )
    .await;

    // First 5 commands should succeed (1-second burst)
    for _ in 0..5 {
        let response = client.command(&["PING"]).await;
        assert_eq!(response, Response::pong());
    }

    // 6th command should be rate limited (PING is exempt, use SET instead)
    // Actually PING is exempt from rate limiting. Let's use SET.
    // Re-create with a fresh approach: exhaust budget with non-exempt commands
    let mut client2 = create_and_auth_user(
        &server,
        &mut admin,
        "limited2",
        "pass2",
        &["+@all", "~*", "ratelimit:cps=5"],
    )
    .await;

    for _ in 0..5 {
        let response = client2.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    // 6th non-exempt command should fail
    let response = client2.command(&["SET", "{k}foo", "bar"]).await;
    assert_error_prefix(&response, "ERR rate limit exceeded: commands per second");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_exempt_commands_work_when_limited() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    let mut client = create_and_auth_user(
        &server,
        &mut admin,
        "limited",
        "pass",
        &["+@all", "~*", "ratelimit:cps=3"],
    )
    .await;

    // Exhaust budget with non-exempt commands
    for _ in 0..3 {
        let response = client.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    // Verify rate-limited
    let response = client.command(&["GET", "{k}foo"]).await;
    assert_error_prefix(&response, "ERR rate limit exceeded");

    // PING should still work (exempt)
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_multiple_connections_share_limit() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create user with 5 cps rate limit
    let response = admin
        .command(&[
            "ACL",
            "SETUSER",
            "shared",
            "on",
            ">pass",
            "+@all",
            "~*",
            "ratelimit:cps=5",
        ])
        .await;
    assert_ok(&response);

    // Connect two clients as the same user
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "shared", "pass"]).await;
    assert_ok(&response);

    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "shared", "pass"]).await;
    assert_ok(&response);

    // Use 3 tokens from client1
    for _ in 0..3 {
        let response = client1.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    // Use 2 tokens from client2 (should succeed since 5 total budget)
    for _ in 0..2 {
        let response = client2.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    // Either client's next command should be rate limited
    let response = client1.command(&["SET", "{k}foo", "bar"]).await;
    assert_error_prefix(&response, "ERR rate limit exceeded");

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_acl_getuser_shows_config() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create user with both limits
    let response = admin
        .command(&[
            "ACL",
            "SETUSER",
            "rluser",
            "on",
            ">pass",
            "+@all",
            "~*",
            "ratelimit:cps=100",
            "ratelimit:bps=1048576",
        ])
        .await;
    assert_ok(&response);

    // ACL GETUSER should include rate_limit info
    let response = admin.command(&["ACL", "GETUSER", "rluser"]).await;
    if let Response::Array(items) = &response {
        // Find the rate_limit key
        let mut found_rl = false;
        for (i, item) in items.iter().enumerate() {
            if let Response::Bulk(Some(b)) = item
                && b.as_ref() == b"rate_limit"
            {
                found_rl = true;
                // Next item should be an array with cps=100 and bps=1048576
                if let Response::Array(rl_items) = &items[i + 1] {
                    let strs: Vec<String> = rl_items
                        .iter()
                        .filter_map(|r| {
                            if let Response::Bulk(Some(b)) = r {
                                Some(String::from_utf8_lossy(b).to_string())
                            } else {
                                None
                            }
                        })
                        .collect();
                    assert!(
                        strs.contains(&"cps=100".to_string()),
                        "missing cps=100 in {:?}",
                        strs
                    );
                    assert!(
                        strs.contains(&"bps=1048576".to_string()),
                        "missing bps=1048576 in {:?}",
                        strs
                    );
                } else {
                    panic!("rate_limit value should be an array");
                }
            }
        }
        assert!(
            found_rl,
            "rate_limit key not found in GETUSER response: {:?}",
            response
        );
    } else {
        panic!("Expected Array response from ACL GETUSER");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_deluser_removes_state() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create a rate-limited user
    let response = admin
        .command(&[
            "ACL",
            "SETUSER",
            "tempuser",
            "on",
            ">pass",
            "+@all",
            "~*",
            "ratelimit:cps=5",
        ])
        .await;
    assert_ok(&response);

    // Delete the user
    let response = admin.command(&["ACL", "DELUSER", "tempuser"]).await;
    assert_eq!(response, Response::Integer(1));

    // User should no longer exist
    let response = admin.command(&["ACL", "GETUSER", "tempuser"]).await;
    // Returns Bulk(None) or Null for non-existent user
    assert!(
        matches!(response, Response::Null | Response::Bulk(None)),
        "Expected null response for deleted user, got: {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_resetratelimit_clears() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create user with rate limit
    let response = admin
        .command(&[
            "ACL",
            "SETUSER",
            "rluser",
            "on",
            ">pass",
            "+@all",
            "~*",
            "ratelimit:cps=5",
        ])
        .await;
    assert_ok(&response);

    // Reset rate limit
    let response = admin
        .command(&["ACL", "SETUSER", "rluser", "resetratelimit"])
        .await;
    assert_ok(&response);

    // Verify rate_limit is cleared in GETUSER
    let response = admin.command(&["ACL", "GETUSER", "rluser"]).await;
    if let Response::Array(items) = &response {
        for (i, item) in items.iter().enumerate() {
            if let Response::Bulk(Some(b)) = item
                && b.as_ref() == b"rate_limit"
                && let Response::Array(rl_items) = &items[i + 1]
            {
                assert!(
                    rl_items.is_empty(),
                    "rate_limit should be empty after resetratelimit"
                );
            }
        }
    }

    // User should not be rate limited anymore
    let mut client = server.connect().await;
    let response = client.command(&["AUTH", "rluser", "pass"]).await;
    assert_ok(&response);

    // Lots of commands should succeed
    for _ in 0..20 {
        let response = client.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_acl_list_includes_ratelimit() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    let response = admin
        .command(&[
            "ACL",
            "SETUSER",
            "rluser",
            "on",
            ">pass",
            "+@all",
            "~*",
            "ratelimit:cps=100",
        ])
        .await;
    assert_ok(&response);

    let response = admin.command(&["ACL", "LIST"]).await;
    if let Response::Array(items) = &response {
        let acl_strs: Vec<String> = items
            .iter()
            .filter_map(|r| {
                if let Response::Bulk(Some(b)) = r {
                    Some(String::from_utf8_lossy(b).to_string())
                } else {
                    None
                }
            })
            .collect();
        let rl_line = acl_strs
            .iter()
            .find(|s| s.contains("rluser"))
            .expect("rluser not found in ACL LIST");
        assert!(
            rl_line.contains("ratelimit:cps=100"),
            "ACL LIST should include ratelimit rule: {rl_line}"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_bytes_per_second() {
    let (server, mut admin) = start_server_with_admin("adminpass").await;

    // Create user with 100 bytes/second limit
    let mut client = create_and_auth_user(
        &server,
        &mut admin,
        "bytelimited",
        "pass",
        &["+@all", "~*", "ratelimit:bps=100"],
    )
    .await;

    // Send a command with lots of data to exceed byte budget
    let big_value = "x".repeat(200);
    let response = client.command(&["SET", "{k}key", &big_value]).await;
    // First one might succeed (burst budget is 100 bytes)
    // But second definitely should fail
    let response2 = client.command(&["SET", "{k}key2", &big_value]).await;
    // At least one should be rate limited
    let either_limited =
        matches!(&response, Response::Error(_)) || matches!(&response2, Response::Error(_));
    assert!(
        either_limited,
        "Expected at least one byte-rate-limited response"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_ratelimit_no_limit_default_user() {
    // Default user should have no rate limits — commands should succeed indefinitely
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for _ in 0..50 {
        let response = client.command(&["SET", "{k}foo", "bar"]).await;
        assert_ok(&response);
    }

    server.shutdown().await;
}
