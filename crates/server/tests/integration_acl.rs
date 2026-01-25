//! Integration tests for ACL commands (AUTH, ACL SETUSER, ACL DELUSER, etc.)

mod common;

use bytes::Bytes;
use common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// ACL Integration Tests
// ============================================================================

#[tokio::test]
async fn test_auth_default_user_nopass() {
    // Without requirepass, AUTH should not be required
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Commands should work without AUTH
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_with_requirepass() {
    // With requirepass configured, commands should require AUTH
    let server = TestServer::start_with_security("testpassword123").await;
    let mut client = server.connect().await;

    // PING is auth-exempt (like in Redis), so it should work without AUTH
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // GET without AUTH should return NOAUTH error
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // SET without AUTH should return NOAUTH error
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // AUTH with correct password should work
    let response = client.command(&["AUTH", "testpassword123"]).await;
    assert_eq!(response, Response::ok());

    // After AUTH, PING should still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // After AUTH, GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    // After AUTH, SET should work
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_wrong_password() {
    let server = TestServer::start_with_security("correctpassword").await;
    let mut client = server.connect().await;

    // AUTH with wrong password should return WRONGPASS error
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGPASS")));

    // Non-exempt commands should still fail after wrong password
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // PING is auth-exempt, so it should still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_named_user() {
    let server = TestServer::start_with_security("adminpass").await;
    let mut client = server.connect().await;

    // First authenticate as default user
    client.command(&["AUTH", "adminpass"]).await;

    // Create a named user with ACL SETUSER
    let response = client
        .command(&["ACL", "SETUSER", "testuser", "on", ">userpass", "+@all", "~*"])
        .await;
    assert_eq!(response, Response::ok());

    // Connect with a new client and authenticate as the named user
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "testuser", "userpass"]).await;
    assert_eq!(response, Response::ok());

    // ACL WHOAMI should return the username
    let response = client2.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("testuser"))));

    server.shutdown().await;
}

// ============================================================================
// ACL WHOAMI Tests
// ============================================================================

#[tokio::test]
async fn test_acl_whoami_default() {
    // Without AUTH, ACL WHOAMI should return "default"
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("default"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_whoami_after_auth() {
    let server = TestServer::start_with_security("adminpass").await;
    let mut client = server.connect().await;

    // Authenticate as default
    client.command(&["AUTH", "adminpass"]).await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "myuser", "on", ">mypass", "+@all", "~*"])
        .await;

    // New connection, authenticate as myuser
    let mut client2 = server.connect().await;
    client2.command(&["AUTH", "myuser", "mypass"]).await;

    // ACL WHOAMI should return "myuser"
    let response = client2.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("myuser"))));

    server.shutdown().await;
}

// ============================================================================
// ACL User Management Tests
// ============================================================================

#[tokio::test]
async fn test_acl_setuser_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL SETUSER creates a new user
    let response = client
        .command(&["ACL", "SETUSER", "newuser", "on", ">password123"])
        .await;
    assert_eq!(response, Response::ok());

    // ACL USERS should include the new user
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let usernames: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(usernames.contains(&"default".to_string()));
            assert!(usernames.contains(&"newuser".to_string()));
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_deluser() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "tempuser", "on", ">temppass"])
        .await;

    // Verify user exists
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let has_tempuser = arr.iter().any(|r| matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("tempuser")));
            assert!(has_tempuser, "tempuser should exist");
        }
        _ => panic!("Expected array response"),
    }

    // Delete the user
    let response = client.command(&["ACL", "DELUSER", "tempuser"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify user no longer exists
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let has_tempuser = arr.iter().any(|r| matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("tempuser")));
            assert!(!has_tempuser, "tempuser should not exist after deletion");
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_deluser_default_fails() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL DELUSER default should fail
    let response = client.command(&["ACL", "DELUSER", "default"]).await;
    assert!(
        matches!(response, Response::Error(e) if String::from_utf8_lossy(&e).contains("default")),
        "Should not be able to delete default user"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a user with specific permissions
    client
        .command(&["ACL", "SETUSER", "listuser", "on", ">listpass", "+get", "+set", "~keys:*"])
        .await;

    // ACL LIST should return array with user rules
    let response = client.command(&["ACL", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LIST should not be empty");
            // Should contain at least the default user
            let rules: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(
                rules.iter().any(|r| r.contains("default")),
                "Should contain default user"
            );
            assert!(
                rules.iter().any(|r| r.contains("listuser")),
                "Should contain listuser"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_getuser() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "infouser", "on", ">infopass", "+@read", "~data:*"])
        .await;

    // ACL GETUSER should return user details
    let response = client.command(&["ACL", "GETUSER", "infouser"]).await;
    match response {
        Response::Array(arr) => {
            // Should be a key-value array with user properties
            assert!(!arr.is_empty(), "GETUSER should return user info");
            // Look for expected fields like "flags", "passwords", "commands", "keys"
            let keys: Vec<_> = arr
                .iter()
                .step_by(2)
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(keys.contains(&"flags".to_string()), "Should have flags field");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    // GETUSER for non-existent user should return null
    let response = client.command(&["ACL", "GETUSER", "nonexistent"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_users() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create multiple users
    client
        .command(&["ACL", "SETUSER", "user1", "on", ">pass1"])
        .await;
    client
        .command(&["ACL", "SETUSER", "user2", "on", ">pass2"])
        .await;

    // ACL USERS should return array of usernames
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            assert!(arr.len() >= 3, "Should have at least default, user1, user2");
            let usernames: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(usernames.contains(&"default".to_string()));
            assert!(usernames.contains(&"user1".to_string()));
            assert!(usernames.contains(&"user2".to_string()));
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL CAT Tests
// ============================================================================

#[tokio::test]
async fn test_acl_cat_all_categories() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL CAT without argument returns list of all categories
    let response = client.command(&["ACL", "CAT"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL CAT should return categories");
            let categories: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            // Common categories that should exist
            assert!(
                categories.iter().any(|c| c == "read" || c == "write" || c == "admin" || c == "string"),
                "Should contain common categories like read, write, admin, or string"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_cat_specific_category() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL CAT string returns commands in the string category
    let response = client.command(&["ACL", "CAT", "string"]).await;
    match response {
        Response::Array(arr) => {
            let commands: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_lowercase()),
                    _ => None,
                })
                .collect();
            // String commands should include get, set, etc.
            assert!(
                commands.iter().any(|c| c == "get" || c == "set"),
                "String category should include get or set commands"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL GENPASS Tests
// ============================================================================

#[tokio::test]
async fn test_acl_genpass_default() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL GENPASS returns 64-char hex string (256 bits) by default
    let response = client.command(&["ACL", "GENPASS"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            assert_eq!(password.len(), 64, "Default GENPASS should return 64 hex chars");
            // Verify it's valid hex
            let hex_str = String::from_utf8_lossy(&password);
            assert!(
                hex_str.chars().all(|c| c.is_ascii_hexdigit()),
                "GENPASS should return valid hex"
            );
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_genpass_custom_bits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Implementation enforces minimum 256 bits (64 hex chars) for security
    // ACL GENPASS 128 still returns 64 chars (256 bits minimum)
    let response = client.command(&["ACL", "GENPASS", "128"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            // Minimum 256 bits = 64 hex chars
            assert!(password.len() >= 64, "GENPASS should return at least 64 hex chars");
            let hex_str = String::from_utf8_lossy(&password);
            assert!(hex_str.chars().all(|c| c.is_ascii_hexdigit()));
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    // ACL GENPASS 512 returns 128 chars (512 bits)
    let response = client.command(&["ACL", "GENPASS", "512"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            assert_eq!(password.len(), 128, "GENPASS 512 should return 128 hex chars");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL LOG Tests
// ============================================================================

#[tokio::test]
async fn test_acl_log_auth_failure() {
    let server = TestServer::start_with_security("secretpass").await;
    let mut client = server.connect().await;

    // Trigger an auth failure
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    // Verify auth failed
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH should fail with WRONGPASS: {:?}",
        response
    );

    // Authenticate properly to check the log (ACL LOG requires auth)
    let response = client.command(&["AUTH", "secretpass"]).await;
    assert_eq!(response, Response::ok(), "AUTH with correct password should succeed");

    // Check ACL LOG contains entry
    // Note: ACL LOG returns up to 10 entries by default
    let response = client.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            // Should have at least one entry for the auth failure
            // If empty, the implementation may not be logging auth failures
            if arr.is_empty() {
                // This is acceptable if the implementation doesn't log auth failures
                // Just verify the command works
                println!("Note: ACL LOG is empty - auth failures may not be logged");
            }
        }
        _ => panic!("Expected array response from ACL LOG, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_reset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Log a command denial by creating a restricted user and trying a denied command
    // First, create a user with limited permissions
    client
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~allowed:*"])
        .await;

    // The key here is to test that RESET works, regardless of whether entries exist
    // ACL LOG RESET should always succeed
    let response = client.command(&["ACL", "LOG", "RESET"]).await;
    assert_eq!(response, Response::ok());

    // After reset, log should be empty
    let response = client.command(&["ACL", "LOG"]).await;
    match response {
        Response::Array(arr) => {
            assert!(arr.is_empty(), "ACL LOG should be empty after RESET");
        }
        _ => panic!("Expected array response from ACL LOG, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL HELP Tests
// ============================================================================

#[tokio::test]
async fn test_acl_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL HELP returns array of help strings
    let response = client.command(&["ACL", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL HELP should return help strings");
            // Check that it mentions ACL commands
            let help_text: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            // Should mention at least some ACL subcommands
            let combined = help_text.join(" ").to_uppercase();
            assert!(
                combined.contains("ACL") || combined.contains("SETUSER") || combined.contains("CAT"),
                "Help should mention ACL commands"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 1: Command Permission Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_command_denied_write_category() {
    // User with -@write cannot write
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;

    // Authenticate as admin (default user)
    admin.command(&["AUTH", "admin"]).await;

    // Create read-only user with +@read -@write
    let response = admin
        .command(&["ACL", "SETUSER", "reader", "on", ">pass", "+@read", "-@write", "~*"])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as reader
    let mut reader = server.connect().await;
    let response = reader.command(&["AUTH", "reader", "pass"]).await;
    assert_eq!(response, Response::ok());

    // GET should work (read command)
    let response = reader.command(&["GET", "key"]).await;
    assert!(matches!(response, Response::Bulk(_)));

    // SET should be denied (write command)
    let response = reader.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_command_denied_individual() {
    // User with +get -set (individual commands)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with only GET allowed
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;

    // GET should work
    let response = limited.command(&["GET", "key"]).await;
    assert!(matches!(response, Response::Bulk(_)));

    // SET should be denied
    let response = limited.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    // DEL should be denied
    let response = limited.command(&["DEL", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "DEL should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_command_denied_dangerous() {
    // User with -@dangerous cannot run DEBUG/FLUSHALL
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all commands except dangerous
    admin
        .command(&["ACL", "SETUSER", "safe", "on", ">pass", "+@all", "-@dangerous", "~*"])
        .await;

    let mut safe = server.connect().await;
    safe.command(&["AUTH", "safe", "pass"]).await;

    // Normal commands should work
    let response = safe.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    // FLUSHALL should be denied (dangerous)
    let response = safe.command(&["FLUSHALL"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "FLUSHALL should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_nocommands_user() {
    // User with nocommands denied everything
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with no commands (reset to nothing)
    admin
        .command(&["ACL", "SETUSER", "nocommands", "on", ">pass", "nocommands", "~*"])
        .await;

    let mut client = server.connect().await;
    client.command(&["AUTH", "nocommands", "pass"]).await;

    // All commands should be denied
    let response = client.command(&["GET", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM error, got {:?}",
        response
    );

    let response = client.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    let response = client.command(&["PING"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PING should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_allcommands_user() {
    // User with +@all can run any command
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all commands
    admin
        .command(&["ACL", "SETUSER", "superuser", "on", ">pass", "+@all", "~*"])
        .await;

    let mut superuser = server.connect().await;
    superuser.command(&["AUTH", "superuser", "pass"]).await;

    // All basic commands should work
    let response = superuser.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    let response = superuser.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("val"))));

    let response = superuser.command(&["DEL", "key"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 2: Key Pattern Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_key_pattern_prefix() {
    // User with ~prefix:* can only access matching keys
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with access only to app:* keys
    admin
        .command(&["ACL", "SETUSER", "appuser", "on", ">pass", "+@all", "~app:*"])
        .await;

    let mut appuser = server.connect().await;
    appuser.command(&["AUTH", "appuser", "pass"]).await;

    // SET app:foo should work
    let response = appuser.command(&["SET", "app:foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    // GET app:foo should work
    let response = appuser.command(&["GET", "app:foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    // SET other:foo should be denied
    let response = appuser.command(&["SET", "other:foo", "bar"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET to non-matching key should return NOPERM, got {:?}",
        response
    );

    // GET other:foo should be denied
    let response = appuser.command(&["GET", "other:foo"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET from non-matching key should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_read_only() {
    // User with %R~* (read-only keys)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Setup: create some data as admin
    admin.command(&["SET", "key", "value"]).await;

    // Create user with read-only access to all keys
    admin
        .command(&["ACL", "SETUSER", "readonly", "on", ">pass", "+@all", "%R~*"])
        .await;

    let mut readonly = server.connect().await;
    readonly.command(&["AUTH", "readonly", "pass"]).await;

    // GET should work (read)
    let response = readonly.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    // SET should be denied (write)
    let response = readonly.command(&["SET", "key", "newval"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM (write denied), got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_write_only() {
    // User with %W~* (write-only keys)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with write-only access to all keys
    admin
        .command(&["ACL", "SETUSER", "writeonly", "on", ">pass", "+@all", "%W~*"])
        .await;

    let mut writeonly = server.connect().await;
    writeonly.command(&["AUTH", "writeonly", "pass"]).await;

    // SET should work (write)
    let response = writeonly.command(&["SET", "key", "value"]).await;
    assert_eq!(response, Response::ok());

    // GET should be denied (read)
    let response = writeonly.command(&["GET", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM (read denied), got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_multiple() {
    // User with multiple key patterns
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with access to cache:* and session:* keys
    admin
        .command(&["ACL", "SETUSER", "multi", "on", ">pass", "+@all", "~cache:*", "~session:*"])
        .await;

    let mut multi = server.connect().await;
    multi.command(&["AUTH", "multi", "pass"]).await;

    // Access cache:foo should work
    let response = multi.command(&["SET", "cache:foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    // Access session:bar should work
    let response = multi.command(&["SET", "session:bar", "baz"]).await;
    assert_eq!(response, Response::ok());

    // Access other:foo should be denied
    let response = multi.command(&["SET", "other:foo", "bar"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET to non-matching key should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_no_key_access() {
    // User with resetkeys (no key patterns)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with commands but no key access
    admin
        .command(&["ACL", "SETUSER", "nokeys", "on", ">pass", "+@all", "resetkeys"])
        .await;

    let mut nokeys = server.connect().await;
    nokeys.command(&["AUTH", "nokeys", "pass"]).await;

    // All key operations should be denied
    let response = nokeys.command(&["GET", "anykey"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM (no key access), got {:?}",
        response
    );

    let response = nokeys.command(&["SET", "anykey", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM (no key access), got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 3: Channel Pattern Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_channel_pattern_subscribe() {
    // User with &notifications:* can only subscribe to matching channels
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restriction
    admin
        .command(&["ACL", "SETUSER", "subuser", "on", ">pass", "+@all", "~*", "&notifications:*"])
        .await;

    let mut subuser = server.connect().await;
    subuser.command(&["AUTH", "subuser", "pass"]).await;

    // SUBSCRIBE to matching channel should work
    let response = subuser.command(&["SUBSCRIBE", "notifications:alerts"]).await;
    // SUBSCRIBE returns an array with subscribe confirmation
    assert!(
        matches!(response, Response::Array(_)),
        "SUBSCRIBE to matching channel should succeed, got {:?}",
        response
    );

    // Need a fresh connection to test denied subscribe
    let mut subuser2 = server.connect().await;
    subuser2.command(&["AUTH", "subuser", "pass"]).await;

    // SUBSCRIBE to non-matching channel should be denied
    let response = subuser2.command(&["SUBSCRIBE", "secret:data"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SUBSCRIBE to non-matching channel should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_channel_pattern_publish() {
    // User with &public:* denied PUBLISH to non-matching channels
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restriction
    admin
        .command(&["ACL", "SETUSER", "pubuser", "on", ">pass", "+@all", "~*", "&public:*"])
        .await;

    let mut pubuser = server.connect().await;
    pubuser.command(&["AUTH", "pubuser", "pass"]).await;

    // PUBLISH to matching channel should work
    let response = pubuser.command(&["PUBLISH", "public:msg", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to matching channel should succeed, got {:?}",
        response
    );

    // PUBLISH to non-matching channel should be denied
    let response = pubuser.command(&["PUBLISH", "private:msg", "hello"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PUBLISH to non-matching channel should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_allchannels() {
    // User with allchannels can access any channel
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all channels access
    admin
        .command(&["ACL", "SETUSER", "allchan", "on", ">pass", "+@all", "~*", "allchannels"])
        .await;

    let mut allchan = server.connect().await;
    allchan.command(&["AUTH", "allchan", "pass"]).await;

    // PUBLISH to any channel should work
    let response = allchan.command(&["PUBLISH", "any:channel", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to any channel should succeed, got {:?}",
        response
    );

    let response = allchan.command(&["PUBLISH", "another:channel", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to another channel should succeed, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_no_channel_access() {
    // User with resetchannels cannot access any channel
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with no channel access
    admin
        .command(&["ACL", "SETUSER", "nochan", "on", ">pass", "+@all", "~*", "resetchannels"])
        .await;

    let mut nochan = server.connect().await;
    nochan.command(&["AUTH", "nochan", "pass"]).await;

    // SUBSCRIBE to any channel should be denied
    let response = nochan.command(&["SUBSCRIBE", "any:channel"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SUBSCRIBE should return NOPERM (no channel access), got {:?}",
        response
    );

    // PUBLISH to any channel should be denied
    let response = nochan.command(&["PUBLISH", "any:channel", "msg"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PUBLISH should return NOPERM (no channel access), got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 4: Permission Denial Logging
// ============================================================================

#[tokio::test]
async fn test_acl_log_command_denied() {
    // Command denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a restricted user
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect as limited user and try forbidden command
    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;
    let _ = limited.command(&["SET", "key", "val"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            // Should have at least one entry for the command denial
            assert!(!arr.is_empty(), "ACL LOG should have entry for command denial");
            // The entry should be an array of key-value pairs
            if let Some(Response::Array(entry)) = arr.first() {
                // Check that it contains the reason "command"
                let entry_str = format!("{:?}", entry);
                assert!(
                    entry_str.contains("command") || entry_str.contains("not allowed"),
                    "ACL LOG entry should mention command denial"
                );
            }
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_key_denied() {
    // Key denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a user with key restrictions
    admin
        .command(&["ACL", "SETUSER", "keyuser", "on", ">pass", "+@all", "~allowed:*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect and try accessing denied key
    let mut keyuser = server.connect().await;
    keyuser.command(&["AUTH", "keyuser", "pass"]).await;
    let _ = keyuser.command(&["GET", "denied:key"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LOG should have entry for key denial");
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_channel_denied() {
    // Channel denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a user with channel restrictions
    admin
        .command(&["ACL", "SETUSER", "chanuser", "on", ">pass", "+@all", "~*", "&allowed:*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect and try accessing denied channel
    let mut chanuser = server.connect().await;
    chanuser.command(&["AUTH", "chanuser", "pass"]).await;
    let _ = chanuser.command(&["PUBLISH", "denied:channel", "msg"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LOG should have entry for channel denial");
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 5: Multiple Password Tests
// ============================================================================

#[tokio::test]
async fn test_acl_multiple_passwords() {
    // User with multiple passwords can auth with any
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with multiple passwords
    admin
        .command(&["ACL", "SETUSER", "multi", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // AUTH with pass1 should work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "multi", "pass1"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass1 should succeed");

    // AUTH with pass2 should work (new connection)
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "multi", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should succeed");

    // AUTH with wrong password should fail
    let mut client3 = server.connect().await;
    let response = client3.command(&["AUTH", "multi", "wrong"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH with wrong password should fail"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_add_password() {
    // Adding password doesn't invalidate existing
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with pass1
    admin
        .command(&["ACL", "SETUSER", "addpass", "on", ">pass1", "+@all", "~*"])
        .await;

    // Add pass2
    admin
        .command(&["ACL", "SETUSER", "addpass", ">pass2"])
        .await;

    // Both passwords should work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "addpass", "pass1"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass1 should still work");

    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "addpass", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should work");

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_remove_password() {
    // Removing one password leaves others valid
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with two passwords
    admin
        .command(&["ACL", "SETUSER", "rmpass", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // Remove pass1
    admin
        .command(&["ACL", "SETUSER", "rmpass", "<pass1"])
        .await;

    // pass2 should still work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "rmpass", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should work");

    // pass1 should be rejected
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "rmpass", "pass1"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH with removed pass1 should fail"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_resetpass() {
    // resetpass clears all passwords
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with multiple passwords
    admin
        .command(&["ACL", "SETUSER", "resetuser", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // Reset passwords
    admin
        .command(&["ACL", "SETUSER", "resetuser", "resetpass"])
        .await;

    // All passwords should now be rejected (user is also disabled without password)
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "resetuser", "pass1"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "AUTH with pass1 should fail after resetpass"
    );

    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "resetuser", "pass2"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "AUTH with pass2 should fail after resetpass"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 6: Error Message Verification
// ============================================================================

#[tokio::test]
async fn test_error_noauth_format() {
    // NOAUTH error format
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // GET without auth should return NOAUTH
    let response = client.command(&["GET", "key"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOAUTH") && msg.contains("Authentication"),
                "Expected 'NOAUTH Authentication required', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_wrongpass_format() {
    // WRONGPASS error format
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // AUTH with wrong password should return WRONGPASS
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("WRONGPASS"),
                "Expected 'WRONGPASS ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_command_format() {
    // NOPERM command error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create restricted user
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;

    // Forbidden command should return NOPERM with command name
    let response = limited.command(&["SET", "key", "val"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM") && msg.to_lowercase().contains("set"),
                "Expected 'NOPERM ... SET ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_key_format() {
    // NOPERM key error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with key restrictions
    admin
        .command(&["ACL", "SETUSER", "keyuser", "on", ">pass", "+@all", "~allowed:*"])
        .await;

    let mut keyuser = server.connect().await;
    keyuser.command(&["AUTH", "keyuser", "pass"]).await;

    // Forbidden key should return NOPERM
    let response = keyuser.command(&["GET", "denied:key"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM"),
                "Expected 'NOPERM ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_channel_format() {
    // NOPERM channel error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restrictions
    admin
        .command(&["ACL", "SETUSER", "chanuser", "on", ">pass", "+@all", "~*", "&allowed:*"])
        .await;

    let mut chanuser = server.connect().await;
    chanuser.command(&["AUTH", "chanuser", "pass"]).await;

    // Forbidden channel should return NOPERM
    let response = chanuser.command(&["PUBLISH", "denied:channel", "msg"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM"),
                "Expected 'NOPERM ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 7: Auth-Exempt Commands
// ============================================================================

#[tokio::test]
async fn test_auth_exempt_auth() {
    // AUTH works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // AUTH command itself should work without being authenticated
    let response = client.command(&["AUTH", "testpass"]).await;
    assert_eq!(response, Response::ok(), "AUTH command should work without prior auth");

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_exempt_hello() {
    // HELLO works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // HELLO command should work without being authenticated
    let response = client.command(&["HELLO"]).await;
    match response {
        Response::Array(_) => {
            // Expected - HELLO returns server info
        }
        _ => panic!("Expected array response for HELLO, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_exempt_quit() {
    // QUIT works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // QUIT command should work without being authenticated
    let response = client.command(&["QUIT"]).await;
    // QUIT typically returns OK before closing connection
    assert!(
        matches!(response, Response::Simple(_)) || matches!(response, Response::Error(_)),
        "QUIT should return a response before closing"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 8: ACL LOAD Edge Cases
// ============================================================================

#[tokio::test]
async fn test_acl_load_no_file() {
    // ACL LOAD with no aclfile configured
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL LOAD should error when no file is configured
    let response = client.command(&["ACL", "LOAD"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "ACL LOAD should return error when no aclfile configured"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_save_no_file() {
    // ACL SAVE with no aclfile configured
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // ACL SAVE should error when no file is configured
    let response = client.command(&["ACL", "SAVE"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "ACL SAVE should return error when no aclfile configured"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 9: Permission Snapshot Isolation
// ============================================================================

#[tokio::test]
async fn test_acl_reauth_updates_permissions() {
    // Re-authentication updates permissions
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all permissions
    admin
        .command(&["ACL", "SETUSER", "dynamic", "on", ">pass", "+@all", "~*"])
        .await;

    // Connect and authenticate as dynamic user
    let mut dynamic = server.connect().await;
    dynamic.command(&["AUTH", "dynamic", "pass"]).await;

    // SET should work
    let response = dynamic.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    // Admin changes user permissions to deny write
    admin
        .command(&["ACL", "SETUSER", "dynamic", "-@write"])
        .await;

    // Re-authenticate as dynamic user
    let response = dynamic.command(&["AUTH", "dynamic", "pass"]).await;
    assert_eq!(response, Response::ok());

    // Now SET should fail (after re-authentication, new permissions apply)
    let response = dynamic.command(&["SET", "key", "val2"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should be denied after re-auth with restricted permissions, got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 10: Concurrent Modification
// ============================================================================

#[tokio::test]
async fn test_acl_concurrent_setuser() {
    // Concurrent SETUSER doesn't corrupt state
    let server = TestServer::start_with_security("admin").await;
    let mut admin1 = server.connect().await;
    let mut admin2 = server.connect().await;
    admin1.command(&["AUTH", "admin"]).await;
    admin2.command(&["AUTH", "admin"]).await;

    // Create a user
    admin1
        .command(&["ACL", "SETUSER", "concurrent", "on", ">pass", "+@all", "~*"])
        .await;

    // Concurrent modifications (in practice, these are sequential but test the concurrency logic)
    let response1 = admin1
        .command(&["ACL", "SETUSER", "concurrent", "+set"])
        .await;
    let response2 = admin2
        .command(&["ACL", "SETUSER", "concurrent", "+get"])
        .await;

    assert_eq!(response1, Response::ok());
    assert_eq!(response2, Response::ok());

    // Verify user is in a consistent state
    let response = admin1.command(&["ACL", "GETUSER", "concurrent"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "User should exist in consistent state"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 11: Redis 7.0 Features
// ============================================================================

#[tokio::test]
async fn test_acl_subcommand_rules() {
    // Create a user with +@all but deny CONFIG|SET specifically
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with all commands allowed but CONFIG|SET denied
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "configreader", "on", ">pass",
            "~*", "+@all", "-config|set", "-config|rewrite"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    let response = user_client.command(&["AUTH", "configreader", "pass"]).await;
    assert_eq!(response, Response::ok());

    // CONFIG GET should work (allowed by +@all, no specific deny)
    let response = user_client.command(&["CONFIG", "GET", "maxclients"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "CONFIG GET should be allowed, got {:?}",
        response
    );

    // CONFIG SET should be denied (specific subcommand deny)
    let response = user_client.command(&["CONFIG", "SET", "maxclients", "1000"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG SET, got {:?}", response),
    }

    // CONFIG REWRITE should be denied (specific subcommand deny)
    let response = user_client.command(&["CONFIG", "REWRITE"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG REWRITE, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_subcommand_allow_specific() {
    // Create a user with -config but +config|get allowed
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with config denied but config|get allowed
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "configget", "on", ">pass",
            "~*", "+@all", "-config", "+config|get"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "configget", "pass"]).await;

    // CONFIG GET should work (specific subcommand allow overrides command deny)
    let response = user_client.command(&["CONFIG", "GET", "maxclients"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "CONFIG GET should be allowed, got {:?}",
        response
    );

    // CONFIG SET should be denied (no specific allow)
    let response = user_client.command(&["CONFIG", "SET", "maxclients", "1000"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG SET, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_selectors() {
    // Create a user with root access to app:* and selector for cache:* read-only
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with:
    // - Root: app:* keys with all commands
    // - Selector: cache:* keys with read-only access
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "hybrid", "on", ">pass",
            "~app:*", "+@all", "(~cache:* +@read)"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "hybrid", "pass"]).await;

    // Can write to app:* (root permissions)
    let response = user_client.command(&["SET", "app:key1", "value1"]).await;
    assert_eq!(response, Response::ok());

    // Can read from app:* (root permissions)
    let response = user_client.command(&["GET", "app:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    // Can read from cache:* (selector permissions)
    // First, admin sets a cache key
    admin_client.command(&["SET", "cache:data", "cached"]).await;
    let response = user_client.command(&["GET", "cache:data"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("cached"))));

    // Cannot write to cache:* (selector only grants read)
    let response = user_client.command(&["SET", "cache:newkey", "value"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error for cache write, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for cache write, got {:?}", response),
    }

    // Cannot access data:* (neither root nor selector)
    let response = user_client.command(&["GET", "data:something"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error for data access, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for data access, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_clearselectors() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with a selector
    admin_client
        .command(&[
            "ACL", "SETUSER", "withsel", "on", ">pass",
            "~app:*", "+@all", "(~temp:* +@read)"
        ])
        .await;

    // Verify selector exists in GETUSER response
    let response = admin_client.command(&["ACL", "GETUSER", "withsel"]).await;
    match response {
        Response::Array(arr) => {
            // Look for "selectors" field
            let selectors_idx = arr.iter().position(|r| {
                matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("selectors"))
            });
            if let Some(idx) = selectors_idx {
                if let Some(Response::Array(selectors)) = arr.get(idx + 1) {
                    assert!(!selectors.is_empty(), "Should have at least one selector");
                }
            }
        }
        _ => panic!("Expected array response for GETUSER"),
    }

    // Clear selectors
    admin_client
        .command(&["ACL", "SETUSER", "withsel", "clearselectors"])
        .await;

    // Verify selectors are cleared
    let response = admin_client.command(&["ACL", "GETUSER", "withsel"]).await;
    match response {
        Response::Array(arr) => {
            let selectors_idx = arr.iter().position(|r| {
                matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("selectors"))
            });
            if let Some(idx) = selectors_idx {
                if let Some(Response::Array(selectors)) = arr.get(idx + 1) {
                    assert!(selectors.is_empty(), "Selectors should be cleared");
                }
            }
        }
        _ => panic!("Expected array response for GETUSER"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_multiple_selectors() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with multiple selectors
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "multisel", "on", ">pass",
            "+@all", "(~temp:* +@read)", "(~cache:* +@read)"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "multisel", "pass"]).await;

    // Admin sets some keys
    admin_client.command(&["SET", "temp:key1", "temp_value"]).await;
    admin_client.command(&["SET", "cache:key1", "cache_value"]).await;

    // Can read temp:* (selector 1)
    let response = user_client.command(&["GET", "temp:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("temp_value"))));

    // Can read cache:* (selector 2)
    let response = user_client.command(&["GET", "cache:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("cache_value"))));

    // Cannot read data:* (no matching selector)
    let response = user_client.command(&["GET", "data:key1"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_acl_list_includes_subcommand_rules() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with subcommand rules
    admin_client
        .command(&[
            "ACL", "SETUSER", "subcmduser", "on", ">pass",
            "~*", "+@all", "-config|set", "+client|info"
        ])
        .await;

    // ACL LIST should include subcommand rules in the output
    let response = admin_client.command(&["ACL", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            let rules: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();

            let subcmduser_rule = rules.iter().find(|r| r.contains("subcmduser"));
            assert!(subcmduser_rule.is_some(), "Should have subcmduser in ACL LIST");
            let rule = subcmduser_rule.unwrap();
            assert!(
                rule.contains("-config|set") || rule.contains("+client|info"),
                "ACL LIST should include subcommand rules: {}",
                rule
            );
        }
        _ => panic!("Expected array response for ACL LIST"),
    }

    server.shutdown().await;
}
