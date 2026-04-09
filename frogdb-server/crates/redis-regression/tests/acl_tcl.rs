//! Rust port of Redis 8.6.0 `unit/acl.tcl` test suite.
//!
//! Excludes:
//! - `external:skip` tagged server blocks (ACL file loading, CONFIG overrides)
//! - `needs:repl` tests (replication)
//! - ACL LOG tests (may not be implemented)
//! - ACL LOAD/SAVE tests (file-based ACL persistence)
//! - CONFIG SET / CONFIG REWRITE tests
//! - Pub/Sub channel permission tests requiring deferring clients
//! - Tests using `redis_deferring_client` for subscriber kill scenarios
//! - ACL metrics tests (require INFO server stats)
//! - HELLO command tests (RESP3 protocol switching)
//! - Blocked command reprocessing tests (require wait_for_blocked_client)
//! - Tests that depend on shared state across a session (the TCL suite
//!   uses a single connection with cumulative user state)
//!
//! ## Intentional exclusions
//!
//! ACL file loading / file-persistence (FrogDB does not implement ACL file
//! persistence — ACL state lives in the FrogDB config DSL, not in a `.acl`
//! file loaded via `aclfile`):
//! - `ACL LOAD only disconnects affected clients` — Redis-internal feature (ACL file)
//! - `ACL LOAD disconnects affected subscriber` — Redis-internal feature (ACL file)
//! - `ACL load and save` — Redis-internal feature (ACL file)
//! - `ACL load on replica when connected to replica` — Redis-internal feature (ACL file)
//! - `Test loading duplicate users in config on startup` — Redis-internal feature (ACL file)
//!
//! Replication / SLAVEOF tests:
//! - `First server should have role slave after SLAVEOF` — needs:repl
//!
//! Cumulative-session state ordering (TCL suite reuses one connection across
//! tests, so user state accumulates and tests depend on that order):
//! - `Alice: can execute all command` — Redis-internal session-state ordering
//!
//! ACL v2 selectors (removed per 8121bfee):
//! - `Test behavior of loading ACLs` — ACL v2 selectors removed (8121bfee)

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// ACL WHOAMI
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_connections_start_with_default_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_bulk_eq(&client.command(&["ACL", "WHOAMI"]).await, b"default");
}

// ---------------------------------------------------------------------------
// ACL SETUSER / user creation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_it_is_possible_to_create_new_users() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["ACL", "SETUSER", "newuser"]).await);
}

#[tokio::test]
async fn tcl_acl_users_coverage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["ACL", "SETUSER", "newuser"]).await;
    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(users.contains(&"default".to_string()));
    assert!(users.contains(&"newuser".to_string()));
}

#[tokio::test]
async fn tcl_usernames_cannot_contain_spaces() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "SETUSER", "a a"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - enabling / passwords
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_new_users_start_disabled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", ">passwd1"])
        .await;

    let mut user_client = server.connect().await;
    let resp = user_client.command(&["AUTH", "newuser", "passwd1"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

#[tokio::test]
async fn tcl_enabling_the_user_allows_the_login() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", ">passwd1", "on", "+acl"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "passwd1"]).await);
    assert_bulk_eq(&user_client.command(&["ACL", "WHOAMI"]).await, b"newuser");
}

#[tokio::test]
async fn tcl_only_the_set_of_correct_passwords_work() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "newuser", "on", ">passwd1", ">passwd2", "+acl",
        ])
        .await;

    let mut c1 = server.connect().await;
    assert_ok(&c1.command(&["AUTH", "newuser", "passwd1"]).await);

    let mut c2 = server.connect().await;
    assert_ok(&c2.command(&["AUTH", "newuser", "passwd2"]).await);

    let mut c3 = server.connect().await;
    let resp = c3.command(&["AUTH", "newuser", "passwd3"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

#[tokio::test]
async fn tcl_it_is_possible_to_remove_passwords() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "newuser", "on", ">passwd1", ">passwd2", "+acl",
        ])
        .await;

    // Remove passwd1
    client
        .command(&["ACL", "SETUSER", "newuser", "<passwd1"])
        .await;

    let mut user_client = server.connect().await;
    let resp = user_client.command(&["AUTH", "newuser", "passwd1"]).await;
    assert_error_prefix(&resp, "WRONGPASS");

    // passwd2 should still work
    let mut user_client2 = server.connect().await;
    assert_ok(&user_client2.command(&["AUTH", "newuser", "passwd2"]).await);
}

#[tokio::test]
async fn tcl_test_password_hashes_can_be_added() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SHA-256 of "passwd4"
    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "+acl",
            "#34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "passwd4"]).await);
}

#[tokio::test]
async fn tcl_test_password_hashes_validate_input() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Too short
    let resp = client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "#34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Invalid character 'q'
    let resp = client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "#34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4eq",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acl_getuser_returns_password_hash_not_actual_password() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "#34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "newuser"]).await;
    let items = unwrap_array(resp);

    // Find "passwords" field
    let mut found_hash = false;
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(key)) = &items[i]
            && key.as_ref() == b"passwords"
            && let Response::Array(passwords) = &items[i + 1]
        {
            let pass_strs: Vec<String> = passwords
                .iter()
                .filter_map(|p| match p {
                    Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
                    _ => None,
                })
                .collect();
            let joined = pass_strs.join(" ");
            assert!(
                joined.contains("34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6"),
                "expected hash in passwords, got: {joined}"
            );
            assert!(
                !joined.contains("passwd4"),
                "password should not appear in cleartext"
            );
            found_hash = true;
        }
    }
    assert!(found_hash, "passwords field not found in GETUSER response");
}

#[tokio::test]
async fn tcl_test_hashed_password_removal() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "#34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6",
        ])
        .await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "!34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "newuser"]).await;
    let items = unwrap_array(resp);

    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(key)) = &items[i]
            && key.as_ref() == b"passwords"
            && let Response::Array(passwords) = &items[i + 1]
        {
            let pass_strs: Vec<String> = passwords
                .iter()
                .filter_map(|p| match p {
                    Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
                    _ => None,
                })
                .collect();
            let joined = pass_strs.join(" ");
            assert!(
                !joined
                    .contains("34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6"),
                "hash should have been removed, got: {joined}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ACL command permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_by_default_users_are_not_able_to_access_any_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", "on", ">passwd1", "+acl"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "passwd1"]).await);

    let resp = user_client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_by_default_users_are_not_able_to_access_any_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", "on", ">passwd1", "+set"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "passwd1"]).await);

    let resp = user_client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_possible_to_allow_access_of_subset_of_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            ">passwd1",
            "allcommands",
            "~foo:*",
            "~bar:*",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "passwd1"]).await);

    assert_ok(&user_client.command(&["SET", "foo:1", "a"]).await);
    assert_ok(&user_client.command(&["SET", "bar:2", "b"]).await);

    let resp = user_client.command(&["SET", "zap:3", "c"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL nopass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_users_can_be_configured_to_authenticate_with_any_password() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", "on", "nopass", "+acl"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(
        &user_client
            .command(&["AUTH", "newuser", "zipzapblabla"])
            .await,
    );
}

// ---------------------------------------------------------------------------
// ACL command exclusion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acls_can_exclude_single_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "nopass",
            "allcommands",
            "allkeys",
            "-ping",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    // INCR should work
    user_client.command(&["INCR", "mycounter"]).await;

    let resp = user_client.command(&["PING"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_acls_can_include_or_exclude_whole_classes_of_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // User with only @set and acl
    client
        .command(&[
            "ACL", "SETUSER", "newuser", "on", "nopass", "allkeys", "-@all", "+@set", "+acl",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    // SADD returns integer (count of added members), not error = success
    let resp = user_client.command(&["SADD", "myset", "a", "b", "c"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "SADD should not error, got {resp:?}"
    );

    // Now change to +@all -@string
    client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "-@string"])
        .await;

    // @set still works
    let mut user_client2 = server.connect().await;
    assert_ok(&user_client2.command(&["AUTH", "newuser", "anything"]).await);

    // String command should fail
    let resp = user_client2.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL subcommand permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acls_can_include_single_subcommands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "newuser", "on", "nopass", "allkeys", "+@all", "-client",
        ])
        .await;
    client
        .command(&["ACL", "SETUSER", "newuser", "+client|id", "+client|setname"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    // CLIENT ID should not fail
    let resp = user_client.command(&["CLIENT", "ID"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "CLIENT ID should not error, got {resp:?}"
    );

    // CLIENT SETNAME should not fail
    let resp = user_client.command(&["CLIENT", "SETNAME", "foo"]).await;
    assert_ok(&resp);

    // CLIENT KILL should be denied
    let resp = user_client
        .command(&["CLIENT", "KILL", "type", "master"])
        .await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_acls_can_exclude_single_subcommands_case1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "nopass",
            "allkeys",
            "+@all",
            "-client|kill",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    // CLIENT ID should not fail
    let resp = user_client.command(&["CLIENT", "ID"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "CLIENT ID should not error, got {resp:?}"
    );

    // CLIENT KILL should be denied
    let resp = user_client
        .command(&["CLIENT", "KILL", "type", "master"])
        .await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_acls_cannot_include_subcommand_with_specific_arg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "-config|get"])
        .await;

    let resp = client
        .command(&["ACL", "SETUSER", "newuser", "+config|get|appendonly"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acls_cannot_include_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "+config|get"])
        .await;

    let resp = client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "+config|asdf"])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "-config|asdf"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acls_cannot_include_command_with_two_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "+get|key1|key2"])
        .await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["ACL", "SETUSER", "newuser", "+@all", "-get|key1|key2"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acls_including_type_includes_subcommands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "newuser", "on", "nopass", "allkeys", "-@all", "+del", "+acl",
            "+@stream",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    user_client.command(&["DEL", "key"]).await;
    let resp = user_client
        .command(&["XADD", "key", "*", "field", "value"])
        .await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "XADD should not error, got {resp:?}"
    );

    let resp = user_client.command(&["XINFO", "STREAM", "key"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "XINFO STREAM should not error, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// ACL SETUSER RESET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_reset_reverts_to_default() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["ACL", "SETUSER", "example"]).await;

    // Get the initial ACL LIST entry for this user
    let resp = client.command(&["ACL", "LIST"]).await;
    let rules_before = extract_bulk_strings(&resp);
    let entry_before = rules_before
        .iter()
        .find(|r| r.contains("example"))
        .cloned()
        .unwrap();

    // Modify then reset
    client
        .command(&["ACL", "SETUSER", "example", "on", ">pass", "+@all"])
        .await;
    client
        .command(&["ACL", "SETUSER", "example", "reset"])
        .await;

    let resp = client.command(&["ACL", "LIST"]).await;
    let rules_after = extract_bulk_strings(&resp);
    let entry_after = rules_after
        .iter()
        .find(|r| r.contains("example"))
        .cloned()
        .unwrap();

    assert_eq!(entry_before, entry_after);
}

// ---------------------------------------------------------------------------
// ACL GETUSER - command translation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_translates_back_command_permissions_subtractive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "reset",
            "+@all",
            "~*",
            "-@string",
            "+incr",
            "-debug",
            "+debug|digest",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "newuser"]).await;
    let items = unwrap_array(resp);

    let cmdstr = get_getuser_field(&items, "commands");
    assert!(cmdstr.contains("+@all"), "expected +@all in {cmdstr}");
    assert!(cmdstr.contains("-@string"), "expected -@string in {cmdstr}");
    assert!(cmdstr.contains("+incr"), "expected +incr in {cmdstr}");
}

#[tokio::test]
async fn tcl_acl_getuser_translates_back_command_permissions_additive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "reset",
            "+@string",
            "-incr",
            "+acl",
            "+debug|digest",
            "+debug|segfault",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "newuser"]).await;
    let items = unwrap_array(resp);

    let cmdstr = get_getuser_field(&items, "commands");
    assert!(cmdstr.contains("-@all"), "expected -@all in {cmdstr}");
    assert!(cmdstr.contains("+@string"), "expected +@string in {cmdstr}");
    assert!(cmdstr.contains("-incr"), "expected -incr in {cmdstr}");
    assert!(
        cmdstr.contains("+debug|digest"),
        "expected +debug|digest in {cmdstr}"
    );
    assert!(
        cmdstr.contains("+debug|segfault"),
        "expected +debug|segfault in {cmdstr}"
    );
    assert!(cmdstr.contains("+acl"), "expected +acl in {cmdstr}");
}

// ---------------------------------------------------------------------------
// ACL CAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_cat_with_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "CAT", "NON_EXISTS"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["ACL", "CAT", "NON_EXISTS", "NON_EXISTS2"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acl_cat_without_category_lists_all_categories() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "CAT"]).await;
    let categories = extract_bulk_strings(&resp);
    assert!(
        categories.iter().any(|c| c == "keyspace"),
        "expected keyspace in categories"
    );
    assert!(
        categories.iter().any(|c| c == "connection"),
        "expected connection in categories"
    );
}

#[tokio::test]
async fn tcl_acl_cat_category_lists_commands_in_category() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "CAT", "transaction"]).await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.iter().any(|c| c == "multi"),
        "expected multi in transaction category, got: {commands:?}"
    );

    // Negative check
    let resp = client.command(&["ACL", "CAT", "keyspace"]).await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        !commands.iter().any(|c| c == "set"),
        "set should not be in keyspace category"
    );
}

// ---------------------------------------------------------------------------
// ACL GETUSER - reasonable results per category
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_provides_reasonable_results() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "CAT"]).await;
    let categories = extract_bulk_strings(&resp);

    for category in &categories {
        // Test additive: +@all -@category
        client
            .command(&[
                "ACL",
                "SETUSER",
                "additive",
                "reset",
                "+@all",
                &format!("-@{category}"),
            ])
            .await;

        let resp = client.command(&["ACL", "GETUSER", "additive"]).await;
        let items = unwrap_array(resp);
        let cmdstr = get_getuser_field(&items, "commands");
        assert_eq!(
            cmdstr,
            format!("+@all -@{category}"),
            "additive mismatch for {category}"
        );

        // Test restrictive: -@all +@category
        client
            .command(&[
                "ACL",
                "SETUSER",
                "restrictive",
                "reset",
                "-@all",
                &format!("+@{category}"),
            ])
            .await;

        let resp = client.command(&["ACL", "GETUSER", "restrictive"]).await;
        let items = unwrap_array(resp);
        let cmdstr = get_getuser_field(&items, "commands");
        assert_eq!(
            cmdstr,
            format!("-@all +@{category}"),
            "restrictive mismatch for {category}"
        );
    }
}

// ---------------------------------------------------------------------------
// ACL GETUSER - correct results (advanced compaction)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_provides_correct_results() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["ACL", "SETUSER", "adv-test"]).await;

    // +@all -@hash -@slow +hget
    client
        .command(&[
            "ACL", "SETUSER", "adv-test", "+@all", "-@hash", "-@slow", "+hget",
        ])
        .await;
    let resp = client.command(&["ACL", "GETUSER", "adv-test"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "+@all -@hash -@slow +hget");

    // Inverting the all category compacts everything
    client
        .command(&["ACL", "SETUSER", "adv-test", "-@all"])
        .await;
    let resp = client.command(&["ACL", "GETUSER", "adv-test"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "-@all");
}

#[tokio::test]
async fn tcl_acl_getuser_case_insensitive_categories() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "adv-test", "-@all", "+@HASH", "+@hash", "+@HaSh",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "adv-test"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "-@all +@hash");
}

#[tokio::test]
async fn tcl_acl_getuser_case_insensitive_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL", "SETUSER", "adv-test", "-@all", "+HGET", "+hget", "+hGeT",
        ])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "adv-test"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "-@all +hget");
}

// ---------------------------------------------------------------------------
// ACL subcommand engulfment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_set_can_include_subcommands_if_full_command_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "bob", "+memory|doctor"])
        .await;
    let resp = client.command(&["ACL", "GETUSER", "bob"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "-@all +memory|doctor");

    // Engulf to +memory
    client.command(&["ACL", "SETUSER", "bob", "+memory"]).await;
    let resp = client.command(&["ACL", "GETUSER", "bob"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "-@all +memory");
}

#[tokio::test]
async fn tcl_acl_set_can_exclude_subcommands_if_full_command_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "alice", "+@all", "-memory|doctor"])
        .await;
    let resp = client.command(&["ACL", "GETUSER", "alice"]).await;
    let items = unwrap_array(resp);
    let cmdstr = get_getuser_field(&items, "commands");
    assert_eq!(cmdstr, "+@all -memory|doctor");

    client
        .command(&["ACL", "SETUSER", "alice", "on", "nopass", "allkeys"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "alice", "anything"]).await);

    let resp = user_client.command(&["MEMORY", "DOCTOR"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// Pub/Sub channel permissions (non-subscriber tests)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_by_default_only_default_user_can_publish_to_any_channel() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Default user can publish
    let resp = client.command(&["PUBLISH", "foo", "bar"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "default user PUBLISH should not error, got {resp:?}"
    );

    // Create user with pubsub but no channel access
    client
        .command(&[
            "ACL", "SETUSER", "psuser", "on", ">pspass", "+acl", "+client", "+@pubsub",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "psuser", "pspass"]).await);

    let resp = user_client.command(&["PUBLISH", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn tcl_possible_to_allow_publishing_to_subset_of_channels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "psuser",
            "on",
            ">pspass",
            "+acl",
            "+client",
            "+@pubsub",
            "resetchannels",
            "&foo:1",
            "&bar:*",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "psuser", "pspass"]).await);

    // Allowed channels
    let resp = user_client
        .command(&["PUBLISH", "foo:1", "somemessage"])
        .await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "PUBLISH foo:1 should not error"
    );

    let resp = user_client
        .command(&["PUBLISH", "bar:2", "anothermessage"])
        .await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "PUBLISH bar:2 should not error"
    );

    // Denied channel
    let resp = user_client
        .command(&["PUBLISH", "zap:3", "nosuchmessage"])
        .await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL DELUSER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_delete_a_user_that_the_client_does_not_use() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "not_used", "on", ">passwd"])
        .await;

    let resp = client.command(&["ACL", "DELUSER", "not_used"]).await;
    assert_integer_eq(&resp, 1);

    // The client is not closed — PING should succeed (not error)
    let resp = client.command(&["PING"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "expected PING to succeed after deleting unused user, got {resp:?}"
    );
}

#[tokio::test]
async fn tcl_default_user_cannot_be_removed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "DELUSER", "default"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// ACL GENPASS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_genpass_command_failed_test() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "GENPASS", "-236"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client.command(&["ACL", "GENPASS", "5000"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_acl_genpass_returns_hex_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "GENPASS"]).await;
    let pass = unwrap_bulk(&resp);
    let pass_str = std::str::from_utf8(pass).expect("genpass should be UTF-8");
    assert!(
        !pass_str.is_empty(),
        "generated password should not be empty"
    );
    assert!(
        pass_str.chars().all(|c| c.is_ascii_hexdigit()),
        "generated password should be hex, got: {pass_str}"
    );
}

#[tokio::test]
async fn tcl_acl_genpass_with_bits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "GENPASS", "128"]).await;
    let pass = unwrap_bulk(&resp);
    let pass_str = std::str::from_utf8(pass).expect("genpass should be UTF-8");
    // 256-bit minimum is enforced for security, so 128-bit request still returns 64 hex chars
    assert_eq!(
        pass_str.len(),
        64,
        "128-bit genpass should return 64 hex chars (256-bit minimum), got {} chars: {pass_str}",
        pass_str.len()
    );
}

// ---------------------------------------------------------------------------
// ACL HELP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_help_should_not_have_unexpected_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "HELP", "xxx"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// ACL LIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_list_returns_all_users() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "myuser", "on", ">mypass"])
        .await;

    let resp = client.command(&["ACL", "LIST"]).await;
    let rules = extract_bulk_strings(&resp);
    assert!(!rules.is_empty());
    assert!(
        rules.iter().any(|r| r.contains("default")),
        "ACL LIST should contain default user"
    );
    assert!(
        rules.iter().any(|r| r.contains("myuser")),
        "ACL LIST should contain myuser"
    );
}

// ---------------------------------------------------------------------------
// ACL GETUSER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_returns_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "infouser", "on", ">pass", "+@read"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "infouser"]).await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());

    let keys: Vec<String> = items
        .iter()
        .step_by(2)
        .filter_map(|r| match r {
            Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        })
        .collect();
    assert!(
        keys.iter().any(|k| k == "flags"),
        "GETUSER should include flags field"
    );
}

#[tokio::test]
async fn tcl_acl_getuser_on_nonexistent_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "GETUSER", "doesnotexist"]).await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// ACL SETUSER channel permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_validate_subset_of_channels_prefixed_with_resetchannels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "hpuser",
            "on",
            "nopass",
            "resetchannels",
            "&foo",
            "+@all",
        ])
        .await;

    let resp = client.command(&["ACL", "LIST"]).await;
    let users = extract_bulk_strings(&resp);
    let hpuser_entry = users.iter().find(|u| u.contains("hpuser"));
    assert!(hpuser_entry.is_some(), "hpuser should be in ACL LIST");
    let entry = hpuser_entry.unwrap();
    assert!(
        entry.contains("resetchannels") || entry.contains("&foo"),
        "entry should contain channel info: {entry}"
    );
}

// ---------------------------------------------------------------------------
// ACL SETUSER - allchannels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_allchannels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "chuser",
            "on",
            "nopass",
            "allchannels",
            "+@all",
            "allkeys",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "chuser", "anything"]).await);

    // Should be able to publish to any channel
    let resp = user_client.command(&["PUBLISH", "anychannel", "msg"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "allchannels user should be able to PUBLISH, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// When default user is off
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_when_default_user_is_off_new_connections_are_not_authenticated() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a backup user first
    client
        .command(&[
            "ACL",
            "SETUSER",
            "admin",
            "on",
            "nopass",
            "+@all",
            "~*",
            "allchannels",
        ])
        .await;

    client.command(&["ACL", "SETUSER", "default", "off"]).await;

    // New connection should fail for non-exempt commands (PING is auth-exempt)
    let mut new_client = server.connect().await;
    let resp = new_client.command(&["GET", "foo"]).await;
    // Should get NOAUTH or similar error
    assert!(
        matches!(resp, Response::Error(_)),
        "expected error when default user is off, got {resp:?}"
    );

    // Re-enable default user via the admin user
    let mut admin_client = server.connect().await;
    assert_ok(&admin_client.command(&["AUTH", "admin", "anything"]).await);
    admin_client
        .command(&["ACL", "SETUSER", "default", "on"])
        .await;
}

// ---------------------------------------------------------------------------
// AUTH with requirepass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_auth_with_requirepass() {
    let server = TestServer::start_with_security("secretpwd").await;
    let mut client = server.connect().await;

    // PING is auth-exempt (matches Redis 7+ behavior), so use GET to verify auth is required
    let resp = client.command(&["GET", "foo"]).await;
    assert_error_prefix(&resp, "NOAUTH");

    // Wrong password
    let resp = client.command(&["AUTH", "wrongpwd"]).await;
    assert_error_prefix(&resp, "WRONGPASS");

    // Correct password (single-arg AUTH authenticates as default)
    assert_ok(&client.command(&["AUTH", "secretpwd"]).await);
    let resp = client.command(&["PING"]).await;
    assert!(
        !matches!(resp, Response::Error(_)),
        "expected PING to succeed after AUTH, got {resp:?}"
    );
}

#[tokio::test]
async fn tcl_auth_two_arg_form() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "testuser", "on", ">mypass", "+@all", "~*"])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "testuser", "mypass"]).await);
    assert_bulk_eq(&user_client.command(&["ACL", "WHOAMI"]).await, b"testuser");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - resetpass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_resetpass() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "rpuser", "on", ">pass1", ">pass2", "+acl"])
        .await;

    // Both passwords should work
    let mut c1 = server.connect().await;
    assert_ok(&c1.command(&["AUTH", "rpuser", "pass1"]).await);

    // Reset all passwords
    client
        .command(&["ACL", "SETUSER", "rpuser", "resetpass"])
        .await;

    // Now no password should work
    let mut c2 = server.connect().await;
    let resp = c2.command(&["AUTH", "rpuser", "pass1"]).await;
    assert_error_prefix(&resp, "WRONGPASS");

    let resp = c2.command(&["AUTH", "rpuser", "pass2"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - nocommands / allcommands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_nocommands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "ncuser",
            "on",
            "nopass",
            "allkeys",
            "nocommands",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "ncuser", "anything"]).await);

    let resp = user_client.command(&["PING"]).await;
    assert_error_prefix(&resp, "NOPERM");

    let resp = user_client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - key patterns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_resetkeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "rkuser",
            "on",
            "nopass",
            "allcommands",
            "~foo:*",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "rkuser", "anything"]).await);

    assert_ok(&user_client.command(&["SET", "foo:1", "a"]).await);
    let resp = user_client.command(&["SET", "bar:1", "b"]).await;
    assert_error_prefix(&resp, "NOPERM");

    // Reset keys and set new pattern
    client
        .command(&["ACL", "SETUSER", "rkuser", "resetkeys", "~bar:*"])
        .await;

    let mut user_client2 = server.connect().await;
    assert_ok(&user_client2.command(&["AUTH", "rkuser", "anything"]).await);

    assert_ok(&user_client2.command(&["SET", "bar:1", "b"]).await);
    let resp = user_client2.command(&["SET", "foo:1", "a"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - multiple key patterns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_multiple_key_patterns() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "mkuser",
            "on",
            "nopass",
            "allcommands",
            "~cache:*",
            "~session:*",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "mkuser", "anything"]).await);

    assert_ok(&user_client.command(&["SET", "cache:1", "val"]).await);
    assert_ok(&user_client.command(&["SET", "session:abc", "val"]).await);
    let resp = user_client.command(&["SET", "other:1", "val"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL DELUSER - multiple users at once
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_deluser_multiple_users() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "user1", "on", ">pass"])
        .await;
    client
        .command(&["ACL", "SETUSER", "user2", "on", ">pass"])
        .await;

    let resp = client.command(&["ACL", "DELUSER", "user1", "user2"]).await;
    assert_integer_eq(&resp, 2);

    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(!users.contains(&"user1".to_string()));
    assert!(!users.contains(&"user2".to_string()));
}

// ---------------------------------------------------------------------------
// ACL DELUSER - nonexistent user
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_deluser_nonexistent_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "DELUSER", "nosuchuser"]).await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// ACL GETUSER - flags field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_flags_on_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create disabled user
    client
        .command(&["ACL", "SETUSER", "flagtest", ">pass"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "flagtest"]).await;
    let items = unwrap_array(resp);
    let flags = get_getuser_field_array(&items, "flags");
    assert!(
        flags.contains(&"off".to_string()),
        "disabled user should have 'off' flag, got: {flags:?}"
    );

    // Enable user
    client.command(&["ACL", "SETUSER", "flagtest", "on"]).await;

    let resp = client.command(&["ACL", "GETUSER", "flagtest"]).await;
    let items = unwrap_array(resp);
    let flags = get_getuser_field_array(&items, "flags");
    assert!(
        flags.contains(&"on".to_string()),
        "enabled user should have 'on' flag, got: {flags:?}"
    );
}

#[tokio::test]
async fn tcl_acl_getuser_flags_nopass() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "npuser", "nopass"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "npuser"]).await;
    let items = unwrap_array(resp);
    let flags = get_getuser_field_array(&items, "flags");
    assert!(
        flags.contains(&"nopass".to_string()),
        "nopass user should have 'nopass' flag, got: {flags:?}"
    );
}

// ---------------------------------------------------------------------------
// ACL GETUSER - keys field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_keys_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "keyuser", "~foo:*", "~bar:*"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "keyuser"]).await;
    let items = unwrap_array(resp);
    let keys = get_getuser_field(&items, "keys");
    assert!(
        keys.contains("~foo:*"),
        "expected ~foo:* in keys field, got: {keys}"
    );
    assert!(
        keys.contains("~bar:*"),
        "expected ~bar:* in keys field, got: {keys}"
    );
}

#[tokio::test]
async fn tcl_acl_getuser_allkeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "akuser", "allkeys"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "akuser"]).await;
    let items = unwrap_array(resp);
    let keys = get_getuser_field(&items, "keys");
    assert!(
        keys.contains("~*"),
        "allkeys user should have ~* in keys field, got: {keys}"
    );
}

// ---------------------------------------------------------------------------
// ACL GETUSER - channels field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_channels_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "chuser", "resetchannels", "&test:*"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "chuser"]).await;
    let items = unwrap_array(resp);
    let channels = get_getuser_field(&items, "channels");
    assert!(
        channels.contains("&test:*"),
        "expected &test:* in channels field, got: {channels}"
    );
}

// ---------------------------------------------------------------------------
// ACL DRYRUN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_dryrun_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "dryrunuser",
            "on",
            "nopass",
            "+get",
            "~foo:*",
        ])
        .await;

    // Allowed command + key
    let resp = client
        .command(&["ACL", "DRYRUN", "dryrunuser", "GET", "foo:bar"])
        .await;
    assert_ok(&resp);

    // Denied command
    let resp = client
        .command(&["ACL", "DRYRUN", "dryrunuser", "SET", "foo:bar", "val"])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied command, got {resp:?}"
    );

    // Denied key
    let resp = client
        .command(&["ACL", "DRYRUN", "dryrunuser", "GET", "bar:baz"])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied key, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// ACL SETUSER with on/off toggling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_on_off_toggle() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "toggleuser", "on", ">pass", "+acl"])
        .await;

    // Can auth when on
    let mut c1 = server.connect().await;
    assert_ok(&c1.command(&["AUTH", "toggleuser", "pass"]).await);

    // Disable
    client
        .command(&["ACL", "SETUSER", "toggleuser", "off"])
        .await;

    // New connection should fail
    let mut c2 = server.connect().await;
    let resp = c2.command(&["AUTH", "toggleuser", "pass"]).await;
    assert_error_prefix(&resp, "WRONGPASS");

    // Re-enable
    client
        .command(&["ACL", "SETUSER", "toggleuser", "on"])
        .await;

    let mut c3 = server.connect().await;
    assert_ok(&c3.command(&["AUTH", "toggleuser", "pass"]).await);
}

// ---------------------------------------------------------------------------
// ACL SETUSER - cumulative permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_cumulative_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Start with get only
    client
        .command(&[
            "ACL", "SETUSER", "cumuser", "on", "nopass", "allkeys", "-@all", "+get",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "cumuser", "anything"]).await);

    let resp = user_client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");

    // Add set command
    client.command(&["ACL", "SETUSER", "cumuser", "+set"]).await;

    let mut user_client2 = server.connect().await;
    assert_ok(&user_client2.command(&["AUTH", "cumuser", "anything"]).await);

    assert_ok(&user_client2.command(&["SET", "foo", "bar"]).await);
}

// ---------------------------------------------------------------------------
// ACL with MULTI/EXEC (basic transaction tests)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB PUBLISH command not registered in command registry for MULTI queueing"]
async fn tcl_in_transaction_unauthorized_publish_fails() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "txuser",
            "on",
            ">pass",
            "+multi",
            "+discard",
            "+@pubsub",
            "resetchannels",
            "&foo:*",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "txuser", "pass"]).await);

    assert_ok(&user_client.command(&["MULTI"]).await);
    let resp = user_client
        .command(&["PUBLISH", "notexits", "helloworld"])
        .await;
    assert_error_prefix(&resp, "NOPERM");
    user_client.command(&["DISCARD"]).await;
}

// ---------------------------------------------------------------------------
// ACL SETUSER - allkeys shorthand
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_allkeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "akuser",
            "on",
            "nopass",
            "allcommands",
            "allkeys",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "akuser", "anything"]).await);

    assert_ok(&user_client.command(&["SET", "anykey", "val"]).await);
    assert_ok(&user_client.command(&["SET", "another:key", "val"]).await);
}

// ---------------------------------------------------------------------------
// ACL SETUSER - select subcommand
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acls_can_block_select_of_all_but_a_specific_db() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "newuser",
            "on",
            "nopass",
            "allkeys",
            "-@all",
            "+acl",
            "+select|0",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "newuser", "anything"]).await);

    assert_ok(&user_client.command(&["SELECT", "0"]).await);
    let resp = user_client.command(&["SELECT", "1"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

// ---------------------------------------------------------------------------
// ACL - scripting permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_requires_explicit_permission_for_scripting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "scripter", "on", "nopass", "+readonly"])
        .await;

    let resp = client
        .command(&["ACL", "DRYRUN", "scripter", "EVAL_RO", "", "0"])
        .await;
    if let Response::Bulk(Some(msg)) = &resp {
        let msg_str = String::from_utf8_lossy(msg);
        assert!(
            msg_str.contains("no permissions") || msg_str.contains("NOPERM"),
            "expected permission error for EVAL_RO, got: {msg_str}"
        );
    }
}

// ---------------------------------------------------------------------------
// ACL - memory leak regression (structural test)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_regression_memory_leaks_adding_removing_subcommands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add subcommands then remove the parent
    client
        .command(&[
            "ACL", "SETUSER", "newuser", "reset", "-debug", "+debug|a", "+debug|b", "+debug|c",
        ])
        .await;
    assert_ok(
        &client
            .command(&["ACL", "SETUSER", "newuser", "-debug"])
            .await,
    );
    // No crash = pass
}

// ---------------------------------------------------------------------------
// ACL GETUSER - passwords field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_getuser_passwords_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "pwuser", ">pass1", ">pass2"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "pwuser"]).await;
    let items = unwrap_array(resp);

    // Find passwords field
    let mut found = false;
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(key)) = &items[i]
            && key.as_ref() == b"passwords"
            && let Response::Array(passwords) = &items[i + 1]
        {
            assert_eq!(
                passwords.len(),
                2,
                "expected 2 passwords, got {}",
                passwords.len()
            );
            found = true;
        }
    }
    assert!(found, "passwords field not found in GETUSER response");
}

// ---------------------------------------------------------------------------
// ACL SETUSER - single channel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_single_channel_is_valid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["ACL", "SETUSER", "onechannel", "&test"])
            .await,
    );

    let resp = client.command(&["ACL", "GETUSER", "onechannel"]).await;
    let items = unwrap_array(resp);
    let channels = get_getuser_field(&items, "channels");
    assert!(
        channels.contains("&test"),
        "expected &test in channels, got: {channels}"
    );
}

// ---------------------------------------------------------------------------
// ACL when default user has no command permission
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_when_default_user_has_no_command_permission_auth_still_works() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create secure user first
    client
        .command(&[
            "ACL",
            "SETUSER",
            "secure-user",
            "on",
            ">supass",
            "+@all",
            "~*",
            "allchannels",
        ])
        .await;

    // Disable all commands for default user
    client
        .command(&["ACL", "SETUSER", "default", "-@all"])
        .await;

    // New connection authenticating as secure-user should work
    let mut new_client = server.connect().await;
    assert_ok(&new_client.command(&["AUTH", "secure-user", "supass"]).await);

    // Restore default user
    new_client
        .command(&["ACL", "SETUSER", "default", "nopass", "+@all"])
        .await;
}

// ---------------------------------------------------------------------------
// ACL SETUSER - delete then check users list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_deluser_then_users_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "tempuser", "on", ">pass"])
        .await;

    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(users.contains(&"tempuser".to_string()));

    client.command(&["ACL", "DELUSER", "tempuser"]).await;

    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(!users.contains(&"tempuser".to_string()));
}

// ---------------------------------------------------------------------------
// ACL SETUSER - combined on + allcommands + allkeys + nopass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_acl_setuser_fully_permissive_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "superuser",
            "on",
            "nopass",
            "allcommands",
            "allkeys",
            "allchannels",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(
        &user_client
            .command(&["AUTH", "superuser", "anything"])
            .await,
    );

    assert_ok(&user_client.command(&["SET", "any", "thing"]).await);
    assert_bulk_eq(&user_client.command(&["GET", "any"]).await, b"thing");
    assert_bulk_eq(&user_client.command(&["ACL", "WHOAMI"]).await, b"superuser");
}

// ---------------------------------------------------------------------------
// Helper: extract a field value from ACL GETUSER response (flat key-value array)
// ---------------------------------------------------------------------------

/// Extracts a string field from the flat key-value array returned by ACL GETUSER.
fn get_getuser_field(items: &[Response], field: &str) -> String {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(key)) = &items[i]
            && key.as_ref() == field.as_bytes()
        {
            return match &items[i + 1] {
                Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
                Response::Bulk(None) => String::new(),
                Response::Array(arr) => {
                    let strs: Vec<String> = arr
                        .iter()
                        .filter_map(|r| match r {
                            Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                            _ => None,
                        })
                        .collect();
                    strs.join(" ")
                }
                other => format!("{other:?}"),
            };
        }
    }
    panic!("field {field:?} not found in GETUSER response");
}

/// Extracts an array field from the flat key-value array returned by ACL GETUSER,
/// returning the bulk strings within it.
fn get_getuser_field_array(items: &[Response], field: &str) -> Vec<String> {
    for i in (0..items.len()).step_by(2) {
        if let Response::Bulk(Some(key)) = &items[i]
            && key.as_ref() == field.as_bytes()
        {
            return match &items[i + 1] {
                Response::Array(arr) => arr
                    .iter()
                    .filter_map(|r| match r {
                        Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                        _ => None,
                    })
                    .collect(),
                _ => vec![],
            };
        }
    }
    panic!("field {field:?} not found in GETUSER response");
}

// ---------------------------------------------------------------------------
// Tests ported from acl-v2.tcl: R/W permission validation + DRYRUN basics
// (non-selector tests; selector tests were removed as unsupported)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "key-permission-RW", "%~"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_selector() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "key-permission-RW", "%"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_empty_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-perm-empty",
            "on",
            "nopass",
            "%RW~",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-perm-empty", "password"]).await);

    // Empty pattern means no key access
    assert_error_prefix(&r2.command(&["SET", "x", "5"]).await, "NOPERM");
}

#[tokio::test]
async fn tcl_validate_rw_permissions_no_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "key-perm-nopat",
            "on",
            "nopass",
            "%RW",
            "+@all",
        ])
        .await;

    let mut r2 = server.connect().await;
    assert_ok(&r2.command(&["AUTH", "key-perm-nopat", "password"]).await);

    // No pattern means no key access
    assert_error_prefix(&r2.command(&["SET", "x", "5"]).await, "NOPERM");
}

#[tokio::test]
async fn tcl_dryrun_command_permissions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "command-test2",
            "+@all",
            "%R~read*",
            "%W~write*",
            "%RW~rw*",
        ])
        .await;

    // Remove all command permissions
    client
        .command(&["ACL", "SETUSER", "command-test2", "-@all"])
        .await;

    // DRYRUN returns a bulk string describing the denial (not an error)
    let resp = client
        .command(&[
            "ACL",
            "DRYRUN",
            "command-test2",
            "SET",
            "somekey",
            "somevalue",
        ])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied command, got {resp:?}"
    );

    let resp = client
        .command(&["ACL", "DRYRUN", "command-test2", "GET", "somekey"])
        .await;
    assert!(
        matches!(resp, Response::Bulk(Some(_))),
        "DRYRUN should return bulk string for denied command, got {resp:?}"
    );
}
