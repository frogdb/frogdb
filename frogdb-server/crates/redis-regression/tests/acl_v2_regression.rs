use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn acl_selector_syntax_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Selector syntax should be rejected
    let resp = client
        .command(&[
            "ACL",
            "SETUSER",
            "sel1",
            "on",
            "nopass",
            "(+@write ~write::*)",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn acl_clearselectors_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["ACL", "SETUSER", "sel-del", "clearselectors"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn acl_read_key_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // %R~ = read-only key pattern
    client
        .command(&[
            "ACL",
            "SETUSER",
            "reader",
            "on",
            "nopass",
            "%R~read:*",
            "+@all",
        ])
        .await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "reader", "password"]).await);

    // Pre-populate via admin
    client.command(&["SET", "read:foo", "bar"]).await;

    let resp = user.command(&["GET", "read:foo"]).await;
    assert_bulk_eq(&resp, b"bar");

    // Write to read-only key denied
    let resp = user.command(&["SET", "read:foo", "new"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn acl_write_key_permission() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // %W~ = write-only key pattern
    client
        .command(&[
            "ACL",
            "SETUSER",
            "writer",
            "on",
            "nopass",
            "%W~write:*",
            "+@all",
        ])
        .await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "writer", "password"]).await);

    // Write to write:: key is allowed
    let resp = user.command(&["SET", "write:foo", "bar"]).await;
    assert_ok(&resp);

    // Read from write-only key denied
    let resp = user.command(&["GET", "write:foo"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

/// Regression: a STORE-family command must be checked with *per-key* access —
/// the destination needs write, the sources need only read. A `%R~src:* %W~dst:*`
/// user was previously denied because SINTERSTORE (a WRITE command) had its
/// command-level `ReadWrite` access applied uniformly to every key, so the
/// source keys demanded write the user did not have. This is the fix's headline
/// case.
#[tokio::test]
async fn acl_store_command_split_access_allowed() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    admin
        .command(&[
            "ACL", "SETUSER", "splitter", "on", "nopass", "%R~src:*", "%W~dst:*", "+@all",
        ])
        .await;

    // Admin seeds the source sets (the splitter has only read on src:*).
    // Shared `{g}` hash tag keeps all keys in one slot (SINTERSTORE is
    // single-slot); the `src:`/`dst:` prefixes still drive the ACL match.
    admin.command(&["SADD", "src:{g}:a", "x", "y", "z"]).await;
    admin.command(&["SADD", "src:{g}:b", "y", "z", "w"]).await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "splitter", "password"]).await);

    // SINTERSTORE writes dst, reads src:a + src:b — allowed by split access.
    let resp = user
        .command(&["SINTERSTORE", "dst:{g}:out", "src:{g}:a", "src:{g}:b"])
        .await;
    assert_integer_eq(&resp, 2); // {y, z}
}

/// Inverse of the split-access case: denials still fire per key.
/// - a source outside the read grant is denied (write on dst does not imply read
///   on an ungranted src);
/// - a destination the user can only read is denied the write the command needs.
#[tokio::test]
async fn acl_store_command_denied_per_key() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    admin
        .command(&[
            "ACL",
            "SETUSER",
            "splitter2",
            "on",
            "nopass",
            "%R~src:*",
            "%W~dst:*",
            "+@all",
        ])
        .await;
    admin.command(&["SADD", "src:a", "x"]).await;
    admin.command(&["SADD", "other:a", "x"]).await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "splitter2", "password"]).await);

    // Source `other:a` is not covered by %R~src:* → read denied.
    let resp = user
        .command(&["SINTERSTORE", "dst:out", "src:a", "other:a"])
        .await;
    assert_error_prefix(&resp, "NOPERM");

    // Destination `src:z` only has read grant, but SINTERSTORE needs write on it.
    let resp = user.command(&["SINTERSTORE", "src:z", "src:a"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

/// Second store-family shape: ZUNIONSTORE (DEST NUMKEYS src…). Same per-key
/// split-access semantics as the set STORE commands.
#[tokio::test]
async fn acl_zunionstore_split_access_allowed() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    admin
        .command(&[
            "ACL", "SETUSER", "zsplit", "on", "nopass", "%R~src:*", "%W~dst:*", "+@all",
        ])
        .await;
    admin
        .command(&["ZADD", "src:{g}:z1", "1", "a", "2", "b"])
        .await;
    admin
        .command(&["ZADD", "src:{g}:z2", "3", "b", "4", "c"])
        .await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "zsplit", "password"]).await);

    let resp = user
        .command(&["ZUNIONSTORE", "dst:{g}:z", "2", "src:{g}:z1", "src:{g}:z2"])
        .await;
    assert_integer_eq(&resp, 3); // {a, b, c}
}

/// DRYRUN must agree with live enforcement: the split-access case returns OK,
/// and the denied case returns the denial message — both derived from the same
/// per-key helper the live guard uses.
#[tokio::test]
async fn acl_dryrun_agrees_with_per_key_enforcement() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    admin
        .command(&[
            "ACL",
            "SETUSER",
            "dryrunner",
            "on",
            "nopass",
            "%R~src:*",
            "%W~dst:*",
            "+@all",
        ])
        .await;

    // Allowed case → OK.
    let resp = admin
        .command(&[
            "ACL",
            "DRYRUN",
            "dryrunner",
            "SINTERSTORE",
            "dst:out",
            "src:a",
            "src:b",
        ])
        .await;
    assert_ok(&resp);

    // Denied case (source outside read grant) → bulk denial string, not OK.
    let resp = admin
        .command(&[
            "ACL",
            "DRYRUN",
            "dryrunner",
            "SINTERSTORE",
            "dst:out",
            "other:a",
        ])
        .await;
    let msg = unwrap_bulk(&resp);
    assert!(
        msg.starts_with(b"This user has no permissions"),
        "DRYRUN should report the per-key denial, got {:?}",
        String::from_utf8_lossy(msg)
    );
}

/// MULTI/EXEC queue path enforces the same per-key access as direct dispatch:
/// the split-access user can queue and execute SINTERSTORE.
#[tokio::test]
async fn acl_multi_queue_allows_store_command_split_access() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    admin
        .command(&[
            "ACL", "SETUSER", "txsplit", "on", "nopass", "%R~src:*", "%W~dst:*", "+@all",
        ])
        .await;
    admin.command(&["SADD", "src:{g}:a", "x", "y"]).await;
    admin.command(&["SADD", "src:{g}:b", "y", "z"]).await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "txsplit", "password"]).await);

    assert_ok(&user.command(&["MULTI"]).await);
    // Queue-time ACL check must not reject this (QUEUED, not NOPERM).
    let queued = user
        .command(&["SINTERSTORE", "dst:{g}:out", "src:{g}:a", "src:{g}:b"])
        .await;
    assert!(
        matches!(&queued, frogdb_protocol::Response::Simple(s) if s == "QUEUED"),
        "SINTERSTORE should queue, not be denied at queue time, got {queued:?}"
    );

    let exec = user.command(&["EXEC"]).await;
    let results = unwrap_array(exec);
    assert_eq!(results.len(), 1);
    assert_integer_eq(&results[0], 1); // {y}
}

/// Pin: plain single-key commands still enforce the obvious access — GET needs
/// read, SET needs write — so the per-key change is a no-regression there.
#[tokio::test]
async fn acl_single_key_access_unchanged() {
    let server = TestServer::start_standalone().await;
    let mut admin = server.connect().await;

    // Read-only grant on k:*.
    admin
        .command(&["ACL", "SETUSER", "ro", "on", "nopass", "%R~k:*", "+@all"])
        .await;
    admin.command(&["SET", "k:1", "v"]).await;

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "ro", "password"]).await);

    assert_bulk_eq(&user.command(&["GET", "k:1"]).await, b"v");
    assert_error_prefix(&user.command(&["SET", "k:1", "w"]).await, "NOPERM");
}

#[tokio::test]
async fn acl_getuser_returns_selectors_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "getuser-test", "on", ">pass"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "getuser-test"]).await;
    let items = unwrap_array(resp);

    let keys: Vec<String> = items
        .iter()
        .step_by(2)
        .filter_map(|r| match r {
            frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        })
        .collect();

    assert!(
        keys.iter().any(|k| k == "selectors"),
        "GETUSER should include selectors field"
    );
}
