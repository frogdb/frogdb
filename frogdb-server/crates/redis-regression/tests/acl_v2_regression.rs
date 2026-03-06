use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn acl_selectors_basic_multiple() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create user with no permissions initially
    client
        .command(&[
            "ACL",
            "SETUSER",
            "sel1",
            "on",
            "-@all",
            "resetkeys",
            "nopass",
        ])
        .await;

    // Add selectors: write access to write::* and read access to read::*
    assert_ok(
        &client
            .command(&[
                "ACL",
                "SETUSER",
                "sel1",
                "(+@write ~write::*)",
                "(+@read ~read::*)",
            ])
            .await,
    );

    let mut user = server.connect().await;
    assert_ok(&user.command(&["AUTH", "sel1", "password"]).await);

    // Write key in write:: namespace is allowed
    let resp = user.command(&["SET", "write::foo", "bar"]).await;
    assert_ok(&resp);

    // Read from read:: namespace (key doesn't exist → nil, not error)
    let resp = user.command(&["GET", "read::foo"]).await;
    assert!(!matches!(resp, frogdb_protocol::Response::Error(_)));

    // Cross-namespace access denied
    let resp = user.command(&["GET", "write::foo"]).await;
    assert_error_prefix(&resp, "NOPERM");

    let resp = user.command(&["SET", "read::foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn acl_clearselectors_removes_secondary_selectors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add a selector
    client
        .command(&["ACL", "SETUSER", "sel-del", "on", "(~added-selector)"])
        .await;

    // Clear selectors
    assert_ok(
        &client
            .command(&["ACL", "SETUSER", "sel-del", "clearselectors"])
            .await,
    );

    let resp = client.command(&["ACL", "GETUSER", "sel-del"]).await;
    let items = unwrap_array(resp);
    // Find selectors field
    // GETUSER returns flat key-value pairs; find the "selectors" key
    let selectors_pos = items.iter().step_by(2).position(
        |r| matches!(r, frogdb_protocol::Response::Bulk(Some(k)) if k.as_ref() == b"selectors"),
    );
    assert!(
        selectors_pos.is_some(),
        "selectors field not found in GETUSER response"
    );
    let idx = selectors_pos.unwrap() * 2 + 1;
    let sels = unwrap_array(items[idx].clone());
    assert!(
        sels.is_empty(),
        "selectors should be empty after clearselectors"
    );
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
