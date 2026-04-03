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
