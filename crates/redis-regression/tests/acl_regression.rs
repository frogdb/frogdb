use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn acl_whoami_returns_default_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "WHOAMI"]).await;
    assert_bulk_eq(&resp, b"default");
}

#[tokio::test]
async fn acl_setuser_creates_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["ACL", "SETUSER", "limited", "on", ">pass"])
            .await,
    );

    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(users.contains(&"limited".to_string()));
}

#[tokio::test]
async fn acl_user_command_restriction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create user that can only GET, not SET (with full key access)
    client
        .command(&[
            "ACL", "SETUSER", "readonly", "on", ">pass", "~*", "-@all", "+get",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "readonly", "pass"]).await);

    // GET should work (empty key returns nil, not error)
    let resp = user_client.command(&["GET", "foo"]).await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "GET should not error"
    );

    // SET should be denied
    let resp = user_client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn acl_user_key_pattern_restriction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "ACL",
            "SETUSER",
            "limited",
            "on",
            ">pass",
            "~prefix:*",
            "+@all",
        ])
        .await;

    let mut user_client = server.connect().await;
    assert_ok(&user_client.command(&["AUTH", "limited", "pass"]).await);

    // Allowed key
    let resp = user_client.command(&["SET", "prefix:foo", "bar"]).await;
    assert_ok(&resp);

    // Denied key
    let resp = user_client.command(&["SET", "other:foo", "bar"]).await;
    assert_error_prefix(&resp, "NOPERM");
}

#[tokio::test]
async fn acl_list_returns_correct_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "myuser", "on", ">mypass"])
        .await;

    let resp = client.command(&["ACL", "LIST"]).await;
    let rules = extract_bulk_strings(&resp);
    assert!(!rules.is_empty());
    assert!(rules.iter().any(|r| r.contains("default")));
    assert!(rules.iter().any(|r| r.contains("myuser")));
}

#[tokio::test]
async fn acl_getuser_returns_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "infouser", "on", ">pass", "+@read"])
        .await;

    let resp = client.command(&["ACL", "GETUSER", "infouser"]).await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());

    // Keys come in pairs; find "flags" key
    let keys: Vec<String> = items
        .iter()
        .step_by(2)
        .filter_map(|r| match r {
            frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        })
        .collect();
    assert!(keys.iter().any(|k| k == "flags"));
}

#[tokio::test]
async fn acl_deluser_removes_user() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ACL", "SETUSER", "tempuser", "on", ">pass"])
        .await;
    let resp = client.command(&["ACL", "DELUSER", "tempuser"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    let resp = client.command(&["ACL", "USERS"]).await;
    let users = extract_bulk_strings(&resp);
    assert!(!users.contains(&"tempuser".to_string()));
}

#[tokio::test]
async fn acl_getuser_on_nonexistent_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["ACL", "GETUSER", "doesnotexist"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}
