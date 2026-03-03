use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn client_setname_and_getname() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set name
    assert_ok(&client.command(&["CLIENT", "SETNAME", "myconn"]).await);

    // Get name
    let resp = client.command(&["CLIENT", "GETNAME"]).await;
    assert_bulk_eq(&resp, b"myconn");
}

#[tokio::test]
async fn client_getname_before_setname_returns_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "GETNAME"]).await;
    // Unnamed client returns nil or empty bulk string
    let is_empty = match &resp {
        frogdb_protocol::Response::Bulk(None) => true,
        frogdb_protocol::Response::Bulk(Some(b)) => b.is_empty(),
        _ => false,
    };
    assert!(is_empty, "unnamed client GETNAME should return nil or empty");
}

#[tokio::test]
async fn client_list_returns_expected_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "LIST"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // CLIENT LIST returns lines with key=value pairs
    assert!(text.contains("id=") || text.contains("addr="),
        "CLIENT LIST should contain client info: {text}");
}

#[tokio::test]
async fn client_id_returns_non_negative_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "ID"]).await;
    let id = unwrap_integer(&resp);
    assert!(id >= 0, "CLIENT ID should return a non-negative integer, got {id}");
}

#[tokio::test]
async fn client_setname_validates_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Names with spaces are invalid
    let resp = client.command(&["CLIENT", "SETNAME", "invalid name"]).await;
    assert_error_prefix(&resp, "ERR");
}
