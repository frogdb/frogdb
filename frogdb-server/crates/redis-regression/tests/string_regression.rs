use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn set_with_get_returns_previous_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "oldval"]).await);
    let resp = client.command(&["SET", "mykey", "newval", "GET"]).await;
    assert_bulk_eq(&resp, b"oldval");
}

#[tokio::test]
async fn set_with_get_on_nonexistent_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SET", "mykey", "val", "GET"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn set_with_get_on_wrong_type_returns_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a list key
    client.command(&["RPUSH", "{k}mykey", "a"]).await;
    let resp = client.command(&["SET", "{k}mykey", "val", "GET"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn substr_alias_behaves_like_getrange() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello world"]).await);
    let substr_resp = client.command(&["SUBSTR", "mykey", "0", "4"]).await;
    let getrange_resp = client.command(&["GETRANGE", "mykey", "0", "4"]).await;
    assert_bulk_eq(&substr_resp, b"hello");
    assert_bulk_eq(&getrange_resp, b"hello");
}

#[tokio::test]
async fn strlen_on_integer_encoded_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "12345"]).await);
    let resp = client.command(&["STRLEN", "mykey"]).await;
    assert_eq!(unwrap_integer(&resp), 5);

    // Negative integer
    assert_ok(&client.command(&["SET", "mykey", "-42"]).await);
    let resp = client.command(&["STRLEN", "mykey"]).await;
    assert_eq!(unwrap_integer(&resp), 3);
}
