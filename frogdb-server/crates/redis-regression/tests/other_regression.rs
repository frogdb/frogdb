use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn command_help_returns_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // COMMAND HELP should return an array of help strings
    let resp = client.command(&["COMMAND", "HELP"]).await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty(), "COMMAND HELP should return help text");
}

#[tokio::test]
async fn leading_zeros_preserved_in_string_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "007"]).await);
    let resp = client.command(&["GET", "mykey"]).await;
    assert_bulk_eq(&resp, b"007");

    assert_ok(&client.command(&["SET", "mykey", "00100"]).await);
    let resp = client.command(&["GET", "mykey"]).await;
    assert_bulk_eq(&resp, b"00100");
}
