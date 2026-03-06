use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn copy_with_invalid_destination_db_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "{k}src", "hello"]).await);

    // Invalid DB argument (not a number)
    let resp = client
        .command(&["COPY", "{k}src", "{k}dst", "DB", "notanumber"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn keys_with_backtracking_glob_matches_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create keys that test glob backtracking
    assert_ok(&client.command(&["SET", "hellofoobarbaz", "1"]).await);
    assert_ok(&client.command(&["SET", "foobarbaz", "1"]).await);
    assert_ok(&client.command(&["SET", "foobar", "1"]).await);
    assert_ok(&client.command(&["SET", "nope", "1"]).await);

    // Pattern with multiple wildcards requiring backtracking
    let resp = client.command(&["KEYS", "*foo*bar*"]).await;
    let mut keys = extract_bulk_strings(&resp);
    keys.sort();
    assert_eq!(keys, vec!["foobar", "foobarbaz", "hellofoobarbaz"]);
}
