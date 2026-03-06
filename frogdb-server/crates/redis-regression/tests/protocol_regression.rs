use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn wrong_argument_count_returns_arity_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PING with too many args
    let resp = client.command(&["PING", "x", "y", "z"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn get_with_wrong_argument_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // GET with no args
    let resp = client.command(&["GET"]).await;
    assert_error_prefix(&resp, "ERR");

    // GET with too many args
    let resp = client.command(&["GET", "k1", "k2"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn set_with_wrong_argument_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET with no key/value
    let resp = client.command(&["SET"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn large_argument_count_mset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Build MSET with 500 key-value pairs (1000 args + command)
    let mut args = vec!["MSET"];
    let pairs: Vec<String> = (0..500)
        .flat_map(|i| [format!("{{k}}key{i}"), format!("val{i}")])
        .collect();
    for p in &pairs {
        args.push(p.as_str());
    }
    let resp = client.command(&args).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn unknown_command_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["NOTACOMMAND"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn ping_works_at_any_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PING without arg
    let resp = client.command(&["PING"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Simple(_)));

    // PING with message
    let resp = client.command(&["PING", "hello"]).await;
    assert_bulk_eq(&resp, b"hello");
}
