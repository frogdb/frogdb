use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

#[tokio::test]
async fn quit_returns_ok() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["QUIT"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn connection_closes_after_quit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["QUIT"]).await);

    // After QUIT, reading another response should time out or return None
    let resp = client.read_response(Duration::from_millis(200)).await;
    assert!(resp.is_none(), "connection should be closed after QUIT");
}
