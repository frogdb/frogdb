use bytes::Bytes;
use frogdb_test_harness::server::TestServer;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::Duration;

#[tokio::test]
async fn ping_in_resp3_subscribed_mode_returns_pong() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    // Switch to RESP3
    let _hello = client.command(&["HELLO", "3"]).await;

    // Subscribe to enter pubsub mode
    let _sub = client.command(&["SUBSCRIBE", "ch1"]).await;

    // PING in subscribed RESP3 mode should return simple string "PONG"
    let resp = client.command(&["PING"]).await;
    match resp {
        Resp3Frame::SimpleString { data, .. } => {
            assert_eq!(data, Bytes::from_static(b"PONG"));
        }
        other => panic!("expected SimpleString PONG, got {other:?}"),
    }
}

#[tokio::test]
async fn ping_with_message_in_resp3_subscribed_mode() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    let _hello = client.command(&["HELLO", "3"]).await;
    let _sub = client.command(&["SUBSCRIBE", "ch1"]).await;

    let resp = client.command(&["PING", "hello"]).await;
    match resp {
        Resp3Frame::BlobString { data, .. } => {
            assert_eq!(data, Bytes::from_static(b"hello"));
        }
        other => panic!("expected BlobString 'hello', got {other:?}"),
    }
}

#[tokio::test]
async fn ping_in_resp2_subscribed_mode_returns_array() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Subscribe to enter pubsub mode
    let _sub = client.command(&["SUBSCRIBE", "ch1"]).await;

    // Send PING and read response
    client.send_only(&["PING"]).await;
    let resp = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("should get pong response");

    // RESP2 pubsub PING returns ["pong", ""]
    let items = frogdb_test_harness::response::unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(
        frogdb_test_harness::response::unwrap_bulk(&items[0]),
        b"pong"
    );
    assert_eq!(frogdb_test_harness::response::unwrap_bulk(&items[1]), b"");
}
