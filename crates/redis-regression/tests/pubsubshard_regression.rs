use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

#[tokio::test]
async fn ssubscribe_spublish_basic_message_delivery() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe
    let sub_resp = subscriber.command(&["SSUBSCRIBE", "chan1"]).await;
    let items = unwrap_array(sub_resp);
    assert_bulk_eq(&items[0], b"ssubscribe");
    assert_bulk_eq(&items[1], b"chan1");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Publish
    let pub_resp = publisher.command(&["SPUBLISH", "chan1", "hello"]).await;
    assert_eq!(unwrap_integer(&pub_resp), 1);

    // Receive message
    let msg = subscriber
        .read_message(Duration::from_secs(5))
        .await
        .expect("should receive message");
    let parts = unwrap_array(msg);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");
}

#[tokio::test]
async fn sunsubscribe_from_specific_channel() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    subscriber.command(&["SSUBSCRIBE", "chan1", "chan2"]).await;
    // Consume the subscribe confirmations
    subscriber.read_message(Duration::from_secs(2)).await;

    // Unsubscribe from chan1
    subscriber.send_only(&["SUNSUBSCRIBE", "chan1"]).await;
    let unsub_resp = subscriber
        .read_message(Duration::from_secs(5))
        .await
        .expect("unsubscribe response");
    let parts = unwrap_array(unsub_resp);
    assert_bulk_eq(&parts[0], b"sunsubscribe");
    assert_bulk_eq(&parts[1], b"chan1");

    // Publishing to chan1 should now have 0 recipients
    let resp = publisher.command(&["SPUBLISH", "chan1", "msg"]).await;
    assert_eq!(unwrap_integer(&resp), 0);

    // Publishing to chan2 should still reach the subscriber
    let resp = publisher.command(&["SPUBLISH", "chan2", "msg"]).await;
    assert_eq!(unwrap_integer(&resp), 1);
}

#[tokio::test]
async fn pubsub_shardnumsub_returns_subscriber_counts() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    subscriber.command(&["SSUBSCRIBE", "mychan"]).await;

    let resp = client.command(&["PUBSUB", "SHARDNUMSUB", "mychan", "otherchan"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4); // [chan, count, chan, count]
    assert_bulk_eq(&items[0], b"mychan");
    assert_eq!(unwrap_integer(&items[1]), 1);
    assert_bulk_eq(&items[2], b"otherchan");
    assert_eq!(unwrap_integer(&items[3]), 0);
}

#[tokio::test]
async fn pubsub_shardchannels_lists_active_channels() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    subscriber.command(&["SSUBSCRIBE", "active1", "active2"]).await;

    let resp = client.command(&["PUBSUB", "SHARDCHANNELS"]).await;
    let channels = extract_bulk_strings(&resp);
    assert!(channels.contains(&"active1".to_string()));
    assert!(channels.contains(&"active2".to_string()));
}

#[tokio::test]
async fn multiple_subscribers_receive_same_sharded_message() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut publisher = server.connect().await;

    sub1.command(&["SSUBSCRIBE", "shared"]).await;
    sub2.command(&["SSUBSCRIBE", "shared"]).await;

    let pub_resp = publisher.command(&["SPUBLISH", "shared", "broadcast"]).await;
    assert_eq!(unwrap_integer(&pub_resp), 2);

    // Both subscribers receive the message
    let msg1 = sub1.read_message(Duration::from_secs(5)).await.expect("sub1 msg");
    let msg2 = sub2.read_message(Duration::from_secs(5)).await.expect("sub2 msg");

    let p1 = unwrap_array(msg1);
    let p2 = unwrap_array(msg2);
    assert_bulk_eq(&p1[2], b"broadcast");
    assert_bulk_eq(&p2[2], b"broadcast");
}
