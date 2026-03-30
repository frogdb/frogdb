//! Rust port of Redis 8.6.0 `unit/pubsubshard.tcl` test suite.
//!
//! Excludes:
//! - Replication tests (`needs:repl`) — publish to master, receive on replica
//! - "PubSubShard with CLIENT REPLY OFF" — requires RESP3 `HELLO 3` + `CLIENT REPLY OFF`

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// SPUBLISH / SSUBSCRIBE basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spublish_ssubscribe_basics() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to two channels
    let resp = rd1.command(&["SSUBSCRIBE", "chan1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[2]), 1);

    rd1.send_only(&["SSUBSCRIBE", "chan2"]).await;
    let resp = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("ssubscribe chan2 response");
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[2]), 2);

    // Publish to both channels
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan1", "hello"]).await, 1);
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan2", "world"]).await, 1);

    // Read messages
    let msg1 = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("smessage chan1");
    let parts = unwrap_array(msg1);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");

    let msg2 = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("smessage chan2");
    let parts = unwrap_array(msg2);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan2");
    assert_bulk_eq(&parts[2], b"world");

    // Unsubscribe from chan1
    rd1.send_only(&["SUNSUBSCRIBE", "chan1"]).await;
    let resp = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("sunsubscribe chan1 response");
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"sunsubscribe");
    assert_bulk_eq(&items[1], b"chan1");

    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan2", "world"]).await, 1);

    let msg = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("smessage chan2 after unsub");
    let parts = unwrap_array(msg);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan2");
    assert_bulk_eq(&parts[2], b"world");

    // Unsubscribe from chan2
    rd1.send_only(&["SUNSUBSCRIBE", "chan2"]).await;
    let resp = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("sunsubscribe chan2 response");
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"sunsubscribe");
    assert_bulk_eq(&items[1], b"chan2");

    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan2", "world"]).await, 0);
}

// ---------------------------------------------------------------------------
// SPUBLISH / SSUBSCRIBE with two clients
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spublish_ssubscribe_with_two_clients() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut publisher = server.connect().await;

    rd1.command(&["SSUBSCRIBE", "chan1"]).await;
    rd2.command(&["SSUBSCRIBE", "chan1"]).await;

    assert_integer_eq(
        &publisher.command(&["SPUBLISH", "chan1", "hello"]).await,
        2,
    );

    let msg1 = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("rd1 smessage");
    let parts = unwrap_array(msg1);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");

    let msg2 = rd2
        .read_message(Duration::from_secs(5))
        .await
        .expect("rd2 smessage");
    let parts = unwrap_array(msg2);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");
}

// ---------------------------------------------------------------------------
// SPUBLISH / SSUBSCRIBE after SUNSUBSCRIBE without arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spublish_ssubscribe_after_sunsubscribe_without_args() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to three channels
    let resp = rd1.command(&["SSUBSCRIBE", "chan1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[2]), 1);

    rd1.send_only(&["SSUBSCRIBE", "chan2"]).await;
    rd1.read_message(Duration::from_secs(5)).await;

    rd1.send_only(&["SSUBSCRIBE", "chan3"]).await;
    rd1.read_message(Duration::from_secs(5)).await;

    // Unsubscribe from all (no arguments)
    rd1.send_only(&["SUNSUBSCRIBE"]).await;
    // Drain all three sunsubscribe confirmations
    for _ in 0..3 {
        rd1.read_message(Duration::from_secs(5)).await;
    }

    // No subscribers left
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan2", "hello"]).await, 0);
    assert_integer_eq(&publisher.command(&["SPUBLISH", "chan3", "hello"]).await, 0);
}

// ---------------------------------------------------------------------------
// SSUBSCRIBE to one channel more than once
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_ssubscribe_to_one_channel_more_than_once() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to chan1 three times — count should remain 1
    let resp = rd1.command(&["SSUBSCRIBE", "chan1", "chan1", "chan1"]).await;
    let items = unwrap_array(resp);
    // First ssubscribe response: count = 1
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Consume the additional ssubscribe confirmations (still count 1)
    rd1.read_message(Duration::from_secs(2)).await;
    rd1.read_message(Duration::from_secs(2)).await;

    // Only one message should be received
    assert_integer_eq(
        &publisher.command(&["SPUBLISH", "chan1", "hello"]).await,
        1,
    );

    let msg = rd1
        .read_message(Duration::from_secs(5))
        .await
        .expect("smessage chan1");
    let parts = unwrap_array(msg);
    assert_bulk_eq(&parts[0], b"smessage");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");
}

// ---------------------------------------------------------------------------
// SUNSUBSCRIBE from non-subscribed channels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sunsubscribe_from_non_subscribed_channels() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;

    // Unsubscribing from channels we never subscribed to should return count 0
    let resp = rd1.command(&["SUNSUBSCRIBE", "foo"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"sunsubscribe");
    assert_bulk_eq(&items[1], b"foo");
    assert_eq!(unwrap_integer(&items[2]), 0);

    let resp = rd1.command(&["SUNSUBSCRIBE", "bar"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"sunsubscribe");
    assert_bulk_eq(&items[1], b"bar");
    assert_eq!(unwrap_integer(&items[2]), 0);

    let resp = rd1.command(&["SUNSUBSCRIBE", "quux"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"sunsubscribe");
    assert_bulk_eq(&items[1], b"quux");
    assert_eq!(unwrap_integer(&items[2]), 0);
}

// ---------------------------------------------------------------------------
// PUBSUB SHARDNUMSUB basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pubsub_shardnumsub_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["PUBSUB", "SHARDNUMSUB", "abc", "def"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4);
    assert_bulk_eq(&items[0], b"abc");
    assert_eq!(unwrap_integer(&items[1]), 0);
    assert_bulk_eq(&items[2], b"def");
    assert_eq!(unwrap_integer(&items[3]), 0);
}

// ---------------------------------------------------------------------------
// SPUBLISH / SSUBSCRIBE with two clients + PUBSUB introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spublish_ssubscribe_two_clients_pubsub_introspection() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut client = server.connect().await;

    rd1.command(&["SSUBSCRIBE", "chan1"]).await;
    rd2.command(&["SSUBSCRIBE", "chan1"]).await;

    assert_integer_eq(&client.command(&["SPUBLISH", "chan1", "hello"]).await, 2);

    // PUBSUB SHARDNUMSUB should show 2 subscribers for chan1
    let resp = client
        .command(&["PUBSUB", "SHARDNUMSUB", "chan1"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"chan1");
    assert_eq!(unwrap_integer(&items[1]), 2);

    // PUBSUB SHARDCHANNELS should list chan1
    let resp = client.command(&["PUBSUB", "SHARDCHANNELS"]).await;
    let channels = extract_bulk_strings(&resp);
    assert!(
        channels.contains(&"chan1".to_string()),
        "expected chan1 in shardchannels, got {channels:?}",
    );
}

// ---------------------------------------------------------------------------
// SPUBLISH / SSUBSCRIBE mixed with PUBLISH / SUBSCRIBE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spublish_ssubscribe_with_publish_subscribe() {
    let server = TestServer::start_standalone().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;
    let mut client = server.connect().await;

    // rd1 uses shard pubsub, rd2 uses regular pubsub — both on "chan1"
    rd1.command(&["SSUBSCRIBE", "chan1"]).await;
    rd2.command(&["SUBSCRIBE", "chan1"]).await;

    // SPUBLISH only reaches shard subscribers
    assert_integer_eq(&client.command(&["SPUBLISH", "chan1", "hello"]).await, 1);
    // PUBLISH only reaches regular subscribers
    assert_integer_eq(&client.command(&["PUBLISH", "chan1", "hello"]).await, 1);

    // PUBSUB SHARDNUMSUB — only shard subscriptions
    let resp = client
        .command(&["PUBSUB", "SHARDNUMSUB", "chan1"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"chan1");
    assert_eq!(unwrap_integer(&items[1]), 1);

    // PUBSUB NUMSUB — only regular subscriptions
    let resp = client.command(&["PUBSUB", "NUMSUB", "chan1"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"chan1");
    assert_eq!(unwrap_integer(&items[1]), 1);

    // PUBSUB SHARDCHANNELS should list chan1
    let resp = client.command(&["PUBSUB", "SHARDCHANNELS"]).await;
    let channels = extract_bulk_strings(&resp);
    assert!(
        channels.contains(&"chan1".to_string()),
        "expected chan1 in shardchannels, got {channels:?}",
    );

    // PUBSUB CHANNELS should list chan1
    let resp = client.command(&["PUBSUB", "CHANNELS"]).await;
    let channels = extract_bulk_strings(&resp);
    assert!(
        channels.contains(&"chan1".to_string()),
        "expected chan1 in channels, got {channels:?}",
    );
}
