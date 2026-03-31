//! Rust port of Redis 8.6.0 `unit/pubsub.tcl` test suite.
//!
//! Excludes:
//! - All keyspace notification tests (require `CONFIG SET notify-keyspace-events`)
//! - `needs:debug` tests (hash events with `assert_encoding`, `debug set-active-expire`)
//! - `needs:config-maxmemory` tests (evicted events)
//! - `resp3`-only tests (publish-to-self inside MULTI/script, `CLIENT REPLY OFF`)
//! - Tests that use `CONFIG SET`

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// PING in subscribed mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pubsub_ping_resp2() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;

    // Subscribe to enter pubsub mode
    let resp = sub.command(&["SUBSCRIBE", "somechannel"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"subscribe");
    assert_bulk_eq(&items[1], b"somechannel");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // PING without argument in RESP2 subscribed mode returns ["pong", ""]
    sub.send_only(&["PING"]).await;
    let resp = sub
        .read_response(Duration::from_secs(5))
        .await
        .expect("should get pong response");
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"pong");
    assert_bulk_eq(&items[1], b"");

    // PING with argument in RESP2 subscribed mode returns ["pong", "foo"]
    sub.send_only(&["PING", "foo"]).await;
    let resp = sub
        .read_response(Duration::from_secs(5))
        .await
        .expect("should get pong foo response");
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[0], b"pong");
    assert_bulk_eq(&items[1], b"foo");

    // Unsubscribe, then PING should return simple PONG
    sub.send_only(&["UNSUBSCRIBE", "somechannel"]).await;
    let _unsub = sub.read_response(Duration::from_secs(5)).await;

    let resp = sub.command(&["PING"]).await;
    match &resp {
        Response::Simple(s) => assert_eq!(s.as_ref(), b"PONG"),
        other => panic!("expected Simple PONG, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// PUBLISH / SUBSCRIBE basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_publish_subscribe_basics() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Subscribe to two channels
    let resp = sub.command(&["SUBSCRIBE", "chan1", "chan2"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"subscribe");
    assert_bulk_eq(&items[1], b"chan1");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Read second subscribe confirmation
    let resp2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("second subscribe confirmation");
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"subscribe");
    assert_bulk_eq(&items2[1], b"chan2");
    assert_eq!(unwrap_integer(&items2[2]), 2);

    // Publish to both channels
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 1);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan2", "world"]).await, 1);

    // Read messages
    let msg1 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("message on chan1");
    let parts1 = unwrap_array(msg1);
    assert_bulk_eq(&parts1[0], b"message");
    assert_bulk_eq(&parts1[1], b"chan1");
    assert_bulk_eq(&parts1[2], b"hello");

    let msg2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("message on chan2");
    let parts2 = unwrap_array(msg2);
    assert_bulk_eq(&parts2[0], b"message");
    assert_bulk_eq(&parts2[1], b"chan2");
    assert_bulk_eq(&parts2[2], b"world");

    // Unsubscribe from chan1
    sub.send_only(&["UNSUBSCRIBE", "chan1"]).await;
    let _unsub = sub.read_message(Duration::from_secs(5)).await;

    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan2", "world"]).await, 1);

    let msg3 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("message on chan2 after unsub chan1");
    let parts3 = unwrap_array(msg3);
    assert_bulk_eq(&parts3[0], b"message");
    assert_bulk_eq(&parts3[1], b"chan2");
    assert_bulk_eq(&parts3[2], b"world");

    // Unsubscribe from chan2
    sub.send_only(&["UNSUBSCRIBE", "chan2"]).await;
    let _unsub2 = sub.read_message(Duration::from_secs(5)).await;

    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan2", "world"]).await, 0);
}

#[tokio::test]
async fn tcl_publish_subscribe_with_two_clients() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut pub_client = server.connect().await;

    // Both subscribe to chan1
    let resp1 = sub1.command(&["SUBSCRIBE", "chan1"]).await;
    let items1 = unwrap_array(resp1);
    assert_bulk_eq(&items1[0], b"subscribe");
    assert_eq!(unwrap_integer(&items1[2]), 1);

    let resp2 = sub2.command(&["SUBSCRIBE", "chan1"]).await;
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"subscribe");
    assert_eq!(unwrap_integer(&items2[2]), 1);

    // Publish
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 2);

    // Both receive the message
    let msg1 = sub1
        .read_message(Duration::from_secs(5))
        .await
        .expect("sub1 message");
    let parts1 = unwrap_array(msg1);
    assert_bulk_eq(&parts1[0], b"message");
    assert_bulk_eq(&parts1[1], b"chan1");
    assert_bulk_eq(&parts1[2], b"hello");

    let msg2 = sub2
        .read_message(Duration::from_secs(5))
        .await
        .expect("sub2 message");
    let parts2 = unwrap_array(msg2);
    assert_bulk_eq(&parts2[0], b"message");
    assert_bulk_eq(&parts2[1], b"chan1");
    assert_bulk_eq(&parts2[2], b"hello");
}

#[tokio::test]
async fn tcl_publish_subscribe_after_unsubscribe_without_arguments() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Subscribe to three channels
    let resp = sub.command(&["SUBSCRIBE", "chan1", "chan2", "chan3"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"subscribe");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Read remaining subscribe confirmations
    let _sub2 = sub.read_message(Duration::from_secs(5)).await;
    let _sub3 = sub.read_message(Duration::from_secs(5)).await;

    // Unsubscribe from all (no arguments)
    sub.send_only(&["UNSUBSCRIBE"]).await;
    // Read the three unsubscribe confirmations
    let _unsub1 = sub.read_message(Duration::from_secs(5)).await;
    let _unsub2 = sub.read_message(Duration::from_secs(5)).await;
    let _unsub3 = sub.read_message(Duration::from_secs(5)).await;

    // Publishing should reach nobody
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan2", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan3", "hello"]).await, 0);
}

#[tokio::test]
async fn tcl_subscribe_to_one_channel_more_than_once() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Subscribe to the same channel three times
    let resp = sub.command(&["SUBSCRIBE", "chan1", "chan1", "chan1"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"subscribe");
    assert_bulk_eq(&items[1], b"chan1");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Read second and third subscribe confirmations (count stays 1)
    let resp2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("second subscribe ack");
    let items2 = unwrap_array(resp2);
    assert_eq!(unwrap_integer(&items2[2]), 1);

    let resp3 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("third subscribe ack");
    let items3 = unwrap_array(resp3);
    assert_eq!(unwrap_integer(&items3[2]), 1);

    // Publish should reach only 1 subscriber
    assert_integer_eq(&pub_client.command(&["PUBLISH", "chan1", "hello"]).await, 1);

    // Only one message delivered
    let msg = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("single message");
    let parts = unwrap_array(msg);
    assert_bulk_eq(&parts[0], b"message");
    assert_bulk_eq(&parts[1], b"chan1");
    assert_bulk_eq(&parts[2], b"hello");
}

#[tokio::test]
async fn tcl_unsubscribe_from_non_subscribed_channels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Unsubscribe from channels we never subscribed to
    // The first UNSUBSCRIBE puts us in pubsub mode context for the response
    client
        .send_only(&["UNSUBSCRIBE", "foo", "bar", "quux"])
        .await;

    let resp = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("first unsub response");
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"unsubscribe");
    assert_bulk_eq(&items[1], b"foo");
    assert_eq!(unwrap_integer(&items[2]), 0);

    let resp2 = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("second unsub response");
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"unsubscribe");
    assert_bulk_eq(&items2[1], b"bar");
    assert_eq!(unwrap_integer(&items2[2]), 0);

    let resp3 = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("third unsub response");
    let items3 = unwrap_array(resp3);
    assert_bulk_eq(&items3[0], b"unsubscribe");
    assert_bulk_eq(&items3[1], b"quux");
    assert_eq!(unwrap_integer(&items3[2]), 0);
}

// ---------------------------------------------------------------------------
// PUBLISH / PSUBSCRIBE basics (pattern subscriptions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_publish_psubscribe_basics() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Pattern-subscribe to two patterns
    let resp = sub.command(&["PSUBSCRIBE", "foo.*", "bar.*"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"psubscribe");
    assert_bulk_eq(&items[1], b"foo.*");
    assert_eq!(unwrap_integer(&items[2]), 1);

    let resp2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("second psubscribe ack");
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"psubscribe");
    assert_bulk_eq(&items2[1], b"bar.*");
    assert_eq!(unwrap_integer(&items2[2]), 2);

    // Matching publishes
    assert_integer_eq(&pub_client.command(&["PUBLISH", "foo.1", "hello"]).await, 1);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "bar.1", "hello"]).await, 1);
    // Non-matching publishes
    assert_integer_eq(&pub_client.command(&["PUBLISH", "foo1", "hello"]).await, 0);
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "barfoo.1", "hello"]).await,
        0,
    );
    assert_integer_eq(&pub_client.command(&["PUBLISH", "qux.1", "hello"]).await, 0);

    // Read matching messages
    let msg1 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("pmessage foo.*");
    let parts1 = unwrap_array(msg1);
    assert_bulk_eq(&parts1[0], b"pmessage");
    assert_bulk_eq(&parts1[1], b"foo.*");
    assert_bulk_eq(&parts1[2], b"foo.1");
    assert_bulk_eq(&parts1[3], b"hello");

    let msg2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("pmessage bar.*");
    let parts2 = unwrap_array(msg2);
    assert_bulk_eq(&parts2[0], b"pmessage");
    assert_bulk_eq(&parts2[1], b"bar.*");
    assert_bulk_eq(&parts2[2], b"bar.1");
    assert_bulk_eq(&parts2[3], b"hello");

    // Punsubscribe from foo.*
    sub.send_only(&["PUNSUBSCRIBE", "foo.*"]).await;
    let unsub_resp = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("punsubscribe foo.*");
    let unsub_parts = unwrap_array(unsub_resp);
    assert_bulk_eq(&unsub_parts[0], b"punsubscribe");
    assert_bulk_eq(&unsub_parts[1], b"foo.*");
    assert_eq!(unwrap_integer(&unsub_parts[2]), 1);

    assert_integer_eq(&pub_client.command(&["PUBLISH", "foo.1", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "bar.1", "hello"]).await, 1);

    let msg3 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("pmessage bar.* after punsubscribe foo.*");
    let parts3 = unwrap_array(msg3);
    assert_bulk_eq(&parts3[0], b"pmessage");
    assert_bulk_eq(&parts3[1], b"bar.*");
    assert_bulk_eq(&parts3[2], b"bar.1");
    assert_bulk_eq(&parts3[3], b"hello");

    // Punsubscribe from bar.*
    sub.send_only(&["PUNSUBSCRIBE", "bar.*"]).await;
    let _unsub2 = sub.read_message(Duration::from_secs(5)).await;

    assert_integer_eq(&pub_client.command(&["PUBLISH", "foo.1", "hello"]).await, 0);
    assert_integer_eq(&pub_client.command(&["PUBLISH", "bar.1", "hello"]).await, 0);
}

#[tokio::test]
async fn tcl_publish_psubscribe_with_two_clients() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut pub_client = server.connect().await;

    // Both subscribe to pattern chan.*
    let resp1 = sub1.command(&["PSUBSCRIBE", "chan.*"]).await;
    let items1 = unwrap_array(resp1);
    assert_bulk_eq(&items1[0], b"psubscribe");
    assert_eq!(unwrap_integer(&items1[2]), 1);

    let resp2 = sub2.command(&["PSUBSCRIBE", "chan.*"]).await;
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"psubscribe");
    assert_eq!(unwrap_integer(&items2[2]), 1);

    // Publish
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "chan.foo", "hello"]).await,
        2,
    );

    // Both receive the pmessage
    let msg1 = sub1
        .read_message(Duration::from_secs(5))
        .await
        .expect("sub1 pmessage");
    let parts1 = unwrap_array(msg1);
    assert_bulk_eq(&parts1[0], b"pmessage");
    assert_bulk_eq(&parts1[1], b"chan.*");
    assert_bulk_eq(&parts1[2], b"chan.foo");
    assert_bulk_eq(&parts1[3], b"hello");

    let msg2 = sub2
        .read_message(Duration::from_secs(5))
        .await
        .expect("sub2 pmessage");
    let parts2 = unwrap_array(msg2);
    assert_bulk_eq(&parts2[0], b"pmessage");
    assert_bulk_eq(&parts2[1], b"chan.*");
    assert_bulk_eq(&parts2[2], b"chan.foo");
    assert_bulk_eq(&parts2[3], b"hello");
}

#[tokio::test]
async fn tcl_publish_psubscribe_after_punsubscribe_without_arguments() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Pattern-subscribe to three patterns
    let resp = sub
        .command(&["PSUBSCRIBE", "chan1.*", "chan2.*", "chan3.*"])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"psubscribe");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Read remaining psubscribe confirmations
    let _psub2 = sub.read_message(Duration::from_secs(5)).await;
    let _psub3 = sub.read_message(Duration::from_secs(5)).await;

    // Punsubscribe from all (no arguments)
    sub.send_only(&["PUNSUBSCRIBE"]).await;
    // Read the three punsubscribe confirmations
    let _punsub1 = sub.read_message(Duration::from_secs(5)).await;
    let _punsub2 = sub.read_message(Duration::from_secs(5)).await;
    let _punsub3 = sub.read_message(Duration::from_secs(5)).await;

    // Publishing should reach nobody
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "chan1.hi", "hello"]).await,
        0,
    );
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "chan2.hi", "hello"]).await,
        0,
    );
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "chan3.hi", "hello"]).await,
        0,
    );
}

#[tokio::test]
async fn tcl_punsubscribe_from_non_subscribed_channels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Punsubscribe from patterns we never subscribed to
    client
        .send_only(&["PUNSUBSCRIBE", "foo.*", "bar.*", "quux.*"])
        .await;

    let resp = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("first punsub response");
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"punsubscribe");
    assert_bulk_eq(&items[1], b"foo.*");
    assert_eq!(unwrap_integer(&items[2]), 0);

    let resp2 = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("second punsub response");
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"punsubscribe");
    assert_bulk_eq(&items2[1], b"bar.*");
    assert_eq!(unwrap_integer(&items2[2]), 0);

    let resp3 = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("third punsub response");
    let items3 = unwrap_array(resp3);
    assert_bulk_eq(&items3[0], b"punsubscribe");
    assert_bulk_eq(&items3[1], b"quux.*");
    assert_eq!(unwrap_integer(&items3[2]), 0);
}

// ---------------------------------------------------------------------------
// PUBSUB NUMSUB / NUMPAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_numsub_returns_numbers_not_strings() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["PUBSUB", "NUMSUB", "abc", "def"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 4);
    assert_bulk_eq(&items[0], b"abc");
    assert_eq!(unwrap_integer(&items[1]), 0);
    assert_bulk_eq(&items[2], b"def");
    assert_eq!(unwrap_integer(&items[3]), 0);
}

#[tokio::test]
async fn tcl_numpat_returns_number_of_unique_patterns() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut client = server.connect().await;

    // Three unique patterns, one overlapping (foo* subscribed by both)
    sub1.command(&["PSUBSCRIBE", "foo*"]).await;
    sub2.command(&["PSUBSCRIBE", "foo*"]).await;

    // sub1 also subscribes to bar*
    sub1.send_only(&["PSUBSCRIBE", "bar*"]).await;
    let _ack = sub1.read_message(Duration::from_secs(5)).await;

    // sub2 also subscribes to baz*
    sub2.send_only(&["PSUBSCRIBE", "baz*"]).await;
    let _ack = sub2.read_message(Duration::from_secs(5)).await;

    // NUMPAT returns the number of unique patterns subscribed across all clients
    let resp = client.command(&["PUBSUB", "NUMPAT"]).await;
    let numpat = unwrap_integer(&resp);
    // foo*, bar*, baz* = 3 unique patterns
    assert_eq!(numpat, 3);
}

// ---------------------------------------------------------------------------
// Mix SUBSCRIBE and PSUBSCRIBE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_mix_subscribe_and_psubscribe() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut pub_client = server.connect().await;

    // Subscribe to exact channel
    let resp = sub.command(&["SUBSCRIBE", "foo.bar"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"subscribe");
    assert_eq!(unwrap_integer(&items[2]), 1);

    // Also pattern-subscribe to foo.*
    sub.send_only(&["PSUBSCRIBE", "foo.*"]).await;
    let psub_resp = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("psubscribe ack");
    let psub_items = unwrap_array(psub_resp);
    assert_bulk_eq(&psub_items[0], b"psubscribe");
    assert_eq!(unwrap_integer(&psub_items[2]), 2);

    // Publish to foo.bar - should match both subscriptions
    assert_integer_eq(
        &pub_client.command(&["PUBLISH", "foo.bar", "hello"]).await,
        2,
    );

    // Read both messages: one from SUBSCRIBE, one from PSUBSCRIBE
    let msg1 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("first message");
    let parts1 = unwrap_array(msg1);
    assert_bulk_eq(&parts1[0], b"message");
    assert_bulk_eq(&parts1[1], b"foo.bar");
    assert_bulk_eq(&parts1[2], b"hello");

    let msg2 = sub
        .read_message(Duration::from_secs(5))
        .await
        .expect("second message (pmessage)");
    let parts2 = unwrap_array(msg2);
    assert_bulk_eq(&parts2[0], b"pmessage");
    assert_bulk_eq(&parts2[1], b"foo.*");
    assert_bulk_eq(&parts2[2], b"foo.bar");
    assert_bulk_eq(&parts2[3], b"hello");
}

// ---------------------------------------------------------------------------
// PUNSUBSCRIBE and UNSUBSCRIBE should always reply
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_punsubscribe_and_unsubscribe_should_always_reply() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PUNSUBSCRIBE when not subscribed to anything
    let resp = client.command(&["PUNSUBSCRIBE"]).await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"punsubscribe");
    // Channel name should be nil or empty
    assert_eq!(unwrap_integer(&items[2]), 0);

    // UNSUBSCRIBE when not subscribed to anything
    let resp2 = client.command(&["UNSUBSCRIBE"]).await;
    let items2 = unwrap_array(resp2);
    assert_bulk_eq(&items2[0], b"unsubscribe");
    assert_eq!(unwrap_integer(&items2[2]), 0);
}

// ---------------------------------------------------------------------------
// PUBSUB CHANNELS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pubsub_channels_lists_active_channels() {
    let server = TestServer::start_standalone().await;
    let mut sub = server.connect().await;
    let mut client = server.connect().await;

    // Subscribe to some channels
    sub.command(&["SUBSCRIBE", "active1", "active2", "other"])
        .await;
    // Read remaining confirmations
    let _ack2 = sub.read_message(Duration::from_secs(5)).await;
    let _ack3 = sub.read_message(Duration::from_secs(5)).await;

    // PUBSUB CHANNELS without pattern should list all
    let resp = client.command(&["PUBSUB", "CHANNELS"]).await;
    let channels = extract_bulk_strings(&resp);
    assert!(channels.contains(&"active1".to_string()));
    assert!(channels.contains(&"active2".to_string()));
    assert!(channels.contains(&"other".to_string()));

    // PUBSUB CHANNELS with pattern
    let resp2 = client.command(&["PUBSUB", "CHANNELS", "active*"]).await;
    let channels2 = extract_bulk_strings(&resp2);
    assert!(channels2.contains(&"active1".to_string()));
    assert!(channels2.contains(&"active2".to_string()));
    assert!(!channels2.contains(&"other".to_string()));
}

// ---------------------------------------------------------------------------
// PUBSUB NUMSUB with active subscriptions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pubsub_numsub_with_active_subscriptions() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut client = server.connect().await;

    // Both subscribe to chan1, only sub1 subscribes to chan2
    sub1.command(&["SUBSCRIBE", "chan1", "chan2"]).await;
    let _ack = sub1.read_message(Duration::from_secs(5)).await;
    sub2.command(&["SUBSCRIBE", "chan1"]).await;

    let resp = client
        .command(&["PUBSUB", "NUMSUB", "chan1", "chan2", "chan3"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 6);
    assert_bulk_eq(&items[0], b"chan1");
    assert_eq!(unwrap_integer(&items[1]), 2);
    assert_bulk_eq(&items[2], b"chan2");
    assert_eq!(unwrap_integer(&items[3]), 1);
    assert_bulk_eq(&items[4], b"chan3");
    assert_eq!(unwrap_integer(&items[5]), 0);
}
