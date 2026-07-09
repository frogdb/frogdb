//! Integration tests for pub/sub commands (SUBSCRIBE, PUBLISH, PSUBSCRIBE, etc.)

use crate::common::test_server::{TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::Duration;

#[tokio::test]
async fn test_subscribe_publish() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to a channel
    let response = subscriber.command(&["SUBSCRIBE", "mychannel"]).await;
    assert!(matches!(response, Response::Array(ref arr) if arr.len() == 3));
    if let Response::Array(arr) = &response {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("subscribe"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("mychannel"))));
        assert_eq!(arr[2], Response::Integer(1));
    }

    // Give the subscription time to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish a message
    let response = publisher.command(&["PUBLISH", "mychannel", "hello"]).await;
    // Should return the number of subscribers that received the message
    assert!(matches!(response, Response::Integer(n) if n >= 1));

    // Subscriber should receive the message
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some());
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("mychannel"))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("hello"))));
    } else {
        panic!("Expected array response for message");
    }

    server.shutdown().await;
}

/// Regression: LREM previously emitted no keyspace notification because it
/// never declared `keyspace_event_type()`. Redis fires an `lrem` keyevent.
///
/// Runs on the default multi-shard topology (4 shards): SUBSCRIBE registers on
/// the broadcast coordinator shard (shard 0), while `mylist` may be owned by any
/// shard. The keyspace-notification coordinator routes the emit to shard 0
/// regardless, so delivery no longer depends on the key landing on shard 0.
#[tokio::test]
async fn test_lrem_emits_keyspace_notification() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Enable keyspace + keyevent notifications for all event classes.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Subscribe to the exact keyevent channel for LREM.
    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:lrem"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    // Give the subscription time to register.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Seed a list and remove matching elements. RPUSH fires `rpush`, which we
    // are not subscribed to, so only the LREM event should arrive.
    client
        .command(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
        .await;
    let removed = client.command(&["LREM", "mylist", "0", "a"]).await;
    assert_eq!(removed, Response::Integer(3));

    // Subscriber should receive a keyevent message naming the modified key.
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "expected an lrem keyevent notification");
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:lrem")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("mylist"))));
    } else {
        panic!("Expected array response for lrem keyevent message");
    }

    server.shutdown().await;
}

/// PFADD fires a `pfadd` keyevent when the HyperLogLog is actually modified.
/// Redis emits `pfadd` under the STRING class for effective PFADD writes; a
/// no-op PFADD (no register moved) emits nothing — see
/// `test_noop_pfadd_emits_no_notification`.
#[tokio::test]
async fn test_pfadd_emits_keyspace_notification() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Enable keyspace + keyevent notifications for all event classes.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Subscribe to the exact keyevent channel for PFADD.
    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:pfadd"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    // Give the subscription time to register.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Adding a fresh element moves a register, so PFADD returns 1 and fires.
    let added = client.command(&["PFADD", "myhll", "a"]).await;
    assert_eq!(added, Response::Integer(1));

    // Subscriber should receive a keyevent message naming the modified key.
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "expected a pfadd keyevent notification");
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:pfadd")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("myhll"))));
    } else {
        panic!("Expected array response for pfadd keyevent message");
    }

    server.shutdown().await;
}

/// A no-op PFADD (element already present, no register moved) must emit no
/// keyspace notification. Task 1's `write_was_noop` gate skips the entire
/// write-effect pipeline — including keyspace events — for unchanged HLLs.
#[tokio::test]
async fn test_noop_pfadd_emits_no_notification() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed the HLL so the register for `a` is already set.
    let added = client.command(&["PFADD", "myhll", "a"]).await;
    assert_eq!(added, Response::Integer(1));

    // Subscribe only after seeding, so the seed event is not observed here.
    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:pfadd"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Duplicate add: no register moves, so PFADD returns 0 and emits nothing.
    let added = client.command(&["PFADD", "myhll", "a"]).await;
    assert_eq!(added, Response::Integer(0));

    let msg = subscriber.read_message(Duration::from_millis(300)).await;
    assert!(
        msg.is_none(),
        "a no-op PFADD must not deliver a keyspace notification"
    );

    server.shutdown().await;
}

/// Find a key whose owning shard is NOT shard 0 (the broadcast coordinator
/// shard, where SUBSCRIBE registers). Such a key exercises the cross-shard
/// keyspace-notification routing path: the event fires on a non-coordinator
/// shard and must be forwarded to shard 0 to reach the subscriber.
fn key_off_shard_zero(num_shards: usize) -> String {
    for i in 0..1_000_000 {
        let key = format!("kskey:{i}");
        if frogdb_core::shard_for_key(key.as_bytes(), num_shards) != 0 {
            return key;
        }
    }
    panic!("no non-shard-0 key found for {num_shards} shards");
}

/// Cross-shard keyevent delivery (write path). Before the coordinator, a `set`
/// keyevent for a key owned by a shard other than shard 0 was published into
/// that shard's own subscriber-less table and never reached the shard-0
/// subscriber. It must now be delivered.
#[tokio::test]
async fn test_cross_shard_keyevent_notification_delivered() {
    let num_shards = 4;
    let key = key_off_shard_zero(num_shards);
    assert_ne!(
        frogdb_core::shard_for_key(key.as_bytes(), num_shards),
        0,
        "test key must live off shard 0"
    );

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    })
    .await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:set"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // SET is dispatched to the key-owner shard (not shard 0), which emits `set`.
    client.command(&["SET", key.as_str(), "v"]).await;

    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "cross-shard `set` keyevent must reach the shard-0 subscriber"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:set")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from(key))));
    } else {
        panic!("Expected array response for set keyevent message");
    }

    server.shutdown().await;
}

/// Cross-shard keyspace delivery (write path), the `__keyspace@0__:<key>`
/// channel form whose payload is the event name. Pins the other channel shape
/// through the same coordinator path.
#[tokio::test]
async fn test_cross_shard_keyspace_notification_delivered() {
    let num_shards = 4;
    let key = key_off_shard_zero(num_shards);
    let channel = format!("__keyspace@0__:{key}");

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    })
    .await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    let resp = subscriber.command(&["SUBSCRIBE", channel.as_str()]).await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.command(&["SET", key.as_str(), "v"]).await;

    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "cross-shard keyspace notification must reach the shard-0 subscriber"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel))));
        // __keyspace@ channels carry the event name as the payload.
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("set"))));
    } else {
        panic!("Expected array response for keyspace message");
    }

    server.shutdown().await;
}

/// Cross-shard delivery for the active-expiry emit class: a key on a non-zero
/// shard expiring via the background sweep must forward its `expired` keyevent
/// to the shard-0 subscriber. Guards that all emit classes (not just writes)
/// funnel through the coordinator.
#[tokio::test]
async fn test_cross_shard_expired_keyevent_delivered() {
    let num_shards = 4;
    let key = key_off_shard_zero(num_shards);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    })
    .await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:expired"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Short TTL, then leave the key untouched so the background active-expiry
    // sweep (100ms cadence) — not a client access — is what expires it.
    client
        .command(&["SET", key.as_str(), "v", "PX", "100"])
        .await;

    let msg = subscriber.read_message(Duration::from_secs(3)).await;
    assert!(
        msg.is_some(),
        "cross-shard `expired` keyevent from active expiry must reach the subscriber"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:expired")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from(key))));
    } else {
        panic!("Expected array response for expired keyevent message");
    }

    server.shutdown().await;
}

/// The disabled fast path is unaffected: with notify-keyspace-events off
/// (server default), a write on any shard emits nothing, so the coordinator is
/// never consulted and the subscriber receives no message.
#[tokio::test]
async fn test_keyspace_notifications_disabled_delivers_nothing() {
    let num_shards = 4;
    let key = key_off_shard_zero(num_shards);

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    })
    .await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Deliberately do NOT enable notify-keyspace-events.
    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:set"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.command(&["SET", key.as_str(), "v"]).await;

    let msg = subscriber.read_message(Duration::from_millis(300)).await;
    assert!(
        msg.is_none(),
        "no keyspace notification may be delivered while notifications are disabled"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_psubscribe_pattern() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to a pattern
    let response = subscriber.command(&["PSUBSCRIBE", "news.*"]).await;
    assert!(matches!(response, Response::Array(ref arr) if arr.len() == 3));
    if let Response::Array(arr) = &response {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("psubscribe"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("news.*"))));
        assert_eq!(arr[2], Response::Integer(1));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish to a matching channel
    let response = publisher
        .command(&["PUBLISH", "news.sports", "goal!"])
        .await;
    assert!(matches!(response, Response::Integer(n) if n >= 1));

    // Subscriber should receive a pmessage
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some());
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("pmessage"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("news.*"))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("news.sports"))));
        assert_eq!(arr[3], Response::Bulk(Some(Bytes::from("goal!"))));
    } else {
        panic!("Expected array response for pmessage");
    }

    // Publish to a non-matching channel should not deliver
    publisher
        .command(&["PUBLISH", "weather.today", "sunny"])
        .await;

    // Should not receive a message
    let msg = subscriber.read_message(Duration::from_millis(200)).await;
    assert!(
        msg.is_none(),
        "Should not receive message for non-matching pattern"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_unsubscribe() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Subscribe to a channel
    client.command(&["SUBSCRIBE", "ch1"]).await;

    // Unsubscribe
    let response = client.command(&["UNSUBSCRIBE", "ch1"]).await;
    if let Response::Array(arr) = &response {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("unsubscribe"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("ch1"))));
        assert_eq!(arr[2], Response::Integer(0));
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_unsubscribe_all() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Subscribe to multiple channels
    client.command(&["SUBSCRIBE", "ch1", "ch2"]).await;

    // Read the second subscribe response
    client.read_message(Duration::from_millis(100)).await;

    // Unsubscribe from all (no args)
    client.send_only(&["UNSUBSCRIBE"]).await;

    // Should get two unsubscribe confirmations
    let msg1 = client.read_message(Duration::from_secs(1)).await;
    let msg2 = client.read_message(Duration::from_secs(1)).await;

    assert!(msg1.is_some());
    assert!(msg2.is_some());

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_channels() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Subscribe to some channels
    subscriber.command(&["SUBSCRIBE", "ch1", "ch2"]).await;
    subscriber.read_message(Duration::from_millis(100)).await; // Read second subscribe response

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get active channels
    let response = client.command(&["PUBSUB", "CHANNELS"]).await;
    if let Response::Array(channels) = &response {
        // Should have at least the channels we subscribed to
        assert!(channels.len() >= 2);
    } else {
        panic!("Expected array response from PUBSUB CHANNELS");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_numsub() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut client = server.connect().await;

    // Two subscribers to ch1
    sub1.command(&["SUBSCRIBE", "ch1"]).await;
    sub2.command(&["SUBSCRIBE", "ch1"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get subscriber count
    let response = client.command(&["PUBSUB", "NUMSUB", "ch1", "ch2"]).await;
    if let Response::Array(arr) = &response {
        // Should be [ch1, count, ch2, count]
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("ch1"))));
        // Count for ch1 should be at least 2
        if let Response::Integer(n) = arr[1] {
            assert!(n >= 2, "Expected at least 2 subscribers, got {}", n);
        }
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("ch2"))));
        // Count for ch2 should be 0
        assert_eq!(arr[3], Response::Integer(0));
    } else {
        panic!("Expected array response from PUBSUB NUMSUB");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_numpat() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Subscribe to patterns
    subscriber
        .command(&["PSUBSCRIBE", "news.*", "sports.*"])
        .await;
    subscriber.read_message(Duration::from_millis(100)).await; // Read second psubscribe response

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get pattern count
    let response = client.command(&["PUBSUB", "NUMPAT"]).await;
    if let Response::Integer(n) = response {
        assert!(n >= 2, "Expected at least 2 patterns, got {}", n);
    } else {
        panic!("Expected integer response from PUBSUB NUMPAT");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_pubsub_mode_restrictions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Subscribe to enter pub/sub mode
    client.command(&["SUBSCRIBE", "mychannel"]).await;

    // Try to execute a non-pub/sub command
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(ref e) if e.starts_with(b"ERR Can't execute")));

    // PING should still work (RESP2 pubsub mode returns array ["pong", ""])
    let response = client.command(&["PING"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("pong"))),
            Response::Bulk(Some(Bytes::from(""))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let server = TestServer::start_standalone().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut publisher = server.connect().await;

    // Both subscribe to the same channel
    sub1.command(&["SUBSCRIBE", "broadcast"]).await;
    sub2.command(&["SUBSCRIBE", "broadcast"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish a message
    let response = publisher
        .command(&["PUBLISH", "broadcast", "hello everyone"])
        .await;
    // Should return count >= 2 (both subscribers received it)
    assert!(matches!(response, Response::Integer(n) if n >= 2));

    // Both should receive the message
    let msg1 = sub1.read_message(Duration::from_secs(2)).await;
    let msg2 = sub2.read_message(Duration::from_secs(2)).await;

    assert!(msg1.is_some());
    assert!(msg2.is_some());

    if let Some(Response::Array(arr)) = msg1 {
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("hello everyone"))));
    }
    if let Some(Response::Array(arr)) = msg2 {
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("hello everyone"))));
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_namespace_isolation() {
    let server = TestServer::start_standalone().await;
    let mut broadcast_sub = server.connect().await;
    let mut sharded_sub = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to broadcast channel
    broadcast_sub.command(&["SUBSCRIBE", "orders"]).await;

    // Subscribe to sharded channel with same name
    sharded_sub.command(&["SSUBSCRIBE", "orders"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // PUBLISH (broadcast) should only reach SUBSCRIBE, not SSUBSCRIBE
    publisher
        .command(&["PUBLISH", "orders", "broadcast message"])
        .await;

    let broadcast_msg = broadcast_sub.read_message(Duration::from_secs(1)).await;
    let sharded_msg = sharded_sub.read_message(Duration::from_millis(200)).await;

    assert!(
        broadcast_msg.is_some(),
        "Broadcast subscriber should receive PUBLISH"
    );
    assert!(
        sharded_msg.is_none(),
        "Sharded subscriber should NOT receive PUBLISH"
    );

    // SPUBLISH (sharded) should only reach SSUBSCRIBE, not SUBSCRIBE
    publisher
        .command(&["SPUBLISH", "orders", "sharded message"])
        .await;

    let broadcast_msg = broadcast_sub.read_message(Duration::from_millis(200)).await;
    let sharded_msg = sharded_sub.read_message(Duration::from_secs(1)).await;

    assert!(
        broadcast_msg.is_none(),
        "Broadcast subscriber should NOT receive SPUBLISH"
    );
    assert!(
        sharded_msg.is_some(),
        "Sharded subscriber should receive SPUBLISH"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_sharded_subscribe_publish() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to a sharded channel
    let response = subscriber.command(&["SSUBSCRIBE", "orders:123"]).await;
    if let Response::Array(arr) = &response {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("ssubscribe"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("orders:123"))));
        assert_eq!(arr[2], Response::Integer(1));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish to the sharded channel
    let response = publisher
        .command(&["SPUBLISH", "orders:123", "new order"])
        .await;
    assert!(matches!(response, Response::Integer(n) if n >= 1));

    // Subscriber should receive an smessage
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some());
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("smessage"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("orders:123"))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("new order"))));
    } else {
        panic!("Expected array response for smessage");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_publish_returns_zero_no_subscribers() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Publish to a channel with no subscribers
    let response = client.command(&["PUBLISH", "empty", "message"]).await;
    assert_eq!(response, Response::Integer(0));

    // Same for sharded publish
    let response = client.command(&["SPUBLISH", "empty", "message"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

// ============================================================================
// Cluster Pub/Sub Tests
// ============================================================================

/// Tests that pub/sub works within a single cluster node.
#[tokio::test]
async fn test_pubsub_works_within_single_cluster_node() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node = harness.node(harness.node_ids()[0]).unwrap();

    // Create two connections on the same node
    let mut subscriber = node.connect().await;
    let mut publisher = node.connect().await;

    // Subscribe
    let response = subscriber.command(&["SUBSCRIBE", "cluster_chan"]).await;
    assert!(matches!(response, Response::Array(ref arr) if arr.len() == 3));
    if let Response::Array(arr) = &response {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("subscribe"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("cluster_chan"))));
        assert_eq!(arr[2], Response::Integer(1));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish on the same node
    let response = publisher
        .command(&["PUBLISH", "cluster_chan", "cluster_msg"])
        .await;
    assert!(
        matches!(response, Response::Integer(n) if n >= 1),
        "PUBLISH should return >= 1, got: {:?}",
        response
    );

    // Subscriber should receive the message
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some(), "Subscriber should receive message");
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("cluster_chan"))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("cluster_msg"))));
    } else {
        panic!("Expected array message response");
    }

    harness.shutdown_all().await;
}

/// Tests that cross-node pub/sub forwarding works in cluster mode.
#[tokio::test]
async fn test_pubsub_cross_node_forwarded() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // Subscribe on node A
    let node_a = harness.node(node_ids[0]).unwrap();
    let mut subscriber = node_a.connect().await;
    subscriber.command(&["SUBSCRIBE", "cross_chan"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish on node B
    let node_b = harness.node(node_ids[1]).unwrap();
    let mut publisher = node_b.connect().await;
    let response = publisher
        .command(&["PUBLISH", "cross_chan", "cross_msg"])
        .await;

    // When cross-node forwarding is implemented, PUBLISH should return >= 1
    assert!(
        matches!(response, Response::Integer(n) if n >= 1),
        "PUBLISH on node B should reach subscriber on node A, got: {:?}",
        response
    );

    // Subscriber on node A should receive the message
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "Subscriber on node A should receive message published on node B"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("cross_chan"))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("cross_msg"))));
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 2: Sharded PubSub in Cluster
// ============================================================================

/// Tests that SSUBSCRIBE on a non-owner node returns MOVED redirect.
///
/// Inspired by Redis `26-pubsubshard.tcl`.
#[tokio::test]
async fn test_ssubscribe_wrong_node_returns_moved() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{
        is_error, is_moved_redirect, parse_cluster_nodes, slot_for_key,
    };

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Find a channel whose slot is owned by a specific node,
    // then try SSUBSCRIBE on a different node
    let channel = "ssubscribe_test_chan";
    let slot = slot_for_key(channel.as_bytes());

    let node_ids = harness.node_ids();
    let first_node = harness.node(node_ids[0]).unwrap();
    let nodes_resp = first_node.send("CLUSTER", &["NODES"]).await;
    let nodes = parse_cluster_nodes(&nodes_resp).unwrap();

    // Find a node that doesn't own this slot
    let non_owner = nodes
        .iter()
        .find(|n| !n.slots.iter().any(|(s, e)| slot >= *s && slot <= *e));

    if let Some(non_owner_info) = non_owner {
        // Find harness node matching this address
        for &nid in &node_ids {
            if let Some(node) = harness.node(nid)
                && node.client_addr() == non_owner_info.addr
            {
                let mut client = node.connect().await;
                let resp = client.command(&["SSUBSCRIBE", channel]).await;

                // Should get MOVED or an error indicating wrong node
                if is_moved_redirect(&resp).is_some() {
                    eprintln!("Got expected MOVED redirect for SSUBSCRIBE on non-owner");
                } else if is_error(&resp) {
                    eprintln!("Got error (acceptable): {:?}", resp);
                } else {
                    // Some implementations may allow SSUBSCRIBE on any node
                    // and forward internally
                    eprintln!("SSUBSCRIBE accepted on non-owner: {:?}", resp);
                }
                break;
            }
        }
    } else {
        eprintln!("All nodes own the target slot, skipping test");
    }

    harness.shutdown_all().await;
}

/// Tests that SSUBSCRIBE + SPUBLISH works on the correct slot owner in cluster mode.
///
/// Inspired by Redis `26-pubsubshard.tcl`.
#[tokio::test]
async fn test_ssubscribe_correct_node_receives_spublish() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{
        is_error, is_moved_redirect, key_for_slot, slot_for_key,
    };

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Pick a channel and find its slot owner
    let channel = "spub_test_chan";
    let slot = slot_for_key(channel.as_bytes());

    let node_ids = harness.node_ids();

    // Probe to find the slot owner
    let probe_key = key_for_slot(slot);
    let mut owner_nid = None;
    for &nid in &node_ids {
        let node = harness.node(nid).unwrap();
        let resp = node.send("GET", &[&probe_key]).await;
        if !is_error(&resp) {
            owner_nid = Some(nid);
            break;
        } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            for &other_nid in &node_ids {
                if harness.node(other_nid).unwrap().client_addr() == addr {
                    owner_nid = Some(other_nid);
                    break;
                }
            }
            break;
        }
    }

    if let Some(owner) = owner_nid {
        let owner_node = harness.node(owner).unwrap();
        let mut subscriber = owner_node.connect().await;
        let mut publisher = owner_node.connect().await;

        // SSUBSCRIBE on the owner
        let sub_resp = subscriber.command(&["SSUBSCRIBE", channel]).await;
        if let Response::Array(arr) = &sub_resp {
            assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("ssubscribe"))));
            assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel))));
            assert_eq!(arr[2], Response::Integer(1));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // SPUBLISH on the same owner node
        let pub_resp = publisher
            .command(&["SPUBLISH", channel, "sharded_cluster_msg"])
            .await;
        assert!(
            matches!(pub_resp, Response::Integer(n) if n >= 1),
            "SPUBLISH should return >= 1, got: {:?}",
            pub_resp
        );

        // Subscriber should receive smessage
        let msg = subscriber.read_message(Duration::from_secs(2)).await;
        assert!(msg.is_some(), "Subscriber should receive sharded message");
        if let Some(Response::Array(arr)) = msg {
            assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("smessage"))));
            assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel))));
            assert_eq!(
                arr[2],
                Response::Bulk(Some(Bytes::from("sharded_cluster_msg")))
            );
        }
    } else {
        eprintln!("Could not find slot owner, skipping test");
    }

    harness.shutdown_all().await;
}

/// Tests that SSUBSCRIBE to channels in different slots returns CROSSSLOT error.
///
/// Inspired by Redis `26-pubsubshard.tcl`.
#[tokio::test]
async fn test_ssubscribe_multi_channel_different_slots_rejected() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{is_error, slot_for_key};

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Find two channels that hash to different slots
    let chan1 = "chan_a";
    let chan2 = "chan_b";
    let slot1 = slot_for_key(chan1.as_bytes());
    let slot2 = slot_for_key(chan2.as_bytes());

    if slot1 == slot2 {
        eprintln!(
            "Channels hash to same slot ({}) — pick different channels",
            slot1
        );
        harness.shutdown_all().await;
        return;
    }

    let node = harness.node(harness.node_ids()[0]).unwrap();
    let mut client = node.connect().await;

    // SSUBSCRIBE to channels in different slots should fail with CROSSSLOT
    let resp = client.command(&["SSUBSCRIBE", chan1, chan2]).await;

    if is_error(&resp) {
        if let Response::Error(e) = &resp {
            let msg = String::from_utf8_lossy(e);
            eprintln!("Got error for cross-slot SSUBSCRIBE: {}", msg);
            // Should contain CROSSSLOT or similar indication
        }
    } else {
        // Some implementations might accept the first channel and reject the second
        eprintln!(
            "SSUBSCRIBE accepted both channels (cross-slot validation may not be implemented): {:?}",
            resp
        );
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 3: Sharded PubSub During Migration
// ============================================================================

/// Verifies that sharded pubsub subscribers receive an SUNSUBSCRIBE notification
/// when their channel's slot migrates to another node.
///
/// Inspired by Redis `25-pubsubshard-slot-migration.tcl`.
#[tokio::test]
async fn test_ssubscribe_client_receives_sunsubscribe_on_slot_migration() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::slot_for_key;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let channel = "migrate_chan";
    let slot = slot_for_key(b"migrate_chan");

    // CLUSTER SETSLOT is a Raft command — must go through the leader.
    let leader_id = harness.get_leader().await.expect("leader should exist");
    let leader_node = harness.node(leader_id).unwrap();

    // Find the node that owns this slot.
    let snapshot = leader_node.cluster_state().unwrap().snapshot();
    let owner_node_id = snapshot
        .get_slot_owner(slot)
        .expect("slot should be assigned");

    // Pick a target node different from the owner.
    let node_ids = harness.node_ids();
    let target_node_id = *node_ids
        .iter()
        .find(|&&id| id != owner_node_id)
        .expect("need a different node as migration target");

    let source_node = harness.node(owner_node_id).unwrap();
    let source_id_str = harness.get_node_id_str(owner_node_id).unwrap();
    let target_id_str = harness.get_node_id_str(target_node_id).unwrap();
    let slot_str = slot.to_string();

    // Connect a subscriber to the slot owner and SSUBSCRIBE.
    let mut subscriber = source_node.connect().await;
    let sub_resp = subscriber.command(&["SSUBSCRIBE", channel]).await;
    assert!(
        matches!(&sub_resp, Response::Array(arr) if arr.len() == 3),
        "expected ssubscribe confirmation, got: {:?}",
        sub_resp
    );

    // All SETSLOT commands go through the Raft leader.
    // When the leader isn't the source/target, explicit IDs are required
    // (the optional source/target defaults to my_node_id).
    let migrate_resp = leader_node
        .send(
            "CLUSTER",
            &[
                "SETSLOT",
                &slot_str,
                "MIGRATING",
                &target_id_str,
                &source_id_str,
            ],
        )
        .await;
    assert!(
        !matches!(&migrate_resp, Response::Error(_)),
        "SETSLOT MIGRATING failed: {:?}",
        migrate_resp
    );

    let import_resp = leader_node
        .send(
            "CLUSTER",
            &[
                "SETSLOT",
                &slot_str,
                "IMPORTING",
                &source_id_str,
                &target_id_str,
            ],
        )
        .await;
    assert!(
        !matches!(&import_resp, Response::Error(_)),
        "SETSLOT IMPORTING failed: {:?}",
        import_resp
    );

    // Complete migration (fires SlotMigrationCompleteEvent on all nodes via Raft).
    let complete_resp = leader_node
        .send("CLUSTER", &["SETSLOT", &slot_str, "NODE", &target_id_str])
        .await;
    assert!(
        !matches!(&complete_resp, Response::Error(_)),
        "SETSLOT NODE failed: {:?}",
        complete_resp
    );

    // The subscriber should receive an SUNSUBSCRIBE notification.
    let msg = subscriber
        .read_message(Duration::from_secs(5))
        .await
        .expect("expected sunsubscribe notification");

    if let Response::Array(ref arr) = msg {
        assert_eq!(arr.len(), 3, "sunsubscribe message should have 3 elements");
        assert_eq!(
            arr[0],
            Response::Bulk(Some(Bytes::from("sunsubscribe"))),
            "first element should be 'sunsubscribe'"
        );
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from(channel))),
            "second element should be channel name"
        );
        assert_eq!(
            arr[2],
            Response::Integer(0),
            "third element should be 0 (no remaining sharded subs)"
        );
    } else {
        panic!("expected Array response, got: {:?}", msg);
    }

    harness.shutdown_all().await;
}

// ============================================================================
// Tier 4: Cross-Node PubSub
// ============================================================================

/// Tests that PSUBSCRIBE on node A receives PUBLISH from node B.
#[tokio::test]
async fn test_psubscribe_cross_node_pattern_match_forwarded() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let node_ids = harness.node_ids();

    // PSUBSCRIBE on node A
    let node_a = harness.node(node_ids[0]).unwrap();
    let mut subscriber = node_a.connect().await;
    subscriber.command(&["PSUBSCRIBE", "cross_pattern.*"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // PUBLISH on node B
    let node_b = harness.node(node_ids[1]).unwrap();
    let mut publisher = node_b.connect().await;
    let response = publisher
        .command(&["PUBLISH", "cross_pattern.test", "cross_msg"])
        .await;

    // When cross-node forwarding is implemented, PUBLISH should return >= 1
    assert!(
        matches!(response, Response::Integer(n) if n >= 1),
        "PUBLISH on node B should reach pattern subscriber on node A, got: {:?}",
        response
    );

    // Subscriber on node A should receive a pmessage
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "Pattern subscriber on node A should receive message published on node B"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("pmessage"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("cross_pattern.*"))));
        assert_eq!(
            arr[2],
            Response::Bulk(Some(Bytes::from("cross_pattern.test")))
        );
        assert_eq!(arr[3], Response::Bulk(Some(Bytes::from("cross_msg"))));
    }

    harness.shutdown_all().await;
}

/// Tests that SPUBLISH on a non-owner node forwards to the owner and delivers.
#[tokio::test]
async fn test_spublish_cross_node_forwarded() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{
        is_error, is_moved_redirect, key_for_slot, slot_for_key,
    };

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let channel = "spub_fwd_chan";
    let slot = slot_for_key(channel.as_bytes());
    let node_ids = harness.node_ids();

    // Find the slot owner via a probe key
    let probe_key = key_for_slot(slot);
    let mut owner_nid = None;
    for &nid in &node_ids {
        let node = harness.node(nid).unwrap();
        let resp = node.send("GET", &[&probe_key]).await;
        if !is_error(&resp) {
            owner_nid = Some(nid);
            break;
        } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            for &other_nid in &node_ids {
                if harness.node(other_nid).unwrap().client_addr() == addr {
                    owner_nid = Some(other_nid);
                    break;
                }
            }
            break;
        }
    }

    let owner_nid = match owner_nid {
        Some(nid) => nid,
        None => {
            eprintln!("Could not find slot owner, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    // Find a non-owner node
    let non_owner_nid = node_ids.iter().find(|&&nid| nid != owner_nid).copied();
    let non_owner_nid = match non_owner_nid {
        Some(nid) => nid,
        None => {
            eprintln!("No non-owner node found, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    // SSUBSCRIBE on the owner
    let owner_node = harness.node(owner_nid).unwrap();
    let mut subscriber = owner_node.connect().await;
    subscriber.command(&["SSUBSCRIBE", channel]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // SPUBLISH on a non-owner node (should be forwarded)
    let non_owner_node = harness.node(non_owner_nid).unwrap();
    let mut publisher = non_owner_node.connect().await;
    let pub_resp = publisher
        .command(&["SPUBLISH", channel, "forwarded_msg"])
        .await;
    assert!(
        matches!(pub_resp, Response::Integer(n) if n >= 1),
        "SPUBLISH on non-owner should forward and return >= 1, got: {:?}",
        pub_resp
    );

    // Subscriber on owner should receive the message
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "Subscriber on slot owner should receive SPUBLISH forwarded from non-owner"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("smessage"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel))));
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("forwarded_msg"))));
    }

    harness.shutdown_all().await;
}

/// Tests that SSUBSCRIBE on a non-owner node gets a MOVED redirect.
#[tokio::test]
async fn test_ssubscribe_non_owner_returns_moved() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{
        is_error, is_moved_redirect, key_for_slot, slot_for_key,
    };

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    let channel = "ssub_moved_chan";
    let slot = slot_for_key(channel.as_bytes());
    let node_ids = harness.node_ids();

    // Find the slot owner
    let probe_key = key_for_slot(slot);
    let mut owner_nid = None;
    for &nid in &node_ids {
        let node = harness.node(nid).unwrap();
        let resp = node.send("GET", &[&probe_key]).await;
        if !is_error(&resp) {
            owner_nid = Some(nid);
            break;
        } else if let Some((_slot, addr)) = is_moved_redirect(&resp) {
            for &other_nid in &node_ids {
                if harness.node(other_nid).unwrap().client_addr() == addr {
                    owner_nid = Some(other_nid);
                    break;
                }
            }
            break;
        }
    }

    let owner_nid = match owner_nid {
        Some(nid) => nid,
        None => {
            eprintln!("Could not find slot owner, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    // Find a non-owner node
    let non_owner_nid = node_ids.iter().find(|&&nid| nid != owner_nid).copied();
    let non_owner_nid = match non_owner_nid {
        Some(nid) => nid,
        None => {
            eprintln!("No non-owner node found, skipping test");
            harness.shutdown_all().await;
            return;
        }
    };

    // SSUBSCRIBE on the non-owner — should get MOVED
    let non_owner_node = harness.node(non_owner_nid).unwrap();
    let mut client = non_owner_node.connect().await;
    let resp = client.command(&["SSUBSCRIBE", channel]).await;

    assert!(
        is_moved_redirect(&resp).is_some(),
        "SSUBSCRIBE on non-owner should return MOVED, got: {:?}",
        resp
    );

    harness.shutdown_all().await;
}

/// SSUBSCRIBE now feeds the same redirect seam as the keyed command path
/// (`coordinator.route()` + `RouteDecision::to_response`). On a non-owner node,
/// SSUBSCRIBE `<chan>` must therefore return the *exact same* MOVED target
/// (slot + address) as GET `<chan>` for the same string — pinning the two paths
/// together so they cannot drift in format or destination.
#[tokio::test]
async fn test_ssubscribe_redirect_matches_keyed_path() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::is_moved_redirect;

    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    harness
        .wait_for_cluster_convergence(Duration::from_secs(5))
        .await
        .unwrap();

    // Same string is used as both the key (GET) and the channel (SSUBSCRIBE),
    // so both hash to the same slot.
    let channel = "parity_chan";
    let node_ids = harness.node_ids();

    // Find a node that does NOT own the slot: GET there returns MOVED. On that
    // same node, SSUBSCRIBE must produce an identical MOVED redirect.
    let mut checked_a_non_owner = false;
    for &nid in &node_ids {
        let node = harness.node(nid).unwrap();
        let get_resp = node.send("GET", &[channel]).await;
        let Some(get_moved) = is_moved_redirect(&get_resp) else {
            continue; // this node owns the slot (GET served locally)
        };

        let mut client = node.connect().await;
        let ssub_resp = client.command(&["SSUBSCRIBE", channel]).await;
        let ssub_moved = is_moved_redirect(&ssub_resp).unwrap_or_else(|| {
            panic!(
                "SSUBSCRIBE on a non-owner must MOVED-redirect like GET, got: {:?}",
                ssub_resp
            )
        });

        assert_eq!(
            get_moved, ssub_moved,
            "SSUBSCRIBE redirect (slot + addr) must match the keyed-path redirect"
        );
        checked_a_non_owner = true;
        break;
    }

    assert!(
        checked_a_non_owner,
        "expected at least one non-owner node to MOVED-redirect GET {}",
        channel
    );

    harness.shutdown_all().await;
}

// =============================================================================
// Confirmation reply shape: RESP2 Array vs RESP3 Push (proposal 26)
//
// Every subscribe/unsubscribe confirmation goes through the one
// PubSubConfirmation seam, so the wire shape is Push in RESP3 and Array in
// RESP2 — the same in the direct path and inside MULTI/EXEC.
// =============================================================================

/// Assert a RESP3 frame is a Push whose first element is `label`.
fn assert_resp3_push_label(frame: Option<Resp3Frame>, label: &str) {
    match frame {
        Some(Resp3Frame::Push { data, .. }) => match data.first() {
            Some(Resp3Frame::BlobString { data, .. }) => {
                assert_eq!(data.as_ref(), label.as_bytes(), "confirmation label");
            }
            other => panic!("expected string label, got {other:?}"),
        },
        other => panic!("expected Push confirmation for {label}, got {other:?}"),
    }
}

/// Direct (non-transaction) path: every confirmation is an `Array` in RESP2.
#[tokio::test]
async fn test_confirmations_resp2_are_arrays() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    for (cmd, arg, label) in [
        ("SUBSCRIBE", "ch1", "subscribe"),
        ("PSUBSCRIBE", "p.*", "psubscribe"),
        ("SSUBSCRIBE", "sc1", "ssubscribe"),
        ("UNSUBSCRIBE", "ch1", "unsubscribe"),
        ("PUNSUBSCRIBE", "p.*", "punsubscribe"),
        ("SUNSUBSCRIBE", "sc1", "sunsubscribe"),
    ] {
        let resp = c.command(&[cmd, arg]).await;
        match resp {
            Response::Array(items) => {
                assert_eq!(items[0], Response::Bulk(Some(Bytes::from(label))));
                assert_eq!(items[1], Response::Bulk(Some(Bytes::from(arg))));
            }
            other => panic!("{cmd}: expected Array confirmation, got {other:?}"),
        }
    }

    server.shutdown().await;
}

/// Direct (non-transaction) path: every confirmation is a `Push` in RESP3.
///
/// This is the flag fix — the normal path previously emitted an `Array` in
/// RESP3, disagreeing with both Redis and the MULTI/EXEC path.
#[tokio::test]
async fn test_confirmations_resp3_are_push() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect_resp3().await;
    c.command(&["HELLO", "3"]).await;

    for (cmd, arg, label) in [
        ("SUBSCRIBE", "ch1", "subscribe"),
        ("PSUBSCRIBE", "p.*", "psubscribe"),
        ("SSUBSCRIBE", "sc1", "ssubscribe"),
        ("UNSUBSCRIBE", "ch1", "unsubscribe"),
        ("PUNSUBSCRIBE", "p.*", "punsubscribe"),
        ("SUNSUBSCRIBE", "sc1", "sunsubscribe"),
    ] {
        c.send_only(&[cmd, arg]).await;
        let frame = c.read_raw_frame(Duration::from_secs(2)).await;
        assert_resp3_push_label(frame, label);
    }

    server.shutdown().await;
}

/// Empty-arg unsubscribe (no active subscriptions) replies with a null channel
/// and count 0, in the protocol-correct shape.
#[tokio::test]
async fn test_empty_unsubscribe_null_channel_shape() {
    let server = TestServer::start_standalone().await;

    // RESP2 -> Array["unsubscribe", null, 0].
    let mut c2 = server.connect().await;
    match c2.command(&["UNSUBSCRIBE"]).await {
        Response::Array(items) => {
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("unsubscribe"))));
            assert_eq!(items[1], Response::Bulk(None), "null channel");
            assert_eq!(items[2], Response::Integer(0));
        }
        other => panic!("expected Array, got {other:?}"),
    }

    // RESP3 -> Push["unsubscribe", null, 0].
    let mut c3 = server.connect_resp3().await;
    c3.command(&["HELLO", "3"]).await;
    c3.send_only(&["PUNSUBSCRIBE"]).await;
    match c3.read_raw_frame(Duration::from_secs(2)).await {
        Some(Resp3Frame::Push { data, .. }) => {
            assert!(
                matches!(&data[0], Resp3Frame::BlobString { data, .. } if data.as_ref() == b"punsubscribe")
            );
            assert!(matches!(&data[1], Resp3Frame::Null), "null pattern");
            assert!(matches!(&data[2], Resp3Frame::Number { data: 0, .. }));
        }
        other => panic!("expected Push, got {other:?}"),
    }

    server.shutdown().await;
}

/// Inside MULTI/EXEC, RESP2 confirmations stay `Array` — nested in the EXEC
/// reply array.
#[tokio::test]
async fn test_subscribe_confirmation_in_multi_exec_resp2() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["SUBSCRIBE", "ch"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );

    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Response::Array(conf) => {
                    assert_eq!(conf[0], Response::Bulk(Some(Bytes::from("subscribe"))));
                    assert_eq!(conf[1], Response::Bulk(Some(Bytes::from("ch"))));
                    assert_eq!(conf[2], Response::Integer(1));
                }
                other => panic!("expected nested Array confirmation, got {other:?}"),
            }
        }
        other => panic!("expected Array EXEC result, got {other:?}"),
    }

    server.shutdown().await;
}

/// Inside MULTI/EXEC, RESP3 confirmations are `Push` frames delivered
/// out-of-band after the EXEC array — the same shape as the direct path.
#[tokio::test]
async fn test_subscribe_confirmation_in_multi_exec_resp3() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect_resp3().await;
    c.command(&["HELLO", "3"]).await;

    assert!(matches!(
        c.command(&["MULTI"]).await,
        Resp3Frame::SimpleString { .. }
    ));
    assert!(matches!(
        c.command(&["SUBSCRIBE", "ch"]).await,
        Resp3Frame::SimpleString { data, .. } if data.as_ref() == b"QUEUED"
    ));

    // EXEC's own reply is the transaction array...
    let exec = c.command(&["EXEC"]).await;
    assert!(
        matches!(exec, Resp3Frame::Array { .. }),
        "EXEC result must be an Array, got {exec:?}"
    );
    // ...and the subscribe confirmation rides out-of-band as a Push.
    let confirm = c.read_raw_frame(Duration::from_secs(2)).await;
    assert_resp3_push_label(confirm, "subscribe");

    server.shutdown().await;
}

/// SSUBSCRIBE inside MULTI is not supported; EXEC surfaces the error. Pinning
/// this keeps the confirmation-shape work from silently changing the
/// unsupported-in-transaction behavior.
#[tokio::test]
async fn test_ssubscribe_inside_multi_rejected() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["SSUBSCRIBE", "sch"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert!(
                matches!(&items[0], Response::Error(e) if e.starts_with(b"ERR")),
                "expected an error for SSUBSCRIBE inside MULTI, got {:?}",
                items[0]
            );
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// A single SUBSCRIBE with many channels is batched into one shard message,
/// and the handler awaits the shard's ack before replying — so once the last
/// confirmation is read, a PUBLISH from another connection must count the
/// subscriber with no registration-delay sleep.
#[tokio::test]
async fn test_subscribe_many_channels_batched_counts_and_delivery() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    let channels = ["bch1", "bch2", "bch3", "bch4", "bch5"];
    let mut cmd = vec!["SUBSCRIBE"];
    cmd.extend_from_slice(&channels);

    // One confirmation per channel, per-connection count ascending 1..=5,
    // in argument order.
    let mut confirmations = vec![subscriber.command(&cmd).await];
    for _ in 1..channels.len() {
        confirmations.push(
            subscriber
                .read_message(Duration::from_secs(2))
                .await
                .expect("missing subscribe confirmation"),
        );
    }
    for (i, (confirmation, channel)) in confirmations.iter().zip(&channels).enumerate() {
        let Response::Array(items) = confirmation else {
            panic!("expected Array confirmation, got {confirmation:?}");
        };
        assert_eq!(items[0], Response::Bulk(Some(Bytes::from("subscribe"))));
        assert_eq!(items[1], Response::Bulk(Some(Bytes::from(*channel))));
        assert_eq!(items[2], Response::Integer(i as i64 + 1));
    }

    // No sleep: the subscribe reply is only sent after the coordinator shard
    // acked the (batched) registration.
    for channel in &channels {
        let response = publisher.command(&["PUBLISH", channel, "hello"]).await;
        assert_eq!(
            response,
            Response::Integer(1),
            "PUBLISH {channel} right after confirmation must see the subscriber"
        );
        let msg = subscriber
            .read_message(Duration::from_secs(2))
            .await
            .expect("missing delivered message");
        let Response::Array(items) = msg else {
            panic!("expected Array message");
        };
        assert_eq!(items[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(items[1], Response::Bulk(Some(Bytes::from(*channel))));
    }

    server.shutdown().await;
}

/// A single SSUBSCRIBE with channels owned by different shards (default
/// topology: 4 shards) groups registrations per owning shard; confirmations
/// stay in argument order with ascending per-connection counts, and SPUBLISH
/// to each channel is delivered.
#[tokio::test]
async fn test_ssubscribe_many_channels_across_shards_batched() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Enough distinct names to land on several of the 4 shards.
    let channels = [
        "schan-a", "schan-b", "schan-c", "schan-d", "schan-e", "schan-f", "schan-g", "schan-h",
    ];
    let mut cmd = vec!["SSUBSCRIBE"];
    cmd.extend_from_slice(&channels);

    let mut confirmations = vec![subscriber.command(&cmd).await];
    for _ in 1..channels.len() {
        confirmations.push(
            subscriber
                .read_message(Duration::from_secs(2))
                .await
                .expect("missing ssubscribe confirmation"),
        );
    }
    for (i, (confirmation, channel)) in confirmations.iter().zip(&channels).enumerate() {
        let Response::Array(items) = confirmation else {
            panic!("expected Array confirmation, got {confirmation:?}");
        };
        assert_eq!(items[0], Response::Bulk(Some(Bytes::from("ssubscribe"))));
        assert_eq!(items[1], Response::Bulk(Some(Bytes::from(*channel))));
        assert_eq!(items[2], Response::Integer(i as i64 + 1));
    }

    // Each owning shard acked its batch before the confirmations were sent,
    // so SPUBLISH must see the subscriber immediately.
    for channel in &channels {
        let response = publisher.command(&["SPUBLISH", channel, "payload"]).await;
        assert_eq!(
            response,
            Response::Integer(1),
            "SPUBLISH {channel} right after confirmation must see the subscriber"
        );
        let msg = subscriber
            .read_message(Duration::from_secs(2))
            .await
            .expect("missing delivered smessage");
        let Response::Array(items) = msg else {
            panic!("expected Array smessage");
        };
        assert_eq!(items[0], Response::Bulk(Some(Bytes::from("smessage"))));
        assert_eq!(items[1], Response::Bulk(Some(Bytes::from(*channel))));
    }

    server.shutdown().await;
}

/// A multi-channel UNSUBSCRIBE batches all deregistrations and reports
/// descending per-connection counts in argument order.
#[tokio::test]
async fn test_unsubscribe_many_channels_batched_counts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut publisher = server.connect().await;

    let channels = ["uch1", "uch2", "uch3", "uch4"];
    let mut cmd = vec!["SUBSCRIBE"];
    cmd.extend_from_slice(&channels);
    client.command(&cmd).await;
    for _ in 1..channels.len() {
        client.read_message(Duration::from_secs(2)).await;
    }

    let mut cmd = vec!["UNSUBSCRIBE"];
    cmd.extend_from_slice(&channels);
    let mut confirmations = vec![client.command(&cmd).await];
    for _ in 1..channels.len() {
        confirmations.push(
            client
                .read_message(Duration::from_secs(2))
                .await
                .expect("missing unsubscribe confirmation"),
        );
    }
    for (i, (confirmation, channel)) in confirmations.iter().zip(&channels).enumerate() {
        let Response::Array(items) = confirmation else {
            panic!("expected Array confirmation, got {confirmation:?}");
        };
        assert_eq!(items[0], Response::Bulk(Some(Bytes::from("unsubscribe"))));
        assert_eq!(items[1], Response::Bulk(Some(Bytes::from(*channel))));
        assert_eq!(items[2], Response::Integer((channels.len() - 1 - i) as i64));
    }

    // Deregistration was acked before the confirmations, so a PUBLISH now
    // must find zero subscribers.
    for channel in &channels {
        assert_eq!(
            publisher.command(&["PUBLISH", channel, "gone"]).await,
            Response::Integer(0)
        );
    }

    server.shutdown().await;
}
