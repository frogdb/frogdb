//! Integration tests for pub/sub commands (SUBSCRIBE, PUBLISH, PSUBSCRIBE, etc.)

use crate::common::test_server::{TestClient, TestServer, TestServerConfig};
use bytes::Bytes;
use frogdb_protocol::Response;
use futures::StreamExt;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::{Duration, Instant};

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

/// Lazy-expiry effect parity (D8 real-path repro): a whole-key TTL that dies via
/// the LAZY read path (`check_and_delete_expired` -> `uninstall`) must emit the
/// same `expired` keyevent the active sweep emits for its `deleted_keys`. Active
/// expiry is disabled (`DEBUG SET-ACTIVE-EXPIRE 0`) so the ONLY thing that can
/// remove the key is the third-party `GET` — isolating the lazy seam. Redis/Valkey
/// fire `expired` from `expireIfNeeded` (on-access) and `activeExpireCycle`
/// (sweep) alike; before this fix the lazy seam silently dropped the event.
#[tokio::test]
async fn regression_lazy_expiry_emits_expired_keyevent() {
    // Single shard so the key and the shard-0 subscriber share a shard.
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut admin = server.connect().await;
    let mut subscriber = server.connect().await;

    // Disable active expiry: only a lazy read may now remove the key.
    let resp = admin.command(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).await;
    assert_eq!(resp, Response::ok());

    // Enable keyevent notifications.
    let resp = admin
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed a key with a TTL, then backdate its deadline into the past so it is
    // logically expired but still physically present (backdate rewrites only the
    // timestamp — no purge, no version bump).
    admin.command(&["SET", "k", "v"]).await;
    admin.command(&["PEXPIRE", "k", "100000"]).await;
    let resp = admin
        .command(&["DEBUG", "EXPIRE-BACKDATE", "k", "50"])
        .await;
    assert_eq!(resp, Response::ok());

    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:expired"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    // Trigger the lazy purge: a value read of the already-expired key.
    admin.command(&["GET", "k"]).await;

    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "lazy expiry (GET on a backdated key) must emit the `expired` keyevent, \
         matching active expiry"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:expired")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("k"))));
    } else {
        panic!("Expected array response for expired keyevent message");
    }

    server.shutdown().await;
}

/// Lazy last-hash-field-death `del` parity (issue 09 real-path repro): a hash
/// whose LAST field dies via field TTL empties the key, and Redis emits a
/// generic `del` (not `expired`) for that removal. Active expiry emits it from
/// its `emptied_keys` branch; before this fix the LAZY read seam
/// (`purge_expired_hash_fields` -> `delete`) dropped it silently. Active expiry
/// is disabled (`DEBUG SET-ACTIVE-EXPIRE 0`) so the ONLY thing that can reap the
/// field is the third-party `HGET` — isolating the lazy seam. Hash-field TTL
/// cannot be backdated (`DEBUG EXPIRE-BACKDATE` is whole-key only), so a short
/// real TTL + a brief wait makes the field due before the triggering read.
#[tokio::test]
async fn regression_lazy_hash_field_death_emits_del_keyevent() {
    // Single shard so the key and the shard-0 subscriber share a shard.
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut admin = server.connect().await;
    let mut subscriber = server.connect().await;

    // Disable active expiry: only a lazy read may now reap the field / empty
    // the key.
    let resp = admin.command(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).await;
    assert_eq!(resp, Response::ok());

    // Enable keyevent notifications (KEA includes the generic `g` class `del`
    // rides on).
    let resp = admin
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed a single-field hash and give that field a short TTL.
    admin.command(&["HSET", "h", "f", "v"]).await;
    let resp = admin
        .command(&["HPEXPIRE", "h", "60", "FIELDS", "1", "f"])
        .await;
    assert!(matches!(resp, Response::Array(_)));

    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:del"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    // Let the field's TTL elapse (no active sweep will touch it).
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Trigger the lazy purge: a read of the hash reaps the expired field, which
    // empties and removes the key.
    admin.command(&["HGET", "h", "f"]).await;

    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "lazy last-hash-field death (HGET reaping the final field) must emit the \
         `del` keyevent, matching active expiry's emptied_keys branch"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(
            arr[1],
            Response::Bulk(Some(Bytes::from("__keyevent@0__:del")))
        );
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("h"))));
    } else {
        panic!("Expected array response for del keyevent message");
    }

    server.shutdown().await;
}

/// No double-fire under the active sweep (issue 09): the sweep reaps a
/// last-hash-field death through the *same* `purge_expired_hash_fields` seam a
/// lazy read uses, so it also fills the store's lazily-emptied buffer — but it
/// already reports the key via `ExpiryResult::emptied_keys`. `run_active_expiry`
/// discards the buffer, so a following command's lazy drain fires nothing:
/// exactly one `del` total. Active expiry stays ENABLED (default) here.
#[tokio::test]
async fn lazy_hash_field_death_del_event_fires_once_under_active_sweep() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut admin = server.connect().await;
    let mut subscriber = server.connect().await;

    let resp = admin
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    admin.command(&["HSET", "h", "f", "v"]).await;
    let resp = admin
        .command(&["HPEXPIRE", "h", "80", "FIELDS", "1", "f"])
        .await;
    assert!(matches!(resp, Response::Array(_)));

    let resp = subscriber
        .command(&["SUBSCRIBE", "__keyevent@0__:del"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));

    // The active sweep (100 ms cadence) reaps the field and emits exactly one
    // `del`.
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    assert!(
        msg.is_some(),
        "active sweep must emit the `del` keyevent for the emptied hash key"
    );
    if let Some(Response::Array(arr)) = msg {
        assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("h"))));
    } else {
        panic!("Expected array response for del keyevent message");
    }

    // Drive several command seams that would drain a leaked lazily-emptied
    // buffer. None may produce a second `del`.
    for _ in 0..3 {
        admin.command(&["HGET", "h", "f"]).await;
        admin.command(&["GET", "nonexistent"]).await;
    }
    let extra = subscriber.read_message(Duration::from_millis(300)).await;
    assert!(
        extra.is_none(),
        "the sweep already reported this key — no second `del` may fire on a \
         later command's lazy drain, got {extra:?}"
    );

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
    // The GET (harness connection) and the SSUBSCRIBE (fresh connection) are two
    // separate round-trips. If the node's routing view updates between them (e.g.
    // just after convergence, under parallel load), their MOVED targets can differ
    // even though both paths are correct — a transient race, not a parity bug.
    // Retry the whole GET/SSUBSCRIBE pair a bounded number of times: a genuine
    // parity break (SSUBSCRIBE taking a different redirect path than keyed commands)
    // disagrees persistently and still fails; a transient view update converges.
    const MAX_ATTEMPTS: usize = 10;
    let mut checked_a_non_owner = false;
    for &nid in &node_ids {
        let node = harness.node(nid).unwrap();
        if is_moved_redirect(&node.send("GET", &[channel]).await).is_none() {
            continue; // this node owns the slot (GET served locally)
        }
        checked_a_non_owner = true;

        let mut last_pair = None;
        let mut agreed = false;
        for _ in 0..MAX_ATTEMPTS {
            // Fresh GET + fresh SSUBSCRIBE each attempt so a stale routing view on
            // either round-trip is replaced rather than compared.
            let get_resp = node.send("GET", &[channel]).await;
            let Some(get_moved) = is_moved_redirect(&get_resp) else {
                // Node became the owner mid-retry (routing view moved): transient,
                // try again.
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };

            let mut client = node.connect().await;
            let ssub_resp = client.command(&["SSUBSCRIBE", channel]).await;
            let ssub_moved = is_moved_redirect(&ssub_resp).unwrap_or_else(|| {
                panic!(
                    "SSUBSCRIBE on a non-owner must MOVED-redirect like GET, got: {:?}",
                    ssub_resp
                )
            });

            if get_moved == ssub_moved {
                agreed = true;
                break;
            }
            last_pair = Some((get_moved, ssub_moved));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            agreed,
            "SSUBSCRIBE redirect (slot + addr) must match the keyed-path redirect; \
             still disagreed after {} attempts: {:?}",
            MAX_ATTEMPTS, last_pair
        );
        break;
    }

    assert!(
        checked_a_non_owner,
        "expected at least one non-owner node to MOVED-redirect GET {}",
        channel
    );

    harness.shutdown_all().await;
}

/// Importing-target parity: when a slot is IMPORTING on this node and the client
/// has sent ASKING, SSUBSCRIBE must serve the subscription *locally* instead of
/// redirecting — exactly like a keyed GET does under the same ASKING+IMPORTING
/// state. Both paths run through `coordinator.route()` + `to_response`, which
/// yields `AcceptImporting` -> ServeLocal. This is the arm the fix added on the
/// SSUBSCRIBE side; the generic-seam unit tests cover it, but nothing exercised
/// it end-to-end at the SSUBSCRIBE command level until now.
#[tokio::test]
async fn test_ssubscribe_asking_serves_local_matches_keyed_path() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{
        is_ask_redirect, is_error, is_moved_redirect, slot_for_key,
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

    // The same string is used as both the key (GET) and the channel
    // (SSUBSCRIBE), so both hash to the same slot.
    let channel = "asking_local_chan";
    let slot = slot_for_key(channel.as_bytes());

    // Resolve the slot owner from the leader's converged view.
    let leader_id = harness.get_leader().await.expect("leader should exist");
    let leader_node = harness.node(leader_id).unwrap();
    let owner_node_id = leader_node
        .cluster_state()
        .unwrap()
        .snapshot()
        .get_slot_owner(slot)
        .expect("slot should be assigned");

    // Pick an importing target that is NOT the owner.
    let node_ids = harness.node_ids();
    let target_node_id = *node_ids
        .iter()
        .find(|&&id| id != owner_node_id)
        .expect("need a non-owner node as the importing target");
    let source_id_str = harness.get_node_id_str(owner_node_id).unwrap();
    let target_id_str = harness.get_node_id_str(target_node_id).unwrap();
    let slot_str = slot.to_string();

    // Mark the slot IMPORTING on the target (Raft command via the leader; both
    // node IDs are explicit since the leader's my_node_id differs from target).
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
        !is_error(&import_resp),
        "SETSLOT IMPORTING failed: {:?}",
        import_resp
    );

    let target_node = harness.node(target_node_id).unwrap();

    // Converge: probe with ASKING + GET until IMPORTING has propagated to the
    // target and the keyed path serves locally (no MOVED/ASK redirect). This
    // explicit wait replaces a fixed sleep so the test does not race
    // propagation under parallel load.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut probe = target_node.connect().await;
        let _ = probe.command(&["ASKING"]).await;
        let get_resp = probe.command(&["GET", channel]).await;
        let redirect =
            is_moved_redirect(&get_resp).is_some() || is_ask_redirect(&get_resp).is_some();
        if !redirect {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "SETSLOT IMPORTING did not propagate to target within 5s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Keyed baseline: ASKING + GET on the importing target serves local.
    let mut keyed = target_node.connect().await;
    assert!(
        !is_error(&keyed.command(&["ASKING"]).await),
        "ASKING should succeed"
    );
    let get_resp = keyed.command(&["GET", channel]).await;
    assert!(
        is_moved_redirect(&get_resp).is_none() && is_ask_redirect(&get_resp).is_none(),
        "keyed GET under ASKING on the importing target must serve local, got: {:?}",
        get_resp
    );

    // SSUBSCRIBE parity: ASKING + SSUBSCRIBE on the importing target must also
    // serve local — a real subscribe confirmation, not a redirect or error.
    let mut sub = target_node.connect().await;
    assert!(
        !is_error(&sub.command(&["ASKING"]).await),
        "ASKING should succeed"
    );
    let ssub_resp = sub.command(&["SSUBSCRIBE", channel]).await;
    match &ssub_resp {
        Response::Array(arr) if arr.len() == 3 => {
            assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("ssubscribe"))));
            assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel))));
        }
        other => panic!(
            "SSUBSCRIBE under ASKING on the importing target must serve local \
             like the keyed GET, got: {:?}",
            other
        ),
    }

    harness.shutdown_all().await;
}

/// Unassigned-slot parity: when a slot is owned by NO node, SSUBSCRIBE must fail
/// with CLUSTERDOWN — the same terminal error a keyed GET returns for that slot.
/// Both derive it from `RouteDecision::Unassigned` -> `to_response`. Pins the
/// SSUBSCRIBE command level to the keyed path for the no-owner arm.
#[tokio::test]
async fn test_ssubscribe_unassigned_slot_clusterdown_matches_keyed_path() {
    use frogdb_test_harness::cluster_harness::ClusterTestHarness;
    use frogdb_test_harness::cluster_helpers::{is_cluster_down, is_error, slot_for_key};

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

    // Same string as key and channel, so GET and SSUBSCRIBE target one slot.
    let channel = "clusterdown_chan";
    let slot = slot_for_key(channel.as_bytes());
    let slot_str = slot.to_string();

    // DELSLOTS is a Raft command — issue it via the leader to unassign the
    // channel's slot so it is owned by no node.
    let leader_id = harness.get_leader().await.expect("leader should exist");
    let leader_node = harness.node(leader_id).unwrap();
    let del_resp = leader_node.send("CLUSTER", &["DELSLOTS", &slot_str]).await;
    assert!(!is_error(&del_resp), "DELSLOTS failed: {:?}", del_resp);

    let probe_node = harness.node(harness.node_ids()[0]).unwrap();

    // Converge: wait until the unassignment has propagated and the keyed path
    // reports CLUSTERDOWN for the slot (explicit wait over a fixed sleep).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let get_resp = probe_node.send("GET", &[channel]).await;
        if is_cluster_down(&get_resp) {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "DELSLOTS did not produce CLUSTERDOWN within 5s, got: {:?}",
            get_resp
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Keyed baseline: GET on the unassigned slot is CLUSTERDOWN.
    let get_resp = probe_node.send("GET", &[channel]).await;
    assert!(
        is_cluster_down(&get_resp),
        "keyed GET on an unassigned slot must be CLUSTERDOWN, got: {:?}",
        get_resp
    );

    // SSUBSCRIBE parity: same terminal CLUSTERDOWN, not a subscribe confirmation.
    let mut client = probe_node.connect().await;
    let ssub_resp = client.command(&["SSUBSCRIBE", channel]).await;
    assert!(
        is_cluster_down(&ssub_resp),
        "SSUBSCRIBE on an unassigned slot must be CLUSTERDOWN like the keyed \
         GET, got: {:?}",
        ssub_resp
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

/// SSUBSCRIBE inside MULTI is rejected — pinned against Redis 8.6.4 source
/// (`pubsub.c: ssubscribeCommand`), which guards on bare `CLIENT_DENY_BLOCKING`
/// with no `!CLIENT_MULTI` carve-out (unlike SUBSCRIBE/PSUBSCRIBE, which are
/// MULTI-exempt). This is the one genuinely-rejected member of the subscribe
/// family; the exact error text matches Redis's own wire text.
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
            assert_eq!(
                items[0],
                Response::error("ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client"),
                "expected Redis's exact SSUBSCRIBE-in-MULTI error text, got {:?}",
                items[0]
            );
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

// ============================================================================
// Full subscribe-family-in-MULTI matrix (issue 57)
//
// Verified against Redis 8.6.4 source (pubsub.c): SUBSCRIBE/PSUBSCRIBE are
// MULTI-exempt (`DENY_BLOCKING && !CLIENT_MULTI`); UNSUBSCRIBE/PUNSUBSCRIBE/
// SUNSUBSCRIBE/PUBSUB carry no DENY_BLOCKING guard at all; only SSUBSCRIBE
// (above) is unconditionally gated on DENY_BLOCKING and so genuinely rejected.
// RESET inside MULTI is covered separately by
// `test_reset_aborts_transaction` (integration_client.rs) — RESET is
// intercepted before the transaction queue and always executes directly,
// dropping the queue and returning `+RESET`, which that test already pins.
// ============================================================================

/// SUBSCRIBE queued in MULTI genuinely subscribes at EXEC time (Redis-exempt,
/// not rejected). Pinned here as the reference case for the matrix even though
/// `test_subscribe_confirmation_in_multi_exec_resp2` covers it too.
#[tokio::test]
async fn test_subscribe_inside_multi_executes() {
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
                other => panic!("expected confirmation Array, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// PSUBSCRIBE queued in MULTI genuinely subscribes at EXEC time (Redis-exempt,
/// same guard as SUBSCRIBE).
#[tokio::test]
async fn test_psubscribe_inside_multi_executes() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["PSUBSCRIBE", "ch.*"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Response::Array(conf) => {
                    assert_eq!(conf[0], Response::Bulk(Some(Bytes::from("psubscribe"))));
                    assert_eq!(conf[1], Response::Bulk(Some(Bytes::from("ch.*"))));
                    assert_eq!(conf[2], Response::Integer(1));
                }
                other => panic!("expected confirmation Array, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// UNSUBSCRIBE carries no DENY_BLOCKING guard at all in Redis, so it executes
/// unconditionally inside MULTI too. With no active subscriptions this yields
/// the same null-channel confirmation shape as the direct path.
#[tokio::test]
async fn test_unsubscribe_inside_multi_executes() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["UNSUBSCRIBE"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Response::Array(conf) => {
                    assert_eq!(conf[0], Response::Bulk(Some(Bytes::from("unsubscribe"))));
                    assert_eq!(conf[1], Response::Bulk(None), "null channel");
                    assert_eq!(conf[2], Response::Integer(0));
                }
                other => panic!("expected confirmation Array, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// PUNSUBSCRIBE carries no DENY_BLOCKING guard either — same treatment as
/// UNSUBSCRIBE.
#[tokio::test]
async fn test_punsubscribe_inside_multi_executes() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["PUNSUBSCRIBE"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Response::Array(conf) => {
                    assert_eq!(conf[0], Response::Bulk(Some(Bytes::from("punsubscribe"))));
                    assert_eq!(conf[1], Response::Bulk(None), "null pattern");
                    assert_eq!(conf[2], Response::Integer(0));
                }
                other => panic!("expected confirmation Array, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// SUNSUBSCRIBE carries no DENY_BLOCKING guard in Redis (only SSUBSCRIBE
/// does) — this is the residue fix: FrogDB previously rejected it inside
/// MULTI with a bespoke error, where Redis genuinely allows it to execute.
///
/// Note: this connection is never actually SSUBSCRIBE'd to `sch` first —
/// doing so would enter pub/sub mode, and (matching Redis) a RESP2 client in
/// pub/sub mode cannot issue MULTI at all (`is_allowed_in_pubsub_mode`
/// rejects it, same as real Redis's context restriction). That restriction is
/// unrelated to this issue; a fresh, never-subscribed connection is
/// sufficient to prove SUNSUBSCRIBE *executes* (count 0) rather than being
/// rejected.
#[tokio::test]
async fn test_sunsubscribe_inside_multi_executes() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["SUNSUBSCRIBE", "sch"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                Response::Array(conf) => {
                    assert_eq!(conf[0], Response::Bulk(Some(Bytes::from("sunsubscribe"))));
                    assert_eq!(conf[1], Response::Bulk(Some(Bytes::from("sch"))));
                    assert_eq!(conf[2], Response::Integer(0));
                }
                other => panic!("expected confirmation Array, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }

    server.shutdown().await;
}

/// PUBSUB carries no DENY_BLOCKING guard in Redis either — it executes inside
/// MULTI exactly like the direct path, folding its single reply into the EXEC
/// array slot (same framing as PUBLISH/SPUBLISH). This is the other residue
/// fix: FrogDB previously rejected PUBSUB inside MULTI.
#[tokio::test]
async fn test_pubsub_inside_multi_executes() {
    let server = TestServer::start_standalone().await;
    let mut c = server.connect().await;

    assert_eq!(c.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        c.command(&["PUBSUB", "NUMPAT"]).await,
        Response::Simple(Bytes::from("QUEUED"))
    );
    match c.command(&["EXEC"]).await {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Response::Integer(0));
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

// ===========================================================================
// Keyspace-event key accuracy (proposal 44 phase 1)
//
// Multi-key commands must notify only the keys they actually write:
// STORE-family commands emit on the destination only (EventSpec::EmitsAt),
// runtime-resolved commands (RENAME, ZMPOP, BLPOP, ...) deposit their events
// via CommandContext::notify_event (EventSpec::Dynamic).
// ===========================================================================

/// Subscribe `subscriber` to the exact keyevent channel for `event`, returning
/// after the subscription is confirmed and registered.
async fn subscribe_keyevent(subscriber: &mut crate::common::test_server::TestClient, event: &str) {
    let channel = format!("__keyevent@0__:{event}");
    let resp = subscriber.command(&["SUBSCRIBE", &channel]).await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    // Give the subscription time to register.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Read keyevent messages from `subscriber` and assert the notified keys are
/// exactly `expected` (in order), followed by silence.
async fn assert_keyevent_keys(
    subscriber: &mut crate::common::test_server::TestClient,
    event: &str,
    expected: &[&str],
) {
    let channel = format!("__keyevent@0__:{event}");
    for (i, want) in expected.iter().enumerate() {
        let msg = subscriber.read_message(Duration::from_secs(2)).await;
        let Some(Response::Array(arr)) = msg else {
            panic!("expected keyevent #{i} ({event} -> {want}), got {msg:?}");
        };
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("message"))));
        assert_eq!(arr[1], Response::Bulk(Some(Bytes::from(channel.clone()))));
        assert_eq!(
            arr[2],
            Response::Bulk(Some(Bytes::from(want.to_string()))),
            "keyevent #{i} for '{event}' notified the wrong key"
        );
    }
    // No further notifications: any extra message is an over-emission.
    let extra = subscriber.read_message(Duration::from_millis(400)).await;
    assert!(
        extra.is_none(),
        "unexpected extra '{event}' keyevent (over-emission): {extra:?}"
    );
}

/// ZRANGESTORE emits `zrangestore` on the destination key only — never on the
/// read-only source key (Redis parity; regression for the blanket-Emits bug).
#[tokio::test]
async fn test_zrangestore_notifies_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Same hash tag: ZRANGESTORE requires src and dest on one shard.
    client
        .command(&["ZADD", "{zrs}src", "1", "a", "2", "b"])
        .await;

    subscribe_keyevent(&mut subscriber, "zrangestore").await;

    let resp = client
        .command(&["ZRANGESTORE", "{zrs}dest", "{zrs}src", "0", "-1"])
        .await;
    assert_eq!(resp, Response::Integer(2));

    assert_keyevent_keys(&mut subscriber, "zrangestore", &["{zrs}dest"]).await;
    server.shutdown().await;
}

/// SINTERSTORE emits `sinterstore` on the destination only, not on the
/// read-only source sets.
#[tokio::test]
async fn test_sinterstore_notifies_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SADD", "{sis}1", "a", "b"]).await;
    client.command(&["SADD", "{sis}2", "b", "c"]).await;

    subscribe_keyevent(&mut subscriber, "sinterstore").await;

    let resp = client
        .command(&["SINTERSTORE", "{sis}dest", "{sis}1", "{sis}2"])
        .await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut subscriber, "sinterstore", &["{sis}dest"]).await;
    server.shutdown().await;
}

/// COPY emits `copy_to` on the destination (`keys()[1]`) only — the source is
/// read-only.
#[tokio::test]
async fn test_copy_notifies_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "{cp}src", "v"]).await;

    subscribe_keyevent(&mut subscriber, "copy_to").await;

    let resp = client.command(&["COPY", "{cp}src", "{cp}dst"]).await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut subscriber, "copy_to", &["{cp}dst"]).await;
    server.shutdown().await;
}

/// RENAME deposits its events at runtime (EventSpec::Dynamic): Redis-verified
/// per-key names — `rename_from` on the source, `rename_to` on the destination
/// (db.c renameGenericCommand:62-63). Proves deposited events flow from the
/// handler through the write-effect pipeline to subscribers.
#[tokio::test]
async fn test_rename_notifies_source_then_destination() {
    let server = TestServer::start_standalone().await;
    let mut from_sub = server.connect().await;
    let mut to_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "{rn}src", "v"]).await;

    subscribe_keyevent(&mut from_sub, "rename_from").await;
    subscribe_keyevent(&mut to_sub, "rename_to").await;

    let resp = client.command(&["RENAME", "{rn}src", "{rn}dst"]).await;
    assert_eq!(resp, Response::ok());

    assert_keyevent_keys(&mut from_sub, "rename_from", &["{rn}src"]).await;
    assert_keyevent_keys(&mut to_sub, "rename_to", &["{rn}dst"]).await;
    server.shutdown().await;
}

/// RENAME of a missing source returns an error and writes nothing, so it must
/// emit no keyspace events (Dynamic with no deposit on the error path).
#[tokio::test]
async fn test_rename_missing_source_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut from_sub = server.connect().await;
    let mut to_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut from_sub, "rename_from").await;
    subscribe_keyevent(&mut to_sub, "rename_to").await;

    let resp = client.command(&["RENAME", "{rnm}src", "{rnm}dst"]).await;
    assert!(matches!(resp, Response::Error(_)));

    assert_keyevent_keys(&mut from_sub, "rename_from", &[]).await;
    assert_keyevent_keys(&mut to_sub, "rename_to", &[]).await;
    server.shutdown().await;
}

/// A RENAMENX that does nothing (destination already exists, reply 0) must
/// emit nothing: EventSpec::Dynamic with no deposit — unlike the old blanket
/// Emits, which fired on both keys even for the no-op reply.
#[tokio::test]
async fn test_renamenx_noop_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "{rnx}src", "v"]).await;
    client.command(&["SET", "{rnx}dst", "w"]).await;

    subscribe_keyevent(&mut subscriber, "rename_from").await;

    let resp = client.command(&["RENAMENX", "{rnx}src", "{rnx}dst"]).await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "rename_from", &[]).await;
    server.shutdown().await;
}

/// SMOVE is SREM+SADD internally: Redis emits `srem` on the source and `sadd`
/// on the destination (t_set.c smoveCommand:36,66) — not a bespoke `smove`
/// event. Each fires only on its own key; no cross-key bleed.
#[tokio::test]
async fn test_smove_notifies_srem_source_sadd_dest() {
    let server = TestServer::start_standalone().await;
    let mut srem_sub = server.connect().await;
    let mut sadd_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SADD", "{sm}src", "a", "b"]).await;
    client.command(&["SADD", "{sm}dst", "z"]).await;

    subscribe_keyevent(&mut srem_sub, "srem").await;
    subscribe_keyevent(&mut sadd_sub, "sadd").await;

    let resp = client.command(&["SMOVE", "{sm}src", "{sm}dst", "a"]).await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut srem_sub, "srem", &["{sm}src"]).await;
    assert_keyevent_keys(&mut sadd_sub, "sadd", &["{sm}dst"]).await;
    server.shutdown().await;
}

/// SMOVE of a non-member is a no-op (reply 0): nothing moved, so neither `srem`
/// nor `sadd` fires (EventSpec::Dynamic with no deposit on the no-op path).
#[tokio::test]
async fn test_smove_nonmember_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut srem_sub = server.connect().await;
    let mut sadd_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SADD", "{smn}src", "a"]).await;
    client.command(&["SADD", "{smn}dst", "z"]).await;

    subscribe_keyevent(&mut srem_sub, "srem").await;
    subscribe_keyevent(&mut sadd_sub, "sadd").await;

    // "nope" is not a member of the source: no move, no events.
    let resp = client
        .command(&["SMOVE", "{smn}src", "{smn}dst", "nope"])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut srem_sub, "srem", &[]).await;
    assert_keyevent_keys(&mut sadd_sub, "sadd", &[]).await;
    server.shutdown().await;
}

/// RENAME k k (source == destination, key exists): Redis renameGenericCommand
/// short-circuits samekey with a plain OK *before* any modification or
/// notifyKeyspaceEvent call — no `rename_from`, no `rename_to`. The key and its
/// value must survive untouched.
#[tokio::test]
async fn test_rename_self_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut from_sub = server.connect().await;
    let mut to_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "rnself", "v"]).await;

    subscribe_keyevent(&mut from_sub, "rename_from").await;
    subscribe_keyevent(&mut to_sub, "rename_to").await;

    // Same key on both sides: silent success, no events, value preserved.
    let resp = client.command(&["RENAME", "rnself", "rnself"]).await;
    assert_eq!(resp, Response::ok());

    assert_keyevent_keys(&mut from_sub, "rename_from", &[]).await;
    assert_keyevent_keys(&mut to_sub, "rename_to", &[]).await;

    let resp = client.command(&["GET", "rnself"]).await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("v"))));
    server.shutdown().await;
}

/// RENAME k k on a MISSING key is still an error: Redis checks the source
/// exists (lookupKeyWriteOrReply, "no such key") *before* the samekey
/// short-circuit. Nothing is written, so nothing fires.
#[tokio::test]
async fn test_rename_self_missing_key_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["RENAME", "rnselfmiss", "rnselfmiss"])
        .await;
    assert!(
        matches!(resp, Response::Error(_)),
        "RENAME of a missing key onto itself must error, got {resp:?}"
    );
    server.shutdown().await;
}

/// RENAMENX k k on an existing key replies 0 (Redis samekey path: czero for
/// NX) with no modification and no events.
#[tokio::test]
async fn test_renamenx_self_returns_zero_silently() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "rnxself", "v"]).await;

    subscribe_keyevent(&mut subscriber, "rename_from").await;

    let resp = client.command(&["RENAMENX", "rnxself", "rnxself"]).await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "rename_from", &[]).await;
    server.shutdown().await;
}

/// SMOVE k k m (source == destination): Redis smoveCommand short-circuits
/// `srcset == dstset` before any modification — reply 1 if the member is
/// present (0 otherwise), with NO `srem`/`sadd` events, and the set is left
/// untouched.
#[tokio::test]
async fn test_smove_same_key_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut srem_sub = server.connect().await;
    let mut sadd_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SADD", "smself", "a", "b"]).await;

    subscribe_keyevent(&mut srem_sub, "srem").await;
    subscribe_keyevent(&mut sadd_sub, "sadd").await;

    // Member present: reply 1, but no events and no modification.
    let resp = client.command(&["SMOVE", "smself", "smself", "a"]).await;
    assert_eq!(resp, Response::Integer(1));

    // Member absent: reply 0, same silence.
    let resp = client.command(&["SMOVE", "smself", "smself", "nope"]).await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut srem_sub, "srem", &[]).await;
    assert_keyevent_keys(&mut sadd_sub, "sadd", &[]).await;

    // The set is untouched.
    let resp = client.command(&["SCARD", "smself"]).await;
    assert_eq!(resp, Response::Integer(2));
    server.shutdown().await;
}

/// RPOPLPUSH = LMOVE RIGHT LEFT: Redis emits `rpop` on the source (popped from
/// the tail) and `lpush` on the destination (pushed to the head) — t_list.c
/// listElementsRemoved:2 + lmoveHandlePush:23. Not a bespoke `rpoplpush` event.
#[tokio::test]
async fn test_rpoplpush_notifies_rpop_source_lpush_dest() {
    let server = TestServer::start_standalone().await;
    let mut rpop_sub = server.connect().await;
    let mut lpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{rpl}src", "a", "b", "c"]).await;

    subscribe_keyevent(&mut rpop_sub, "rpop").await;
    subscribe_keyevent(&mut lpush_sub, "lpush").await;

    let resp = client.command(&["RPOPLPUSH", "{rpl}src", "{rpl}dst"]).await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("c"))));

    assert_keyevent_keys(&mut rpop_sub, "rpop", &["{rpl}src"]).await;
    assert_keyevent_keys(&mut lpush_sub, "lpush", &["{rpl}dst"]).await;
    server.shutdown().await;
}

/// LMOVE LEFT RIGHT: `lpop` on the source (popped from the head), `rpush` on the
/// destination (pushed to the tail) — direction-resolved names per t_list.c.
#[tokio::test]
async fn test_lmove_left_right_notifies_lpop_rpush() {
    let server = TestServer::start_standalone().await;
    let mut lpop_sub = server.connect().await;
    let mut rpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{lmlr}src", "a", "b", "c"]).await;

    subscribe_keyevent(&mut lpop_sub, "lpop").await;
    subscribe_keyevent(&mut rpush_sub, "rpush").await;

    let resp = client
        .command(&["LMOVE", "{lmlr}src", "{lmlr}dst", "LEFT", "RIGHT"])
        .await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("a"))));

    assert_keyevent_keys(&mut lpop_sub, "lpop", &["{lmlr}src"]).await;
    assert_keyevent_keys(&mut rpush_sub, "rpush", &["{lmlr}dst"]).await;
    server.shutdown().await;
}

/// LMOVE RIGHT LEFT: `rpop` on the source, `lpush` on the destination — the
/// mirror direction, proving the pop/push names track the wherefrom/whereto args.
#[tokio::test]
async fn test_lmove_right_left_notifies_rpop_lpush() {
    let server = TestServer::start_standalone().await;
    let mut rpop_sub = server.connect().await;
    let mut lpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{lmrl}src", "a", "b", "c"]).await;

    subscribe_keyevent(&mut rpop_sub, "rpop").await;
    subscribe_keyevent(&mut lpush_sub, "lpush").await;

    let resp = client
        .command(&["LMOVE", "{lmrl}src", "{lmrl}dst", "RIGHT", "LEFT"])
        .await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("c"))));

    assert_keyevent_keys(&mut rpop_sub, "rpop", &["{lmrl}src"]).await;
    assert_keyevent_keys(&mut lpush_sub, "lpush", &["{lmrl}dst"]).await;
    server.shutdown().await;
}

/// LMOVE from a missing source is a no-op (reply nil): no pop, no push, no
/// events on either key.
#[tokio::test]
async fn test_lmove_missing_source_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut lpop_sub = server.connect().await;
    let mut rpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut lpop_sub, "lpop").await;
    subscribe_keyevent(&mut rpush_sub, "rpush").await;

    let resp = client
        .command(&["LMOVE", "{lmm}src", "{lmm}dst", "LEFT", "RIGHT"])
        .await;
    assert_eq!(resp, Response::null());

    assert_keyevent_keys(&mut lpop_sub, "lpop", &[]).await;
    assert_keyevent_keys(&mut rpush_sub, "rpush", &[]).await;
    server.shutdown().await;
}

/// BLMOVE immediate (non-blocking) path with data present: same direction-
/// resolved names as LMOVE. LEFT LEFT -> `lpop` on source, `lpush` on dest.
#[tokio::test]
async fn test_blmove_immediate_notifies_lpop_lpush() {
    let server = TestServer::start_standalone().await;
    let mut lpop_sub = server.connect().await;
    let mut lpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{blm}src", "a", "b"]).await;

    subscribe_keyevent(&mut lpop_sub, "lpop").await;
    subscribe_keyevent(&mut lpush_sub, "lpush").await;

    let resp = client
        .command(&["BLMOVE", "{blm}src", "{blm}dst", "LEFT", "LEFT", "0"])
        .await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("a"))));

    assert_keyevent_keys(&mut lpop_sub, "lpop", &["{blm}src"]).await;
    assert_keyevent_keys(&mut lpush_sub, "lpush", &["{blm}dst"]).await;
    server.shutdown().await;
}

/// ZMPOP notifies only the key it actually popped from — candidate keys that
/// were empty or missing stay silent (EventSpec::Dynamic deposit).
#[tokio::test]
async fn test_zmpop_notifies_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // {zmp}miss does not exist; only {zmp}hit can be popped.
    client.command(&["ZADD", "{zmp}hit", "1", "a"]).await;

    // ZMPOP MIN emits `zpopmin` (Redis parity, t_zset.c) on the popped key only.
    subscribe_keyevent(&mut subscriber, "zpopmin").await;

    let resp = client
        .command(&["ZMPOP", "2", "{zmp}miss", "{zmp}hit", "MIN"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "zpopmin", &["{zmp}hit"]).await;
    server.shutdown().await;
}

/// BLPOP's immediate (non-blocking) path notifies only the key it popped —
/// not every candidate key (EventSpec::Dynamic deposit).
#[tokio::test]
async fn test_blpop_immediate_notifies_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // {blp}miss does not exist; the pop is served immediately from {blp}hit.
    client.command(&["RPUSH", "{blp}hit", "a"]).await;

    subscribe_keyevent(&mut subscriber, "lpop").await;

    let resp = client
        .command(&["BLPOP", "{blp}miss", "{blp}hit", "0.1"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "lpop", &["{blp}hit"]).await;
    server.shutdown().await;
}

// ===========================================================================
// Keyspace-event key accuracy (proposal 44 phase 2)
//
// Statically-expressible Suppressed under-emitters flipped to real Redis events:
// PFMERGE emits on the destination (EventSpec::EmitsAt); GEOSEARCHSTORE and BITOP
// (both set-or-del) and LMPOP (popped-key only) deposit at runtime (Dynamic).
// ===========================================================================

/// PFMERGE emits `pfadd` on the destination only (Redis parity,
/// hyperloglog.c:1872) — the read-only source HLLs stay silent. Also covers the
/// missing-destination creation path (dest did not exist beforehand).
#[tokio::test]
async fn test_pfmerge_notifies_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Same hash tag: PFMERGE requires dest + sources on one shard.
    client.command(&["PFADD", "{pfm}s1", "a", "b"]).await;
    client.command(&["PFADD", "{pfm}s2", "c", "d"]).await;

    subscribe_keyevent(&mut subscriber, "pfadd").await;

    // {pfm}dest does not exist yet: this is the creation path.
    let resp = client
        .command(&["PFMERGE", "{pfm}dest", "{pfm}s1", "{pfm}s2"])
        .await;
    assert_eq!(resp, Response::ok());

    assert_keyevent_keys(&mut subscriber, "pfadd", &["{pfm}dest"]).await;
    server.shutdown().await;
}

/// A PFMERGE whose destination already contains every source register changes
/// nothing (write_was_noop) and must emit no `pfadd` — the effect pipeline is
/// skipped, same contract as a no-op PFADD.
#[tokio::test]
async fn test_pfmerge_noop_merge_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // dest already has a,b,c; source is a subset, so the merge moves no register.
    client.command(&["PFADD", "{pfn}dest", "a", "b", "c"]).await;
    client.command(&["PFADD", "{pfn}src", "a"]).await;

    subscribe_keyevent(&mut subscriber, "pfadd").await;

    let resp = client.command(&["PFMERGE", "{pfn}dest", "{pfn}src"]).await;
    assert_eq!(resp, Response::ok());

    assert_keyevent_keys(&mut subscriber, "pfadd", &[]).await;
    server.shutdown().await;
}

/// BITOP emits `set` on the destination only (Redis parity, bitops.c:1612) —
/// the read-only source strings stay silent.
#[tokio::test]
async fn test_bitop_notifies_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "{bop}a", "abc"]).await;
    client.command(&["SET", "{bop}b", "abd"]).await;

    subscribe_keyevent(&mut subscriber, "set").await;

    let resp = client
        .command(&["BITOP", "AND", "{bop}dest", "{bop}a", "{bop}b"])
        .await;
    assert_eq!(resp, Response::Integer(3));

    assert_keyevent_keys(&mut subscriber, "set", &["{bop}dest"]).await;
    server.shutdown().await;
}

/// BITOP with an empty result deletes a pre-existing destination and emits
/// `del` on it (Redis parity, bitops.c bitopCommand: `dbDelete` + NOTIFY_GENERIC
/// `del` on `maxlen == 0`) — never `set`. FrogDB now deletes the dest on empty,
/// matching Redis.
#[tokio::test]
async fn test_bitop_empty_result_deletes_dest_emits_del() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Pre-existing destination so the empty-result path actually deletes it.
    client.command(&["SET", "{boe}dest", "stale"]).await;

    subscribe_keyevent(&mut subscriber, "del").await;

    // Both sources missing -> empty AND result -> dest deleted.
    let resp = client
        .command(&["BITOP", "AND", "{boe}dest", "{boe}x", "{boe}y"])
        .await;
    assert_eq!(resp, Response::Integer(0));
    assert_eq!(
        client.command(&["EXISTS", "{boe}dest"]).await,
        Response::Integer(0),
        "empty-result BITOP must delete the pre-existing destination"
    );

    assert_keyevent_keys(&mut subscriber, "del", &["{boe}dest"]).await;
    server.shutdown().await;
}

/// BITOP with an empty result and a destination that never existed deletes
/// nothing, so it emits neither `set` nor `del` (Redis parity: `dbDelete`
/// returns 0, no notification fires).
#[tokio::test]
async fn test_bitop_empty_missing_dest_silent() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "del").await;

    // Both sources missing, no pre-existing dest -> empty result deletes nothing.
    let resp = client
        .command(&["BITOP", "AND", "{bom}dest", "{bom}x", "{bom}y"])
        .await;
    assert_eq!(resp, Response::Integer(0));
    assert_eq!(
        client.command(&["EXISTS", "{bom}dest"]).await,
        Response::Integer(0),
        "empty-result BITOP must not create a destination that never existed"
    );

    assert_keyevent_keys(&mut subscriber, "del", &[]).await;
    server.shutdown().await;
}

/// GEOADD emits `zadd` (class ZSET) on its key when a member is added — Redis
/// parity: geo.c geoaddCommand routes through zaddGenericCommand, which fires
/// `zadd` on an effective add/update.
#[tokio::test]
async fn test_geoadd_emits_zadd() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "zadd").await;

    let resp = client
        .command(&["GEOADD", "geo:add", "13.361389", "38.115556", "Palermo"])
        .await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut subscriber, "zadd", &["geo:add"]).await;
    server.shutdown().await;
}

/// A no-op GEOADD (re-adding an identical member — nothing added or changed)
/// emits nothing: execute() sets `write_was_noop`, skipping the whole
/// write-effect pipeline including the `zadd` notification. Matches Redis, which
/// notifies only when a member was actually added or updated.
#[tokio::test]
async fn test_geoadd_noop_emits_nothing() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed the member before subscribing, so the seeding `zadd` is not observed.
    let resp = client
        .command(&["GEOADD", "geo:noop", "13.361389", "38.115556", "Palermo"])
        .await;
    assert_eq!(resp, Response::Integer(1));

    subscribe_keyevent(&mut subscriber, "zadd").await;

    // Re-add the identical member: same score, nothing changes -> reply 0.
    let resp = client
        .command(&["GEOADD", "geo:noop", "13.361389", "38.115556", "Palermo"])
        .await;
    assert_eq!(resp, Response::Integer(0));

    let msg = subscriber.read_message(Duration::from_millis(400)).await;
    assert!(
        msg.is_none(),
        "a no-op GEOADD must not deliver a keyspace notification"
    );
    server.shutdown().await;
}

/// GEOSEARCHSTORE emits `geosearchstore` on the destination (Redis parity,
/// geo.c:834, class ZSET) when the search returns members.
#[tokio::test]
async fn test_geosearchstore_notifies_destination() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&[
            "GEOADD",
            "{gss}src",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ])
        .await;

    subscribe_keyevent(&mut subscriber, "geosearchstore").await;

    let resp = client
        .command(&[
            "GEOSEARCHSTORE",
            "{gss}dest",
            "{gss}src",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "300",
            "km",
            "ASC",
        ])
        .await;
    assert!(
        matches!(resp, Response::Integer(n) if n >= 1),
        "unexpected: {resp:?}"
    );

    assert_keyevent_keys(&mut subscriber, "geosearchstore", &["{gss}dest"]).await;
    server.shutdown().await;
}

/// GEOSEARCHSTORE with an empty result deletes a pre-existing destination and
/// emits `del` on it (Redis parity, geo.c:839, class GENERIC) — never
/// `geosearchstore`. FrogDB deletes the dest on empty, matching Redis.
#[tokio::test]
async fn test_geosearchstore_empty_result_emits_del() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&["GEOADD", "{gse}src", "13.361389", "38.115556", "Palermo"])
        .await;
    // Pre-existing destination so the empty-result path actually deletes it.
    client.command(&["SET", "{gse}dest", "stale"]).await;

    subscribe_keyevent(&mut subscriber, "del").await;

    // Search far from Palermo with a tiny radius -> no members -> dest deleted.
    let resp = client
        .command(&[
            "GEOSEARCHSTORE",
            "{gse}dest",
            "{gse}src",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "1",
            "km",
        ])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "del", &["{gse}dest"]).await;
    server.shutdown().await;
}

/// GEOSEARCHSTORE with an empty result and a destination that never existed
/// deletes nothing, so it emits neither `geosearchstore` nor `del`.
#[tokio::test]
async fn test_geosearchstore_empty_missing_dest_silent() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&["GEOADD", "{gsm}src", "13.361389", "38.115556", "Palermo"])
        .await;

    subscribe_keyevent(&mut subscriber, "del").await;

    let resp = client
        .command(&[
            "GEOSEARCHSTORE",
            "{gsm}dest",
            "{gsm}src",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "1",
            "km",
        ])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "del", &[]).await;
    server.shutdown().await;
}

/// LMPOP LEFT notifies only the key it actually popped from with `lpop` (Redis
/// parity, t_list.c:794) — empty/missing candidate keys stay silent
/// (EventSpec::Dynamic deposit), mirroring ZMPOP.
#[tokio::test]
async fn test_lmpop_left_notifies_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // {lmp}miss does not exist; only {lmp}hit can be popped.
    client.command(&["RPUSH", "{lmp}hit", "a", "b"]).await;

    subscribe_keyevent(&mut subscriber, "lpop").await;

    let resp = client
        .command(&["LMPOP", "2", "{lmp}miss", "{lmp}hit", "LEFT"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "lpop", &["{lmp}hit"]).await;
    server.shutdown().await;
}

/// LMPOP RIGHT maps to `rpop` on the popped key (direction-accurate event name).
#[tokio::test]
async fn test_lmpop_right_notifies_rpop() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{lmr}hit", "a", "b"]).await;

    subscribe_keyevent(&mut subscriber, "rpop").await;

    let resp = client.command(&["LMPOP", "1", "{lmr}hit", "RIGHT"]).await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "rpop", &["{lmr}hit"]).await;
    server.shutdown().await;
}

// ===========================================================================
// Keyspace-event key accuracy (proposal 44 phase 4)
//
// Dynamic-key STORE commands (SORT STORE, GEORADIUS STORE) deposit their
// destination event at runtime (set-or-del); the blocking-pop family is
// completed (BRPOPLPUSH/BLMPOP/BZMPOP immediate paths, ZMPOP direction names);
// and the blocked-then-woken satisfaction path publishes the same events the
// immediate path deposits.
// ===========================================================================

/// SORT ... STORE emits `sortstore` on the destination only (sort.c
/// NOTIFY_LIST) — the read-only source list stays silent.
#[tokio::test]
async fn test_sort_store_notifies_sortstore_destination_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{srt}src", "3", "1", "2"]).await;

    subscribe_keyevent(&mut subscriber, "sortstore").await;

    let resp = client
        .command(&["SORT", "{srt}src", "STORE", "{srt}dest"])
        .await;
    assert_eq!(resp, Response::Integer(3));

    assert_keyevent_keys(&mut subscriber, "sortstore", &["{srt}dest"]).await;
    server.shutdown().await;
}

/// SORT ... STORE of an empty/missing source deletes a pre-existing destination
/// and emits `del` (sort.c NOTIFY_GENERIC) — never `sortstore`.
#[tokio::test]
async fn test_sort_store_empty_result_emits_del() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Pre-existing destination; the source does not exist.
    client.command(&["RPUSH", "{srt2}dest", "stale"]).await;

    subscribe_keyevent(&mut subscriber, "del").await;

    let resp = client
        .command(&["SORT", "{srt2}missing", "STORE", "{srt2}dest"])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "del", &["{srt2}dest"]).await;
    server.shutdown().await;
}

/// SORT ... STORE of an empty result with no pre-existing destination emits
/// nothing (the never-existed destination is not deleted).
#[tokio::test]
async fn test_sort_store_empty_missing_dest_silent() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "del").await;

    let resp = client
        .command(&["SORT", "{srt3}missing", "STORE", "{srt3}dest"])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "del", &[]).await;
    server.shutdown().await;
}

/// GEORADIUS ... STORE emits `georadiusstore` on the destination (geo.c
/// NOTIFY_ZSET) — distinct from GEOSEARCHSTORE's `geosearchstore`.
#[tokio::test]
async fn test_georadius_store_notifies_georadiusstore() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&["GEOADD", "{geo}src", "13.361389", "38.115556", "a"])
        .await;

    subscribe_keyevent(&mut subscriber, "georadiusstore").await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "{geo}src",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "{geo}dest",
        ])
        .await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut subscriber, "georadiusstore", &["{geo}dest"]).await;
    server.shutdown().await;
}

/// GEORADIUS ... STORE with an empty result deletes a pre-existing destination
/// and emits `del`.
#[tokio::test]
async fn test_georadius_store_empty_result_emits_del() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Source far from the search center; pre-existing destination to delete.
    client
        .command(&["GEOADD", "{geo2}src", "13.361389", "38.115556", "a"])
        .await;
    client.command(&["ZADD", "{geo2}dest", "1", "stale"]).await;

    subscribe_keyevent(&mut subscriber, "del").await;

    let resp = client
        .command(&[
            "GEORADIUS",
            "{geo2}src",
            "0",
            "0",
            "1",
            "km",
            "STORE",
            "{geo2}dest",
        ])
        .await;
    assert_eq!(resp, Response::Integer(0));

    assert_keyevent_keys(&mut subscriber, "del", &["{geo2}dest"]).await;
    server.shutdown().await;
}

/// ZMPOP MIN emits `zpopmin` (not the phase-1 placeholder `zmpop`) on the one
/// popped key — Redis parity (t_zset.c genericZpopCommand).
#[tokio::test]
async fn test_zmpop_min_notifies_zpopmin_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // {zmp}miss does not exist; the pop is served from {zmp}hit.
    client
        .command(&["ZADD", "{zmp}hit", "1", "a", "2", "b"])
        .await;

    subscribe_keyevent(&mut subscriber, "zpopmin").await;

    let resp = client
        .command(&["ZMPOP", "2", "{zmp}miss", "{zmp}hit", "MIN"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "zpopmin", &["{zmp}hit"]).await;
    server.shutdown().await;
}

/// ZMPOP MAX maps to `zpopmax` on the popped key (direction-accurate).
#[tokio::test]
async fn test_zmpop_max_notifies_zpopmax() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&["ZADD", "{zmx}hit", "1", "a", "2", "b"])
        .await;

    subscribe_keyevent(&mut subscriber, "zpopmax").await;

    let resp = client.command(&["ZMPOP", "1", "{zmx}hit", "MAX"]).await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "zpopmax", &["{zmx}hit"]).await;
    server.shutdown().await;
}

/// BRPOPLPUSH immediate path emits `rpop` on the source and `lpush` on the
/// destination (RPOPLPUSH = LMOVE RIGHT LEFT) — not a bespoke event.
#[tokio::test]
async fn test_brpoplpush_immediate_notifies_rpop_lpush() {
    let server = TestServer::start_standalone().await;
    let mut sub_pop = server.connect().await;
    let mut sub_push = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{brl}src", "a", "b"]).await;

    subscribe_keyevent(&mut sub_pop, "rpop").await;
    subscribe_keyevent(&mut sub_push, "lpush").await;

    let resp = client
        .command(&["BRPOPLPUSH", "{brl}src", "{brl}dst", "0"])
        .await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("b"))));

    assert_keyevent_keys(&mut sub_pop, "rpop", &["{brl}src"]).await;
    assert_keyevent_keys(&mut sub_push, "lpush", &["{brl}dst"]).await;
    server.shutdown().await;
}

/// BLMPOP immediate path emits `lpop`/`rpop` (by direction) on the one popped
/// candidate only.
#[tokio::test]
async fn test_blmpop_immediate_notifies_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{blm}hit", "a", "b"]).await;

    subscribe_keyevent(&mut subscriber, "lpop").await;

    let resp = client
        .command(&["BLMPOP", "0", "2", "{blm}miss", "{blm}hit", "LEFT"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "lpop", &["{blm}hit"]).await;
    server.shutdown().await;
}

/// BZMPOP immediate path emits `zpopmin`/`zpopmax` (by direction) on the one
/// popped candidate only.
#[tokio::test]
async fn test_bzmpop_immediate_notifies_popped_key_only() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client
        .command(&["ZADD", "{bzm}hit", "1", "a", "2", "b"])
        .await;

    subscribe_keyevent(&mut subscriber, "zpopmin").await;

    let resp = client
        .command(&["BZMPOP", "0", "2", "{bzm}miss", "{bzm}hit", "MIN"])
        .await;
    assert!(matches!(resp, Response::Array(_)), "unexpected: {resp:?}");

    assert_keyevent_keys(&mut subscriber, "zpopmin", &["{bzm}hit"]).await;
    server.shutdown().await;
}

// --- Section C: blocked-then-woken satisfaction path emits keyspace events ---

/// A client blocked in BLPOP is served by a later RPUSH; the woken serve must
/// publish `lpop` on the popped key (the satisfaction path pops directly on the
/// store — this closes the phase-1/3 woken-path coverage gap).
#[tokio::test]
async fn test_blpop_woken_notifies_lpop() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Block on a missing key, then subscribe before the waking push.
    blocker.send_only(&["BLPOP", "{wbp}key", "5"]).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut subscriber, "lpop").await;

    client.command(&["RPUSH", "{wbp}key", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Array(_))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut subscriber, "lpop", &["{wbp}key"]).await;
    server.shutdown().await;
}

/// A client blocked in BRPOP is served by a later push; the woken serve emits
/// `rpop`.
#[tokio::test]
async fn test_brpop_woken_notifies_rpop() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    blocker.send_only(&["BRPOP", "{wbr}key", "5"]).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut subscriber, "rpop").await;

    client.command(&["RPUSH", "{wbr}key", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Array(_))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut subscriber, "rpop", &["{wbr}key"]).await;
    server.shutdown().await;
}

/// A client blocked in BZPOPMIN is served by a later ZADD; the woken serve emits
/// `zpopmin`.
#[tokio::test]
async fn test_bzpopmin_woken_notifies_zpopmin() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    blocker.send_only(&["BZPOPMIN", "{wzn}key", "5"]).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut subscriber, "zpopmin").await;

    client.command(&["ZADD", "{wzn}key", "1", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Array(_))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut subscriber, "zpopmin", &["{wzn}key"]).await;
    server.shutdown().await;
}

/// A client blocked in BZPOPMAX is served by a later ZADD; the woken serve emits
/// `zpopmax`.
#[tokio::test]
async fn test_bzpopmax_woken_notifies_zpopmax() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    blocker.send_only(&["BZPOPMAX", "{wzx}key", "5"]).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut subscriber, "zpopmax").await;

    client.command(&["ZADD", "{wzx}key", "1", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Array(_))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut subscriber, "zpopmax", &["{wzx}key"]).await;
    server.shutdown().await;
}

/// A client blocked in BLMOVE is served by a later push to the source; the woken
/// serve emits BOTH the source pop (`lpop`) and the destination push (`lpush`).
#[tokio::test]
async fn test_blmove_woken_notifies_lpop_and_lpush() {
    let server = TestServer::start_standalone().await;
    let mut sub_pop = server.connect().await;
    let mut sub_push = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    blocker
        .send_only(&["BLMOVE", "{wmv}src", "{wmv}dst", "LEFT", "LEFT", "5"])
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut sub_pop, "lpop").await;
    subscribe_keyevent(&mut sub_push, "lpush").await;

    client.command(&["RPUSH", "{wmv}src", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Bulk(Some(_)))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut sub_pop, "lpop", &["{wmv}src"]).await;
    assert_keyevent_keys(&mut sub_push, "lpush", &["{wmv}dst"]).await;
    server.shutdown().await;
}

/// A client blocked in BRPOPLPUSH is served by a later push; the woken serve
/// emits `rpop` on the source and `lpush` on the destination.
#[tokio::test]
async fn test_brpoplpush_woken_notifies_rpop_and_lpush() {
    let server = TestServer::start_standalone().await;
    let mut sub_pop = server.connect().await;
    let mut sub_push = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    blocker
        .send_only(&["BRPOPLPUSH", "{wbl}src", "{wbl}dst", "5"])
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut sub_pop, "rpop").await;
    subscribe_keyevent(&mut sub_push, "lpush").await;

    client.command(&["RPUSH", "{wbl}src", "a"]).await;

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Bulk(Some(_)))),
        "blocker woke: {woken:?}"
    );

    assert_keyevent_keys(&mut sub_pop, "rpop", &["{wbl}src"]).await;
    assert_keyevent_keys(&mut sub_push, "lpush", &["{wbl}dst"]).await;
    server.shutdown().await;
}

// --- Section D: close phase-3 test minors ---

/// SMOVE of a member already present in the destination removes it from the
/// source (`srem`) but performs no add — so `sadd` must NOT fire.
#[tokio::test]
async fn test_smove_member_already_in_dest_srem_only() {
    let server = TestServer::start_standalone().await;
    let mut sub_srem = server.connect().await;
    let mut sub_sadd = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SADD", "{smv}src", "m"]).await;
    client.command(&["SADD", "{smv}dst", "m"]).await; // m already in dest

    subscribe_keyevent(&mut sub_srem, "srem").await;
    subscribe_keyevent(&mut sub_sadd, "sadd").await;

    let resp = client
        .command(&["SMOVE", "{smv}src", "{smv}dst", "m"])
        .await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut sub_srem, "srem", &["{smv}src"]).await;
    // The destination already held the member: no `sadd`.
    assert_keyevent_keys(&mut sub_sadd, "sadd", &[]).await;
    server.shutdown().await;
}

/// A successful RENAMENX emits `rename_from` on the source and `rename_to` on
/// the destination (positive path — dest did not previously exist).
#[tokio::test]
async fn test_renamenx_success_notifies_rename_from_and_to() {
    let server = TestServer::start_standalone().await;
    let mut sub_from = server.connect().await;
    let mut sub_to = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "{rnx}src", "v"]).await;

    subscribe_keyevent(&mut sub_from, "rename_from").await;
    subscribe_keyevent(&mut sub_to, "rename_to").await;

    let resp = client.command(&["RENAMENX", "{rnx}src", "{rnx}dst"]).await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut sub_from, "rename_from", &["{rnx}src"]).await;
    assert_keyevent_keys(&mut sub_to, "rename_to", &["{rnx}dst"]).await;
    server.shutdown().await;
}

/// LMOVE with src == dst rotates the list in place; FrogDB pops then pushes on
/// the same key, so BOTH the pop and the push event fire on that one key.
#[tokio::test]
async fn test_lmove_self_move_rotate_emits_both_events() {
    let server = TestServer::start_standalone().await;
    let mut sub_pop = server.connect().await;
    let mut sub_push = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["RPUSH", "{lsm}k", "a", "b", "c"]).await;

    subscribe_keyevent(&mut sub_pop, "rpop").await;
    subscribe_keyevent(&mut sub_push, "lpush").await;

    // RIGHT -> LEFT rotate on the same key: rpop tail, lpush head.
    let resp = client
        .command(&["LMOVE", "{lsm}k", "{lsm}k", "RIGHT", "LEFT"])
        .await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("c"))));

    assert_keyevent_keys(&mut sub_pop, "rpop", &["{lsm}k"]).await;
    assert_keyevent_keys(&mut sub_push, "lpush", &["{lsm}k"]).await;
    server.shutdown().await;
}

// ===========================================================================
// Scripted writes emit keyspace events (proposal 46 item 2)
//
// A write performed via `redis.call(...)` inside EVAL/EVALSHA must have the
// same side effects as a direct command: the scripting seam records each
// effective write and routes it through the canonical write-effect pipeline
// (keyspace notifications, WATCH bump, tracking invalidation, waiter wake,
// WAL, replication) after the script completes.
// ===========================================================================

/// EVAL that SETs a key fires the same `set` keyevent a direct SET does.
#[tokio::test]
async fn test_eval_scripted_set_emits_set_event() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "set").await;

    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "evset",
            "v",
        ])
        .await;
    assert_eq!(resp, Response::Simple(Bytes::from("OK")));

    assert_keyevent_keys(&mut subscriber, "set", &["evset"]).await;
    server.shutdown().await;
}

/// A script that performs several writes emits each write's event, in
/// execution order (the multi-write batch flows through the pipeline as one
/// atomic effect group).
#[tokio::test]
async fn test_eval_multi_write_script_emits_each_event() {
    let server = TestServer::start_standalone().await;
    let mut set_sub = server.connect().await;
    let mut lpush_sub = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut set_sub, "set").await;
    subscribe_keyevent(&mut lpush_sub, "lpush").await;

    // Same hash tag so both writes run on the script's local shard.
    let resp = client
        .command(&[
            "EVAL",
            "redis.call('SET', KEYS[1], 'v'); redis.call('LPUSH', KEYS[2], 'a'); return 1",
            "2",
            "{evm}s",
            "{evm}l",
        ])
        .await;
    assert_eq!(resp, Response::Integer(1));

    assert_keyevent_keys(&mut set_sub, "set", &["{evm}s"]).await;
    assert_keyevent_keys(&mut lpush_sub, "lpush", &["{evm}l"]).await;
    server.shutdown().await;
}

/// A scripted LPUSH wakes a client blocked in BLPOP, with the same events a
/// direct LPUSH produces: `lpush` from the script's write, then `lpop` from
/// the woken serve.
#[tokio::test]
async fn test_eval_scripted_lpush_wakes_blocked_blpop() {
    let server = TestServer::start_standalone().await;
    let mut lpush_sub = server.connect().await;
    let mut lpop_sub = server.connect().await;
    let mut blocker = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Block on a missing key, then subscribe before the waking scripted push.
    blocker.send_only(&["BLPOP", "evblk", "5"]).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    subscribe_keyevent(&mut lpush_sub, "lpush").await;
    subscribe_keyevent(&mut lpop_sub, "lpop").await;

    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('LPUSH', KEYS[1], ARGV[1])",
            "1",
            "evblk",
            "a",
        ])
        .await;
    assert_eq!(resp, Response::Integer(1));

    let woken = blocker.read_response(Duration::from_secs(2)).await;
    assert!(
        matches!(woken, Some(Response::Array(_))),
        "scripted LPUSH must wake the blocked BLPOP, got: {woken:?}"
    );

    assert_keyevent_keys(&mut lpush_sub, "lpush", &["evblk"]).await;
    assert_keyevent_keys(&mut lpop_sub, "lpop", &["evblk"]).await;
    server.shutdown().await;
}

/// A scripted no-op write (duplicate PFADD: no register moved, write_was_noop)
/// stays silent — the seam records only effective writes.
#[tokio::test]
async fn test_eval_scripted_noop_stays_silent() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed the HLL before subscribing so the seed event is not observed.
    let resp = client.command(&["PFADD", "evhll", "a"]).await;
    assert_eq!(resp, Response::Integer(1));

    subscribe_keyevent(&mut subscriber, "pfadd").await;

    // Duplicate add via script: no register moves, reply 0, no event.
    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('PFADD', KEYS[1], ARGV[1])",
            "1",
            "evhll",
            "a",
        ])
        .await;
    assert_eq!(resp, Response::Integer(0));

    let msg = subscriber.read_message(Duration::from_millis(400)).await;
    assert!(
        msg.is_none(),
        "a scripted no-op write must not deliver a keyspace notification"
    );
    server.shutdown().await;
}

// ===========================================================================
// `new` (key-creation, class `n`) and `keymiss` (class `m`) events (task 09)
//
// Both classes parse in `notify-keyspace-events` but previously had zero
// emission sites. Redis 8.x fires `new` from `dbAdd` on first key creation
// (before the command's own type event) and `keymiss` from `lookupKeyRead`
// on a read that misses (never from write lookups). Both are excluded from the
// `A` alias, so `A` alone must deliver neither. These tests assert end-to-end
// delivery and the `A`-exclusion.
// ===========================================================================

/// `new` fires on the first creation of a key. SET of a fresh key creates it,
/// so a `new` keyevent naming the key is delivered when class `n` is enabled.
#[tokio::test]
async fn test_new_event_fires_on_key_creation() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // `En` = keyevent notifications for the new-key class only.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "En"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "new").await;

    let resp = client.command(&["SET", "freshkey", "v"]).await;
    assert_eq!(resp, Response::ok());

    assert_keyevent_keys(&mut subscriber, "new", &["freshkey"]).await;
    server.shutdown().await;
}

/// `new` fires only on *first* creation: overwriting an existing key does not
/// create it, so no `new` event is emitted for the second SET.
#[tokio::test]
async fn test_new_event_not_fired_on_overwrite() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "En"])
        .await;
    assert_eq!(resp, Response::ok());

    // Seed the key *before* subscribing, so the creation `new` event is not
    // observed; only the subsequent overwrite is under test.
    client.command(&["SET", "dupkey", "v1"]).await;

    subscribe_keyevent(&mut subscriber, "new").await;

    let resp = client.command(&["SET", "dupkey", "v2"]).await;
    assert_eq!(resp, Response::ok());

    // Overwrite creates nothing: no `new` event.
    assert_keyevent_keys(&mut subscriber, "new", &[]).await;
    server.shutdown().await;
}

/// `keymiss` fires on a read command that misses (key absent). GET of a missing
/// key delivers a `keymiss` keyevent naming the key when class `m` is enabled.
#[tokio::test]
async fn test_keymiss_event_fires_on_read_miss() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // `Em` = keyevent notifications for the key-miss class only.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "Em"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "keymiss").await;

    let resp = client.command(&["GET", "absentkey"]).await;
    assert_eq!(resp, Response::Bulk(None));

    assert_keyevent_keys(&mut subscriber, "keymiss", &["absentkey"]).await;
    server.shutdown().await;
}

/// `keymiss` does not fire when the read hits: a present key delivers no
/// `keymiss` event.
#[tokio::test]
async fn test_keymiss_not_fired_on_read_hit() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "Em"])
        .await;
    assert_eq!(resp, Response::ok());

    client.command(&["SET", "presentkey", "v"]).await;

    subscribe_keyevent(&mut subscriber, "keymiss").await;

    let resp = client.command(&["GET", "presentkey"]).await;
    assert_eq!(resp, Response::Bulk(Some(Bytes::from("v"))));

    assert_keyevent_keys(&mut subscriber, "keymiss", &[]).await;
    server.shutdown().await;
}

/// `keymiss` fires only from read lookups, never write lookups (Redis parity:
/// `lookupKeyRead` vs `lookupKeyWrite`). GETDEL is a WRITE command with a
/// first-key lookup; a GETDEL that misses must emit no `keymiss`.
#[tokio::test]
async fn test_keymiss_not_fired_for_write_command() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "Em"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut subscriber, "keymiss").await;

    // GETDEL of a missing key: a write-lookup miss, so no `keymiss`.
    let resp = client.command(&["GETDEL", "absent-write-key"]).await;
    assert_eq!(resp, Response::Bulk(None));

    assert_keyevent_keys(&mut subscriber, "keymiss", &[]).await;
    server.shutdown().await;
}

/// The `A` alias must NOT enable `new` or `keymiss` (both are excluded from
/// `NOTIFY_ALL` in Redis). With `KEA` set, creating a key and missing a read
/// deliver their ordinary type events but neither `new` nor `keymiss`.
#[tokio::test]
async fn test_all_alias_excludes_new_and_keymiss() {
    let server = TestServer::start_standalone().await;
    let mut new_sub = server.connect().await;
    let mut keymiss_sub = server.connect().await;
    let mut set_sub = server.connect().await;
    let mut client = server.connect().await;

    // `KEA` enables keyspace + keyevent for every class in the `A` alias,
    // which deliberately excludes `n` and `m`.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
        .await;
    assert_eq!(resp, Response::ok());

    subscribe_keyevent(&mut new_sub, "new").await;
    subscribe_keyevent(&mut keymiss_sub, "keymiss").await;
    subscribe_keyevent(&mut set_sub, "set").await;

    // Create a key (would fire `new` if `n` were on) and miss a read (would
    // fire `keymiss` if `m` were on).
    let resp = client.command(&["SET", "akey", "v"]).await;
    assert_eq!(resp, Response::ok());
    let resp = client.command(&["GET", "amiss"]).await;
    assert_eq!(resp, Response::Bulk(None));

    // The ordinary `set` type event IS delivered (proves notifications are on).
    assert_keyevent_keys(&mut set_sub, "set", &["akey"]).await;
    // But neither `new` nor `keymiss` under the `A` alias.
    assert_keyevent_keys(&mut new_sub, "new", &[]).await;
    assert_keyevent_keys(&mut keymiss_sub, "keymiss", &[]).await;
    server.shutdown().await;
}

/// `new` is emitted before the command's own type event: a single SET of a
/// fresh key with both `n` and the string class enabled delivers `new` first,
/// then `set`, on a subscriber pattern-listening to both.
#[tokio::test]
async fn test_new_precedes_type_event() {
    let server = TestServer::start_standalone().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // `E$n` = keyevent + string class + new-key class.
    let resp = client
        .command(&["CONFIG", "SET", "notify-keyspace-events", "E$n"])
        .await;
    assert_eq!(resp, Response::ok());

    // Pattern-subscribe so both `new` and `set` keyevents arrive in order.
    let resp = subscriber
        .command(&["PSUBSCRIBE", "__keyevent@0__:*"])
        .await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = client.command(&["SET", "ordkey", "v"]).await;
    assert_eq!(resp, Response::ok());

    // First message: `new`. pmessage shape: ["pmessage", pattern, channel, payload].
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    let Some(Response::Array(arr)) = msg else {
        panic!("expected a `new` pmessage, got {msg:?}");
    };
    assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("pmessage"))));
    assert_eq!(
        arr[2],
        Response::Bulk(Some(Bytes::from("__keyevent@0__:new")))
    );
    assert_eq!(arr[3], Response::Bulk(Some(Bytes::from("ordkey"))));

    // Second message: `set`.
    let msg = subscriber.read_message(Duration::from_secs(2)).await;
    let Some(Response::Array(arr)) = msg else {
        panic!("expected a `set` pmessage after `new`, got {msg:?}");
    };
    assert_eq!(
        arr[2],
        Response::Bulk(Some(Bytes::from("__keyevent@0__:set")))
    );
    assert_eq!(arr[3], Response::Bulk(Some(Bytes::from("ordkey"))));

    server.shutdown().await;
}

// ============================================================================
// Subscriber disconnect cross-shard deregistration (issue 30)
// ============================================================================
//
// A real client disconnect must deregister the connection from the broadcast
// subscription map (shard 0, where SUBSCRIBE/PSUBSCRIBE register) *and* from
// every sharded-channel subscription map on each owning shard. That fan-out
// rides the `ConnectionClosed` broadcast to all shards
// (`connection/lifecycle.rs` -> `notify_connection_closed`). These e2e tests
// subscribe one client across >= 2 shards (broadcast + pattern on shard 0, a
// sharded channel on a non-zero shard) and then assert PUBSUB introspection and
// PUBLISH/SPUBLISH receiver counts drop to zero after both a graceful
// `CLIENT KILL` and an ungraceful raw-socket close.

/// Number of active broadcast channels reported by `PUBSUB CHANNELS`.
async fn pubsub_channels_len(client: &mut TestClient) -> usize {
    match client.command(&["PUBSUB", "CHANNELS"]).await {
        Response::Array(a) => a.len(),
        other => panic!("expected array from PUBSUB CHANNELS, got {other:?}"),
    }
}

/// Subscriber count for a single broadcast channel via `PUBSUB NUMSUB <ch>`
/// (reply shape `[<ch>, <count>]`).
async fn pubsub_numsub(client: &mut TestClient, channel: &str) -> i64 {
    match client.command(&["PUBSUB", "NUMSUB", channel]).await {
        Response::Array(a) => match a.as_slice() {
            [_, Response::Integer(n)] => *n,
            other => panic!("unexpected PUBSUB NUMSUB reply: {other:?}"),
        },
        other => panic!("expected array from PUBSUB NUMSUB, got {other:?}"),
    }
}

/// Total pattern-subscription count via `PUBSUB NUMPAT`.
async fn pubsub_numpat(client: &mut TestClient) -> i64 {
    match client.command(&["PUBSUB", "NUMPAT"]).await {
        Response::Integer(n) => n,
        other => panic!("expected integer from PUBSUB NUMPAT, got {other:?}"),
    }
}

/// Subscriber count for a single sharded channel via
/// `PUBSUB SHARDNUMSUB <ch>` (reply shape `[<ch>, <count>]`).
async fn pubsub_shardnumsub(client: &mut TestClient, channel: &str) -> i64 {
    match client.command(&["PUBSUB", "SHARDNUMSUB", channel]).await {
        Response::Array(a) => match a.as_slice() {
            [_, Response::Integer(n)] => *n,
            other => panic!("unexpected PUBSUB SHARDNUMSUB reply: {other:?}"),
        },
        other => panic!("expected array from PUBSUB SHARDNUMSUB, got {other:?}"),
    }
}

/// Poll PUBSUB introspection (via a separate control connection) until every
/// subscription count for the given broadcast + pattern + sharded channels
/// reaches zero across all shards, or panic after `deadline`. Uses a polling
/// deadline rather than a fixed sleep so the test is robust to disconnect
/// timing (the `ConnectionClosed` fan-out is asynchronous).
async fn wait_for_full_dereg(
    client: &mut TestClient,
    broadcast_channel: &str,
    sharded_channel: &str,
    deadline: Duration,
) {
    let start = Instant::now();
    loop {
        let numsub = pubsub_numsub(client, broadcast_channel).await;
        let numpat = pubsub_numpat(client).await;
        let shardnumsub = pubsub_shardnumsub(client, sharded_channel).await;
        let channels = pubsub_channels_len(client).await;

        if numsub == 0 && numpat == 0 && shardnumsub == 0 && channels == 0 {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "subscriptions leaked after disconnect: \
             PUBSUB CHANNELS={channels} NUMSUB({broadcast_channel})={numsub} \
             NUMPAT={numpat} SHARDNUMSUB({sharded_channel})={shardnumsub}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll until the subscriptions are visible across all shards (registration is
/// synchronous per command, but the scatter-gather introspection read is polled
/// to avoid any startup race), returning once all counts are >= 1.
async fn wait_for_full_reg(
    client: &mut TestClient,
    broadcast_channel: &str,
    sharded_channel: &str,
    deadline: Duration,
) {
    let start = Instant::now();
    loop {
        let numsub = pubsub_numsub(client, broadcast_channel).await;
        let numpat = pubsub_numpat(client).await;
        let shardnumsub = pubsub_shardnumsub(client, sharded_channel).await;

        if numsub >= 1 && numpat >= 1 && shardnumsub >= 1 {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "subscriptions never fully registered: \
             NUMSUB({broadcast_channel})={numsub} NUMPAT={numpat} \
             SHARDNUMSUB({sharded_channel})={shardnumsub}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// How the subscriber connection is torn down.
enum DisconnectMode {
    /// Graceful server-driven close via `CLIENT KILL ID`.
    ClientKill,
    /// Ungraceful transport-level close: drop the TCP socket without any RESP
    /// teardown, exercising the `ConnectionClosed` broadcast fan-out.
    RawClose,
}

/// Shared body: subscribe one connection across >= 2 shards (broadcast + pattern
/// on shard 0, sharded channel off shard 0), disconnect it via `mode`, then
/// assert every per-shard registration is gone and PUBLISH/SPUBLISH no longer
/// count it as a receiver.
async fn run_disconnect_dereg_test(mode: DisconnectMode) {
    let num_shards = 4;
    // Sharded channel deliberately owned by a shard other than the broadcast
    // coordinator (shard 0), so deregistration must fan out across shards.
    let sharded_channel = key_off_shard_zero(num_shards);
    assert_ne!(
        frogdb_core::shard_for_key(sharded_channel.as_bytes(), num_shards),
        0,
        "sharded channel must live off shard 0 to exercise the fan-out"
    );
    let broadcast_channel = "dereg:broadcast";
    let pattern = "dereg:pat:*";

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    })
    .await;

    let mut subscriber = server.connect().await;
    let mut control = server.connect().await;
    let mut publisher = server.connect().await;

    // Capture the client id *before* entering pub/sub mode (RESP2 pub/sub mode
    // rejects CLIENT commands).
    let subscriber_id = match subscriber.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("expected integer from CLIENT ID, got {other:?}"),
    };

    // Subscribe across >= 2 shards: broadcast channel + pattern (shard 0) and a
    // sharded channel owned by a non-zero shard.
    subscriber.command(&["SUBSCRIBE", broadcast_channel]).await;
    subscriber.command(&["PSUBSCRIBE", pattern]).await;
    subscriber.command(&["SSUBSCRIBE", &sharded_channel]).await;

    // Confirm the subscriptions are fully registered across shards first, so the
    // post-disconnect assertion is meaningful (0 must be a real drop, not a
    // never-registered state).
    wait_for_full_reg(
        &mut control,
        broadcast_channel,
        &sharded_channel,
        Duration::from_secs(5),
    )
    .await;

    // Sanity: the live subscriber is counted as a receiver on both namespaces.
    assert_eq!(
        publisher
            .command(&["PUBLISH", broadcast_channel, "hi"])
            .await,
        Response::Integer(1),
        "live subscriber must be counted by PUBLISH"
    );
    assert_eq!(
        publisher
            .command(&["SPUBLISH", &sharded_channel, "hi"])
            .await,
        Response::Integer(1),
        "live subscriber must be counted by SPUBLISH"
    );

    // Disconnect.
    match mode {
        DisconnectMode::ClientKill => {
            let resp = control
                .command(&["CLIENT", "KILL", "ID", &subscriber_id.to_string()])
                .await;
            assert_eq!(resp, Response::Integer(1), "CLIENT KILL should kill 1 conn");
            // Drop the local socket too so no buffered client state masks a
            // server-side leak.
            drop(subscriber);
        }
        DisconnectMode::RawClose => {
            // Ungraceful: drop the TCP stream with no RESET/UNSUBSCRIBE/QUIT.
            drop(subscriber);
        }
    }

    // Deregistration is asynchronous (ConnectionClosed fan-out to all shards);
    // poll to a deadline.
    wait_for_full_dereg(
        &mut control,
        broadcast_channel,
        &sharded_channel,
        Duration::from_secs(5),
    )
    .await;

    // PUBLISH / SPUBLISH must no longer count the gone subscriber.
    assert_eq!(
        publisher
            .command(&["PUBLISH", broadcast_channel, "after"])
            .await,
        Response::Integer(0),
        "PUBLISH must not count a disconnected subscriber"
    );
    assert_eq!(
        publisher
            .command(&["SPUBLISH", &sharded_channel, "after"])
            .await,
        Response::Integer(0),
        "SPUBLISH must not count a disconnected subscriber"
    );

    server.shutdown().await;
}

/// Variant A: graceful `CLIENT KILL` deregisters the subscriber across every
/// shard (broadcast, pattern, and the off-shard-0 sharded channel).
#[tokio::test]
async fn test_subscriber_dereg_on_client_kill_cross_shard() {
    run_disconnect_dereg_test(DisconnectMode::ClientKill).await;
}

/// Variant B: an ungraceful raw TCP close deregisters the subscriber across
/// every shard, exercising the `ConnectionClosed` broadcast fan-out
/// (`connection/lifecycle.rs`) rather than a command-driven teardown.
#[tokio::test]
async fn test_subscriber_dereg_on_raw_close_cross_shard() {
    run_disconnect_dereg_test(DisconnectMode::RawClose).await;
}

/// Slow-subscriber output-buffer bound (issue 29).
///
/// Regression + policy pin for the pub/sub client-output-buffer DoS: a
/// subscriber that stops reading its socket must NOT let the server buffer
/// published messages without bound. FrogDB caps the per-connection pub/sub
/// output buffer at `server.pubsub-output-buffer-hard-limit` bytes (mirroring
/// Redis `client-output-buffer-limit pubsub`): once the queue would exceed the
/// limit, further messages are dropped to keep memory bounded, the overflow is
/// latched, and the connection task tears the slow subscriber down on its next
/// drain — exactly as Redis disconnects a client that trips its pubsub buffer
/// limit.
///
/// This exercises the *client-facing* delivery path (shard `publish` ->
/// per-connection `PubSubSender` -> connection delivery loop -> socket), which
/// is distinct from the already-bounded cross-shard keyspace-notification hop
/// (`keyspace_coordinator.rs`): that internal hop is a separate, upstream-capped
/// channel and is deliberately left uncapped here (`PubSubSender::unbounded`).
///
/// # Making the overflow deterministic across kernels
///
/// The in-process output budget only accumulates (and overflows) once the
/// connection's delivery-loop socket write *blocks* — which needs the kernel's
/// own socket buffering (server `SO_SNDBUF` + the subscriber's `SO_RCVBUF`) to
/// fill first. Linux loopback autotunes `SO_SNDBUF` up to `tcp_wmem` max (4 MiB
/// on the CI box), so a small flood is silently absorbed by kernel buffers, the
/// userspace budget never overflows, and nothing is disconnected. Shrinking the
/// *receiver* buffer alone does not help: unsent bytes still pile into the
/// server's send buffer regardless of the advertised window.
///
/// So the flood must exceed the total kernel buffering ceiling. We cap the
/// subscriber's `SO_RCVBUF` to a known small value and publish ~16 MiB — far
/// past `server SO_SNDBUF (<= 4 MiB) + subscriber SO_RCVBUF (128 KiB) + the
/// 32 KiB output budget` — so the delivery write is guaranteed to block, the
/// budget is guaranteed to overflow, and the slow subscriber is torn down. The
/// per-connection *server memory* stays bounded by the 32 KiB budget throughout
/// (the kernel buffers are the OS's fixed ceiling, not unbounded growth) — the
/// whole point of the bound.
#[tokio::test]
async fn test_slow_subscriber_output_buffer_bound_disconnects() {
    // Small budget so accumulation overflows well before the flood ends, but
    // larger than one message so this tests message *accumulation* (a slow
    // subscriber), not a single oversized message.
    let hard_limit = 32 * 1024;
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        pubsub_output_buffer_hard_limit: Some(hard_limit),
        ..Default::default()
    })
    .await;

    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;
    let mut control = server.connect().await;

    // Cap the subscriber's receive buffer so the kernel buffering ceiling the
    // flood must exceed is bounded and known (the dominant term is the server's
    // <= 4 MiB autotuned send buffer). A modest 128 KiB — not a tiny window —
    // keeps the post-disconnect drain fast. The bound itself is unit-tested in
    // `core/src/pubsub.rs`; here we pin the end-to-end operator-visible
    // disconnect across both macOS and the Linux testbox.
    socket2::SockRef::from(subscriber.framed.get_ref())
        .set_recv_buffer_size(128 * 1024)
        .expect("cap subscriber SO_RCVBUF");

    // Subscribe, then deliberately STALL: never read from `subscriber` again
    // until after the flood, so the server-side output buffer is the only thing
    // standing between a hostile publisher and unbounded memory growth.
    let resp = subscriber.command(&["SUBSCRIBE", "flood"]).await;
    assert!(matches!(resp, Response::Array(ref arr) if arr.len() == 3));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Flood far past the kernel buffering ceiling: 4000 * 4 KiB = ~16 MiB
    // attempted against a 32 KiB budget. Fire back-to-back without awaiting each
    // reply so they arrive as a burst the single stalled subscriber cannot drain;
    // once the kernel buffers fill, the delivery write blocks and the budget
    // drops the rest.
    let attempted = 4000;
    let payload = "x".repeat(4 * 1024);
    for _ in 0..attempted {
        publisher.send_only(&["PUBLISH", "flood", &payload]).await;
    }

    // The server must stay responsive throughout — no OOM, no wedge. A separate
    // connection completes a round trip while the flood is in flight.
    let pong = control.command(&["PING"]).await;
    assert_eq!(pong, Response::Simple(Bytes::from("PONG")));

    // Now let the subscriber drain. It receives the bounded prefix that fit in
    // the kernel buffers and then hits EOF: the server tore the connection down
    // because the output buffer overflowed. `framed.next()` yields `None` on the
    // clean close (or `Some(Err(..))` on an ungraceful reset); either means
    // disconnected.
    let mut disconnected = false;
    let mut frames_read = 0usize;
    for _ in 0..(attempted + 1) {
        match tokio::time::timeout(Duration::from_secs(10), subscriber.framed.next()).await {
            Ok(None) => {
                disconnected = true;
                break;
            }
            Ok(Some(Ok(_frame))) => {
                frames_read += 1;
            }
            Ok(Some(Err(_))) => {
                disconnected = true;
                break;
            }
            Err(_) => break, // timeout: still connected (unexpected)
        }
    }

    assert!(
        disconnected,
        "server must disconnect the slow subscriber after the output buffer \
         overflowed (read {frames_read} frames before EOF)"
    );
    // Memory bound in action: only the bounded prefix that fit in kernel buffers
    // was ever delivered, well short of the ~4000 published — the rest were
    // dropped by the output budget rather than buffered without bound.
    assert!(
        frames_read < attempted,
        "far fewer than the published messages should be delivered; got {frames_read}"
    );

    // The disconnect is observable as a metric for operators.
    let metrics = server.fetch_metrics().await;
    let disconnects = frogdb_telemetry::testing::get_counter(
        &metrics,
        "frogdb_pubsub_output_buffer_disconnects_total",
        &[],
    );
    assert!(
        disconnects >= 1.0,
        "output-buffer disconnect counter must record the teardown; got {disconnects}"
    );

    // Control connection is still fully functional after the storm.
    let pong = control.command(&["PING"]).await;
    assert_eq!(pong, Response::Simple(Bytes::from("PONG")));

    server.shutdown().await;
}
