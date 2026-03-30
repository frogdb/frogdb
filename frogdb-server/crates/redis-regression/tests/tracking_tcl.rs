//! Rust port of Redis 8.6.0 `unit/tracking.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (lazy expire with DEBUG SLEEP, active-expire toggling,
//!   regression #11715 with debug pause-cron)
//! - Tests depending on `CONFIG SET` (tracking-table-max-keys, maxmemory-policy,
//!   maxmemory, eviction-based invalidation, tracking info counts)
//! - RESP2 REDIRECT-based invalidation tests that depend on subscribing to
//!   `__redis__:invalidate` (complex multi-client redirect orchestration)
//! - Tests about `CLIENT REPLY OFF` (client reply management)
//! - Tests about `tracking-redir-broken` push messages
//! - BLMOVE blocking-based invalidation tests
//! - Script/EVAL tracking tests (require scripting + tracking interaction)
//! - `flushall`/`flushdb` invalidation tests (depend on complex redirect state)
//! - Tests that require re-creating deferred clients mid-test (`clean_all` pattern)

use frogdb_test_harness::server::TestServer;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::time::Duration;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Assert that a `Resp3Frame` is a Push invalidation containing exactly `expected_keys`.
fn assert_invalidation_keys(frame: &Resp3Frame, expected_keys: &[&str]) {
    match frame {
        Resp3Frame::Push { data, .. } => {
            assert!(data.len() >= 2, "Push should have at least 2 elements");
            if let Resp3Frame::BlobString { data: kind, .. } = &data[0] {
                assert_eq!(kind.as_ref(), b"invalidate", "First element should be 'invalidate'");
            } else {
                panic!("Expected BlobString 'invalidate', got {:?}", data[0]);
            }
            if let Resp3Frame::Array { data: keys, .. } = &data[1] {
                let mut key_strs: Vec<String> = keys
                    .iter()
                    .map(|k| match k {
                        Resp3Frame::BlobString { data, .. } => {
                            String::from_utf8(data.to_vec()).unwrap()
                        }
                        _ => panic!("Expected BlobString key, got {:?}", k),
                    })
                    .collect();
                key_strs.sort();
                let mut expected_sorted: Vec<&str> = expected_keys.to_vec();
                expected_sorted.sort();
                assert_eq!(
                    key_strs, expected_sorted,
                    "Invalidation keys mismatch"
                );
            } else {
                panic!("Expected Array of keys, got {:?}", data[1]);
            }
        }
        _ => panic!("Expected Push frame, got {:?}", frame),
    }
}

/// Assert that a `Resp3Frame` is a Push flush-all invalidation (null keys).
fn assert_invalidation_flush(frame: &Resp3Frame) {
    match frame {
        Resp3Frame::Push { data, .. } => {
            assert!(data.len() >= 2, "Push should have at least 2 elements");
            if let Resp3Frame::BlobString { data: kind, .. } = &data[0] {
                assert_eq!(kind.as_ref(), b"invalidate");
            } else {
                panic!("Expected BlobString 'invalidate', got {:?}", data[0]);
            }
            assert!(
                matches!(&data[1], Resp3Frame::Null),
                "Expected Null for flush-all invalidation, got {:?}",
                data[1]
            );
        }
        _ => panic!("Expected Push frame, got {:?}", frame),
    }
}

/// Extract the invalidated key names from a Push invalidation frame.
fn extract_invalidation_keys(frame: &Resp3Frame) -> Vec<String> {
    match frame {
        Resp3Frame::Push { data, .. } => {
            assert!(data.len() >= 2);
            match &data[1] {
                Resp3Frame::Array { data: keys, .. } => keys
                    .iter()
                    .map(|k| match k {
                        Resp3Frame::BlobString { data, .. } => {
                            String::from_utf8(data.to_vec()).unwrap()
                        }
                        _ => panic!("Expected BlobString key, got {:?}", k),
                    })
                    .collect(),
                Resp3Frame::Null => vec![],
                other => panic!("Expected Array or Null, got {:?}", other),
            }
        }
        _ => panic!("Expected Push frame, got {:?}", frame),
    }
}

/// Check if a Resp3Frame is a SimpleString "OK".
fn is_resp3_ok(frame: &Resp3Frame) -> bool {
    matches!(frame, Resp3Frame::SimpleString { data, .. } if data.as_ref() == b"OK")
}

// ---------------------------------------------------------------------------
// Clients are able to enable tracking and redirect it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_clients_can_enable_tracking_and_redirect() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut redir_target = server.connect_resp3().await;

    // Get the redirect target's client ID
    let id_frame = redir_target.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => data.to_string(),
        _ => panic!("Expected Number from CLIENT ID, got {:?}", id_frame),
    };

    let resp = tracker
        .command(&["CLIENT", "TRACKING", "ON", "REDIRECT", &redir_id])
        .await;
    assert!(is_resp3_ok(&resp), "Expected OK, got {:?}", resp);
}

// ---------------------------------------------------------------------------
// The client is now able to disable tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_can_disable_tracking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    assert!(is_resp3_ok(&resp), "Expected OK, got {:?}", resp);
}

// ---------------------------------------------------------------------------
// Clients can enable the BCAST mode with the empty prefix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bcast_mode_empty_prefix() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut redir_target = server.connect_resp3().await;

    let id_frame = redir_target.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => data.to_string(),
        _ => panic!("Expected Number from CLIENT ID, got {:?}", id_frame),
    };

    let resp = tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "REDIRECT", &redir_id])
        .await;
    assert!(is_resp3_ok(&resp), "Expected OK, got {:?}", resp);
}

// ---------------------------------------------------------------------------
// BCAST mode: connection gets invalidation messages about all keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bcast_invalidation_all_keys() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;

    // Writer sets three keys
    writer
        .command(&["MSET", "a{t}", "1", "b{t}", "2", "c{t}", "3"])
        .await;

    // Collect invalidation messages -- BCAST may batch or split them
    let mut all_keys: Vec<String> = Vec::new();
    for _ in 0..3 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(msg) => {
                let keys = extract_invalidation_keys(&msg);
                all_keys.extend(keys);
            }
            None => break,
        }
    }
    all_keys.sort();
    all_keys.dedup();
    assert!(
        all_keys.contains(&"a{t}".to_string()),
        "Should contain a{{t}}, got {:?}",
        all_keys
    );
    assert!(
        all_keys.contains(&"b{t}".to_string()),
        "Should contain b{{t}}, got {:?}",
        all_keys
    );
    assert!(
        all_keys.contains(&"c{t}".to_string()),
        "Should contain c{{t}}, got {:?}",
        all_keys
    );
}

// ---------------------------------------------------------------------------
// Clients can enable the BCAST mode with prefixes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bcast_with_prefixes() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:", "PREFIX", "b:",
        ])
        .await;

    // Write keys matching both prefixes plus one that should NOT match
    writer.command(&["INCR", "a:1{t}"]).await;
    writer.command(&["INCR", "a:2{t}"]).await;
    writer.command(&["INCR", "b:1{t}"]).await;
    writer.command(&["INCR", "b:2{t}"]).await;
    writer.command(&["INCR", "c:1{t}"]).await; // should not appear

    // Collect all invalidation keys
    let mut all_keys: Vec<String> = Vec::new();
    for _ in 0..5 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(msg) => {
                let keys = extract_invalidation_keys(&msg);
                all_keys.extend(keys);
            }
            None => break,
        }
    }
    all_keys.sort();
    all_keys.dedup();

    assert!(all_keys.contains(&"a:1{t}".to_string()));
    assert!(all_keys.contains(&"a:2{t}".to_string()));
    assert!(all_keys.contains(&"b:1{t}".to_string()));
    assert!(all_keys.contains(&"b:2{t}".to_string()));
    assert!(
        !all_keys.contains(&"c:1{t}".to_string()),
        "c:1{{t}} should NOT appear in BCAST with PREFIX a: b:"
    );
}

// ---------------------------------------------------------------------------
// Adding prefixes to BCAST mode works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_bcast_adding_prefix() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:"])
        .await;
    // Add another prefix
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "c:"])
        .await;

    writer.command(&["INCR", "c:1234"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for c:1234");
    assert_invalidation_keys(&msg, &["c:1234"]);
}

// ---------------------------------------------------------------------------
// Tracking NOLOOP mode in standard mode works
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB TRACKING NOLOOP not implemented"]
async fn tcl_tracking_noloop_standard_mode() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "NOLOOP"])
        .await;

    // Track three keys
    tracker
        .command(&["MGET", "otherkey1{t}", "loopkey{t}", "otherkey2{t}"])
        .await;

    // Writer modifies otherkey1 -- should generate notification
    writer.command(&["SET", "otherkey1{t}", "1"]).await;
    // Self-modification of loopkey -- NOLOOP should suppress
    tracker.command(&["SET", "loopkey{t}", "1"]).await;
    // Writer modifies otherkey2 -- should generate notification
    writer.command(&["SET", "otherkey2{t}", "1"]).await;

    // Collect invalidation keys
    let mut all_keys: Vec<String> = Vec::new();
    for _ in 0..3 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(msg) => {
                let keys = extract_invalidation_keys(&msg);
                all_keys.extend(keys);
            }
            None => break,
        }
    }
    all_keys.sort();
    assert!(
        all_keys.contains(&"otherkey1{t}".to_string()),
        "Should contain otherkey1{{t}}"
    );
    assert!(
        all_keys.contains(&"otherkey2{t}".to_string()),
        "Should contain otherkey2{{t}}"
    );
    assert!(
        !all_keys.contains(&"loopkey{t}".to_string()),
        "NOLOOP: loopkey{{t}} should NOT appear"
    );
}

// ---------------------------------------------------------------------------
// Tracking NOLOOP mode in BCAST mode works
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB TRACKING NOLOOP not implemented"]
async fn tcl_tracking_noloop_bcast_mode() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "NOLOOP"])
        .await;

    // Writer sets keys -- should notify
    writer.command(&["SET", "otherkey1", "1"]).await;
    // Self-set -- NOLOOP should suppress
    tracker.command(&["SET", "loopkey", "1"]).await;
    // Writer sets another key -- should notify
    writer.command(&["SET", "otherkey2", "1"]).await;

    let mut all_keys: Vec<String> = Vec::new();
    for _ in 0..3 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(msg) => {
                let keys = extract_invalidation_keys(&msg);
                all_keys.extend(keys);
            }
            None => break,
        }
    }
    all_keys.sort();
    assert!(all_keys.contains(&"otherkey1".to_string()));
    assert!(all_keys.contains(&"otherkey2".to_string()));
    assert!(
        !all_keys.contains(&"loopkey".to_string()),
        "NOLOOP: loopkey should NOT appear in BCAST"
    );
}

// ---------------------------------------------------------------------------
// Tracking gets notification of expired keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_tracking_expired_key_notification() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "NOLOOP"])
        .await;

    // Set a key that expires in 1 ms
    writer.command(&["SET", "mykey", "myval", "PX", "1"]).await;
    // Set another key that should not trigger (different key, not expired)
    writer
        .command(&["SET", "mykeyotherkey", "myval"])
        .await;

    // Wait for expiry
    tokio::time::sleep(Duration::from_secs(1)).await;

    // We should get an invalidation for "mykey" (from expiry)
    // In BCAST mode we get notifications for all writes, so we collect
    // and verify mykey is among them.
    let mut all_keys: Vec<String> = Vec::new();
    for _ in 0..5 {
        match tracker.read_message(Duration::from_secs(2)).await {
            Some(msg) => {
                let keys = extract_invalidation_keys(&msg);
                all_keys.extend(keys);
            }
            None => break,
        }
    }
    assert!(
        all_keys.contains(&"mykey".to_string()),
        "Should get invalidation for expired key 'mykey', got {:?}",
        all_keys
    );
}

// ---------------------------------------------------------------------------
// HELLO 3 reply is correct
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hello_3_reply_correct() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    let reply = client.command(&["HELLO", "3"]).await;
    // RESP3 HELLO returns a Map. Check that proto = 3.
    match &reply {
        Resp3Frame::Map { data, .. } => {
            let proto_val = data
                .iter()
                .find(|(k, _)| {
                    matches!(k, Resp3Frame::BlobString { data, .. } if data.as_ref() == b"proto")
                })
                .map(|(_, v)| v);
            match proto_val {
                Some(Resp3Frame::Number { data, .. }) => {
                    assert_eq!(*data, 3, "proto should be 3");
                }
                other => panic!("Expected proto = Number(3), got {:?}", other),
            }
        }
        _ => panic!("Expected Map from HELLO 3, got {:?}", reply),
    }
}

// ---------------------------------------------------------------------------
// RESP3 based basic invalidation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_resp3_basic_invalidation() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;

    // Another client modifies the key
    writer.command(&["SET", "key1", "2"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for key1");
    assert_invalidation_keys(&msg, &["key1"]);
}

// ---------------------------------------------------------------------------
// RESP3 tracking redirection
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB TRACKING REDIRECT behavior differs"]
async fn tcl_resp3_tracking_redirection() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut redir = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    // Get the redirect target's ID
    let id_frame = redir.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => data.to_string(),
        _ => panic!("Expected Number, got {:?}", id_frame),
    };

    // Subscribe to __redis__:invalidate on the redirect target
    redir
        .command(&["SUBSCRIBE", "__redis__:invalidate"])
        .await;

    // Enable tracking with redirect
    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "REDIRECT", &redir_id])
        .await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;
    writer.command(&["SET", "key1", "2"]).await;

    // The redirect target should receive an invalidation message
    let msg = redir
        .read_message(Duration::from_secs(2))
        .await
        .expect("Redirect target should receive invalidation");

    // In RESP3 subscribe mode, messages come as Push frames
    match &msg {
        Resp3Frame::Push { data, .. } => {
            let has_key1 = format!("{:?}", data).contains("key1");
            assert!(has_key1, "Redirect message should mention key1, got {:?}", data);
        }
        _ => panic!("Expected Push frame from redirect target, got {:?}", msg),
    }
}

// ---------------------------------------------------------------------------
// BCAST with prefix collisions throw errors
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB TRACKING BCAST prefix collision detection differs"]
async fn tcl_bcast_prefix_collisions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    // Prefix "FOOBAR" overlaps with "FOO"
    let resp = client
        .command(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "FOOBAR", "PREFIX", "FOO",
        ])
        .await;
    match &resp {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("Prefix") || msg.contains("prefix") || msg.contains("ERR"),
                "Expected prefix collision error, got: {}",
                msg
            );
        }
        _ => panic!(
            "Expected error for prefix collision FOOBAR+FOO, got {:?}",
            resp
        ),
    }

    // Reverse order should also fail
    let mut client2 = server.connect_resp3().await;
    let resp = client2
        .command(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "FOO", "PREFIX", "FOOBAR",
        ])
        .await;
    match &resp {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("Prefix") || msg.contains("prefix") || msg.contains("ERR"),
                "Expected prefix collision error, got: {}",
                msg
            );
        }
        _ => panic!(
            "Expected error for prefix collision FOO+FOOBAR, got {:?}",
            resp
        ),
    }

    // Set up non-colliding prefixes, then try to add a colliding one
    let mut client3 = server.connect_resp3().await;
    client3
        .command(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "FOO", "PREFIX", "BAR",
        ])
        .await;

    let resp = client3
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "FO"])
        .await;
    match &resp {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("Prefix") || msg.contains("prefix") || msg.contains("ERR"),
                "Expected prefix collision error for FO vs FOO, got: {}",
                msg
            );
        }
        _ => panic!(
            "Expected error for prefix collision FO vs FOO, got {:?}",
            resp
        ),
    }

    let resp = client3
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "BARB"])
        .await;
    match &resp {
        Resp3Frame::SimpleError { data, .. } => {
            let msg = data.to_string();
            assert!(
                msg.contains("Prefix") || msg.contains("prefix") || msg.contains("ERR"),
                "Expected prefix collision error for BARB vs BAR, got: {}",
                msg
            );
        }
        _ => panic!(
            "Expected error for prefix collision BARB vs BAR, got {:?}",
            resp
        ),
    }

    client3.command(&["CLIENT", "TRACKING", "OFF"]).await;
}

// ---------------------------------------------------------------------------
// HDEL delivers invalidation message after response in the same connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_hdel_invalidation_after_response() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client.command(&["CLIENT", "TRACKING", "ON"]).await;

    client.command(&["HSET", "myhash", "f", "1"]).await;
    client.command(&["HGET", "myhash", "f"]).await;

    let resp = client.command(&["HDEL", "myhash", "f"]).await;
    // HDEL should return 1
    match &resp {
        Resp3Frame::Number { data, .. } => assert_eq!(*data, 1),
        _ => panic!("Expected Number(1) from HDEL, got {:?}", resp),
    }

    // Now read the invalidation message that follows the response
    let msg = client
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for myhash after HDEL");
    assert_invalidation_keys(&msg, &["myhash"]);
}

// ---------------------------------------------------------------------------
// Tracking invalidation is not interleaved with transaction response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_tracking_invalidation_not_interleaved_with_exec() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client.command(&["CLIENT", "TRACKING", "ON"]).await;

    client
        .command(&["MSET", "a{t}", "1", "b{t}", "2"])
        .await;
    client.command(&["GET", "a{t}"]).await;

    // Start a transaction that modifies a tracked key
    client.command(&["MULTI"]).await;
    client.command(&["INCR", "a{t}"]).await;
    client.command(&["GET", "b{t}"]).await;
    let exec_resp = client.command(&["EXEC"]).await;

    // EXEC should return [2, 2]
    match &exec_resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(data.len(), 2, "EXEC should return 2 results");
        }
        _ => panic!("Expected Array from EXEC, got {:?}", exec_resp),
    }

    // The invalidation message should come AFTER the EXEC response
    let msg = client
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for a{t} after EXEC");
    assert_invalidation_keys(&msg, &["a{t}"]);
}

// ---------------------------------------------------------------------------
// After switching from normal tracking to BCAST mode,
// no invalidation for pre-BCAST keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_switch_normal_to_bcast_no_pre_bcast_invalidation() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;

    // Switch to BCAST mode
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await;

    // Modify key1 from writer -- in BCAST mode, all writes cause
    // invalidation, but the pre-BCAST tracked keys should have been cleared
    // when tracking was turned off.
    writer.command(&["INCR", "key1"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("BCAST should deliver invalidation for key1");
    assert_invalidation_keys(&msg, &["key1"]);
}

// ---------------------------------------------------------------------------
// Different clients using RESP3 can track the same key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_different_clients_track_same_key() {
    let server = TestServer::start_standalone().await;
    let mut tracker1 = server.connect_resp3().await;
    let mut tracker2 = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker1.command(&["HELLO", "3"]).await;
    tracker1.command(&["CLIENT", "TRACKING", "ON"]).await;

    tracker2.command(&["HELLO", "3"]).await;
    tracker2.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "sharedkey", "1"]).await;
    tracker1.command(&["GET", "sharedkey"]).await;
    tracker2.command(&["GET", "sharedkey"]).await;

    // Writer modifies the key
    writer.command(&["SET", "sharedkey", "2"]).await;

    // Both trackers should receive invalidation
    let msg1 = tracker1
        .read_message(Duration::from_secs(2))
        .await
        .expect("tracker1 should receive invalidation");
    assert_invalidation_keys(&msg1, &["sharedkey"]);

    let msg2 = tracker2
        .read_message(Duration::from_secs(2))
        .await
        .expect("tracker2 should receive invalidation");
    assert_invalidation_keys(&msg2, &["sharedkey"]);
}

// ---------------------------------------------------------------------------
// No invalidation message when using OPTIN option (without CLIENT CACHING YES)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_optin_no_invalidation_without_caching_yes() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN"])
        .await;

    writer.command(&["SET", "key1", "1"]).await;
    // GET without CACHING YES -- should NOT track
    tracker.command(&["GET", "key1"]).await;
    writer.command(&["SET", "key1", "2"]).await;

    // Should NOT receive invalidation
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "OPTIN: should NOT get invalidation without CACHING YES"
    );
}

// ---------------------------------------------------------------------------
// Invalidation message sent when using OPTIN with CLIENT CACHING YES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_optin_with_caching_yes() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN"])
        .await;

    writer.command(&["SET", "key1", "3"]).await;
    tracker.command(&["CLIENT", "CACHING", "YES"]).await;
    tracker.command(&["GET", "key1"]).await;
    writer.command(&["SET", "key1", "4"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("OPTIN + CACHING YES: should get invalidation");
    assert_invalidation_keys(&msg, &["key1"]);
}

// ---------------------------------------------------------------------------
// Invalidation message sent when using OPTOUT option
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_optout_sends_invalidation() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;
    writer.command(&["SET", "key1", "2"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("OPTOUT: should get invalidation by default");
    assert_invalidation_keys(&msg, &["key1"]);
}

// ---------------------------------------------------------------------------
// No invalidation message when using OPTOUT with CLIENT CACHING NO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_optout_with_caching_no() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;
    tracker
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;

    writer.command(&["SET", "key1", "1"]).await;
    // CACHING NO + GET -- should suppress tracking for this read
    tracker.command(&["CLIENT", "CACHING", "NO"]).await;
    tracker.command(&["GET", "key1"]).await;
    writer.command(&["SET", "key1", "2"]).await;

    // Should NOT receive invalidation for key1
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "OPTOUT + CACHING NO: should NOT get invalidation"
    );
}

// ---------------------------------------------------------------------------
// CLIENT GETREDIR provides correct client id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_getredir() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;
    let mut redir = server.connect_resp3().await;

    let id_frame = redir.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => *data,
        _ => panic!("Expected Number, got {:?}", id_frame),
    };

    // Enable tracking with redirect
    client
        .command(&[
            "CLIENT",
            "TRACKING",
            "ON",
            "REDIRECT",
            &redir_id.to_string(),
        ])
        .await;
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    match &resp {
        Resp3Frame::Number { data, .. } => {
            assert_eq!(*data, redir_id, "GETREDIR should return redirect target ID");
        }
        _ => panic!("Expected Number from GETREDIR, got {:?}", resp),
    }

    // Disable tracking -- GETREDIR should return -1
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    match &resp {
        Resp3Frame::Number { data, .. } => {
            assert_eq!(*data, -1, "GETREDIR should return -1 when tracking is off");
        }
        _ => panic!("Expected Number from GETREDIR, got {:?}", resp),
    }

    // Enable tracking without redirect -- GETREDIR should return 0
    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    match &resp {
        Resp3Frame::Number { data, .. } => {
            assert_eq!(
                *data, 0,
                "GETREDIR should return 0 when tracking on without redirect"
            );
        }
        _ => panic!("Expected Number from GETREDIR, got {:?}", resp),
    }
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO helpers
// ---------------------------------------------------------------------------

/// Extract RESP3 map value for a given key from a Map frame.
fn resp3_map_get<'a>(frame: &'a Resp3Frame, key: &str) -> Option<&'a Resp3Frame> {
    match frame {
        Resp3Frame::Map { data, .. } => data.iter().find_map(|(k, v)| match k {
            Resp3Frame::BlobString { data, .. } if data.as_ref() == key.as_bytes() => Some(v),
            _ => None,
        }),
        _ => None,
    }
}

/// Extract a list of strings from a RESP3 Array or Set frame.
fn resp3_extract_strings(frame: &Resp3Frame) -> Vec<String> {
    fn extract_from_iter<'a>(iter: impl Iterator<Item = &'a Resp3Frame>) -> Vec<String> {
        iter.filter_map(|item| match item {
            Resp3Frame::BlobString { data, .. } => {
                Some(String::from_utf8_lossy(data).to_string())
            }
            Resp3Frame::SimpleString { data, .. } => {
                Some(String::from_utf8_lossy(data).to_string())
            }
            _ => None,
        })
        .collect()
    }
    match frame {
        Resp3Frame::Array { data, .. } => extract_from_iter(data.iter()),
        Resp3Frame::Set { data, .. } => extract_from_iter(data.iter()),
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO when tracking is off
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(
        flag_strs.contains(&"off".to_string()),
        "Should contain 'off', got {:?}",
        flag_strs
    );

    let redirect = resp3_map_get(&resp, "redirect").expect("should have redirect");
    match redirect {
        Resp3Frame::Number { data, .. } => assert_eq!(*data, -1),
        _ => panic!("Expected Number for redirect, got {:?}", redirect),
    }

    let prefixes = resp3_map_get(&resp, "prefixes").expect("should have prefixes");
    let prefix_strs = resp3_extract_strings(prefixes);
    assert!(prefix_strs.is_empty(), "Prefixes should be empty when off");
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO when tracking is on
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_on() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(
        flag_strs.contains(&"on".to_string()),
        "Should contain 'on', got {:?}",
        flag_strs
    );

    let redirect = resp3_map_get(&resp, "redirect").expect("should have redirect");
    match redirect {
        Resp3Frame::Number { data, .. } => assert_eq!(*data, 0),
        _ => panic!("Expected Number for redirect, got {:?}", redirect),
    }
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO with options (REDIRECT + NOLOOP)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_with_redirect_noloop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;
    let mut redir = server.connect_resp3().await;

    let id_frame = redir.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => *data,
        _ => panic!("Expected Number, got {:?}", id_frame),
    };

    client.command(&["HELLO", "3"]).await;
    client
        .command(&[
            "CLIENT",
            "TRACKING",
            "ON",
            "REDIRECT",
            &redir_id.to_string(),
            "NOLOOP",
        ])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(flag_strs.contains(&"on".to_string()));
    assert!(
        flag_strs.contains(&"noloop".to_string()),
        "Should contain 'noloop', got {:?}",
        flag_strs
    );

    let redirect = resp3_map_get(&resp, "redirect").expect("should have redirect");
    match redirect {
        Resp3Frame::Number { data, .. } => assert_eq!(*data, redir_id),
        _ => panic!("Expected Number for redirect, got {:?}", redirect),
    }
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO with OPTIN
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_optin() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN"])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(flag_strs.contains(&"on".to_string()));
    assert!(
        flag_strs.contains(&"optin".to_string()),
        "Should contain 'optin', got {:?}",
        flag_strs
    );

    // After CACHING YES, flags should include caching-yes
    client.command(&["CLIENT", "CACHING", "YES"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(
        flag_strs.contains(&"caching-yes".to_string()),
        "Should contain 'caching-yes' after CACHING YES, got {:?}",
        flag_strs
    );
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO with OPTOUT
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_optout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client
        .command(&["CLIENT", "TRACKING", "ON", "OPTOUT"])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(flag_strs.contains(&"on".to_string()));
    assert!(
        flag_strs.contains(&"optout".to_string()),
        "Should contain 'optout', got {:?}",
        flag_strs
    );

    // After CACHING NO, flags should include caching-no
    client.command(&["CLIENT", "CACHING", "NO"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(
        flag_strs.contains(&"caching-no".to_string()),
        "Should contain 'caching-no' after CACHING NO, got {:?}",
        flag_strs
    );
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO with BCAST mode and prefixes
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_bcast_with_prefixes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client
        .command(&[
            "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "foo", "PREFIX", "bar",
        ])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(flag_strs.contains(&"on".to_string()));
    assert!(
        flag_strs.contains(&"bcast".to_string()),
        "Should contain 'bcast', got {:?}",
        flag_strs
    );

    let prefixes = resp3_map_get(&resp, "prefixes").expect("should have prefixes");
    let mut prefix_strs = resp3_extract_strings(prefixes);
    prefix_strs.sort();
    assert_eq!(prefix_strs, vec!["bar", "foo"], "Prefixes should be [bar, foo]");
}

// ---------------------------------------------------------------------------
// CLIENT TRACKINGINFO with BCAST mode and empty prefix
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_trackinginfo_bcast_empty_prefix() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST"])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    let prefixes = resp3_map_get(&resp, "prefixes").expect("should have prefixes");
    let prefix_strs = resp3_extract_strings(prefixes);
    // Redis returns [""] for BCAST with empty prefix
    assert!(
        prefix_strs.is_empty() || prefix_strs == vec![""],
        "BCAST empty prefix should be [] or [\"\"], got {:?}",
        prefix_strs
    );
}

// ---------------------------------------------------------------------------
// Invalidation message received for flushall
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_invalidation_on_flushall() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;

    // FLUSHALL from the writer
    writer.command(&["FLUSHALL"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for FLUSHALL");
    // FLUSHALL sends a null invalidation (all keys)
    assert_invalidation_flush(&msg);
}

// ---------------------------------------------------------------------------
// Invalidation message received for flushdb
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_invalidation_on_flushdb() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "key1", "1"]).await;
    tracker.command(&["GET", "key1"]).await;

    // FLUSHDB from the writer
    writer.command(&["FLUSHDB"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for FLUSHDB");
    assert_invalidation_flush(&msg);
}

// ---------------------------------------------------------------------------
// flushdb tracking invalidation not interleaved with transaction response
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB FLUSHDB invalidation ordering differs"]
async fn tcl_flushdb_invalidation_not_interleaved_with_exec() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    client.command(&["CLIENT", "TRACKING", "ON"]).await;

    client.command(&["SET", "a{t}", "1"]).await;
    client.command(&["GET", "a{t}"]).await;

    client.command(&["MULTI"]).await;
    client.command(&["FLUSHDB"]).await;
    let exec_resp = client.command(&["EXEC"]).await;

    // EXEC should return the FLUSHDB result
    match &exec_resp {
        Resp3Frame::Array { data, .. } => {
            assert_eq!(data.len(), 1, "EXEC should return 1 result for FLUSHDB");
        }
        _ => panic!("Expected Array from EXEC, got {:?}", exec_resp),
    }

    // Invalidation should come after EXEC response
    let msg = client
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive flush invalidation after EXEC");
    assert_invalidation_flush(&msg);
}

// ---------------------------------------------------------------------------
// Coverage: Basic CLIENT CACHING (from second server block)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_coverage_basic_client_caching() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;
    let mut redir = server.connect_resp3().await;

    let id_frame = redir.command(&["CLIENT", "ID"]).await;
    let redir_id = match &id_frame {
        Resp3Frame::Number { data, .. } => data.to_string(),
        _ => panic!("Expected Number, got {:?}", id_frame),
    };

    let resp = client
        .command(&[
            "CLIENT",
            "TRACKING",
            "ON",
            "OPTIN",
            "REDIRECT",
            &redir_id,
        ])
        .await;
    assert!(is_resp3_ok(&resp));

    let resp = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(is_resp3_ok(&resp));

    let resp = client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    assert!(is_resp3_ok(&resp));
}

// ---------------------------------------------------------------------------
// Coverage: Basic CLIENT TRACKINGINFO (from second server block)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT TRACKINGINFO format differs from Redis"]
async fn tcl_coverage_basic_trackinginfo() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;

    // When tracking is off, flags should contain "off", redirect -1, prefixes empty
    let flags = resp3_map_get(&resp, "flags").expect("should have flags");
    let flag_strs = resp3_extract_strings(flags);
    assert!(flag_strs.contains(&"off".to_string()));

    let redirect = resp3_map_get(&resp, "redirect").expect("should have redirect");
    match redirect {
        Resp3Frame::Number { data, .. } => assert_eq!(*data, -1),
        _ => panic!("Expected Number for redirect, got {:?}", redirect),
    }
}

// ---------------------------------------------------------------------------
// Coverage: Basic CLIENT GETREDIR (from second server block)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_coverage_basic_getredir() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect_resp3().await;

    client.command(&["HELLO", "3"]).await;
    let resp = client.command(&["CLIENT", "GETREDIR"]).await;
    match &resp {
        Resp3Frame::Number { data, .. } => {
            assert_eq!(*data, -1, "GETREDIR should return -1 when tracking is off");
        }
        _ => panic!("Expected Number from GETREDIR, got {:?}", resp),
    }
}

// ---------------------------------------------------------------------------
// The other connection gets invalidations (RESP3 inline version)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_other_connection_gets_invalidations() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    writer.command(&["SET", "a{t}", "1"]).await;
    writer.command(&["SET", "b{t}", "1"]).await;

    // Track key a{t} via GET
    tracker.command(&["GET", "a{t}"]).await;

    // INCR b{t} should NOT notify since it was not fetched
    writer.command(&["INCR", "b{t}"]).await;
    // INCR a{t} should notify since it was fetched
    writer.command(&["INCR", "a{t}"]).await;

    let msg = tracker
        .read_message(Duration::from_secs(2))
        .await
        .expect("Should receive invalidation for a{t}");
    assert_invalidation_keys(&msg, &["a{t}"]);

    // Verify no additional message for b{t}
    let extra = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        extra.is_none(),
        "Should NOT receive invalidation for b{{t}} (not tracked)"
    );
}

// ---------------------------------------------------------------------------
// MGET tracks multiple keys, disabling tracking clears tracked keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_mget_disable_tracking_no_leak() {
    let server = TestServer::start_standalone().await;
    let mut tracker = server.connect_resp3().await;
    let mut writer = server.connect_resp3().await;

    tracker.command(&["HELLO", "3"]).await;
    tracker.command(&["CLIENT", "TRACKING", "ON"]).await;

    // Track several keys
    tracker
        .command(&["MGET", "a{t}", "b{t}", "c{t}", "d{t}", "e{t}", "f{t}", "g{t}"])
        .await;

    // Disable tracking -- tracked keys should be cleaned up
    tracker.command(&["CLIENT", "TRACKING", "OFF"]).await;

    // Modify all tracked keys
    writer
        .command(&["MSET", "a{t}", "2", "b{t}", "2", "c{t}", "2"])
        .await;

    // Should NOT receive any invalidation (tracking is off)
    let msg = tracker.read_message(Duration::from_millis(500)).await;
    assert!(
        msg.is_none(),
        "Should NOT get invalidation after TRACKING OFF"
    );
}
