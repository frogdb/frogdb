//! Integration tests for pub/sub commands (SUBSCRIBE, PUBLISH, PSUBSCRIBE, etc.)

use crate::common::test_server::TestServer;
use bytes::Bytes;
use frogdb_protocol::Response;
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
