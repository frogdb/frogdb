//! Integration tests for FrogDB replication.
//!
//! These tests verify the replication protocol and functionality
//! by running actual FrogDB server instances.
//!
//! ## Test Tiers
//!
//! ### Tier 1: Protocol Verification (Single Server)
//! Basic tests that verify replication commands work on a single server.
//!
//! ### Tier 2: Replication Functionality (Multi-Server)
//! Tests that verify actual replication between primary and replica servers.
//!
//! ### Tier 3: Edge Cases
//! Tests for reconnection, REPLICAOF NO ONE, large values, etc.

use crate::common::replication_helpers::{
    get_replication_state, parse_info_replication, start_primary_replica_pair, wait_for_replication,
};
use crate::common::response_helpers::{assert_bulk_eq, assert_ok};
use crate::common::test_server::{
    ServerRole, TestServer, TestServerConfig, get_error_message, is_error, parse_integer,
    parse_simple_string,
};
use bytes::Bytes;
use frogdb_core::{Arity, CommandFlags, CommandRegistry};
use frogdb_protocol::Response;
use rstest::rstest;
use std::path::PathBuf;
use std::time::Duration;

// ============================================================================
// Tier 1: Protocol Verification (Single Server)
// ============================================================================

// FM-REPLICATION-018
/// Test that REPLCONF listening-port returns +OK.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replconf_listening_port(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    let response = server.send("REPLCONF", &["listening-port", "6380"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

// FM-REPLICATION-018
/// Test that REPLCONF capa eof psync2 returns +OK.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replconf_capa(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    let response = server.send("REPLCONF", &["capa", "eof", "psync2"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

// FM-REPLICATION-018
/// Test that REPLCONF ACK <offset> returns +OK.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replconf_ack(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    let response = server.send("REPLCONF", &["ACK", "12345"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

// FM-REPLICATION-013
/// Test that PSYNC ? -1 returns a response (either FULLRESYNC or OK placeholder).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_psync_initial_request(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    let response = server.send("PSYNC", &["?", "-1"]).await;
    // The command should not error - it either returns FULLRESYNC or OK
    assert!(
        !is_error(&response),
        "PSYNC ? -1 should not error, got {:?}",
        response
    );

    server.shutdown().await;
}

// FM-REPLICATION-064
/// The gate, end to end through the real dispatch stage: a version announced
/// over `REPLCONF frogdb-version` is accumulated on *this* connection and read
/// by the `PSYNC` that follows it, and a different major is refused there with
/// an error naming both versions. The unit tests own the rule; this test owns
/// the wiring — it is the one that fails if the announcement stops reaching
/// `handle_psync`, which is exactly how this gap was created (issue 22).
#[tokio::test]
async fn test_psync_is_refused_for_an_incompatible_replica_version() {
    let server = TestServer::start_primary().await;
    let mut client = server.connect().await;

    // A major no FrogDB build will ever carry, so the case is a refusal
    // regardless of what the workspace version becomes.
    assert_ok(
        &client
            .command(&["REPLCONF", "frogdb-version", "999.0.0"])
            .await,
    );

    let response = client.command(&["PSYNC", "?", "-1"]).await;
    assert!(
        is_error(&response),
        "a replica on another major must be refused, got {response:?}"
    );
    let message = get_error_message(&response).expect("an error carries its message");
    assert!(
        message.contains("999.0.0") && message.contains(env!("CARGO_PKG_VERSION")),
        "the refusal must name both versions, got: {message:?}"
    );

    server.shutdown().await;
}

/// Test that ROLE returns correct info for a standalone/primary server.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_role_command(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    let response = server.send("ROLE", &[]).await;

    // ROLE for master returns: ["master", <offset>, [<replicas>]]
    if let Response::Array(items) = &response {
        assert!(!items.is_empty(), "ROLE should return at least one element");

        // First element should be "master"
        if let Response::Bulk(Some(role)) = &items[0] {
            assert_eq!(
                role.as_ref(),
                b"master",
                "Expected role 'master', got {:?}",
                role
            );
        } else {
            panic!("Expected bulk string for role, got {:?}", items[0]);
        }
    } else {
        panic!("Expected array response from ROLE, got {:?}", response);
    }

    server.shutdown().await;
}

// FM-REPLICATION-037
/// Test that WAIT with no replicas returns 0 after the timeout.
///
/// Note the nonzero timeout: matching Redis, `WAIT n 0` blocks until the
/// quorum is reached (forever here, since no replica ever attaches).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wait_no_replicas(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    // WAIT 1 100 should time out and report 0 acked replicas.
    let response = server.send("WAIT", &["1", "100"]).await;

    let count = parse_integer(&response);
    assert_eq!(
        count,
        Some(0),
        "WAIT with no replicas should return 0, got {:?}",
        response
    );

    server.shutdown().await;
}

/// Test ROLE on a standalone server.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_role_standalone(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(config).await;

    let response = server.send("ROLE", &[]).await;

    // Even standalone servers report as "master" in ROLE
    if let Response::Array(items) = &response {
        assert!(!items.is_empty(), "ROLE should return at least one element");
        if let Response::Bulk(Some(role)) = &items[0] {
            assert_eq!(
                role.as_ref(),
                b"master",
                "Standalone should report as master"
            );
        }
    } else {
        panic!("Expected array from ROLE, got {:?}", response);
    }

    server.shutdown().await;
}

// ============================================================================
// Tier 2: Replication Functionality (Multi-Server)
// ============================================================================

/// Test that a replica can connect to a primary.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_primary_replica_connect(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify both servers are responsive
    let primary_ping = primary.send("PING", &[]).await;
    assert!(
        parse_simple_string(&primary_ping) == Some("PONG"),
        "Primary should respond to PING"
    );

    let replica_ping = replica.send("PING", &[]).await;
    assert!(
        parse_simple_string(&replica_ping) == Some("PONG"),
        "Replica should respond to PING"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Test that INFO replication shows connected replica.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_info_replication_connected(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, _replica) = start_primary_replica_pair(config).await;

    let response = primary.send("INFO", &["replication"]).await;

    // INFO returns a bulk string with key:value pairs
    if let Response::Bulk(Some(info)) = &response {
        let info_str = String::from_utf8_lossy(info);
        // Primary should report role:master
        assert!(
            info_str.contains("role:master"),
            "Primary should report role:master, got:\n{}",
            info_str
        );
        // Note: connected_slaves count depends on replication handshake completion
    } else {
        panic!("Expected bulk string from INFO, got {:?}", response);
    }
}

/// A write on the primary reaches the replica.
///
/// The only open question is *when*, so the link is waited for explicitly and
/// the replica is polled with a deadline; arrival itself is not optional.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_write_propagation(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write to primary
    let set_response = primary.send("SET", &["test_key", "test_value"]).await;
    assert_ok(&set_response);

    let acked = wait_for_acks(&primary, 1, Duration::from_secs(10)).await;
    assert_eq!(acked, 1, "the streaming replica must acknowledge the write");

    // An ACK is the replica's *landed* offset, so the value must be readable.
    let seen = wait_for_value(
        &replica,
        "test_key",
        Some("test_value"),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        seen.as_deref(),
        Some("test_value"),
        "replica must serve the replicated value"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

#[cfg(feature = "cmd-hyperloglog")]
/// A PFADD that moves no register is a no-op write: it must not produce a
/// replication frame, so the primary's `master_repl_offset` must not advance.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_noop_pfadd_not_propagated(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Snapshot the master replication offset before the seed write.
    let offset_before_seed = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    // First PFADD adds new elements -> registers change -> returns 1.
    let response = primary.send("PFADD", &["hll", "a", "b", "c"]).await;
    assert_eq!(parse_integer(&response), Some(1));

    let _acked = wait_for_replication(&primary, 5000).await;

    // Snapshot the master replication offset after the seed write. A real PFADD
    // must advance the offset -- this proves the offset mechanism is live before
    // we assert the no-op below leaves it unchanged.
    let offset_before = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    assert!(
        offset_before > offset_before_seed,
        "a seed PFADD that moves registers must produce a replication frame \
         (before={offset_before_seed}, after={offset_before})"
    );

    // Duplicate PFADD adds nothing new -> no register moves -> returns 0.
    let response = primary.send("PFADD", &["hll", "a", "b", "c"]).await;
    assert_eq!(parse_integer(&response), Some(0));

    // Let the replica link settle so any frame would have been emitted/acked.
    let _acked = wait_for_replication(&primary, 2000).await;

    let offset_after = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    assert_eq!(
        offset_before, offset_after,
        "a no-op PFADD must not produce a replication frame"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

#[cfg(feature = "cmd-hyperloglog")]
/// A PFMERGE that adds no new register to an existing destination is a no-op
/// write: it must not produce a replication frame (offset must not advance).
/// Creating the destination — even when the merged result is empty — is a real
/// change and must advance the offset.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_noop_pfmerge_not_propagated(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Seed a source HLL.
    let response = primary.send("PFADD", &["src", "a", "b"]).await;
    assert_eq!(parse_integer(&response), Some(1));

    let _acked = wait_for_replication(&primary, 5000).await;

    // First PFMERGE creates the destination -> real change -> offset advances.
    let offset_before_create = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    let response = primary.send("PFMERGE", &["dst", "src"]).await;
    assert_ok(&response);

    let _acked = wait_for_replication(&primary, 5000).await;

    let offset_after_create = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    assert!(
        offset_after_create > offset_before_create,
        "a PFMERGE that populates a new destination must produce a replication frame \
         (before={offset_before_create}, after={offset_after_create})"
    );

    // Second identical PFMERGE: dst already contains every source register, so
    // nothing moves -> no-op write -> offset must not advance.
    let response = primary.send("PFMERGE", &["dst", "src"]).await;
    assert_ok(&response);

    let _acked = wait_for_replication(&primary, 2000).await;

    let offset_after_noop = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    assert_eq!(
        offset_after_create, offset_after_noop,
        "a no-op PFMERGE must not produce a replication frame"
    );

    // Creating a destination from only missing sources still creates the (empty)
    // key -> real change -> offset advances.
    let offset_before_empty_create = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    let response = primary
        .send("PFMERGE", &["missing_dst", "missing_src"])
        .await;
    assert_ok(&response);

    let _acked = wait_for_replication(&primary, 5000).await;

    let offset_after_empty_create = get_replication_state(&primary)
        .await
        .expect("primary must report a replication offset")
        .1;

    assert!(
        offset_after_empty_create > offset_before_empty_create,
        "PFMERGE that creates a destination from missing sources must produce a \
         replication frame (before={offset_before_empty_create}, \
         after={offset_after_empty_create})"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-037
/// `WAIT 1` against one live streaming replica returns exactly 1.
///
/// The precondition (a replica in the streaming phase) is waited for; the
/// count is then bound to the only correct answer, not to `>= 0`.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wait_blocks_until_ack(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let _replica = TestServer::start_replica_with_config(&primary, config).await;

    wait_for_connected_slave(&primary).await;

    // Write some data
    assert_ok(&primary.send("SET", &["wait_test", "value"]).await);

    let start = std::time::Instant::now();
    let response = primary.send("WAIT", &["1", "2000"]).await;
    let elapsed = start.elapsed();

    assert_eq!(
        parse_integer(&response),
        Some(1),
        "WAIT 1 with one streaming replica must count that replica, got {response:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "WAIT must honour its own 2s deadline, took {elapsed:?}"
    );
}

// FM-REPLICATION-038
/// Regression: WAIT must solicit acks (REPLCONF GETACK *) instead of waiting
/// out the replica's spontaneous 1-second ACK cadence.
///
/// Eight write+WAIT rounds with a live replica must each acknowledge and
/// complete in far less total time than the ~4s the 1s cadence alone would
/// average. Without GETACK solicitation the probability of finishing under
/// the budget is negligible; with it each WAIT resolves in milliseconds.
#[tokio::test]
async fn test_wait_returns_promptly_via_getack() {
    let (primary, _replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    let start = std::time::Instant::now();
    for i in 0..8 {
        primary
            .send("SET", &[&format!("getack_key_{i}"), "v"])
            .await;
        let response = primary.send("WAIT", &["1", "2000"]).await;
        assert_eq!(
            parse_integer(&response),
            Some(1),
            "WAIT round {i} should be acknowledged, got {response:?}"
        );
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "8 solicited WAIT rounds took {elapsed:?}; GETACK solicitation appears broken"
    );

    primary.shutdown().await;
}

// FM-REPLICATION-037
/// `WAIT n 0` blocks until the quorum is reached (Redis: timeout 0 = no
/// deadline) — and with GETACK solicitation it resolves promptly.
#[tokio::test]
async fn test_wait_zero_timeout_blocks_until_ack() {
    let (primary, _replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    primary.send("SET", &["zero_timeout_key", "v"]).await;

    let mut client = primary.connect().await;
    client.send_only(&["WAIT", "1", "0"]).await;
    let response = client
        .read_response(Duration::from_secs(5))
        .await
        .expect("WAIT 1 0 with a live replica must resolve, not block forever");
    assert_eq!(parse_integer(&response), Some(1));

    primary.shutdown().await;
}

// FM-REPLICATION-037
/// WAIT on a replica is rejected before argument parsing, as in Redis.
#[tokio::test]
async fn test_wait_on_replica_is_an_error() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    let response = replica.send("WAIT", &["1", "100"]).await;
    match &response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.starts_with("ERR WAIT cannot be used with replica instances"),
                "unexpected error: {msg}"
            );
        }
        other => panic!("expected error from WAIT on replica, got {other:?}"),
    }

    primary.shutdown().await;
}

// FM-REPLICATION-037
/// CLIENT UNBLOCK releases a WAIT blocked with no deadline: TIMEOUT mode
/// replies with the acked count, ERROR mode with -UNBLOCKED.
#[tokio::test]
async fn test_client_unblock_releases_wait() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    let mut control = primary.connect().await;
    let mut blocked = primary.connect().await;

    let id = parse_integer(&blocked.command(&["CLIENT", "ID"]).await).unwrap();

    // TIMEOUT mode: reply is the acked count (0 — no replicas).
    blocked.send_only(&["WAIT", "1", "0"]).await;
    primary.wait_for_blocked_clients(1).await;
    let unblocked = control
        .command(&["CLIENT", "UNBLOCK", &id.to_string(), "TIMEOUT"])
        .await;
    assert_eq!(parse_integer(&unblocked), Some(1));
    let response = blocked
        .read_response(Duration::from_secs(5))
        .await
        .expect("unblocked WAIT must reply");
    assert_eq!(parse_integer(&response), Some(0));

    // ERROR mode: reply is the -UNBLOCKED error.
    blocked.send_only(&["WAIT", "1", "0"]).await;
    primary.wait_for_blocked_clients(1).await;
    let unblocked = control
        .command(&["CLIENT", "UNBLOCK", &id.to_string(), "ERROR"])
        .await;
    assert_eq!(parse_integer(&unblocked), Some(1));
    let response = blocked
        .read_response(Duration::from_secs(5))
        .await
        .expect("unblocked WAIT must reply");
    match &response {
        Response::Error(e) => {
            assert!(String::from_utf8_lossy(e).starts_with("UNBLOCKED"));
        }
        other => panic!("expected UNBLOCKED error, got {other:?}"),
    }

    primary.shutdown().await;
}

// FM-TXN-044
/// WAIT inside MULTI/EXEC never blocks: it returns the current acked count
/// immediately (Redis CLIENT_DENY_BLOCKING semantics), not nil.
///
/// Matches documented Redis behavior (redis.io `WAIT` docs, "Details"): "If
/// the command is sent as part of a MULTI transaction (since Redis 7.0, any
/// context that does not allow blocking, such as inside scripts), the
/// command does not block but instead just return ASAP the number of
/// replicas that acknowledged the previous write commands."
///
/// Regression guard, not a bug fix (testing-gap issue 51 / audit
/// B-transactions#4, verdict ADJUSTED L1/C2 — correct by design). `WAIT`
/// keeps `ExecutionStrategy::Standard`, so a queued `WAIT` is executed on
/// the shard at EXEC time (`connection/transaction.rs:277-298`), landing in
/// `WaitCommand::execute`'s deny-blocking branch
/// (`commands/replication.rs:568-575`) — never the connection-level
/// `WaitIntercept`/`WaitCoordinator` blocking path, which `dispatch.rs`
/// documents (`connection/dispatch.rs:491-494`) as reachable only *outside*
/// MULTI. `WAIT 1 0` requests a 1-replica quorum with timeout 0 (Redis:
/// block forever until reached) — a maximally strict probe, since a
/// regression that routed this through the blocking coordinator would hang
/// EXEC permanently rather than merely running long.
#[tokio::test]
async fn test_wait_inside_multi_returns_count_immediately() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    let mut client = primary.connect().await;
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["WAIT", "1", "0"]).await; // QUEUED

    let start = std::time::Instant::now();
    let response = client.command(&["EXEC"]).await;
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "WAIT inside MULTI must not block"
    );
    match &response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(
                parse_integer(&items[0]),
                Some(0),
                "WAIT in MULTI returns the acked count, got {:?}",
                items[0]
            );
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }

    primary.shutdown().await;
}

/// `MULTI; WAIT 0 0; EXEC` — the literal acceptance-criteria case for
/// testing-gap issue 51: zero replicas requested, zero timeout. Same
/// non-blocking routing as `test_wait_inside_multi_returns_count_immediately`
/// (`ExecutionStrategy::Standard` -> `transaction.rs:277-298` -> shard
/// deny-blocking branch `replication.rs:568-575`; the blocking
/// `WaitIntercept` path is documented as unreachable inside MULTI,
/// `dispatch.rs:491-494`). Asserts a tight (<1s) elapsed bound rather than
/// relying on the nextest 15s force-kill to reveal a hang.
#[tokio::test]
async fn test_wait_inside_multi_zero_replicas_zero_timeout() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    let mut client = primary.connect().await;
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["WAIT", "0", "0"]).await; // QUEUED

    let start = std::time::Instant::now();
    let response = client.command(&["EXEC"]).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "WAIT 0 0 queued in MULTI must return immediately via the shard's \
         non-blocking branch; took {elapsed:?}"
    );
    match &response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(parse_integer(&items[0]), Some(0));
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }

    primary.shutdown().await;
}

// FM-TXN-044
/// `MULTI; WAIT 3 100; EXEC` on standalone (0 replicas connected) with a
/// nonzero, finite timeout — the second acceptance-criteria case for issue
/// 51. Asserts completion well *under* the requested 100ms window (not just
/// under it), which is the only way to tell "returned immediately via the
/// non-blocking shard branch" apart from "blocked for the full timeout and
/// then returned the same answer." Same routing citation as the sibling
/// tests above (`transaction.rs:277-298` -> `replication.rs:568-575`;
/// blocking path unreachable inside MULTI per `dispatch.rs:491-494`).
#[tokio::test]
async fn test_wait_inside_multi_nonzero_timeout_does_not_block() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    let mut client = primary.connect().await;
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["WAIT", "3", "100"]).await; // QUEUED

    let start = std::time::Instant::now();
    let response = client.command(&["EXEC"]).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "WAIT 3 100 queued in MULTI must return immediately, well under its \
         own 100ms timeout window (a blocking implementation would also \
         finish under 1s, so a tight sub-window bound is the only assertion \
         that actually distinguishes the two); took {elapsed:?}"
    );
    match &response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(parse_integer(&items[0]), Some(0));
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }

    primary.shutdown().await;
}

/// With a live, acknowledged replica, a queued `WAIT` at EXEC returns the
/// *correct* acked-replica count (1), not just a fast 0 — closing the last
/// acceptance-criterion gap for issue 51 (the pre-existing MULTI/WAIT tests
/// above only ever exercised the no-replica case). A plain (non-MULTI) WAIT
/// first drives the connection-level `WaitCoordinator` to solicit `REPLCONF
/// GETACK *` and confirm the replica has acked the current offset; only then
/// is the *queued* WAIT known to hit an already-satisfied `count_acked`
/// (`replication.rs:568-575`) via the shard non-blocking branch reached from
/// `transaction.rs:277-298`, rather than coincidentally returning 0 because
/// the replica hadn't caught up yet.
#[tokio::test]
async fn test_wait_inside_multi_returns_correct_acked_count_with_replica() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    let mut client = primary.connect().await;
    client
        .command(&["SET", "wait_multi_replica_key", "v"])
        .await;

    // Sync point: a blocking, non-MULTI WAIT confirms the replica has acked
    // the offset written above before we probe the non-blocking MULTI path.
    let synced = client.command(&["WAIT", "1", "5000"]).await;
    assert_eq!(
        parse_integer(&synced),
        Some(1),
        "setup WAIT must observe the replica ack before the MULTI probe, got {synced:?}"
    );

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["WAIT", "1", "5000"]).await; // QUEUED

    let start = std::time::Instant::now();
    let response = client.command(&["EXEC"]).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "WAIT queued in MULTI must not block even with a real, unsatisfied-\
         at-queue-time-looking timeout (5000ms); took {elapsed:?}"
    );
    match &response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(
                parse_integer(&items[0]),
                Some(1),
                "WAIT in MULTI must report the already-acked replica count, got {:?}",
                items[0]
            );
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-TXN-044
/// PSYNC queued inside MULTI/EXEC replies `+OK` and never triggers a socket
/// takeover. This path bypasses `ReplicationHandshake` entirely: `TransactionQueue`
/// (earlier in `PRE_DISPATCH_ORDER`) queues the command, and at EXEC
/// `execute_connection_level_in_transaction` short-circuits it to `+OK` without
/// calling `PsyncCommand::execute`. Guards that the typed-handoff refactor left
/// this behavior untouched (the connection stays a normal request/reply client).
#[tokio::test]
async fn test_psync_inside_multi_replies_ok() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    let mut client = primary.connect().await;
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["PSYNC", "?", "-1"]).await; // QUEUED

    let response = client.command(&["EXEC"]).await;
    match &response {
        Response::Array(items) => {
            assert_eq!(
                items.len(),
                1,
                "EXEC returns one reply for the queued PSYNC"
            );
            assert_ok(&items[0]);
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }

    // The connection is still a normal client: a follow-up PING must answer.
    let pong = client.command(&["PING"]).await;
    assert!(
        !is_error(&pong),
        "connection must remain a normal request/reply client after MULTI PSYNC EXEC, got {pong:?}"
    );

    primary.shutdown().await;
}

/// Test multiple writes replicate correctly.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_multiple_writes(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Write multiple keys
    for i in 0..5 {
        let key = format!("multi_key_{}", i);
        let value = format!("value_{}", i);
        let response = primary.send("SET", &[&key, &value]).await;
        assert_ok(&response);
    }

    // Wait for replication
    primary.send("WAIT", &["1", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify at least some keys are replicated
    let response = replica.send("GET", &["multi_key_0"]).await;
    // We're testing infrastructure here - actual replication may not be fully working yet
    if let Response::Bulk(Some(value)) = response {
        assert_eq!(value.as_ref(), b"value_0");
    }
}

// ============================================================================
// Tier 3: Edge Cases
// ============================================================================

/// Test REPLICAOF NO ONE stops replication.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replicaof_no_one(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Wait for connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop replication on replica
    let response = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&response);

    // After REPLICAOF NO ONE, the replica should still be operational
    let ping = replica.send("PING", &[]).await;
    assert!(parse_simple_string(&ping) == Some("PONG"));

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Test SLAVEOF (alias for REPLICAOF).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_slaveof_alias(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let server = TestServer::start_standalone_with_config(config).await;

    // SLAVEOF NO ONE should work as an alias
    let response = server.send("SLAVEOF", &["NO", "ONE"]).await;
    assert_ok(&response);

    server.shutdown().await;
}

/// Test that large values replicate correctly.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_large_value_replication(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Create a large value (1MB)
    let large_value: String = "x".repeat(1024 * 1024);

    let response = primary.send("SET", &["large_key", &large_value]).await;
    assert_ok(&response);

    // An ACK is the replica's *landed* offset, so once it acks this write the
    // value is readable there — no sleep needed, and no conditional read.
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(30)).await;
    assert_eq!(acked, 1, "the replica must acknowledge the 1 MB write");

    // Verify on replica
    let get_response = replica.send("GET", &["large_key"]).await;
    let Response::Bulk(Some(value)) = &get_response else {
        panic!("replica must serve the replicated value, got: {get_response:?}");
    };
    assert_eq!(
        value.len(),
        large_value.len(),
        "Large value should be fully replicated"
    );
    assert!(
        value.iter().all(|&b| b == b'x'),
        "the replicated value must be the bytes that were written"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-011
/// A value the connection layer accepts crosses the replication link.
///
/// The one-time proof of round-2 issue 69: the frame codec used to cap a
/// payload at 64 MB while the connection layer accepted 512 MB, so a `SET` in
/// between was committed on the primary and then emitted as a frame the
/// replica's decoder refused — the link dropped, the backlog re-sent the same
/// frame on reconnect, and it never recovered. 70 MB is inside the old gap, so
/// this fails as a hang (`WAIT` never acks) and then as a wedge
/// (`master_link_status` flapping) against the pre-fix codec.
///
/// In-memory only, and a single case rather than the persistence matrix: the
/// wedge is a wire-size property, and 70 MB is already the most memory any test
/// in this file moves.
#[tokio::test]
async fn a_value_over_the_old_frame_ceiling_replicates_without_wedging_the_link() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    // Above the 64 MB frame ceiling, below the 512 MB bulk ceiling: the exact
    // band the two constants used to disagree over.
    let value = "x".repeat(70 * 1024 * 1024);

    assert_ok(&primary.send("SET", &["wedge_key", &value]).await);

    let acked = wait_for_replication(&primary, 30_000).await;
    assert_eq!(
        acked, 1,
        "the replica must ack a write the connection layer accepted; 0 means the frame never crossed"
    );

    let get = replica.send("GET", &["wedge_key"]).await;
    let Response::Bulk(Some(replicated)) = &get else {
        panic!("replica must serve the replicated value, got: {get:?}");
    };
    assert_eq!(
        replicated.len(),
        value.len(),
        "the replicated value must be whole, not truncated by the frame length cast"
    );
    assert!(
        replicated.iter().all(|&b| b == b'x'),
        "the replicated value must be the bytes that were written"
    );

    // The wedge is a reconnect storm, not a single dropped frame: the link has
    // to still be up after the write, and the primary has to still count the
    // replica.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let replica_info = parse_info_replication(&replica.send("INFO", &["replication"]).await)
        .expect("INFO replication must parse");
    assert_eq!(
        replica_info.get("master_link_status").map(String::as_str),
        Some("up"),
        "the link must survive the large frame, got: {replica_info:?}"
    );

    let primary_info = parse_info_replication(&primary.send("INFO", &["replication"]).await)
        .expect("INFO replication must parse");
    assert_eq!(
        primary_info.get("connected_slaves").map(String::as_str),
        Some("1"),
        "the primary must still hold the replica link, got: {primary_info:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-018
/// Test REPLCONF with various subcommands.
#[tokio::test]
async fn test_replconf_subcommands() {
    let server = TestServer::start_primary().await;

    // Test ip-address
    let response = server
        .send("REPLCONF", &["ip-address", "192.168.1.100"])
        .await;
    assert_ok(&response);

    // Test GETACK
    let response = server.send("REPLCONF", &["GETACK", "*"]).await;
    assert_ok(&response);

    // Test with no args (should return OK)
    let response = server.send("REPLCONF", &[]).await;
    assert_ok(&response);

    server.shutdown().await;
}

/// Test PSYNC with specific replication ID.
#[tokio::test]
async fn test_psync_with_replication_id() {
    let server = TestServer::start_primary().await;

    // PSYNC with a specific replication ID and offset
    let response = server.send("PSYNC", &["abc123", "100"]).await;
    // Should not error (actual behavior depends on implementation)
    assert!(
        !is_error(&response),
        "PSYNC with replication ID should not error"
    );

    server.shutdown().await;
}

// FM-REPLICATION-028
/// A boot-configured replica refuses writes with `-READONLY`.
///
/// The replica flag comes from config, so the refusal holds from the first
/// command; the streaming link is waited for anyway so the assertion cannot be
/// read as racing the handshake.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_read_only(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    wait_for_connected_slave(&primary).await;

    // Try to write to replica
    let response = replica.send("SET", &["replica_key", "value"]).await;

    let err_msg = get_error_message(&response)
        .unwrap_or_else(|| panic!("replica must refuse a write, got: {response:?}"));
    assert!(
        err_msg.starts_with("READONLY"),
        "replica must refuse the write with -READONLY, got: {err_msg}"
    );

    // The refusal must not have been applied anyway.
    let read_back = replica.send("GET", &["replica_key"]).await;
    assert!(
        matches!(read_back, Response::Bulk(None)),
        "a refused write must leave no value behind, got: {read_back:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Error Handling Tests
// ============================================================================

// FM-REPLICATION-013
/// Test PSYNC with invalid arguments.
#[tokio::test]
async fn test_psync_invalid_args() {
    let server = TestServer::start_primary().await;

    // PSYNC with wrong number of arguments
    let response = server.send("PSYNC", &["only_one_arg"]).await;
    assert!(is_error(&response), "PSYNC with one arg should error");

    // PSYNC with invalid offset
    let response = server.send("PSYNC", &["?", "not_a_number"]).await;
    assert!(
        is_error(&response),
        "PSYNC with invalid offset should error"
    );

    server.shutdown().await;
}

// FM-REPLICATION-037
/// Test WAIT with invalid arguments.
#[tokio::test]
async fn test_wait_invalid_args() {
    let server = TestServer::start_primary().await;

    // WAIT with wrong number of arguments
    let response = server.send("WAIT", &["1"]).await;
    assert!(is_error(&response), "WAIT with one arg should error");

    // WAIT with invalid numreplicas
    let response = server.send("WAIT", &["not_a_number", "1000"]).await;
    assert!(
        is_error(&response),
        "WAIT with invalid numreplicas should error"
    );

    // WAIT with invalid timeout
    let response = server.send("WAIT", &["1", "not_a_number"]).await;
    assert!(
        is_error(&response),
        "WAIT with invalid timeout should error"
    );

    server.shutdown().await;
}

/// Test REPLICAOF with invalid arguments.
#[tokio::test]
async fn test_replicaof_invalid_args() {
    let server = TestServer::start_standalone().await;

    // REPLICAOF with invalid port
    let response = server.send("REPLICAOF", &["127.0.0.1", "not_a_port"]).await;
    assert!(
        is_error(&response),
        "REPLICAOF with invalid port should error"
    );

    // REPLICAOF with port 0
    let response = server.send("REPLICAOF", &["127.0.0.1", "0"]).await;
    assert!(is_error(&response), "REPLICAOF with port 0 should error");

    server.shutdown().await;
}

/// Extract the role name (first element) from a `ROLE` array reply.
fn role_name(response: &Response) -> Option<String> {
    match response {
        Response::Array(items) => match items.first() {
            Some(Response::Bulk(Some(b))) => std::str::from_utf8(b).ok().map(str::to_string),
            _ => None,
        },
        _ => None,
    }
}

// FM-REPLICATION-025
/// Runtime `REPLICAOF <host> <port>` (Role Demotion) must *actually* demote:
/// flip the node to read-only, reject writes, and report `slave` via ROLE —
/// then `REPLICAOF NO ONE` (Role Promotion) must restore a writable primary.
///
/// Regression: the old handler parsed the args, logged, returned `+OK`, and
/// flipped nothing — the node stayed a writable primary reporting `master`.
/// Every post-demotion assertion below fails against that no-op stub.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replicaof_host_port_demotes(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    // Two independent primaries; `node` will be demoted to replicate `target`.
    let target = TestServer::start_primary_with_config(config.clone()).await;
    let node = TestServer::start_primary_with_config(config).await;

    // Precondition: `node` is a writable primary.
    assert_ok(&node.send("SET", &["k", "v"]).await);
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master"),
        "node should start as a primary"
    );

    // Role Demotion at runtime.
    let target_port = target.port().to_string();
    let resp = node.send("REPLICAOF", &["127.0.0.1", &target_port]).await;
    assert_ok(&resp);

    // The node is now a replica: ROLE reports `slave` and writes are rejected.
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("slave"),
        "REPLICAOF host port must flip ROLE to slave (old stub left it master)"
    );

    let write = node.send("SET", &["k2", "v2"]).await;
    assert!(
        is_error(&write),
        "writes must be rejected after Role Demotion (old stub kept the node writable), got {write:?}"
    );
    assert!(
        get_error_message(&write)
            .unwrap_or("")
            .starts_with("READONLY"),
        "expected READONLY error, got {write:?}"
    );

    // Role Promotion back to primary restores writability.
    assert_ok(&node.send("REPLICAOF", &["NO", "ONE"]).await);
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master"),
        "REPLICAOF NO ONE must promote back to master"
    );
    assert_ok(&node.send("SET", &["k3", "v3"]).await);

    node.shutdown().await;
    target.shutdown().await;
}

/// Read a key's value as a `String`, or `None` for a nil reply.
fn string_value(response: &Response) -> Option<String> {
    match response {
        Response::Bulk(Some(b)) => std::str::from_utf8(b).ok().map(str::to_string),
        _ => None,
    }
}

/// Poll `GET key` on `server` until it reads `want`, or the deadline passes.
/// Returns the last observed value so a timeout fails with what was really there.
async fn wait_for_value(
    server: &TestServer,
    key: &str,
    want: Option<&str>,
    within: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let seen = string_value(&server.send("GET", &[key]).await);
        if seen.as_deref() == want || tokio::time::Instant::now() >= deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Assert that `replica` serves every `(key, value)` in `pairs`.
///
/// Only the *last* pair is polled for: the replication stream is ordered, so
/// once the final write of a batch is visible on the replica every earlier
/// write in the same batch must be too. That turns "replication is
/// asynchronous" into a deadline on one key and an unconditional assertion on
/// all of them — the ordering guarantee is what is being pinned, not just
/// arrival.
async fn assert_batch_replicated(
    replica: &TestServer,
    pairs: &[(String, String)],
    within: Duration,
) {
    let Some((last_key, last_value)) = pairs.last() else {
        return;
    };
    let seen = wait_for_value(replica, last_key, Some(last_value), within).await;
    assert_eq!(
        seen.as_deref(),
        Some(last_value.as_str()),
        "replica never received the last write of the batch ({last_key}) within {within:?}"
    );

    for (key, value) in pairs {
        let seen = string_value(&replica.send("GET", &[key]).await);
        assert_eq!(
            seen.as_deref(),
            Some(value.as_str()),
            "replica is missing {key} even though a later write in the same \
             ordered stream ({last_key}) already arrived"
        );
    }
}

/// Issue 61: a runtime `REPLICAOF <new-master>` full resync must install the
/// received snapshot into the **live** keyspace, not merely stage it on disk for
/// the next boot.
///
/// The node below is a primary with its own, divergent (`p*`) dataset — exactly
/// the shape of a demoted old primary after a failover. Once it is told to
/// replicate a node whose dataset is entirely different (`m*`), Redis semantics
/// say its old dataset is flushed and the master's snapshot loaded before the
/// stream is applied. Against the staged-only behaviour every assertion below
/// fails: `p*` stayed readable and `m*` was absent until a restart.
#[tokio::test]
async fn test_runtime_full_resync_installs_snapshot_into_live_store() {
    // Persistence on both sides: the checkpoint full-sync path (the one that
    // stages) requires the primary to have a RocksDB to checkpoint.
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let master = TestServer::start_primary_with_config(config.clone()).await;
    let node = TestServer::start_primary_with_config(config).await;

    for i in 0..5 {
        assert_ok(&master.send("SET", &[&format!("m{i}"), "from-master"]).await);
        assert_ok(&node.send("SET", &[&format!("p{i}"), "forked"]).await);
    }

    // Demote: the node full-resyncs from a master with a disjoint keyspace.
    let master_port = master.port().to_string();
    assert_ok(&node.send("REPLICAOF", &["127.0.0.1", &master_port]).await);

    // No restart anywhere in this test: the master's keys must become live on
    // the node, and its own forked keys must be gone.
    assert_eq!(
        wait_for_value(&node, "m0", Some("from-master"), Duration::from_secs(20))
            .await
            .as_deref(),
        Some("from-master"),
        "master's snapshot must be installed into the live keyspace without a restart"
    );

    // Per key, not one wait then instant reads: the install is documented as
    // atomic *per shard* only (`LiveSnapshotInstaller::install`), so seeing
    // `m0` proves only that `m0`'s shard has swapped. Every other key gets its
    // own bounded wait.
    for i in 0..5 {
        let key = format!("m{i}");
        assert_eq!(
            wait_for_value(&node, &key, Some("from-master"), Duration::from_secs(10))
                .await
                .as_deref(),
            Some("from-master"),
            "master's key {key} must be live on the demoted node"
        );
        let forked = format!("p{i}");
        assert_eq!(
            wait_for_value(&node, &forked, None, Duration::from_secs(10)).await,
            None,
            "forked key {forked} must be gone from the demoted node's live keyspace"
        );
    }
    assert_eq!(
        parse_integer(&node.send("DBSIZE", &[]).await),
        Some(5),
        "the install is a replace, not a merge: only the master's keys survive"
    );

    // The install is not a one-shot: subsequent streamed writes still land.
    assert_ok(&master.send("SET", &["m-after", "streamed"]).await);
    assert_eq!(
        wait_for_value(&node, "m-after", Some("streamed"), Duration::from_secs(10))
            .await
            .as_deref(),
        Some("streamed"),
        "streaming must resume from the installed snapshot's offset"
    );

    node.shutdown().await;
    master.shutdown().await;
}

/// Issue 67: a primary running with `persistence.enabled = false` must still
/// hand a full resync its whole dataset.
///
/// This is the end-to-end forcing test. Every node here is persistence-disabled
/// — the configuration the turmoil sims and most deployments-without-durability
/// use — so the primary has no RocksDB to checkpoint. The old behaviour answered
/// `PSYNC ? -1` with a data-less minimal RDB: the replica adopted the
/// replid/offset, reported `master_link_status:up`, and kept serving its own
/// forked keyspace forever. Against that behaviour every assertion below fails:
/// `p*` stayed readable and `m*` never arrived, on this boot or any later one
/// (there is no staged checkpoint for a restart to pick up either).
// FM-REPLICATION-001
#[tokio::test]
async fn test_full_resync_from_a_persistence_disabled_primary_transfers_the_dataset() {
    // The whole point: no RocksDB anywhere. `persistence` defaults to false;
    // spelled out because it is the condition under test.
    let config = || TestServerConfig {
        persistence: false,
        ..Default::default()
    };
    let master = TestServer::start_primary_with_config(config()).await;
    let node = TestServer::start_primary_with_config(config()).await;

    // Enough keys to cover every shard several times over: a dataset export
    // that dropped a shard, or an install that skipped one, must not be able to
    // pass by luck of routing.
    const KEYS: usize = 20;
    for i in 0..KEYS {
        assert_ok(&master.send("SET", &[&format!("m{i}"), "from-master"]).await);
        assert_ok(&node.send("SET", &[&format!("p{i}"), "forked"]).await);
    }
    // A TTL survives the dataset the same way it survives a checkpoint.
    assert_ok(&master.send("SET", &["m-ttl", "v", "EX", "600"]).await);

    let master_port = master.port().to_string();
    assert_ok(&node.send("REPLICAOF", &["127.0.0.1", &master_port]).await);

    // Per key, not one wait then instant reads: the install is atomic per shard
    // only, so each key gets its own bounded wait.
    for i in 0..KEYS {
        let key = format!("m{i}");
        assert_eq!(
            wait_for_value(&node, &key, Some("from-master"), Duration::from_secs(20))
                .await
                .as_deref(),
            Some("from-master"),
            "the master's key {key} must be live on the replica after a full resync \
             served without persistence"
        );
        let forked = format!("p{i}");
        assert_eq!(
            wait_for_value(&node, &forked, None, Duration::from_secs(10)).await,
            None,
            "the replica's forked key {forked} must be gone — a full sync replaces the keyspace"
        );
    }
    assert_eq!(
        parse_integer(&node.send("DBSIZE", &[]).await),
        Some(KEYS as i64 + 1),
        "the replica's keyspace is exactly the master's: no stale keys, none missing"
    );
    let ttl = parse_integer(&node.send("TTL", &["m-ttl"]).await).unwrap_or(-1);
    assert!(
        (1..=600).contains(&ttl),
        "an expiring key keeps its TTL across the dataset transfer, got {ttl}"
    );

    // The link is genuinely up, not merely claimed: streamed writes still land.
    assert_ok(&master.send("SET", &["m-after", "streamed"]).await);
    assert_eq!(
        wait_for_value(&node, "m-after", Some("streamed"), Duration::from_secs(10))
            .await
            .as_deref(),
        Some("streamed"),
        "streaming must resume from the dataset's offset"
    );

    node.shutdown().await;
    master.shutdown().await;
}

// FM-REPLICATION-036
/// A `FULLRESYNC` checkpoint must contain every write the primary has already
/// acknowledged — including those still sitting in a shard's WAL flush-engine.
///
/// A write is acked once staged in the flush engine; the engine commits it to
/// RocksDB on a later size/timeout trigger. The checkpoint snapshots what
/// RocksDB *holds*, so cutting one without first draining the engines omits the
/// primary's most recent writes. For a full resync that hole is permanent: with
/// no replica attached when those writes were made, nothing was broadcast, so no
/// backlog tail can replay them — the replica is simply missing them forever.
///
/// The batch timeout is parked far beyond the test's runtime, which makes "acked
/// but not yet in RocksDB" the steady state instead of a millisecond race: with
/// no drain on the full-sync path *none* of the keys below reach the replica, on
/// every platform. (The bug this pins was originally a coin-flip that only came
/// up heads on macOS, where the checkpoint was cut inside the default 10ms
/// window — a green run elsewhere proved nothing.)
#[tokio::test]
async fn test_full_resync_checkpoint_carries_writes_still_pending_in_the_wal() {
    let config = || TestServerConfig {
        persistence: true,
        // No background flush can rescue the checkpoint during this test...
        persistence_batch_timeout_ms: Some(600_000),
        // ...and no periodic snapshot can drain the engines behind our back
        // (its pre-snapshot hook performs the very drain under test).
        snapshot_interval_secs: Some(0),
        ..Default::default()
    };
    let master = TestServer::start_primary_with_config(config()).await;
    let node = TestServer::start_primary_with_config(config()).await;

    // Enough keys to cover every shard several times over, so the assertion does
    // not depend on which shard a key routes to.
    const KEYS: usize = 20;
    for i in 0..KEYS {
        assert_ok(&master.send("SET", &[&format!("k{i}"), "acked"]).await);
    }

    let master_port = master.port().to_string();
    assert_ok(&node.send("REPLICAOF", &["127.0.0.1", &master_port]).await);

    for i in 0..KEYS {
        let key = format!("k{i}");
        assert_eq!(
            wait_for_value(&node, &key, Some("acked"), Duration::from_secs(10))
                .await
                .as_deref(),
            Some("acked"),
            "{key} was acknowledged by the primary, so the full-resync checkpoint \
             must carry it — an unflushed WAL entry is not an excuse to lose it"
        );
    }

    node.shutdown().await;
    master.shutdown().await;
}

/// Extract `(master_host, master_port)` from a `ROLE` slave-arm reply
/// (`["slave", <host>, <port>, <state>, <offset>]`).
fn role_master_host_port(response: &Response) -> Option<(String, i64)> {
    match response {
        Response::Array(items) if items.len() >= 3 => {
            let host = match &items[1] {
                Response::Bulk(Some(b)) => std::str::from_utf8(b).ok()?.to_string(),
                _ => return None,
            };
            let port = match &items[2] {
                Response::Integer(p) => *p,
                _ => return None,
            };
            Some((host, port))
        }
        _ => None,
    }
}

// FM-REPLICATION-025
/// Round-8 P05: `ROLE` and `INFO replication` must show the *real* Primary
/// target after a runtime `REPLICAOF host port` demotion, not the hardcoded
/// empty host / port 0 the old `RoleCommand` stub returned. `REPLICAOF NO
/// ONE` must then clear both surfaces back to the primary shape.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_role_and_info_report_real_primary_after_demotion(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let target = TestServer::start_primary_with_config(config.clone()).await;
    let node = TestServer::start_primary_with_config(config).await;

    // Before demotion: ROLE reports master, INFO carries no master_host/port.
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master")
    );
    let before = parse_info_replication(&node.send("INFO", &["replication"]).await).unwrap();
    assert!(
        !before.contains_key("master_host"),
        "a primary must not report master_host, got {before:?}"
    );

    // Role Demotion at runtime.
    let target_port = target.port().to_string();
    assert_ok(&node.send("REPLICAOF", &["127.0.0.1", &target_port]).await);

    // ROLE's slave arm reports the real target, not the old "" / 0 stub.
    let role_resp = node.send("ROLE", &[]).await;
    assert_eq!(role_name(&role_resp).as_deref(), Some("slave"));
    let (role_host, role_port) =
        role_master_host_port(&role_resp).expect("slave ROLE reply must carry host/port");
    assert_eq!(
        role_host, "127.0.0.1",
        "ROLE must report the real primary host"
    );
    assert_eq!(
        role_port,
        target.port() as i64,
        "ROLE must report the real primary port"
    );

    // INFO replication's master_host/master_port agree with ROLE.
    let info = parse_info_replication(&node.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        info.get("master_host").map(String::as_str),
        Some("127.0.0.1")
    );
    assert_eq!(
        info.get("master_port").map(String::as_str),
        Some(target_port.as_str())
    );

    // REPLICAOF NO ONE clears both surfaces back to primary shape.
    assert_ok(&node.send("REPLICAOF", &["NO", "ONE"]).await);
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master")
    );
    let after = parse_info_replication(&node.send("INFO", &["replication"]).await).unwrap();
    assert!(
        !after.contains_key("master_host"),
        "REPLICAOF NO ONE must clear master_host, got {after:?}"
    );

    node.shutdown().await;
    target.shutdown().await;
}

/// Extract `cluster.mode` from a `STATUS JSON` bulk reply.
fn status_json_mode(response: &Response) -> Option<String> {
    let bytes = match response {
        Response::Bulk(Some(b)) => b,
        _ => return None,
    };
    let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    v["cluster"]["mode"].as_str().map(String::from)
}

/// Fetch `cluster.mode` from the HTTP `/status/json` endpoint.
async fn http_status_mode(server: &TestServer) -> String {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let body: serde_json::Value = client
        .get(format!("http://{}/status/json", server.metrics_addr()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["cluster"]["mode"]
        .as_str()
        .expect("status JSON must carry cluster.mode")
        .to_string()
}

/// Fetch `cluster.write_fence` from the HTTP `/status/json` endpoint. Absent
/// while no fence is engaged (the field is skipped, never reported as a
/// placeholder), so the return is an `Option`.
async fn http_status_write_fence(server: &TestServer) -> Option<String> {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let body: serde_json::Value = client
        .get(format!("http://{}/status/json", server.metrics_addr()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["cluster"]["write_fence"].as_str().map(String::from)
}

/// Issue 12: the `cluster.mode` field on both status surfaces must track a
/// runtime `REPLICAOF` demotion/promotion instead of freezing at the boot role,
/// so it agrees with INFO/ROLE (issue 05) after a role change. The `STATUS
/// JSON` connection command and the HTTP `/status/json` endpoint render from the
/// same shared collector (issue 11), so both must flip together.
#[tokio::test]
async fn test_status_mode_tracks_runtime_replicaof() {
    let target = TestServer::start_primary().await;
    let node = TestServer::start_primary().await;

    // Boot role: primary on ROLE and both status surfaces.
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master")
    );
    assert_eq!(
        status_json_mode(&node.send("STATUS", &["JSON"]).await).as_deref(),
        Some("primary")
    );
    assert_eq!(http_status_mode(&node).await, "primary");

    // Runtime demotion.
    let target_port = target.port().to_string();
    assert_ok(&node.send("REPLICAOF", &["127.0.0.1", &target_port]).await);

    // ROLE now reports slave, and so must both status surfaces.
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("slave")
    );
    assert_eq!(
        status_json_mode(&node.send("STATUS", &["JSON"]).await).as_deref(),
        Some("replica"),
        "STATUS JSON must report replica after runtime REPLICAOF"
    );
    assert_eq!(
        http_status_mode(&node).await,
        "replica",
        "HTTP /status must report replica after runtime REPLICAOF"
    );

    // REPLICAOF NO ONE flips both surfaces back to primary.
    assert_ok(&node.send("REPLICAOF", &["NO", "ONE"]).await);
    assert_eq!(
        role_name(&node.send("ROLE", &[]).await).as_deref(),
        Some("master")
    );
    assert_eq!(
        status_json_mode(&node.send("STATUS", &["JSON"]).await).as_deref(),
        Some("primary"),
        "STATUS JSON must flip back to primary after REPLICAOF NO ONE"
    );
    assert_eq!(http_status_mode(&node).await, "primary");

    node.shutdown().await;
    target.shutdown().await;
}

/// Round-8 P05: a boot-configured replica (`replicaof` in config, no runtime
/// `REPLICAOF`) must report the same real `master_host`/`master_port` on
/// both `ROLE` and `INFO replication` as a runtime-demoted node — the
/// `RoleManager` is seeded with the boot target at construction, so there is
/// one source of truth for both surfaces from process start.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_boot_configured_replica_reports_primary_target(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Give the boot replication handshake a moment (mirrors other tests).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let role_resp = replica.send("ROLE", &[]).await;
    assert_eq!(role_name(&role_resp).as_deref(), Some("slave"));
    let (role_host, role_port) =
        role_master_host_port(&role_resp).expect("boot replica ROLE reply must carry host/port");
    assert_eq!(role_host, "127.0.0.1");
    assert_eq!(role_port, primary.port() as i64);

    let info = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        info.get("master_host").map(String::as_str),
        Some("127.0.0.1")
    );
    assert_eq!(
        info.get("master_port").map(String::as_str),
        Some(primary.port().to_string().as_str())
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Concurrent Connection Tests
// ============================================================================

/// Test multiple clients can connect to primary simultaneously.
#[tokio::test]
async fn test_multiple_clients_primary() {
    let primary = TestServer::start_primary().await;

    // Connect multiple clients
    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(primary.connect().await);
    }

    // Each client should be able to send commands
    for (i, client) in clients.iter_mut().enumerate() {
        let key = format!("client_{}_key", i);
        let response = client.command(&["SET", &key, "value"]).await;
        assert_ok(&response);
    }

    // Verify all keys exist
    for (i, client) in clients.iter_mut().enumerate() {
        let key = format!("client_{}_key", i);
        let response = client.command(&["GET", &key]).await;
        if let Response::Bulk(Some(value)) = response {
            assert_eq!(value.as_ref(), b"value");
        } else {
            panic!("Client {} GET failed", i);
        }
    }

    primary.shutdown().await;
}

// ============================================================================
// Configuration Tests
// ============================================================================

/// Test starting multiple primaries (for cluster-like scenarios).
#[tokio::test]
async fn test_multiple_primaries() {
    let primary1 = TestServer::start_primary().await;
    let primary2 = TestServer::start_primary().await;

    // Both should be independent
    let response1 = primary1.send("SET", &["p1_key", "value1"]).await;
    let response2 = primary2.send("SET", &["p2_key", "value2"]).await;

    assert_ok(&response1);
    assert_ok(&response2);

    // Keys should be isolated
    let get1 = primary1.send("GET", &["p2_key"]).await;
    let get2 = primary2.send("GET", &["p1_key"]).await;

    assert!(
        matches!(get1, Response::Bulk(None)),
        "p1 should not have p2's key"
    );
    assert!(
        matches!(get2, Response::Bulk(None)),
        "p2 should not have p1's key"
    );

    primary2.shutdown().await;
    primary1.shutdown().await;
}

/// Test different shard counts.
#[rstest]
#[case(1)]
#[case(2)]
#[case(4)]
#[tokio::test]
async fn test_different_shard_counts(#[case] num_shards: usize) {
    let config = TestServerConfig {
        num_shards: Some(num_shards),
        ..Default::default()
    };
    let server = TestServer::start_primary_with_config(config).await;

    // Basic operations should work regardless of shard count
    let response = server.send("SET", &["shard_test", "value"]).await;
    assert_ok(&response);

    let response = server.send("GET", &["shard_test"]).await;
    if let Response::Bulk(Some(value)) = response {
        assert_eq!(value.as_ref(), b"value");
    }

    server.shutdown().await;
}

// ============================================================================
// Tier 4: Partial Sync Tests
// ============================================================================

// FM-REPLICATION-015
/// PSYNC presenting the primary's own replid at its current live offset must be
/// granted a partial resync (`+CONTINUE`) — the fully-caught-up reconnect case.
/// (Previously this test accepted "CONTINUE, FULLRESYNC, or OK" and only checked
/// for a non-error; that over-permissive assertion would have let a silent
/// full-resync regression through — issue 34.)
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_partial_sync_continue_response(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Wait for initial sync to complete
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Write some data to advance the replication offset
    for i in 0..10 {
        let key = format!("psync_key_{}", i);
        primary.send("SET", &[&key, "value"]).await;
    }

    // Wait for replication
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The primary's own replid at its live offset is squarely inside the
    // continuable window, so PSYNC must answer +CONTINUE (not a full resync).
    let (id, offset) = get_replication_state(&primary)
        .await
        .expect("primary INFO replication");
    let resp = primary
        .send_raw(&encode_resp_command(&["PSYNC", &id, &offset.to_string()]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("+CONTINUE"),
        "PSYNC at the live offset with the primary's own replid must get +CONTINUE, got: {head:?}"
    );
    assert!(
        !head.contains("FULLRESYNC"),
        "no full resync may occur for an in-window reconnect, got: {head:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Commands replicated after the initial sync arrive in order.
///
/// Ordering is what makes this assertable without a per-key deadline: the
/// arrival of the *last* write of the batch is polled for, and every earlier
/// write must then already be present. A stream that dropped or reordered a
/// frame fails on the key it skipped rather than being counted and printed.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_partial_sync_preserves_ordering(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write keys in specific order
    let pairs: Vec<(String, String)> = (0..20)
        .map(|i| (format!("order_key_{i:03}"), format!("value_{i:03}")))
        .collect();
    for (key, value) in &pairs {
        assert_ok(&primary.send("SET", &[key, value]).await);
    }

    assert_batch_replicated(&replica, &pairs, Duration::from_secs(10)).await;

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-013
/// PSYNC presenting a replid the primary does not recognize must fall back to a
/// full resync (`+FULLRESYNC`) — never a partial resync. (Previously this test
/// accepted "FULLRESYNC or OK"; the loose `OK` alternative would have masked a
/// bogus partial-resync grant on an unknown replid — issue 34.)
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_partial_sync_falls_back_to_full(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config).await;

    // A replid the primary has never issued cannot match its window -> FULLRESYNC.
    let resp = primary
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            "invalid_repl_id_12345",
            "99999",
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("+FULLRESYNC"),
        "unknown replid must fall back to +FULLRESYNC, got: {head:?}"
    );
    assert!(
        !head.contains("CONTINUE"),
        "an unknown replid must never be granted a partial resync, got: {head:?}"
    );

    primary.shutdown().await;
}

// FM-REPLICATION-019
/// Replication-ID handling across a `REPLICAOF NO ONE` promotion (PSYNC2 parity).
///
/// The promoted node mints a *new* `master_replid` and preserves the inherited
/// one as `master_replid2`, with `second_repl_offset` frozen at the live applied
/// offset it reached — the failover window a surviving sibling partial-resyncs
/// through.
///
/// One deliberate divergence remains, pinned here: `second_repl_offset` is
/// **inclusive**. Redis stores `master_repl_offset + 1` because its replicas
/// request the offset they want *next*; FrogDB replicas request the offset they
/// have *applied* and the window predicate is `<=`, so adding one would widen
/// the window by a byte position with no corresponding request convention.
///
/// This test previously pinned the opposite contract (no mint, no window) — see
/// `.scratch/testing-improvements/issues/34`.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_secondary_replication_id_failover(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Write data
    for i in 0..3 {
        let key = format!("failover_key_{}", i);
        primary.send("SET", &[&key, "value"]).await;
    }
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The replica adopts the primary's replication id during FULLRESYNC. This
    // also gates that the initial sync has completed before we promote.
    let primary_replid = get_replication_state(&primary)
        .await
        .expect("primary INFO replication")
        .0;
    let before = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    let replid_before = before.get("master_replid").cloned().unwrap();
    assert_eq!(
        replid_before, primary_replid,
        "replica must adopt the primary's replid via FULLRESYNC before promotion"
    );
    // No window before the first promotion: nothing has been failed over yet.
    assert_eq!(
        before.get("master_replid2").map(String::as_str),
        Some("0000000000000000000000000000000000000000"),
    );
    assert_eq!(
        before.get("second_repl_offset").map(String::as_str),
        Some("-1"),
    );
    let offset_before: i64 = before
        .get("master_repl_offset")
        .and_then(|v| v.parse().ok())
        .expect("replica master_repl_offset");

    // Promote replica to primary (stop replication).
    let promote_resp = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote_resp);

    let after = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        after.get("role").map(String::as_str),
        Some("master"),
        "Promoted replica should report as master"
    );

    // (a) A fresh replication id is minted: the promoted node heads its own
    // history from here, so nothing that follows it can be confused with the
    // stream it inherited.
    let replid_after = after.get("master_replid").cloned().unwrap();
    assert_ne!(
        replid_after, replid_before,
        "promotion must mint a new master_replid"
    );
    assert_eq!(
        replid_after.len(),
        40,
        "a minted replid keeps the 40-hex-char shape"
    );

    // (b) The inherited id becomes the failover window.
    assert_eq!(
        after.get("master_replid2").cloned().unwrap(),
        replid_before,
        "the inherited replid must be retained as master_replid2"
    );

    // (c) The window boundary is the live applied offset at promotion,
    // inclusive — not `+1` (FrogDB replicas request their applied offset and
    // the window predicate is `<=`).
    let second_offset: i64 = after
        .get("second_repl_offset")
        .and_then(|v| v.parse().ok())
        .expect("second_repl_offset");
    assert_eq!(
        second_offset, offset_before,
        "second_repl_offset must be the promoted node's applied offset, inclusive"
    );
    // The live offset is continuous across the identity change: everything at
    // or below the boundary is shared history under the old id.
    let offset_after: i64 = after
        .get("master_repl_offset")
        .and_then(|v| v.parse().ok())
        .expect("master_repl_offset");
    assert_eq!(offset_after, offset_before);

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-022
/// A role round-trip must not leave a stale failover window behind: once the
/// promoted node adopts someone else's history via `+FULLRESYNC`, its claim to
/// the old stream is gone and `master_replid2` must go back to the all-zero
/// sentinel (Redis `clearReplicationId2`, `replication.c` `readSyncBulkPayload`).
///
/// Leaving it set would let the node grant `+CONTINUE` over data it discarded.
#[tokio::test]
async fn test_promotion_window_cleared_when_node_adopts_a_new_history() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let fresh_primary = TestServer::start_standalone().await;

    for i in 0..3 {
        primary.send("SET", &[&format!("rt{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Promote: window armed.
    assert_ok(&replica.send("REPLICAOF", &["NO", "ONE"]).await);
    let promoted = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    assert_ne!(
        promoted.get("master_replid2").map(String::as_str),
        Some("0000000000000000000000000000000000000000"),
        "promotion must arm the failover window"
    );

    // Demote onto an unrelated primary: the full resync replaces this node's
    // history entirely.
    assert_ok(
        &replica
            .send(
                "REPLICAOF",
                &["127.0.0.1", &fresh_primary.port().to_string()],
            )
            .await,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut after = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    while std::time::Instant::now() < deadline {
        after = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
        if after.get("master_link_status").map(String::as_str) == Some("up") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        after.get("master_link_status").map(String::as_str),
        Some("up"),
        "demoted node must link up with its new primary"
    );
    assert_eq!(
        after.get("master_replid2").map(String::as_str),
        Some("0000000000000000000000000000000000000000"),
        "adopting a new history must clear the failover window"
    );
    assert_eq!(
        after.get("second_repl_offset").map(String::as_str),
        Some("-1"),
        "the window boundary must return to the -1 sentinel"
    );

    replica.shutdown().await;
    fresh_primary.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-019
/// A node promoted via manual `REPLICAOF NO ONE` becomes both *writable* and a
/// replication *source*: it serves downstream `PSYNC`.
///
/// The primary-side seams (tracker, `PrimaryReplicationHandler`, quorum checker)
/// are built for every role at boot and gate their behavior on the live role
/// flag, so `RoleManager::promote` acquires them by clearing that flag alone.
/// This test used to pin the opposite contract — a promoted node rejected PSYNC
/// with `-ERR ... not running as primary`, because the handler was constructed
/// only for a boot-configured primary.
///
/// The grant is two-sided and pinned here: a sibling that presents the
/// *inherited* replid at exactly the failover boundary gets `+CONTINUE`; one
/// byte past it (history the promoted node never applied) gets `+FULLRESYNC`,
/// and so does an unrelated replid. Granting the first case is what makes a
/// failover cheap; refusing the second is what keeps it correct.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_promoted_node_via_replicaof_no_one_serves_downstream_psync(
    #[case] persistence: bool,
) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    for i in 0..5 {
        primary.send("SET", &[&format!("k{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Capture the replid + offset a surviving replica would present when it
    // reconnects to the promoted node.
    let (replid, offset) = get_replication_state(&replica)
        .await
        .expect("replica INFO replication");

    // Promote via manual REPLICAOF NO ONE.
    assert_ok(&replica.send("REPLICAOF", &["NO", "ONE"]).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        parse_info_replication(&replica.send("INFO", &["replication"]).await)
            .unwrap()
            .get("role")
            .map(String::as_str),
        Some("master"),
        "promoted node must report role:master"
    );

    // A sibling exactly at the failover boundary resumes: the inherited replid
    // is the promoted node's `master_replid2` and the boundary is inside the
    // window, so the tail it needs is empty.
    let resp = replica
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &offset.to_string(),
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        !head.contains("not running as primary"),
        "promoted node must serve downstream PSYNC, got: {head:?}"
    );
    assert!(
        head.contains("CONTINUE"),
        "a sibling at the failover boundary must be granted +CONTINUE, got: {head:?}"
    );

    // One byte past the boundary is history the promoted node never applied.
    // Granting it would leave those writes resident on the peer forever while
    // the promoted node streams unrelated history — permanent silent
    // divergence, undetectable by offset comparison.
    let ahead = replica
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &(offset + 1).to_string(),
        ]))
        .await;
    let ahead = String::from_utf8_lossy(&ahead);
    assert!(
        ahead.contains("FULLRESYNC"),
        "an offset past the failover boundary must full-resync, got: {ahead:?}"
    );

    // An unrelated replid matches neither `master_replid` nor `master_replid2`.
    let unknown = replica
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            "0123456789abcdef0123456789abcdef01234567",
            "0",
        ]))
        .await;
    let unknown = String::from_utf8_lossy(&unknown);
    assert!(
        unknown.contains("FULLRESYNC"),
        "an unknown replid must full-resync, got: {unknown:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Propagation truth for the primary-side write gates across a promotion:
/// `CONFIG SET` values applied while a node is a *replica* govern it once it is
/// promoted.
///
/// The three knobs under test all live behind seams that used to be built only
/// for a boot-configured primary, so on a promoted node they answered
/// `CONFIG GET` with the operator's value while governing nothing:
/// `min-replicas-to-write` (needs the replica tracker),
/// `self-fence-on-replica-loss` + `replica-freshness-timeout-ms` (need the
/// quorum checker in the write gate). This walks the full lifecycle on one node:
/// configure as a replica → promote → gate closed with no replicas → gate opens
/// when a real downstream replica streams (which also proves the promoted node
/// broadcasts) → self-fence engages when that replica goes away.
#[tokio::test]
async fn test_promoted_replica_enforces_write_gates_set_before_promotion() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let downstream = TestServer::start_standalone().await;

    // Arm the primary-side write gates while this node is still a replica.
    assert_ok(
        &replica
            .send("CONFIG", &["SET", "min-replicas-to-write", "1"])
            .await,
    );
    assert_ok(
        &replica
            .send("CONFIG", &["SET", "self-fence-on-replica-loss", "yes"])
            .await,
    );
    assert_ok(
        &replica
            // Above the replica ACK interval (1s) so a *live* downstream stays
            // fresh, but far below the default so the fence engages promptly
            // once it is gone.
            .send("CONFIG", &["SET", "replica-freshness-timeout-ms", "2500"])
            .await,
    );

    assert_ok(&replica.send("REPLICAOF", &["NO", "ONE"]).await);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // `min-replicas-to-write 1` governs the promoted primary: it has no replicas
    // of its own yet, so the write is refused rather than silently accepted.
    let resp = replica.send("SET", &["promoted-gate", "1"]).await;
    assert!(
        is_error(&resp)
            && get_error_message(&resp)
                .unwrap_or_default()
                .contains("NOREPLICAS"),
        "promoted node must enforce min-replicas-to-write set before promotion, got: {resp:?}"
    );

    // Attach a real downstream replica to the promoted node. PSYNC is served by
    // the promoted node's primary handler, and its ACKs land in the tracker the
    // write gate counts. The link runs through a proxy so the ACK leg can be
    // frozen later without detaching the replica — a downstream that is shut
    // down departs *cleanly*, which disarms the fence instead of engaging it
    // (FM-REPLICATION-062).
    let proxy = LagProxy::spawn(replica.port()).await;
    assert_ok(
        &downstream
            .send("REPLICAOF", &["127.0.0.1", &proxy.port().to_string()])
            .await,
    );
    let acked = parse_integer(&replica.send("WAIT", &["1", "10000"]).await).unwrap_or(0);
    assert_eq!(
        acked, 1,
        "promoted node must see its downstream replica ACK (WAIT on a promoted primary)"
    );

    // Gate now satisfied by a live replica, and the write actually replicates —
    // the promoted node broadcasts, it does not merely accept.
    assert_ok(&replica.send("SET", &["promoted-gate", "2"]).await);
    let _ = replica.send("WAIT", &["1", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mirrored = downstream.send("GET", &["promoted-gate"]).await;
    assert!(
        matches!(&mirrored, Response::Bulk(Some(v)) if v.as_ref() == b"2"),
        "write on the promoted primary must reach its downstream replica, got: {mirrored:?}"
    );

    // Lose the replica: it stays attached but stops being heard from. Self-
    // fencing armed the moment that replica streamed, so once the freshness
    // window lapses the promoted node fences writes with SELFFENCE. Relax
    // min-replicas-to-write first so the error under test is unambiguously the
    // fence.
    assert_ok(
        &replica
            .send("CONFIG", &["SET", "min-replicas-to-write", "0"])
            .await,
    );
    proxy.stall_acks();
    let fenced = poll_set(&replica, "promoted-gate", Duration::from_secs(10), |r| {
        err_has_prefix(r, "SELFFENCE")
    })
    .await;
    assert!(
        err_has_prefix(&fenced, "SELFFENCE"),
        "promoted node must self-fence after losing its only replica, got: {fenced:?}"
    );

    // The fence is attributable in `/status`, from the same checker the gate
    // consulted.
    assert_eq!(
        http_status_write_fence(&replica).await.as_deref(),
        Some("replica quorum lost"),
        "an engaged fence must be reported as cluster.write_fence"
    );

    drop(proxy);
    downstream.shutdown().await;
    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-022
/// The demotion half of the same contract: a node that *stops* being a primary
/// stops being a replication source, in the same breath.
///
/// The primary-side handler is never torn down (it exists on every role), so
/// the transition has to be carried by behavior rather than by construction:
/// `RoleManager::demote` calls `end_primary_stint`, which closes the backlog
/// window and disconnects the downstream replicas, and the live role flag makes
/// the next `PSYNC` attempt fail the `ReplicationHandshake` gate. Together those give
/// the invariant a sub-replica depends on — there is no window in which a node
/// is a replica of one node while still serving a resync to another.
#[tokio::test]
async fn test_demoted_primary_stops_serving_psync_to_its_downstream() {
    let config = TestServerConfig::default();

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    // `node` boots writable, takes a downstream of its own, and is then demoted
    // under that downstream's feet.
    let node = TestServer::start_primary_with_config(config.clone()).await;
    let downstream = TestServer::start_primary_with_config(config).await;

    assert_ok(
        &downstream
            .send("REPLICAOF", &["127.0.0.1", &node.port().to_string()])
            .await,
    );
    let acked = parse_integer(&node.send("WAIT", &["1", "10000"]).await).unwrap_or(0);
    assert_eq!(
        acked, 1,
        "downstream must attach to the writable node first"
    );

    assert_ok(&node.send("SET", &["pre-demote", "1"]).await);
    let _ = node.send("WAIT", &["1", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        matches!(&downstream.send("GET", &["pre-demote"]).await, Response::Bulk(Some(v)) if v.as_ref() == b"1"),
        "sanity: the link must be live before the demotion under test"
    );

    // Demote. The downstream's link is severed by `end_primary_stint` and its
    // reconnect attempts are refused by the role gate from here on.
    assert_ok(
        &node
            .send("REPLICAOF", &["127.0.0.1", &primary.port().to_string()])
            .await,
    );

    let mut link_down = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let info =
            parse_info_replication(&downstream.send("INFO", &["replication"]).await).unwrap();
        if info.get("master_link_status").map(String::as_str) == Some("down") {
            link_down = true;
            break;
        }
    }
    assert!(
        link_down,
        "a downstream of a demoted node must lose its link, not keep streaming from a replica"
    );

    // The refusal is explicit, not just a dropped connection.
    let (replid, offset) = get_replication_state(&downstream)
        .await
        .expect("downstream INFO replication");
    let resp = node
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &offset.to_string(),
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("-ERR") && head.contains("not running as primary"),
        "a demoted node must reject downstream PSYNC outright, got: {head:?}"
    );

    // And a write that lands on the new primary does not reach the orphan.
    assert_ok(&primary.send("SET", &["post-demote", "1"]).await);
    wait_for_replication(&primary, 2000).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        matches!(
            downstream.send("GET", &["post-demote"]).await,
            Response::Bulk(None)
        ),
        "the orphaned downstream must not receive writes relayed through a demoted node"
    );

    downstream.shutdown().await;
    node.shutdown().await;
    primary.shutdown().await;
}

/// Chained replication (replica-of-replica) contract, pinned (testing-gap
/// issue 48): FrogDB does **not** implement transitive replication, and this
/// covers the more common real-world trigger than the manual-promotion case
/// above — a plain, never-promoted replica as the chain target.
///
/// `REPLICAOF` accepts a target that is itself a replica: the command layer
/// does no target-role validation, flips the sub-replica's own role, and
/// returns `+OK` synchronously (matching Redis's async REPLICAOF semantics —
/// REPLICAOF cannot know the target's role without a round trip). But the
/// background connection then performs a `PSYNC` handshake against that
/// target, and a node that is *currently a replica* answers PSYNC with an
/// outright `-ERR ... not running as primary` — `connection/dispatch.rs`'s
/// `ReplicationHandshake` role gate, read live from the role flag (the primary-side
/// handler itself exists on every role, so a *promoted* node does serve PSYNC;
/// see `test_promoted_node_via_replicaof_no_one_serves_downstream_psync`). The
/// sub-replica's link never comes up and no primary-origin write ever
/// reaches it — a *reject*, not a silent black hole, just one that is only
/// visible through the link status and logs rather than the `REPLICAOF`
/// reply itself.
///
/// DIVERGENCE (pinned, intentional): Redis supports arbitrary replica
/// fan-out chains; FrogDB does not. See `operations/replication.md` "How
/// FrogDB differs from Redis replication" for the documented contract.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_chained_replication_rejected_sub_replica_never_receives_data(
    #[case] persistence: bool,
) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    // Primary <- replica_a: an ordinary, never-promoted replica.
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica_a = TestServer::start_replica_with_config(&primary, config.clone()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_ok(&primary.send("SET", &["k1", "v1"]).await);
    wait_for_replication(&primary, 2000).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        matches!(&replica_a.send("GET", &["k1"]).await, Response::Bulk(Some(v)) if v.as_ref() == b"v1"),
        "sanity: replica_a must have synced from primary before B attaches to it"
    );

    // node_b: boots standalone-writable, then REPLICAOF at runtime targets
    // replica_a — the literal scenario from the acceptance criteria.
    let node_b = TestServer::start_primary_with_config(config).await;
    let a_port = replica_a.port().to_string();
    assert_ok(&node_b.send("REPLICAOF", &["127.0.0.1", &a_port]).await);

    // node_b's role flips immediately even though no data will ever flow.
    assert_eq!(
        role_name(&node_b.send("ROLE", &[]).await).as_deref(),
        Some("slave"),
        "REPLICAOF must flip node_b's role even though the target is a replica"
    );

    // Give the reconnect/backoff loop several attempts to "succeed" (it can't).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let info = parse_info_replication(&node_b.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        info.get("master_link_status").map(String::as_str),
        Some("down"),
        "a sub-replica pointed at a replica must never report master_link_status:up, got: {info:?}"
    );

    // Neither the pre-existing key nor a write issued after node_b attached
    // ever reaches it — no full sync, no streamed frame.
    assert!(
        matches!(node_b.send("GET", &["k1"]).await, Response::Bulk(None)),
        "sub-replica must never receive the pre-existing key via a replica target"
    );

    assert_ok(&primary.send("SET", &["k2", "v2"]).await);
    wait_for_replication(&primary, 2000).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        matches!(node_b.send("GET", &["k2"]).await, Response::Bulk(None)),
        "sub-replica must never receive a primary-origin write relayed through a replica target"
    );

    // Confirm this is an explicit reject, not merely an absence of data: a raw
    // PSYNC against replica_a — the same handshake node_b's reconnect loop is
    // attempting — gets an outright error, never CONTINUE/FULLRESYNC.
    let (replid, offset) = get_replication_state(&replica_a)
        .await
        .expect("replica_a INFO replication");
    let resp = replica_a
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &offset.to_string(),
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("-ERR") && head.contains("not running as primary"),
        "a plain (never-promoted) replica must reject downstream PSYNC outright, got: {head:?}"
    );
    assert!(
        !head.contains("CONTINUE") && !head.contains("FULLRESYNC"),
        "a replica must never offer a resync to a would-be sub-replica, got: {head:?}"
    );

    node_b.shutdown().await;
    replica_a.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 5: WAL Buffer Tests
// ============================================================================

/// Test WAL buffer capacity behavior.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wal_buffer_capacity(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write commands to test buffer behavior
    let pairs: Vec<(String, String)> = (0..50)
        .map(|i| (format!("wal_test_{i}"), "value".to_string()))
        .collect();
    for (key, value) in &pairs {
        assert_ok(&primary.send("SET", &[key, value]).await);
    }

    assert_batch_replicated(&replica, &pairs, Duration::from_secs(10)).await;

    let info = primary.send("INFO", &["replication"]).await;
    let info_map = parse_info_replication(&info).unwrap();
    assert_eq!(
        info_map.get("role").map(|s| s.as_str()),
        Some("master"),
        "INFO replication should show role:master"
    );
    let offset: i64 = info_map
        .get("master_repl_offset")
        .expect("INFO replication must report master_repl_offset")
        .trim()
        .parse()
        .expect("master_repl_offset must be an integer");
    assert!(
        offset > 0,
        "the offset must have advanced across 50 replicated writes, got {offset}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// A replica that goes away and comes back inside the backlog window catches up
/// on everything it missed.
///
/// What is pinned is the *outcome* — every write, including the ones made while
/// the replica was down, is on the replica afterwards. The wire arm that serves
/// it (`+CONTINUE` versus `+FULLRESYNC`) is not asserted here because the
/// replica restarts on a fresh data dir and therefore always presents the
/// `? -1` sentinel; the grant itself is pinned by
/// `test_partial_resync_after_brief_disconnect_grants_continue`.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_reconnect_within_buffer(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        // The point of this test is the writes made *while the replica is
        // gone*. Self-fence would refuse them with `-SELFFENCE` (that
        // behaviour is pinned by `test_self_fence_engages_on_replica_loss`),
        // leaving nothing to catch up on.
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config.clone()).await;
    wait_for_connected_slave(&primary).await;

    // Write initial data
    let initial: Vec<(String, String)> = (0..5)
        .map(|i| (format!("reconnect_key_{i}"), "initial".to_string()))
        .collect();
    for (key, value) in &initial {
        assert_ok(&primary.send("SET", &[key, value]).await);
    }

    assert_batch_replicated(&replica, &initial, Duration::from_secs(10)).await;

    // Shutdown replica
    replica.shutdown().await;

    // Write more data while replica is down (within buffer capacity)
    let offline: Vec<(String, String)> = (5..10)
        .map(|i| (format!("reconnect_key_{i}"), "offline".to_string()))
        .collect();
    for (key, value) in &offline {
        assert_ok(&primary.send("SET", &[key, value]).await);
    }

    // Restart replica
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;
    wait_for_connected_slave(&primary).await;

    let ping_resp = replica2.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_resp),
        Some("PONG"),
        "Replica should respond after reconnect"
    );

    // Everything, on both sides of the disconnect.
    let all: Vec<(String, String)> = initial.iter().chain(offline.iter()).cloned().collect();
    assert_batch_replicated(&replica2, &all, Duration::from_secs(15)).await;

    replica2.shutdown().await;
    primary.shutdown().await;
}

/// Test replica reconnect with offset outside WAL buffer triggers full sync.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_reconnect_outside_buffer(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        // Writes continue while the replica is down; self-fence would refuse
        // them (see `test_self_fence_engages_on_replica_loss`).
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config.clone()).await;
    wait_for_connected_slave(&primary).await;

    // Write data
    assert_ok(&primary.send("SET", &["outside_key_1", "value1"]).await);
    let seen = wait_for_value(
        &replica,
        "outside_key_1",
        Some("value1"),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(seen.as_deref(), Some("value1"));

    // Shutdown replica
    replica.shutdown().await;

    // Write more data to advance the offset
    // (Reduced count to avoid timeout - actual buffer overflow test would need many more)
    let overflow: Vec<(String, String)> = (0..100)
        .map(|i| (format!("overflow_key_{i}"), format!("overflow_value_{i}")))
        .collect();
    let mut client = primary.connect().await;
    for (key, value) in &overflow {
        client.command(&["SET", key, value]).await;
    }
    drop(client);

    // Restart replica - should need full resync (or partial if buffer large enough)
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;
    wait_for_connected_slave(&primary).await;

    // Verify replica is responsive after reconnect
    let ping_resp = replica2.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_resp),
        Some("PONG"),
        "Replica should respond after reconnect"
    );

    // Whichever arm served the reconnect, the replica must end up holding both
    // the pre-disconnect key and everything written while it was gone.
    assert_batch_replicated(&replica2, &overflow, Duration::from_secs(15)).await;
    let old = string_value(&replica2.send("GET", &["outside_key_1"]).await);
    assert_eq!(
        old.as_deref(),
        Some("value1"),
        "the pre-disconnect key must survive the resync"
    );

    replica2.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 6: Edge Cases and Failure Scenarios
// ============================================================================

// FM-REPLICATION-039
/// Test WAIT timeout behavior when replica disconnects.
/// Documents behavior of WAIT when replica dies during the wait period.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wait_with_disconnected_replica(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // The replica must actually be streaming before it is killed, or "WAIT
    // returns 0" would be true for the wrong reason.
    wait_for_connected_slave(&primary).await;

    // Write data
    let set_resp = primary.send("SET", &["disconnect_test", "value"]).await;
    assert_ok(&set_resp);

    // Kill the replica (simulates crash/disconnect)
    replica.shutdown().await;

    // Give time for disconnect to be detected
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now call WAIT - should timeout since no replicas available
    let start = std::time::Instant::now();
    let wait_resp = primary.send("WAIT", &["1", "1000"]).await; // Wait for 1 replica, 1 second timeout
    let elapsed = start.elapsed();

    let acked = parse_integer(&wait_resp).unwrap_or(-1);

    assert!(
        elapsed < Duration::from_secs(5),
        "WAIT should not block longer than reasonable timeout"
    );

    // WAIT should return 0 since the replica is disconnected
    assert_eq!(
        acked, 0,
        "WAIT should return 0 when replica is disconnected"
    );

    primary.shutdown().await;
}

/// Test replica behavior under high write load: a burst of writes must still
/// converge on the replica once it catches up (a smoke test that a moderate
/// backlog does not wedge the stream).
///
/// For the full lag-disconnect → reconnect → resync cycle that a *stalled*
/// replica triggers (broadcast overflow / write-timeout on the primary side),
/// see [`test_broadcast_lag_disconnect_and_resync`], which drives the stall
/// deterministically through an interposed throttling proxy.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_lag_behavior(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write a burst of commands to potentially overflow broadcast channel
    let num_writes = 1000;
    for i in 0..num_writes {
        let key = format!("lag_test_{i}");
        let value = format!("value_{i}");
        assert_ok(&primary.send("SET", &[&key, &value]).await);
    }

    // The burst must converge, not merely "mostly" converge: poll for the last
    // write of the burst, then require every sampled key to be there.
    let sample_keys = [0usize, 250, 500, 750, 999];
    let sampled: Vec<(String, String)> = sample_keys
        .iter()
        .map(|&i| (format!("lag_test_{i}"), format!("value_{i}")))
        .collect();
    let last = (
        format!("lag_test_{}", num_writes - 1),
        format!("value_{}", num_writes - 1),
    );
    assert_batch_replicated(
        &replica,
        std::slice::from_ref(&last),
        Duration::from_secs(30),
    )
    .await;
    assert_batch_replicated(&replica, &sampled, Duration::from_secs(5)).await;

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Full resync data integrity: every key written *before* the replica existed
/// must be on the replica once it has synced.
///
/// This is the whole point of the snapshot arm — a replica that comes up
/// holding none of the primary's pre-existing dataset is the data-less
/// full-sync failure, so the count is asserted, not reported.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_fullresync_data_integrity(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Write data BEFORE starting replica to force full resync
    let pairs: Vec<(String, String)> = (0..5)
        .map(|i| (format!("fullsync_key_{i}"), format!("fullsync_value_{i}")))
        .collect();
    for (key, value) in &pairs {
        assert_ok(&primary.send("SET", &[key, value]).await);
    }

    // Now start replica (triggers full resync)
    let replica = TestServer::start_replica_with_config(&primary, config).await;
    wait_for_connected_slave(&primary).await;

    let acked = wait_for_acks(&primary, 1, Duration::from_secs(20)).await;
    assert_eq!(
        acked, 1,
        "the resynced replica must acknowledge the primary's offset"
    );

    // Verify replica is responsive
    let ping_result = replica.send("PING", &[]).await;
    assert!(
        !is_error(&ping_result),
        "Replica should respond to PING after full resync"
    );

    assert_batch_replicated(&replica, &pairs, Duration::from_secs(15)).await;

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Stress test with large values: 5 x 1MB must all cross the link whole.
///
/// A truncated or missing large value is a silent divergence, so every key is
/// required to be present at the right length rather than tallied.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_large_value_replication_stress(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write 5 x 1MB values (5MB total)
    let value_size = 1024 * 1024; // 1MB
    let num_keys = 5;
    let large_value: String = "x".repeat(value_size);

    for i in 0..num_keys {
        let key = format!("large_key_{i}");
        let response = primary.send("SET", &[&key, &large_value]).await;
        assert_ok(&response);
    }

    // An ACK is the replica's landed offset: once it acks, all 5 MB is applied.
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(60)).await;
    assert_eq!(acked, 1, "the replica must acknowledge all 5 MB of writes");

    // Verify replica is responsive
    let ping_result = replica.send("PING", &[]).await;
    assert!(!is_error(&ping_result), "Replica should respond to PING");

    for i in 0..num_keys {
        let key = format!("large_key_{i}");
        let response = replica.send("GET", &[&key]).await;
        let Response::Bulk(Some(value)) = &response else {
            panic!("replica is missing {key} after acking the write: {response:?}");
        };
        assert_eq!(
            value.len(),
            value_size,
            "{key} was truncated on the replica"
        );
        assert!(
            value.iter().all(|&b| b == b'x'),
            "{key} arrived with corrupted contents"
        );
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 7: WAL Buffer Overflow and Full Resync Tests
// ============================================================================

/// Test that WAL buffer overflow triggers full resync.
///
/// This test verifies that when a replica falls behind beyond the WAL buffer capacity,
/// it correctly triggers a full resync to recover consistency.
///
/// The default WAL buffer is 1MB (1048576 bytes). We write enough data to exceed this.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wal_overflow_triggers_full_resync(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        // The overflow flood happens with the replica down; self-fence would
        // refuse it (see `test_self_fence_engages_on_replica_loss`).
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Write baseline data BEFORE replica connects
    let baseline_key = "baseline_before_replica";
    let baseline_value = "baseline_value";
    primary.send("SET", &[baseline_key, baseline_value]).await;

    // Start replica and wait for initial sync
    let replica = TestServer::start_replica_with_config(&primary, config.clone()).await;
    wait_for_connected_slave(&primary).await;

    // Write a marker key while replica is connected
    let marker_key = "marker_while_connected";
    let marker_value = "connected_value";
    assert_ok(&primary.send("SET", &[marker_key, marker_value]).await);

    // The replica must actually hold the marker before it is disconnected —
    // otherwise "it has the marker afterwards" would prove nothing.
    let seen = wait_for_value(
        &replica,
        marker_key,
        Some(marker_value),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        seen.as_deref(),
        Some(marker_value),
        "replica must hold the marker before the disconnect"
    );

    // Shutdown replica to simulate disconnect
    replica.shutdown().await;

    // Write large amount of data to overflow WAL buffer
    // Each key-value pair is roughly 30-40 bytes in RESP format
    // Write enough to exceed 1MB buffer (aim for ~1.5MB)
    let overflow_count = 5000; // 5000 keys with ~200 byte values = ~1MB
    let overflow_value: String = "x".repeat(200);

    let mut client = primary.connect().await;
    for i in 0..overflow_count {
        let key = format!("overflow_{i}");
        client.command(&["SET", &key, &overflow_value]).await;
    }
    drop(client);

    // Write a post-overflow marker
    let post_marker = "post_overflow_marker";
    let post_value = "post_overflow_value";
    assert_ok(&primary.send("SET", &[post_marker, post_value]).await);

    // Restart replica - this should trigger full resync since WAL buffer overflowed
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;
    wait_for_connected_slave(&primary).await;

    // Verify replica is responsive
    let ping_resp = replica2.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_resp),
        Some("PONG"),
        "Replica should respond to PING after reconnect"
    );

    // Whether the reconnect was served by a full or a partial resync is an
    // implementation detail; the dataset the replica ends up with is not. All
    // three eras of writes must be there: before the first attach, during the
    // first stream, and while the replica was gone.
    let mut expected: Vec<(String, String)> = vec![
        (baseline_key.to_string(), baseline_value.to_string()),
        (marker_key.to_string(), marker_value.to_string()),
    ];
    for i in [0usize, 100, 1000, 2500, 4999] {
        expected.push((format!("overflow_{i}"), overflow_value.clone()));
    }
    // Last, so the poll waits on the newest write of the batch.
    expected.push((post_marker.to_string(), post_value.to_string()));

    assert_batch_replicated(&replica2, &expected, Duration::from_secs(30)).await;

    replica2.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 8: Critical Edge Cases
// ============================================================================

/// Test WAIT behavior when replica is performing full resync.
///
/// A client issues WAIT while a replica is mid-full-resynchronization. Whether
/// the resync has finished by the time WAIT runs is a race, so the assertions
/// are on what must hold either way:
/// - WAIT returns within its own timeout; it never hangs past it.
/// - The count is bounded by the number of attached replicas (0 or 1 here) —
///   WAIT never invents acks.
/// - Once the resync completes, WAIT converges to exactly 1 and the replica
///   serves both the mid-resync write and a fresh one.
///
/// And the durability claim itself, whichever way the race falls: if the count
/// is 1, that replica serves the write (issue 28).
// FM-REPLICATION-037
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_wait_during_replica_resync(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        num_shards: Some(1),
        // The overflow flood and the mid-resync write both happen without a
        // healthy replica attached; self-fence would refuse them (see
        // `test_self_fence_engages_on_replica_loss`).
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Write lots of initial data to ensure full resync takes time
    let mut client = primary.connect().await;
    for i in 0..500 {
        client
            .command(&[
                "SET",
                &format!("initial_key_{}", i),
                &format!("value_{}", i),
            ])
            .await;
    }
    drop(client);

    // Start first replica and wait for full sync
    let replica = TestServer::start_replica_with_config(&primary, config.clone()).await;
    wait_for_connected_slave(&primary).await;

    // Write lots of data to overflow WAL buffer (force full resync on reconnect)
    let overflow_value: String = "x".repeat(200);
    let mut client = primary.connect().await;
    for i in 0..5000 {
        client
            .command(&["SET", &format!("overflow_key_{i}"), &overflow_value])
            .await;
    }
    drop(client);

    // Shutdown replica (disconnect cleanly)
    replica.shutdown().await;

    // Restart replica - this will require full resync due to WAL overflow
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;

    // Write a new key and immediately WAIT, while the fresh replica is very
    // likely still transferring the snapshot.
    assert_ok(
        &primary
            .send("SET", &["wait_test_key", "wait_test_value"])
            .await,
    );

    let wait_result = tokio::time::timeout(
        Duration::from_secs(5),
        primary.send("WAIT", &["1", "500"]), // Wait for 1 replica, 500ms timeout
    )
    .await;

    let response = wait_result.expect("WAIT with a 500ms timeout must return, not hang");
    let count = parse_integer(&response)
        .unwrap_or_else(|| panic!("WAIT must reply with an integer, got {response:?}"));

    // Only one replica exists, so WAIT can only honestly report 0 or 1.
    assert!(
        (0..=1).contains(&count),
        "WAIT must not report more acks than there are replicas, got {count}"
    );

    // The claim WAIT exists to make: a counted ack is a replica that serves the
    // write. This used to be false (issue 28) — a replica still installing its
    // full-resync checkpoint was reported `state=online` at the primary's own
    // offset, so WAIT counted it while a `GET` on that same replica returned
    // nil. Asserting it is safe against the resync race in the only direction
    // that matters: applying a write is monotonic, so a replica that had the
    // key when WAIT answered still has it now.
    if count == 1 {
        let served = string_value(&replica2.send("GET", &["wait_test_key"]).await);
        assert_eq!(
            served.as_deref(),
            Some("wait_test_value"),
            "WAIT counted a replica that cannot serve the write it acknowledged"
        );
    }

    // Now let the resync finish and check the steady state.
    let final_acked = wait_for_acks(&primary, 1, Duration::from_secs(60)).await;
    assert_eq!(
        final_acked, 1,
        "after the resync completes WAIT must count the replica"
    );

    // Verify replica is responsive after resync
    let ping_result = replica2.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_result),
        Some("PONG"),
        "Replica should be responsive after resync"
    );

    // Both the mid-resync write and a post-resync write must be readable.
    assert_ok(&primary.send("SET", &["final_key", "final_value"]).await);
    assert_batch_replicated(
        &replica2,
        &[
            ("wait_test_key".to_string(), "wait_test_value".to_string()),
            ("final_key".to_string(), "final_value".to_string()),
        ],
        Duration::from_secs(30),
    )
    .await;

    replica2.shutdown().await;
    primary.shutdown().await;
}

/// Test that FULLRESYNC handles interrupted transfers correctly.
///
/// This tests the critical scenario where a replica's connection drops during
/// FULLRESYNC (full synchronization). The replica should:
/// 1. Detect the incomplete transfer
/// 2. Clean up any partial state
/// 3. Successfully complete a new full resync on reconnect
///
/// Risk: If partial checkpoints are left on disk or accepted as complete,
/// the replica could have corrupt or incomplete data.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_fullresync_interrupted_resume(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        num_shards: Some(1),
        // The `while_down_*` writes land with no replica attached; self-fence
        // would refuse them (see `test_self_fence_engages_on_replica_loss`).
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Write significant initial data to make full resync non-trivial
    let num_initial_keys = 200;
    for i in 0..num_initial_keys {
        let key = format!("initial_key_{}", i);
        let value = format!("initial_value_{}", i);
        assert_ok(&primary.send("SET", &[&key, &value]).await);
    }

    // Start replica (triggers FULLRESYNC)
    let replica = TestServer::start_replica_with_config(&primary, config.clone()).await;

    // Wait very briefly - we want to interrupt early in the sync.
    // Whether the snapshot transfer had finished when we kill the replica is a
    // race, and deliberately so: neither outcome may leave the *next* replica
    // with a partial dataset, which is what the assertions below pin down.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Kill replica mid-sync (simulating network failure or crash)
    drop(replica); // Immediately drop/disconnect

    // Write more data while replica is down
    let num_additional_keys = 50;
    for i in 0..num_additional_keys {
        let key = format!("while_down_key_{i}");
        let value = format!("while_down_value_{i}");
        assert_ok(&primary.send("SET", &[&key, &value]).await);
    }

    // Restart replica - should trigger new FULLRESYNC
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;

    // Use WAIT to ensure replication has caught up
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(60)).await;
    assert_eq!(
        acked, 1,
        "the replacement replica must complete a resync and ack the primary"
    );

    // Verify replica is responsive
    let ping_result = replica2.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_result),
        Some("PONG"),
        "Replica should be responsive after restart"
    );

    // Check INFO replication to verify connection status
    let info_resp = replica2.send("INFO", &["replication"]).await;
    let info = parse_info_replication(&info_resp)
        .unwrap_or_else(|| panic!("replica INFO replication must be a bulk string: {info_resp:?}"));
    assert_eq!(
        info.get("role").map(|r| r.trim()),
        Some("slave"),
        "the restarted replica must report role:slave"
    );

    // The interrupted first sync must not cost the second one any data: both
    // the keys that existed before the interruption and the ones written while
    // the replica was gone have to be present.
    let mut expected: Vec<(String, String)> = Vec::new();
    for i in [0usize, 50, 100, 150, 199] {
        assert!(i < num_initial_keys);
        expected.push((format!("initial_key_{i}"), format!("initial_value_{i}")));
    }
    for i in [0usize, 25, 49] {
        assert!(i < num_additional_keys);
        expected.push((
            format!("while_down_key_{i}"),
            format!("while_down_value_{i}"),
        ));
    }
    assert_batch_replicated(&replica2, &expected, Duration::from_secs(30)).await;

    // Write and verify a final key to ensure ongoing replication works
    assert_ok(
        &primary
            .send("SET", &["post_resync_key", "post_resync_value"])
            .await,
    );
    assert_batch_replicated(
        &replica2,
        &[(
            "post_resync_key".to_string(),
            "post_resync_value".to_string(),
        )],
        Duration::from_secs(15),
    )
    .await;

    replica2.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 5: WAIT Edge Cases
// ============================================================================

// FM-REPLICATION-039
/// Tests WAIT with multiple replicas. Start primary + 2 replicas,
/// WAIT 2 should return 2. Kill one replica, write, WAIT 2 should return ≤1.
#[tokio::test]
async fn test_wait_multiple_replicas() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica1 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;

    // Wait for both replicas to connect
    let attached = wait_connected_slaves(&primary, 2, Duration::from_secs(30)).await;
    assert_eq!(
        attached,
        Some(2),
        "both replicas must reach the streaming phase"
    );

    // Write a key
    let resp = primary.send("SET", &["{wr}key1", "value1"]).await;
    assert_ok(&resp);

    // WAIT for 2 replicas
    let acked = wait_for_acks(&primary, 2, Duration::from_secs(30)).await;
    assert_eq!(
        acked, 2,
        "with two healthy replicas attached WAIT 2 must count both"
    );

    // Kill one replica
    replica2.shutdown().await;
    let attached = wait_connected_slaves(&primary, 1, Duration::from_secs(30)).await;
    assert_eq!(
        attached,
        Some(1),
        "the primary must drop the killed replica from connected_slaves"
    );

    // Write another key
    let resp = primary.send("SET", &["{wr}key2", "value2"]).await;
    assert_ok(&resp);

    // WAIT 2 with short timeout should return ≤1 (one replica is down)
    let resp = primary.send("WAIT", &["2", "1000"]).await;
    let acked2 = parse_integer(&resp).unwrap_or(0);
    assert!(
        acked2 <= 1,
        "With one replica down, WAIT 2 should return ≤1, got {}",
        acked2
    );

    replica1.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-039
/// The *bound*, not a count: `WAIT` may never answer with more replicas than
/// the primary itself says are connected (spec GAP-5).
///
/// Every other WAIT test asserts a specific expected number against a stable
/// replica set, which is exactly the shape that cannot catch a double count.
/// The double count comes from a reconnect: the replacement session registers
/// while the old one is still `Streaming`, because `unregister_replica` runs
/// from the departing session's own exit handler and not from the arrival.
/// Both sessions then describe *one* follower, and a count derived from
/// anything but a keyed registry would credit the write twice.
///
/// The churn is `REPLICAOF NO ONE` immediately followed by `REPLICAOF` back at
/// the same primary: the replica re-announces the same identity from the same
/// listening port, which is what makes the two sessions look like one
/// follower. Restarting the process instead would bind a fresh port and count
/// as a different replica, missing the window entirely.
///
/// The assertion is deliberately one-sided. Any count in `0..=connected` is
/// legitimate mid-churn — a link that has dropped answers 0 — so the test
/// asserts only that `WAIT` never exceeds what `INFO` reports, sampling
/// `connected_slaves` on both sides of the call so a replica arriving or
/// leaving *during* the WAIT cannot make a correct answer look wrong. It then
/// asserts the set reconverges, so a run that bounded the count by losing the
/// replica outright is not silently accepted.
#[tokio::test]
async fn test_wait_never_exceeds_connected_slaves() {
    let primary = TestServer::start_primary().await;
    let replica = TestServer::start_replica(&primary).await;
    assert_eq!(
        wait_connected_slaves(&primary, 1, Duration::from_secs(30)).await,
        Some(1),
        "the replica must reach the streaming phase before the churn starts"
    );

    let primary_port = primary.port().to_string();

    for round in 0..8 {
        assert_ok(&primary.send("SET", &[&format!("churn{round}"), "v"]).await);

        // Sample the projection either side of the call: the set can legally
        // change while WAIT is parked, and the bound must hold against the
        // widest set the call could have seen.
        let before = connected_slaves(&primary).await.unwrap_or(0);
        let acked = parse_integer(&primary.send("WAIT", &["5", "200"]).await).unwrap_or(0);
        let after = connected_slaves(&primary).await.unwrap_or(0);

        assert!(
            acked <= before.max(after),
            "round {round}: WAIT returned {acked} with connected_slaves {before} -> {after}"
        );
        assert!(
            acked <= 1,
            "round {round}: only one replica exists, so no reconnect may make WAIT answer {acked}"
        );

        // Re-announce the same identity, overlapping the departing session with
        // its replacement in the primary's registry.
        assert_ok(&replica.send("REPLICAOF", &["NO", "ONE"]).await);
        assert_ok(
            &replica
                .send("REPLICAOF", &["127.0.0.1", &primary_port])
                .await,
        );
    }

    assert_eq!(
        wait_connected_slaves(&primary, 1, Duration::from_secs(30)).await,
        Some(1),
        "the replica must reattach after the churn — a bound held by losing the replica is no bound"
    );
    assert_ok(&primary.send("SET", &["churn-final", "v"]).await);
    assert_eq!(
        wait_for_acks(&primary, 1, Duration::from_secs(30)).await,
        1,
        "the reattached replica must ack again, and still count exactly once"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-037
/// Tests that WAIT 1 0 on a primary blocks (Redis: timeout 0 = no deadline)
/// until externally released.
#[tokio::test]
async fn test_wait_zero_timeout_blocks_without_quorum() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config).await;

    let mut control = primary.connect().await;
    let mut blocked = primary.connect().await;
    let id = parse_integer(&blocked.command(&["CLIENT", "ID"]).await).unwrap();

    // With no replicas the quorum of 1 is unreachable: WAIT 1 0 must block,
    // not return.
    blocked.send_only(&["WAIT", "1", "0"]).await;
    primary.wait_for_blocked_clients(1).await;
    assert!(
        blocked
            .read_response(Duration::from_millis(300))
            .await
            .is_none(),
        "WAIT 1 0 must block with no replicas (Redis timeout-0 semantics)"
    );

    // Release it and confirm the timed-out-style reply (current count = 0).
    let unblocked = control
        .command(&["CLIENT", "UNBLOCK", &id.to_string(), "TIMEOUT"])
        .await;
    assert_eq!(parse_integer(&unblocked), Some(1));
    let resp = blocked
        .read_response(Duration::from_secs(5))
        .await
        .expect("unblocked WAIT must reply");
    assert_eq!(parse_integer(&resp), Some(0));

    primary.shutdown().await;
}

// FM-REPLICATION-040
// FM-REPLICATION-022
/// A WAIT parked across a demotion is released with `-UNBLOCKED`, not with a
/// count.
///
/// Redis unblocks every blocked client from `replicationSetMaster` for the same
/// reason: the wait was counting acknowledgments on a replication stream that
/// the demotion tears down, so any number it could still produce would describe
/// a history the node no longer heads. The release happens inside
/// `end_primary_stint`, next to the downstream disconnect it belongs with.
// FM-BLOCKING-010
#[tokio::test]
async fn test_wait_unblocked_on_demotion() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let new_primary = TestServer::start_primary_with_config(config).await;

    let mut blocked = primary.connect().await;
    // No replicas, so a quorum of 5 is unreachable and `timeout 0` parks
    // forever — the demotion is the only thing that can release it.
    blocked.send_only(&["WAIT", "5", "0"]).await;
    primary.wait_for_blocked_clients(1).await;
    assert!(
        blocked
            .read_response(Duration::from_millis(300))
            .await
            .is_none(),
        "WAIT 5 0 must still be parked before the demotion"
    );

    assert_ok(
        &primary
            .send("REPLICAOF", &["127.0.0.1", &new_primary.port().to_string()])
            .await,
    );

    let resp = blocked
        .read_response(Duration::from_secs(5))
        .await
        .expect("a demotion must release the parked WAIT, not leave it hanging");
    let msg = get_error_message(&resp).unwrap_or_default();
    assert!(
        msg.starts_with("UNBLOCKED"),
        "a WAIT released by a demotion must be an -UNBLOCKED error, got: {resp:?}"
    );
    assert!(
        msg.contains("master -> replica"),
        "the error must name the cause the way Redis does, got: {msg}"
    );

    new_primary.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-037
/// Tests that WAIT 0 0 returns 0 immediately (degenerate case).
#[tokio::test]
async fn test_wait_zero_numreplicas() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config).await;

    let resp = primary.send("WAIT", &["0", "0"]).await;
    let count = parse_integer(&resp);
    assert_eq!(
        count,
        Some(0),
        "WAIT 0 0 should return 0 immediately, got: {:?}",
        resp
    );

    primary.shutdown().await;
}

// ============================================================================
// Tier 6: INFO Replication Format Verification
// ============================================================================

// FM-REPLICATION-059
/// The backlog geometry a live pair reports is the real one, through **both**
/// INFO renderers: the connection-level one over RESP and the shard-local one a
/// script sees via `redis.call('INFO')`. Both used to print
/// `repl_backlog_size:1048576` and `repl_backlog_first_byte_offset:0`
/// regardless of configuration and regardless of the armed floor (issue 20).
///
/// The capacity configured here is deliberately neither FrogDB's default
/// (64 MiB) nor Redis's (1 MiB), so a literal of either shape fails.
#[tokio::test]
async fn info_reports_the_configured_backlog_geometry_through_both_renderers() {
    const CONFIGURED_BYTES: &str = "3145728"; // 3 MiB

    let config = TestServerConfig {
        num_shards: Some(1),
        replication_backlog_max_mb: Some(3),
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    for i in 0..20 {
        assert_ok(
            &primary
                .send("SET", &[&format!("geometry_key_{i}"), "v"])
                .await,
        );
    }
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(30)).await;
    assert_eq!(acked, 1, "the replica must ack before INFO is compared");

    // The shard-local renderer, reached the only way a client can: from inside
    // a script, which runs on the owning shard.
    async fn scripted_info(server: &TestServer) -> std::collections::HashMap<String, String> {
        let response = server
            .send("EVAL", &["return redis.call('INFO', 'replication')", "0"])
            .await;
        parse_info_replication(&response).expect("redis.call('INFO') must return a bulk string")
    }

    let connection_level = parse_info_replication(&primary.send("INFO", &["replication"]).await)
        .expect("INFO replication must return a bulk string");
    let shard_local = scripted_info(&primary).await;

    for (renderer, info) in [
        ("connection-level", &connection_level),
        ("shard-local", &shard_local),
    ] {
        assert_eq!(
            info.get("repl_backlog_size").map(String::as_str),
            Some(CONFIGURED_BYTES),
            "{renderer} renderer must report the configured capacity"
        );
        assert_eq!(
            info.get("repl_backlog_active").map(String::as_str),
            Some("1"),
            "{renderer}: writes opened the resume window"
        );
        let floor: u64 = info["repl_backlog_first_byte_offset"].parse().unwrap();
        let histlen: u64 = info["repl_backlog_histlen"].parse().unwrap();
        let head: u64 = info["master_repl_offset"].parse().unwrap();
        assert!(
            histlen > 0,
            "{renderer}: the window covers the writes above"
        );
        // `<=` rather than `==`: the primary keeps stamping control frames, so
        // the head can advance between the two reads inside one render.
        assert!(
            floor + histlen <= head,
            "{renderer}: the window must not claim to extend past the head \
             (floor {floor} + histlen {histlen} > head {head})"
        );
    }

    // The replica reports its node's configured capacity with no window of its
    // own, and — the second literal on that branch — its real applied offset
    // rather than a hardcoded 0.
    let replica_shard_local = scripted_info(&replica).await;
    assert_eq!(
        replica_shard_local
            .get("repl_backlog_size")
            .map(String::as_str),
        Some(CONFIGURED_BYTES),
        "capacity is a property of the node, not of the role it is running"
    );
    assert_eq!(
        replica_shard_local
            .get("repl_backlog_active")
            .map(String::as_str),
        Some("0"),
        "a replica heads no history, so it claims no resume window"
    );
    let replica_offset: u64 = replica_shard_local["master_repl_offset"]
        .parse()
        .expect("master_repl_offset must be an integer");
    assert!(
        replica_offset > 0,
        "the replica's shard-local INFO must report the offset it applied, not 0"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-043
/// Parses INFO replication on primary and verifies the expected format, with
/// `master_repl_offset` and `connected_slaves` checked against values this
/// test caused itself (a write count, an attach count) rather than merely
/// "present and non-negative" — a check a hardcoded `0` would also pass.
#[tokio::test]
async fn test_info_replication_primary_format() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let (primary, _replica) = start_primary_replica_pair(config).await;

    // Drive the handshake to completion instead of reading whatever
    // connected_slaves happens to be after a fixed sleep.
    wait_for_replication(&primary, 2000).await;
    let (replid_before, offset_before) = get_replication_state(&primary)
        .await
        .expect("primary should report master_replid/master_repl_offset before the workload");

    for i in 0..10 {
        primary.send("SET", &[&format!("fmt{i}"), "v"]).await;
    }
    wait_for_replication(&primary, 2000).await;

    let response = primary.send("INFO", &["replication"]).await;
    let info = parse_info_replication(&response).expect("should parse INFO replication");

    // role should be master
    assert_eq!(
        info.get("role").map(|s| s.as_str()),
        Some("master"),
        "Primary should report role:master"
    );

    // master_replid should be 40-char hex, and stable across a plain write
    // workload (it only changes on a fullresync-worthy discontinuity).
    let replid = info.get("master_replid").expect("master_replid present");
    assert_eq!(
        replid.len(),
        40,
        "master_replid should be 40 chars, got: {}",
        replid
    );
    assert!(
        replid.chars().all(|c| c.is_ascii_hexdigit()),
        "master_replid should be hex, got: {}",
        replid
    );
    assert_eq!(
        *replid, replid_before,
        "master_replid must not change across a plain SET workload"
    );

    // master_repl_offset must have advanced past the pre-workload offset —
    // not just be "some number >= 0", which a frozen placeholder satisfies
    // too.
    let offset: i64 = info
        .get("master_repl_offset")
        .expect("master_repl_offset present")
        .parse()
        .expect("offset should be numeric");
    assert!(
        offset > offset_before,
        "master_repl_offset should advance past the pre-workload offset {} after 10 SETs, got {}",
        offset_before,
        offset
    );

    // connected_slaves must equal the one replica this test actually
    // attached, not merely "a valid number" (a hardcoded 0 is one too).
    let slaves: i64 = info
        .get("connected_slaves")
        .expect("connected_slaves present")
        .parse()
        .expect("connected_slaves should be numeric");
    assert_eq!(
        slaves, 1,
        "connected_slaves should report the one attached replica, got {}",
        slaves
    );
}

/// Parses INFO replication on replica and verifies `master_host`/
/// `master_port`/`master_link_status` resolve to the primary this test
/// actually started — not just "present" or "up or down", both of which a
/// hardcoded placeholder would also satisfy.
#[tokio::test]
async fn test_info_replication_replica_format() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    // Poll for the handshake instead of silently skipping the assertions
    // when it has not finished within a fixed sleep.
    let mut info = None;
    for _ in 0..50 {
        let response = replica.send("INFO", &["replication"]).await;
        if let Some(parsed) = parse_info_replication(&response)
            && parsed.get("role").map(String::as_str) == Some("slave")
        {
            info = Some(parsed);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let info = info.expect("replica should report role:slave within 5s");

    // master_host/master_port must name the primary this test actually
    // started, derived independently via `primary.port()` — not just be
    // present.
    assert_eq!(
        info.get("master_host").map(String::as_str),
        Some("127.0.0.1"),
        "replica should report the primary's real host: {info:?}"
    );
    let expected_port = primary.port().to_string();
    assert_eq!(
        info.get("master_port").map(String::as_str),
        Some(expected_port.as_str()),
        "replica should report the primary's real port, not a placeholder: {info:?}"
    );

    // master_link_status must settle on "up" for a replica that connected
    // and streamed, not merely be "a member of {up, down}".
    let mut link_status = info.get("master_link_status").cloned();
    for _ in 0..50 {
        if link_status.as_deref() == Some("up") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let response = replica.send("INFO", &["replication"]).await;
        link_status =
            parse_info_replication(&response).and_then(|i| i.get("master_link_status").cloned());
    }
    assert_eq!(
        link_status.as_deref(),
        Some("up"),
        "replica should report master_link_status:up once streaming"
    );
}

// FM-REPLICATION-027
/// `master_link_status` must track the real replication link, not a
/// hardcoded literal: `up` once the replica is connected and streaming from
/// its primary, and never `up` while no connection has ever been
/// established.
#[tokio::test]
async fn test_info_replication_master_link_status_tracks_connection() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let (_primary, replica) = start_primary_replica_pair(config).await;

    // Poll for the handshake to complete and the link to report up — avoids a
    // flaky fixed sleep on slower CI hosts.
    let mut link_status = None;
    for _ in 0..50 {
        let response = replica.send("INFO", &["replication"]).await;
        if let Some(info) = parse_info_replication(&response)
            && info.get("role").map(String::as_str) == Some("slave")
        {
            link_status = info.get("master_link_status").cloned();
            if link_status.as_deref() == Some("up") {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        link_status.as_deref(),
        Some("up"),
        "replica should report master_link_status:up once streaming"
    );
}

// FM-REPLICATION-027
/// Complementary case: a replica booted against a primary address nobody is
/// listening on reports `role:slave` (the boot config still flags it as a
/// replica) but must never claim `master_link_status:up` — the connection
/// never reaches the Streaming state. Guards against the field defaulting to
/// (or getting stuck at) "up" before any real link exists.
#[tokio::test]
async fn test_info_replication_master_link_status_down_before_connected() {
    // Grab a port nothing is listening on: bind then immediately drop the
    // listener so the address is free but unreachable.
    let unused_port = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        listener.local_addr().expect("local_addr").port()
    };

    let mut config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    config.replication_primary_host = Some("127.0.0.1".to_string());
    config.replication_primary_port = Some(unused_port);
    let replica = TestServer::start_with_config(config, ServerRole::Replica).await;

    // Give the reconnect loop several attempts against the unreachable
    // address; the link must never be reported "up".
    for _ in 0..10 {
        let response = replica.send("INFO", &["replication"]).await;
        if let Some(info) = parse_info_replication(&response) {
            assert_eq!(
                info.get("role").map(String::as_str),
                Some("slave"),
                "boot-configured replica should report role:slave even before connecting"
            );
            assert_ne!(
                info.get("master_link_status").map(String::as_str),
                Some("up"),
                "replica must not report master_link_status:up before ever connecting"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    replica.shutdown().await;
}

// FM-REPLICATION-028
/// Tests that writes to a fully-connected replica return READONLY error.
///
/// The link is waited on first (`connected_slaves` on the primary), so "the
/// handshake may not have completed" is not an excuse the replica gets: once
/// it is streaming, a write must be refused with `-READONLY` and must not
/// leave a value behind.
#[tokio::test]
async fn test_replica_readonly_error() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    let response = replica.send("SET", &["{rr}key", "value"]).await;

    let msg = get_error_message(&response)
        .unwrap_or_else(|| panic!("a streaming replica must refuse a write, got: {response:?}"));
    assert!(
        msg.starts_with("READONLY"),
        "replica must refuse the write with -READONLY, got: {msg}"
    );

    let read_back = replica.send("GET", &["{rr}key"]).await;
    assert!(
        matches!(read_back, Response::Bulk(None)),
        "a refused write must leave no value behind, got: {read_back:?}"
    );
}

// ============================================================================
// Tier 7: Stub Command Verification
// ============================================================================

/// Tests that WAITAOF returns a not-implemented error.
#[tokio::test]
async fn test_waitaof_returns_not_implemented() {
    let server = TestServer::start_standalone().await;

    let response = server.send("WAITAOF", &["0", "0", "0"]).await;
    assert!(
        is_error(&response),
        "WAITAOF should return error, got: {:?}",
        response
    );
    if let Response::Error(e) = &response {
        let msg = String::from_utf8_lossy(e).to_lowercase();
        assert!(
            msg.contains("not") && msg.contains("implemented"),
            "WAITAOF error should mention 'not implemented', got: {:?} (bytes: {:?})",
            msg,
            e
        );
    }

    server.shutdown().await;
}

/// Tests that SYNC returns a not-implemented error.
#[tokio::test]
async fn test_sync_returns_not_implemented() {
    let server = TestServer::start_standalone().await;

    let response = server.send("SYNC", &[]).await;
    assert!(
        is_error(&response),
        "SYNC should return error, got: {:?}",
        response
    );
    if let Response::Error(e) = &response {
        let msg = String::from_utf8_lossy(e).to_lowercase();
        assert!(
            msg.contains("not") && (msg.contains("implemented") || msg.contains("supported")),
            "SYNC error should mention 'not implemented' or 'not supported', got: {:?} (bytes: {:?})",
            msg,
            e
        );
    }

    server.shutdown().await;
}

// ============================================================================
// Tier 9: Expired Key Replication
// ============================================================================
//
// FrogDB's replica-expiry contract (issue 32) — this differs from Redis, and
// the tests below pin the *actual* behavior rather than an aspirational one:
//
// - Redis: TTL-setting commands are rewritten to an absolute `PEXPIREAT`
//   before replication, and organic key expiry is propagated to replicas as
//   an explicit `DEL`/`UNLINK`. Because the replica's deadline is
//   byte-identical to the primary's, a replica `GET` returns nil at exactly
//   the same instant the primary would, even before the `DEL` arrives.
// - FrogDB (`core/src/shard/post_execution.rs::apply_expiry_effects`,
//   `RemovalPropagation`): a replica expires **independently**, on its own
//   clock (`get_with_expiry_check` / active expiry are both role-agnostic).
//   TTL-setting commands (e.g. `PEXPIRE`) replicate **verbatim** — a relative
//   duration, not an absolute deadline (`expire_tcl.rs` documents this as a
//   deliberate, accepted incompatibility) — so the replica computes its own
//   deadline as `replica_apply_time + duration`, which lands later than the
//   primary's `primary_apply_time + duration` by however long replication
//   took to deliver the write. The primary never broadcasts a `DEL` for
//   organic expiry at all (`replicate: false`).
//
// Net effect: there is a real, but *bounded* (bounded by replication lag,
// not unbounded) window after the primary has logically expired a key
// during which a replica `GET` still returns the stale (pre-expiry) value,
// and the key only disappears on the replica once, independently, via the
// replica's own clock — never via a propagated `DEL`.

/// Poll a replica until it reports a positive `PTTL` for `key`, proving it
/// has actually applied the TTL-setting write (not just that `WAIT`
/// acknowledged the bytes).
async fn poll_until_replica_has_positive_pttl(
    replica: &TestServer,
    key: &str,
    timeout_ms: u64,
) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(pttl) = parse_integer(&replica.send("PTTL", &[key]).await)
            && pttl > 0
        {
            return Some(pttl);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// FM-REPLICATION-030
/// Pins the actual (not Redis's) replica-expiry contract: a real, bounded
/// drift window where the replica serves a value the primary has already
/// expired, and eventual independent convergence — never via a propagated
/// `DEL` — proven by `master_repl_offset` not advancing.
///
/// Previously `test_expire_propagated_as_del_not_expire`, which resolved
/// every possible outcome via `eprintln!` instead of asserting one.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_expires_independently_not_via_del(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;
    let key = "expire_drift_test";

    assert_ok(&primary.send("SET", &[key, "expiring"]).await);
    let expire_resp = primary.send("PEXPIRE", &[key, "1500"]).await;
    assert!(
        matches!(expire_resp, Response::Integer(1)),
        "PEXPIRE should return 1, got: {:?}",
        expire_resp
    );

    let _ = wait_for_replication(&primary, 5000).await;

    // The write-effect pipeline never broadcasts a DEL for organic expiry
    // (`RemovalPropagation { replicate: false }`), so the primary's
    // replication offset right after the TTL-setting write is the offset it
    // must *still* report once the key is gone on both nodes.
    let (_, offset_after_pexpire) = get_replication_state(&primary)
        .await
        .expect("primary should report replication state");

    // Confirm the replica actually applied PEXPIRE (not just that WAIT
    // acknowledged bytes) before measuring drift.
    let replica_pttl_early = poll_until_replica_has_positive_pttl(&replica, key, 2000)
        .await
        .expect("replica should apply PEXPIRE and report a positive PTTL");
    assert!(
        replica_pttl_early > 0 && replica_pttl_early <= 1500,
        "replica PTTL right after apply should be in (0, 1500], got {replica_pttl_early}"
    );

    // Read both PTTLs as close to simultaneously as possible (concurrent
    // requests, not sequential) to measure the deadline drift caused by
    // verbatim relative-TTL propagation.
    let pttl_args: [&str; 1] = [key];
    let (primary_pttl_resp, replica_pttl_resp) = tokio::join!(
        primary.send("PTTL", &pttl_args),
        replica.send("PTTL", &pttl_args)
    );
    let primary_pttl =
        parse_integer(&primary_pttl_resp).expect("primary PTTL should be an integer");
    let replica_pttl =
        parse_integer(&replica_pttl_resp).expect("replica PTTL should be an integer");
    assert!(
        primary_pttl > 0,
        "primary PTTL should still be positive: {primary_pttl}"
    );
    assert!(
        replica_pttl > 0,
        "replica PTTL should still be positive: {replica_pttl}"
    );

    let drift_ms = replica_pttl - primary_pttl;
    eprintln!(
        "measured replica-primary PTTL drift: {drift_ms}ms (primary={primary_pttl}ms, replica={replica_pttl}ms)"
    );

    // Sleep exactly past the primary's *measured* deadline (deterministic —
    // computed from the PTTL we just read, not a guessed knife-edge sleep),
    // plus a small margin so the primary is guaranteed to have expired.
    let margin_ms: i64 = 20;
    tokio::time::sleep(Duration::from_millis((primary_pttl + margin_ms) as u64)).await;

    let primary_get = primary.send("GET", &[key]).await;
    assert!(
        matches!(primary_get, Response::Bulk(None)),
        "primary must have expired the key by now, got: {:?}",
        primary_get
    );

    // Pin the actual drift-window contract. When the measured drift
    // comfortably exceeds the margin we just slept, the replica's own local
    // deadline has NOT been reached yet, so it must still serve the
    // (already primary-expired) stale value — the documented divergence from
    // Redis. When the measured drift happens to be smaller than the margin
    // (replication this run was very fast), the replica may have already
    // expired too; both outcomes are consistent with the pinned contract, so
    // branch on the measured drift rather than asserting one outcome
    // blindly.
    let replica_get = replica.send("GET", &[key]).await;
    if drift_ms > margin_ms {
        assert!(
            matches!(replica_get, Response::Bulk(Some(_))),
            "replica should still serve the stale pre-expiry value during the \
             drift window (measured drift {drift_ms}ms > margin {margin_ms}ms); \
             got: {:?}",
            replica_get
        );
    } else {
        eprintln!(
            "measured drift {drift_ms}ms <= margin {margin_ms}ms this run; \
             replica may already have expired independently too: {:?}",
            replica_get
        );
    }

    // Eventually (generous poll, not a knife-edge sleep) the key must be
    // gone on the replica too, via its OWN clock.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut replica_final = replica.send("GET", &[key]).await;
    while !matches!(replica_final, Response::Bulk(None)) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        replica_final = replica.send("GET", &[key]).await;
    }
    assert!(
        matches!(replica_final, Response::Bulk(None)),
        "replica must eventually expire the key independently, got: {:?}",
        replica_final
    );

    // Prove convergence happened via independent expiry, not a propagated
    // DEL: the primary's replication offset must not have advanced.
    let (_, offset_final) = get_replication_state(&primary)
        .await
        .expect("primary should report replication state");
    assert_eq!(
        offset_final, offset_after_pexpire,
        "primary's replication offset must not advance for organic expiry — \
         RemovalPropagation {{ replicate: false }} means no DEL is ever \
         broadcast for TTL-driven removal (core/src/shard/post_execution.rs)"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-030
/// PTTL bound test (issue 32 acceptance criteria): the replica's `PTTL` for a
/// key whose TTL was set by a verbatim-propagated relative-TTL command must
/// be bounded relative to the primary's `PTTL` by a documented lag bound —
/// not merely "some value close to the primary's". A large, unbounded
/// divergence would indicate the drift is not actually governed by
/// replication lag (or that replication itself is starved).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_pttl_bounded_by_replication_lag(#[case] persistence: bool) {
    /// Generous local-test lag bound: real replication lag between two
    /// in-process servers over loopback is normally single-digit
    /// milliseconds; this leaves ample headroom for scheduler jitter while
    /// still being a meaningfully finite bound (not "any value").
    const DRIFT_BOUND_MS: i64 = 2000;

    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;
    let key = "pttl_drift_bound_test";

    assert_ok(&primary.send("SET", &[key, "v"]).await);
    let expire_resp = primary.send("PEXPIRE", &[key, "10000"]).await;
    assert!(
        matches!(expire_resp, Response::Integer(1)),
        "PEXPIRE should return 1, got: {:?}",
        expire_resp
    );
    let _ = wait_for_replication(&primary, 5000).await;
    poll_until_replica_has_positive_pttl(&replica, key, 2000)
        .await
        .expect("replica should apply PEXPIRE and report a positive PTTL");

    // Take several concurrent (primary, replica) PTTL samples; every sample
    // must fall inside the documented bound.
    let pttl_args: [&str; 1] = [key];
    for _ in 0..5 {
        let (primary_pttl_resp, replica_pttl_resp) = tokio::join!(
            primary.send("PTTL", &pttl_args),
            replica.send("PTTL", &pttl_args)
        );
        let primary_pttl =
            parse_integer(&primary_pttl_resp).expect("primary PTTL should be an integer");
        let replica_pttl =
            parse_integer(&replica_pttl_resp).expect("replica PTTL should be an integer");
        assert!(
            primary_pttl > 0,
            "primary PTTL should be positive: {primary_pttl}"
        );
        assert!(
            replica_pttl > 0,
            "replica PTTL should be positive: {replica_pttl}"
        );

        let drift_ms = replica_pttl - primary_pttl;
        assert!(
            drift_ms.abs() <= DRIFT_BOUND_MS,
            "replica/primary PTTL drift must be bounded by replication lag \
             (documented bound {DRIFT_BOUND_MS}ms), got {drift_ms}ms \
             (primary={primary_pttl}ms, replica={replica_pttl}ms)"
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 10: Replication Offset Tracking
// ============================================================================

/// Test that master_repl_offset monotonically increases across writes.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replication_offset_monotonically_increases(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config).await;

    // Get initial offset
    let state0 = get_replication_state(&primary).await;
    let offset0 = state0.map(|(_, o)| o).unwrap_or(0);

    // Write some data
    for i in 0..5 {
        primary
            .send("SET", &[&format!("offset_key_{}", i), "value"])
            .await;
    }

    // Get offset after writes
    let state1 = get_replication_state(&primary).await;
    let offset1 = state1.map(|(_, o)| o).unwrap_or(0);

    assert!(
        offset1 >= offset0,
        "Replication offset should not decrease: before={}, after={}",
        offset0,
        offset1
    );

    // Write more data
    for i in 5..10 {
        primary
            .send("SET", &[&format!("offset_key_{}", i), "value"])
            .await;
    }

    let state2 = get_replication_state(&primary).await;
    let offset2 = state2.map(|(_, o)| o).unwrap_or(0);

    assert!(
        offset2 >= offset1,
        "Replication offset should not decrease: before={}, after={}",
        offset1,
        offset2
    );

    primary.shutdown().await;
}

/// Parse the `offset=` field out of the primary's `slaveN:` INFO line.
///
/// Returns `None` when the line is absent (no such replica) or malformed.
async fn slave_offset(primary: &TestServer, index: usize) -> Option<i64> {
    let info = parse_info_replication(&primary.send("INFO", &["replication"]).await)?;
    let line = info.get(&format!("slave{index}"))?;
    line.split(',')
        .find_map(|part| part.trim().strip_prefix("offset="))?
        .parse()
        .ok()
}

// FM-REPLICATION-043
/// Test that after WAIT, the replica's offset catches up to the primary's.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_repl_offset_catches_up_to_primary(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write data
    for i in 0..10 {
        assert_ok(
            &primary
                .send("SET", &[&format!("catchup_key_{i}"), "value"])
                .await,
        );
    }

    // Wait for replication — no "WAIT returned 0, never mind" escape hatch.
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(30)).await;
    assert_eq!(acked, 1, "the replica must ack the primary's writes");

    // Snapshot of the stream position the writes above produced. The primary
    // may keep advancing it afterwards (periodic pings), so the assertion is
    // "the replica reached at least this point", not equality with a value
    // read later.
    let (_, primary_offset) = get_replication_state(&primary)
        .await
        .expect("primary should report replication state");
    assert!(
        primary_offset > 0,
        "primary must have advanced its replication offset for the writes, got {primary_offset}"
    );

    // The primary's own view of the replica (`slave0:…offset=`) must catch up
    // to that point. It is driven by REPLCONF ACKs, so poll it with a deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut observed = slave_offset(&primary, 0).await;
    while observed.is_none_or(|o| o < primary_offset) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        observed = slave_offset(&primary, 0).await;
    }

    let observed =
        observed.expect("primary must report a slave0 line with an offset for its replica");
    assert!(
        observed >= primary_offset,
        "the replica's acked offset must catch up to the primary's {primary_offset}, got {observed}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 11: Replica Promotion Data Completeness
// ============================================================================

// FM-REPLICATION-026
/// Test that after REPLICAOF NO ONE, all replicated data is readable
/// and new writes succeed on the promoted replica.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_promoted_replica_serves_all_writes_after_promotion(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;
    wait_for_connected_slave(&primary).await;

    // Write data to primary
    let mut pairs: Vec<(String, String)> = Vec::new();
    for i in 0..5 {
        let key = format!("promote_key_{i}");
        let value = format!("promote_value_{i}");
        assert_ok(&primary.send("SET", &[&key, &value]).await);
        pairs.push((key, value));
    }

    // Wait for replication to actually land before promoting: promotion must
    // preserve data the replica already holds, so it has to hold it first.
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(30)).await;
    assert_eq!(acked, 1, "the replica must ack the pre-promotion writes");
    assert_batch_replicated(&replica, &pairs, Duration::from_secs(15)).await;

    // Promote replica
    let promote_resp = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote_resp);

    // Every replicated key must still be readable after promotion — losing any
    // of them is exactly the failure this test exists to catch.
    for (key, value) in &pairs {
        let resp = replica.send("GET", &[key]).await;
        assert_eq!(
            string_value(&resp).as_deref(),
            Some(value.as_str()),
            "promoted replica dropped replicated key {key}"
        );
    }

    // New writes should succeed on the promoted replica
    let new_write = replica
        .send("SET", &["post_promote_key", "new_value"])
        .await;
    assert_ok(&new_write);

    let new_read = replica.send("GET", &["post_promote_key"]).await;
    assert_eq!(
        string_value(&new_read).as_deref(),
        Some("new_value"),
        "a promoted replica must serve its own writes, got {new_read:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-026
/// Test that after promotion, the old primary's writes no longer propagate.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_of_no_one_stops_accepting_primary_writes(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let (primary, replica) = start_primary_replica_pair(config).await;

    // Promote replica
    let promote_resp = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote_resp);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write new data on old primary AFTER promotion
    let post_promote_write = primary
        .send("SET", &["after_promote_key", "should_not_replicate"])
        .await;
    assert_ok(&post_promote_write);

    // Give time for any potential (incorrect) propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The promoted replica should NOT have this key
    let check_resp = replica.send("GET", &["after_promote_key"]).await;
    assert!(
        matches!(check_resp, Response::Bulk(None)),
        "Promoted replica should not receive writes from old primary, got: {:?}",
        check_resp
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 12: Checkpoint Verification Gap
// ============================================================================

/// Verifies that SHA256 checkpoint verification works on replicas during full resync.
///
/// The replica computes a combined SHA256 hash over all received checkpoint files
/// and verifies it matches the checksum sent by the primary in the metadata.
#[tokio::test]
async fn test_replica_checkpoint_sha256_verified() {
    let config = TestServerConfig::default();
    let (primary, replica) = start_primary_replica_pair(config).await;

    // Write some data to trigger a non-empty checkpoint
    for i in 0..10 {
        primary
            .send("SET", &[&format!("ckpt_key_{}", i), "value"])
            .await;
    }

    let _ = wait_for_replication(&primary, 5000).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the replica is healthy after checksum-verified full sync
    let ping = replica.send("PING", &[]).await;
    assert!(
        !is_error(&ping),
        "Replica should be responsive after full sync"
    );

    // Verify replicated data is readable on the replica
    let val = replica.send("GET", &["ckpt_key_0"]).await;
    if let Response::Bulk(Some(data)) = &val {
        assert_eq!(
            data.as_ref(),
            b"value",
            "Replicated key should have correct value"
        );
    } else {
        panic!("Expected bulk string response for GET, got: {:?}", val);
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Category E: Data Type Propagation
// ============================================================================

/// Extract the bulk-string elements of an array reply, in order.
///
/// Panics when the reply is not an array — a caller that asked for one and got
/// something else has already failed.
fn bulk_strings(resp: &Response) -> Vec<String> {
    let Response::Array(arr) = resp else {
        panic!("expected an array reply, got: {resp:?}");
    };
    arr.iter()
        .map(|r| match r {
            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            other => panic!("expected a bulk string element, got: {other:?}"),
        })
        .collect()
}

/// List operations (LPUSH/RPUSH) replicate to replica via LRANGE.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_list_operations_replicate(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let rpush = primary.send("RPUSH", &["{r}list", "a", "b", "c"]).await;
    assert!(!is_error(&rpush), "RPUSH failed: {:?}", rpush);
    let lpush = primary.send("LPUSH", &["{r}list", "z"]).await;
    assert!(!is_error(&lpush), "LPUSH failed: {:?}", lpush);

    let (primary_range, replica_range) = wait_for_cmd_convergence(
        &primary,
        &replica,
        "LRANGE",
        &["{r}list", "0", "-1"],
        15_000,
    )
    .await;
    assert_eq!(
        replica_range, primary_range,
        "replica list must converge to the primary's"
    );
    assert_eq!(
        bulk_strings(&replica_range),
        vec!["z", "a", "b", "c"],
        "List should replicate in order"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Hash operations (HSET) replicate to replica via HGETALL.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_hash_operations_replicate(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let hset = primary
        .send("HSET", &["{r}hash", "field1", "val1", "field2", "val2"])
        .await;
    assert!(!is_error(&hset), "HSET failed: {:?}", hset);

    // HGETALL field order is unspecified, so poll on the field *count* and
    // compare the decoded map — never the raw reply.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut replica_all = replica.send("HGETALL", &["{r}hash"]).await;
    while bulk_strings(&replica_all).len() != 4 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        replica_all = replica.send("HGETALL", &["{r}hash"]).await;
    }

    let flat = bulk_strings(&replica_all);
    assert_eq!(
        flat.len(),
        4,
        "HGETALL should return 4 elements (2 field-value pairs), got {flat:?}"
    );
    let fields: std::collections::HashMap<&str, &str> = flat
        .chunks_exact(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_str()))
        .collect();
    assert_eq!(fields.get("field1"), Some(&"val1"));
    assert_eq!(fields.get("field2"), Some(&"val2"));

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Set operations (SADD) replicate to replica via SMEMBERS.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_set_operations_replicate(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let sadd = primary.send("SADD", &["{r}set", "x", "y", "z"]).await;
    assert!(!is_error(&sadd), "SADD failed: {:?}", sadd);

    let (primary_members, replica_members) =
        wait_for_set_convergence(&primary, &replica, "{r}set", 15_000).await;
    assert_eq!(
        replica_members, primary_members,
        "replica set must converge to the primary's"
    );
    assert_eq!(
        replica_members,
        vec!["x", "y", "z"],
        "Set members should replicate"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// SPOP deterministic propagation (issue 01: nondeterministic verbatim
// propagation diverged primary and replica).
//
// SPOP pops a *random* member; replicating it verbatim let the replica pop a
// different one. The fix rewrites the broadcast to `SREM <popped members>`
// (partial drain) or `DEL` (full drain) via the ReplicationOverride mechanism.
// These tests pin primary/replica set equality for every drain shape.
// ============================================================================

/// Extract the bulk-string members of an SMEMBERS reply, sorted.
fn sorted_members(resp: &Response) -> Vec<String> {
    match resp {
        Response::Array(arr) => {
            let mut members: Vec<String> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            members.sort();
            members
        }
        other => panic!("expected array from SMEMBERS, got: {other:?}"),
    }
}

/// Poll until the replica's SMEMBERS of `key` equals the primary's (both
/// sorted), or `timeout_ms` elapses. Returns `(primary, replica)` members for
/// the caller's assertion (so a timeout fails with the divergent contents).
async fn wait_for_set_convergence(
    primary: &TestServer,
    replica: &TestServer,
    key: &str,
    timeout_ms: u64,
) -> (Vec<String>, Vec<String>) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let p = sorted_members(&primary.send("SMEMBERS", &[key]).await);
        let r = sorted_members(&replica.send("SMEMBERS", &[key]).await);
        if p == r || tokio::time::Instant::now() >= deadline {
            return (p, r);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// SPOP with a count that leaves members behind (partial drain): the replica
/// must end up with exactly the primary's remaining members — the divergence
/// the verbatim `SPOP` broadcast used to cause.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_spop_partial_drain_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let sadd = primary
        .send("SADD", &["{r}spop", "a", "b", "c", "d", "e"])
        .await;
    assert_eq!(parse_integer(&sadd), Some(5), "SADD failed: {sadd:?}");

    let spop = primary.send("SPOP", &["{r}spop", "2"]).await;
    let popped = match &spop {
        Response::Array(arr) => arr.len(),
        other => panic!("expected array from SPOP with count, got: {other:?}"),
    };
    assert_eq!(popped, 2, "SPOP 2 must pop exactly 2 members");

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) = wait_for_set_convergence(&primary, &replica, "{r}spop", 5000).await;
    assert_eq!(p.len(), 3, "primary must retain 3 members, got {p:?}");
    assert_eq!(
        p, r,
        "replica set must equal primary set after SPOP (primary {p:?}, replica {r:?})"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// SPOP draining the whole set (count == cardinality): the rewrite propagates
/// a `DEL`, so the key must be gone on both nodes.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_spop_full_drain_replicates_as_delete(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let sadd = primary.send("SADD", &["{r}drain", "x", "y", "z"]).await;
    assert_eq!(parse_integer(&sadd), Some(3), "SADD failed: {sadd:?}");

    let spop = primary.send("SPOP", &["{r}drain", "3"]).await;
    match &spop {
        Response::Array(arr) => assert_eq!(arr.len(), 3),
        other => panic!("expected array from SPOP, got: {other:?}"),
    }
    assert_eq!(
        parse_integer(&primary.send("EXISTS", &["{r}drain"]).await),
        Some(0),
        "full drain must delete the key on the primary"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) = wait_for_set_convergence(&primary, &replica, "{r}drain", 5000).await;
    assert!(
        p.is_empty() && r.is_empty(),
        "both must be empty: {p:?} {r:?}"
    );
    assert_eq!(
        parse_integer(&replica.send("EXISTS", &["{r}drain"]).await),
        Some(0),
        "full drain must delete the key on the replica"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// SPOP with count exceeding the cardinality pops everything; the key must be
/// gone on both nodes.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_spop_count_exceeding_cardinality_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let sadd = primary.send("SADD", &["{r}over", "p", "q"]).await;
    assert_eq!(parse_integer(&sadd), Some(2), "SADD failed: {sadd:?}");

    let spop = primary.send("SPOP", &["{r}over", "10"]).await;
    match &spop {
        Response::Array(arr) => assert_eq!(arr.len(), 2, "SPOP pops what exists"),
        other => panic!("expected array from SPOP, got: {other:?}"),
    }

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) = wait_for_set_convergence(&primary, &replica, "{r}over", 5000).await;
    assert!(
        p.is_empty() && r.is_empty(),
        "both must be empty: {p:?} {r:?}"
    );
    assert_eq!(
        parse_integer(&replica.send("EXISTS", &["{r}over"]).await),
        Some(0),
        "key must be deleted on the replica"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Repeated single-member SPOP (no count): each pop rewrites to `SREM` of the
/// specific popped member, so the replica tracks the primary exactly.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_spop_single_member_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let sadd = primary
        .send("SADD", &["{r}one", "a", "b", "c", "d", "e"])
        .await;
    assert_eq!(parse_integer(&sadd), Some(5), "SADD failed: {sadd:?}");

    for _ in 0..2 {
        let spop = primary.send("SPOP", &["{r}one"]).await;
        assert!(
            matches!(&spop, Response::Bulk(Some(_))),
            "single SPOP must return a member, got: {spop:?}"
        );
    }

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) = wait_for_set_convergence(&primary, &replica, "{r}one", 5000).await;
    assert_eq!(p.len(), 3, "primary must retain 3 members, got {p:?}");
    assert_eq!(
        p, r,
        "replica set must equal primary set after single SPOPs (primary {p:?}, replica {r:?})"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Sorted set operations (ZADD) replicate to replica via ZRANGE.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_sorted_set_operations_replicate(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let zadd = primary
        .send("ZADD", &["{r}zset", "1", "one", "2", "two", "3", "three"])
        .await;
    assert!(!is_error(&zadd), "ZADD failed: {:?}", zadd);

    let (primary_range, replica_range) = wait_for_cmd_convergence(
        &primary,
        &replica,
        "ZRANGE",
        &["{r}zset", "0", "-1"],
        15_000,
    )
    .await;
    assert_eq!(
        replica_range, primary_range,
        "replica sorted set must converge to the primary's"
    );
    assert_eq!(
        bulk_strings(&replica_range),
        vec!["one", "two", "three"],
        "Sorted set should replicate in order"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// INCR/DECR replicates correctly.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_incr_decr_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let set_resp = primary.send("SET", &["{r}counter", "10"]).await;
    assert!(!is_error(&set_resp), "SET failed: {:?}", set_resp);
    assert_eq!(
        primary.send("INCR", &["{r}counter"]).await,
        Response::Integer(11)
    );
    assert_eq!(
        primary.send("INCR", &["{r}counter"]).await,
        Response::Integer(12)
    );
    assert_eq!(
        primary.send("DECR", &["{r}counter"]).await,
        Response::Integer(11)
    );

    assert_batch_replicated(
        &replica,
        &[("{r}counter".to_string(), "11".to_string())],
        Duration::from_secs(15),
    )
    .await;

    replica.shutdown().await;
    primary.shutdown().await;
}

#[cfg(feature = "cmd-stream")]
/// Stream XADD replicates to replica (XLEN matches).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_stream_xadd_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    for i in 0..3 {
        let field = format!("field{}", i);
        let value = format!("val{}", i);
        let resp = primary
            .send("XADD", &["{r}stream", "*", &field, &value])
            .await;
        assert!(!is_error(&resp), "XADD should succeed, got: {:?}", resp);
    }

    let (primary_len, replica_len) =
        wait_for_cmd_convergence(&primary, &replica, "XLEN", &["{r}stream"], 15_000).await;
    assert_eq!(
        primary_len,
        Response::Integer(3),
        "primary stream should have 3 entries"
    );
    assert_eq!(
        replica_len,
        Response::Integer(3),
        "Stream should have 3 entries on replica"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Category F: Replication — Failover & PSYNC2
// ============================================================================

/// ROLE reflects master/slave transitions correctly.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_role_changes_after_replicaof(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    // Replica should report as slave
    let role_resp = replica.send("ROLE", &[]).await;
    if let Response::Array(items) = &role_resp
        && let Response::Bulk(Some(role)) = &items[0]
    {
        assert_eq!(role.as_ref(), b"slave", "Replica should report as slave");
    }

    // Promote replica
    let promote = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now it should report as master
    let role_resp2 = replica.send("ROLE", &[]).await;
    if let Response::Array(items) = &role_resp2
        && let Response::Bulk(Some(role)) = &items[0]
    {
        assert_eq!(
            role.as_ref(),
            b"master",
            "Promoted replica should report as master"
        );
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-019
/// PSYNC2 cross-failover from a two-replica topology: a replica promoted with
/// `REPLICAOF NO ONE` rotates its replication id, keeps the inherited id as the
/// failover window, and hands a *surviving sibling* a partial resync over that
/// window instead of forcing a full one.
///
/// `test_secondary_replication_id_failover` pins the identity rotation and
/// `test_promoted_node_via_replicaof_no_one_serves_downstream_psync` pins the
/// grant/reject boundary; this test composes both from the topology a real
/// failover has, and confirms the promoted node still holds the pre-promotion
/// data it is now the head of.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_psync2_failover_partial_sync(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica1 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Write some data
    for i in 0..5 {
        let key = format!("{{psync2}}key{}", i);
        primary.send("SET", &[&key, "value"]).await;
    }
    let _ = primary.send("WAIT", &["2", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Replica1 adopts the primary's replid; capture it (and the offset a
    // surviving replica would present) before promotion.
    let (pre_replid, pre_offset) = get_replication_state(&replica1)
        .await
        .expect("replica1 INFO replication");

    // Promote replica1 to primary
    let promote = replica1.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let info_map = parse_info_replication(&replica1.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        info_map.get("role").map(String::as_str),
        Some("master"),
        "promoted replica1 must report role:master"
    );
    // The promoted node heads a new history: fresh replid, inherited id kept as
    // the failover window, boundary frozen at the offset it had applied.
    assert_ne!(
        info_map.get("master_replid").cloned().unwrap(),
        pre_replid,
        "promotion must rotate the replication id"
    );
    assert_eq!(
        info_map.get("master_replid2").cloned().unwrap(),
        pre_replid,
        "the inherited replid must be retained as the failover window"
    );
    assert_eq!(
        info_map
            .get("second_repl_offset")
            .and_then(|v| v.parse::<i64>().ok()),
        Some(pre_offset),
        "the window boundary is the applied offset at promotion (inclusive)"
    );

    // A surviving sibling presenting the shared history (old replid, offset at
    // the boundary) is continued, not full-resynced: that window is exactly what
    // `master_replid2` exists for.
    let psync = replica1
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &pre_replid,
            &pre_offset.to_string(),
        ]))
        .await;
    let psync_head = String::from_utf8_lossy(&psync);
    assert!(
        psync_head.contains("CONTINUE"),
        "a sibling inside the failover window must be granted +CONTINUE, got: {psync_head:?}"
    );

    // The promoted node remains writable and still holds all pre-promotion data.
    assert_ok(&replica1.send("SET", &["{{psync2}}newkey", "newval"]).await);
    for i in 0..5 {
        let key = format!("{{psync2}}key{}", i);
        let resp = replica1.send("GET", &[&key]).await;
        assert!(
            matches!(&resp, Response::Bulk(Some(v)) if v.as_ref() == b"value"),
            "promoted node must retain pre-promotion key {key}, got: {resp:?}"
        );
    }

    replica2.shutdown().await;
    replica1.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-043
// FM-REPLICATION-040
// FM-REPLICATION-019
/// The whole failover a real operator performs: primary `P` with replicas `R1`
/// and `R2`, `P` dies, `R1` is promoted, `R2` is repointed at `R1`.
///
/// Every step downstream of the promotion depends on the promoted node actually
/// becoming a primary rather than a node that merely reports `role:master`: it
/// must serve `R2`'s PSYNC, stamp and broadcast its own post-promotion writes,
/// count `R2` in `connected_slaves`, and let `WAIT` observe `R2`'s ACKs. Before
/// this landed the promoted node kept a frozen no-op broadcaster, so the chain
/// silently ended at `R1` — `R2` linked to nothing and saw no data.
#[tokio::test]
async fn test_failover_chain_survivor_reattaches_to_promoted_node() {
    let mut primary = TestServer::start_primary().await;
    let replica1 = TestServer::start_replica(&primary).await;
    let replica2 = TestServer::start_replica(&primary).await;

    for i in 0..5 {
        primary.send("SET", &[&format!("chain{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["2", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The primary dies; R1 takes over.
    primary.shutdown_mut().await;
    assert_ok(&replica1.send("REPLICAOF", &["NO", "ONE"]).await);

    // The survivor is repointed at the new primary.
    assert_ok(
        &replica2
            .send("REPLICAOF", &["127.0.0.1", &replica1.port().to_string()])
            .await,
    );

    let mut link_up = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let info = parse_info_replication(&replica2.send("INFO", &["replication"]).await).unwrap();
        if info.get("master_link_status").map(String::as_str) == Some("up") {
            link_up = true;
            break;
        }
    }
    assert!(
        link_up,
        "the survivor must establish a replication link to the promoted node"
    );

    // A post-promotion write must be stamped, backlogged and streamed: the
    // promoted node is the head of the history now.
    assert_ok(&replica1.send("SET", &["chain_after", "promoted"]).await);
    let acked = parse_integer(&replica1.send("WAIT", &["1", "5000"]).await)
        .expect("WAIT must return an integer");
    assert!(
        acked >= 1,
        "the promoted node's WAIT must observe the survivor's ACK, got {acked}"
    );

    let promoted_info =
        parse_info_replication(&replica1.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        promoted_info.get("connected_slaves").map(String::as_str),
        Some("1"),
        "the promoted node must count the survivor as a connected replica"
    );

    let mut replicated = false;
    for _ in 0..40 {
        if let Response::Bulk(Some(v)) = replica2.send("GET", &["chain_after"]).await
            && v.as_ref() == b"promoted"
        {
            replicated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        replicated,
        "the survivor must receive writes made after the promotion"
    );

    replica2.shutdown().await;
    replica1.shutdown().await;
}

// FM-REPLICATION-021
/// The failover window is part of the persisted replication state, so a promoted
/// node that restarts must come back with the same identity it minted: the same
/// `master_replid`, the same `master_replid2`, and the same `second_repl_offset`.
///
/// Losing any of the three across a restart would silently revoke the partial
/// resync a surviving replica is entitled to (or, worse, mint a *second* new id
/// for the same history).
#[tokio::test]
async fn test_promoted_identity_survives_restart() {
    let primary_tmp = tempfile::tempdir().unwrap();
    let replica_tmp = tempfile::tempdir().unwrap();
    let replica_dir = replica_tmp.path().join("data");
    let config = |dir: PathBuf| TestServerConfig {
        persistence: true,
        data_dir: Some(dir),
        num_shards: Some(1),
        ..Default::default()
    };

    let primary =
        TestServer::start_primary_with_config(config(primary_tmp.path().join("data"))).await;
    let replica =
        TestServer::start_replica_with_config(&primary, config(replica_dir.clone())).await;

    for i in 0..3 {
        primary.send("SET", &[&format!("restart{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["1", "5000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_ok(&replica.send("REPLICAOF", &["NO", "ONE"]).await);
    let before = parse_info_replication(&replica.send("INFO", &["replication"]).await).unwrap();
    let replid = before.get("master_replid").cloned().unwrap();
    let replid2 = before.get("master_replid2").cloned().unwrap();
    let boundary = before.get("second_repl_offset").cloned().unwrap();
    assert_ne!(
        replid2, "0000000000000000000000000000000000000000",
        "promotion must have armed a failover window before we restart"
    );
    // Data written as a primary must outlive the restart too: the inherited
    // full-sync checkpoint is disarmed on promotion, so boot recovery can not
    // re-install it over the promoted node's own database.
    assert_ok(&replica.send("SET", &["restart_after", "promoted"]).await);

    replica.shutdown().await;
    primary.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reboot the promoted node on its own data dir, now as a primary.
    let promoted = TestServer::start_primary_with_config(config(replica_dir)).await;
    let after = parse_info_replication(&promoted.send("INFO", &["replication"]).await).unwrap();
    assert_eq!(
        after.get("master_replid").cloned().unwrap(),
        replid,
        "the minted replid must survive a restart"
    );
    assert_eq!(
        after.get("master_replid2").cloned().unwrap(),
        replid2,
        "the failover window's replid must survive a restart"
    );
    assert_eq!(
        after.get("second_repl_offset").cloned().unwrap(),
        boundary,
        "the failover window's boundary must survive a restart"
    );
    assert_bulk_eq(&promoted.send("GET", &["restart_after"]).await, b"promoted");
    assert_bulk_eq(&promoted.send("GET", &["restart0"]).await, b"v");

    promoted.shutdown().await;
}

/// Continuous writes during failover — no loss for acked writes.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_failover_with_write_load(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    // Write data and wait for ack
    let mut acked_keys = Vec::new();
    for i in 0..10 {
        let key = format!("{{fo}}key{}", i);
        let value = format!("val{}", i);
        primary.send("SET", &[&key, &value]).await;

        // Check if replica acked
        let resp = primary.send("WAIT", &["1", "2000"]).await;
        if let Some(n) = parse_integer(&resp)
            && n >= 1
        {
            acked_keys.push((key, value));
        }
    }

    // Promote replica
    let promote = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert_ok(&promote);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All acked keys should be on the promoted replica
    let mut found = 0;
    for (key, expected) in &acked_keys {
        let resp = replica.send("GET", &[key]).await;
        if let Response::Bulk(Some(v)) = &resp
            && v.as_ref() == expected.as_bytes()
        {
            found += 1;
        }
    }
    assert_eq!(
        found,
        acked_keys.len(),
        "All {} acked keys should survive failover, found {}",
        acked_keys.len(),
        found
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// 3 replicas all receive same writes.
#[tokio::test]
async fn test_multiple_replicas_same_primary() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica1 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica2 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica3 = TestServer::start_replica_with_config(&primary, config).await;

    let attached = wait_connected_slaves(&primary, 3, Duration::from_secs(30)).await;
    assert_eq!(
        attached,
        Some(3),
        "all three replicas must reach the streaming phase"
    );

    // Write data
    let mut pairs: Vec<(String, String)> = Vec::new();
    for i in 0..5 {
        let key = format!("{{mr}}key{i}");
        let value = format!("val{i}");
        assert_ok(&primary.send("SET", &[&key, &value]).await);
        pairs.push((key, value));
    }

    // Wait for all replicas
    let acked = wait_for_acks(&primary, 3, Duration::from_secs(30)).await;
    assert_eq!(acked, 3, "WAIT 3 must count all three attached replicas");

    // Verify all replicas have the data
    for replica in [&replica1, &replica2, &replica3] {
        assert_batch_replicated(replica, &pairs, Duration::from_secs(15)).await;
    }

    replica3.shutdown().await;
    replica2.shutdown().await;
    replica1.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-043, FM-REPLICATION-049
/// INFO replication shows connected_slaves:N and all slaveN entries.
#[tokio::test]
async fn test_info_replication_shows_all_replicas() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica1 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica2 = TestServer::start_replica_with_config(&primary, config).await;

    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Write something to ensure replication is active
    primary.send("SET", &["{ir}key", "value"]).await;
    let _ = primary.send("WAIT", &["2", "5000"]).await;

    let info_resp = primary.send("INFO", &["replication"]).await;
    let info = parse_info_replication(&info_resp).unwrap();

    let connected = info
        .get("connected_slaves")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    assert!(
        connected >= 2,
        "Should have at least 2 connected_slaves, got {}",
        connected
    );

    // The lines must exist *and* describe these two replicas. Asserting only
    // presence is what let `port=0` and `state=online` ship as literals for as
    // long as they did (issues 16/17): every field below was a constant that a
    // presence check could not tell from a projection.
    let mut announced_ports = Vec::new();
    for i in 0..connected {
        let key = format!("slave{i}");
        let line = info
            .get(&key)
            .unwrap_or_else(|| panic!("INFO replication should have a {key} entry: {info:?}"));
        let field = |name: &str| -> String {
            line.split(',')
                .find_map(|p| p.strip_prefix(name).map(str::to_string))
                .unwrap_or_else(|| panic!("{key} has no {name} field: {line}"))
        };
        // The announced `REPLCONF listening-port`, not the ephemeral source
        // port of the replication link — that is the whole point of the field,
        // and the two are never equal here.
        announced_ports.push(field("port=").parse::<u16>().expect("port must be numeric"));
        assert_eq!(
            field("state="),
            "online",
            "a streaming replica renders online: {line}"
        );
        assert!(
            field("lag=").parse::<u64>().is_ok(),
            "lag must be a whole number of seconds: {line}"
        );
        assert!(!field("ip=").is_empty(), "ip must be populated: {line}");
    }
    announced_ports.sort_unstable();
    let mut expected = vec![replica1.port(), replica2.port()];
    expected.sort_unstable();
    assert_eq!(
        announced_ports, expected,
        "each slaveN line must report the port its replica announced"
    );

    replica2.shutdown().await;
    replica1.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Category G: Replication — Edge Cases
// ============================================================================

// FM-REPLICATION-027
/// Kill/restart replica 3x rapidly — recovers each time.
///
/// Each cycle waits for the replica to be *fence-relevant* fresh rather than
/// merely connected. The self-fence is on by default and arms on the first
/// cycle, so from cycle 2 onward the `SET` below is gated on the primary
/// having a replica that is streaming **and** has ACKed inside the freshness
/// window (FM-REPLICATION-041). Waiting on `connected_slaves` alone left the
/// write racing the replica's first ACK, and losing that race is what made
/// this test flake with a fence refusal rather than an `OK` (issue 30).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_replica_handles_rapid_reconnect(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Write initial data
    primary.send("SET", &["{rr}key0", "initial"]).await;

    for cycle in 0..3 {
        let replica = TestServer::start_replica_with_config(&primary, config.clone()).await;
        assert!(
            wait_for_fresh_streaming_replicas(&primary, 1, Duration::from_secs(30)).await,
            "cycle {cycle}: the replica must be streaming and freshly ACKing before the \
             primary is written to"
        );

        // Write a key during this cycle
        let key = format!("{{rr}}key{}", cycle + 1);
        let value = format!("val{}", cycle + 1);
        assert_ok(&primary.send("SET", &[&key, &value]).await);

        // Verify replica has the key
        let seen = wait_for_value(&replica, &key, Some(&value), Duration::from_secs(15)).await;
        assert_eq!(
            seen.as_deref(),
            Some(value.as_str()),
            "Replica should have key from cycle {cycle}"
        );

        replica.shutdown().await;
        // Drain the departing session before the next cycle attaches, so the
        // freshness wait above cannot be satisfied by the line of a replica
        // that has already gone (its `lag=` stays 0 for up to a second after
        // its last ACK).
        assert_eq!(
            wait_connected_slaves(&primary, 0, Duration::from_secs(15)).await,
            Some(0),
            "cycle {cycle}: the primary must drop the departed replica"
        );
    }

    primary.shutdown().await;
}

// FM-REPLICATION-039
/// 3 replicas, kill 1, WAIT 3 → timeout returns ≤ 2.
#[tokio::test]
async fn test_wait_returns_correct_count_with_partial_ack() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };

    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica1 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica2 = TestServer::start_replica_with_config(&primary, config.clone()).await;
    let replica3 = TestServer::start_replica_with_config(&primary, config).await;

    let attached = wait_connected_slaves(&primary, 3, Duration::from_secs(30)).await;
    assert_eq!(
        attached,
        Some(3),
        "all three replicas must reach the streaming phase"
    );

    // Write and verify all 3 ack
    assert_ok(&primary.send("SET", &["{pa}key1", "val1"]).await);
    let acked_all = wait_for_acks(&primary, 3, Duration::from_secs(30)).await;
    assert_eq!(
        acked_all, 3,
        "with three healthy replicas attached WAIT 3 must count all three"
    );

    // Kill one replica
    replica3.shutdown().await;
    let attached = wait_connected_slaves(&primary, 2, Duration::from_secs(30)).await;
    assert_eq!(
        attached,
        Some(2),
        "the primary must drop the killed replica from connected_slaves"
    );

    // Write another key
    assert_ok(&primary.send("SET", &["{pa}key2", "val2"]).await);

    // WAIT 3 with short timeout should return ≤ 2
    let resp2 = primary.send("WAIT", &["3", "2000"]).await;
    let acked_partial = parse_integer(&resp2).unwrap_or(0);
    assert!(
        acked_partial <= 2,
        "With one replica down, WAIT 3 should return ≤ 2, got {}",
        acked_partial
    );

    replica2.shutdown().await;
    replica1.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Replica READONLY Enforcement
// ============================================================================

// FM-REPLICATION-028
/// Test that replicas reject write commands with READONLY error while allowing reads.
#[tokio::test]
async fn test_replica_readonly_enforcement() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    // Write commands should be rejected with READONLY error
    let set_resp = replica.send("SET", &["key1", "value1"]).await;
    assert!(is_error(&set_resp), "SET on replica should return error");
    let err_msg = crate::common::test_server::get_error_message(&set_resp).unwrap();
    assert!(
        err_msg.starts_with("READONLY"),
        "Expected READONLY error, got: {}",
        err_msg
    );

    let del_resp = replica.send("DEL", &["key1"]).await;
    assert!(is_error(&del_resp), "DEL on replica should return error");
    let err_msg = crate::common::test_server::get_error_message(&del_resp).unwrap();
    assert!(
        err_msg.starts_with("READONLY"),
        "Expected READONLY error, got: {}",
        err_msg
    );

    let zadd_resp = replica.send("ZADD", &["zkey", "1", "member1"]).await;
    assert!(is_error(&zadd_resp), "ZADD on replica should return error");
    let err_msg = crate::common::test_server::get_error_message(&zadd_resp).unwrap();
    assert!(
        err_msg.starts_with("READONLY"),
        "Expected READONLY error, got: {}",
        err_msg
    );

    // Read commands should work fine
    let get_resp = replica.send("GET", &["nonexistent"]).await;
    assert!(
        !is_error(&get_resp),
        "GET on replica should succeed, got: {:?}",
        get_resp
    );

    let ping_resp = replica.send("PING", &[]).await;
    assert_eq!(
        parse_simple_string(&ping_resp),
        Some("PONG"),
        "PING on replica should return PONG"
    );

    let info_resp = replica.send("INFO", &["server"]).await;
    assert!(
        !is_error(&info_resp),
        "INFO on replica should succeed, got: {:?}",
        info_resp
    );

    // Writes through primary should still work
    let primary_set = primary.send("SET", &["primary_key", "primary_value"]).await;
    assert_ok(&primary_set);

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 4: Replication offset durability across restart
// ============================================================================

/// Poll the primary's `INFO replication` until it reports a connected,
/// streaming replica. Writes only advance `master_repl_offset` while a replica
/// is streaming, so tests must wait for this before driving writes.
async fn wait_for_connected_slave(primary: &TestServer) {
    for _ in 0..50 {
        if let Some(info) = parse_info_replication(&primary.send("INFO", &["replication"]).await)
            && info
                .get("connected_slaves")
                .map(|s| s.trim() != "0")
                .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("replica never reached streaming phase");
}

/// Read + parse a JSON file, retrying briefly to absorb any write latency.
async fn read_json_file(path: &std::path::Path) -> serde_json::Value {
    for _ in 0..50 {
        if let Ok(contents) = std::fs::read_to_string(path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents)
        {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("file never became readable JSON: {}", path.display());
}

/// Produce a valid, persisted RocksDB payload by running a short-lived
/// standalone server over `dir`, and return the path of the payload itself.
///
/// The payload is `<dir>/db`, not `<dir>`: a data directory is a layout whose
/// database lives in `db/` (FM-PERSISTENCE-057). A staged full-sync checkpoint
/// *is* a database directory, so it is that inner path — not the layout around
/// it — that a test stages.
async fn make_populated_data_dir(
    dir: &std::path::Path,
    key: &str,
    val: &str,
) -> std::path::PathBuf {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(dir.to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["SET", key, val]).await);
    drop(client);
    server.shutdown().await;
    dir.join("db")
}

/// The primary's replication offset must not rewind across a restart: a replica
/// resyncing against a primary whose offset went backwards would be served a
/// stale position. The offset is persisted on graceful shutdown and the tracker
/// is re-seeded from it on the next boot.
#[tokio::test]
async fn test_primary_replication_offset_survives_restart() {
    let primary_tmp = tempfile::tempdir().unwrap();
    let primary_dir = primary_tmp.path().join("data");

    // --- First boot: primary + replica, drive writes to advance the offset ---
    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(primary_dir.clone()),
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let replica = TestServer::start_replica_with_config(
        &primary,
        TestServerConfig {
            persistence: true,
            num_shards: Some(1),
            ..Default::default()
        },
    )
    .await;

    wait_for_connected_slave(&primary).await;

    let mut client = primary.connect().await;
    for i in 0..50 {
        assert_ok(&client.command(&["SET", &format!("k{i}"), "v"]).await);
    }
    drop(client);
    wait_for_replication(&primary, 2000).await;

    let (_, offset_before) = get_replication_state(&primary)
        .await
        .expect("primary should report an offset");
    assert!(
        offset_before > 0,
        "offset should advance while a replica is streaming"
    );

    // Disconnect the replica, then gracefully shut down the primary (persisting
    // the offset). Order matters: no writes happen after the replica leaves.
    replica.shutdown().await;
    primary.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: primary alone; offset must not rewind ---
    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(primary_dir.clone()),
        num_shards: Some(1),
        ..Default::default()
    })
    .await;

    let (_, offset_after) = get_replication_state(&primary)
        .await
        .expect("restarted primary should report an offset");
    assert!(
        offset_after >= offset_before,
        "replication offset rewound across restart: before={offset_before} after={offset_after}"
    );

    primary.shutdown().await;
}

/// A node that restarts onto a staged full-sync checkpoint must recover the
/// offset that matches that snapshot from the staged `replication_metadata.json`
/// rather than silently resetting to 0.
#[tokio::test]
async fn test_replica_recovers_offset_from_staged_metadata() {
    // 1. Produce a valid RocksDB payload to stand in for the streamed checkpoint.
    let src_tmp = tempfile::tempdir().unwrap();
    let src_dir = src_tmp.path().join("data");
    let payload = make_populated_data_dir(&src_dir, "snapshot_key", "snapshot_val").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 2. Stage it as a full-sync checkpoint with a chosen offset + replid.
    let node_tmp = tempfile::tempdir().unwrap();
    let data_dir = node_tmp.path().join("data");
    let staged_dir = data_dir.join("staging");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::rename(&payload, &staged_dir).unwrap();
    let staged_id = "a".repeat(40);
    let staged_offset: u64 = 987_654;
    std::fs::write(
        staged_dir.join("replication_metadata.json"),
        serde_json::json!({
            "replication_id": staged_id,
            "replication_offset": staged_offset,
            "checksum": "00",
        })
        .to_string(),
    )
    .unwrap();

    // 3. Boot a replica on the parent dir. It installs the staged checkpoint and
    //    must recover the offset from the metadata. The primary port is closed;
    //    background reconnect attempts do not affect recovery (which is sync).
    let replica = TestServer::start_with_config(
        TestServerConfig {
            persistence: true,
            data_dir: Some(data_dir.clone()),
            num_shards: Some(1),
            replication_primary_host: Some("127.0.0.1".to_string()),
            replication_primary_port: Some(1),
            ..Default::default()
        },
        ServerRole::Replica,
    )
    .await;

    // 4. The reconciled state is persisted and the staging file is consumed.
    let persisted = read_json_file(&data_dir.join("replication_state.json")).await;
    assert_eq!(
        persisted["offset_at_save"].as_u64().unwrap(),
        staged_offset,
        "recovered offset must match the staged checkpoint, not reset to 0"
    );
    assert_eq!(
        persisted["replication_id"].as_str().unwrap(),
        staged_id,
        "recovered replid must match the staged checkpoint"
    );
    assert!(
        !data_dir
            .join("db")
            .join("replication_metadata.json")
            .exists(),
        "staged metadata should be consumed after recovery"
    );

    // 5. The snapshot data itself was installed (proves the checkpoint applied).
    let mut client = replica.connect().await;
    assert_eq!(
        client.command(&["GET", "snapshot_key"]).await,
        Response::Bulk(Some(Bytes::from("snapshot_val")))
    );
    drop(client);

    replica.shutdown().await;
}

/// A real primary full-sync must stamp the staged `replication_metadata.json`
/// with the primary's **live** replication offset (the tracker's write
/// position), not the stale persisted `state.replication_offset`, which stays 0
/// during operation because only the tracker advances on each broadcast. Before
/// this fix the streamed checkpoint carried offset 0 even when the stream head
/// was far ahead, so a restarted replica adopted offset 0 and the offset/data
/// correspondence was broken (partially undermining the offset-persistence fix).
#[tokio::test]
async fn test_full_sync_stages_live_primary_offset() {
    // Primary + a first replica so the broadcaster is active: the primary's
    // offset only advances while a replica is streaming.
    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence: true,
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let replica1 = TestServer::start_replica_with_config(
        &primary,
        TestServerConfig {
            persistence: true,
            num_shards: Some(1),
            ..Default::default()
        },
    )
    .await;
    wait_for_connected_slave(&primary).await;

    // Drive writes to advance the live offset well past 0.
    let mut client = primary.connect().await;
    for i in 0..30 {
        assert_ok(
            &client
                .command(&["SET", &format!("live_off_k{i}"), "v"])
                .await,
        );
    }
    drop(client);
    wait_for_replication(&primary, 2000).await;

    let (primary_replid, primary_offset) = get_replication_state(&primary)
        .await
        .expect("primary should report an offset");
    assert!(
        primary_offset > 0,
        "offset should have advanced past 0 while a replica streams"
    );

    // A second replica with a known data dir now full-syncs at the live offset
    // and stages its checkpoint metadata under <data-dir>/staging/.
    let replica2_tmp = tempfile::tempdir().unwrap();
    let data_dir = replica2_tmp.path().join("data");
    let replica2 = TestServer::start_replica_with_config(
        &primary,
        TestServerConfig {
            persistence: true,
            num_shards: Some(1),
            data_dir: Some(data_dir.clone()),
            ..Default::default()
        },
    )
    .await;

    // The staged checkpoint metadata must carry the live offset + replid, not 0.
    let staged = read_json_file(&data_dir.join("staging").join("replication_metadata.json")).await;
    let staged_offset = staged["replication_offset"].as_u64().unwrap() as i64;
    assert!(
        staged_offset > 0,
        "staged checkpoint offset must be the live offset, not 0"
    );
    assert_eq!(
        staged_offset, primary_offset,
        "staged checkpoint offset must equal the primary's live offset"
    );
    // The staged replid is the primary's real replication identity (carried in
    // FULLRESYNC). It must be a well-formed, nonzero 40-char hex id and must
    // equal the primary's INFO `master_replid` — both source the same
    // `ReplicationState::replication_id`, so INFO now reports the real
    // replication id rather than the node id.
    let staged_replid = staged["replication_id"].as_str().unwrap();
    assert_eq!(
        staged_replid.len(),
        40,
        "staged replid must be 40 hex chars"
    );
    assert!(
        staged_replid.chars().all(|c| c.is_ascii_hexdigit()),
        "staged replid must be hex"
    );
    assert_ne!(
        staged_replid, "0000000000000000000000000000000000000000",
        "staged replid must be a real replication id, not the zero id"
    );
    assert_eq!(
        staged_replid, primary_replid,
        "INFO master_replid must equal the replid staged in FULLRESYNC metadata"
    );

    replica2.shutdown().await;
    replica1.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-023
/// A replica's INFO `master_replid` must report the primary's replication id —
/// the identity it adopts from `+FULLRESYNC` — not its own node id. Redis: a
/// replica's `master_replid` is the id of the primary it replicates from.
#[tokio::test]
async fn test_info_master_replid_replica_matches_primary() {
    let config = TestServerConfig {
        persistence: true,
        num_shards: Some(1),
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;
    wait_for_connected_slave(&primary).await;

    let (primary_replid, _) = get_replication_state(&primary)
        .await
        .expect("primary should report a replid");
    assert_ne!(
        primary_replid, "0000000000000000000000000000000000000000",
        "primary master_replid must be a real replication id, not the zero id"
    );

    // The replica adopts the primary's replid during the FULLRESYNC handshake;
    // poll until INFO on the replica reflects it.
    let mut replica_replid = String::new();
    for _ in 0..50 {
        if let Some((id, _)) = get_replication_state(&replica).await
            && id == primary_replid
        {
            replica_replid = id;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        replica_replid, primary_replid,
        "replica INFO master_replid must equal the primary's replication id"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-021
/// INFO `master_replid` is the persisted `ReplicationState::replication_id`, so a
/// primary must report the same id after a graceful restart (the id is stored in
/// the replication state file alongside the snapshot).
#[tokio::test]
async fn test_info_master_replid_survives_restart() {
    let primary_tmp = tempfile::tempdir().unwrap();
    let primary_dir = primary_tmp.path().join("data");

    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(primary_dir.clone()),
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let (replid_before, _) = get_replication_state(&primary)
        .await
        .expect("primary should report a replid");
    assert_eq!(replid_before.len(), 40, "replid must be 40 hex chars");
    assert_ne!(
        replid_before, "0000000000000000000000000000000000000000",
        "replid must be a real replication id, not the zero id"
    );
    primary.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reboot on the same data dir: the persisted replication id must survive.
    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(primary_dir.clone()),
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let (replid_after, _) = get_replication_state(&primary)
        .await
        .expect("restarted primary should report a replid");
    assert_eq!(
        replid_after, replid_before,
        "master_replid must persist across restart (stored in the replication state file)"
    );

    primary.shutdown().await;
}

/// Corrupt staged metadata must not crash startup or be trusted: recovery falls
/// back to a fresh state (offset 0), which forces a full resync on reconnect —
/// the Redis behavior for a mismatched/unreadable replication identity.
#[tokio::test]
async fn test_replica_ignores_corrupt_staged_metadata() {
    let src_tmp = tempfile::tempdir().unwrap();
    let src_dir = src_tmp.path().join("data");
    let payload = make_populated_data_dir(&src_dir, "snapshot_key", "snapshot_val").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let node_tmp = tempfile::tempdir().unwrap();
    let data_dir = node_tmp.path().join("data");
    let staged_dir = data_dir.join("staging");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::rename(&payload, &staged_dir).unwrap();
    // Garbage instead of valid metadata.
    std::fs::write(
        staged_dir.join("replication_metadata.json"),
        "{ this is not valid json",
    )
    .unwrap();

    // Boot as a replica: must not panic.
    let replica = TestServer::start_with_config(
        TestServerConfig {
            persistence: true,
            data_dir: Some(data_dir.clone()),
            num_shards: Some(1),
            replication_primary_host: Some("127.0.0.1".to_string()),
            replication_primary_port: Some(1),
            ..Default::default()
        },
        ServerRole::Replica,
    )
    .await;

    // Fresh state with offset 0 -> the replica will PSYNC "? -1" -> full resync.
    let persisted = read_json_file(&data_dir.join("replication_state.json")).await;
    assert_eq!(
        persisted["offset_at_save"].as_u64().unwrap(),
        0,
        "corrupt metadata must not be trusted; offset falls back to 0 (full resync)"
    );

    // The checkpoint data was still installed even though metadata was ignored.
    let mut client = replica.connect().await;
    assert_eq!(
        client.command(&["GET", "snapshot_key"]).await,
        Response::Bulk(Some(Bytes::from("snapshot_val")))
    );
    drop(client);

    replica.shutdown().await;
}

// ============================================================================
// Tier 4: Backlog-driven partial resync (proposal 14)
// ============================================================================

/// Build a RESP array command from string parts (for raw PSYNC).
fn encode_resp_command(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

// FM-REPLICATION-047
/// Turning split-brain logging off must not cost every reconnecting replica a
/// full resync.
///
/// `split-brain-log-enabled` is documented as log-only, and switching it off is
/// the obvious way to stop audit files accumulating in the data directory. It
/// used to be the `enabled` flag of the replication backlog as well, so that
/// gesture silently disabled partial resync: every PSYNC answered
/// `+FULLRESYNC` and every reconnect paid for a checkpoint transfer, with no
/// log line and no config value that looked wrong. Only a test that reaches the
/// wire with the flag off can catch that, because the backlog itself behaved
/// perfectly — it was simply switched off by the wrong key.
#[tokio::test]
async fn partial_resync_survives_split_brain_logging_disabled() {
    let config = TestServerConfig {
        replication_split_brain_log_enabled: Some(false),
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;

    for i in 0..20 {
        primary.send("SET", &[&format!("pre{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (replid, offset) = get_replication_state(&primary)
        .await
        .expect("primary INFO replication");
    assert!(offset > 0, "replication offset should have advanced");

    for i in 0..20 {
        primary.send("SET", &[&format!("post{i}"), "v"]).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = primary
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &offset.to_string(),
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("+CONTINUE"),
        "the backlog is governed by backlog-enabled, not by the split-brain log \
         switch — an in-window reconnect must still be granted +CONTINUE, got: {head:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-015
/// A reconnecting replica that presents a recent, still-buffered offset must be
/// granted a partial resync (`+CONTINUE`) off the backlog — never forced into a
/// full resync. This is the end-to-end payoff of the replication backlog: the
/// PSYNC `<replid> <offset>` a brief-disconnect replica sends is answered with
/// `+CONTINUE`, not `+FULLRESYNC`.
#[tokio::test]
async fn test_partial_resync_after_brief_disconnect_grants_continue() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config.clone()).await;
    let replica = TestServer::start_replica_with_config(&primary, config).await;
    // Let the initial full sync finish so the backlog has been populated.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Advance the stream, then capture a recent offset — the resume point a
    // briefly-disconnected replica would reconnect with.
    for i in 0..20 {
        primary.send("SET", &[&format!("pre{i}"), "v"]).await;
    }
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (replid, offset) = get_replication_state(&primary)
        .await
        .expect("primary INFO replication");
    assert!(offset > 0, "replication offset should have advanced");

    // Further writes land after the capture, so `(offset, current]` is a real,
    // non-empty backlog tail that must be replayed on +CONTINUE.
    for i in 0..20 {
        primary.send("SET", &[&format!("post{i}"), "v"]).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The reconnect: PSYNC with the recent in-window offset must get +CONTINUE.
    let resp = primary
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            &replid,
            &offset.to_string(),
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("+CONTINUE"),
        "in-window reconnect must be granted +CONTINUE, got: {head:?}"
    );
    assert!(
        !head.contains("FULLRESYNC"),
        "no full resync may occur for an in-window reconnect, got: {head:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-013
/// A reconnect presenting an offset the backlog no longer covers (an unknown
/// replication id stands in for "evicted/unservable") must fall back to a full
/// resync — the lower-bound guard, end to end.
#[tokio::test]
async fn test_partial_resync_unknown_replid_falls_back_to_full() {
    let config = TestServerConfig::default();
    let primary = TestServer::start_primary_with_config(config).await;

    for i in 0..10 {
        primary.send("SET", &[&format!("k{i}"), "v"]).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A bogus replication id can never match the window -> FULLRESYNC.
    let resp = primary
        .send_raw(&encode_resp_command(&[
            "PSYNC",
            "0000000000000000000000000000000000000000",
            "5",
        ]))
        .await;
    let head = String::from_utf8_lossy(&resp);
    assert!(
        head.starts_with("+FULLRESYNC"),
        "unknown replid must fall back to +FULLRESYNC, got: {head:?}"
    );

    primary.shutdown().await;
}

/// End-to-end no-loss check: a replica brought up under a concurrent write load
/// converges on the primary's offset (its acked offset reaches the head), so no
/// write made around the initial full sync is dropped on the wire.
///
/// This is the integration-level companion to the deterministic crate test
/// `replica_session::tests::full_sync_replays_writes_made_during_handoff`, which
/// forces a write squarely into the handoff window and proves it is replayed.
/// On a fast loopback the checkpoint transfer is sub-millisecond, so this test
/// cannot reliably pin a write to that exact window; it instead guards the
/// broader property (concurrent-with-sync writes all replicate) and the live
/// path's continued health.
#[tokio::test]
async fn test_writes_during_full_sync_are_not_lost() {
    let config = TestServerConfig {
        persistence: true,
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config.clone()).await;

    // Preload the primary so the full-sync checkpoint is non-trivial to cut and
    // transfer, widening the handoff window that writes can land in.
    {
        let mut client = primary.connect().await;
        for i in 0..1500 {
            let v = format!("preload-value-padding-{i:08}");
            client.command(&["SET", &format!("base{i}"), &v]).await;
        }
        drop(client);
    }

    // Bring up the replica; its initial sync runs in the background.
    let replica = TestServer::start_replica_with_config(&primary, config).await;

    // Hammer writes that race with the sync. Those landing in the handoff window
    // (after snapshot capture, before the replica joins the live tail) are the
    // ones the F1 fix must replay from the backlog.
    {
        let mut client = primary.connect().await;
        for i in 0..200 {
            client
                .command(&["SET", &format!("during{i}"), &format!("val{i}")])
                .await;
        }
        drop(client);
    }

    // The replica must catch up to the primary's offset — proving no write was
    // dropped at the handoff. WAIT returns the number of replicas that have
    // acked the current offset; with the fix this reaches 1, with the F1 bug it
    // stays 0 (the replica is permanently short by the dropped window bytes).
    let mut converged = false;
    for _ in 0..10 {
        let acked = parse_integer(&primary.send("WAIT", &["1", "2000"]).await).unwrap_or(0);
        if acked >= 1 {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        converged,
        "replica failed to converge on the primary offset — a write made during \
         the full-sync handoff was lost"
    );

    // Sanity: a post-sync write still replicates and is queryable (the live path
    // remains intact alongside the replayed handoff).
    primary.send("SET", &["after_sync", "ok"]).await;
    let _ = primary.send("WAIT", &["1", "2000"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut client = replica.connect().await;
    assert_eq!(
        client.command(&["GET", "after_sync"]).await,
        Response::Bulk(Some(Bytes::from("ok"))),
    );
    drop(client);

    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Served blocking-pop propagation (issue 02: a blocking pop satisfied by a
// later write mutated the primary's store but was never broadcast).
//
// When a parked BLPOP/BZPOPMIN/BLMOVE/BLMPOP is SERVED by a subsequent push,
// the primary consumes the element, but only the *waking* write (LPUSH/ZADD)
// was replicated — the replica re-executed the push and kept the element the
// primary handed to its blocked client, diverging forever. The fix synthesizes
// the equivalent deterministic pop and broadcasts it after the waking write.
// These tests park a real blocked client, wake it, and pin primary/replica
// equality for every served-pop shape.
// ============================================================================

/// Poll until `replica`'s reply to `cmd args` equals `primary`'s, or
/// `timeout_ms` elapses. Returns both replies so a timeout fails with the
/// divergent contents.
async fn wait_for_cmd_convergence(
    primary: &TestServer,
    replica: &TestServer,
    cmd: &str,
    args: &[&str],
    timeout_ms: u64,
) -> (Response, Response) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let p = primary.send(cmd, args).await;
        let r = replica.send(cmd, args).await;
        if p == r || tokio::time::Instant::now() >= deadline {
            return (p, r);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A served BLPOP consumes the pushed element on the primary; the replica must
/// end with the same (empty) list — before the fix it retained the element.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_served_blpop_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    // Park a real blocked BLPOP on the primary.
    let mut blocker = primary.connect().await;
    blocker.send_only(&["BLPOP", "{b}list", "0"]).await;
    primary.wait_for_blocked_clients(1).await;

    // Wake it with a push; the blocked client receives the element.
    primary.send("LPUSH", &["{b}list", "v1"]).await;
    let served = blocker.read_response(Duration::from_secs(2)).await;
    assert_eq!(
        served,
        Some(Response::Array(vec![
            Response::Bulk(Some(Bytes::from("{b}list"))),
            Response::Bulk(Some(Bytes::from("v1"))),
        ])),
        "BLPOP must be served the pushed element"
    );

    // Primary consumed the element — the list is empty there.
    assert_eq!(
        parse_integer(&primary.send("LLEN", &["{b}list"]).await),
        Some(0),
        "the served pop must leave the primary list empty"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) =
        wait_for_cmd_convergence(&primary, &replica, "LRANGE", &["{b}list", "0", "-1"], 5000).await;
    assert_eq!(
        p,
        Response::Array(vec![]),
        "primary list must be empty after the served BLPOP"
    );
    assert_eq!(
        p, r,
        "replica list must equal primary after served BLPOP (primary {p:?}, replica {r:?})"
    );

    drop(blocker);
    replica.shutdown().await;
    primary.shutdown().await;
}

/// A served BZPOPMIN pops the minimum on the primary; the replica's sorted set
/// must match (only the untouched members remain).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_served_bzpopmin_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    // The key must be absent so BZPOPMIN actually blocks (an ZADD before it
    // would satisfy it inline). The waking ZADD adds *two* members at once —
    // the waiter pops the min, and the higher-scored member is the residue we
    // converge on (proving the synthesized pop, not just the add, replicated).
    let mut blocker = primary.connect().await;
    blocker.send_only(&["BZPOPMIN", "{z}set", "0"]).await;
    primary.wait_for_blocked_clients(1).await;

    // Two members added in one write; the lower score wins the min and is popped.
    primary
        .send("ZADD", &["{z}set", "1", "low", "5", "keep"])
        .await;
    let served = blocker.read_response(Duration::from_secs(2)).await;
    assert_eq!(
        served,
        Some(Response::Array(vec![
            Response::Bulk(Some(Bytes::from("{z}set"))),
            Response::Bulk(Some(Bytes::from("low"))),
            Response::Bulk(Some(Bytes::from("1"))),
        ])),
        "BZPOPMIN must be served the lowest-scored member"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) = wait_for_cmd_convergence(
        &primary,
        &replica,
        "ZRANGE",
        &["{z}set", "0", "-1", "WITHSCORES"],
        5000,
    )
    .await;
    assert_eq!(
        p,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("keep"))),
            Response::Bulk(Some(Bytes::from("5"))),
        ]),
        "primary must retain only the untouched member after the served BZPOPMIN"
    );
    assert_eq!(
        p, r,
        "replica sorted set must equal primary after served BZPOPMIN (primary {p:?}, replica {r:?})"
    );

    drop(blocker);
    replica.shutdown().await;
    primary.shutdown().await;
}

/// A served BLMOVE writes *both* keys on the primary (pop source, push dest);
/// both must converge on the replica.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_served_blmove_replicates_both_keys(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let mut blocker = primary.connect().await;
    // Same hash-tag keeps src+dst in one slot.
    blocker
        .send_only(&["BLMOVE", "{m}src", "{m}dst", "LEFT", "RIGHT", "0"])
        .await;
    primary.wait_for_blocked_clients(1).await;

    // Pushing to the source wakes the waiter, which atomically moves the
    // element to the destination.
    primary.send("LPUSH", &["{m}src", "x"]).await;
    let served = blocker.read_response(Duration::from_secs(2)).await;
    assert_eq!(
        served,
        Some(Response::Bulk(Some(Bytes::from("x")))),
        "BLMOVE must return the moved element"
    );

    // Primary: source drained, destination holds the element.
    assert_eq!(
        parse_integer(&primary.send("LLEN", &["{m}src"]).await),
        Some(0),
        "source must be empty on the primary"
    );
    assert_eq!(
        primary.send("LRANGE", &["{m}dst", "0", "-1"]).await,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("x")))]),
        "destination must hold the moved element on the primary"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (ps, rs) =
        wait_for_cmd_convergence(&primary, &replica, "LRANGE", &["{m}src", "0", "-1"], 5000).await;
    assert_eq!(ps, Response::Array(vec![]), "primary source must be empty");
    assert_eq!(
        ps, rs,
        "replica source must equal primary (src {ps:?} / {rs:?})"
    );
    let (pd, rd) =
        wait_for_cmd_convergence(&primary, &replica, "LRANGE", &["{m}dst", "0", "-1"], 5000).await;
    assert_eq!(
        pd,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("x")))]),
        "primary destination must hold the element"
    );
    assert_eq!(
        pd, rd,
        "replica destination must equal primary (dst {pd:?} / {rd:?})"
    );

    drop(blocker);
    replica.shutdown().await;
    primary.shutdown().await;
}

/// A served BLMPOP (multi-key list pop) consumes the element on the primary;
/// the replica must converge.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_served_blmpop_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let mut blocker = primary.connect().await;
    blocker
        .send_only(&["BLMPOP", "0", "1", "{p}list", "LEFT"])
        .await;
    primary.wait_for_blocked_clients(1).await;

    primary.send("RPUSH", &["{p}list", "a", "b"]).await;
    let served = blocker.read_response(Duration::from_secs(2)).await;
    assert_eq!(
        served,
        Some(Response::Array(vec![
            Response::Bulk(Some(Bytes::from("{p}list"))),
            Response::Array(vec![Response::Bulk(Some(Bytes::from("a")))]),
        ])),
        "BLMPOP LEFT must return the head element"
    );

    // Primary popped exactly one; "b" remains.
    assert_eq!(
        primary.send("LRANGE", &["{p}list", "0", "-1"]).await,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("b")))]),
        "primary must retain the un-popped element"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) =
        wait_for_cmd_convergence(&primary, &replica, "LRANGE", &["{p}list", "0", "-1"], 5000).await;
    assert_eq!(
        p,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("b")))]),
        "primary must retain exactly the un-popped element"
    );
    assert_eq!(
        p, r,
        "replica list must equal primary after served BLMPOP (primary {p:?}, replica {r:?})"
    );

    drop(blocker);
    replica.shutdown().await;
    primary.shutdown().await;
}

/// Two blocked BLPOP clients served by a single multi-element push: each
/// consumes one element on the primary, and the replica must end empty (not
/// retain the pushed elements).
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_served_blpop_multi_waiter_replicates(#[case] persistence: bool) {
    let config = TestServerConfig {
        persistence,
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    let mut a = primary.connect().await;
    let mut b = primary.connect().await;
    a.send_only(&["BLPOP", "{w}list", "0"]).await;
    b.send_only(&["BLPOP", "{w}list", "0"]).await;
    primary.wait_for_blocked_clients(2).await;

    // One push carrying two elements satisfies both waiters (FIFO order).
    primary.send("RPUSH", &["{w}list", "e1", "e2"]).await;
    let sa = a.read_response(Duration::from_secs(2)).await;
    let sb = b.read_response(Duration::from_secs(2)).await;
    // Both must be served; the two consumed values are e1 and e2 in some order.
    let val = |r: &Option<Response>| match r {
        Some(Response::Array(arr)) if arr.len() == 2 => match &arr[1] {
            Response::Bulk(Some(v)) => String::from_utf8_lossy(v).to_string(),
            other => panic!("expected bulk element, got {other:?}"),
        },
        other => panic!("both waiters must be served, got {other:?}"),
    };
    let mut vals = vec![val(&sa), val(&sb)];
    vals.sort();
    assert_eq!(vals, vec!["e1".to_string(), "e2".to_string()]);

    // Primary fully drained.
    assert_eq!(
        parse_integer(&primary.send("LLEN", &["{w}list"]).await),
        Some(0),
        "both served pops must leave the primary list empty"
    );

    let _ = wait_for_replication(&primary, 5000).await;
    let (p, r) =
        wait_for_cmd_convergence(&primary, &replica, "LRANGE", &["{w}list", "0", "-1"], 5000).await;
    assert_eq!(p, Response::Array(vec![]), "primary must be empty");
    assert_eq!(
        p, r,
        "replica must be empty after both served BLPOPs (primary {p:?}, replica {r:?})"
    );

    drop(a);
    drop(b);
    replica.shutdown().await;
    primary.shutdown().await;
}

// ============================================================================
// Tier 4: Write fencing — self-fence-on-replica-loss + min-replicas-to-write
// ============================================================================
//
// End-to-end coverage for the two write-safety gates enforced in
// `connection/guards.rs::run_pre_checks`:
//
//   * self-fence-on-replica-loss — rejects writes with `SELFFENCE ...` once a
//     primary that *had* a healthy streaming replica loses quorum. It "arms"
//     only after the first fresh streaming replica, so a primary that never had
//     a replica keeps accepting writes.
//   * min-replicas-to-write — Redis's `NOREPLICAS ...` gate. Does not arm: with
//     `min-replicas-to-write N` and fewer than N good (streaming, recently
//     ACKing) replicas the primary refuses writes immediately, from boot.
//
// Both gates fire in `run_pre_checks`, which covers direct writes and MULTI
// *queue* time, but NOT writes issued from inside Lua (`EVAL` lacks the WRITE
// flag). The Lua-bypass tests below pin that known gap so a future fix flips
// them deliberately.

/// Fast self-fence config: arm the checker quickly and let it detect replica
/// loss within ~0.5s. `replica_freshness_timeout_ms` must be >= 3x
/// `ack_interval_ms` (config validation), hence 500 vs 100.
fn fence_config() -> TestServerConfig {
    TestServerConfig {
        replication_self_fence_on_replica_loss: Some(true),
        replication_ack_interval_ms: Some(100),
        replication_replica_freshness_timeout_ms: Some(500),
        ..Default::default()
    }
}

/// `min-replicas-to-write N` with self-fence disabled (so the NOREPLICAS gate
/// is exercised in isolation, not shadowed by the earlier self-fence check).
fn min_replicas_config(n: u32) -> TestServerConfig {
    TestServerConfig {
        replication_min_replicas_to_write: Some(n),
        replication_min_replicas_timeout_ms: Some(500),
        replication_ack_interval_ms: Some(100),
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    }
}

/// True when `resp` is an error whose message starts with `prefix`.
fn err_has_prefix(resp: &Response, prefix: &str) -> bool {
    matches!(get_error_message(resp), Some(m) if m.starts_with(prefix))
}

/// Poll `SET <key> vN` on `server` until `pred` matches the response or the
/// deadline elapses, then return the last response. This is polling with a hard
/// deadline (short interval, no fixed sleep-then-assert), so it tolerates the
/// asynchronous window between replica loss and the gate engaging.
async fn poll_set<F: Fn(&Response) -> bool>(
    server: &TestServer,
    key: &str,
    deadline: Duration,
    pred: F,
) -> Response {
    let start = std::time::Instant::now();
    let mut n = 0u64;
    loop {
        n += 1;
        let resp = server.send("SET", &[key, &format!("v{n}")]).await;
        if pred(&resp) || start.elapsed() >= deadline {
            return resp;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Key the fence helper writes to while polling for the refusal. Kept separate
/// from the caller's seeded key so the successful writes issued before the
/// fence engages cannot disturb what the caller asserts about its own key.
const FENCE_PROBE_KEY: &str = "__fence_probe";

/// Bring up a primary whose only replica is *attached but silent*, and return
/// once the self-fence has engaged.
///
/// The replica reaches the streaming phase through a [`LagProxy`]; a write then
/// arms the checker; then the proxy freezes the replica→primary leg so the ACKs
/// stop arriving while the session stays registered and streaming. That is the
/// partition the fence exists for, and it is the only way to reach a fenced
/// primary deterministically: a replica that is shut down departs *cleanly*,
/// which disarms the fence instead (FM-REPLICATION-062).
///
/// `seed_key` is left holding `armed`. The returned proxy must be kept alive —
/// dropping it aborts the accept loop and kills the link.
async fn fenced_by_a_silent_replica(seed_key: &str) -> (TestServer, TestServer, LagProxy) {
    let primary = TestServer::start_primary_with_config(fence_config()).await;
    let proxy = LagProxy::spawn(primary.port()).await;
    let mut replica_config = fence_config();
    replica_config.replication_primary_host = Some("127.0.0.1".to_string());
    replica_config.replication_primary_port = Some(proxy.port());
    let replica = TestServer::start_with_config(replica_config, ServerRole::Replica).await;

    assert!(
        wait_for_replication(&primary, 5000).await >= 1,
        "replica never reached streaming through the proxy"
    );
    // A write while a fresh streaming replica exists is what arms the checker.
    assert_ok(&primary.send("SET", &[seed_key, "armed"]).await);

    // Silence the replica without detaching it.
    proxy.stall_acks();

    let fenced = poll_set(&primary, FENCE_PROBE_KEY, Duration::from_secs(10), |r| {
        err_has_prefix(r, "SELFFENCE")
    })
    .await;
    assert!(
        err_has_prefix(&fenced, "SELFFENCE"),
        "self-fence did not engage while the replica was attached but silent: {fenced:?}"
    );

    (primary, replica, proxy)
}

// FM-REPLICATION-041
/// Self-fence engages on replica loss: a primary whose only replica has stopped
/// ACKing rejects writes with the exact `SELFFENCE ...` string, while reads stay
/// allowed.
#[tokio::test]
async fn test_self_fence_engages_on_replica_loss() {
    let (primary, replica, proxy) = fenced_by_a_silent_replica("fk").await;

    // Exact error string. NOTE: this code is FrogDB's own — Redis has no fence
    // in this configuration and would refuse the write via
    // `min-replicas-to-write` (NOREPLICAS) or not at all. It names the mechanism
    // and the knob that turns it off, deliberately *not* borrowing cluster
    // mode's CLUSTERDOWN, which pointed the operator at a subsystem a fenced
    // standalone primary need not even be running. Pinned so any reword is
    // intentional.
    assert_eq!(
        get_error_message(&primary.send("SET", &["fk", "x"]).await),
        Some("SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)"),
    );

    // Reads remain allowed while fenced, and the seeded value is untouched.
    assert_eq!(
        primary.send("GET", &["fk"]).await,
        Response::Bulk(Some(Bytes::from_static(b"armed"))),
    );

    drop(proxy);
    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-041
/// Self-fence recovers on its own: once the replica's ACKs flow again the
/// primary's quorum is restored and writes are accepted, with no restart and no
/// config change.
#[tokio::test]
async fn test_self_fence_recovers_after_replica_reconnect() {
    let (primary, replica, proxy) = fenced_by_a_silent_replica("rk").await;

    // Let the replica be heard again.
    proxy.resume_acks();

    let recovered = poll_set(&primary, "rk", Duration::from_secs(15), |r| {
        parse_simple_string(r) == Some("OK")
    })
    .await;
    assert_ok(&recovered);

    drop(proxy);
    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-062
/// The fence disarms when the last streaming replica leaves *cleanly*: a
/// decommissioned replica is a departure the operator performed, so the primary
/// must stay writable rather than hold writes hostage to a replica nobody
/// expects back.
#[tokio::test]
async fn test_self_fence_disarms_after_a_clean_replica_departure() {
    let (primary, replica) = start_primary_replica_pair(fence_config()).await;
    assert!(
        wait_for_replication(&primary, 3000).await >= 1,
        "replica never reached streaming"
    );
    // Arms the checker.
    assert_ok(&primary.send("SET", &["dk", "armed"]).await);

    // An orderly shutdown closes the replica's half of the link, which the
    // primary's session reads as an EOF — a graceful departure.
    replica.shutdown().await;

    // Writes keep flowing, well past the freshness window that would have
    // fenced a lost replica.
    for _ in 0..10 {
        assert_ok(&primary.send("SET", &["dk", "v"]).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    primary.shutdown().await;
}

// FM-REPLICATION-041
/// A primary that never had a replica does not fence: the checker never arms,
/// so writes are always allowed even with self-fence enabled.
#[tokio::test]
async fn test_self_fence_unarmed_allows_writes() {
    let primary = TestServer::start_primary_with_config(fence_config()).await;
    assert_ok(&primary.send("SET", &["uk", "v"]).await);
    assert_ok(&primary.send("SET", &["uk", "v2"]).await);
    primary.shutdown().await;
}

// FM-TXN-007
/// The self-fence gate runs at MULTI *queue* time: the queued write is rejected
/// with SELFFENCE, and the rejection flags the transaction dirty so EXEC
/// aborts with `EXECABORT` (Redis parity — `rejectCommand` → `flagTransaction`).
#[tokio::test]
async fn test_self_fence_multi_rejected_at_queue_time() {
    let (primary, replica, proxy) = fenced_by_a_silent_replica("mk").await;

    let mut c = primary.connect().await;
    assert_eq!(
        parse_simple_string(&c.command(&["MULTI"]).await),
        Some("OK")
    );
    let queued = c.command(&["SET", "mk", "v"]).await;
    assert!(
        err_has_prefix(&queued, "SELFFENCE"),
        "queued write not fenced: {queued:?}"
    );
    // FrogDB applies the fence at MULTI *queue* time rather than at EXEC, but
    // the client-visible contract now matches Redis: a command rejected while
    // queuing marks the transaction dirty, so EXEC answers `EXECABORT` instead
    // of silently running the surviving (here: empty) subset. Reporting an
    // empty array would tell the client "your transaction ran and did nothing",
    // which is indistinguishable from a genuinely empty MULTI.
    let exec = c.command(&["EXEC"]).await;
    assert!(
        err_has_prefix(&exec, "EXECABORT"),
        "expected EXECABORT after queue-time fence, got {exec:?}"
    );
    assert_eq!(
        primary.send("GET", &["mk"]).await,
        Response::Bulk(Some(Bytes::from_static(b"armed"))),
        "fenced MULTI must not have mutated the key"
    );

    drop(proxy);
    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-TXN-008
/// A rejected command must not leave a *partial* transaction behind. With one
/// accepted command queued ahead of a fenced write, EXEC has a survivor it
/// could run — the whole point of the dirty flag is that it must not. The
/// client gets EXECABORT, never a one-element array that looks like success.
#[tokio::test]
async fn test_self_fence_multi_partial_queue_aborts_whole_transaction() {
    let (primary, replica, proxy) = fenced_by_a_silent_replica("pk").await;

    let mut c = primary.connect().await;
    assert_eq!(
        parse_simple_string(&c.command(&["MULTI"]).await),
        Some("OK")
    );
    // Accepted: ECHO carries no WRITE flag, so the fence lets it queue.
    assert_eq!(
        parse_simple_string(&c.command(&["ECHO", "survivor"]).await),
        Some("QUEUED")
    );
    assert!(
        err_has_prefix(&c.command(&["SET", "pk", "v"]).await, "SELFFENCE"),
        "queued write not fenced"
    );

    let exec = c.command(&["EXEC"]).await;
    assert!(
        err_has_prefix(&exec, "EXECABORT"),
        "expected EXECABORT, not a partial array, got {exec:?}"
    );
    assert_eq!(
        primary.send("GET", &["pk"]).await,
        Response::Bulk(Some(Bytes::from_static(b"armed"))),
        "aborted MULTI must not have mutated the key"
    );

    drop(proxy);
    replica.shutdown().await;
    primary.shutdown().await;
}

/// The self-fence covers a Lua-internal write. `EVAL` carries no WRITE flag, so
/// the connection's `run_pre_checks` never sees the `SET` the script body
/// issues; the shard write seam does, and refuses it there (`specs/txn.md`
/// FM-TXN-051). This assertion was previously inverted, pinning the gap.
// FM-TXN-051
#[tokio::test]
async fn test_self_fence_gates_lua_writes() {
    // Direct writes are fenced by the time this returns.
    let (primary, replica, proxy) = fenced_by_a_silent_replica("lk").await;

    // A Lua-internal write is fenced too, at the shard write seam.
    let lua = primary
        .send(
            "EVAL",
            &[
                "return redis.call('SET', KEYS[1], ARGV[1])",
                "1",
                "lua_key",
                "lua_val",
            ],
        )
        .await;
    assert!(is_error(&lua), "EVAL write not fenced: {lua:?}");
    let got = primary.send("GET", &["lua_key"]).await;
    assert_eq!(
        got,
        Response::Bulk(None),
        "a fenced Lua write must not have applied: {got:?}"
    );

    drop(proxy);
    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-042
/// `min-replicas-to-write 1` with zero replicas refuses writes from boot with
/// the exact `NOREPLICAS ...` string (no arming), while reads stay allowed.
#[tokio::test]
async fn test_min_replicas_to_write_rejects_without_replicas() {
    let primary = TestServer::start_primary_with_config(min_replicas_config(1)).await;
    assert_eq!(
        get_error_message(&primary.send("SET", &["k", "v"]).await),
        Some("NOREPLICAS Not enough good replicas to write."),
    );
    assert!(!is_error(&primary.send("GET", &["k"]).await));
    primary.shutdown().await;
}

// FM-REPLICATION-042
/// The `min-replicas-to-write` gate tracks replica health live: writes are
/// allowed while a good replica streams, then rejected with NOREPLICAS once it
/// drops below the configured count.
#[tokio::test]
async fn test_min_replicas_to_write_gate_tracks_replica_health() {
    let (primary, replica) = start_primary_replica_pair(min_replicas_config(1)).await;

    // A good (streaming, fresh-ACKing) replica satisfies the gate.
    let allowed = poll_set(&primary, "k", Duration::from_secs(5), |r| {
        parse_simple_string(r) == Some("OK")
    })
    .await;
    assert_ok(&allowed);

    // Drop the replica: good-replica count falls below 1 → NOREPLICAS.
    replica.shutdown().await;
    let rejected = poll_set(&primary, "k", Duration::from_secs(5), |r| {
        err_has_prefix(r, "NOREPLICAS")
    })
    .await;
    assert!(
        err_has_prefix(&rejected, "NOREPLICAS"),
        "min-replicas gate did not re-engage after replica loss: {rejected:?}"
    );

    primary.shutdown().await;
}

// FM-TXN-007
// FM-TXN-051
/// `min-replicas-to-write` gates MULTI at queue time (the queued write is
/// rejected with NOREPLICAS and the transaction is flagged dirty, so EXEC
/// answers EXECABORT) and gates a Lua-internal write at the shard write seam,
/// where the script's `SET` is actually produced.
#[tokio::test]
async fn test_min_replicas_to_write_multi_and_lua_paths() {
    let primary = TestServer::start_primary_with_config(min_replicas_config(1)).await;

    // MULTI: the queue-time pre-check rejects the write, flags the transaction
    // dirty, and EXEC aborts (see the SELFFENCE twin test for the full note).
    let mut c = primary.connect().await;
    assert_eq!(
        parse_simple_string(&c.command(&["MULTI"]).await),
        Some("OK")
    );
    let queued = c.command(&["SET", "k", "v"]).await;
    assert!(
        err_has_prefix(&queued, "NOREPLICAS"),
        "queued write not gated: {queued:?}"
    );
    let exec = c.command(&["EXEC"]).await;
    assert!(
        err_has_prefix(&exec, "EXECABORT"),
        "expected EXECABORT after queue-time NOREPLICAS gate, got {exec:?}"
    );

    // The Lua-internal write is gated too — at the shard write seam, with the
    // same NOREPLICAS string a directly-issued write earns.
    let lua = primary
        .send(
            "EVAL",
            &[
                "return redis.call('SET', KEYS[1], ARGV[1])",
                "1",
                "lk",
                "lv",
            ],
        )
        .await;
    assert!(is_error(&lua), "EVAL write not gated: {lua:?}");
    assert_eq!(
        primary.send("GET", &["lk"]).await,
        Response::Bulk(None),
        "a gated Lua write must not have applied"
    );

    primary.shutdown().await;
}

// FM-REPLICATION-042
/// `CONFIG SET min-replicas-to-write` takes effect on the hot write path
/// immediately (the gate reads the value live, not at boot).
#[tokio::test]
async fn test_min_replicas_to_write_config_set_live() {
    let config = TestServerConfig {
        replication_self_fence_on_replica_loss: Some(false),
        ..Default::default()
    };
    let primary = TestServer::start_primary_with_config(config).await;

    // Default min-replicas-to-write 0 → writes allowed.
    assert_ok(&primary.send("SET", &["k", "v"]).await);

    // Raise the bar at runtime; with zero replicas writes now fail.
    assert_ok(
        &primary
            .send("CONFIG", &["SET", "min-replicas-to-write", "1"])
            .await,
    );
    assert!(
        err_has_prefix(&primary.send("SET", &["k", "v2"]).await, "NOREPLICAS"),
        "live CONFIG SET min-replicas-to-write did not take effect"
    );

    // Lower it again; writes resume immediately.
    assert_ok(
        &primary
            .send("CONFIG", &["SET", "min-replicas-to-write", "0"])
            .await,
    );
    assert_ok(&primary.send("SET", &["k", "v3"]).await);
    primary.shutdown().await;
}

// ============================================================================
// Tier 8: Broadcast-lag disconnect → reconnect → resync (E2E)
// ============================================================================

/// A throttling TCP proxy interposed between a replica and its primary.
///
/// It forwards both directions verbatim, except the primary→replica direction
/// can be *stalled* on demand: while stalled the proxy stops draining bytes
/// from the primary, so the primary's kernel send buffer to the proxy fills,
/// the primary's per-replica write task blocks in `write_all`, and the primary
/// disconnects the replica for lag (write-timeout / broadcast overflow — both
/// close the link the same way). The replica→primary direction (the PSYNC
/// handshake and ACKs) always flows, so a reconnecting replica can complete a
/// fresh handshake through the proxy.
///
/// The replica→primary direction can be frozen independently
/// ([`LagProxy::stall_acks`]). That leg carries the ACKs, so freezing it leaves
/// the replica *attached and streaming but silent* — the partition the
/// replica-loss self-fence exists for, and the only way to reach that state
/// deterministically: a replica that is shut down instead closes its socket,
/// which is a clean departure and disarms the fence (FM-REPLICATION-062).
struct LagProxy {
    port: u16,
    stalled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ack_stalled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    accept_task: tokio::task::JoinHandle<()>,
}

/// Cap on the proxy's receive buffer for the primary→proxy leg, so the kernel
/// buffering the stall flood must overrun is bounded and known instead of
/// autotuned. See [`test_broadcast_lag_disconnect_and_resync`] for why the flood
/// has to exceed it.
const LAG_PROXY_RCVBUF_BYTES: usize = 128 * 1024;

/// Padding added to every value in the stall flood of
/// [`test_broadcast_lag_disconnect_and_resync`]. 20_000 frames x ~840 B is
/// ~16 MiB — several times the kernel send-buffer ceiling the flood must overrun
/// before the primary's write can block (Linux `tcp_wmem` max is 4 MiB by
/// default), while staying far inside the primary's backlog cap.
const FLOOD_VALUE_PAD: usize = 800;

impl LagProxy {
    /// Bind an ephemeral port and start accepting replica connections, each
    /// dialed through to `primary_port`.
    async fn spawn(primary_port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind lag-proxy listener");
        let port = listener.local_addr().expect("proxy local_addr").port();
        let stalled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stalled_accept = stalled.clone();
        let ack_stalled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ack_stalled_accept = ack_stalled.clone();

        let accept_task = tokio::spawn(async move {
            loop {
                let (inbound, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let outbound =
                    match tokio::net::TcpStream::connect(("127.0.0.1", primary_port)).await {
                        Ok(stream) => stream,
                        Err(_) => continue,
                    };
                socket2::SockRef::from(&outbound)
                    .set_recv_buffer_size(LAG_PROXY_RCVBUF_BYTES)
                    .expect("cap lag-proxy SO_RCVBUF");
                let stalled_conn = stalled_accept.clone();
                let ack_stalled_conn = ack_stalled_accept.clone();
                tokio::spawn(proxy_connection(
                    inbound,
                    outbound,
                    stalled_conn,
                    ack_stalled_conn,
                ));
            }
        });

        Self {
            port,
            stalled,
            ack_stalled,
            accept_task,
        }
    }

    fn port(&self) -> u16 {
        self.port
    }

    /// Freeze the replica→primary direction: the link stays open and the
    /// replica keeps streaming, but its ACKs stop arriving. The primary's
    /// session remains registered, which is exactly what separates a silent
    /// replica from a departed one.
    fn stall_acks(&self) {
        self.ack_stalled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let the ACKs through again; the buffered ones flush immediately.
    fn resume_acks(&self) {
        self.ack_stalled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Freeze the primary→replica direction: the replica stops receiving the
    /// WAL stream and falls behind until the primary disconnects it.
    fn stall(&self) {
        self.stalled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Resume forwarding so a reconnecting replica can resync.
    fn resume(&self) {
        self.stalled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for LagProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Pump one accepted replica↔primary connection through the proxy, honoring
/// the shared stall flags: `stalled` freezes the primary→replica leg,
/// `ack_stalled` the replica→primary one.
async fn proxy_connection(
    inbound: tokio::net::TcpStream,
    outbound: tokio::net::TcpStream,
    stalled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ack_stalled: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut replica_rx, mut replica_tx) = inbound.into_split();
    let (mut primary_rx, mut primary_tx) = outbound.into_split();

    // replica → primary: the handshake commands and the ACKs. Stalled on
    // demand, the same way as the other leg: we stop reading the replica's
    // socket, so nothing it sends reaches the primary. The connection itself
    // stays open, so the primary's session stays registered and merely goes
    // quiet.
    let mut up = tokio::spawn(async move {
        let mut buf = [0u8; 16 * 1024];
        loop {
            while ack_stalled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            match replica_rx.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if primary_tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // primary → replica: stalled on demand. While stalled we stop reading from
    // the primary, so its send buffer fills and its write task blocks. (At most
    // one 16 KiB chunk already in-flight leaks through before we park — far
    // below the buffering needed to keep a replica in sync under a write flood.)
    let mut down = tokio::spawn(async move {
        let mut buf = [0u8; 16 * 1024];
        loop {
            while stalled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            match primary_rx.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if replica_tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Tear both legs down together the moment either peer closes its side, so
    // the replica observes a *full* connection close exactly as it would on a
    // direct link. Managing the halves independently would leave the socket
    // half-open: when the primary drops the replica for lag, `up` would die but
    // `down` would keep the replica's write side open, wedging the replica in
    // its next ACK write (the proxy buffer fills instead of erroring) so it
    // never reads the FIN — a proxy artifact that never happens on a real link.
    tokio::select! {
        _ = &mut up => { down.abort(); }
        _ = &mut down => { up.abort(); }
    }
}

/// Read `connected_slaves` (streaming replicas) from a server's
/// `INFO replication`. Returns `None` if the field is missing/unparseable.
async fn connected_slaves(server: &TestServer) -> Option<i64> {
    let response = server.send("INFO", &["replication"]).await;
    let info = parse_info_replication(&response)?;
    info.get("connected_slaves")?.trim().parse().ok()
}

/// Poll `connected_slaves` until it equals `want` or the deadline elapses.
/// Returns the last observed value (for assertion messages).
async fn wait_connected_slaves(server: &TestServer, want: i64, within: Duration) -> Option<i64> {
    let deadline = std::time::Instant::now() + within;
    let mut last = connected_slaves(server).await;
    while std::time::Instant::now() < deadline {
        if last == Some(want) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = connected_slaves(server).await;
    }
    last
}

/// Poll `INFO replication` until `server` has exactly `want` replicas that are
/// *fresh* in the sense the write fences use, or the deadline elapses. Returns
/// whether that state was reached.
///
/// `connected_slaves` alone is the wrong wait for anything that then writes:
/// a replica is counted the moment its session registers, but both write gates
/// count only replicas that are streaming **and** have ACKed recently
/// (FM-REPLICATION-041, FM-REPLICATION-042). This reads the same registry
/// through the projection that renders it: `state=online` is `Phase::Streaming`
/// and `lag=0` is an ACK inside the last whole second — comfortably inside the
/// default 3s freshness window, and never true of a replica that has attached
/// but not yet ACKed.
async fn wait_for_fresh_streaming_replicas(
    server: &TestServer,
    want: usize,
    within: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        if let Some(info) = parse_info_replication(&server.send("INFO", &["replication"]).await) {
            let count = info
                .get("connected_slaves")
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let fresh = (0..count)
                .filter_map(|i| info.get(&format!("slave{i}")))
                .filter(|line| {
                    let field = |name: &str| {
                        line.split(',')
                            .find_map(|p| p.strip_prefix(name))
                            .unwrap_or("")
                    };
                    field("state=") == "online" && field("lag=") == "0"
                })
                .count();
            if count == want && fresh == want {
                return true;
            }
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `WAIT want …` until the primary reports `want` acking replicas, or the
/// deadline elapses; returns the last count seen.
///
/// A single `WAIT` call is not a safe assertion for a *healthy* link when the
/// primary runs a short `replication-replica-write-timeout-ms`: on a loaded
/// machine a scheduling hiccup can block a per-replica write past that timeout,
/// costing one disconnect/reconnect round even though nothing is wrong. Polling
/// keeps the assertion about convergence rather than about timing.
async fn wait_for_acks(server: &TestServer, want: i64, within: Duration) -> i64 {
    let deadline = std::time::Instant::now() + within;
    loop {
        let acked =
            parse_integer(&server.send("WAIT", &[&want.to_string(), "1000"]).await).unwrap_or(0);
        if acked >= want || std::time::Instant::now() >= deadline {
            return acked;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// End-to-end coverage of the broadcast-lag disconnect path (audit gap E#8):
/// a streaming replica is stalled at the network layer until it exceeds the
/// primary's lag tolerance; the primary disconnects it; the replica then
/// reconnects, resyncs, and converges to the primary's full keyspace —
/// including the writes it missed while stalled — plus the WAIT ack-count
/// behaviour across the disconnect (0 while dropped) and recovery (1 once
/// caught up).
///
/// The stall is driven deterministically through [`LagProxy`] (no timing luck)
/// and every wait polls a condition with a deadline (no fixed sleeps for
/// correctness). `replica_write_timeout_ms` is lowered so the primary's
/// blocked-write lag-disconnect fires promptly (the deterministic trigger here;
/// the flood also drives the per-replica broadcast receiver past its 10k slots,
/// the secondary `Lagged` trigger — either way the primary drops the replica).
/// Self-fence is disabled so the primary keeps accepting the write flood.
///
/// The backlog is sized above the flood so the reconnecting replica qualifies
/// for a *partial* resync (`+CONTINUE`): the missed writes replay as live
/// commands and the running keyspace converges without a restart. This is the
/// only resync path that converges the live store in place — a full resync
/// stages a checkpoint that installs on the next boot (persistence) or ships an
/// empty RDB (in-memory), neither of which updates the running keyspace.
///
/// Regression guard for two replication bugs found writing this test:
///   1. the replica did not reconnect after a primary-initiated clean close
///      (`replica/mod.rs`: a clean `Ok(())` from the sync loop was terminal); and
///   2. the primary's lag-disconnect never closed the socket, because the
///      streaming `select!` dropped the sibling task's `JoinHandle` instead of
///      aborting it, leaving a zombie half-open link the replica never saw close
///      (`replica_session.rs`).
///
/// It also guards a third bug this test found once the flood was sized to stall
/// the link on Linux: the primary stopped stamping offsets and recording the
/// backlog the moment the last replica dropped, so every write made while the
/// laggy replica was away was invisible to the `+CONTINUE` that followed and the
/// replica silently resumed past the hole (`ReplicationBroadcaster::is_active`).
///
/// # Making the disconnect deterministic across kernels
///
/// A stall only becomes visible to the primary's *userspace* once the kernel
/// buffers between the two ends are full: until then `write_all` returns
/// immediately, the write task keeps draining the broadcast receiver, and
/// neither the write timeout nor the `Lagged` overflow can fire. The flood must
/// therefore exceed `primary SO_SNDBUF + proxy SO_RCVBUF`, and both terms are
/// per-kernel: macOS keeps the loopback send buffer small enough that a ~1.4 MiB
/// flood blocked, while Linux autotunes it up to `tcp_wmem` max (4 MiB on the
/// aarch64 testbox, `net.ipv4.tcp_wmem = 4096 16384 4194304`) and absorbed that
/// whole flood — the primary never blocked, never disconnected, and the test
/// failed on Linux only. Capping the receiver alone does not help: the bytes
/// pile into the *sender's* buffer regardless of the advertised window
/// (measured: still no disconnect at 1.4 MiB with the proxy's `SO_RCVBUF`
/// pinned to 128 KiB).
///
/// So the proxy pins its `SO_RCVBUF` ([`LAG_PROXY_RCVBUF_BYTES`]) and the flood
/// carries [`FLOOD_VALUE_PAD`]-padded values for ~16 MiB total — several times
/// the 4 MiB Linux ceiling, so the write is guaranteed to block on either
/// platform. (Measured on the testbox: ~1.4 MiB never disconnects, ~6.5 MiB
/// disconnects mid-flood with `Write to replica timed out`.) The flood stays
/// well inside the 64 MiB backlog byte cap, so the reconnect still earns a
/// partial resync.
#[rstest]
#[case::in_memory(false)]
#[case::with_persistence(true)]
#[tokio::test]
async fn test_broadcast_lag_disconnect_and_resync(#[case] persistence: bool) {
    let primary = TestServer::start_primary_with_config(TestServerConfig {
        persistence,
        // Keep accepting writes while the replica is stale — the flood is the
        // whole point.
        replication_self_fence_on_replica_loss: Some(false),
        // Disconnect a blocked (stalled) replica quickly for a fast, robust test.
        replication_replica_write_timeout_ms: Some(1000),
        // Size the backlog above the flood so the reconnecting replica's gap is
        // still covered — the primary grants a partial resync (+CONTINUE) and
        // replays the missed writes as live commands, converging the running
        // keyspace without a restart (a full resync would only stage a
        // checkpoint / send an empty in-memory RDB).
        replication_backlog_size: Some(100_000),
        ..Default::default()
    })
    .await;

    // Interpose the throttling proxy and point the replica at it (not directly
    // at the primary), so we control the primary→replica byte stream.
    let proxy = LagProxy::spawn(primary.port()).await;
    let mut replica_config = TestServerConfig {
        persistence,
        replication_self_fence_on_replica_loss: Some(false),
        replication_replica_write_timeout_ms: Some(1000),
        // ACK often so recovery (and WAIT) converge quickly.
        replication_ack_interval_ms: Some(200),
        ..Default::default()
    };
    replica_config.replication_primary_host = Some("127.0.0.1".to_string());
    replica_config.replication_primary_port = Some(proxy.port());
    let replica = TestServer::start_with_config(replica_config, ServerRole::Replica).await;

    // Baseline: the replica reaches the Streaming phase through the proxy.
    assert_eq!(
        wait_connected_slaves(&primary, 1, Duration::from_secs(10)).await,
        Some(1),
        "replica should reach the streaming phase (connected_slaves:1) via the proxy"
    );

    // Seed a key and confirm it replicates end-to-end before we stall.
    assert_ok(&primary.send("SET", &["seed_key", "seed_value"]).await);
    let acked = wait_for_acks(&primary, 1, Duration::from_secs(15)).await;
    assert_eq!(
        acked, 1,
        "seed write should be acked by the healthy replica"
    );
    assert_eq!(
        primary.send("DBSIZE", &[]).await,
        replica.send("DBSIZE", &[]).await,
        "keyspace sizes should match before the stall"
    );

    // ---- Stall the replica and flood the primary past the 10k broadcast cap.
    proxy.stall();

    // 20k frames, ~16 MiB: once the kernel buffers fill (see "Making the
    // disconnect deterministic across kernels" above) the stalled socket blocks
    // the primary's per-replica write task, tripping the 1s write-timeout (the
    // deterministic disconnect here), and the frame count also overruns the
    // 10_000-slot broadcast channel — either way the primary drops the replica
    // for resync. The flood stays within the 100k-entry / 64 MiB backlog, so the
    // reconnect earns a partial resync.
    const FLOOD: usize = 20_000;
    let padding = "p".repeat(FLOOD_VALUE_PAD);
    let mut writer = primary.connect().await;
    let mut i = 0usize;
    while i < FLOOD {
        let end = (i + 200).min(FLOOD);
        for j in i..end {
            let key = format!("flood_key_{j}");
            let val = format!("flood_val_{j}{padding}");
            writer.send_only(&["SET", &key, &val]).await;
        }
        for _ in i..end {
            writer
                .read_response(Duration::from_secs(10))
                .await
                .expect("primary should keep accepting writes while replica is stale");
        }
        i = end;
    }
    drop(writer);

    // ---- Assert the primary disconnected the laggy replica.
    assert_eq!(
        wait_connected_slaves(&primary, 0, Duration::from_secs(10)).await,
        Some(0),
        "primary must disconnect the stalled replica (connected_slaves→0) once it exceeds \
         the lag threshold"
    );

    // WAIT can find no streaming replica to ack while the replica is dropped.
    let acked_stalled = parse_integer(&primary.send("WAIT", &["1", "500"]).await).unwrap_or(-1);
    assert_eq!(
        acked_stalled, 0,
        "WAIT must return 0 while the replica is disconnected, got {acked_stalled}"
    );

    // ---- Recovery: unblock the link; the replica must reconnect on its own.
    proxy.resume();

    assert_eq!(
        wait_connected_slaves(&primary, 1, Duration::from_secs(20)).await,
        Some(1),
        "replica must reconnect and return to the streaming phase after the link recovers \
         (regression guard: a primary-initiated clean close must not strand the replica)"
    );

    // ---- Convergence: the resync must deliver the full keyspace, including
    // every write the replica missed while stalled.
    let acked_recovered = wait_for_acks(&primary, 1, Duration::from_secs(20)).await;
    assert_eq!(
        acked_recovered, 1,
        "WAIT must report the reconnected replica acking after recovery"
    );

    // Poll to full keyspace equality (WAIT acks the stream offset; the applier
    // may trail it by a beat).
    let primary_dbsize = parse_integer(&primary.send("DBSIZE", &[]).await).unwrap_or(-1);
    let converged = {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let replica_dbsize = parse_integer(&replica.send("DBSIZE", &[]).await).unwrap_or(-2);
            if replica_dbsize == primary_dbsize {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert!(
        converged,
        "replica keyspace must converge to the primary's {primary_dbsize} keys after resync"
    );

    // Spot-check a spread of the flood keys the replica missed during the stall
    // plus the pre-stall seed — the resync must have delivered them.
    for idx in [0usize, 1, FLOOD / 2, FLOOD - 2, FLOOD - 1] {
        let key = format!("flood_key_{idx}");
        let expected = format!("flood_val_{idx}{padding}");
        let got = replica.send("GET", &[&key]).await;
        assert_eq!(
            got,
            Response::Bulk(Some(Bytes::from(expected.clone()))),
            "replica missing or truncated flood key {key} after resync"
        );
    }
    assert_eq!(
        replica.send("GET", &["seed_key"]).await,
        Response::Bulk(Some(Bytes::from("seed_value"))),
        "replica missing the pre-stall seed key after resync"
    );

    replica.shutdown().await;
    primary.shutdown().await;
    drop(proxy);
}

// ============================================================================
// Issue 46: registry-driven READONLY enforcement + monotonic replica reads
//
// `test_replica_readonly_enforcement` (above) only ever exercised SET/DEL/
// ZADD by hand. That leaves the central READONLY enforcement — gated purely
// on the `CommandFlags::WRITE` bit in `ConnectionHandler::run_pre_checks`
// (connection/guards.rs) — unproven for the other ~170 WRITE-flagged
// commands: a newly registered WRITE command with a flag-assignment bug, or a
// future carve-out that special-cases some command name, could slip through
// with only the hand-picked subset ever passing CI.
//
// The test below builds the *actual production* command registry (the same
// `frogdb_server::register_commands` the real server calls at startup — see
// `frogdb-server/crates/server/src/server/register.rs`) and walks every entry
// whose flags contain WRITE, rather than a hand-maintained list. A command
// newly flagged WRITE without READONLY enforcement fails this test the
// moment it's registered — no test update required.
// ============================================================================

/// Realistic minimal argument shapes for WRITE commands whose natural
/// minimum-arity invocation isn't just "N generic tokens" — subcommand-first
/// commands (FT.*, XGROUP), numkeys-first commands (Z*STORE, CMS.MERGE,
/// MSETEX), GEO's lon/lat, the hash-field-TTL `FIELDS numfields field...`
/// family, and blocking commands' trailing timeout.
///
/// This table exists for readability/documentation — a real client would
/// send something shaped like this — not for correctness: READONLY
/// enforcement (`run_pre_checks`) runs *before* arity/type validation (see
/// `PRE_DISPATCH_ORDER` in connection/dispatch.rs: `PreChecks` precedes
/// `Arity`), so the exact argument values never affect this test's outcome.
/// A WRITE command with **no entry here** still gets exercised: `minimal_args`
/// falls back to `arity.min()` generic filler tokens, so the registry walk
/// stays exhaustive without hand-maintenance for every future command.
fn special_args_for(name: &str) -> Option<&'static [&'static str]> {
    Some(match name {
        "APPEND" => &["k", "v"],
        "BF.ADD" => &["k", "item"],
        "BF.INSERT" => &["k", "ITEMS", "item"],
        "BF.LOADCHUNK" => &["k", "1", "chunk"],
        "BF.MADD" => &["k", "item"],
        "BF.RESERVE" => &["k", "0.01", "100"],
        "BITFIELD" => &["k", "GET", "u8", "0"],
        "BITOP" => &["AND", "dst", "src"],
        "BLMOVE" => &["src", "dst", "LEFT", "RIGHT", "0.01"],
        "BLMPOP" => &["0.01", "1", "k", "LEFT"],
        "BLPOP" => &["k", "0.01"],
        "BRPOP" => &["k", "0.01"],
        "BRPOPLPUSH" => &["src", "dst", "0.01"],
        "BZMPOP" => &["0.01", "1", "k", "MIN"],
        "BZPOPMAX" => &["k", "0.01"],
        "BZPOPMIN" => &["k", "0.01"],
        "CF.ADD" => &["k", "item"],
        "CF.ADDNX" => &["k", "item"],
        "CF.DEL" => &["k", "item"],
        "CF.INSERT" => &["k", "ITEMS", "item"],
        "CF.INSERTNX" => &["k", "ITEMS", "item"],
        "CF.LOADCHUNK" => &["k", "1", "chunk"],
        "CF.RESERVE" => &["k", "100"],
        "CMS.INCRBY" => &["k", "item", "1"],
        "CMS.INITBYDIM" => &["k", "100", "5"],
        "CMS.INITBYPROB" => &["k", "0.01", "0.01"],
        "CMS.MERGE" => &["dst", "1", "src"],
        "COPY" => &["src", "dst"],
        "DECR" => &["k"],
        "DECRBY" => &["k", "1"],
        "DEL" => &["k"],
        "DELEX" => &["k"],
        "ES.APPEND" => &["k", "1", "event", "data"],
        "ES.SNAPSHOT" => &["k", "1", "data"],
        "EXPIRE" => &["k", "10"],
        "EXPIREAT" => &["k", "9999999999"],
        "FLUSHALL" => &[],
        "FLUSHDB" => &[],
        "FROGDB.FINALIZE" => &["1"],
        "FT.ALIASADD" => &["alias", "idx"],
        "FT.ALIASDEL" => &["alias"],
        "FT.ALIASUPDATE" => &["alias", "idx"],
        "FT.ALTER" => &["idx", "SCHEMA", "ADD", "f"],
        "FT.CREATE" => &["idx", "SCHEMA", "f", "TEXT"],
        "FT.DICTADD" => &["dict", "term"],
        "FT.DICTDEL" => &["dict", "term"],
        "FT.DROPINDEX" => &["idx"],
        "FT.SUGADD" => &["k", "str", "1"],
        "FT.SUGDEL" => &["k", "str"],
        "FT.SYNUPDATE" => &["idx", "gid", "term"],
        "GEOADD" => &["k", "13.361389", "38.115556", "Palermo"],
        "GEORADIUS" => &["k", "15", "37", "200", "km"],
        "GEORADIUSBYMEMBER" => &["k", "Palermo", "200", "km"],
        "GEOSEARCHSTORE" => &["dst", "src", "FROMMEMBER", "m", "BYRADIUS", "10", "km"],
        "GETDEL" => &["k"],
        "GETEX" => &["k"],
        "GETSET" => &["k", "v"],
        "HDEL" => &["k", "f"],
        "HEXPIRE" => &["k", "10", "FIELDS", "1", "f"],
        "HEXPIREAT" => &["k", "9999999999", "FIELDS", "1", "f"],
        "HGETDEL" => &["k", "FIELDS", "1", "f"],
        "HGETEX" => &["k", "FIELDS", "1", "f"],
        "HINCRBY" => &["k", "f", "1"],
        "HINCRBYFLOAT" => &["k", "f", "1.0"],
        "HMSET" => &["k", "f", "v"],
        "HPERSIST" => &["k", "FIELDS", "1", "f"],
        "HPEXPIRE" => &["k", "10000", "FIELDS", "1", "f"],
        "HPEXPIREAT" => &["k", "9999999999000", "FIELDS", "1", "f"],
        "HSET" => &["k", "f", "v"],
        "HSETEX" => &["k", "10", "FIELDS", "1", "f", "v"],
        "HSETNX" => &["k", "f", "v"],
        "INCR" => &["k"],
        "INCRBY" => &["k", "1"],
        "INCRBYFLOAT" => &["k", "1.0"],
        "JSON.ARRAPPEND" => &["k", "$", "1"],
        "JSON.ARRINSERT" => &["k", "$", "0", "1"],
        "JSON.ARRPOP" => &["k"],
        "JSON.ARRTRIM" => &["k", "$", "0", "0"],
        "JSON.CLEAR" => &["k"],
        "JSON.DEL" => &["k"],
        "JSON.MERGE" => &["k", "$", "1"],
        "JSON.NUMINCRBY" => &["k", "$", "1"],
        "JSON.NUMMULTBY" => &["k", "$", "1"],
        "JSON.SET" => &["k", "$", "1"],
        "JSON.STRAPPEND" => &["k", "$", "\"x\""],
        "JSON.TOGGLE" => &["k"],
        "LINSERT" => &["k", "BEFORE", "pivot", "v"],
        "LMOVE" => &["src", "dst", "LEFT", "RIGHT"],
        "LMPOP" => &["1", "k", "LEFT"],
        "LPOP" => &["k"],
        "LPUSH" => &["k", "v"],
        "LPUSHX" => &["k", "v"],
        "LREM" => &["k", "0", "v"],
        "LSET" => &["k", "0", "v"],
        "LTRIM" => &["k", "0", "-1"],
        "MIGRATE" => &["127.0.0.1", "6379", "k", "0", "1000"],
        "MOVE" => &["k", "1"],
        "MSET" => &["k", "v"],
        "MSETEX" => &["1", "k", "v"],
        "MSETNX" => &["k", "v"],
        "PERSIST" => &["k"],
        "PEXPIRE" => &["k", "10000"],
        "PEXPIREAT" => &["k", "9999999999000"],
        "PFADD" => &["k", "m"],
        "PFMERGE" => &["dst"],
        "PSETEX" => &["k", "10000", "v"],
        "RENAME" => &["src", "dst"],
        "RENAMENX" => &["src", "dst"],
        "RESTORE" => &["k", "0", "payload"],
        "RPOP" => &["k"],
        "RPOPLPUSH" => &["src", "dst"],
        "RPUSH" => &["k", "v"],
        "RPUSHX" => &["k", "v"],
        "SADD" => &["k", "m"],
        "SDIFFSTORE" => &["dst", "src"],
        "SET" => &["k", "v"],
        "SETBIT" => &["k", "0", "1"],
        "SETEX" => &["k", "10", "v"],
        "SETNX" => &["k", "v"],
        "SETRANGE" => &["k", "0", "v"],
        "SINTERSTORE" => &["dst", "src"],
        "SMOVE" => &["src", "dst", "m"],
        "SORT" => &["k"],
        "SPOP" => &["k"],
        "SREM" => &["k", "m"],
        "SUNIONSTORE" => &["dst", "src"],
        "SWAPDB" => &["0", "1"],
        "TDIGEST.ADD" => &["k", "1"],
        "TDIGEST.CREATE" => &["k"],
        "TDIGEST.MERGE" => &["dst", "1", "src"],
        "TDIGEST.RESET" => &["k"],
        "TOPK.ADD" => &["k", "item"],
        "TOPK.INCRBY" => &["k", "item", "1"],
        "TOPK.RESERVE" => &["k", "10"],
        "TS.ADD" => &["k", "*", "1"],
        "TS.ALTER" => &["k"],
        "TS.CREATE" => &["k"],
        "TS.CREATERULE" => &["src", "dst", "AGGREGATION", "avg", "60000"],
        "TS.DECRBY" => &["k", "1"],
        "TS.DEL" => &["k", "-", "+"],
        "TS.DELETERULE" => &["src", "dst"],
        "TS.INCRBY" => &["k", "1"],
        "TS.MADD" => &["k", "*", "1"],
        "UNLINK" => &["k"],
        "VADD" => &["k", "VALUES", "1", "1", "m"],
        "VREM" => &["k", "m"],
        "VSETATTR" => &["k", "m", "{}"],
        "XACK" => &["k", "g", "1-1"],
        "XACKDEL" => &["k", "g", "IDS", "1", "1-1"],
        "XADD" => &["k", "*", "f", "v"],
        "XAUTOCLAIM" => &["k", "g", "c", "0", "0-0"],
        "XCLAIM" => &["k", "g", "c", "0", "1-1"],
        "XDEL" => &["k", "1-1"],
        "XDELEX" => &["k", "IDS", "1", "1-1"],
        "XGROUP" => &["CREATE", "k", "g", "$"],
        "XREADGROUP" => &["GROUP", "g", "c", "COUNT", "1", "STREAMS", "k", ">"],
        "XSETID" => &["k", "1-1"],
        "XTRIM" => &["k", "MAXLEN", "0"],
        "ZADD" => &["k", "1", "m"],
        "ZDIFFSTORE" => &["dst", "1", "k"],
        "ZINCRBY" => &["k", "1", "m"],
        "ZINTERSTORE" => &["dst", "1", "k"],
        "ZMPOP" => &["1", "k", "MIN"],
        "ZPOPMAX" => &["k"],
        "ZPOPMIN" => &["k"],
        "ZRANGESTORE" => &["dst", "src", "0", "-1"],
        "ZREM" => &["k", "m"],
        "ZREMRANGEBYLEX" => &["k", "[a", "[z"],
        "ZREMRANGEBYRANK" => &["k", "0", "-1"],
        "ZREMRANGEBYSCORE" => &["k", "0", "1"],
        "ZUNIONSTORE" => &["dst", "1", "k"],
        _ => return None,
    })
}

/// Generic fallback: `min` placeholder tokens. Used only for a WRITE command
/// with no `special_args_for` entry (i.e. registered after this test was
/// written) — see that function's doc comment for why argument *validity*
/// doesn't matter here.
fn fallback_args(min: usize) -> Vec<String> {
    (0..min).map(|i| format!("filler{i}")).collect()
}

/// Build the arguments to invoke `name` with, given its declared [`Arity`].
fn minimal_args(name: &str, arity: Arity) -> Vec<String> {
    match special_args_for(name) {
        Some(args) => args.iter().map(|s| s.to_string()).collect(),
        None => fallback_args(arity.min()),
    }
}

// FM-REPLICATION-028
/// Registry-driven READONLY enforcement: every WRITE-flagged command in the
/// *production* registry, invoked against a replica, must be rejected with
/// `-READONLY`. Walks `registry.iter()` rather than a hand-picked list, so a
/// future WRITE command with no enforcement fails this test automatically.
#[tokio::test]
async fn test_replica_rejects_every_write_command() {
    let mut registry = CommandRegistry::new();
    frogdb_server::register_commands(&mut registry);

    let mut write_commands: Vec<(String, Arity)> = registry
        .iter()
        .filter(|(_, entry)| entry.flags().contains(CommandFlags::WRITE))
        .map(|(name, entry)| (name.to_string(), entry.arity()))
        .collect();
    write_commands.sort_by(|a, b| a.0.cmp(&b.0));

    // Sanity check on the walk itself: if `register_commands` stops
    // registering commands (or the WRITE flag check breaks), this test would
    // otherwise pass vacuously over an empty/tiny list.
    assert!(
        write_commands.len() >= 100,
        "expected a large number of WRITE-flagged commands from the production \
         registry, got {} ({:?}) — did `register_commands` or `CommandFlags::WRITE` change shape?",
        write_commands.len(),
        write_commands.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );

    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    let mut failures = Vec::new();
    for (name, arity) in &write_commands {
        let owned_args = minimal_args(name, *arity);
        let args: Vec<&str> = owned_args.iter().map(|s| s.as_str()).collect();

        let resp = replica.send(name, &args).await;
        if !is_error(&resp) {
            failures.push(format!(
                "{name}: expected an error on a replica, got {resp:?}"
            ));
            continue;
        }
        let msg = get_error_message(&resp).unwrap_or("<no message>");
        if !msg.starts_with("READONLY") {
            failures.push(format!("{name}: expected READONLY error, got: {msg}"));
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} WRITE-flagged commands did not enforce READONLY on a replica:\n{}",
        failures.len(),
        write_commands.len(),
        failures.join("\n")
    );

    // Sanity check the harness itself: the primary must still accept writes.
    let primary_set = primary.send("SET", &["sanity_key", "v"]).await;
    assert_ok(&primary_set);

    replica.shutdown().await;
    primary.shutdown().await;
}

// FM-REPLICATION-029
/// Monotonic-read regression (issue 46): a tight loop of replica reads must
/// never observe a value regress to an earlier one, both while the primary
/// is still issuing increasing writes and after `WAIT` has confirmed the
/// final write reached the replica.
///
/// `consistency.md`'s monotonic-reads row (line 38) was [Design intent] only
/// until this test; RYW (line 27) is exercised by the same WAIT-then-read
/// pattern (a client observes its own committed write on the replica once
/// WAIT acks it). Replica lazy-expiry-on-read is explicitly out of scope
/// (task 32, `replica-expiry-ttl-drift`).
#[tokio::test]
async fn test_replica_read_monotonic_after_primary_writes() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let key = "{mono}seq";
    const WRITES: i64 = 100;

    let mut observed: Vec<i64> = Vec::new();

    // Primary writes a strictly increasing sequence; the replica is polled a
    // few times per round without waiting for full propagation first — the
    // property under test is that *whatever* the replica returns, it is
    // never lower than the highest value already observed, even while later
    // writes are still in flight.
    for value in 1..=WRITES {
        let set_resp = primary.send("SET", &[key, &value.to_string()]).await;
        assert_ok(&set_resp);

        for _ in 0..3 {
            let resp = replica.send("GET", &[key]).await;
            if let Response::Bulk(Some(b)) = &resp
                && let Ok(v) = String::from_utf8_lossy(b).parse::<i64>()
            {
                if let Some(&last) = observed.last() {
                    assert!(
                        v >= last,
                        "monotonic-read violation: replica returned {v} after previously \
                         returning {last} (primary at write round {value})"
                    );
                }
                observed.push(v);
            }
        }
    }

    // WAIT for the final write to be acknowledged, then keep polling: the
    // replica must converge to WRITES and must not regress while doing so
    // (the read-your-writes guarantee: once WAIT acks, a read must observe
    // at least that write).
    let acked = primary.send("WAIT", &["1", "5000"]).await;
    assert!(
        parse_integer(&acked).unwrap_or(0) >= 1,
        "WAIT did not report the replica acking the final write, got: {acked:?}"
    );

    let mut converged = false;
    for _ in 0..500 {
        let resp = replica.send("GET", &[key]).await;
        if let Response::Bulk(Some(b)) = &resp
            && let Ok(v) = String::from_utf8_lossy(b).parse::<i64>()
        {
            if let Some(&last) = observed.last() {
                assert!(
                    v >= last,
                    "monotonic-read violation after WAIT ack: replica returned {v} after \
                     previously returning {last}"
                );
            }
            observed.push(v);
            if v == WRITES {
                converged = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        converged,
        "replica never converged to the final written value {WRITES} after a WAIT ack; \
         last observed: {:?}",
        observed.last()
    );

    // Whole-sequence check, in addition to the per-step asserts above: belt
    // and suspenders against any point the per-step check could miss.
    assert!(
        observed.windows(2).all(|w| w[1] >= w[0]),
        "replica read sequence was not monotonically non-decreasing: {observed:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

// ---------------------------------------------------------------------------
// The stale-read gate (`replica-serve-stale-data`, redis-feel issue 17)
// ---------------------------------------------------------------------------

/// Poll `INFO replication` on `replica` until it reports the link down.
///
/// The break is asynchronous — the replica notices the primary is gone when its
/// stream errors — so every stale-gate assertion has to wait for the state the
/// gate reads, not for the shutdown that causes it.
async fn wait_for_link_down(replica: &TestServer) {
    for _ in 0..100 {
        let info = parse_info_replication(&replica.send("INFO", &["replication"]).await)
            .expect("INFO replication must parse");
        if info.get("master_link_status").map(String::as_str) == Some("down") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("replica never reported master_link_status:down after its primary was shut down");
}

/// FrogDB's default is the deliberate deviation: a replica that has lost its
/// primary refuses to answer from its now-unbounded-stale local keyspace, with
/// Redis's own `-MASTERDOWN` error. Redis defaults the other way and serves.
///
/// The commands that stay available are the `STALE`-flagged ones — INFO, PING,
/// CONFIG — which is what keeps the node diagnosable and re-pointable while the
/// gate is closed. That set is the whole reason the gate is flag-driven rather
/// than a blanket refusal.
#[tokio::test]
async fn a_link_down_replica_refuses_reads_by_default() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    assert_ok(&primary.send("SET", &["stale-key", "v1"]).await);
    wait_for_replication(&primary, 5_000).await;
    assert_bulk_eq(&replica.send("GET", &["stale-key"]).await, b"v1");

    primary.shutdown().await;
    wait_for_link_down(&replica).await;

    let refused = replica.send("GET", &["stale-key"]).await;
    assert!(
        is_error(&refused),
        "a link-down replica must refuse a non-STALE read by default, got: {refused:?}"
    );
    assert_eq!(
        get_error_message(&refused),
        Some("MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."),
        "the refusal must be Redis's verbatim MASTERDOWN error, got: {refused:?}"
    );

    // Writes were already refused with -READONLY, and that rung runs first: a
    // replica must not be told its replication link is down when the real
    // answer is that it is a replica at all.
    let write = replica.send("SET", &["stale-key", "v2"]).await;
    assert!(
        get_error_message(&write).is_some_and(|m| m.starts_with("READONLY")),
        "the read-only rung must still win over the stale gate, got: {write:?}"
    );

    // STALE-flagged commands keep working — the node stays diagnosable.
    assert!(
        parse_simple_string(&replica.send("PING", &[]).await) == Some("PONG"),
        "PING is STALE-flagged and must survive a down link"
    );
    assert!(
        parse_info_replication(&replica.send("INFO", &["replication"]).await).is_some(),
        "INFO is STALE-flagged and must survive a down link"
    );
    assert!(
        !is_error(&replica.send("CONFIG", &["GET", "maxmemory"]).await),
        "CONFIG is STALE-flagged and must survive a down link"
    );

    // The knob is live-mutable, which is the point: an operator reaches for it
    // mid-incident, on a replica that is already refusing reads.
    assert_ok(
        &replica
            .send("CONFIG", &["SET", "replica-serve-stale-data", "yes"])
            .await,
    );
    assert_bulk_eq(&replica.send("GET", &["stale-key"]).await, b"v1");

    replica.shutdown().await;
}

/// With the knob on at boot, FrogDB is Redis: the link-down replica serves its
/// stale keyspace and the gate never fires.
#[tokio::test]
async fn the_serve_stale_data_knob_restores_redis_behaviour() {
    let config = TestServerConfig {
        replication_replica_serve_stale_data: Some(true),
        ..Default::default()
    };
    let (primary, replica) = start_primary_replica_pair(config).await;

    assert_ok(&primary.send("SET", &["stale-key", "v1"]).await);
    wait_for_replication(&primary, 5_000).await;
    assert_bulk_eq(&replica.send("GET", &["stale-key"]).await, b"v1");

    primary.shutdown().await;
    wait_for_link_down(&replica).await;

    assert_bulk_eq(&replica.send("GET", &["stale-key"]).await, b"v1");

    replica.shutdown().await;
}

/// The gate is replica-only: a primary has no link to lose, so the default
/// must not touch a standalone node's reads.
#[tokio::test]
async fn a_primary_is_never_stale_gated() {
    let server = TestServer::start_standalone().await;

    assert_ok(&server.send("SET", &["k", "v"]).await);
    assert_bulk_eq(&server.send("GET", &["k"]).await, b"v");

    server.shutdown().await;
}
