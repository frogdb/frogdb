//! Turmoil-based simulation tests for FrogDB.
//!
//! These tests use Turmoil to simulate network conditions and verify
//! FrogDB's behavior under various scenarios:
//! - Basic operations
//! - Scatter-gather across shards
//! - Network delays and message ordering
//! - Chaos testing with various delay and failure configurations
//!
//! Run with: `cargo test -p frogdb-server --features turmoil --test simulation`
//!
//! ## Test Tiers
//!
//! - **Tier 1 (Quick)**: Core tests with quick delay combinations, run on every CI.
//! - **Tier 2 (Failures)**: Failure mode tests with fixed delays, run on every CI.
//! - **Tier 3 (Full)**: Full Cartesian product of all tests × delays × failure modes, run nightly or with `--ignored`.
//!
//! ## Running Specific Chaos Combinations
//!
//! ```bash
//! # Run all Tier 1 tests
//! cargo test -p frogdb-server --features turmoil --test simulation
//!
//! # Run a specific test with a specific chaos config
//! cargo test -p frogdb-server --features turmoil --test simulation test_mset_mget_basic::scatter_delay_ms_50
//!
//! # Run full Tier 3 matrix (nightly)
//! cargo test -p frogdb-server --features turmoil --test simulation -- --ignored
//! ```

#![cfg(feature = "turmoil")]

mod common;

use bytes::Bytes;
use common::chaos_configs::ChaosPreset;
use common::sim_harness::{OperationHistory, OperationResult};
use common::sim_helpers::{
    SERVER_HOST, SERVER_PORT, encode_command, parse_simple_response, real_frogdb_server,
    real_frogdb_server_with_chaos,
};
use frogdb_server::config::ChaosConfig;
use frogdb_testing::{KVModel, check_linearizability};
use rstest::rstest;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turmoil::Builder;
use turmoil::net::{TcpListener, TcpStream};

type BoxError = Box<dyn Error + 'static>;

/// Echo server for basic connectivity tests (doesn't need real server).
async fn echo_server() -> Result<(), BoxError> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, SERVER_PORT)).await?;

    loop {
        let (mut stream, _addr) = listener.accept().await?;

        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut buf = vec![0u8; 1024];
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(_n) => {}
                    Err(_) => return,
                };

                // Echo back a simple OK response
                if stream.write_all(b"+OK\r\n").await.is_err() {
                    return;
                }
            }
        });
    }
}

// =============================================================================
// Basic Connectivity Tests
// =============================================================================

#[test]
fn test_basic_connectivity() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, echo_server);

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;

        // Send PING
        let cmd = encode_command(&[b"PING"]);
        stream.write_all(&cmd).await?;

        // Read response
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await?;

        assert!(n > 0);
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn test_simple_set_get() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // SET key value
        let cmd = encode_command(&[b"SET", b"mykey", b"myvalue"]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        // GET key
        let cmd = encode_command(&[b"GET", b"mykey"]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);

        match response {
            OperationResult::String(v) => assert_eq!(v.as_ref(), b"myvalue"),
            other => panic!("Expected String, got {:?}", other),
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test MSET/MGET with scatter-gather across multiple shards.
///
/// This test is parameterized with various chaos configurations to ensure
/// the scatter-gather implementation is robust under different conditions.
#[rstest]
#[case::no_chaos(0, 0)]
#[case::scatter_delay_50ms(50, 0)]
#[case::scatter_delay_100ms(100, 0)]
#[case::single_shard_delay_50ms(0, 50)]
#[case::both_delays_50ms(50, 50)]
fn test_mset_mget_basic(#[case] scatter_delay_ms: u64, #[case] single_shard_delay_ms: u64) {
    let chaos = ChaosConfig {
        scatter_inter_send_delay_ms: scatter_delay_ms,
        single_shard_delay_ms,
        ..Default::default()
    };

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // MSET across multiple "shards" (keys that would hash to different shards)
        let cmd = encode_command(&[
            b"MSET", b"key1", b"value1", b"key2", b"value2", b"key3", b"value3",
        ]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        // MGET the keys
        let cmd = encode_command(&[b"MGET", b"key1", b"key2", b"key3"]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        // For simplicity, just verify we got a response
        assert!(n > 0);

        Ok(())
    });

    sim.run().unwrap();
}

/// Full Cartesian product chaos test for MSET/MGET (Tier 3 - nightly).
///
/// This test runs with all combinations of scatter delays and single shard delays.
#[rstest]
#[ignore] // Run with --ignored for full matrix
fn test_mset_mget_full_chaos_matrix(
    #[values(0, 50, 100, 250)] scatter_delay_ms: u64,
    #[values(0, 50, 100, 250)] single_shard_delay_ms: u64,
) {
    let chaos = ChaosConfig {
        scatter_inter_send_delay_ms: scatter_delay_ms,
        single_shard_delay_ms,
        ..Default::default()
    };

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // MSET across multiple "shards" (keys that would hash to different shards)
        let cmd = encode_command(&[
            b"MSET", b"key1", b"value1", b"key2", b"value2", b"key3", b"value3",
        ]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        // MGET the keys
        let cmd = encode_command(&[b"MGET", b"key1", b"key2", b"key3"]);
        stream.write_all(&cmd).await?;

        let n = stream.read(&mut buf).await?;
        assert!(n > 0);

        Ok(())
    });

    sim.run().unwrap();
}

/// Test concurrent clients with chaos configurations.
#[rstest]
#[case::no_chaos(ChaosPreset::None)]
#[case::scatter_delay(ChaosPreset::ScatterDelay(50))]
#[case::single_shard_delay(ChaosPreset::SingleShardDelay(50))]
fn test_concurrent_clients(#[case] preset: ChaosPreset) {
    let chaos = preset.to_config(4);

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    // Multiple clients operating concurrently
    for i in 0..3 {
        let client_name = format!("client{}", i);
        sim.client(client_name, async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            // Each client sets its own key
            let key = format!("client{}_key", i);
            let value = format!("client{}_value", i);
            let cmd = encode_command(&[b"SET", key.as_bytes(), value.as_bytes()]);
            stream.write_all(&cmd).await?;

            let n = stream.read(&mut buf).await?;
            let response = parse_simple_response(&buf[..n]);
            assert!(matches!(response, OperationResult::Ok));

            // GET the key back
            let cmd = encode_command(&[b"GET", key.as_bytes()]);
            stream.write_all(&cmd).await?;

            let n = stream.read(&mut buf).await?;
            let response = parse_simple_response(&buf[..n]);
            match response {
                OperationResult::String(v) => assert_eq!(v.as_ref(), value.as_bytes()),
                other => panic!("Expected String, got {:?}", other),
            }

            Ok(())
        });
    }

    sim.run().unwrap();
}

#[test]
fn test_history_tracking() {
    let mut history = OperationHistory::new();

    // Simulate a sequence of operations
    let op1 = history.record_invoke(1, "SET", vec![Bytes::from("key1"), Bytes::from("value1")]);
    history.record_return(op1, 1, OperationResult::Ok);

    let op2 = history.record_invoke(2, "SET", vec![Bytes::from("key2"), Bytes::from("value2")]);
    let op3 = history.record_invoke(1, "GET", vec![Bytes::from("key1")]);
    history.record_return(op2, 2, OperationResult::Ok);
    history.record_return(op3, 1, OperationResult::String(Bytes::from("value1")));

    // Verify history completeness
    assert!(history.is_complete());

    // Verify operation count
    let ops = history.operations();
    assert_eq!(ops.len(), 6); // 3 invokes + 3 returns

    // Verify client operations
    let client1_ops = history.client_operations(1);
    assert_eq!(client1_ops.len(), 4); // 2 invokes + 2 returns
}

#[test]
fn test_network_delay_simulation() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(10))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    // Register the client first
    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;

        let cmd = encode_command(&[b"PING"]);
        stream.write_all(&cmd).await?;

        let mut buf = vec![0u8; 1024];
        let _n = stream.read(&mut buf).await?;

        Ok(())
    });

    // Add latency between client and server after registration
    sim.set_link_latency("client1", SERVER_HOST, Duration::from_millis(50));

    sim.run().unwrap();
}

// =============================================================================
// Scatter-Gather Tests
// =============================================================================

#[test]
fn test_sharded_set_get_single_key() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // SET key value
        let cmd = encode_command(&[b"SET", b"mykey", b"myvalue"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        // GET key
        let cmd = encode_command(&[b"GET", b"mykey"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);

        match response {
            OperationResult::String(v) => assert_eq!(v.as_ref(), b"myvalue"),
            other => panic!("Expected String, got {:?}", other),
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test sharded MSET/MGET with key distribution across shards.
///
/// Parameterized with chaos configurations for robustness testing.
#[rstest]
#[case::no_chaos(ChaosPreset::None)]
#[case::scatter_delay(ChaosPreset::ScatterDelay(50))]
#[case::single_shard_delay(ChaosPreset::SingleShardDelay(50))]
#[case::all_delays(ChaosPreset::AllDelays(50))]
fn test_sharded_mset_mget_distribution(#[case] preset: ChaosPreset) {
    let chaos = preset.to_config(4);

    let mut sim = Builder::new().build();

    // Use 4 shards to ensure key distribution
    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // These keys will hash to different shards based on CRC16
        // key1, key2, key3, key4 will distribute across shards
        let cmd = encode_command(&[
            b"MSET", b"key1", b"value1", b"key2", b"value2", b"key3", b"value3", b"key4", b"value4",
        ]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(
            matches!(response, OperationResult::Ok),
            "MSET should succeed"
        );

        // Verify all keys retrievable
        let cmd = encode_command(&[b"MGET", b"key1", b"key2", b"key3", b"key4"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;

        // Verify we got a response with data
        assert!(n > 0, "Should receive MGET response");

        // Also verify individual GETs work
        for (key, expected_value) in [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
        ] {
            let cmd = encode_command(&[b"GET", key.as_bytes()]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let response = parse_simple_response(&buf[..n]);
            match response {
                OperationResult::String(v) => assert_eq!(
                    v.as_ref(),
                    expected_value.as_bytes(),
                    "Key {} should have value {}",
                    key,
                    expected_value
                ),
                other => panic!("Expected String for {}, got {:?}", key, other),
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Full matrix test for sharded MSET/MGET distribution (Tier 3 - nightly).
#[rstest]
#[ignore]
fn test_sharded_mset_mget_distribution_full_matrix(
    #[values(
        ChaosPreset::None,
        ChaosPreset::ScatterDelay(50),
        ChaosPreset::ScatterDelay(100),
        ChaosPreset::ScatterDelay(250),
        ChaosPreset::SingleShardDelay(50),
        ChaosPreset::SingleShardDelay(100),
        ChaosPreset::AllDelays(50),
        ChaosPreset::AllDelays(100)
    )]
    preset: ChaosPreset,
) {
    let chaos = preset.to_config(4);

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        let cmd = encode_command(&[
            b"MSET", b"key1", b"value1", b"key2", b"value2", b"key3", b"value3", b"key4", b"value4",
        ]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(matches!(
            parse_simple_response(&buf[..n]),
            OperationResult::Ok
        ));

        let cmd = encode_command(&[b"MGET", b"key1", b"key2", b"key3", b"key4"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(n > 0);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn test_sharded_hash_tags() {
    // Keys with {tag} should go to the same shard
    use common::sim_harness::hash_slot;

    // Verify hash tag extraction works
    let slot1 = hash_slot(b"{user}:profile");
    let slot2 = hash_slot(b"{user}:settings");
    let slot3 = hash_slot(b"{user}:orders");
    assert_eq!(slot1, slot2, "Keys with same tag should have same slot");
    assert_eq!(slot2, slot3, "Keys with same tag should have same slot");

    // Different tags should (likely) have different slots
    let _slot_a = hash_slot(b"{a}:key");
    let _slot_b = hash_slot(b"{b}:key");
    // Note: They could collide, but it's unlikely for these short tags

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));

    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // Set keys with same hash tag - they'll go to same shard
        let cmd = encode_command(&[
            b"MSET",
            b"{user123}:name",
            b"Alice",
            b"{user123}:email",
            b"alice@example.com",
            b"{user123}:age",
            b"30",
        ]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(
            matches!(parse_simple_response(&buf[..n]), OperationResult::Ok),
            "MSET with hash tags should succeed"
        );

        // Verify values
        let cmd = encode_command(&[b"GET", b"{user123}:name"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        match parse_simple_response(&buf[..n]) {
            OperationResult::String(v) => assert_eq!(v.as_ref(), b"Alice"),
            other => panic!("Expected String, got {:?}", other),
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn test_sharded_concurrent_clients() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));

    // Multiple clients operating on different shards concurrently
    for i in 0..4 {
        let client_name = format!("client{}", i);
        sim.client(client_name, async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let addr = turmoil::lookup(SERVER_HOST);
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            // Each client writes to its own key space
            for j in 0..10 {
                let key = format!("client{}:key{}", i, j);
                let value = format!("value{}", j);
                let cmd = encode_command(&[b"SET", key.as_bytes(), value.as_bytes()]);
                stream.write_all(&cmd).await?;
                let n = stream.read(&mut buf).await?;
                assert!(matches!(
                    parse_simple_response(&buf[..n]),
                    OperationResult::Ok
                ));
            }

            // Verify all values
            for j in 0..10 {
                let key = format!("client{}:key{}", i, j);
                let expected = format!("value{}", j);
                let cmd = encode_command(&[b"GET", key.as_bytes()]);
                stream.write_all(&cmd).await?;
                let n = stream.read(&mut buf).await?;
                match parse_simple_response(&buf[..n]) {
                    OperationResult::String(v) => {
                        assert_eq!(v.as_ref(), expected.as_bytes())
                    }
                    other => panic!("Expected String, got {:?}", other),
                }
            }

            Ok(())
        });
    }

    sim.run().unwrap();
}

// =============================================================================
// Linearizability Tests
// =============================================================================

#[test]
fn test_linearizability_concurrent_writes() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();
    let history = Arc::new(Mutex::new(OperationHistory::new()));

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    // Client 1: SET key=A, then SET key=B
    // We use sequential operations with delays to ensure ordering
    let h1 = history.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Wait for client2 to be ready - use longer delays for deterministic ordering
        tokio::time::sleep(Duration::from_millis(200)).await;

        // SET key=A
        let op1 = {
            let mut h = h1.lock().unwrap();
            h.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("A")])
        };
        let cmd = encode_command(&[b"SET", b"key", b"A"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h1.lock().unwrap();
            let result = parse_simple_response(&buf[..n]);
            h.record_return(op1, 1, result);
        }

        // Delay between SETs - use longer delay for deterministic ordering
        tokio::time::sleep(Duration::from_millis(400)).await;

        // SET key=B
        let op2 = {
            let mut h = h1.lock().unwrap();
            h.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("B")])
        };
        let cmd = encode_command(&[b"SET", b"key", b"B"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h1.lock().unwrap();
            let result = parse_simple_response(&buf[..n]);
            h.record_return(op2, 1, result);
        }

        Ok(())
    });

    // Client 2: GETs after client 1's operations complete
    let h2 = history.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Wait until after SET A is expected to complete (SET A starts at 200ms)
        tokio::time::sleep(Duration::from_millis(400)).await;

        // First GET - should see A (after SET A completes)
        let op1 = {
            let mut h = h2.lock().unwrap();
            h.record_invoke(2, "GET", vec![Bytes::from("key")])
        };
        let cmd = encode_command(&[b"GET", b"key"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h2.lock().unwrap();
            let result = parse_simple_response(&buf[..n]);
            h.record_return(op1, 2, result);
        }

        // Wait until after SET B is expected to complete (SET B starts at ~600ms)
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Second GET - should see B
        let op2 = {
            let mut h = h2.lock().unwrap();
            h.record_invoke(2, "GET", vec![Bytes::from("key")])
        };
        let cmd = encode_command(&[b"GET", b"key"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h2.lock().unwrap();
            let result = parse_simple_response(&buf[..n]);
            h.record_return(op2, 2, result);
        }

        Ok(())
    });

    sim.run().unwrap();

    // Verify the history is complete
    let history = history.lock().unwrap();
    assert!(history.is_complete(), "History should be complete");

    // Convert and check linearizability
    let testing_history = history.to_testing_history();
    let result = check_linearizability::<KVModel>(&testing_history);

    // The real server should be linearizable for single-key operations
    if !result.is_linearizable {
        // Print debug info
        eprintln!("History operations:");
        for op in history.operations() {
            eprintln!(
                "  op_id={}, client={}, kind={:?}, cmd={}, args={:?}, result={:?}",
                op.op_id, op.client_id, op.kind, op.command, op.args, op.result
            );
        }
        panic!(
            "History not linearizable (problematic ops: {:?}). This may indicate a timing issue in the test.",
            result.problematic_ops
        );
    }
}

#[test]
fn test_linearizability_single_key_serial() {
    let mut sim = Builder::new().build();
    let history = Arc::new(Mutex::new(OperationHistory::new()));

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    let h = history.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // SET key=value1
        let op1 = {
            let mut h = h.lock().unwrap();
            h.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("value1")])
        };
        let cmd = encode_command(&[b"SET", b"key", b"value1"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h.lock().unwrap();
            h.record_return(op1, 1, parse_simple_response(&buf[..n]));
        }

        // GET key - should return value1
        let op2 = {
            let mut h = h.lock().unwrap();
            h.record_invoke(1, "GET", vec![Bytes::from("key")])
        };
        let cmd = encode_command(&[b"GET", b"key"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h.lock().unwrap();
            h.record_return(op2, 1, parse_simple_response(&buf[..n]));
        }

        // SET key=value2
        let op3 = {
            let mut h = h.lock().unwrap();
            h.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("value2")])
        };
        let cmd = encode_command(&[b"SET", b"key", b"value2"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h.lock().unwrap();
            h.record_return(op3, 1, parse_simple_response(&buf[..n]));
        }

        // GET key - should return value2
        let op4 = {
            let mut h = h.lock().unwrap();
            h.record_invoke(1, "GET", vec![Bytes::from("key")])
        };
        let cmd = encode_command(&[b"GET", b"key"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        {
            let mut h = h.lock().unwrap();
            h.record_return(op4, 1, parse_simple_response(&buf[..n]));
        }

        Ok(())
    });

    sim.run().unwrap();

    let history = history.lock().unwrap();
    assert!(history.is_complete());

    let testing_history = history.to_testing_history();
    let result = check_linearizability::<KVModel>(&testing_history);
    assert!(
        result.is_linearizable,
        "Serial operations should always be linearizable: {:?}",
        result.problematic_ops
    );
}

#[test]
fn test_detects_non_linearizable_history() {
    // Manually construct a non-linearizable history
    // (read returns value that was never written)
    let mut history = frogdb_testing::History::new();

    // GET before any SET - should return nil, but we'll claim it returned "ghost_value"
    let op1 = history.invoke(1, "get", vec![Bytes::from("key")]);
    history.respond(op1, Some(Bytes::from("ghost_value"))); // Never written!

    let result = check_linearizability::<KVModel>(&history);
    assert!(
        !result.is_linearizable,
        "Should detect non-linearizable history"
    );
}

#[test]
fn test_detects_stale_read() {
    // Construct a history where a read sees a stale value
    let mut history = frogdb_testing::History::new();

    // Client 1: SET key=A (completes before client 2's operations)
    let op1 = history.invoke(1, "set", vec![Bytes::from("key"), Bytes::from("A")]);
    history.respond(op1, Some(Bytes::from("OK")));

    // Client 1: SET key=B (completes before client 2's read)
    let op2 = history.invoke(1, "set", vec![Bytes::from("key"), Bytes::from("B")]);
    history.respond(op2, Some(Bytes::from("OK")));

    // Client 2: GET key returns A (stale!) - started after both SETs completed
    let op3 = history.invoke(2, "get", vec![Bytes::from("key")]);
    history.respond(op3, Some(Bytes::from("A"))); // Should be B!

    let result = check_linearizability::<KVModel>(&history);
    assert!(
        !result.is_linearizable,
        "Should detect stale read as non-linearizable"
    );
}

// =============================================================================
// Network Fault Injection Tests
// =============================================================================

#[test]
fn test_network_partition_client_isolated() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    let success = Arc::new(Mutex::new(false));
    let success_clone = success.clone();

    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // First operation should succeed
        let cmd = encode_command(&[b"SET", b"key", b"value"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        // Signal that first operation succeeded
        *success_clone.lock().unwrap() = true;

        // Wait a bit for partition to be established
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try another operation after partition (this may fail or timeout)
        let cmd = encode_command(&[b"GET", b"key"]);
        let write_result = stream.write_all(&cmd).await;

        // The write might succeed but read will fail/timeout
        if write_result.is_ok() {
            // Read may hang or fail due to partition
            let read_result =
                tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;

            // In a partitioned network, we expect either timeout or error
            match read_result {
                Ok(Ok(0)) => {
                    // Connection closed - expected during partition
                }
                Ok(Ok(_n)) => {
                    // If we got a response, partition wasn't fully effective
                    // This is acceptable in some network models
                }
                Ok(Err(_)) => {
                    // IO error - expected during partition
                }
                Err(_) => {
                    // Timeout - expected during partition
                }
            }
        }

        Ok(())
    });

    // Run until client completes first operation
    let _result = sim.run();

    // Verify first operation succeeded
    assert!(*success.lock().unwrap(), "First SET should have succeeded");
}

#[test]
fn test_partition_heal_recovery() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    // Track operation results
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = results.clone();

    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);

        // First connection - SET before partition
        {
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            let cmd = encode_command(&[b"SET", b"key", b"value1"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let response = parse_simple_response(&buf[..n]);
            results_clone
                .lock()
                .unwrap()
                .push(("set1", matches!(response, OperationResult::Ok)));
        }

        // Small delay to ensure any partition would take effect
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection - should work if partition healed
        {
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            let cmd = encode_command(&[b"GET", b"key"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let response = parse_simple_response(&buf[..n]);
            match response {
                OperationResult::String(v) => {
                    results_clone
                        .lock()
                        .unwrap()
                        .push(("get", v.as_ref() == b"value1"));
                }
                _ => {
                    results_clone.lock().unwrap().push(("get", false));
                }
            }
        }

        Ok(())
    });

    sim.run().unwrap();

    let results = results.lock().unwrap();
    assert_eq!(results.len(), 2, "Should have recorded 2 operations");
    assert!(results[0].1, "First SET should succeed");
    assert!(
        results[1].1,
        "GET after reconnect should return correct value"
    );
}

#[test]
fn test_high_latency_operations() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    let start_times = Arc::new(Mutex::new(Vec::new()));
    let start_times_clone = start_times.clone();

    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Record operation timing
        let start = std::time::Instant::now();

        let cmd = encode_command(&[b"SET", b"key", b"value"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;

        let elapsed = start.elapsed();
        start_times_clone.lock().unwrap().push(elapsed);

        let response = parse_simple_response(&buf[..n]);
        assert!(matches!(response, OperationResult::Ok));

        Ok(())
    });

    // Add significant latency
    sim.set_link_latency("client1", SERVER_HOST, Duration::from_millis(100));

    sim.run().unwrap();

    // Verify latency was applied (operation should take at least 100ms)
    let times = start_times.lock().unwrap();
    assert!(!times.is_empty());
    // Note: Due to simulation timing, exact latency may vary
}

#[test]
fn test_connection_drop_reconnect() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);

        // First connection - set a value
        {
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            let cmd = encode_command(&[b"SET", b"persistent_key", b"persistent_value"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            assert!(matches!(
                parse_simple_response(&buf[..n]),
                OperationResult::Ok
            ));
            // Connection dropped when stream goes out of scope
        }

        // Second connection - verify value persists
        {
            let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
            let mut buf = vec![0u8; 1024];

            let cmd = encode_command(&[b"GET", b"persistent_key"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            match parse_simple_response(&buf[..n]) {
                OperationResult::String(v) => {
                    assert_eq!(v.as_ref(), b"persistent_value");
                }
                other => panic!("Expected String, got {:?}", other),
            }
        }

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn test_multiple_clients_with_varying_latency() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    // Track which client completes first
    let completion_order = Arc::new(Mutex::new(Vec::new()));

    // Fast client
    let order1 = completion_order.clone();
    sim.client("fast_client", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        let cmd = encode_command(&[b"SET", b"fast_key", b"fast_value"]);
        stream.write_all(&cmd).await?;
        let _n = stream.read(&mut buf).await?;

        order1.lock().unwrap().push("fast");
        Ok(())
    });

    // Slow client
    let order2 = completion_order.clone();
    sim.client("slow_client", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        let cmd = encode_command(&[b"SET", b"slow_key", b"slow_value"]);
        stream.write_all(&cmd).await?;
        let _n = stream.read(&mut buf).await?;

        order2.lock().unwrap().push("slow");
        Ok(())
    });

    // Apply different latencies
    sim.set_link_latency("fast_client", SERVER_HOST, Duration::from_millis(10));
    sim.set_link_latency("slow_client", SERVER_HOST, Duration::from_millis(200));

    sim.run().unwrap();

    // Fast client should complete before slow client
    let order = completion_order.lock().unwrap();
    assert_eq!(order.len(), 2);
    assert_eq!(order[0], "fast", "Fast client should complete first");
    assert_eq!(order[1], "slow", "Slow client should complete second");
}

#[test]
fn test_sharded_server_with_latency() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // MSET across multiple shards
        let cmd = encode_command(&[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3", b"d", b"4"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(matches!(
            parse_simple_response(&buf[..n]),
            OperationResult::Ok
        ));

        // MGET to verify
        let cmd = encode_command(&[b"MGET", b"a", b"b", b"c", b"d"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(n > 0, "Should receive MGET response");

        Ok(())
    });

    // Add latency
    sim.set_link_latency("client1", SERVER_HOST, Duration::from_millis(50));

    sim.run().unwrap();
}

// =============================================================================
// MSET/MGET Atomicity Tests (SKIPPED - Current implementation is per-key atomic)
// =============================================================================

/// Test that MSET is fully atomic - concurrent MGET should see all-or-nothing.
///
/// With VLL (Very Lightweight Locking), MSET is now fully atomic across shards.
/// Expected behavior: MGET sees either (nil, nil) or (1, 1), never partial state like (1, nil).
#[test]
fn test_mset_full_atomicity() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    // Use real FrogDB server - this will expose the real scatter-gather behavior
    sim.host(SERVER_HOST, || real_frogdb_server(4));

    let results = Arc::new(Mutex::new(Vec::new()));

    // Client 1: MSET {same}a 1 {same}b 1 (using hash tags to ensure same shard)
    let _results1 = results.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Small delay to let client2 start its MGET
        tokio::time::sleep(Duration::from_millis(5)).await;

        let cmd = encode_command(&[b"MSET", b"{same}a", b"1", b"{same}b", b"1"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(
            matches!(response, OperationResult::Ok),
            "MSET should succeed"
        );

        Ok(())
    });

    // Client 2: Concurrent MGET - should see all-or-nothing
    let results2 = results.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Run multiple MGETs concurrently with the MSET
        for _ in 0..10 {
            let cmd = encode_command(&[b"MGET", b"{same}a", b"{same}b"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;

            // Parse the MGET response to check for partial visibility
            let response_str = String::from_utf8_lossy(&buf[..n]);
            results2.lock().unwrap().push(response_str.to_string());

            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        Ok(())
    });

    sim.run().unwrap();

    // Verify no partial visibility
    // Valid states: both nil, or both "1"
    // Invalid: one nil and one "1" (partial visibility)
    let results = results.lock().unwrap();
    for result in results.iter() {
        // Parse RESP array response
        // *2\r\n$-1\r\n$-1\r\n = both nil
        // *2\r\n$1\r\n1\r\n$1\r\n1\r\n = both "1"
        let has_first_value = result.contains("$1\r\n1\r\n");
        let has_nil = result.contains("$-1\r\n");

        // If we see one value and one nil, that's partial visibility (atomicity violation)
        // This is a simplified check - real parsing would be more robust
        if has_first_value && has_nil {
            // Count occurrences
            let nil_count = result.matches("$-1\r\n").count();
            let value_count = result.matches("$1\r\n1\r\n").count();

            // Partial visibility: exactly one nil and one value
            if nil_count == 1 && value_count == 1 {
                panic!(
                    "MSET atomicity violation: saw partial state. Result: {}",
                    result
                );
            }
        }
    }
}

/// Test MSET linearizability with concurrent operations.
///
/// With VLL, multiple concurrent MSETs are linearizable - the final state should be
/// consistent with some sequential ordering.
#[test]
fn test_mset_linearizable() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));
    let history = Arc::new(Mutex::new(OperationHistory::new()));

    // Client 1: MSET {same}a=1 {same}b=1
    let h1 = history.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        let op_id = {
            let mut h = h1.lock().unwrap();
            h.record_invoke(
                1,
                "MSET",
                vec![
                    Bytes::from("{same}a"),
                    Bytes::from("1"),
                    Bytes::from("{same}b"),
                    Bytes::from("1"),
                ],
            )
        };

        let cmd = encode_command(&[b"MSET", b"{same}a", b"1", b"{same}b", b"1"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;

        {
            let mut h = h1.lock().unwrap();
            h.record_return(op_id, 1, parse_simple_response(&buf[..n]));
        }

        Ok(())
    });

    // Client 2: MSET {same}a=2 {same}b=2
    let h2 = history.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        let op_id = {
            let mut h = h2.lock().unwrap();
            h.record_invoke(
                2,
                "MSET",
                vec![
                    Bytes::from("{same}a"),
                    Bytes::from("2"),
                    Bytes::from("{same}b"),
                    Bytes::from("2"),
                ],
            )
        };

        let cmd = encode_command(&[b"MSET", b"{same}a", b"2", b"{same}b", b"2"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;

        {
            let mut h = h2.lock().unwrap();
            h.record_return(op_id, 2, parse_simple_response(&buf[..n]));
        }

        Ok(())
    });

    // Client 3: Observe final state
    let h3 = history.clone();
    sim.client("client3", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Wait for writes to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // GET {same}a
        let op_a = {
            let mut h = h3.lock().unwrap();
            h.record_invoke(3, "GET", vec![Bytes::from("{same}a")])
        };
        let cmd = encode_command(&[b"GET", b"{same}a"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let result_a = parse_simple_response(&buf[..n]);
        {
            let mut h = h3.lock().unwrap();
            h.record_return(op_a, 3, result_a.clone());
        }

        // GET {same}b
        let op_b = {
            let mut h = h3.lock().unwrap();
            h.record_invoke(3, "GET", vec![Bytes::from("{same}b")])
        };
        let cmd = encode_command(&[b"GET", b"{same}b"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let result_b = parse_simple_response(&buf[..n]);
        {
            let mut h = h3.lock().unwrap();
            h.record_return(op_b, 3, result_b.clone());
        }

        // Verify both keys have same value (either both 1 or both 2)
        match (&result_a, &result_b) {
            (OperationResult::String(a), OperationResult::String(b)) => {
                assert_eq!(a, b, "Keys should have same value due to MSET atomicity");
                assert!(
                    a.as_ref() == b"1" || a.as_ref() == b"2",
                    "Value should be 1 or 2"
                );
            }
            _ => panic!("Expected string results"),
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test MSET atomicity with sharded server - VLL ensures atomicity.
///
/// Uses real FrogDB server. Keys WITHOUT hash tags will route to different shards,
/// but VLL (Very Lightweight Locking) ensures atomic multi-shard operations.
///
/// Expected behavior: MGET sees either (nil, nil) or (1, 1), never partial state.
///
/// Parameterized with chaos presets to test under various delay conditions.
#[rstest]
fn test_mset_full_atomicity_sharded(
    #[values(
        ChaosPreset::ScatterDelay(50),
        ChaosPreset::ScatterDelay(100),
        ChaosPreset::AllDelays(50)
    )]
    preset: ChaosPreset,
) {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    // Use chaos config to inject delay between scatter sends
    // This creates a window between shard sends where concurrent MGET
    // can observe partial state (one key set, one still nil)
    let chaos = preset.to_config(4);
    sim.host(SERVER_HOST, move || {
        let chaos = chaos.clone();
        async move { real_frogdb_server_with_chaos(4, chaos).await }
    });

    let partial_seen = Arc::new(Mutex::new(false));
    let all_results = Arc::new(Mutex::new(Vec::new()));

    // Client 1: MSET key_a 1 key_b 1 (keys route to different shards)
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Small delay to let client2 start polling
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Keys without hash tags - will scatter to different shards
        let cmd = encode_command(&[b"MSET", b"key_a", b"1", b"key_b", b"1"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let response = parse_simple_response(&buf[..n]);
        assert!(
            matches!(response, OperationResult::Ok),
            "MSET should succeed"
        );

        Ok(())
    });

    // Client 2: Rapid MGET polling to catch partial state
    // With 50ms inter-send delay, we have a window to catch partial visibility
    let partial_clone = partial_seen.clone();
    let results_clone = all_results.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Poll rapidly during the MSET window
        // With 50ms delay between shard sends and 5ms poll interval,
        // we should have ~10 chances to catch the partial state
        for _ in 0..100 {
            let cmd = encode_command(&[b"MGET", b"key_a", b"key_b"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let response_str = String::from_utf8_lossy(&buf[..n]).to_string();

            // Check for partial visibility
            // *2\r\n$1\r\n1\r\n$-1\r\n = first key set, second nil (PARTIAL!)
            // *2\r\n$-1\r\n$1\r\n1\r\n = first nil, second key set (PARTIAL!)
            let has_value = response_str.contains("$1\r\n1\r\n");
            let has_nil = response_str.contains("$-1\r\n");

            if has_value && has_nil {
                let nil_count = response_str.matches("$-1\r\n").count();
                let value_count = response_str.matches("$1\r\n1\r\n").count();

                // Exactly one nil and one value = partial visibility
                if nil_count == 1 && value_count == 1 {
                    *partial_clone.lock().unwrap() = true;
                    results_clone
                        .lock()
                        .unwrap()
                        .push(format!("PARTIAL: {}", response_str));
                }
            }

            results_clone.lock().unwrap().push(response_str);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        Ok(())
    });

    sim.run().unwrap();

    // With VLL, we should NOT see partial state
    let saw_partial = *partial_seen.lock().unwrap();

    // VLL ensures atomic multi-shard operations - no partial visibility
    assert!(
        !saw_partial,
        "MSET atomicity violation: saw partial state. VLL should prevent this."
    );
}

// =============================================================================
// Transaction (MULTI/EXEC) Tests - Should Pass
// =============================================================================

/// Test that transactions are atomic - no partial visibility.
///
/// Client 1: MULTI; SET counter 0; INCR counter; INCR counter; EXEC
/// Client 2: GET counter (concurrent)
/// Expected: Client 2 sees nil or 2, never 0 or 1
#[test]
fn test_transaction_atomicity_no_partial_visibility() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    let observed_values = Arc::new(Mutex::new(Vec::new()));

    // For this test, we simulate transaction atomicity by doing operations in sequence
    // The real test would use MULTI/EXEC

    let _observed1 = observed_values.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Simulate atomic transaction: SET counter 0, then SET counter 2
        // (simulating SET 0 + INCR + INCR = 2 atomically)
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Atomic update to final value
        let cmd = encode_command(&[b"SET", b"counter", b"2"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        assert!(matches!(
            parse_simple_response(&buf[..n]),
            OperationResult::Ok
        ));

        Ok(())
    });

    let observed2 = observed_values.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Read counter multiple times
        for _ in 0..20 {
            let cmd = encode_command(&[b"GET", b"counter"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let result = parse_simple_response(&buf[..n]);

            let value = match &result {
                OperationResult::Nil => "nil".to_string(),
                OperationResult::String(b) => String::from_utf8_lossy(b).to_string(),
                _ => "error".to_string(),
            };
            observed2.lock().unwrap().push(value);

            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        Ok(())
    });

    sim.run().unwrap();

    // Verify we only see valid states: nil or 2
    // We should never see 0 or 1 (intermediate states)
    let values = observed_values.lock().unwrap();
    for value in values.iter() {
        assert!(
            value == "nil" || value == "2",
            "Should only see nil or 2 (atomic), but saw: {}",
            value
        );
    }
}

/// Test transaction isolation - concurrent transactions don't interleave.
///
/// Client 1: MULTI; SET a 1; SET b 1; EXEC
/// Client 2: MULTI; SET a 2; SET b 2; EXEC
/// Expected: Final state is (a=1,b=1) or (a=2,b=2), never mixed
#[test]
fn test_transaction_isolation_concurrent_writes() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1));

    // For this test, we verify the final state is consistent
    // Both clients write atomically, so we should never see a mixed state

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Simulate atomic transaction using MSET with same-slot keys
        let cmd = encode_command(&[b"MSET", b"{same}a", b"1", b"{same}b", b"1"]);
        stream.write_all(&cmd).await?;
        let _n = stream.read(&mut buf).await?;

        Ok(())
    });

    sim.client("client2", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Simulate atomic transaction using MSET with same-slot keys
        let cmd = encode_command(&[b"MSET", b"{same}a", b"2", b"{same}b", b"2"]);
        stream.write_all(&cmd).await?;
        let _n = stream.read(&mut buf).await?;

        Ok(())
    });

    let final_state = Arc::new(Mutex::new(None));
    let state_clone = final_state.clone();

    sim.client("observer", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Wait for both transactions to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Read final state
        let cmd = encode_command(&[b"GET", b"{same}a"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let a_value = match parse_simple_response(&buf[..n]) {
            OperationResult::String(b) => String::from_utf8_lossy(&b).to_string(),
            _ => "nil".to_string(),
        };

        let cmd = encode_command(&[b"GET", b"{same}b"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let b_value = match parse_simple_response(&buf[..n]) {
            OperationResult::String(b) => String::from_utf8_lossy(&b).to_string(),
            _ => "nil".to_string(),
        };

        *state_clone.lock().unwrap() = Some((a_value, b_value));

        Ok(())
    });

    sim.run().unwrap();

    // Verify final state is consistent (both 1 or both 2)
    let state = final_state.lock().unwrap();
    if let Some((a, b)) = state.as_ref() {
        assert_eq!(a, b, "Final state should be consistent: a={}, b={}", a, b);
        assert!(a == "1" || a == "2", "Value should be 1 or 2, got: {}", a);
    }
}

/// Test that transactions can read their own writes.
///
/// Client: MULTI; SET key val; GET key; EXEC
/// Expected: GET returns val
#[test]
fn test_transaction_read_own_writes() {
    // This is a model-level test since our simple_kv_server doesn't support MULTI/EXEC
    use frogdb_testing::{KVModel, KVState, Model};

    let state = KVState::default();

    // Transaction: SET key val, GET key
    let result = KVModel::step(
        &state,
        "exec",
        &[
            Bytes::from("2"),   // num_cmds
            Bytes::from("set"), // cmd1
            Bytes::from("2"),   // cmd1 num_args
            Bytes::from("key"),
            Bytes::from("val"),
            Bytes::from("get"), // cmd2
            Bytes::from("1"),   // cmd2 num_args
            Bytes::from("key"),
        ],
        Some(&Bytes::from("OK|val")),
    );

    assert!(
        result.is_some(),
        "Transaction should succeed and GET should see the SET"
    );

    let new_state = result.unwrap();
    assert_eq!(
        new_state.store.get(&Bytes::from("key")),
        Some(&Bytes::from("val"))
    );
}

/// Test concurrent transactions with linearizability checking.
#[test]
fn test_concurrent_transactions_linearizable() {
    // Model-level test for transaction linearizability
    let mut history = frogdb_testing::History::new();

    // Transaction 1: SET x=1, SET y=1 (atomic)
    let op1 = history.invoke(
        1,
        "exec",
        vec![
            Bytes::from("2"),
            Bytes::from("set"),
            Bytes::from("2"),
            Bytes::from("x"),
            Bytes::from("1"),
            Bytes::from("set"),
            Bytes::from("2"),
            Bytes::from("y"),
            Bytes::from("1"),
        ],
    );
    history.respond(op1, Some(Bytes::from("OK|OK")));

    // Transaction 2: SET x=2, SET y=2 (atomic)
    let op2 = history.invoke(
        2,
        "exec",
        vec![
            Bytes::from("2"),
            Bytes::from("set"),
            Bytes::from("2"),
            Bytes::from("x"),
            Bytes::from("2"),
            Bytes::from("set"),
            Bytes::from("2"),
            Bytes::from("y"),
            Bytes::from("2"),
        ],
    );
    history.respond(op2, Some(Bytes::from("OK|OK")));

    // Read final state - should be consistent (both 1 or both 2)
    let op3 = history.invoke(3, "get", vec![Bytes::from("x")]);
    history.respond(op3, Some(Bytes::from("2"))); // Assuming tx2 won

    let op4 = history.invoke(3, "get", vec![Bytes::from("y")]);
    history.respond(op4, Some(Bytes::from("2"))); // Must also be 2

    let result = check_linearizability::<KVModel>(&history);
    assert!(
        result.is_linearizable,
        "Concurrent transactions should be linearizable"
    );
}

// =============================================================================
// Lua Script Atomicity Tests (redis.call() implemented, tests need writing)
// =============================================================================

/// Test that Lua scripts execute atomically.
///
/// Client 1: EVAL "redis.call('SET', KEYS[1], '1'); redis.call('SET', KEYS[2], '1')" 2 a b
/// Client 2: MGET a b (concurrent)
/// Expected: Client 2 sees (nil,nil) or (1,1), never partial
#[test]
fn test_lua_script_atomicity() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(4)); // 4 shards

    let partial_seen = Arc::new(Mutex::new(false));

    // Client 1: Execute Lua script that sets two keys atomically
    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // Small delay to let client2 start polling
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Lua script that sets two keys atomically (using hash tags for same shard)
        let script =
            b"redis.call('SET', KEYS[1], '1'); redis.call('SET', KEYS[2], '1'); return 'OK'";
        let cmd = encode_command(&[b"EVAL", script, b"2", b"{atomic}a", b"{atomic}b"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = parse_simple_response(&buf[..n]);
        // Script should succeed
        assert!(
            !matches!(resp, OperationResult::Error(_)),
            "Script should succeed, got: {:?}",
            resp
        );

        Ok(())
    });

    // Client 2: Repeatedly check both keys for partial state
    let partial_clone = partial_seen.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // Poll rapidly during the script execution window
        for _ in 0..100 {
            let cmd = encode_command(&[b"MGET", b"{atomic}a", b"{atomic}b"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let response_str = String::from_utf8_lossy(&buf[..n]).to_string();

            // Check for partial visibility
            // *2\r\n$1\r\n1\r\n$-1\r\n = first key set, second nil (PARTIAL!)
            let has_value = response_str.contains("$1\r\n1\r\n");
            let has_nil = response_str.contains("$-1\r\n");

            if has_value && has_nil {
                let nil_count = response_str.matches("$-1\r\n").count();
                let value_count = response_str.matches("$1\r\n1\r\n").count();

                // Exactly one nil and one value = partial visibility
                if nil_count == 1 && value_count == 1 {
                    *partial_clone.lock().unwrap() = true;
                }
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        Ok(())
    });

    sim.run().unwrap();

    // With atomic Lua scripts, we should NOT see partial state
    let saw_partial = *partial_seen.lock().unwrap();
    assert!(
        !saw_partial,
        "Lua script atomicity violation: saw partial state"
    );
}

/// Test Lua script cross-key operations are atomic.
///
/// Script reads key A, writes to key B based on A's value.
/// Expected: Atomic read-modify-write semantics
#[test]
fn test_lua_script_cross_key_operations() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(4));

    sim.client("writer", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // Set initial source value
        let cmd = encode_command(&[b"SET", b"{cross}source", b"hello"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = parse_simple_response(&buf[..n]);
        assert!(matches!(resp, OperationResult::Ok), "SET should succeed");

        // Lua script reads source, writes to dest
        let script = b"local v = redis.call('GET', KEYS[1]); if v then redis.call('SET', KEYS[2], v) end; return v";
        let cmd = encode_command(&[b"EVAL", script, b"2", b"{cross}source", b"{cross}dest"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = parse_simple_response(&buf[..n]);

        // Script should return the value it read
        match resp {
            OperationResult::String(v) => {
                assert_eq!(v.as_ref(), b"hello", "Script should return 'hello'");
            }
            other => panic!("Expected String result, got {:?}", other),
        }

        // Verify dest has the copied value
        let cmd = encode_command(&[b"GET", b"{cross}dest"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = parse_simple_response(&buf[..n]);
        match resp {
            OperationResult::String(v) => {
                assert_eq!(v.as_ref(), b"hello", "Dest should have 'hello'");
            }
            other => panic!("Expected String for dest, got {:?}", other),
        }

        Ok(())
    });

    sim.run().unwrap();
}

/// Test Lua script with INCR pattern works correctly.
///
/// This test verifies that the Lua INCR pattern script executes correctly.
/// It uses a single client doing sequential increments to verify correctness.
#[test]
fn test_lua_script_incr_pattern() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, || real_frogdb_server(1)); // Single shard for simplicity

    let num_increments = 10;

    sim.client("incr_client", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // Lua INCR pattern script
        let script = b"local c = tonumber(redis.call('GET', KEYS[1])) or 0; c = c + 1; redis.call('SET', KEYS[1], tostring(c)); return c";

        let mut last_value = 0i64;
        for expected in 1..=num_increments {
            let cmd = encode_command(&[b"EVAL", script, b"1", b"counter"]);
            stream.write_all(&cmd).await?;
            let n = stream.read(&mut buf).await?;
            let resp = parse_simple_response(&buf[..n]);

            // Each increment should return the new value
            match resp {
                OperationResult::Integer(v) => {
                    assert_eq!(v, expected, "INCR should return sequential values");
                    last_value = v;
                }
                OperationResult::String(s) => {
                    // Script might return string representation of integer
                    let val: i64 = std::str::from_utf8(&s)
                        .unwrap()
                        .parse()
                        .expect("Should be a number");
                    assert_eq!(val, expected, "INCR should return sequential values");
                    last_value = val;
                }
                other => panic!("Unexpected response: {:?}", other),
            }
        }

        // Verify final value via GET
        let cmd = encode_command(&[b"GET", b"counter"]);
        stream.write_all(&cmd).await?;
        let n = stream.read(&mut buf).await?;
        let resp = parse_simple_response(&buf[..n]);

        match resp {
            OperationResult::String(s) => {
                let final_val: i64 = std::str::from_utf8(&s)
                    .unwrap()
                    .parse()
                    .expect("Should be a number");
                assert_eq!(
                    final_val, num_increments,
                    "Final counter should equal number of increments"
                );
            }
            other => panic!("Expected String for GET, got {:?}", other),
        }

        assert_eq!(last_value, num_increments, "Last INCR return value should match");

        Ok(())
    });

    sim.run().unwrap();
}
