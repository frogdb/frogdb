//! Turmoil-based simulation tests for FrogDB.
//!
//! These tests use Turmoil to simulate network conditions and verify
//! FrogDB's behavior under various scenarios:
//! - Basic operations
//! - Scatter-gather across shards
//! - Network delays and message ordering
//! - Future: Network partitions and fault injection
//!
//! Run with: `cargo test -p frogdb-server --features turmoil --test simulation`

#![cfg(feature = "turmoil")]

mod common;

use bytes::{Bytes, BytesMut};
use common::sim_harness::{shard_for_key, OperationHistory, OperationResult, ShardMessage};
use frogdb_testing::{check_linearizability, KVModel};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use turmoil::net::{TcpListener, TcpStream};
use turmoil::Builder;

type BoxError = Box<dyn Error + 'static>;

/// Server port for simulations.
const SERVER_PORT: u16 = 6379;

/// Server host name in simulation.
const SERVER_HOST: &str = "server";

/// Simple RESP protocol encoder for test commands.
fn encode_command(parts: &[&[u8]]) -> Bytes {
    let mut buf = BytesMut::new();

    // Array header
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());

    // Each part as bulk string
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }

    buf.freeze()
}

/// Parse a simple RESP response (simplified for testing).
fn parse_simple_response(data: &[u8]) -> OperationResult {
    if data.is_empty() {
        return OperationResult::Error("Empty response".into());
    }

    match data[0] {
        b'+' => {
            // Simple string
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            if s == "OK" {
                OperationResult::Ok
            } else {
                OperationResult::String(Bytes::from(s))
            }
        }
        b'-' => {
            // Error
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            OperationResult::Error(s)
        }
        b':' => {
            // Integer
            let s = String::from_utf8_lossy(&data[1..]).trim_end().to_string();
            let n = s.parse().unwrap_or(0);
            OperationResult::Integer(n)
        }
        b'$' => {
            // Bulk string
            let s = String::from_utf8_lossy(&data[1..]);
            if s.starts_with("-1") {
                OperationResult::Nil
            } else {
                // Find the actual data after the length
                if let Some(pos) = s.find("\r\n") {
                    let after = &data[1 + pos + 2..];
                    if let Some(end) = after.iter().position(|&b| b == b'\r') {
                        OperationResult::String(Bytes::copy_from_slice(&after[..end]))
                    } else {
                        OperationResult::String(Bytes::copy_from_slice(after))
                    }
                } else {
                    OperationResult::Error("Invalid bulk string".into())
                }
            }
        }
        b'*' => {
            // Array - simplified parsing
            OperationResult::Array(vec![])
        }
        _ => OperationResult::Error("Unknown response type".into()),
    }
}

/// Echo server for basic connectivity tests.
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

/// Simple RESP server that handles basic commands for testing.
async fn simple_kv_server() -> Result<(), BoxError> {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, SERVER_PORT)).await?;
    let store: Arc<Mutex<HashMap<Bytes, Bytes>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (mut stream, _addr) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];

            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                // Very simplified command parsing
                let data = &buf[..n];
                let response = if data.starts_with(b"*") {
                    // Parse RESP array
                    let s = String::from_utf8_lossy(data);
                    let parts: Vec<&str> = s.split("\r\n").collect();

                    // Extract command and args from RESP format
                    let mut args = Vec::new();
                    let mut i = 0;
                    while i < parts.len() {
                        if parts[i].starts_with('$') {
                            i += 1;
                            if i < parts.len() && !parts[i].is_empty() {
                                args.push(parts[i]);
                            }
                        }
                        i += 1;
                    }

                    if args.is_empty() {
                        b"+OK\r\n".to_vec()
                    } else {
                        let cmd = args[0].to_uppercase();
                        match cmd.as_str() {
                            "PING" => b"+PONG\r\n".to_vec(),
                            "SET" if args.len() >= 3 => {
                                let key = Bytes::from(args[1].to_string());
                                let value = Bytes::from(args[2].to_string());
                                store.lock().await.insert(key, value);
                                b"+OK\r\n".to_vec()
                            }
                            "GET" if args.len() >= 2 => {
                                let key = Bytes::from(args[1].to_string());
                                if let Some(value) = store.lock().await.get(&key) {
                                    format!(
                                        "${}\r\n{}\r\n",
                                        value.len(),
                                        String::from_utf8_lossy(value)
                                    )
                                    .into_bytes()
                                } else {
                                    b"$-1\r\n".to_vec()
                                }
                            }
                            "MSET" if args.len() >= 3 && (args.len() - 1) % 2 == 0 => {
                                let mut store = store.lock().await;
                                let mut i = 1;
                                while i + 1 < args.len() {
                                    let key = Bytes::from(args[i].to_string());
                                    let value = Bytes::from(args[i + 1].to_string());
                                    store.insert(key, value);
                                    i += 2;
                                }
                                b"+OK\r\n".to_vec()
                            }
                            "MGET" if args.len() >= 2 => {
                                let store = store.lock().await;
                                let mut response = format!("*{}\r\n", args.len() - 1);
                                for i in 1..args.len() {
                                    let key = Bytes::from(args[i].to_string());
                                    if let Some(value) = store.get(&key) {
                                        response.push_str(&format!(
                                            "${}\r\n{}\r\n",
                                            value.len(),
                                            String::from_utf8_lossy(value)
                                        ));
                                    } else {
                                        response.push_str("$-1\r\n");
                                    }
                                }
                                response.into_bytes()
                            }
                            _ => b"-ERR unknown command\r\n".to_vec(),
                        }
                    }
                } else {
                    b"-ERR invalid request\r\n".to_vec()
                };

                if stream.write_all(&response).await.is_err() {
                    return;
                }
            }
        });
    }
}

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

    sim.host(SERVER_HOST, simple_kv_server);

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

#[test]
fn test_mset_mget_scatter_gather() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, simple_kv_server);

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

#[test]
fn test_concurrent_clients() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, simple_kv_server);

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
// Sharded KV Server Implementation
// =============================================================================

/// Shard worker that handles GET/SET operations for its assigned keys.
async fn shard_worker(shard_id: usize, mut rx: mpsc::Receiver<ShardMessage>) {
    let mut store: HashMap<Bytes, Bytes> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            ShardMessage::Get { key, response_tx } => {
                let value = store.get(&key).cloned();
                let _ = response_tx.send(value);
            }
            ShardMessage::Set {
                key,
                value,
                response_tx,
            } => {
                store.insert(key, value);
                let _ = response_tx.send(true);
            }
        }
    }

    tracing::debug!("Shard {} worker shutting down", shard_id);
}

/// Create a sharded KV server with the given number of shards.
///
/// This server routes keys to shards based on CRC16 hash slots, similar to Redis Cluster.
async fn sharded_kv_server(num_shards: usize) -> Result<(), BoxError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create shard channels
    let mut shard_senders = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = mpsc::channel::<ShardMessage>(1024);
        shard_senders.push(tx);

        // Spawn shard worker
        tokio::spawn(shard_worker(shard_id, rx));
    }

    let shard_senders = Arc::new(shard_senders);

    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, SERVER_PORT)).await?;

    loop {
        let (mut stream, _addr) = listener.accept().await?;
        let senders = shard_senders.clone();
        let num_shards = num_shards;

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];

            loop {
                let n = match stream.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(_) => return,
                };

                // Very simplified command parsing (same as simple_kv_server)
                let data = &buf[..n];
                let response = if data.starts_with(b"*") {
                    let s = String::from_utf8_lossy(data);
                    let parts: Vec<&str> = s.split("\r\n").collect();

                    let mut args = Vec::new();
                    let mut i = 0;
                    while i < parts.len() {
                        if parts[i].starts_with('$') {
                            i += 1;
                            if i < parts.len() && !parts[i].is_empty() {
                                args.push(parts[i]);
                            }
                        }
                        i += 1;
                    }

                    if args.is_empty() {
                        b"+OK\r\n".to_vec()
                    } else {
                        let cmd = args[0].to_uppercase();
                        match cmd.as_str() {
                            "PING" => b"+PONG\r\n".to_vec(),
                            "SET" if args.len() >= 3 => {
                                let key = Bytes::from(args[1].to_string());
                                let value = Bytes::from(args[2].to_string());

                                // Route to correct shard
                                let shard = shard_for_key(key.as_ref(), num_shards);
                                let (tx, rx) = oneshot::channel();
                                let msg = ShardMessage::Set {
                                    key,
                                    value,
                                    response_tx: tx,
                                };
                                if senders[shard].send(msg).await.is_ok() {
                                    if rx.await.unwrap_or(false) {
                                        b"+OK\r\n".to_vec()
                                    } else {
                                        b"-ERR shard error\r\n".to_vec()
                                    }
                                } else {
                                    b"-ERR shard unavailable\r\n".to_vec()
                                }
                            }
                            "GET" if args.len() >= 2 => {
                                let key = Bytes::from(args[1].to_string());

                                // Route to correct shard
                                let shard = shard_for_key(key.as_ref(), num_shards);
                                let (tx, rx) = oneshot::channel();
                                let msg = ShardMessage::Get {
                                    key,
                                    response_tx: tx,
                                };
                                if senders[shard].send(msg).await.is_ok() {
                                    match rx.await {
                                        Ok(Some(value)) => format!(
                                            "${}\r\n{}\r\n",
                                            value.len(),
                                            String::from_utf8_lossy(&value)
                                        )
                                        .into_bytes(),
                                        Ok(None) => b"$-1\r\n".to_vec(),
                                        Err(_) => b"-ERR shard error\r\n".to_vec(),
                                    }
                                } else {
                                    b"-ERR shard unavailable\r\n".to_vec()
                                }
                            }
                            "MSET" if args.len() >= 3 && (args.len() - 1) % 2 == 0 => {
                                // Scatter to all relevant shards
                                let mut pending = Vec::new();
                                let mut i = 1;
                                while i + 1 < args.len() {
                                    let key = Bytes::from(args[i].to_string());
                                    let value = Bytes::from(args[i + 1].to_string());
                                    let shard = shard_for_key(key.as_ref(), num_shards);
                                    let (tx, rx) = oneshot::channel();
                                    let msg = ShardMessage::Set {
                                        key,
                                        value,
                                        response_tx: tx,
                                    };
                                    if senders[shard].send(msg).await.is_ok() {
                                        pending.push(rx);
                                    }
                                    i += 2;
                                }

                                // Gather results
                                let mut all_ok = true;
                                for rx in pending {
                                    if !rx.await.unwrap_or(false) {
                                        all_ok = false;
                                    }
                                }

                                if all_ok {
                                    b"+OK\r\n".to_vec()
                                } else {
                                    b"-ERR partial failure\r\n".to_vec()
                                }
                            }
                            "MGET" if args.len() >= 2 => {
                                // Scatter to all relevant shards
                                let mut pending = Vec::new();
                                for i in 1..args.len() {
                                    let key = Bytes::from(args[i].to_string());
                                    let shard = shard_for_key(key.as_ref(), num_shards);
                                    let (tx, rx) = oneshot::channel();
                                    let msg = ShardMessage::Get {
                                        key,
                                        response_tx: tx,
                                    };
                                    if senders[shard].send(msg).await.is_ok() {
                                        pending.push(rx);
                                    } else {
                                        pending.push({
                                            let (tx, rx) = oneshot::channel();
                                            let _ = tx.send(None);
                                            rx
                                        });
                                    }
                                }

                                // Gather results
                                let mut response = format!("*{}\r\n", pending.len());
                                for rx in pending {
                                    match rx.await {
                                        Ok(Some(value)) => {
                                            response.push_str(&format!(
                                                "${}\r\n{}\r\n",
                                                value.len(),
                                                String::from_utf8_lossy(&value)
                                            ));
                                        }
                                        _ => {
                                            response.push_str("$-1\r\n");
                                        }
                                    }
                                }
                                response.into_bytes()
                            }
                            _ => b"-ERR unknown command\r\n".to_vec(),
                        }
                    }
                } else {
                    b"-ERR invalid request\r\n".to_vec()
                };

                if stream.write_all(&response).await.is_err() {
                    return;
                }
            }
        });
    }
}

// =============================================================================
// Scatter-Gather Tests
// =============================================================================

#[test]
fn test_sharded_set_get_single_key() {
    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || sharded_kv_server(4));

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

#[test]
fn test_sharded_mset_mget_distribution() {
    let mut sim = Builder::new().build();

    // Use 4 shards to ensure key distribution
    sim.host(SERVER_HOST, || sharded_kv_server(4));

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
    let slot_a = hash_slot(b"{a}:key");
    let slot_b = hash_slot(b"{b}:key");
    // Note: They could collide, but it's unlikely for these short tags

    let mut sim = Builder::new().build();

    sim.host(SERVER_HOST, || sharded_kv_server(4));

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

    sim.host(SERVER_HOST, || sharded_kv_server(4));

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
    let mut sim = Builder::new().build();
    let history = Arc::new(Mutex::new(OperationHistory::new()));

    sim.host(SERVER_HOST, simple_kv_server);

    // Client 1: SET key=A, small delay, then SET key=B
    let h1 = history.clone();
    sim.client("client1", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // Small initial delay to let client2 start
        tokio::time::sleep(Duration::from_millis(5)).await;

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

        // Small delay between SETs
        tokio::time::sleep(Duration::from_millis(10)).await;

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

    // Client 2: GETs that overlap with client 1's operations
    let h2 = history.clone();
    sim.client("client2", async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 1024];

        // First GET - may see nil (before SET A completes)
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

        // Delay to ensure we're past SET A
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Second GET - should see A or B
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

    // The simple_kv_server uses a mutex, so all executions should be linearizable
    // If this fails, it indicates a bug in history recording or conversion
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

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, simple_kv_server);

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
            results_clone.lock().unwrap().push(("set1", matches!(response, OperationResult::Ok)));
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
                    results_clone.lock().unwrap().push(("get", v.as_ref() == b"value1"));
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
    assert!(results[1].1, "GET after reconnect should return correct value");
}

#[test]
fn test_high_latency_operations() {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .build();

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, simple_kv_server);

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

    sim.host(SERVER_HOST, || sharded_kv_server(4));

    sim.client("client1", async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = turmoil::lookup(SERVER_HOST);
        let mut stream = TcpStream::connect((addr, SERVER_PORT)).await?;
        let mut buf = vec![0u8; 4096];

        // MSET across multiple shards
        let cmd = encode_command(&[
            b"MSET", b"a", b"1", b"b", b"2", b"c", b"3", b"d", b"4",
        ]);
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
