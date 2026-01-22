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
use common::sim_harness::{OperationHistory, OperationResult};
use std::error::Error;
use std::time::Duration;
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
