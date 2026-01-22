//! Property-based tests for INCR/DECR commands.
//!
//! These tests use proptest to verify mathematical properties of increment/decrement
//! operations through the RESP protocol.

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_server::{Config, Server};
use futures::{SinkExt, StreamExt};
use proptest::prelude::*;
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Helper struct for managing a test server.
struct TestServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestServer {
    /// Start a new test server on an available port.
    async fn start() -> Self {
        let temp_dir = TempDir::new().unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        drop(metrics_listener);

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string();
        config.persistence.data_dir = temp_dir.path().to_path_buf();
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = metrics_addr.port();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let server = Server::new(config).await.unwrap();

            tokio::select! {
                result = server.run() => {
                    if let Err(e) = result {
                        eprintln!("Server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    // Shutdown requested
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        TestServer {
            addr,
            shutdown_tx: Some(shutdown_tx),
            handle,
            temp_dir,
        }
    }

    /// Connect to the test server.
    async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
    }

    /// Shutdown the test server.
    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }
}

/// Helper struct for sending commands and receiving responses.
struct TestClient {
    framed: Framed<TcpStream, Resp2>,
}

impl TestClient {
    /// Send a command and receive a response.
    async fn command(&mut self, args: &[&str]) -> Response {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        self.framed.send(frame).await.unwrap();

        let response_frame = timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error");

        frame_to_response(response_frame)
    }
}

/// Convert a BytesFrame to our Response type.
fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(e.into_inner()),
        BytesFrame::Integer(n) => Response::Integer(n),
        BytesFrame::BulkString(b) => Response::Bulk(Some(b)),
        BytesFrame::Null => Response::Bulk(None),
        BytesFrame::Array(items) => {
            Response::Array(items.into_iter().map(frame_to_response).collect())
        }
    }
}

/// Extract integer from response
fn extract_integer(response: &Response) -> Option<i64> {
    match response {
        Response::Integer(n) => Some(*n),
        _ => None,
    }
}

/// Extract float from response (stored as bulk string)
fn extract_float(response: &Response) -> Option<f64> {
    match response {
        Response::Bulk(Some(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

/// Check if response is an error
fn is_error(response: &Response) -> bool {
    matches!(response, Response::Error(_))
}

// ==================== Proptest Strategies ====================

/// Strategy for generating unique test keys
fn key_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,10}".prop_map(|s| format!("proptest:{}", s))
}

/// Strategy for safe initial integer values
fn safe_initial() -> impl Strategy<Value = i64> {
    -1_000_000i64..=1_000_000i64
}

/// Strategy for increment delta values
fn increment_delta() -> impl Strategy<Value = i64> {
    -10_000i64..=10_000i64
}

/// Strategy for non-integer strings
fn non_integer_string() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z]{1,5}",
        "[0-9]+\\.[0-9]+\\.[0-9]+", // Version-like strings
    ]
}

// ==================== Property Tests ====================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// INCRBY/DECRBY roundtrip: SET k v; INCRBY k d; DECRBY k d returns original
    #[test]
    fn test_incrby_decrby_roundtrip(
        key in key_strategy(),
        initial in safe_initial(),
        delta in increment_delta()
    ) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET initial value
            client.command(&["SET", &key, &initial.to_string()]).await;

            // INCRBY delta
            let after_incr = client.command(&["INCRBY", &key, &delta.to_string()]).await;
            let expected_after_incr = initial + delta;
            prop_assert_eq!(extract_integer(&after_incr), Some(expected_after_incr));

            // DECRBY delta (should return to original)
            let after_decr = client.command(&["DECRBY", &key, &delta.to_string()]).await;
            prop_assert_eq!(extract_integer(&after_decr), Some(initial));

            // Verify with GET
            let final_val = client.command(&["GET", &key]).await;
            if let Response::Bulk(Some(b)) = final_val {
                let val: i64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
                prop_assert_eq!(val, initial);
            } else {
                prop_assert!(false, "Expected bulk string response");
            }

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// INCR sequence equivalence: INCR n times == INCRBY n
    #[test]
    fn test_incr_sequence_equivalence(
        key in key_strategy(),
        initial in safe_initial(),
        n in 1i64..=10i64
    ) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            let key1 = format!("{}:1", key);
            let key2 = format!("{}:2", key);

            // Method 1: INCRBY n
            client.command(&["SET", &key1, &initial.to_string()]).await;
            let result1 = client.command(&["INCRBY", &key1, &n.to_string()]).await;

            // Method 2: INCR n times
            client.command(&["SET", &key2, &initial.to_string()]).await;
            let mut result2 = Response::Integer(initial);
            for _ in 0..n {
                result2 = client.command(&["INCR", &key2]).await;
            }

            prop_assert_eq!(extract_integer(&result1), extract_integer(&result2));

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// Nonexistent key creation: INCR on missing key creates with value 1
    #[test]
    fn test_incr_creates_nonexistent_key(key in key_strategy()) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // Ensure key doesn't exist
            client.command(&["DEL", &key]).await;

            // INCR on nonexistent key
            let result = client.command(&["INCR", &key]).await;
            prop_assert_eq!(extract_integer(&result), Some(1));

            // Verify
            let get_result = client.command(&["GET", &key]).await;
            if let Response::Bulk(Some(b)) = get_result {
                let val: i64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
                prop_assert_eq!(val, 1);
            } else {
                prop_assert!(false, "Expected bulk string response");
            }

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// Error on non-integer: INCR on string like "hello" returns ERR
    #[test]
    fn test_incr_error_on_non_integer(
        key in key_strategy(),
        value in non_integer_string()
    ) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET non-integer value
            client.command(&["SET", &key, &value]).await;

            // INCR should fail
            let result = client.command(&["INCR", &key]).await;
            prop_assert!(is_error(&result), "Expected error for non-integer value");

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// INCRBYFLOAT precision: Float roundtrip within epsilon
    #[test]
    fn test_incrbyfloat_precision(
        key in key_strategy(),
        initial in -1000.0f64..1000.0f64,
        delta in -100.0f64..100.0f64
    ) {
        // Skip very small deltas to avoid precision issues
        if delta.abs() < 0.0001 {
            return Ok(());
        }

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET initial value
            client.command(&["SET", &key, &initial.to_string()]).await;

            // INCRBYFLOAT delta
            let _after_incr = client.command(&["INCRBYFLOAT", &key, &delta.to_string()]).await;

            // INCRBYFLOAT -delta (should return close to original)
            let neg_delta = format!("{}", -delta);
            let after_decr = client.command(&["INCRBYFLOAT", &key, &neg_delta]).await;

            let final_val = extract_float(&after_decr).unwrap();

            // Check within epsilon
            let epsilon = if initial.abs() > 1.0 {
                initial.abs() * 1e-10
            } else {
                1e-10
            };

            prop_assert!(
                (final_val - initial).abs() < epsilon,
                "Expected {} to be within {} of {}, difference was {}",
                final_val, epsilon, initial, (final_val - initial).abs()
            );

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// INCRBYFLOAT rejects infinity result
    #[test]
    fn test_incrbyfloat_rejects_overflow(key in key_strategy()) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET to max f64
            client.command(&["SET", &key, &f64::MAX.to_string()]).await;

            // Try to increment by max f64 again (should overflow to infinity)
            let result = client.command(&["INCRBYFLOAT", &key, &f64::MAX.to_string()]).await;
            prop_assert!(is_error(&result), "Expected error for infinity result");

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// Overflow at boundaries: INCRBY 1 on i64::MAX returns error
    #[test]
    fn test_incrby_overflow_at_max(key in key_strategy(), delta in 1i64..=1000i64) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET to max i64
            client.command(&["SET", &key, &i64::MAX.to_string()]).await;

            // INCRBY should fail
            let result = client.command(&["INCRBY", &key, &delta.to_string()]).await;
            prop_assert!(is_error(&result), "Expected error for overflow");

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// Underflow at boundaries: DECRBY 1 on i64::MIN returns error
    #[test]
    fn test_decrby_underflow_at_min(key in key_strategy(), delta in 1i64..=1000i64) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // SET to min i64
            client.command(&["SET", &key, &i64::MIN.to_string()]).await;

            // DECRBY should fail
            let result = client.command(&["DECRBY", &key, &delta.to_string()]).await;
            prop_assert!(is_error(&result), "Expected error for underflow");

            server.shutdown().await;
            Ok(())
        })?;
    }

    // ==================== RESET Command Property Tests ====================

    /// RESET is idempotent - multiple RESETs don't fail
    #[test]
    fn test_reset_idempotent(reset_count in 1usize..10) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            for _ in 0..reset_count {
                let response = client.command(&["RESET"]).await;
                prop_assert_eq!(response, Response::Simple(Bytes::from("RESET")));
            }

            // Connection should still work
            let response = client.command(&["PING"]).await;
            prop_assert_eq!(response, Response::Simple(Bytes::from("PONG")));

            server.shutdown().await;
            Ok(())
        })?;
    }

    /// RESET after any state modification returns connection to clean state
    #[test]
    fn test_reset_restores_clean_state(
        client_name in "[a-z]{1,10}",
        _channel in "[a-z]{1,10}"
    ) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = TestServer::start().await;
            let mut client = server.connect().await;

            // Set client name
            client.command(&["CLIENT", "SETNAME", &client_name]).await;

            // Start transaction
            client.command(&["MULTI"]).await;
            client.command(&["SET", "key", "value"]).await;

            // RESET
            let response = client.command(&["RESET"]).await;
            prop_assert_eq!(response, Response::Simple(Bytes::from("RESET")));

            // Verify clean state: no name
            let response = client.command(&["CLIENT", "GETNAME"]).await;
            prop_assert!(matches!(response, Response::Bulk(None)));

            // Verify clean state: not in transaction (can start new MULTI)
            let response = client.command(&["MULTI"]).await;
            prop_assert_eq!(response, Response::Simple(Bytes::from("OK")));
            client.command(&["DISCARD"]).await;

            // Verify clean state: key was not set
            let response = client.command(&["GET", "key"]).await;
            prop_assert!(matches!(response, Response::Bulk(None)));

            server.shutdown().await;
            Ok(())
        })?;
    }
}
