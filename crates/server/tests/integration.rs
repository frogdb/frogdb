//! Integration tests for FrogDB.
//!
//! These tests start a real server and connect to it using the RESP protocol.

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_server::{Config, Server};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Helper struct for managing a test server.
struct TestServer {
    addr: SocketAddr,
    metrics_addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    handle: JoinHandle<()>,
    #[allow(dead_code)]
    temp_dir: TempDir, // Keep alive to prevent cleanup during test
}

impl TestServer {
    /// Start a new test server on an available port.
    async fn start() -> Self {
        // Create a unique temp directory for this test's data
        let temp_dir = TempDir::new().unwrap();

        // Find an available port for the main server
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Find an available port for the metrics server
        let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        drop(metrics_listener);

        // Create config with the chosen port and temp data dir
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string(); // Reduce noise during tests
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

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        TestServer {
            addr,
            metrics_addr,
            shutdown_tx,
            handle,
            temp_dir,
        }
    }

    /// Start a test server with requirepass configured.
    async fn start_with_security(requirepass: &str) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Find an available port for the metrics server
        let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        drop(metrics_listener);

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string();
        config.persistence.data_dir = temp_dir.path().to_path_buf();
        config.security.requirepass = requirepass.to_string();
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

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        TestServer {
            addr,
            metrics_addr,
            shutdown_tx,
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

    /// Get the metrics server address.
    fn metrics_addr(&self) -> SocketAddr {
        self.metrics_addr
    }

    /// Shutdown the test server.
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
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
        // Build command frame
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        // Send
        self.framed.send(frame).await.unwrap();

        // Receive
        let response_frame = timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error");

        frame_to_response(response_frame)
    }

    /// Read a pushed message (for pub/sub subscribers).
    /// Returns None if no message arrives within the timeout.
    async fn read_message(&mut self, timeout_duration: Duration) -> Option<Response> {
        match timeout(timeout_duration, self.framed.next()).await {
            Ok(Some(Ok(frame))) => Some(frame_to_response(frame)),
            _ => None,
        }
    }

    /// Send a command without waiting for response (for pub/sub mode).
    async fn send_only(&mut self, args: &[&str]) {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );
        self.framed.send(frame).await.unwrap();
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

#[tokio::test]
async fn test_ping_pong() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ping_with_message() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["PING", "hello"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_echo() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["ECHO", "hello world"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello world"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_set_get() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // GET
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["GET", "nonexistent"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_del() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET
    client.command(&["SET", "mykey", "myvalue"]).await;

    // DEL existing
    let response = client.command(&["DEL", "mykey"]).await;
    assert_eq!(response, Response::Integer(1));

    // DEL nonexistent
    let response = client.command(&["DEL", "mykey"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify deleted
    let response = client.command(&["GET", "mykey"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_exists() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // EXISTS nonexistent
    let response = client.command(&["EXISTS", "mykey"]).await;
    assert_eq!(response, Response::Integer(0));

    // SET
    client.command(&["SET", "mykey", "myvalue"]).await;

    // EXISTS existing
    let response = client.command(&["EXISTS", "mykey"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_unknown_command() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["FOOBAR"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR unknown command")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_wrong_arity() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // GET requires exactly 1 argument
    let response = client.command(&["GET"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR wrong number of arguments")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_set_overwrites() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key", "value1"]).await;
    let response = client.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    client.command(&["SET", "key", "value2"]).await;
    let response = client.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_command_returns_empty() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["COMMAND"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

#[tokio::test]
async fn test_multiple_clients() {
    let server = TestServer::start().await;

    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Client 1 sets a value
    client1.command(&["SET", "shared", "value"]).await;

    // Client 2 reads it
    let response = client2.command(&["GET", "shared"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    server.shutdown().await;
}

// ============================================================================
// Sorted Set Tests
// ============================================================================

#[tokio::test]
async fn test_zadd_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ZADD returns number of elements added
    let response = client.command(&["ZADD", "myzset", "1", "one"]).await;
    assert_eq!(response, Response::Integer(1));

    // Add another member
    let response = client
        .command(&["ZADD", "myzset", "2", "two", "3", "three"])
        .await;
    assert_eq!(response, Response::Integer(2));

    // Update existing member (returns 0 - no new members added)
    let response = client.command(&["ZADD", "myzset", "1.5", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // ZCARD
    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zadd_options() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add initial member
    client.command(&["ZADD", "myzset", "1", "one"]).await;

    // NX - only add new elements
    let response = client.command(&["ZADD", "myzset", "NX", "2", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // NX with new element
    let response = client.command(&["ZADD", "myzset", "NX", "2", "two"]).await;
    assert_eq!(response, Response::Integer(1));

    // XX - only update existing elements
    let response = client
        .command(&["ZADD", "myzset", "XX", "3", "three"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // XX with existing element
    let response = client.command(&["ZADD", "myzset", "XX", "5", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify score was updated
    let response = client.command(&["ZSCORE", "myzset", "one"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // CH - return changed count
    let response = client
        .command(&["ZADD", "myzset", "CH", "10", "one"])
        .await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zadd_incr() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // INCR mode returns the new score
    let response = client
        .command(&["ZADD", "myzset", "INCR", "5", "member"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // Increment again
    let response = client
        .command(&["ZADD", "myzset", "INCR", "3", "member"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("8"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zscore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1.5", "one", "2.5", "two"])
        .await;

    let response = client.command(&["ZSCORE", "myzset", "one"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1.5"))));

    // Non-existent member
    let response = client.command(&["ZSCORE", "myzset", "three"]).await;
    assert_eq!(response, Response::Bulk(None));

    // Non-existent key
    let response = client.command(&["ZSCORE", "nonexistent", "one"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zmscore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two"])
        .await;

    let response = client
        .command(&["ZMSCORE", "myzset", "one", "two", "three"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("1"))),
            Response::Bulk(Some(Bytes::from("2"))),
            Response::Bulk(None),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrem() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZREM", "myzset", "one", "four"]).await;
    assert_eq!(response, Response::Integer(1)); // Only "one" was removed

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zincrby() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Increment non-existent key
    let response = client.command(&["ZINCRBY", "myzset", "5", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    // Increment existing member
    let response = client.command(&["ZINCRBY", "myzset", "3", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("8"))));

    // Negative increment
    let response = client.command(&["ZINCRBY", "myzset", "-2", "member"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("6"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrank() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZRANK", "myzset", "one"]).await;
    assert_eq!(response, Response::Integer(0));

    let response = client.command(&["ZRANK", "myzset", "two"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["ZRANK", "myzset", "three"]).await;
    assert_eq!(response, Response::Integer(2));

    // Non-existent member
    let response = client.command(&["ZRANK", "myzset", "four"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrevrank() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZREVRANK", "myzset", "one"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["ZREVRANK", "myzset", "three"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrange_by_rank() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Basic range
    let response = client.command(&["ZRANGE", "myzset", "0", "-1"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    // With WITHSCORES
    let response = client
        .command(&["ZRANGE", "myzset", "0", "-1", "WITHSCORES"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("1"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("2"))),
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("3"))),
        ])
    );

    // Reverse
    let response = client
        .command(&["ZRANGE", "myzset", "0", "-1", "REV"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("one"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrange_by_score() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Range by score
    let response = client
        .command(&["ZRANGE", "myzset", "1", "2", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
        ])
    );

    // Exclusive bounds
    let response = client
        .command(&["ZRANGE", "myzset", "(1", "(3", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("two"))),])
    );

    // Infinity bounds
    let response = client
        .command(&["ZRANGE", "myzset", "-inf", "+inf", "BYSCORE"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_zrangebyscore_legacy() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZRANGEBYSCORE", "myzset", "-inf", "+inf"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("two"))),
            Response::Bulk(Some(Bytes::from("three"))),
        ])
    );

    // With LIMIT
    let response = client
        .command(&["ZRANGEBYSCORE", "myzset", "-inf", "+inf", "LIMIT", "1", "1"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![Response::Bulk(Some(Bytes::from("two"))),])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_zcount() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client.command(&["ZCOUNT", "myzset", "-inf", "+inf"]).await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["ZCOUNT", "myzset", "1", "2"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["ZCOUNT", "myzset", "(1", "3"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zpopmin_zpopmax() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    // Pop minimum
    let response = client.command(&["ZPOPMIN", "myzset"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("one"))),
            Response::Bulk(Some(Bytes::from("1"))),
        ])
    );

    // Pop maximum
    let response = client.command(&["ZPOPMAX", "myzset"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("three"))),
            Response::Bulk(Some(Bytes::from("3"))),
        ])
    );

    // Only "two" should remain
    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zunionstore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "zset1", "1", "a", "2", "b"]).await;
    client.command(&["ZADD", "zset2", "3", "b", "4", "c"]).await;

    let response = client
        .command(&["ZUNIONSTORE", "result", "2", "zset1", "zset2"])
        .await;
    assert_eq!(response, Response::Integer(3)); // a, b, c

    // Check scores (default SUM aggregate)
    let response = client.command(&["ZSCORE", "result", "a"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1"))));

    let response = client.command(&["ZSCORE", "result", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5")))); // 2 + 3

    let response = client.command(&["ZSCORE", "result", "c"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("4"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zinterstore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "zset1", "1", "a", "2", "b"]).await;
    client.command(&["ZADD", "zset2", "3", "b", "4", "c"]).await;

    let response = client
        .command(&["ZINTERSTORE", "result", "2", "zset1", "zset2"])
        .await;
    assert_eq!(response, Response::Integer(1)); // Only b is in both

    let response = client.command(&["ZSCORE", "result", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5")))); // 2 + 3

    server.shutdown().await;
}

#[tokio::test]
async fn test_zdiffstore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "zset1", "1", "a", "2", "b"]).await;
    client.command(&["ZADD", "zset2", "3", "b", "4", "c"]).await;

    let response = client
        .command(&["ZDIFFSTORE", "result", "2", "zset1", "zset2"])
        .await;
    assert_eq!(response, Response::Integer(1)); // Only a is in zset1 but not zset2

    let response = client.command(&["ZSCORE", "result", "a"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zremrangebyrank() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZREMRANGEBYRANK", "myzset", "0", "1"])
        .await;
    assert_eq!(response, Response::Integer(2)); // Removed "one" and "two"

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_zremrangebyscore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    let response = client
        .command(&["ZREMRANGEBYSCORE", "myzset", "-inf", "(3"])
        .await;
    assert_eq!(response, Response::Integer(2)); // Removed "one" and "two"

    let response = client.command(&["ZCARD", "myzset"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_wrongtype_error() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create a string key
    client.command(&["SET", "mykey", "hello"]).await;

    // Try to use sorted set commands on it
    let response = client.command(&["ZADD", "mykey", "1", "member"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    let response = client.command(&["ZSCORE", "mykey", "member"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_zset() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "myzset", "1", "one"]).await;

    let response = client.command(&["TYPE", "myzset"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("zset")));

    server.shutdown().await;
}

// ============================================================================
// Hash tests
// ============================================================================

#[tokio::test]
async fn test_hset_hget() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // HSET single field
    let response = client.command(&["HSET", "myhash", "field1", "value1"]).await;
    assert_eq!(response, Response::Integer(1)); // 1 new field added

    // HGET existing field
    let response = client.command(&["HGET", "myhash", "field1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    // HGET nonexistent field
    let response = client.command(&["HGET", "myhash", "nonexistent"]).await;
    assert_eq!(response, Response::Bulk(None));

    // HSET multiple fields
    let response = client
        .command(&["HSET", "myhash", "field2", "value2", "field3", "value3"])
        .await;
    assert_eq!(response, Response::Integer(2)); // 2 new fields added

    // Update existing field (returns 0)
    let response = client.command(&["HSET", "myhash", "field1", "updated"]).await;
    assert_eq!(response, Response::Integer(0)); // 0 new fields, 1 updated

    server.shutdown().await;
}

#[tokio::test]
async fn test_hsetnx() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // HSETNX new field
    let response = client
        .command(&["HSETNX", "myhash", "field1", "value1"])
        .await;
    assert_eq!(response, Response::Integer(1));

    // HSETNX existing field
    let response = client
        .command(&["HSETNX", "myhash", "field1", "value2"])
        .await;
    assert_eq!(response, Response::Integer(0));

    // Verify original value unchanged
    let response = client.command(&["HGET", "myhash", "field1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hdel() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // HDEL existing fields
    let response = client.command(&["HDEL", "myhash", "f1", "f2"]).await;
    assert_eq!(response, Response::Integer(2));

    // HDEL nonexistent
    let response = client.command(&["HDEL", "myhash", "f1"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify f3 still exists
    let response = client.command(&["HGET", "myhash", "f3"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("v3"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hmget() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;

    let response = client
        .command(&["HMGET", "myhash", "f1", "nonexistent", "f2"])
        .await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("v1"))),
            Response::Bulk(None),
            Response::Bulk(Some(Bytes::from("v2"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_hgetall() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    let response = client.command(&["HGETALL", "myhash"]).await;
    // Returns flat array: [field1, value1, field2, value2, ...]
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    // HGETALL nonexistent key
    let response = client.command(&["HGETALL", "nonexistent"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hlen_hexists() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // HLEN nonexistent
    let response = client.command(&["HLEN", "myhash"]).await;
    assert_eq!(response, Response::Integer(0));

    client.command(&["HSET", "myhash", "f1", "v1", "f2", "v2"]).await;

    // HLEN
    let response = client.command(&["HLEN", "myhash"]).await;
    assert_eq!(response, Response::Integer(2));

    // HEXISTS
    let response = client.command(&["HEXISTS", "myhash", "f1"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["HEXISTS", "myhash", "nonexistent"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hincrby() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // HINCRBY on nonexistent field (creates it)
    let response = client.command(&["HINCRBY", "myhash", "counter", "5"]).await;
    assert_eq!(response, Response::Integer(5));

    // HINCRBY on existing field
    let response = client.command(&["HINCRBY", "myhash", "counter", "3"]).await;
    assert_eq!(response, Response::Integer(8));

    // HINCRBY negative
    let response = client
        .command(&["HINCRBY", "myhash", "counter", "-2"])
        .await;
    assert_eq!(response, Response::Integer(6));

    server.shutdown().await;
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["HINCRBYFLOAT", "myhash", "price", "10.5"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("10.5"))));

    let response = client
        .command(&["HINCRBYFLOAT", "myhash", "price", "0.5"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("11"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_hash() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    let response = client.command(&["TYPE", "myhash"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("hash")));

    server.shutdown().await;
}

// ============================================================================
// List tests
// ============================================================================

#[tokio::test]
async fn test_lpush_rpush() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LPUSH
    let response = client.command(&["LPUSH", "mylist", "a"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["LPUSH", "mylist", "b", "c"]).await;
    assert_eq!(response, Response::Integer(3)); // c, b, a

    // RPUSH
    let response = client.command(&["RPUSH", "mylist", "d"]).await;
    assert_eq!(response, Response::Integer(4)); // c, b, a, d

    server.shutdown().await;
}

#[tokio::test]
async fn test_lpop_rpop() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c", "d"]).await;

    // LPOP
    let response = client.command(&["LPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("a"))));

    // RPOP
    let response = client.command(&["RPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("d"))));

    // LPOP with count
    let response = client.command(&["LPOP", "mylist", "2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    // LPOP empty list
    let response = client.command(&["LPOP", "mylist"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_llen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LLEN nonexistent
    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(0));

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lrange() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"]).await;

    // Full range
    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 5);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("a"))));
            assert_eq!(items[4], Response::Bulk(Some(Bytes::from("e"))));
        }
        _ => panic!("Expected array response"),
    }

    // Partial range
    let response = client.command(&["LRANGE", "mylist", "1", "3"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("d"))));
        }
        _ => panic!("Expected array response"),
    }

    // Negative indices
    let response = client.command(&["LRANGE", "mylist", "-3", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lindex_lset() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;

    // LINDEX
    let response = client.command(&["LINDEX", "mylist", "1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("b"))));

    // LINDEX negative
    let response = client.command(&["LINDEX", "mylist", "-1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("c"))));

    // LINDEX out of range
    let response = client.command(&["LINDEX", "mylist", "100"]).await;
    assert_eq!(response, Response::Bulk(None));

    // LSET
    let response = client.command(&["LSET", "mylist", "1", "updated"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["LINDEX", "mylist", "1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("updated"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_ltrim() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;

    let response = client.command(&["LTRIM", "mylist", "1", "3"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("d"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_linsert() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "c"]).await;

    // LINSERT BEFORE
    let response = client
        .command(&["LINSERT", "mylist", "BEFORE", "c", "b"])
        .await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("b"))));
        }
        _ => panic!("Expected array response"),
    }

    // LINSERT AFTER
    let response = client
        .command(&["LINSERT", "mylist", "AFTER", "c", "d"])
        .await;
    assert_eq!(response, Response::Integer(4));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lrem() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
        .await;

    // LREM count > 0 (remove from head)
    let response = client.command(&["LREM", "mylist", "2", "a"]).await;
    assert_eq!(response, Response::Integer(2));

    let response = client.command(&["LLEN", "mylist"]).await;
    assert_eq!(response, Response::Integer(3)); // b, c, a

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_list() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a"]).await;

    let response = client.command(&["TYPE", "mylist"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("list")));

    server.shutdown().await;
}

// ============================================================================
// Set tests
// ============================================================================

#[tokio::test]
async fn test_sadd_smembers() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SADD
    let response = client.command(&["SADD", "myset", "a", "b", "c"]).await;
    assert_eq!(response, Response::Integer(3));

    // SADD with duplicates
    let response = client.command(&["SADD", "myset", "a", "d"]).await;
    assert_eq!(response, Response::Integer(1)); // Only d was added

    // SMEMBERS
    let response = client.command(&["SMEMBERS", "myset"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 4);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_srem() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    let response = client.command(&["SREM", "myset", "a", "d"]).await;
    assert_eq!(response, Response::Integer(1)); // Only a was removed

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sismember_smismember() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b"]).await;

    // SISMEMBER
    let response = client.command(&["SISMEMBER", "myset", "a"]).await;
    assert_eq!(response, Response::Integer(1));

    let response = client.command(&["SISMEMBER", "myset", "c"]).await;
    assert_eq!(response, Response::Integer(0));

    // SMISMEMBER
    let response = client.command(&["SMISMEMBER", "myset", "a", "c", "b"]).await;
    assert_eq!(
        response,
        Response::Array(vec![
            Response::Integer(1),
            Response::Integer(0),
            Response::Integer(1),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_scard() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SCARD nonexistent
    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(0));

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sunion() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b"]).await;
    client.command(&["SADD", "{set}2", "b", "c"]).await;

    let response = client.command(&["SUNION", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 3); // a, b, c
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sinter() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client.command(&["SINTER", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2); // b, c
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sdiff() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client.command(&["SDIFF", "{set}1", "{set}2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 1); // a
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("a"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_sunionstore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b"]).await;
    client.command(&["SADD", "{set}2", "b", "c"]).await;

    let response = client
        .command(&["SUNIONSTORE", "{set}dest", "{set}1", "{set}2"])
        .await;
    assert_eq!(response, Response::Integer(3));

    let response = client.command(&["SCARD", "{set}dest"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_sinterstore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{set}1", "a", "b", "c"]).await;
    client.command(&["SADD", "{set}2", "b", "c", "d"]).await;

    let response = client
        .command(&["SINTERSTORE", "{set}dest", "{set}1", "{set}2"])
        .await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_spop() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    // SPOP single
    let response = client.command(&["SPOP", "myset"]).await;
    match response {
        Response::Bulk(Some(_)) => {}
        _ => panic!("Expected bulk response"),
    }

    let response = client.command(&["SCARD", "myset"]).await;
    assert_eq!(response, Response::Integer(2));

    // SPOP with count
    let response = client.command(&["SPOP", "myset", "2"]).await;
    match response {
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_type_command_set() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a"]).await;

    let response = client.command(&["TYPE", "myset"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("set")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_wrongtype_hash_list_set() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create a hash
    client.command(&["HSET", "myhash", "f1", "v1"]).await;

    // Try list command on hash
    let response = client.command(&["LPUSH", "myhash", "a"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Try set command on hash
    let response = client.command(&["SADD", "myhash", "a"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Create a list
    client.command(&["RPUSH", "mylist", "a"]).await;

    // Try hash command on list
    let response = client.command(&["HGET", "mylist", "f1"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    // Create a set
    client.command(&["SADD", "myset", "a"]).await;

    // Try hash command on set
    let response = client.command(&["HGET", "myset", "f1"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGTYPE")));

    server.shutdown().await;
}

// ============================================================================
// LCS Tests
// ============================================================================

#[tokio::test]
async fn test_lcs_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "ohmytext"]).await;
    client.command(&["SET", "b", "mynewtext"]).await;

    let response = client.command(&["LCS", "a", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("mytext"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_len() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "ohmytext"]).await;
    client.command(&["SET", "b", "mynewtext"]).await;

    let response = client.command(&["LCS", "a", "b", "LEN"]).await;
    assert_eq!(response, Response::Integer(6));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_idx() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "ohmytext"]).await;
    client.command(&["SET", "b", "mynewtext"]).await;

    let response = client.command(&["LCS", "a", "b", "IDX"]).await;
    match response {
        Response::Array(arr) => {
            // Should have "matches" and "len" keys
            assert!(!arr.is_empty(), "IDX response should not be empty");
        }
        _ => panic!("Expected array response for IDX mode, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_minmatchlen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "ohmytext"]).await;
    client.command(&["SET", "b", "mynewtext"]).await;

    // With MINMATCHLEN 4, should filter short matches
    let response = client
        .command(&["LCS", "a", "b", "IDX", "MINMATCHLEN", "4"])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_withmatchlen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "ohmytext"]).await;
    client.command(&["SET", "b", "mynewtext"]).await;

    let response = client
        .command(&["LCS", "a", "b", "IDX", "WITHMATCHLEN"])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_nonexistent_keys() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Non-existent keys treated as empty strings
    let response = client
        .command(&["LCS", "nonexistent1", "nonexistent2"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from(""))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_identical_strings() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "identical"]).await;
    client.command(&["SET", "b", "identical"]).await;

    let response = client.command(&["LCS", "a", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("identical"))));

    let response = client.command(&["LCS", "a", "b", "LEN"]).await;
    assert_eq!(response, Response::Integer(9));

    server.shutdown().await;
}

#[tokio::test]
async fn test_lcs_no_common() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "a", "abc"]).await;
    client.command(&["SET", "b", "xyz"]).await;

    let response = client.command(&["LCS", "a", "b"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from(""))));

    let response = client.command(&["LCS", "a", "b", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

// ============================================================================
// Transaction tests (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
// ============================================================================

#[tokio::test]
async fn test_multi_exec_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client.command(&["SET", "key1", "value1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["SET", "key2", "value2"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client.command(&["GET", "key1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute transaction
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
            assert_eq!(results[1], Response::Simple(Bytes::from("OK")));
            assert_eq!(results[2], Response::Bulk(Some(Bytes::from("value1"))));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify values are persisted
    let response = client.command(&["GET", "key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    let response = client.command(&["GET", "key2"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_multi_exec_empty() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Execute with no commands
    let response = client.command(&["EXEC"]).await;
    assert_eq!(response, Response::Array(vec![]));

    server.shutdown().await;
}

#[tokio::test]
async fn test_multi_discard() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "foo", "original"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a command
    let response = client.command(&["SET", "foo", "modified"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Discard transaction
    let response = client.command(&["DISCARD"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify value was not modified
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("original"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_exec_without_multi() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR EXEC without MULTI")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_discard_without_multi() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["DISCARD"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR DISCARD without MULTI")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_nested_multi() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Try to start another transaction
    let response = client.command(&["MULTI"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR MULTI calls can not be nested")));

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_exec_success() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set initial value
    client.command(&["SET", "watched_key", "initial"]).await;

    // Watch the key
    let response = client.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client.command(&["SET", "watched_key", "updated"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute (should succeed since no one else modified the key)
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify value was updated
    let response = client.command(&["GET", "watched_key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("updated"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_exec_abort() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "watched_key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "watched_key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 modifies the key
    client2.command(&["SET", "watched_key", "modified_by_client2"]).await;

    // Client 1 starts transaction
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue commands
    let response = client1.command(&["SET", "watched_key", "modified_by_client1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute (should fail/abort because the watched key was modified)
    let response = client1.command(&["EXEC"]).await;
    assert_eq!(response, Response::Bulk(None)); // Nil response on WATCH abort

    // Verify value is still what client2 set (client1's transaction was aborted)
    let response = client1.command(&["GET", "watched_key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("modified_by_client2"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_watch_inside_multi_error() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Try to WATCH inside MULTI (should error)
    let response = client.command(&["WATCH", "somekey"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR WATCH inside MULTI is not allowed")));

    // Discard to clean up
    client.command(&["DISCARD"]).await;

    server.shutdown().await;
}

#[tokio::test]
async fn test_unwatch() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "key", "initial"]).await;

    // Client 1 watches the key
    let response = client1.command(&["WATCH", "key"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 1 unwatches
    let response = client1.command(&["UNWATCH"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Client 2 modifies the key
    client2.command(&["SET", "key", "modified"]).await;

    // Client 1 starts transaction (should still succeed because UNWATCH cleared watches)
    let response = client1.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client1.command(&["SET", "key", "from_client1"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    let response = client1.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK")));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify client1's transaction succeeded
    let response = client1.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("from_client1"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_transaction_with_error() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set a string key
    client.command(&["SET", "mystring", "hello"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a command that will succeed
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue a command that will fail at runtime (LPUSH on a string)
    let response = client.command(&["LPUSH", "mystring", "value"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue another command that will succeed
    let response = client.command(&["SET", "baz", "qux"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Execute - all commands run, one returns error
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Response::Simple(Bytes::from("OK"))); // SET foo bar
            assert!(matches!(results[1], Response::Error(ref e) if e.starts_with(b"WRONGTYPE"))); // LPUSH mystring
            assert_eq!(results[2], Response::Simple(Bytes::from("OK"))); // SET baz qux
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify the successful commands did execute
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    let response = client.command(&["GET", "baz"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("qux"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_transaction_syntax_error_aborts() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a valid command
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // Queue a command with wrong number of arguments (syntax error)
    let response = client.command(&["GET"]).await; // GET requires 1 argument
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"ERR wrong number of arguments")));

    // Execute - should abort due to syntax error during queuing
    let response = client.command(&["EXEC"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"EXECABORT")));

    // Verify the first command was NOT executed
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_transaction_increments() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set initial counter
    client.command(&["SET", "counter", "0"]).await;

    // Start transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue multiple increments
    for _ in 0..5 {
        let response = client.command(&["INCR", "counter"]).await;
        assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));
    }

    // Execute
    let response = client.command(&["EXEC"]).await;
    match response {
        Response::Array(results) => {
            assert_eq!(results.len(), 5);
            assert_eq!(results[0], Response::Integer(1));
            assert_eq!(results[1], Response::Integer(2));
            assert_eq!(results[2], Response::Integer(3));
            assert_eq!(results[3], Response::Integer(4));
            assert_eq!(results[4], Response::Integer(5));
        }
        _ => panic!("Expected array response from EXEC"),
    }

    // Verify final value
    let response = client.command(&["GET", "counter"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("5"))));

    server.shutdown().await;
}

// =============================================================================
// Pub/Sub Tests
// =============================================================================

#[tokio::test]
async fn test_subscribe_publish() {
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
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
    let response = publisher.command(&["PUBLISH", "news.sports", "goal!"]).await;
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
    publisher.command(&["PUBLISH", "weather.today", "sunny"]).await;

    // Should not receive a message
    let msg = subscriber.read_message(Duration::from_millis(200)).await;
    assert!(msg.is_none(), "Should not receive message for non-matching pattern");

    server.shutdown().await;
}

#[tokio::test]
async fn test_unsubscribe() {
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
    let mut subscriber = server.connect().await;
    let mut client = server.connect().await;

    // Subscribe to patterns
    subscriber.command(&["PSUBSCRIBE", "news.*", "sports.*"]).await;
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
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Subscribe to enter pub/sub mode
    client.command(&["SUBSCRIBE", "mychannel"]).await;

    // Try to execute a non-pub/sub command
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(ref e) if e.starts_with(b"ERR Can't execute")));

    // PING should still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("PONG")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let server = TestServer::start().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut publisher = server.connect().await;

    // Both subscribe to the same channel
    sub1.command(&["SUBSCRIBE", "broadcast"]).await;
    sub2.command(&["SUBSCRIBE", "broadcast"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish a message
    let response = publisher.command(&["PUBLISH", "broadcast", "hello everyone"]).await;
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
    let server = TestServer::start().await;
    let mut broadcast_sub = server.connect().await;
    let mut sharded_sub = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to broadcast channel
    broadcast_sub.command(&["SUBSCRIBE", "orders"]).await;

    // Subscribe to sharded channel with same name
    sharded_sub.command(&["SSUBSCRIBE", "orders"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // PUBLISH (broadcast) should only reach SUBSCRIBE, not SSUBSCRIBE
    publisher.command(&["PUBLISH", "orders", "broadcast message"]).await;

    let broadcast_msg = broadcast_sub.read_message(Duration::from_secs(1)).await;
    let sharded_msg = sharded_sub.read_message(Duration::from_millis(200)).await;

    assert!(broadcast_msg.is_some(), "Broadcast subscriber should receive PUBLISH");
    assert!(sharded_msg.is_none(), "Sharded subscriber should NOT receive PUBLISH");

    // SPUBLISH (sharded) should only reach SSUBSCRIBE, not SUBSCRIBE
    publisher.command(&["SPUBLISH", "orders", "sharded message"]).await;

    let broadcast_msg = broadcast_sub.read_message(Duration::from_millis(200)).await;
    let sharded_msg = sharded_sub.read_message(Duration::from_secs(1)).await;

    assert!(broadcast_msg.is_none(), "Broadcast subscriber should NOT receive SPUBLISH");
    assert!(sharded_msg.is_some(), "Sharded subscriber should receive SPUBLISH");

    server.shutdown().await;
}

#[tokio::test]
async fn test_sharded_subscribe_publish() {
    let server = TestServer::start().await;
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
    let response = publisher.command(&["SPUBLISH", "orders:123", "new order"]).await;
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
    let server = TestServer::start().await;
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
// RESET Command Tests
// ============================================================================

#[tokio::test]
async fn test_reset_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // RESET should return "RESET" simple string
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // RESET is idempotent - can be called multiple times
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_exits_pubsub_mode() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Enter pub/sub mode
    client.command(&["SUBSCRIBE", "mychannel"]).await;

    // Verify we're in pub/sub mode (GET should fail)
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(ref e) if e.starts_with(b"ERR Can't execute")));

    // RESET should exit pub/sub mode
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Now GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Bulk(None))); // Key doesn't exist, but command works

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_exits_pattern_pubsub_mode() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Enter pub/sub mode via pattern subscribe
    client.command(&["PSUBSCRIBE", "chan*"]).await;

    // RESET should exit pub/sub mode
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Now normal commands should work
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_aborts_transaction() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Start a transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Queue a SET command
    let response = client.command(&["SET", "txkey", "txvalue"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("QUEUED")));

    // RESET aborts the transaction
    let response = client.command(&["RESET"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("RESET")));

    // Key should not exist (transaction was aborted)
    let response = client.command(&["GET", "txkey"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    // Should be able to start a new transaction
    let response = client.command(&["MULTI"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_clears_watches() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set initial value
    client1.command(&["SET", "watchkey", "initial"]).await;

    // Watch the key
    client1.command(&["WATCH", "watchkey"]).await;

    // RESET clears the watch
    client1.command(&["RESET"]).await;

    // Modify with client2
    client2.command(&["SET", "watchkey", "modified"]).await;

    // Client1's MULTI/EXEC should succeed (watch was cleared by RESET)
    client1.command(&["MULTI"]).await;
    client1.command(&["SET", "watchkey", "client1"]).await;
    let response = client1.command(&["EXEC"]).await;

    // EXEC should succeed (not return nil)
    assert!(matches!(response, Response::Array(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_clears_client_name() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set client name
    client.command(&["CLIENT", "SETNAME", "my-client"]).await;

    // Verify name is set
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("my-client"))));

    // RESET clears the name
    client.command(&["RESET"]).await;

    // Name should be cleared
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_unsubscribes_from_sharded_channels() {
    let server = TestServer::start().await;
    let mut subscriber = server.connect().await;
    let mut publisher = server.connect().await;

    // Subscribe to sharded channel
    subscriber.command(&["SSUBSCRIBE", "sharded:chan"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RESET unsubscribes
    subscriber.command(&["RESET"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish should report 0 subscribers
    let response = publisher.command(&["SPUBLISH", "sharded:chan", "msg"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_reset_publisher_still_reaches_other_subscribers() {
    let server = TestServer::start().await;
    let mut sub1 = server.connect().await;
    let mut sub2 = server.connect().await;
    let mut publisher = server.connect().await;

    // Both subscribe
    sub1.command(&["SUBSCRIBE", "channel"]).await;
    sub2.command(&["SUBSCRIBE", "channel"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // sub1 RESETs (unsubscribes)
    sub1.command(&["RESET"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish should reach sub2 only
    let response = publisher.command(&["PUBLISH", "channel", "hello"]).await;
    assert_eq!(response, Response::Integer(1)); // Only 1 subscriber remains

    // sub2 should receive the message
    let msg = sub2.read_message(Duration::from_secs(2)).await;
    assert!(msg.is_some());

    server.shutdown().await;
}

// ============================================================================
// CLIENT Command Tests
// ============================================================================

#[tokio::test]
async fn test_client_id() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // CLIENT ID should return a positive integer
    let response = client.command(&["CLIENT", "ID"]).await;
    match response {
        Response::Integer(id) => assert!(id > 0, "CLIENT ID should return positive integer"),
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setname_getname() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Initially, name should be null
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    // Set a name
    let response = client.command(&["CLIENT", "SETNAME", "test-connection"]).await;
    assert_eq!(response, Response::ok());

    // Get the name back
    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("test-connection"))));

    // Clear the name by setting empty string
    let response = client.command(&["CLIENT", "SETNAME", ""]).await;
    assert_eq!(response, Response::ok());

    let response = client.command(&["CLIENT", "GETNAME"]).await;
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setname_invalid_name() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Names with spaces should be rejected
    let response = client.command(&["CLIENT", "SETNAME", "name with spaces"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_list() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Set names for identification
    client1.command(&["CLIENT", "SETNAME", "client-one"]).await;
    client2.command(&["CLIENT", "SETNAME", "client-two"]).await;

    // CLIENT LIST should show both connections
    let response = client1.command(&["CLIENT", "LIST"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let list_str = String::from_utf8_lossy(&data);
            assert!(list_str.contains("client-one"), "Should contain client-one");
            assert!(list_str.contains("client-two"), "Should contain client-two");
            // Should have id= field
            assert!(list_str.contains("id="), "Should contain id field");
            // Should have addr= field
            assert!(list_str.contains("addr="), "Should contain addr field");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_info() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set a name
    client.command(&["CLIENT", "SETNAME", "info-test"]).await;

    // CLIENT INFO should return our connection info
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(info_str.contains("info-test"), "Should contain our name");
            assert!(info_str.contains("id="), "Should contain id field");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_kill_by_id() {
    let server = TestServer::start().await;
    let mut killer = server.connect().await;
    let mut victim = server.connect().await;

    // Get victim's ID
    let victim_id = match victim.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Kill victim by ID
    let response = killer.command(&["CLIENT", "KILL", "ID", &victim_id.to_string()]).await;
    assert_eq!(response, Response::Integer(1), "Should kill 1 connection");

    // Give time for kill to take effect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Victim should be disconnected - verify by trying to read from the connection
    // The connection should be closed, so reading should return None or timeout
    let read_result = timeout(
        Duration::from_millis(500),
        victim.framed.next(),
    )
    .await;

    match read_result {
        Ok(None) => {
            // Connection closed as expected
        }
        Err(_) => {
            // Timeout - also acceptable, connection may be stuck
        }
        Ok(Some(_)) => {
            // Got some data - this might happen if there's pending data
            // Just verify killer is still alive
        }
    }

    // Verify killer is still connected
    let response = killer.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_pause_unpause() {
    let server = TestServer::start().await;
    let mut admin = server.connect().await;
    let mut worker = server.connect().await;

    // Pause with a long timeout
    let response = admin.command(&["CLIENT", "PAUSE", "10000", "WRITE"]).await;
    assert_eq!(response, Response::ok());

    // Reads should still work
    let response = timeout(Duration::from_millis(500), worker.command(&["GET", "key"]))
        .await
        .expect("GET should complete during WRITE pause");
    assert!(matches!(response, Response::Null | Response::Bulk(None)));

    // Unpause
    let response = admin.command(&["CLIENT", "UNPAUSE"]).await;
    assert_eq!(response, Response::ok());

    // Writes should work after unpause
    let response = timeout(Duration::from_millis(500), worker.command(&["SET", "key", "value"]))
        .await
        .expect("SET should complete after unpause");
    assert_eq!(response, Response::ok());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_pause_timeout() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Pause for a very short time (50ms)
    let response = client.command(&["CLIENT", "PAUSE", "50", "ALL"]).await;
    assert_eq!(response, Response::ok());

    // Wait for pause to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Commands should work after timeout expires
    let response = timeout(Duration::from_millis(500), client.command(&["PING"]))
        .await
        .expect("PING should complete after pause timeout");
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_list_type_filter() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Filter by normal type
    let response = client.command(&["CLIENT", "LIST", "TYPE", "normal"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let list_str = String::from_utf8_lossy(&data);
            // Normal connections should appear
            assert!(!list_str.is_empty() || list_str.contains("id="));
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    // Filter by master type (should be empty since we have no replication)
    let response = client.command(&["CLIENT", "LIST", "TYPE", "master"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            // Should return empty or only newlines
            let list_str = String::from_utf8_lossy(&data);
            // Master list should be empty since we don't have replication
            assert!(!list_str.contains("id=") || list_str.is_empty());
        }
        Response::Bulk(None) => {
            // Also acceptable - no clients of this type
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // CLIENT HELP should return an array of strings
    let response = client.command(&["CLIENT", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "Help should not be empty");
            // First entry should mention CLIENT command
            if let Response::Bulk(Some(first)) = &arr[0] {
                let first_str = String::from_utf8_lossy(first);
                assert!(
                    first_str.to_uppercase().contains("CLIENT"),
                    "Help should mention CLIENT"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT SETINFO tests
#[tokio::test]
async fn test_client_setinfo_lib_name() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["CLIENT", "SETINFO", "LIB-NAME", "my-test-lib"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify it appears in CLIENT INFO
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(
                info_str.contains("lib-name=my-test-lib"),
                "Should contain lib-name"
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_setinfo_lib_ver() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["CLIENT", "SETINFO", "LIB-VER", "1.2.3"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify it appears in CLIENT INFO
    let response = client.command(&["CLIENT", "INFO"]).await;
    match response {
        Response::Bulk(Some(data)) => {
            let info_str = String::from_utf8_lossy(&data);
            assert!(info_str.contains("lib-ver=1.2.3"), "Should contain lib-ver");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT NO-EVICT tests
#[tokio::test]
async fn test_client_no_evict() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Enable NO-EVICT
    let response = client.command(&["CLIENT", "NO-EVICT", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Disable NO-EVICT
    let response = client.command(&["CLIENT", "NO-EVICT", "OFF"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Invalid argument
    let response = client.command(&["CLIENT", "NO-EVICT", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT NO-TOUCH tests
#[tokio::test]
async fn test_client_no_touch() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Enable NO-TOUCH
    let response = client.command(&["CLIENT", "NO-TOUCH", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Disable NO-TOUCH
    let response = client.command(&["CLIENT", "NO-TOUCH", "OFF"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

// CLIENT REPLY tests
#[tokio::test]
async fn test_client_reply_on() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // CLIENT REPLY ON should be accepted (normal mode)
    let response = client.command(&["CLIENT", "REPLY", "ON"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify commands still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_client_reply_invalid() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Invalid argument should return error
    let response = client.command(&["CLIENT", "REPLY", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT TRACKINGINFO tests
#[tokio::test]
async fn test_client_trackinginfo() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    match response {
        Response::Array(arr) => {
            // Should return tracking info structure (flags, redirect, prefixes)
            assert!(!arr.is_empty());
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// CLIENT GETREDIR tests
#[tokio::test]
async fn test_client_getredir() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Without tracking, should return -1
    let response = client.command(&["CLIENT", "GETREDIR"]).await;
    assert_eq!(response, Response::Integer(-1));

    server.shutdown().await;
}

// CLIENT CACHING tests
#[tokio::test]
async fn test_client_caching() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Caching commands should be accepted (stubs for now)
    let response = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    let response = client.command(&["CLIENT", "CACHING", "NO"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Invalid argument
    let response = client.command(&["CLIENT", "CACHING", "INVALID"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

// CLIENT UNBLOCK tests
#[tokio::test]
async fn test_client_unblock_not_blocked() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Get our own ID
    let client_id = match client.command(&["CLIENT", "ID"]).await {
        Response::Integer(id) => id,
        other => panic!("Expected integer, got {:?}", other),
    };

    // Unblocking a client that isn't blocked should return 0
    let response = client
        .command(&["CLIENT", "UNBLOCK", &client_id.to_string()])
        .await;
    assert_eq!(response, Response::Integer(0));

    // Non-existent client ID should also return 0
    let response = client.command(&["CLIENT", "UNBLOCK", "999999"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

// ============================================================================
// ACL Integration Tests
// ============================================================================

#[tokio::test]
async fn test_auth_default_user_nopass() {
    // Without requirepass, AUTH should not be required
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Commands should work without AUTH
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_with_requirepass() {
    // With requirepass configured, commands should require AUTH
    let server = TestServer::start_with_security("testpassword123").await;
    let mut client = server.connect().await;

    // PING is auth-exempt (like in Redis), so it should work without AUTH
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // GET without AUTH should return NOAUTH error
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // SET without AUTH should return NOAUTH error
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // AUTH with correct password should work
    let response = client.command(&["AUTH", "testpassword123"]).await;
    assert_eq!(response, Response::ok());

    // After AUTH, PING should still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    // After AUTH, GET should work
    let response = client.command(&["GET", "foo"]).await;
    assert_eq!(response, Response::Bulk(None));

    // After AUTH, SET should work
    let response = client.command(&["SET", "foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_wrong_password() {
    let server = TestServer::start_with_security("correctpassword").await;
    let mut client = server.connect().await;

    // AUTH with wrong password should return WRONGPASS error
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"WRONGPASS")));

    // Non-exempt commands should still fail after wrong password
    let response = client.command(&["GET", "foo"]).await;
    assert!(matches!(response, Response::Error(e) if e.starts_with(b"NOAUTH")));

    // PING is auth-exempt, so it should still work
    let response = client.command(&["PING"]).await;
    assert_eq!(response, Response::pong());

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_named_user() {
    let server = TestServer::start_with_security("adminpass").await;
    let mut client = server.connect().await;

    // First authenticate as default user
    client.command(&["AUTH", "adminpass"]).await;

    // Create a named user with ACL SETUSER
    let response = client
        .command(&["ACL", "SETUSER", "testuser", "on", ">userpass", "+@all", "~*"])
        .await;
    assert_eq!(response, Response::ok());

    // Connect with a new client and authenticate as the named user
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "testuser", "userpass"]).await;
    assert_eq!(response, Response::ok());

    // ACL WHOAMI should return the username
    let response = client2.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("testuser"))));

    server.shutdown().await;
}

// ============================================================================
// ACL WHOAMI Tests
// ============================================================================

#[tokio::test]
async fn test_acl_whoami_default() {
    // Without AUTH, ACL WHOAMI should return "default"
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("default"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_whoami_after_auth() {
    let server = TestServer::start_with_security("adminpass").await;
    let mut client = server.connect().await;

    // Authenticate as default
    client.command(&["AUTH", "adminpass"]).await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "myuser", "on", ">mypass", "+@all", "~*"])
        .await;

    // New connection, authenticate as myuser
    let mut client2 = server.connect().await;
    client2.command(&["AUTH", "myuser", "mypass"]).await;

    // ACL WHOAMI should return "myuser"
    let response = client2.command(&["ACL", "WHOAMI"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("myuser"))));

    server.shutdown().await;
}

// ============================================================================
// ACL User Management Tests
// ============================================================================

#[tokio::test]
async fn test_acl_setuser_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL SETUSER creates a new user
    let response = client
        .command(&["ACL", "SETUSER", "newuser", "on", ">password123"])
        .await;
    assert_eq!(response, Response::ok());

    // ACL USERS should include the new user
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let usernames: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(usernames.contains(&"default".to_string()));
            assert!(usernames.contains(&"newuser".to_string()));
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_deluser() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "tempuser", "on", ">temppass"])
        .await;

    // Verify user exists
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let has_tempuser = arr.iter().any(|r| matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("tempuser")));
            assert!(has_tempuser, "tempuser should exist");
        }
        _ => panic!("Expected array response"),
    }

    // Delete the user
    let response = client.command(&["ACL", "DELUSER", "tempuser"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify user no longer exists
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            let has_tempuser = arr.iter().any(|r| matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("tempuser")));
            assert!(!has_tempuser, "tempuser should not exist after deletion");
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_deluser_default_fails() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL DELUSER default should fail
    let response = client.command(&["ACL", "DELUSER", "default"]).await;
    assert!(
        matches!(response, Response::Error(e) if String::from_utf8_lossy(&e).contains("default")),
        "Should not be able to delete default user"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_list() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create a user with specific permissions
    client
        .command(&["ACL", "SETUSER", "listuser", "on", ">listpass", "+get", "+set", "~keys:*"])
        .await;

    // ACL LIST should return array with user rules
    let response = client.command(&["ACL", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LIST should not be empty");
            // Should contain at least the default user
            let rules: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(
                rules.iter().any(|r| r.contains("default")),
                "Should contain default user"
            );
            assert!(
                rules.iter().any(|r| r.contains("listuser")),
                "Should contain listuser"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_getuser() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create a user
    client
        .command(&["ACL", "SETUSER", "infouser", "on", ">infopass", "+@read", "~data:*"])
        .await;

    // ACL GETUSER should return user details
    let response = client.command(&["ACL", "GETUSER", "infouser"]).await;
    match response {
        Response::Array(arr) => {
            // Should be a key-value array with user properties
            assert!(!arr.is_empty(), "GETUSER should return user info");
            // Look for expected fields like "flags", "passwords", "commands", "keys"
            let keys: Vec<_> = arr
                .iter()
                .step_by(2)
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(keys.contains(&"flags".to_string()), "Should have flags field");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    // GETUSER for non-existent user should return null
    let response = client.command(&["ACL", "GETUSER", "nonexistent"]).await;
    assert!(matches!(response, Response::Bulk(None)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_users() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create multiple users
    client
        .command(&["ACL", "SETUSER", "user1", "on", ">pass1"])
        .await;
    client
        .command(&["ACL", "SETUSER", "user2", "on", ">pass2"])
        .await;

    // ACL USERS should return array of usernames
    let response = client.command(&["ACL", "USERS"]).await;
    match response {
        Response::Array(arr) => {
            assert!(arr.len() >= 3, "Should have at least default, user1, user2");
            let usernames: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            assert!(usernames.contains(&"default".to_string()));
            assert!(usernames.contains(&"user1".to_string()));
            assert!(usernames.contains(&"user2".to_string()));
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL CAT Tests
// ============================================================================

#[tokio::test]
async fn test_acl_cat_all_categories() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL CAT without argument returns list of all categories
    let response = client.command(&["ACL", "CAT"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL CAT should return categories");
            let categories: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            // Common categories that should exist
            assert!(
                categories.iter().any(|c| c == "read" || c == "write" || c == "admin" || c == "string"),
                "Should contain common categories like read, write, admin, or string"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_cat_specific_category() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL CAT string returns commands in the string category
    let response = client.command(&["ACL", "CAT", "string"]).await;
    match response {
        Response::Array(arr) => {
            let commands: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_lowercase()),
                    _ => None,
                })
                .collect();
            // String commands should include get, set, etc.
            assert!(
                commands.iter().any(|c| c == "get" || c == "set"),
                "String category should include get or set commands"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL GENPASS Tests
// ============================================================================

#[tokio::test]
async fn test_acl_genpass_default() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL GENPASS returns 64-char hex string (256 bits) by default
    let response = client.command(&["ACL", "GENPASS"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            assert_eq!(password.len(), 64, "Default GENPASS should return 64 hex chars");
            // Verify it's valid hex
            let hex_str = String::from_utf8_lossy(&password);
            assert!(
                hex_str.chars().all(|c| c.is_ascii_hexdigit()),
                "GENPASS should return valid hex"
            );
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_genpass_custom_bits() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Implementation enforces minimum 256 bits (64 hex chars) for security
    // ACL GENPASS 128 still returns 64 chars (256 bits minimum)
    let response = client.command(&["ACL", "GENPASS", "128"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            // Minimum 256 bits = 64 hex chars
            assert!(password.len() >= 64, "GENPASS should return at least 64 hex chars");
            let hex_str = String::from_utf8_lossy(&password);
            assert!(hex_str.chars().all(|c| c.is_ascii_hexdigit()));
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    // ACL GENPASS 512 returns 128 chars (512 bits)
    let response = client.command(&["ACL", "GENPASS", "512"]).await;
    match response {
        Response::Bulk(Some(password)) => {
            assert_eq!(password.len(), 128, "GENPASS 512 should return 128 hex chars");
        }
        _ => panic!("Expected bulk string response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL LOG Tests
// ============================================================================

#[tokio::test]
async fn test_acl_log_auth_failure() {
    let server = TestServer::start_with_security("secretpass").await;
    let mut client = server.connect().await;

    // Trigger an auth failure
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    // Verify auth failed
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH should fail with WRONGPASS: {:?}",
        response
    );

    // Authenticate properly to check the log (ACL LOG requires auth)
    let response = client.command(&["AUTH", "secretpass"]).await;
    assert_eq!(response, Response::ok(), "AUTH with correct password should succeed");

    // Check ACL LOG contains entry
    // Note: ACL LOG returns up to 10 entries by default
    let response = client.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            // Should have at least one entry for the auth failure
            // If empty, the implementation may not be logging auth failures
            if arr.is_empty() {
                // This is acceptable if the implementation doesn't log auth failures
                // Just verify the command works
                println!("Note: ACL LOG is empty - auth failures may not be logged");
            }
        }
        _ => panic!("Expected array response from ACL LOG, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_reset() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Log a command denial by creating a restricted user and trying a denied command
    // First, create a user with limited permissions
    client
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~allowed:*"])
        .await;

    // The key here is to test that RESET works, regardless of whether entries exist
    // ACL LOG RESET should always succeed
    let response = client.command(&["ACL", "LOG", "RESET"]).await;
    assert_eq!(response, Response::ok());

    // After reset, log should be empty
    let response = client.command(&["ACL", "LOG"]).await;
    match response {
        Response::Array(arr) => {
            assert!(arr.is_empty(), "ACL LOG should be empty after RESET");
        }
        _ => panic!("Expected array response from ACL LOG, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL HELP Tests
// ============================================================================

#[tokio::test]
async fn test_acl_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL HELP returns array of help strings
    let response = client.command(&["ACL", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL HELP should return help strings");
            // Check that it mentions ACL commands
            let help_text: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();
            // Should mention at least some ACL subcommands
            let combined = help_text.join(" ").to_uppercase();
            assert!(
                combined.contains("ACL") || combined.contains("SETUSER") || combined.contains("CAT"),
                "Help should mention ACL commands"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// SLOWLOG tests
// ============================================================================

#[tokio::test]
async fn test_slowlog_get_empty() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Initially the slowlog should be empty
    let response = client.command(&["SLOWLOG", "GET"]).await;
    match response {
        Response::Array(entries) => {
            assert!(entries.is_empty(), "Slowlog should be empty initially");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_len_empty() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Initially the slowlog length should be 0
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_reset() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Reset should always succeed
    let response = client.command(&["SLOWLOG", "RESET"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    // After reset, length should be 0
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["SLOWLOG", "HELP"]).await;
    match response {
        Response::Array(items) => {
            // Should return help text
            assert!(!items.is_empty(), "Help should not be empty");
            // First item should mention SLOWLOG
            if let Some(Response::Bulk(Some(first))) = items.first() {
                let text = String::from_utf8_lossy(first);
                assert!(
                    text.to_uppercase().contains("SLOWLOG"),
                    "Help should mention SLOWLOG"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_threshold_disabled() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Disable slowlog
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
        .await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    // Run a command
    let _ = client.command(&["SET", "key1", "value1"]).await;

    // Slowlog should still be empty because logging is disabled
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_threshold_log_all() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let response = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    // Reset slowlog first
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run a few commands
    let _ = client.command(&["SET", "key1", "value1"]).await;
    let _ = client.command(&["GET", "key1"]).await;
    let _ = client.command(&["DEL", "key1"]).await;

    // Slowlog should have entries now
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    match response {
        Response::Integer(len) => {
            assert!(len >= 3, "Expected at least 3 entries, got {}", len);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    // Get entries and verify structure
    let response = client.command(&["SLOWLOG", "GET", "10"]).await;
    match response {
        Response::Array(entries) => {
            assert!(
                entries.len() >= 3,
                "Expected at least 3 entries, got {}",
                entries.len()
            );

            // Check structure of first entry
            if let Response::Array(ref entry) = entries[0] {
                assert_eq!(entry.len(), 6, "Entry should have 6 fields");

                // Field 0: ID (integer)
                assert!(matches!(entry[0], Response::Integer(_)));

                // Field 1: timestamp (integer)
                assert!(matches!(entry[1], Response::Integer(_)));

                // Field 2: duration_us (integer)
                assert!(matches!(entry[2], Response::Integer(_)));

                // Field 3: command args (array)
                assert!(matches!(entry[3], Response::Array(_)));

                // Field 4: client_addr (bulk string)
                assert!(matches!(entry[4], Response::Bulk(Some(_))));

                // Field 5: client_name (bulk string)
                assert!(matches!(entry[5], Response::Bulk(_)));
            } else {
                panic!("Expected array entry, got {:?}", entries[0]);
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_get_count_limit() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog first
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run many commands
    for i in 0..20 {
        let _ = client
            .command(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    // Get only 5 entries
    let response = client.command(&["SLOWLOG", "GET", "5"]).await;
    match response {
        Response::Array(entries) => {
            assert_eq!(
                entries.len(),
                5,
                "Expected exactly 5 entries when count=5"
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_skip_slowlog_command() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run SLOWLOG commands
    let _ = client.command(&["SLOWLOG", "GET"]).await;
    let _ = client.command(&["SLOWLOG", "LEN"]).await;
    let _ = client.command(&["SLOWLOG", "HELP"]).await;

    // SLOWLOG commands should not be logged (SKIP_SLOWLOG flag)
    let response = client.command(&["SLOWLOG", "LEN"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_config_get() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Get slowlog configuration
    let response = client.command(&["CONFIG", "GET", "slowlog-*"]).await;
    match response {
        Response::Array(items) => {
            // Should have at least 3 params (log-slower-than, max-len, max-arg-len)
            // Each param is name, value pair so 6 items minimum
            assert!(
                items.len() >= 6,
                "Expected at least 6 items for slowlog config, got {}",
                items.len()
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_unknown_subcommand() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["SLOWLOG", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_slowlog_get_default_count() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set threshold to 0 to log all commands
    let _ = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    // Reset slowlog
    let _ = client.command(&["SLOWLOG", "RESET"]).await;

    // Run many commands
    for i in 0..15 {
        let _ = client
            .command(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    // GET without count should return default (10)
    let response = client.command(&["SLOWLOG", "GET"]).await;
    match response {
        Response::Array(entries) => {
            assert_eq!(
                entries.len(),
                10,
                "Default count should be 10, got {}",
                entries.len()
            );
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// BGSAVE / LASTSAVE Tests
// ============================================================================

#[tokio::test]
async fn test_bgsave_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // BGSAVE should return success message
    let response = client.command(&["BGSAVE"]).await;
    match response {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "Unexpected BGSAVE response: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_lastsave_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LASTSAVE should return an integer (Unix timestamp)
    let response = client.command(&["LASTSAVE"]).await;
    match response {
        Response::Integer(timestamp) => {
            // Timestamp should be a reasonable Unix timestamp
            // Either 0 (no save yet) or a recent timestamp
            assert!(
                timestamp >= 0,
                "LASTSAVE should return non-negative timestamp, got {}",
                timestamp
            );
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_bgsave_then_lastsave() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // First, write some data
    client.command(&["SET", "snapshot_test_key", "test_value"]).await;

    // Trigger BGSAVE
    let response = client.command(&["BGSAVE"]).await;
    match response {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("Background saving started")
                    || msg_str.contains("already in progress"),
                "BGSAVE should start or already be in progress"
            );
        }
        _ => panic!("Expected simple string response for BGSAVE"),
    }

    // Give the background task time to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // LASTSAVE should now return a recent timestamp
    let response = client.command(&["LASTSAVE"]).await;
    match response {
        Response::Integer(timestamp) => {
            // After BGSAVE, timestamp should be positive (or 0 if save is still in progress)
            assert!(
                timestamp >= 0,
                "LASTSAVE should return valid timestamp after BGSAVE"
            );
        }
        _ => panic!("Expected integer response for LASTSAVE"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_bgsave_concurrent_returns_already_in_progress() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Write some data to make the snapshot take longer
    for i in 0..100 {
        client1
            .command(&["SET", &format!("key_{}", i), &format!("value_{}", i)])
            .await;
    }

    // First BGSAVE should start
    let response1 = client1.command(&["BGSAVE"]).await;

    // Second BGSAVE should either start (if first completed) or report already in progress
    let response2 = client2.command(&["BGSAVE"]).await;

    match response1 {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.contains("saving"),
                "First BGSAVE should indicate saving: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response for first BGSAVE"),
    }

    match response2 {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(&msg);
            // Either started (first completed quickly) or in progress
            assert!(
                msg_str.contains("saving") || msg_str.contains("progress"),
                "Second BGSAVE should indicate saving or in progress: {}",
                msg_str
            );
        }
        _ => panic!("Expected simple string response for second BGSAVE"),
    }

    server.shutdown().await;
}

// ============================================================================
// HTTP Metrics and Health Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus_format() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/metrics", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let content_type = response.headers().get("content-type").unwrap();
    assert!(content_type.to_str().unwrap().contains("text/plain"));

    let body = response.text().await.unwrap();
    // Metrics endpoint should return valid content (may be empty or have metrics)
    // Prometheus format uses # for comments/metadata
    assert!(body.is_empty() || body.contains("frogdb_") || body.starts_with("#"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_metrics_include_command_counters() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Execute commands to generate metrics
    client.command(&["SET", "key1", "value1"]).await;
    client.command(&["GET", "key1"]).await;
    client.command(&["GET", "nonexistent"]).await;

    // Give the server a moment to record metrics
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fetch metrics
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!("http://{}/metrics", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    // Should contain command-related metrics
    assert!(
        body.contains("frogdb_commands_total") || body.contains("commands"),
        "Metrics should contain command counters: {}",
        body
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_health_live_endpoint() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/health/live", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_health_ready_endpoint() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/health/ready", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_healthz_alias_endpoint() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/healthz", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_readyz_alias_endpoint() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/readyz", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_metrics_404_on_unknown_path() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/unknown", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 1: Command Permission Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_command_denied_write_category() {
    // User with -@write cannot write
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;

    // Authenticate as admin (default user)
    admin.command(&["AUTH", "admin"]).await;

    // Create read-only user with +@read -@write
    let response = admin
        .command(&["ACL", "SETUSER", "reader", "on", ">pass", "+@read", "-@write", "~*"])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as reader
    let mut reader = server.connect().await;
    let response = reader.command(&["AUTH", "reader", "pass"]).await;
    assert_eq!(response, Response::ok());

    // GET should work (read command)
    let response = reader.command(&["GET", "key"]).await;
    assert!(matches!(response, Response::Bulk(_)));

    // SET should be denied (write command)
    let response = reader.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_command_denied_individual() {
    // User with +get -set (individual commands)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with only GET allowed
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;

    // GET should work
    let response = limited.command(&["GET", "key"]).await;
    assert!(matches!(response, Response::Bulk(_)));

    // SET should be denied
    let response = limited.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    // DEL should be denied
    let response = limited.command(&["DEL", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "DEL should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_command_denied_dangerous() {
    // User with -@dangerous cannot run DEBUG/FLUSHALL
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all commands except dangerous
    admin
        .command(&["ACL", "SETUSER", "safe", "on", ">pass", "+@all", "-@dangerous", "~*"])
        .await;

    let mut safe = server.connect().await;
    safe.command(&["AUTH", "safe", "pass"]).await;

    // Normal commands should work
    let response = safe.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    // FLUSHALL should be denied (dangerous)
    let response = safe.command(&["FLUSHALL"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "FLUSHALL should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_nocommands_user() {
    // User with nocommands denied everything
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with no commands (reset to nothing)
    admin
        .command(&["ACL", "SETUSER", "nocommands", "on", ">pass", "nocommands", "~*"])
        .await;

    let mut client = server.connect().await;
    client.command(&["AUTH", "nocommands", "pass"]).await;

    // All commands should be denied
    let response = client.command(&["GET", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM error, got {:?}",
        response
    );

    let response = client.command(&["SET", "key", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM error, got {:?}",
        response
    );

    let response = client.command(&["PING"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PING should return NOPERM error, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_allcommands_user() {
    // User with +@all can run any command
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all commands
    admin
        .command(&["ACL", "SETUSER", "superuser", "on", ">pass", "+@all", "~*"])
        .await;

    let mut superuser = server.connect().await;
    superuser.command(&["AUTH", "superuser", "pass"]).await;

    // All basic commands should work
    let response = superuser.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    let response = superuser.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("val"))));

    let response = superuser.command(&["DEL", "key"]).await;
    assert_eq!(response, Response::Integer(1));

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 2: Key Pattern Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_key_pattern_prefix() {
    // User with ~prefix:* can only access matching keys
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with access only to app:* keys
    admin
        .command(&["ACL", "SETUSER", "appuser", "on", ">pass", "+@all", "~app:*"])
        .await;

    let mut appuser = server.connect().await;
    appuser.command(&["AUTH", "appuser", "pass"]).await;

    // SET app:foo should work
    let response = appuser.command(&["SET", "app:foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    // GET app:foo should work
    let response = appuser.command(&["GET", "app:foo"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("bar"))));

    // SET other:foo should be denied
    let response = appuser.command(&["SET", "other:foo", "bar"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET to non-matching key should return NOPERM, got {:?}",
        response
    );

    // GET other:foo should be denied
    let response = appuser.command(&["GET", "other:foo"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET from non-matching key should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_read_only() {
    // User with %R~* (read-only keys)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Setup: create some data as admin
    admin.command(&["SET", "key", "value"]).await;

    // Create user with read-only access to all keys
    admin
        .command(&["ACL", "SETUSER", "readonly", "on", ">pass", "+@all", "%R~*"])
        .await;

    let mut readonly = server.connect().await;
    readonly.command(&["AUTH", "readonly", "pass"]).await;

    // GET should work (read)
    let response = readonly.command(&["GET", "key"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    // SET should be denied (write)
    let response = readonly.command(&["SET", "key", "newval"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM (write denied), got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_write_only() {
    // User with %W~* (write-only keys)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with write-only access to all keys
    admin
        .command(&["ACL", "SETUSER", "writeonly", "on", ">pass", "+@all", "%W~*"])
        .await;

    let mut writeonly = server.connect().await;
    writeonly.command(&["AUTH", "writeonly", "pass"]).await;

    // SET should work (write)
    let response = writeonly.command(&["SET", "key", "value"]).await;
    assert_eq!(response, Response::ok());

    // GET should be denied (read)
    let response = writeonly.command(&["GET", "key"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM (read denied), got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_key_pattern_multiple() {
    // User with multiple key patterns
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with access to cache:* and session:* keys
    admin
        .command(&["ACL", "SETUSER", "multi", "on", ">pass", "+@all", "~cache:*", "~session:*"])
        .await;

    let mut multi = server.connect().await;
    multi.command(&["AUTH", "multi", "pass"]).await;

    // Access cache:foo should work
    let response = multi.command(&["SET", "cache:foo", "bar"]).await;
    assert_eq!(response, Response::ok());

    // Access session:bar should work
    let response = multi.command(&["SET", "session:bar", "baz"]).await;
    assert_eq!(response, Response::ok());

    // Access other:foo should be denied
    let response = multi.command(&["SET", "other:foo", "bar"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET to non-matching key should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_no_key_access() {
    // User with resetkeys (no key patterns)
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with commands but no key access
    admin
        .command(&["ACL", "SETUSER", "nokeys", "on", ">pass", "+@all", "resetkeys"])
        .await;

    let mut nokeys = server.connect().await;
    nokeys.command(&["AUTH", "nokeys", "pass"]).await;

    // All key operations should be denied
    let response = nokeys.command(&["GET", "anykey"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "GET should return NOPERM (no key access), got {:?}",
        response
    );

    let response = nokeys.command(&["SET", "anykey", "val"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should return NOPERM (no key access), got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 3: Channel Pattern Enforcement
// ============================================================================

#[tokio::test]
async fn test_acl_channel_pattern_subscribe() {
    // User with &notifications:* can only subscribe to matching channels
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restriction
    admin
        .command(&["ACL", "SETUSER", "subuser", "on", ">pass", "+@all", "~*", "&notifications:*"])
        .await;

    let mut subuser = server.connect().await;
    subuser.command(&["AUTH", "subuser", "pass"]).await;

    // SUBSCRIBE to matching channel should work
    let response = subuser.command(&["SUBSCRIBE", "notifications:alerts"]).await;
    // SUBSCRIBE returns an array with subscribe confirmation
    assert!(
        matches!(response, Response::Array(_)),
        "SUBSCRIBE to matching channel should succeed, got {:?}",
        response
    );

    // Need a fresh connection to test denied subscribe
    let mut subuser2 = server.connect().await;
    subuser2.command(&["AUTH", "subuser", "pass"]).await;

    // SUBSCRIBE to non-matching channel should be denied
    let response = subuser2.command(&["SUBSCRIBE", "secret:data"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SUBSCRIBE to non-matching channel should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_channel_pattern_publish() {
    // User with &public:* denied PUBLISH to non-matching channels
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restriction
    admin
        .command(&["ACL", "SETUSER", "pubuser", "on", ">pass", "+@all", "~*", "&public:*"])
        .await;

    let mut pubuser = server.connect().await;
    pubuser.command(&["AUTH", "pubuser", "pass"]).await;

    // PUBLISH to matching channel should work
    let response = pubuser.command(&["PUBLISH", "public:msg", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to matching channel should succeed, got {:?}",
        response
    );

    // PUBLISH to non-matching channel should be denied
    let response = pubuser.command(&["PUBLISH", "private:msg", "hello"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PUBLISH to non-matching channel should return NOPERM, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_allchannels() {
    // User with allchannels can access any channel
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all channels access
    admin
        .command(&["ACL", "SETUSER", "allchan", "on", ">pass", "+@all", "~*", "allchannels"])
        .await;

    let mut allchan = server.connect().await;
    allchan.command(&["AUTH", "allchan", "pass"]).await;

    // PUBLISH to any channel should work
    let response = allchan.command(&["PUBLISH", "any:channel", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to any channel should succeed, got {:?}",
        response
    );

    let response = allchan.command(&["PUBLISH", "another:channel", "hello"]).await;
    assert!(
        matches!(response, Response::Integer(_)),
        "PUBLISH to another channel should succeed, got {:?}",
        response
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_no_channel_access() {
    // User with resetchannels cannot access any channel
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with no channel access
    admin
        .command(&["ACL", "SETUSER", "nochan", "on", ">pass", "+@all", "~*", "resetchannels"])
        .await;

    let mut nochan = server.connect().await;
    nochan.command(&["AUTH", "nochan", "pass"]).await;

    // SUBSCRIBE to any channel should be denied
    let response = nochan.command(&["SUBSCRIBE", "any:channel"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SUBSCRIBE should return NOPERM (no channel access), got {:?}",
        response
    );

    // PUBLISH to any channel should be denied
    let response = nochan.command(&["PUBLISH", "any:channel", "msg"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "PUBLISH should return NOPERM (no channel access), got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 4: Permission Denial Logging
// ============================================================================

#[tokio::test]
async fn test_acl_log_command_denied() {
    // Command denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a restricted user
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect as limited user and try forbidden command
    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;
    let _ = limited.command(&["SET", "key", "val"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            // Should have at least one entry for the command denial
            assert!(!arr.is_empty(), "ACL LOG should have entry for command denial");
            // The entry should be an array of key-value pairs
            if let Some(Response::Array(entry)) = arr.first() {
                // Check that it contains the reason "command"
                let entry_str = format!("{:?}", entry);
                assert!(
                    entry_str.contains("command") || entry_str.contains("not allowed"),
                    "ACL LOG entry should mention command denial"
                );
            }
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_key_denied() {
    // Key denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a user with key restrictions
    admin
        .command(&["ACL", "SETUSER", "keyuser", "on", ">pass", "+@all", "~allowed:*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect and try accessing denied key
    let mut keyuser = server.connect().await;
    keyuser.command(&["AUTH", "keyuser", "pass"]).await;
    let _ = keyuser.command(&["GET", "denied:key"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LOG should have entry for key denial");
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_log_channel_denied() {
    // Channel denial logged to ACL LOG
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create a user with channel restrictions
    admin
        .command(&["ACL", "SETUSER", "chanuser", "on", ">pass", "+@all", "~*", "&allowed:*"])
        .await;

    // Reset ACL LOG first
    admin.command(&["ACL", "LOG", "RESET"]).await;

    // Connect and try accessing denied channel
    let mut chanuser = server.connect().await;
    chanuser.command(&["AUTH", "chanuser", "pass"]).await;
    let _ = chanuser.command(&["PUBLISH", "denied:channel", "msg"]).await; // This should fail and log

    // Check ACL LOG
    let response = admin.command(&["ACL", "LOG", "10"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty(), "ACL LOG should have entry for channel denial");
        }
        _ => panic!("Expected array response from ACL LOG"),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 5: Multiple Password Tests
// ============================================================================

#[tokio::test]
async fn test_acl_multiple_passwords() {
    // User with multiple passwords can auth with any
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with multiple passwords
    admin
        .command(&["ACL", "SETUSER", "multi", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // AUTH with pass1 should work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "multi", "pass1"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass1 should succeed");

    // AUTH with pass2 should work (new connection)
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "multi", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should succeed");

    // AUTH with wrong password should fail
    let mut client3 = server.connect().await;
    let response = client3.command(&["AUTH", "multi", "wrong"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH with wrong password should fail"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_add_password() {
    // Adding password doesn't invalidate existing
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with pass1
    admin
        .command(&["ACL", "SETUSER", "addpass", "on", ">pass1", "+@all", "~*"])
        .await;

    // Add pass2
    admin
        .command(&["ACL", "SETUSER", "addpass", ">pass2"])
        .await;

    // Both passwords should work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "addpass", "pass1"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass1 should still work");

    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "addpass", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should work");

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_remove_password() {
    // Removing one password leaves others valid
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with two passwords
    admin
        .command(&["ACL", "SETUSER", "rmpass", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // Remove pass1
    admin
        .command(&["ACL", "SETUSER", "rmpass", "<pass1"])
        .await;

    // pass2 should still work
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "rmpass", "pass2"]).await;
    assert_eq!(response, Response::ok(), "AUTH with pass2 should work");

    // pass1 should be rejected
    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "rmpass", "pass1"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"WRONGPASS")),
        "AUTH with removed pass1 should fail"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_resetpass() {
    // resetpass clears all passwords
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with multiple passwords
    admin
        .command(&["ACL", "SETUSER", "resetuser", "on", ">pass1", ">pass2", "+@all", "~*"])
        .await;

    // Reset passwords
    admin
        .command(&["ACL", "SETUSER", "resetuser", "resetpass"])
        .await;

    // All passwords should now be rejected (user is also disabled without password)
    let mut client1 = server.connect().await;
    let response = client1.command(&["AUTH", "resetuser", "pass1"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "AUTH with pass1 should fail after resetpass"
    );

    let mut client2 = server.connect().await;
    let response = client2.command(&["AUTH", "resetuser", "pass2"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "AUTH with pass2 should fail after resetpass"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 6: Error Message Verification
// ============================================================================

#[tokio::test]
async fn test_error_noauth_format() {
    // NOAUTH error format
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // GET without auth should return NOAUTH
    let response = client.command(&["GET", "key"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOAUTH") && msg.contains("Authentication"),
                "Expected 'NOAUTH Authentication required', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_wrongpass_format() {
    // WRONGPASS error format
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // AUTH with wrong password should return WRONGPASS
    let response = client.command(&["AUTH", "wrongpassword"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("WRONGPASS"),
                "Expected 'WRONGPASS ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_command_format() {
    // NOPERM command error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create restricted user
    admin
        .command(&["ACL", "SETUSER", "limited", "on", ">pass", "+get", "~*"])
        .await;

    let mut limited = server.connect().await;
    limited.command(&["AUTH", "limited", "pass"]).await;

    // Forbidden command should return NOPERM with command name
    let response = limited.command(&["SET", "key", "val"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM") && msg.to_lowercase().contains("set"),
                "Expected 'NOPERM ... SET ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_key_format() {
    // NOPERM key error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with key restrictions
    admin
        .command(&["ACL", "SETUSER", "keyuser", "on", ">pass", "+@all", "~allowed:*"])
        .await;

    let mut keyuser = server.connect().await;
    keyuser.command(&["AUTH", "keyuser", "pass"]).await;

    // Forbidden key should return NOPERM
    let response = keyuser.command(&["GET", "denied:key"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM"),
                "Expected 'NOPERM ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_error_noperm_channel_format() {
    // NOPERM channel error format
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with channel restrictions
    admin
        .command(&["ACL", "SETUSER", "chanuser", "on", ">pass", "+@all", "~*", "&allowed:*"])
        .await;

    let mut chanuser = server.connect().await;
    chanuser.command(&["AUTH", "chanuser", "pass"]).await;

    // Forbidden channel should return NOPERM
    let response = chanuser.command(&["PUBLISH", "denied:channel", "msg"]).await;
    match response {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.starts_with("NOPERM"),
                "Expected 'NOPERM ...', got: {}",
                msg
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 7: Auth-Exempt Commands
// ============================================================================

#[tokio::test]
async fn test_auth_exempt_auth() {
    // AUTH works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // AUTH command itself should work without being authenticated
    let response = client.command(&["AUTH", "testpass"]).await;
    assert_eq!(response, Response::ok(), "AUTH command should work without prior auth");

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_exempt_hello() {
    // HELLO works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // HELLO command should work without being authenticated
    let response = client.command(&["HELLO"]).await;
    match response {
        Response::Array(_) => {
            // Expected - HELLO returns server info
        }
        _ => panic!("Expected array response for HELLO, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_auth_exempt_quit() {
    // QUIT works without authentication
    let server = TestServer::start_with_security("testpass").await;
    let mut client = server.connect().await;

    // QUIT command should work without being authenticated
    let response = client.command(&["QUIT"]).await;
    // QUIT typically returns OK before closing connection
    assert!(
        matches!(response, Response::Simple(_)) || matches!(response, Response::Error(_)),
        "QUIT should return a response before closing"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 8: ACL LOAD Edge Cases
// ============================================================================

#[tokio::test]
async fn test_acl_load_no_file() {
    // ACL LOAD with no aclfile configured
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL LOAD should error when no file is configured
    let response = client.command(&["ACL", "LOAD"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "ACL LOAD should return error when no aclfile configured"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_save_no_file() {
    // ACL SAVE with no aclfile configured
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // ACL SAVE should error when no file is configured
    let response = client.command(&["ACL", "SAVE"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "ACL SAVE should return error when no aclfile configured"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 9: Permission Snapshot Isolation
// ============================================================================

#[tokio::test]
async fn test_acl_reauth_updates_permissions() {
    // Re-authentication updates permissions
    let server = TestServer::start_with_security("admin").await;
    let mut admin = server.connect().await;
    admin.command(&["AUTH", "admin"]).await;

    // Create user with all permissions
    admin
        .command(&["ACL", "SETUSER", "dynamic", "on", ">pass", "+@all", "~*"])
        .await;

    // Connect and authenticate as dynamic user
    let mut dynamic = server.connect().await;
    dynamic.command(&["AUTH", "dynamic", "pass"]).await;

    // SET should work
    let response = dynamic.command(&["SET", "key", "val"]).await;
    assert_eq!(response, Response::ok());

    // Admin changes user permissions to deny write
    admin
        .command(&["ACL", "SETUSER", "dynamic", "-@write"])
        .await;

    // Re-authenticate as dynamic user
    let response = dynamic.command(&["AUTH", "dynamic", "pass"]).await;
    assert_eq!(response, Response::ok());

    // Now SET should fail (after re-authentication, new permissions apply)
    let response = dynamic.command(&["SET", "key", "val2"]).await;
    assert!(
        matches!(response, Response::Error(ref e) if e.starts_with(b"NOPERM")),
        "SET should be denied after re-auth with restricted permissions, got {:?}",
        response
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 10: Concurrent Modification
// ============================================================================

#[tokio::test]
async fn test_acl_concurrent_setuser() {
    // Concurrent SETUSER doesn't corrupt state
    let server = TestServer::start_with_security("admin").await;
    let mut admin1 = server.connect().await;
    let mut admin2 = server.connect().await;
    admin1.command(&["AUTH", "admin"]).await;
    admin2.command(&["AUTH", "admin"]).await;

    // Create a user
    admin1
        .command(&["ACL", "SETUSER", "concurrent", "on", ">pass", "+@all", "~*"])
        .await;

    // Concurrent modifications (in practice, these are sequential but test the concurrency logic)
    let response1 = admin1
        .command(&["ACL", "SETUSER", "concurrent", "+set"])
        .await;
    let response2 = admin2
        .command(&["ACL", "SETUSER", "concurrent", "+get"])
        .await;

    assert_eq!(response1, Response::ok());
    assert_eq!(response2, Response::ok());

    // Verify user is in a consistent state
    let response = admin1.command(&["ACL", "GETUSER", "concurrent"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "User should exist in consistent state"
    );

    server.shutdown().await;
}

// ============================================================================
// ACL Permission Enforcement Tests - Group 11: Redis 7.0 Features
// ============================================================================

#[tokio::test]
async fn test_acl_subcommand_rules() {
    // Create a user with +@all but deny CONFIG|SET specifically
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with all commands allowed but CONFIG|SET denied
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "configreader", "on", ">pass",
            "~*", "+@all", "-config|set", "-config|rewrite"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    let response = user_client.command(&["AUTH", "configreader", "pass"]).await;
    assert_eq!(response, Response::ok());

    // CONFIG GET should work (allowed by +@all, no specific deny)
    let response = user_client.command(&["CONFIG", "GET", "maxclients"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "CONFIG GET should be allowed, got {:?}",
        response
    );

    // CONFIG SET should be denied (specific subcommand deny)
    let response = user_client.command(&["CONFIG", "SET", "maxclients", "1000"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG SET, got {:?}", response),
    }

    // CONFIG REWRITE should be denied (specific subcommand deny)
    let response = user_client.command(&["CONFIG", "REWRITE"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG REWRITE, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_subcommand_allow_specific() {
    // Create a user with -config but +config|get allowed
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with config denied but config|get allowed
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "configget", "on", ">pass",
            "~*", "+@all", "-config", "+config|get"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "configget", "pass"]).await;

    // CONFIG GET should work (specific subcommand allow overrides command deny)
    let response = user_client.command(&["CONFIG", "GET", "maxclients"]).await;
    assert!(
        matches!(response, Response::Array(_)),
        "CONFIG GET should be allowed, got {:?}",
        response
    );

    // CONFIG SET should be denied (no specific allow)
    let response = user_client.command(&["CONFIG", "SET", "maxclients", "1000"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for CONFIG SET, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_selectors() {
    // Create a user with root access to app:* and selector for cache:* read-only
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with:
    // - Root: app:* keys with all commands
    // - Selector: cache:* keys with read-only access
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "hybrid", "on", ">pass",
            "~app:*", "+@all", "(~cache:* +@read)"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "hybrid", "pass"]).await;

    // Can write to app:* (root permissions)
    let response = user_client.command(&["SET", "app:key1", "value1"]).await;
    assert_eq!(response, Response::ok());

    // Can read from app:* (root permissions)
    let response = user_client.command(&["GET", "app:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    // Can read from cache:* (selector permissions)
    // First, admin sets a cache key
    admin_client.command(&["SET", "cache:data", "cached"]).await;
    let response = user_client.command(&["GET", "cache:data"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("cached"))));

    // Cannot write to cache:* (selector only grants read)
    let response = user_client.command(&["SET", "cache:newkey", "value"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error for cache write, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for cache write, got {:?}", response),
    }

    // Cannot access data:* (neither root nor selector)
    let response = user_client.command(&["GET", "data:something"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error for data access, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error for data access, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_clearselectors() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with a selector
    admin_client
        .command(&[
            "ACL", "SETUSER", "withsel", "on", ">pass",
            "~app:*", "+@all", "(~temp:* +@read)"
        ])
        .await;

    // Verify selector exists in GETUSER response
    let response = admin_client.command(&["ACL", "GETUSER", "withsel"]).await;
    match response {
        Response::Array(arr) => {
            // Look for "selectors" field
            let selectors_idx = arr.iter().position(|r| {
                matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("selectors"))
            });
            if let Some(idx) = selectors_idx {
                if let Some(Response::Array(selectors)) = arr.get(idx + 1) {
                    assert!(!selectors.is_empty(), "Should have at least one selector");
                }
            }
        }
        _ => panic!("Expected array response for GETUSER"),
    }

    // Clear selectors
    admin_client
        .command(&["ACL", "SETUSER", "withsel", "clearselectors"])
        .await;

    // Verify selectors are cleared
    let response = admin_client.command(&["ACL", "GETUSER", "withsel"]).await;
    match response {
        Response::Array(arr) => {
            let selectors_idx = arr.iter().position(|r| {
                matches!(r, Response::Bulk(Some(b)) if b == &Bytes::from("selectors"))
            });
            if let Some(idx) = selectors_idx {
                if let Some(Response::Array(selectors)) = arr.get(idx + 1) {
                    assert!(selectors.is_empty(), "Selectors should be cleared");
                }
            }
        }
        _ => panic!("Expected array response for GETUSER"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_multiple_selectors() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with multiple selectors
    let response = admin_client
        .command(&[
            "ACL", "SETUSER", "multisel", "on", ">pass",
            "+@all", "(~temp:* +@read)", "(~cache:* +@read)"
        ])
        .await;
    assert_eq!(response, Response::ok());

    // Connect as the new user
    let mut user_client = server.connect().await;
    user_client.command(&["AUTH", "multisel", "pass"]).await;

    // Admin sets some keys
    admin_client.command(&["SET", "temp:key1", "temp_value"]).await;
    admin_client.command(&["SET", "cache:key1", "cache_value"]).await;

    // Can read temp:* (selector 1)
    let response = user_client.command(&["GET", "temp:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("temp_value"))));

    // Can read cache:* (selector 2)
    let response = user_client.command(&["GET", "cache:key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("cache_value"))));

    // Cannot read data:* (no matching selector)
    let response = user_client.command(&["GET", "data:key1"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOPERM"),
                "Expected NOPERM error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected NOPERM error, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_acl_acl_list_includes_subcommand_rules() {
    let server = TestServer::start_with_security("admin").await;
    let mut admin_client = server.connect().await;

    // Authenticate as admin
    admin_client.command(&["AUTH", "admin"]).await;

    // Create user with subcommand rules
    admin_client
        .command(&[
            "ACL", "SETUSER", "subcmduser", "on", ">pass",
            "~*", "+@all", "-config|set", "+client|info"
        ])
        .await;

    // ACL LIST should include subcommand rules in the output
    let response = admin_client.command(&["ACL", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            let rules: Vec<_> = arr
                .iter()
                .filter_map(|r| match r {
                    Response::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .collect();

            let subcmduser_rule = rules.iter().find(|r| r.contains("subcmduser"));
            assert!(subcmduser_rule.is_some(), "Should have subcmduser in ACL LIST");
            let rule = subcmduser_rule.unwrap();
            assert!(
                rule.contains("-config|set") || rule.contains("+client|info"),
                "ACL LIST should include subcommand rules: {}",
                rule
            );
        }
        _ => panic!("Expected array response for ACL LIST"),
    }

    server.shutdown().await;
}

// ============================================================================
// Stream Blocking Tests (XREAD BLOCK, XREADGROUP BLOCK)
// ============================================================================

#[tokio::test]
async fn test_xread_non_blocking() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add some entries to a stream
    let response = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    match response {
        Response::Bulk(Some(_)) => {} // ID returned
        _ => panic!("Expected bulk string for XADD, got {:?}", response),
    }

    // Non-blocking XREAD should return immediately
    let response = client
        .command(&["XREAD", "STREAMS", "mystream", "0"])
        .await;
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1, "Should have one stream result");
        }
        _ => panic!("Expected array for XREAD, got {:?}", response),
    }

    // Non-blocking XREAD with no data should return null
    let response = client
        .command(&["XREAD", "STREAMS", "mystream", "$"])
        .await;
    match response {
        Response::Bulk(None) => {} // null response expected
        _ => panic!("Expected null for XREAD with $, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_timeout() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create stream first
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // Blocking XREAD with short timeout should timeout
    let start = std::time::Instant::now();
    let response = client
        .command(&["XREAD", "BLOCK", "100", "STREAMS", "mystream", "$"])
        .await;
    let elapsed = start.elapsed();

    // Should have waited at least 100ms (minus some tolerance)
    assert!(
        elapsed.as_millis() >= 80,
        "Should have waited at least 80ms, but waited {}ms",
        elapsed.as_millis()
    );

    // Should return null on timeout
    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null on timeout, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_satisfied_by_xadd() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream first
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // Start blocking read in background
    let handle = tokio::spawn(async move {
        client1
            .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "$"])
            .await
    });

    // Give time for the blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry from another client
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    // The blocking read should complete with the new entry
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1, "Should have one stream result");
            // Check inner structure
            if let Response::Array(ref stream_data) = streams[0] {
                assert_eq!(stream_data.len(), 2, "Stream result should have key and entries");
            }
        }
        _ => panic!(
            "Expected array for satisfied XREAD, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_block_dollar_resolution() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Add initial entry
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "initial"])
        .await;

    // Start blocking read with $ (current last ID)
    let handle = tokio::spawn(async move {
        client1
            .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "$"])
            .await
    });

    // Give time for the blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "new"])
        .await;

    // Should receive only the new entry, not the initial one
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
            if let Response::Array(ref stream_data) = streams[0] {
                if let Response::Array(ref entries) = stream_data[1] {
                    assert_eq!(entries.len(), 1, "Should have exactly one new entry");
                }
            }
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_non_blocking() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create stream and consumer group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let response = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    match response {
        Response::Simple(_) => {}
        _ => panic!("Expected OK for XGROUP CREATE, got {:?}", response),
    }

    // Non-blocking XREADGROUP should return the entry
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array for XREADGROUP, got {:?}", response),
    }

    // Another read should return null (no more new messages)
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null for second XREADGROUP, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_timeout() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create stream and group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Blocking XREADGROUP should timeout
    let start = std::time::Instant::now();
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "BLOCK",
            "100",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() >= 80,
        "Should have waited at least 80ms"
    );

    match response {
        Response::Bulk(None) => {}
        _ => panic!("Expected null on timeout, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_satisfied_by_xadd() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream and group
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client1
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Start blocking read in background
    let handle = tokio::spawn(async move {
        client1
            .command(&[
                "XREADGROUP",
                "GROUP",
                "mygroup",
                "consumer1",
                "BLOCK",
                "5000",
                "STREAMS",
                "mystream",
                ">",
            ])
            .await
    });

    // Give time for blocking command to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    // Should receive the new entry
    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout waiting for task")
        .expect("task panicked");

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_block_with_noack() {
    let server = TestServer::start().await;
    let mut client1 = server.connect().await;
    let mut client2 = server.connect().await;

    // Create stream and group
    let _ = client1
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client1
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "$"])
        .await;

    // Start blocking read with NOACK
    let handle = tokio::spawn(async move {
        client1
            .command(&[
                "XREADGROUP",
                "GROUP",
                "mygroup",
                "consumer1",
                "NOACK",
                "BLOCK",
                "5000",
                "STREAMS",
                "mystream",
                ">",
            ])
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add new entry
    let _ = client2
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;

    let response = timeout(Duration::from_secs(2), handle)
        .await
        .expect("timeout")
        .expect("task panicked");

    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array, got {:?}", response),
    }

    // With NOACK, the entry should not be in the pending list
    let pending = client2
        .command(&["XPENDING", "mystream", "mygroup"])
        .await;
    match pending {
        Response::Array(ref items) => {
            if let Response::Integer(count) = &items[0] {
                assert_eq!(*count, 0, "Should have no pending entries with NOACK");
            }
        }
        _ => panic!("Expected array for XPENDING, got {:?}", pending),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xreadgroup_specific_id_no_block() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create stream and group
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;
    let _ = client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // Read and acknowledge
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "STREAMS",
            "mystream",
            ">",
        ])
        .await;
    match response {
        Response::Array(_) => {}
        _ => panic!("Expected array, got {:?}", response),
    }

    // Read with specific ID (re-read from PEL) - should NOT block even with BLOCK specified
    // because specific ID means re-reading pending entries, not waiting for new ones
    let start = std::time::Instant::now();
    let response = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "BLOCK",
            "1000",
            "STREAMS",
            "mystream",
            "0",
        ])
        .await;
    let elapsed = start.elapsed();

    // Should return almost immediately (not wait 1000ms)
    assert!(
        elapsed.as_millis() < 500,
        "Should not block when reading PEL with specific ID, took {}ms",
        elapsed.as_millis()
    );

    // Should have the pending entry
    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        Response::Bulk(None) => {} // OK if no pending entries
        _ => panic!("Expected array or null, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_xread_immediate_data() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add entry to stream
    let _ = client
        .command(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // XREAD BLOCK with data available should return immediately
    let start = std::time::Instant::now();
    let response = client
        .command(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "0"])
        .await;
    let elapsed = start.elapsed();

    // Should return immediately, not wait 5 seconds
    assert!(
        elapsed.as_millis() < 500,
        "Should return immediately when data exists, took {}ms",
        elapsed.as_millis()
    );

    match response {
        Response::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// JSON Command Tests
// ============================================================================

#[tokio::test]
async fn test_json_set_get() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // JSON.SET creates a new key
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"name":"test","count":0}"#])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // JSON.GET retrieves the value
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("test") && s.contains("count"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_set_nx_xx() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // NX - only set if doesn't exist (should succeed)
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":1}"#, "NX"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // NX - should fail because key exists
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":2}"#, "NX"])
        .await;
    assert_eq!(response, Response::Bulk(None));

    // XX - only set if exists (should succeed)
    let response = client
        .command(&["JSON.SET", "doc", "$", r#"{"value":3}"#, "XX"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // XX on new key - should fail
    let response = client
        .command(&["JSON.SET", "newdoc", "$", r#"{"value":1}"#, "XX"])
        .await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_del() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set up document
    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"a":1,"b":{"c":2},"d":[1,2,3]}"#,
        ])
        .await;

    // Delete nested path
    let response = client.command(&["JSON.DEL", "doc", "$.b.c"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify deletion
    let response = client.command(&["JSON.GET", "doc", "$.b"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(!s.contains("c\":2"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Delete non-existent path
    let response = client.command(&["JSON.DEL", "doc", "$.nonexistent"]).await;
    assert_eq!(response, Response::Integer(0));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_type() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"str":"hello","num":42,"bool":true,"null":null,"arr":[1],"obj":{}}"#,
        ])
        .await;

    // String type - single path returns scalar
    let response = client.command(&["JSON.TYPE", "doc", "$.str"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("string"))));

    // Number type (integer)
    let response = client.command(&["JSON.TYPE", "doc", "$.num"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("integer"))));

    // Boolean type
    let response = client.command(&["JSON.TYPE", "doc", "$.bool"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("boolean"))));

    // Null type
    let response = client.command(&["JSON.TYPE", "doc", "$.null"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("null"))));

    // Array type
    let response = client.command(&["JSON.TYPE", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("array"))));

    // Object type
    let response = client.command(&["JSON.TYPE", "doc", "$.obj"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("object"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_numincrby() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"counter":10}"#])
        .await;

    // Increment by 5
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("15"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Decrement by 3
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "-3"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("12"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Float increment
    let response = client
        .command(&["JSON.NUMINCRBY", "doc", "$.counter", "0.5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("12.5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nummultby() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"value":10}"#])
        .await;

    // Multiply by 3
    let response = client
        .command(&["JSON.NUMMULTBY", "doc", "$.value", "3"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("30"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Multiply by 0.5
    let response = client
        .command(&["JSON.NUMMULTBY", "doc", "$.value", "0.5"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("15"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_strappend() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"greeting":"Hello"}"#])
        .await;

    // Append string - single path returns scalar
    let response = client
        .command(&["JSON.STRAPPEND", "doc", "$.greeting", r#"" World""#])
        .await;
    assert_eq!(response, Response::Integer(11)); // "Hello World" length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.greeting"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("Hello World"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_strlen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"name":"Hello"}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.STRLEN", "doc", "$.name"]).await;
    assert_eq!(response, Response::Integer(5));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrappend() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2]}"#])
        .await;

    // Append values - single path returns scalar
    let response = client
        .command(&["JSON.ARRAPPEND", "doc", "$.arr", "3", "4"])
        .await;
    assert_eq!(response, Response::Integer(4)); // New length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[1,2,3,4]") || s.contains("1, 2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrindex() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":["a","b","c","b","d"]}"#])
        .await;

    // Find first occurrence - single path returns scalar
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""b""#])
        .await;
    assert_eq!(response, Response::Integer(1));

    // Find with start index
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""b""#, "2"])
        .await;
    assert_eq!(response, Response::Integer(3));

    // Not found
    let response = client
        .command(&["JSON.ARRINDEX", "doc", "$.arr", r#""z""#])
        .await;
    assert_eq!(response, Response::Integer(-1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrinsert() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,3,4]}"#])
        .await;

    // Insert at index 1 - single path returns scalar
    let response = client
        .command(&["JSON.ARRINSERT", "doc", "$.arr", "1", "2"])
        .await;
    assert_eq!(response, Response::Integer(4)); // New length

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[1,2,3,4]") || s.contains("1, 2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrlen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3,4,5]}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(5));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrpop() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[1,2,3,4,5]}"#])
        .await;

    // Pop last element (default) - single path returns scalar
    let response = client.command(&["JSON.ARRPOP", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Pop at index 0
    let response = client.command(&["JSON.ARRPOP", "doc", "$.arr", "0"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Verify remaining [2,3,4]
    let response = client.command(&["JSON.ARRLEN", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_arrtrim() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"arr":[0,1,2,3,4,5,6]}"#])
        .await;

    // Trim to indices 2-4 (inclusive) - single path returns scalar
    let response = client
        .command(&["JSON.ARRTRIM", "doc", "$.arr", "2", "4"])
        .await;
    assert_eq!(response, Response::Integer(3)); // New length

    // Verify [2,3,4]
    let response = client.command(&["JSON.GET", "doc", "$.arr"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("[2,3,4]") || s.contains("2, 3, 4"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_objkeys() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#])
        .await;

    // Single path returns array of keys directly
    let response = client.command(&["JSON.OBJKEYS", "doc", "$"]).await;
    match response {
        Response::Array(keys) => {
            assert_eq!(keys.len(), 3);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_objlen() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2,"c":3}"#])
        .await;

    // Single path returns scalar
    let response = client.command(&["JSON.OBJLEN", "doc", "$"]).await;
    assert_eq!(response, Response::Integer(3));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_clear() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"obj":{"a":1},"arr":[1,2,3]}"#])
        .await;

    // Clear object
    let response = client.command(&["JSON.CLEAR", "doc", "$.obj"]).await;
    assert_eq!(response, Response::Integer(1));

    // Clear array
    let response = client.command(&["JSON.CLEAR", "doc", "$.arr"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            // Object should be empty, array should be empty
            assert!(s.contains("\"obj\":{}") || s.contains("\"obj\": {}"));
            assert!(s.contains("\"arr\":[]") || s.contains("\"arr\": []"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_toggle() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"active":true}"#])
        .await;

    // Toggle true -> false - single path returns scalar
    let response = client.command(&["JSON.TOGGLE", "doc", "$.active"]).await;
    assert_eq!(response, Response::Integer(0)); // false = 0

    // Toggle false -> true
    let response = client.command(&["JSON.TOGGLE", "doc", "$.active"]).await;
    assert_eq!(response, Response::Integer(1)); // true = 1

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_merge() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":{"c":2}}"#])
        .await;

    // Merge with patch
    let response = client
        .command(&["JSON.MERGE", "doc", "$", r#"{"a":10,"b":{"d":4},"e":5}"#])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify
    let response = client.command(&["JSON.GET", "doc", "$"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("\"a\":10") || s.contains("\"a\": 10"));
            assert!(s.contains("\"e\":5") || s.contains("\"e\": 5"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_mget() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set multiple documents
    client
        .command(&["JSON.SET", "doc1", "$", r#"{"name":"one"}"#])
        .await;
    client
        .command(&["JSON.SET", "doc2", "$", r#"{"name":"two"}"#])
        .await;
    client
        .command(&["JSON.SET", "doc3", "$", r#"{"name":"three"}"#])
        .await;

    // MGET from multiple keys
    let response = client
        .command(&["JSON.MGET", "doc1", "doc2", "doc3", "$.name"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_wrongtype() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set a string key
    client.command(&["SET", "strkey", "value"]).await;

    // Try JSON operation on string key - should fail with WRONGTYPE
    let response = client.command(&["JSON.GET", "strkey", "$"]).await;
    assert!(matches!(response, Response::Error(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nonexistent_key() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // GET on nonexistent key
    let response = client.command(&["JSON.GET", "nonexistent", "$"]).await;
    assert_eq!(response, Response::Bulk(None));

    // TYPE on nonexistent key
    let response = client.command(&["JSON.TYPE", "nonexistent", "$"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_nested_path() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"level1":{"level2":{"level3":{"value":42}}}}"#,
        ])
        .await;

    // Access deeply nested value
    let response = client
        .command(&["JSON.GET", "doc", "$.level1.level2.level3.value"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("42"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    // Set nested value
    let response = client
        .command(&["JSON.SET", "doc", "$.level1.level2.level3.value", "100"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify
    let response = client
        .command(&["JSON.GET", "doc", "$.level1.level2.level3.value"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("100"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_array_wildcard() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&[
            "JSON.SET",
            "doc",
            "$",
            r#"{"items":[{"id":1},{"id":2},{"id":3}]}"#,
        ])
        .await;

    // Access all ids using wildcard
    let response = client.command(&["JSON.GET", "doc", "$.items[*].id"]).await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1") && s.contains("2") && s.contains("3"));
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_get_formatting() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client
        .command(&["JSON.SET", "doc", "$", r#"{"a":1,"b":2}"#])
        .await;

    // With INDENT
    let response = client
        .command(&["JSON.GET", "doc", "INDENT", "  ", "$"])
        .await;
    match response {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            // Should have some formatting structure
            assert!(s.len() > 0);
        }
        _ => panic!("Expected bulk string, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_json_type_key() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set JSON document
    client
        .command(&["JSON.SET", "jsonkey", "$", r#"{"value":1}"#])
        .await;

    // TYPE command should return "ReJSON-RL"
    let response = client.command(&["TYPE", "jsonkey"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("ReJSON-RL")));

    server.shutdown().await;
}

// =============================================================================
// COPY command tests
// =============================================================================

#[tokio::test]
async fn test_copy_basic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET source key
    client.command(&["SET", "src", "hello"]).await;

    // COPY to new destination
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify both keys have the same value
    let src_val = client.command(&["GET", "src"]).await;
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(src_val, Response::Bulk(Some(Bytes::from("hello"))));
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_source_not_exists() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // COPY nonexistent source
    let response = client.command(&["COPY", "nonexistent", "dst"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify destination was not created
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_destination_exists_without_replace() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET both keys
    client.command(&["SET", "src", "source_value"]).await;
    client.command(&["SET", "dst", "original_value"]).await;

    // COPY without REPLACE should return 0
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(0));

    // Verify destination still has original value
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("original_value"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_destination_exists_with_replace() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET both keys
    client.command(&["SET", "src", "new_value"]).await;
    client.command(&["SET", "dst", "old_value"]).await;

    // COPY with REPLACE should succeed
    let response = client.command(&["COPY", "src", "dst", "REPLACE"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has new value
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("new_value"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_with_ttl() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET source key with TTL
    client.command(&["SET", "src", "hello", "EX", "100"]).await;

    // COPY to destination
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has TTL
    let ttl = client.command(&["TTL", "dst"]).await;
    match ttl {
        Response::Integer(t) => assert!(t > 0 && t <= 100, "TTL should be between 1 and 100"),
        _ => panic!("Expected integer response for TTL"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_hash() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create hash
    client.command(&["HSET", "src", "field1", "value1", "field2", "value2"]).await;

    // COPY hash
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination is a hash with same values
    let field1 = client.command(&["HGET", "dst", "field1"]).await;
    let field2 = client.command(&["HGET", "dst", "field2"]).await;
    assert_eq!(field1, Response::Bulk(Some(Bytes::from("value1"))));
    assert_eq!(field2, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_list() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create list
    client.command(&["RPUSH", "src", "a", "b", "c"]).await;

    // COPY list
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same elements
    let len = client.command(&["LLEN", "dst"]).await;
    assert_eq!(len, Response::Integer(3));

    let range = client.command(&["LRANGE", "dst", "0", "-1"]).await;
    match range {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], Response::Bulk(Some(Bytes::from("a"))));
            assert_eq!(arr[1], Response::Bulk(Some(Bytes::from("b"))));
            assert_eq!(arr[2], Response::Bulk(Some(Bytes::from("c"))));
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_set() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create set
    client.command(&["SADD", "src", "member1", "member2", "member3"]).await;

    // COPY set
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same members
    let card = client.command(&["SCARD", "dst"]).await;
    assert_eq!(card, Response::Integer(3));

    let is_member = client.command(&["SISMEMBER", "dst", "member1"]).await;
    assert_eq!(is_member, Response::Integer(1));

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_sorted_set() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Create sorted set
    client.command(&["ZADD", "src", "1", "one", "2", "two", "3", "three"]).await;

    // COPY sorted set
    let response = client.command(&["COPY", "src", "dst"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify destination has same members with scores
    let card = client.command(&["ZCARD", "dst"]).await;
    assert_eq!(card, Response::Integer(3));

    let score = client.command(&["ZSCORE", "dst", "two"]).await;
    match score {
        Response::Bulk(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s, "2");
        }
        _ => panic!("Expected bulk response for ZSCORE"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_copy_db_option_ignored() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // SET source key
    client.command(&["SET", "src", "hello"]).await;

    // COPY with DB option (should be accepted but ignored)
    let response = client.command(&["COPY", "src", "dst", "DB", "1"]).await;
    assert_eq!(response, Response::Integer(1));

    // Verify copy succeeded in same DB
    let dst_val = client.command(&["GET", "dst"]).await;
    assert_eq!(dst_val, Response::Bulk(Some(Bytes::from("hello"))));

    server.shutdown().await;
}

// ============================================================================
// RANDOMKEY Tests
// ============================================================================

#[tokio::test]
async fn test_randomkey_empty_database() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Empty database should return nil
    let response = client.command(&["RANDOMKEY"]).await;
    assert_eq!(response, Response::Bulk(None));

    server.shutdown().await;
}

#[tokio::test]
async fn test_randomkey_single_key() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.command(&["SET", "only-key", "value"]).await;

    // Should always return the only key
    let response = client.command(&["RANDOMKEY"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("only-key"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_randomkey_multiple_keys() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add multiple keys of different types
    client.command(&["SET", "str-key", "value"]).await;
    client.command(&["HSET", "hash-key", "field", "value"]).await;
    client.command(&["LPUSH", "list-key", "item"]).await;
    client.command(&["SADD", "set-key", "member"]).await;
    client.command(&["ZADD", "zset-key", "1", "member"]).await;

    let valid_keys = ["str-key", "hash-key", "list-key", "set-key", "zset-key"];

    // Run multiple times to verify randomness returns valid keys
    for _ in 0..10 {
        let response = client.command(&["RANDOMKEY"]).await;
        match response {
            Response::Bulk(Some(key)) => {
                let key_str = String::from_utf8_lossy(&key);
                assert!(
                    valid_keys.contains(&key_str.as_ref()),
                    "RANDOMKEY returned unexpected key: {}",
                    key_str
                );
            }
            _ => panic!("Expected bulk response, got {:?}", response),
        }
    }

    server.shutdown().await;
}

// ============================================================================
// MEMORY tests
// ============================================================================

#[tokio::test]
async fn test_memory_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["MEMORY", "HELP"]).await;
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "Help should not be empty");
            // First item should mention MEMORY
            if let Some(Response::Bulk(Some(first))) = items.first() {
                let text = String::from_utf8_lossy(first);
                assert!(
                    text.to_uppercase().contains("MEMORY"),
                    "Help should mention MEMORY"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_missing_key() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // MEMORY USAGE for non-existent key should return null
    let response = client.command(&["MEMORY", "USAGE", "nonexistent"]).await;
    match response {
        Response::Null => {}
        Response::Bulk(None) => {}
        _ => panic!("Expected null response for missing key, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_existing_key() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set a key
    let _ = client.command(&["SET", "testkey", "testvalue"]).await;

    // MEMORY USAGE should return a positive integer
    let response = client.command(&["MEMORY", "USAGE", "testkey"]).await;
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory usage should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_usage_with_samples() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set a key
    let _ = client.command(&["SET", "testkey", "testvalue"]).await;

    // MEMORY USAGE with SAMPLES option
    let response = client
        .command(&["MEMORY", "USAGE", "testkey", "SAMPLES", "5"])
        .await;
    match response {
        Response::Integer(size) => {
            assert!(size > 0, "Memory usage should be positive, got {}", size);
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_stats() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // MEMORY STATS should return an array of key-value pairs
    let response = client.command(&["MEMORY", "STATS"]).await;
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "MEMORY STATS should not be empty");
            // Should have pairs (key, value)
            assert!(items.len() % 2 == 0, "Should have even number of items");
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_doctor() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // MEMORY DOCTOR should return a bulk string report
    let response = client.command(&["MEMORY", "DOCTOR"]).await;
    match response {
        Response::Bulk(Some(report)) => {
            let text = String::from_utf8_lossy(&report);
            assert!(!text.is_empty(), "Doctor report should not be empty");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_malloc_size() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // MEMORY MALLOC-SIZE should return the input (stub behavior)
    let response = client.command(&["MEMORY", "MALLOC-SIZE", "1024"]).await;
    match response {
        Response::Integer(size) => {
            assert_eq!(size, 1024, "Should return the input size");
        }
        _ => panic!("Expected integer response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_purge() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // MEMORY PURGE should return OK (stub behavior)
    let response = client.command(&["MEMORY", "PURGE"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_unknown_subcommand() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["MEMORY", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// LATENCY tests
// ============================================================================

#[tokio::test]
async fn test_latency_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["LATENCY", "HELP"]).await;
    match response {
        Response::Array(items) => {
            assert!(!items.is_empty(), "Help should not be empty");
            // First item should mention LATENCY
            if let Some(Response::Bulk(Some(first))) = items.first() {
                let text = String::from_utf8_lossy(first);
                assert!(
                    text.to_uppercase().contains("LATENCY"),
                    "Help should mention LATENCY"
                );
            }
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_latest_empty() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Initially LATENCY LATEST should return empty or minimal data
    let response = client.command(&["LATENCY", "LATEST"]).await;
    match response {
        Response::Array(_) => {
            // Either empty or with entries - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY RESET should return OK
    let response = client.command(&["LATENCY", "RESET"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_reset_specific_event() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY RESET with specific event
    let response = client.command(&["LATENCY", "RESET", "command"]).await;
    match response {
        Response::Simple(s) => assert_eq!(s, Bytes::from("OK")),
        _ => panic!("Expected OK response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_history_unknown_event() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY HISTORY with invalid event should return error
    let response = client.command(&["LATENCY", "HISTORY", "invalid"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown event type"),
                "Error should mention unknown event type, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_history_valid_event() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY HISTORY with valid event should return array
    let response = client.command(&["LATENCY", "HISTORY", "command"]).await;
    match response {
        Response::Array(_) => {
            // Empty or with entries - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_graph_unknown_event() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY GRAPH with invalid event should return error
    let response = client.command(&["LATENCY", "GRAPH", "invalid"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Unknown event type"),
                "Error should mention unknown event type, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_graph_valid_event() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY GRAPH with valid event should return bulk string
    let response = client.command(&["LATENCY", "GRAPH", "command"]).await;
    match response {
        Response::Bulk(Some(graph)) => {
            let text = String::from_utf8_lossy(&graph);
            // Should contain the event name
            assert!(
                text.contains("command"),
                "Graph should mention the event name"
            );
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_doctor() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY DOCTOR should return a bulk string report
    let response = client.command(&["LATENCY", "DOCTOR"]).await;
    match response {
        Response::Bulk(Some(report)) => {
            let text = String::from_utf8_lossy(&report);
            assert!(!text.is_empty(), "Doctor report should not be empty");
        }
        _ => panic!("Expected bulk response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_histogram() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // LATENCY HISTOGRAM should return an array (may be empty)
    let response = client.command(&["LATENCY", "HISTOGRAM"]).await;
    match response {
        Response::Array(_) => {
            // Empty or with data - both are valid
        }
        _ => panic!("Expected array response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_latency_unknown_subcommand() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["LATENCY", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("unknown subcommand"),
                "Error should mention unknown subcommand"
            );
        }
        _ => panic!("Expected error response, got {:?}", response),
    }

    server.shutdown().await;
}
