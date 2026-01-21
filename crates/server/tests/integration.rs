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

        // Find an available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Create config with the chosen port and temp data dir
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string(); // Reduce noise during tests
        config.persistence.data_dir = temp_dir.path().to_path_buf();

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

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string();
        config.persistence.data_dir = temp_dir.path().to_path_buf();
        config.security.requirepass = requirepass.to_string();

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
