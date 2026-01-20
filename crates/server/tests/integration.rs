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
}

impl TestServer {
    /// Start a new test server on an available port.
    async fn start() -> Self {
        // Find an available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Create config with the chosen port
        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 1;
        config.logging.level = "warn".to_string(); // Reduce noise during tests

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
        }
    }

    /// Connect to the test server.
    async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let framed = Framed::new(stream, Resp2::default());
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
}

/// Convert a BytesFrame to our Response type.
fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(Bytes::from(e.into_inner())),
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
