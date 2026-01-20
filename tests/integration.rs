//! Integration tests for FrogDB.
//!
//! These tests start a real server and connect to it using the RESP protocol.

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, Response};
use frogdb_server::{Config, Server};
use futures::{SinkExt, StreamExt};
use redis_protocol::resp2::codec::Resp2;
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
                .map(|s| BytesFrame::BulkString(Bytes::from(*s)))
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
        BytesFrame::SimpleError(e) => Response::Error(e),
        BytesFrame::Number(n) => Response::Integer(n),
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
