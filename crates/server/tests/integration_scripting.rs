//! Integration tests for Lua scripting in FrogDB.
//!
//! These tests verify redis.call(), redis.pcall(), and redis.log() functionality.

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
                _ = shutdown_rx => {}
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        TestServer {
            addr,
            shutdown_tx,
            handle,
            temp_dir,
        }
    }

    async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
    }
}

struct TestClient {
    framed: Framed<TcpStream, Resp2>,
}

impl TestClient {
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

// =============================================================================
// redis.call() Tests
// =============================================================================

#[tokio::test]
async fn test_eval_redis_call_set_get() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Use redis.call to SET a value
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "mykey",
            "myvalue",
        ])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Use redis.call to GET the value
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('GET', KEYS[1])",
            "1",
            "mykey",
        ])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("myvalue"))));

    server.shutdown().await;
}

#[tokio::test]
async fn test_eval_redis_call_incr() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Use redis.call to INCR
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('INCR', KEYS[1])",
            "1",
            "counter",
        ])
        .await;
    assert_eq!(response, Response::Integer(1));

    // INCR again
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('INCR', KEYS[1])",
            "1",
            "counter",
        ])
        .await;
    assert_eq!(response, Response::Integer(2));

    server.shutdown().await;
}

#[tokio::test]
async fn test_eval_redis_call_multiple_commands() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Script that sets two keys and returns array of values
    let script = r#"
        redis.call('SET', KEYS[1], ARGV[1])
        redis.call('SET', KEYS[2], ARGV[2])
        return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}
    "#;

    let response = client
        .command(&["EVAL", script, "2", "key1", "key2", "value1", "value2"])
        .await;

    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("value1"))),
            Response::Bulk(Some(Bytes::from("value2"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_eval_redis_call_raises_on_error() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // First set a string value
    client.command(&["SET", "stringkey", "value"]).await;

    // Try to use INCR on a string (should cause error in redis.call)
    // Note: INCR on non-numeric string should fail
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('LPUSH', KEYS[1], 'item')",
            "1",
            "stringkey",
        ])
        .await;

    // Should return an error because LPUSH on string type fails
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(err_str.contains("WRONGTYPE"), "Expected WRONGTYPE error, got: {}", err_str);
        }
        _ => panic!("Expected error response, got: {:?}", response),
    }

    server.shutdown().await;
}

// =============================================================================
// redis.pcall() Tests
// =============================================================================

#[tokio::test]
async fn test_eval_redis_pcall_returns_error_table() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // First set a string value
    client.command(&["SET", "stringkey", "value"]).await;

    // Use pcall - should return error as table instead of raising
    let script = r#"
        local result = redis.pcall('LPUSH', KEYS[1], 'item')
        if result.err then
            return result.err
        end
        return 'no error'
    "#;

    let response = client
        .command(&["EVAL", script, "1", "stringkey"])
        .await;

    // pcall should capture the error and we return the error message
    match response {
        Response::Bulk(Some(b)) => {
            let err_str = String::from_utf8_lossy(&b);
            assert!(err_str.contains("WRONGTYPE"), "Expected WRONGTYPE in error, got: {}", err_str);
        }
        _ => panic!("Expected bulk string with error message, got: {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_eval_redis_pcall_success() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // pcall on successful operation should work normally
    let response = client
        .command(&[
            "EVAL",
            "return redis.pcall('SET', KEYS[1], ARGV[1])",
            "1",
            "mykey",
            "myvalue",
        ])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    server.shutdown().await;
}

// =============================================================================
// Key Validation Tests
// =============================================================================

#[tokio::test]
async fn test_eval_undeclared_key_error() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Try to access a key that wasn't declared in KEYS
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('GET', 'undeclared_key')",
            "0",
        ])
        .await;

    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("undeclared") || err_str.contains("key"),
                "Expected undeclared key error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got: {:?}", response),
    }

    server.shutdown().await;
}

// =============================================================================
// Forbidden Commands Tests
// =============================================================================

#[tokio::test]
async fn test_eval_forbidden_command_blocked() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // EVAL should not be callable from within a script (nested scripts forbidden)
    let response = client
        .command(&[
            "EVAL",
            "return redis.call('EVAL', 'return 1', '0')",
            "0",
        ])
        .await;

    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("nested") || err_str.contains("not allowed"),
                "Expected nested script error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response, got: {:?}", response),
    }

    server.shutdown().await;
}

// =============================================================================
// Write Tracking Tests
// =============================================================================

#[tokio::test]
async fn test_eval_write_tracking() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Execute a script that performs writes
    let script = r#"
        redis.call('SET', KEYS[1], 'value1')
        redis.call('SET', KEYS[2], 'value2')
        return 'done'
    "#;

    let response = client
        .command(&["EVAL", script, "2", "key1", "key2"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("done"))));

    // Verify the writes actually happened
    let response = client.command(&["GET", "key1"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value1"))));

    let response = client.command(&["GET", "key2"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value2"))));

    server.shutdown().await;
}

// =============================================================================
// redis.log() Tests
// =============================================================================

#[tokio::test]
async fn test_eval_redis_log() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // redis.log should not error (we can't easily verify it logs, but we can verify it doesn't crash)
    let script = r#"
        redis.log(redis.LOG_WARNING, 'Test warning message')
        redis.log(redis.LOG_NOTICE, 'Test notice message')
        redis.log(redis.LOG_DEBUG, 'Test debug message')
        return 'logged'
    "#;

    let response = client.command(&["EVAL", script, "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("logged"))));

    server.shutdown().await;
}

// =============================================================================
// Complex Script Tests
// =============================================================================

#[tokio::test]
async fn test_eval_conditional_logic() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Script with conditional logic
    let script = r#"
        local current = redis.call('GET', KEYS[1])
        if current then
            return tonumber(current) + tonumber(ARGV[1])
        else
            redis.call('SET', KEYS[1], ARGV[1])
            return tonumber(ARGV[1])
        end
    "#;

    // First call - key doesn't exist, should set and return ARGV[1]
    let response = client
        .command(&["EVAL", script, "1", "counter", "10"])
        .await;
    assert_eq!(response, Response::Integer(10));

    // Second call - key exists, should add ARGV[1] to current value
    let response = client
        .command(&["EVAL", script, "1", "counter", "5"])
        .await;
    assert_eq!(response, Response::Integer(15));

    server.shutdown().await;
}

#[tokio::test]
async fn test_eval_list_operations() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Script using list operations
    let script = r#"
        redis.call('RPUSH', KEYS[1], ARGV[1])
        redis.call('RPUSH', KEYS[1], ARGV[2])
        redis.call('RPUSH', KEYS[1], ARGV[3])
        return redis.call('LRANGE', KEYS[1], 0, -1)
    "#;

    let response = client
        .command(&["EVAL", script, "1", "mylist", "a", "b", "c"])
        .await;

    assert_eq!(
        response,
        Response::Array(vec![
            Response::Bulk(Some(Bytes::from("a"))),
            Response::Bulk(Some(Bytes::from("b"))),
            Response::Bulk(Some(Bytes::from("c"))),
        ])
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_evalsha_after_script_load() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a script
    let script = "return redis.call('SET', KEYS[1], ARGV[1])";
    let response = client.command(&["SCRIPT", "LOAD", script]).await;

    let sha = match response {
        Response::Bulk(Some(b)) => String::from_utf8_lossy(&b).to_string(),
        _ => panic!("Expected bulk string with SHA, got: {:?}", response),
    };

    // Execute using EVALSHA
    let response = client
        .command(&["EVALSHA", &sha, "1", "shakey", "shavalue"])
        .await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Verify the value was set
    let response = client.command(&["GET", "shakey"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("shavalue"))));

    server.shutdown().await;
}
