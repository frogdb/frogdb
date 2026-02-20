//! Integration tests for Redis Functions (FUNCTION, FCALL, FCALL_RO).

use bytes::Bytes;
use frogdb_metrics::testing::{fetch_metrics, MetricsDelta, MetricsSnapshot};
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
            metrics_addr,
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

    /// Send a command with raw Bytes arguments (for binary data like DUMP output).
    async fn command_raw(&mut self, args: &[&Bytes]) -> Response {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|b| BytesFrame::BulkString((*b).clone()))
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
// FUNCTION LOAD and LIST Tests
// =============================================================================

/// Test loading a simple function library.
#[tokio::test]
async fn test_function_load_and_list() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Get baseline metrics
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);

    // Load a function library
    let code = r#"#!lua name=mylib
redis.register_function('hello', function(keys, args)
    return 'Hello, ' .. (args[1] or 'World')
end)
"#;

    let response = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("mylib"))));

    // List functions
    let response = client.command(&["FUNCTION", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty());
        }
        _ => panic!("Expected array response from FUNCTION LIST"),
    }

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);
    MetricsDelta::new(before, after).assert_counter_increased_gte(
        "frogdb_commands_total",
        &[("command", "FUNCTION")],
        2.0,
    );

    server.shutdown().await;
}

// =============================================================================
// FCALL Tests
// =============================================================================

/// Test calling a function.
#[tokio::test]
async fn test_fcall_simple() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a function library
    let code = r#"#!lua name=mathlib
redis.register_function('add', function(keys, args)
    return tonumber(args[1]) + tonumber(args[2])
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);

    // Call the function
    let response = client.command(&["FCALL", "add", "0", "5", "3"]).await;
    assert_eq!(response, Response::Integer(8));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "FCALL")],
        1.0,
    );

    server.shutdown().await;
}

/// Test calling a function with keys.
#[tokio::test]
async fn test_fcall_with_keys() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set up some data
    client.command(&["SET", "mykey", "hello"]).await;

    // Load a function library that reads a key
    let code = r#"#!lua name=keylib
redis.register_function('getkey', function(keys, args)
    return redis.call('GET', keys[1])
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);

    // Call the function with a key
    let response = client.command(&["FCALL", "getkey", "1", "mykey"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello"))));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "FCALL")],
        1.0,
    );

    server.shutdown().await;
}

// =============================================================================
// FCALL_RO Tests
// =============================================================================

/// Test FCALL_RO with a read-only function.
#[tokio::test]
async fn test_fcall_ro_read_only() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Set up some data
    client.command(&["SET", "rokey", "value"]).await;

    // Load a function library with no-writes flag
    let code = r#"#!lua name=rolib
redis.register_function{
    function_name = 'readonly_get',
    callback = function(keys, args)
        return redis.call('GET', keys[1])
    end,
    flags = {'no-writes'}
}
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);

    // FCALL_RO should work with read-only function
    let response = client
        .command(&["FCALL_RO", "readonly_get", "1", "rokey"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "FCALL_RO")],
        1.0,
    );

    server.shutdown().await;
}

/// Test FCALL_RO error when function doesn't have no-writes flag.
#[tokio::test]
async fn test_fcall_ro_rejects_write_function() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a function library WITHOUT no-writes flag
    let code = r#"#!lua name=writelib
redis.register_function('write_func', function(keys, args)
    return 'ok'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);

    // FCALL_RO should fail for functions without no-writes flag
    let response = client.command(&["FCALL_RO", "write_func", "0"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("no-writes") || err_str.contains("FCALL_RO"),
                "Expected no-writes error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response for FCALL_RO with write function"),
    }

    // Verify metrics - command error should still be tracked
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr).await);
    // FCALL_RO was called but failed
    MetricsDelta::new(before, after).assert_counter_increased(
        "frogdb_commands_total",
        &[("command", "FCALL_RO")],
        1.0,
    );

    server.shutdown().await;
}

// =============================================================================
// FUNCTION DELETE Tests
// =============================================================================

/// Test FUNCTION DELETE.
#[tokio::test]
async fn test_function_delete() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a library
    let code = r#"#!lua name=deletelib
redis.register_function('todelete', function(keys, args)
    return 'will be deleted'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Function should work
    let response = client.command(&["FCALL", "todelete", "0"]).await;
    assert_eq!(
        response,
        Response::Bulk(Some(Bytes::from("will be deleted")))
    );

    // Delete the library
    let response = client.command(&["FUNCTION", "DELETE", "deletelib"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Function should no longer work
    let response = client.command(&["FCALL", "todelete", "0"]).await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error after function deleted"),
    }

    server.shutdown().await;
}

// =============================================================================
// FUNCTION FLUSH Tests
// =============================================================================

/// Test FUNCTION FLUSH.
#[tokio::test]
async fn test_function_flush() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a library
    let code = r#"#!lua name=flushlib
redis.register_function('toflush', function(keys, args)
    return 'will be flushed'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Flush all functions
    let response = client.command(&["FUNCTION", "FLUSH"]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Function should no longer work
    let response = client.command(&["FCALL", "toflush", "0"]).await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error after function flushed"),
    }

    server.shutdown().await;
}

// =============================================================================
// FUNCTION LOAD REPLACE Tests
// =============================================================================

/// Test FUNCTION LOAD with REPLACE.
#[tokio::test]
async fn test_function_load_replace() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load initial version
    let code1 = r#"#!lua name=replacelib
redis.register_function('versioned', function(keys, args)
    return 'version1'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code1]).await;

    // Verify initial version
    let response = client.command(&["FCALL", "versioned", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("version1"))));

    // Try to load without REPLACE - should fail
    let code2 = r#"#!lua name=replacelib
redis.register_function('versioned', function(keys, args)
    return 'version2'
end)
"#;

    let response = client.command(&["FUNCTION", "LOAD", code2]).await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error loading duplicate library without REPLACE"),
    }

    // Load with REPLACE - should succeed
    let response = client
        .command(&["FUNCTION", "LOAD", "REPLACE", code2])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("replacelib"))));

    // Verify updated version
    let response = client.command(&["FCALL", "versioned", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("version2"))));

    server.shutdown().await;
}

// =============================================================================
// FUNCTION DUMP and RESTORE Tests
// =============================================================================

/// Test FUNCTION DUMP and RESTORE.
#[tokio::test]
async fn test_function_dump_restore() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a library
    let code = r#"#!lua name=backuplib
redis.register_function('backup_func', function(keys, args)
    return 'backup data'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Dump functions
    let dump_response = client.command(&["FUNCTION", "DUMP"]).await;
    let dump_data = match dump_response {
        Response::Bulk(Some(data)) => data,
        _ => panic!(
            "Expected bulk string from FUNCTION DUMP, got: {:?}",
            dump_response
        ),
    };

    // Flush all functions
    client.command(&["FUNCTION", "FLUSH"]).await;

    // Verify function is gone
    let response = client.command(&["FCALL", "backup_func", "0"]).await;
    match response {
        Response::Error(_) => {}
        _ => panic!("Expected error after flush"),
    }

    // Restore from dump
    let func = Bytes::from("FUNCTION");
    let restore = Bytes::from("RESTORE");
    let response = client.command_raw(&[&func, &restore, &dump_data]).await;
    assert_eq!(response, Response::Simple(Bytes::from("OK")));

    // Function should work again
    let response = client.command(&["FCALL", "backup_func", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("backup data"))));

    server.shutdown().await;
}

// =============================================================================
// FUNCTION STATS Tests
// =============================================================================

/// Test FUNCTION STATS.
#[tokio::test]
async fn test_function_stats() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a library
    let code = r#"#!lua name=statslib
redis.register_function('stat_func', function(keys, args)
    return 'stats'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get stats
    let response = client.command(&["FUNCTION", "STATS"]).await;
    match response {
        Response::Array(arr) => {
            // Should have running_script and engines
            assert!(arr.len() >= 2);
        }
        _ => panic!("Expected array response from FUNCTION STATS"),
    }

    server.shutdown().await;
}

// =============================================================================
// FUNCTION LIST with Pattern Tests
// =============================================================================

/// Test FUNCTION LIST with LIBRARYNAME filter.
#[tokio::test]
async fn test_function_list_with_pattern() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load multiple libraries
    let code1 = r#"#!lua name=mylib1
redis.register_function('func1', function(keys, args) return 1 end)
"#;
    let code2 = r#"#!lua name=mylib2
redis.register_function('func2', function(keys, args) return 2 end)
"#;
    let code3 = r#"#!lua name=otherlib
redis.register_function('func3', function(keys, args) return 3 end)
"#;

    client.command(&["FUNCTION", "LOAD", code1]).await;
    client.command(&["FUNCTION", "LOAD", code2]).await;
    client.command(&["FUNCTION", "LOAD", code3]).await;

    // List all libraries
    let response = client.command(&["FUNCTION", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("Expected array response"),
    }

    // List with pattern
    let response = client
        .command(&["FUNCTION", "LIST", "LIBRARYNAME", "my*"])
        .await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("Expected array response"),
    }

    server.shutdown().await;
}

// =============================================================================
// Multiple Functions Per Library Tests
// =============================================================================

/// Test function with multiple functions in one library.
#[tokio::test]
async fn test_multiple_functions_per_library() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Load a library with multiple functions
    let code = r#"#!lua name=multilib
redis.register_function('func_a', function(keys, args) return 'A' end)
redis.register_function('func_b', function(keys, args) return 'B' end)
redis.register_function('func_c', function(keys, args) return 'C' end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Call each function
    let response = client.command(&["FCALL", "func_a", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("A"))));

    let response = client.command(&["FCALL", "func_b", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("B"))));

    let response = client.command(&["FCALL", "func_c", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("C"))));

    server.shutdown().await;
}

// =============================================================================
// Error Tests
// =============================================================================

/// Test function not found error.
#[tokio::test]
async fn test_function_not_found() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Try to call non-existent function
    let response = client.command(&["FCALL", "nonexistent", "0"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("not found") || err_str.contains("Function"),
                "Expected function not found error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error for non-existent function"),
    }

    server.shutdown().await;
}

/// Test invalid library code.
#[tokio::test]
async fn test_invalid_library_code() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Missing shebang
    let response = client.command(&["FUNCTION", "LOAD", "local x = 1"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("shebang") || err_str.contains("Missing"),
                "Expected shebang error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error for missing shebang"),
    }

    // Lua syntax error
    let code = r#"#!lua name=badlib
this is not valid lua
"#;
    let response = client.command(&["FUNCTION", "LOAD", code]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("Lua") || err_str.contains("error"),
                "Expected Lua error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error for invalid Lua"),
    }

    // No functions registered
    let code = r#"#!lua name=emptylib
local x = 1
"#;
    let response = client.command(&["FUNCTION", "LOAD", code]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("No functions") || err_str.contains("registered"),
                "Expected no functions error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error for no functions registered"),
    }

    server.shutdown().await;
}

// =============================================================================
// FUNCTION HELP Tests
// =============================================================================

/// Test FUNCTION HELP command.
#[tokio::test]
async fn test_function_help() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    let response = client.command(&["FUNCTION", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty());
            // Should contain help text mentioning various subcommands
        }
        _ => panic!("Expected array response from FUNCTION HELP"),
    }

    server.shutdown().await;
}

// =============================================================================
// FUNCTION KILL Tests
// =============================================================================

/// Test FUNCTION KILL when no function is running.
#[tokio::test]
async fn test_function_kill_not_busy() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // FUNCTION KILL when nothing is running should return NOTBUSY
    let response = client.command(&["FUNCTION", "KILL"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            assert!(
                err_str.contains("NOTBUSY"),
                "Expected NOTBUSY error, got: {}",
                err_str
            );
        }
        _ => panic!("Expected error response for FUNCTION KILL when not busy"),
    }

    server.shutdown().await;
}

// =============================================================================
// Persistence Tests
// =============================================================================

/// Test that functions are loaded from persistence file on startup.
///
/// This test creates a functions.fdb file manually and verifies
/// that the server loads it on startup.
#[tokio::test]
async fn test_function_persistence_across_restart() {
    use frogdb_core::{FunctionFlags, FunctionLibrary, FunctionRegistry, RegisteredFunction};

    let temp_dir = TempDir::new().unwrap();

    // Manually create a functions.fdb file with a library
    {
        let mut registry = FunctionRegistry::new();

        let mut lib = FunctionLibrary::new(
            "persistlib".to_string(),
            "#!lua name=persistlib\nredis.register_function('persist_func', function(keys, args) return 'persisted' end)".to_string(),
        );
        lib.add_function(RegisteredFunction::new(
            "persist_func".to_string(),
            FunctionFlags::empty(),
            None,
        ));

        registry.load_library(lib, false).unwrap();

        let functions_file = temp_dir.path().join("functions.fdb");
        frogdb_core::save_to_file(&registry, &functions_file).unwrap();
    }

    // Start server with persistence enabled
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
    config.persistence.enabled = true;
    config.persistence.data_dir = temp_dir.path().to_path_buf();
    config.metrics.bind = "127.0.0.1".to_string();
    config.metrics.port = metrics_addr.port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

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

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Connect and verify function was loaded from persistence
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, Resp2);

    // Call the persisted function
    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FCALL")),
        BytesFrame::BulkString(Bytes::from("persist_func")),
        BytesFrame::BulkString(Bytes::from("0")),
    ]);
    framed.send(frame).await.unwrap();
    let response = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        frame_to_response(response),
        Response::Bulk(Some(Bytes::from("persisted")))
    );

    // Also verify the library is listed
    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FUNCTION")),
        BytesFrame::BulkString(Bytes::from("LIST")),
    ]);
    framed.send(frame).await.unwrap();
    let response = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match frame_to_response(response) {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1, "Should have exactly one library loaded");
        }
        other => panic!("Expected array from FUNCTION LIST, got: {:?}", other),
    }

    // Cleanup
    drop(framed);
    let _ = shutdown_tx.send(());
    handle.abort();
    let _ = handle.await;
}

/// Test that FUNCTION DELETE updates persistence file.
#[tokio::test]
async fn test_function_persistence_after_delete() {
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
    config.persistence.enabled = true;
    config.persistence.data_dir = temp_dir.path().to_path_buf();
    config.metrics.bind = "127.0.0.1".to_string();
    config.metrics.port = metrics_addr.port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

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

    tokio::time::sleep(Duration::from_millis(150)).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, Resp2);

    // Load a function
    let code = r#"#!lua name=deletelib
redis.register_function('delete_func', function(keys, args)
    return 'to_delete'
end)
"#;

    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FUNCTION")),
        BytesFrame::BulkString(Bytes::from("LOAD")),
        BytesFrame::BulkString(Bytes::from(code)),
    ]);
    framed.send(frame).await.unwrap();
    let _ = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify file exists with content
    let functions_file = temp_dir.path().join("functions.fdb");
    assert!(functions_file.exists());
    let file_size_after_load = std::fs::metadata(&functions_file).unwrap().len();

    // Delete the library
    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FUNCTION")),
        BytesFrame::BulkString(Bytes::from("DELETE")),
        BytesFrame::BulkString(Bytes::from("deletelib")),
    ]);
    framed.send(frame).await.unwrap();
    let _ = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify file size decreased (empty registry has smaller dump)
    let file_size_after_delete = std::fs::metadata(&functions_file).unwrap().len();
    assert!(
        file_size_after_delete < file_size_after_load,
        "File should be smaller after delete: {} >= {}",
        file_size_after_delete,
        file_size_after_load
    );

    // Cleanup
    drop(framed);
    let _ = shutdown_tx.send(());
    handle.abort();
    let _ = handle.await;
}

/// Test that FUNCTION FLUSH updates persistence file.
#[tokio::test]
async fn test_function_persistence_after_flush() {
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
    config.persistence.enabled = true;
    config.persistence.data_dir = temp_dir.path().to_path_buf();
    config.metrics.bind = "127.0.0.1".to_string();
    config.metrics.port = metrics_addr.port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

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

    tokio::time::sleep(Duration::from_millis(150)).await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut framed = Framed::new(stream, Resp2);

    // Load a function
    let code = r#"#!lua name=flushlib
redis.register_function('flush_func', function(keys, args)
    return 'to_flush'
end)
"#;

    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FUNCTION")),
        BytesFrame::BulkString(Bytes::from("LOAD")),
        BytesFrame::BulkString(Bytes::from(code)),
    ]);
    framed.send(frame).await.unwrap();
    let _ = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify file exists with content
    let functions_file = temp_dir.path().join("functions.fdb");
    assert!(functions_file.exists());
    let file_size_after_load = std::fs::metadata(&functions_file).unwrap().len();

    // Flush all functions
    let frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from("FUNCTION")),
        BytesFrame::BulkString(Bytes::from("FLUSH")),
    ]);
    framed.send(frame).await.unwrap();
    let _ = timeout(Duration::from_secs(5), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify file size decreased (empty registry has smaller dump)
    let file_size_after_flush = std::fs::metadata(&functions_file).unwrap().len();
    assert!(
        file_size_after_flush < file_size_after_load,
        "File should be smaller after flush: {} >= {}",
        file_size_after_flush,
        file_size_after_load
    );

    // Cleanup
    drop(framed);
    let _ = shutdown_tx.send(());
    handle.abort();
    let _ = handle.await;
}
