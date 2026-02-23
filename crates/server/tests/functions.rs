//! Integration tests for Redis Functions (FUNCTION, FCALL, FCALL_RO).

mod common;

use bytes::Bytes;
use common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use frogdb_telemetry::testing::{MetricsDelta, MetricsSnapshot, fetch_metrics};
use std::time::Duration;

async fn start_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

// =============================================================================
// FUNCTION LOAD and LIST Tests
// =============================================================================

/// Test loading a simple function library.
#[tokio::test]
async fn test_function_load_and_list() {
    let server = start_server().await;
    let mut client = server.connect().await;

    // Get baseline metrics
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

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
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
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
    let server = start_server().await;
    let mut client = server.connect().await;

    // Load a function library
    let code = r#"#!lua name=mathlib
redis.register_function('add', function(keys, args)
    return tonumber(args[1]) + tonumber(args[2])
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Call the function
    let response = client.command(&["FCALL", "add", "0", "5", "3"]).await;
    assert_eq!(response, Response::Integer(8));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
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
    let server = start_server().await;
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
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // Call the function with a key
    let response = client.command(&["FCALL", "getkey", "1", "mykey"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("hello"))));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
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
    let server = start_server().await;
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
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

    // FCALL_RO should work with read-only function
    let response = client
        .command(&["FCALL_RO", "readonly_get", "1", "rokey"])
        .await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("value"))));

    // Verify metrics
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
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
    let server = start_server().await;
    let mut client = server.connect().await;

    // Load a function library WITHOUT no-writes flag
    let code = r#"#!lua name=writelib
redis.register_function('write_func', function(keys, args)
    return 'ok'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Get baseline metrics (after LOAD)
    let before = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);

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
    let after = MetricsSnapshot::new(fetch_metrics(server.metrics_addr()).await);
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
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

    // Restore from dump (uses command_raw for binary data)
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
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
    let server = start_server().await;
    let mut client = server.connect().await;

    let response = client.command(&["FUNCTION", "HELP"]).await;
    match response {
        Response::Array(arr) => {
            assert!(!arr.is_empty());
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
    let server = start_server().await;
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
#[tokio::test]
async fn test_function_persistence_across_restart() {
    use frogdb_core::{FunctionFlags, FunctionLibrary, FunctionRegistry, RegisteredFunction};
    use tempfile::TempDir;

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

    // Start server with persistence enabled using shared TestServer
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        persistence: true,
        data_dir: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Call the persisted function
    let response = client.command(&["FCALL", "persist_func", "0"]).await;
    assert_eq!(response, Response::Bulk(Some(Bytes::from("persisted"))));

    // Also verify the library is listed
    let response = client.command(&["FUNCTION", "LIST"]).await;
    match response {
        Response::Array(arr) => {
            assert_eq!(arr.len(), 1, "Should have exactly one library loaded");
        }
        other => panic!("Expected array from FUNCTION LIST, got: {:?}", other),
    }

    server.shutdown().await;
}

/// Test that FUNCTION DELETE updates persistence file.
#[tokio::test]
async fn test_function_persistence_after_delete() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        persistence: true,
        data_dir: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Load a function
    let code = r#"#!lua name=deletelib
redis.register_function('delete_func', function(keys, args)
    return 'to_delete'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Verify file exists with content
    let functions_file = temp_dir.path().join("functions.fdb");
    assert!(functions_file.exists());
    let file_size_after_load = std::fs::metadata(&functions_file).unwrap().len();

    // Delete the library
    client.command(&["FUNCTION", "DELETE", "deletelib"]).await;

    // Verify file size decreased (empty registry has smaller dump)
    let file_size_after_delete = std::fs::metadata(&functions_file).unwrap().len();
    assert!(
        file_size_after_delete < file_size_after_load,
        "File should be smaller after delete: {} >= {}",
        file_size_after_delete,
        file_size_after_load
    );

    server.shutdown().await;
}

/// Test that FUNCTION FLUSH updates persistence file.
#[tokio::test]
async fn test_function_persistence_after_flush() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        persistence: true,
        data_dir: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Load a function
    let code = r#"#!lua name=flushlib
redis.register_function('flush_func', function(keys, args)
    return 'to_flush'
end)
"#;

    client.command(&["FUNCTION", "LOAD", code]).await;

    // Verify file exists with content
    let functions_file = temp_dir.path().join("functions.fdb");
    assert!(functions_file.exists());
    let file_size_after_load = std::fs::metadata(&functions_file).unwrap().len();

    // Flush all functions
    client.command(&["FUNCTION", "FLUSH"]).await;

    // Verify file size decreased (empty registry has smaller dump)
    let file_size_after_flush = std::fs::metadata(&functions_file).unwrap().len();
    assert!(
        file_size_after_flush < file_size_after_load,
        "File should be smaller after flush: {} >= {}",
        file_size_after_flush,
        file_size_after_load
    );

    server.shutdown().await;
}
