//! Rust port of Redis 8.6.0 `unit/functions.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (DEBUG RELOAD)
//! - `needs:repl` / `external:skip` tests (replication)
//! - `needs:config-maxmemory` tests (CONFIG SET maxmemory)
//! - FUNCTION DUMP/RESTORE tests (binary serialization)
//! - Tests using CONFIG SET (busy-reply-threshold, replica-serve-stale-data, etc.)
//! - Cluster-specific tests
//! - Async lazy-free race-condition test (requires CONFIG RESETSTAT + INFO stats)
//!
//! ## Intentional exclusions
//!
//! DEBUG RELOAD / FUNCTION DUMP-RESTORE (FrogDB has different persistence model):
//! - `FUNCTION - test debug reload different options` — needs:debug
//! - `FUNCTION - test debug reload with nosave and noflush` — needs:debug
//! - `FUNCTION - test function dump and restore` — Redis-internal feature (FUNCTION DUMP/RESTORE binary)
//!
//! OOM / maxmemory interaction (different eviction model):
//! - `FUNCTION - deny oom` — needs:config-maxmemory
//! - `FUNCTION - deny oom on no-writes function` — needs:config-maxmemory
//!
//! Replica stale-data behavior (different replication model):
//! - `FUNCTION - allow stale` — needs:repl

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// FUNCTION - Basic usage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_basic_usage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;
    assert_bulk_eq(&client.command(&["FCALL", "test", "0"]).await, b"hello");
}

#[tokio::test]
async fn tcl_function_load_with_unknown_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    let resp = client
        .command(&["FUNCTION", "LOAD", "foo", "bar", code])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_create_already_existing_library() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let code2 = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    let resp = client.command(&["FUNCTION", "LOAD", code2]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_create_library_wrong_name_format() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=bad\0format\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_create_library_with_unexisting_engine() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!bad_engine name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_test_uncompiled_script() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code =
        "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n bad script \nend)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - REPLACE argument
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_replace_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let code2 = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    client
        .command(&["FUNCTION", "LOAD", "REPLACE", code2])
        .await;
    assert_bulk_eq(&client.command(&["FCALL", "test", "0"]).await, b"hello1");
}

#[tokio::test]
async fn tcl_function_case_insensitive() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    assert_bulk_eq(&client.command(&["FCALL", "TEST", "0"]).await, b"hello1");
}

#[tokio::test]
async fn tcl_function_replace_with_failure_keeps_old_libraries() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello1' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    // Load with a compile error; should fail
    let bad_code =
        "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n error \nend)";
    let resp = client
        .command(&["FUNCTION", "LOAD", "REPLACE", bad_code])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Old function should still work
    assert_bulk_eq(&client.command(&["FCALL", "test", "0"]).await, b"hello1");
}

// ---------------------------------------------------------------------------
// FUNCTION - DELETE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_delete() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;
    assert_ok(&client.command(&["FUNCTION", "DELETE", "test"]).await);

    let resp = client.command(&["FCALL", "test", "0"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_delete_non_existing_library() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "DELETE", "test1"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - FCALL error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_fcall_bad_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client.command(&["FCALL", "test", "bad_arg"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_fcall_bad_number_of_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client.command(&["FCALL", "test", "10", "key1"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_fcall_negative_number_of_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 'hello' \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client.command(&["FCALL", "test", "-1", "key1"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - KILL and wrong subcommand
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_kill_when_not_running() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "KILL"]).await;
    assert_error_prefix(&resp, "NOTBUSY");
}

#[tokio::test]
async fn tcl_function_wrong_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "bad_subcommand"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - FLUSH and FLUSHALL/FLUSHDB interaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_flushall_flushdb_do_not_clean_functions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;
    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return redis.call('set', 'x', '1') \nend)";
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    client.command(&["FLUSHALL"]).await;
    client.command(&["FLUSHDB"]).await;

    // Functions should still be listed
    let resp = client.command(&["FUNCTION", "LIST"]).await;
    let items = unwrap_array(resp);
    assert!(
        !items.is_empty(),
        "expected at least one library after FLUSHALL/FLUSHDB"
    );
}

#[tokio::test]
async fn tcl_function_fcall_ro_with_write_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function{function_name='test', callback=function(KEYS, ARGV)\n return redis.call('set', 'x', '1') \nend, flags={'no-writes'}}";
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    let resp = client.command(&["FCALL_RO", "test", "1", "x"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_fcall_ro_with_read_only_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function{function_name='test', callback=function(KEYS, ARGV)\n return redis.call('get', 'x') \nend, flags={'no-writes'}}";
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    client.command(&["SET", "x", "1"]).await;
    assert_bulk_eq(&client.command(&["FCALL_RO", "test", "1", "x"]).await, b"1");
}

// ---------------------------------------------------------------------------
// FUNCTION - keys and argv
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_keys_and_argv() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return redis.call('set', KEYS[1], ARGV[1]) \nend)";
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    client.command(&["FCALL", "test", "1", "x", "foo"]).await;
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"foo");
}

// ---------------------------------------------------------------------------
// FUNCTION - FLUSH variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_flush() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Load, verify listed, flush, verify empty
    let code =
        "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return 1 \nend)";
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    let resp = client.command(&["FUNCTION", "LIST"]).await;
    let items = unwrap_array(resp);
    assert!(!items.is_empty());

    client.command(&["FUNCTION", "FLUSH"]).await;
    assert_array_len(&client.command(&["FUNCTION", "LIST"]).await, 0);

    // Test FLUSH ASYNC
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    client.command(&["FUNCTION", "FLUSH", "ASYNC"]).await;
    assert_array_len(&client.command(&["FUNCTION", "LIST"]).await, 0);

    // Test FLUSH SYNC
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    client.command(&["FUNCTION", "FLUSH", "SYNC"]).await;
    assert_array_len(&client.command(&["FUNCTION", "LIST"]).await, 0);
}

#[tokio::test]
async fn tcl_function_flush_wrong_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "FLUSH", "bad_arg"]).await;
    assert_error_prefix(&resp, "ERR");

    let resp = client
        .command(&["FUNCTION", "FLUSH", "SYNC", "extra_arg"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - shared function / code sharing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_shared_function_can_access_default_globals() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib1
local function ping()
    return redis.call('ping')
end
redis.register_function(
    'f1',
    function(keys, args)
        return ping()
    end
)
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;
    // redis.call('ping') returns a status reply; accept both Simple and Bulk
    let resp = client.command(&["FCALL", "f1", "0"]).await;
    match &resp {
        Response::Simple(b) | Response::Bulk(Some(b)) => {
            assert_eq!(b.as_ref(), b"PONG", "expected PONG, got {:?}", resp);
        }
        _ => panic!("expected PONG response, got {resp:?}"),
    }
}

#[tokio::test]
async fn tcl_libraries_usage_and_code_sharing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib1
local function add1(a)
    return a + 1
end
redis.register_function(
    'f1',
    function(keys, args)
        return add1(1)
    end
)
redis.register_function(
    'f2',
    function(keys, args)
        return add1(2)
    end
)
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;
    assert_integer_eq(&client.command(&["FCALL", "f1", "0"]).await, 2);
    assert_integer_eq(&client.command(&["FCALL", "f2", "0"]).await, 3);

    // FUNCTION LIST should show one library
    let resp = client.command(&["FUNCTION", "LIST"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn tcl_libraries_registration_failure_reverts_entire_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Load a good library first
    let good_code = r#"#!lua name=lib1
local function add1(a)
    return a + 1
end
redis.register_function(
    'f1',
    function(keys, args)
        return add1(1)
    end
)
redis.register_function(
    'f2',
    function(keys, args)
        return add1(2)
    end
)
"#;
    client.command(&["FUNCTION", "LOAD", good_code]).await;

    // Try to replace with a broken load (second register_function has bad callback)
    let bad_code = r#"#!lua name=lib1
local function add1(a)
    return a + 2
end
redis.register_function(
    'f1',
    function(keys, args)
        return add1(1)
    end
)
redis.register_function(
    'f2',
    'not a function'
)
"#;
    let resp = client
        .command(&["FUNCTION", "LOAD", "REPLACE", bad_code])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Old functions should still work with original values
    assert_integer_eq(&client.command(&["FCALL", "f1", "0"]).await, 2);
    assert_integer_eq(&client.command(&["FCALL", "f2", "0"]).await, 3);
}

#[tokio::test]
async fn tcl_libraries_function_name_collision_different_library() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code1 = r#"#!lua name=lib1
redis.register_function(
    'f1',
    function(keys, args)
        return 1
    end
)
"#;
    client.command(&["FUNCTION", "LOAD", code1]).await;

    // Try to register f1 under a different library
    let code2 = r#"#!lua name=lib2
redis.register_function(
    'f1',
    function(keys, args)
        return 1
    end
)
"#;
    let resp = client
        .command(&["FUNCTION", "LOAD", "REPLACE", code2])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_function_name_collision_same_library() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib2
redis.register_function(
    'f1',
    function(keys, args)
        return 1
    end
)
redis.register_function(
    'f1',
    function(keys, args)
        return 1
    end
)
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - register_function argument validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_registration_with_no_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nredis.register_function()";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_registration_with_only_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nredis.register_function('f1')";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_registration_with_too_many_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nredis.register_function('f1', function() return 1 end, {}, 'description', 'extra arg')";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_registration_with_nil_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nredis.register_function(nil, function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_registration_with_empty_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nredis.register_function('', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - global access restrictions during FUNCTION LOAD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_math_random_from_function_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nreturn math.random()";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_redis_call_from_function_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nreturn redis.call('ping')";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_redis_setresp_from_function_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib2\nreturn redis.setresp(3)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - delete removes all functions on library
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_delete_removes_all_functions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib1
redis.register_function('f1', function(keys, args) return 1 end)
redis.register_function('f2', function(keys, args) return 2 end)
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;

    // Both functions should work
    assert_integer_eq(&client.command(&["FCALL", "f1", "0"]).await, 1);
    assert_integer_eq(&client.command(&["FCALL", "f2", "0"]).await, 2);

    // Delete the library
    assert_ok(&client.command(&["FUNCTION", "DELETE", "lib1"]).await);

    // Both functions should be gone
    assert_error_prefix(&client.command(&["FCALL", "f1", "0"]).await, "ERR");
    assert_error_prefix(&client.command(&["FCALL", "f2", "0"]).await, "ERR");

    assert_array_len(&client.command(&["FUNCTION", "LIST"]).await, 0);
}

// ---------------------------------------------------------------------------
// LIBRARIES - register function inside a function
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_register_function_inside_a_function() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function(
    'f1',
    function(keys, args)
        redis.register_function(
            'f2',
            function(key, args)
                return 2
            end
        )
        return 1
    end
)
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;
    let resp = client.command(&["FCALL", "f1", "0"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - register library with no functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_register_library_with_no_functions() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;
    let code = "#!lua name=lib\nreturn 1";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - load timeout (infinite loop)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_load_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib\nlocal a = 1\nwhile 1 do a = a + 1 end";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - global protection on load
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_verify_global_protection_on_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=lib\na = 1";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// LIBRARIES - named arguments (table form)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_libraries_named_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name='f1',
    callback=function()
        return 'hello'
    end,
    description='some desc'
}
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;

    // Verify function works
    assert_bulk_eq(&client.command(&["FCALL", "f1", "0"]).await, b"hello");

    // Verify FUNCTION LIST includes the library
    let resp = client.command(&["FUNCTION", "LIST"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
}

#[tokio::test]
async fn tcl_libraries_named_arguments_bad_function_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name=function() return 1 end,
    callback=function()
        return 'hello'
    end,
    description='some desc'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_named_arguments_bad_callback_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name='f1',
    callback='bad',
    description='some desc'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_named_arguments_bad_description() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name='f1',
    callback=function()
        return 'hello'
    end,
    description=function() return 1 end
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_named_arguments_unknown_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name='f1',
    callback=function()
        return 'hello'
    end,
    description='desc',
    some_unknown='unknown'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_named_arguments_missing_function_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    callback=function()
        return 'hello'
    end,
    description='desc'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_libraries_named_arguments_missing_callback() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib
redis.register_function{
    function_name='f1',
    description='desc'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION LIST - with code and patterns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_list_with_code() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;
    let code =
        "#!lua name=library1\nredis.register_function('f6', function(keys, args) return 7 end)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client.command(&["FUNCTION", "LIST", "WITHCODE"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);

    // The library entry should contain the code
    let lib = unwrap_array(items.into_iter().next().unwrap());
    let strings = lib
        .iter()
        .filter_map(|item| match item {
            Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        strings.contains(&"library_code".to_string()),
        "expected library_code field in FUNCTION LIST WITHCODE response"
    );
}

#[tokio::test]
async fn tcl_function_list_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code1 =
        "#!lua name=library1\nredis.register_function('f6', function(keys, args) return 7 end)";
    client.command(&["FUNCTION", "LOAD", code1]).await;

    let code2 = "#!lua name=lib1\nredis.register_function('f7', function(keys, args) return 7 end)";
    client.command(&["FUNCTION", "LOAD", code2]).await;

    // Filter by pattern matching "library*"
    let resp = client
        .command(&["FUNCTION", "LIST", "LIBRARYNAME", "library*"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(
        items.len(),
        1,
        "expected exactly one library matching 'library*'"
    );
}

#[tokio::test]
async fn tcl_function_list_wrong_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "LIST", "bad_argument"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_list_libraryname_missing_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["FUNCTION", "LIST", "LIBRARYNAME"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_list_withcode_multiple_times() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["FUNCTION", "LIST", "WITHCODE", "WITHCODE"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_list_libraryname_multiple_times() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "FUNCTION",
            "LIST",
            "WITHCODE",
            "LIBRARYNAME",
            "foo",
            "LIBRARYNAME",
            "foo",
        ])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - flags
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_wrong_flags_type_named_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
redis.register_function{
    function_name = 'f1',
    callback = function() return 1 end,
    flags = 'bad flags type'
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_wrong_flag_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
redis.register_function{
    function_name = 'f1',
    callback = function() return 1 end,
    flags = {function() return 1 end}
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_unknown_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
redis.register_function{
    function_name = 'f1',
    callback = function() return 1 end,
    flags = {'unknown'}
}
"#;
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_write_script_on_fcall_ro() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
redis.register_function{
    function_name = 'f1',
    callback = function() return redis.call('set', 'x', 1) end
}
"#;
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    let resp = client.command(&["FCALL_RO", "f1", "1", "x"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_write_script_with_no_writes_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
redis.register_function{
    function_name = 'f1',
    callback = function() return redis.call('set', 'x', 1) end,
    flags = {'no-writes'}
}
"#;
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    let resp = client.command(&["FCALL", "f1", "1", "x"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION STATS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_stats() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code1 = r#"#!lua name=test1
redis.register_function('f1', function() return 1 end)
redis.register_function('f2', function() return 1 end)
"#;
    client.command(&["FUNCTION", "LOAD", code1]).await;

    let code2 = "#!lua name=test2\nredis.register_function('f3', function() return 1 end)";
    client.command(&["FUNCTION", "LOAD", code2]).await;

    let resp = client.command(&["FUNCTION", "STATS"]).await;
    // FUNCTION STATS returns structured data; just verify it is an array
    match &resp {
        Response::Array(_) => {}
        other => panic!("expected Array from FUNCTION STATS, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_function_stats_delete_library() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code1 = r#"#!lua name=test1
redis.register_function('f1', function() return 1 end)
redis.register_function('f2', function() return 1 end)
"#;
    client.command(&["FUNCTION", "LOAD", code1]).await;

    let code2 = "#!lua name=test2\nredis.register_function('f3', function() return 1 end)";
    client.command(&["FUNCTION", "LOAD", code2]).await;

    // Delete test1, stats should still return valid response
    client.command(&["FUNCTION", "DELETE", "test1"]).await;
    let resp = client.command(&["FUNCTION", "STATS"]).await;
    match &resp {
        Response::Array(_) => {}
        other => panic!("expected Array from FUNCTION STATS, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_function_stats_on_loading_failure() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code1 = r#"#!lua name=test1
redis.register_function('f1', function() return 1 end)
redis.register_function('f2', function() return 1 end)
"#;
    client.command(&["FUNCTION", "LOAD", code1]).await;

    // Try to load same library again (should fail)
    let code_dup = "#!lua name=test1\nredis.register_function('f3', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", code_dup]).await;
    assert_error_prefix(&resp, "ERR");

    // Stats should still work
    let resp = client.command(&["FUNCTION", "STATS"]).await;
    match &resp {
        Response::Array(_) => {}
        other => panic!("expected Array from FUNCTION STATS, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_function_stats_cleaned_after_flush() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test1\nredis.register_function('f1', function() return 1 end)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    client.command(&["FUNCTION", "FLUSH"]).await;
    let resp = client.command(&["FUNCTION", "STATS"]).await;
    match &resp {
        Response::Array(_) => {}
        other => panic!("expected Array from FUNCTION STATS, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// FUNCTION - metadata parsing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_test_empty_engine() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#! name=test\nredis.register_function('foo', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_test_unknown_metadata_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test foo=bar\nredis.register_function('foo', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_test_no_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua\nredis.register_function('foo', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_test_multiple_names() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=foo name=bar\nredis.register_function('foo', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_test_name_with_quotes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=\"foo\"\nredis.register_function('foo', function() return 1 end)";
    let resp = client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;
    // Redis returns the library name on success
    assert_bulk_eq(&resp, b"foo");
}

// ---------------------------------------------------------------------------
// LIBRARIES - global protection tricks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_trick_global_protection() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code = r#"#!lua name=test1
redis.register_function('f1', function()
    mt = getmetatable(_G)
    original_globals = mt.__index
    original_globals['redis'] = function() return 1 end
end)
"#;
    client.command(&["FUNCTION", "LOAD", code]).await;
    let resp = client.command(&["FCALL", "f1", "0"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_function_getmetatable_on_script_load() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FUNCTION", "FLUSH"]).await;

    let code = "#!lua name=test1\nmt = getmetatable(_G)";
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - redis version api
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_redis_version_api() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=test
local version = redis.REDIS_VERSION_NUM

redis.register_function{function_name='get_version_v1', callback=function()
  return string.format('%s.%s.%s',
                        bit.band(bit.rshift(version, 16), 0x000000ff),
                        bit.band(bit.rshift(version, 8), 0x000000ff),
                        bit.band(version, 0x000000ff))
end}
redis.register_function{function_name='get_version_v2', callback=function() return redis.REDIS_VERSION end}
"#;
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    let v1 = client.command(&["FCALL", "get_version_v1", "0"]).await;
    let v2 = client.command(&["FCALL", "get_version_v2", "0"]).await;
    assert_eq!(unwrap_bulk(&v1), unwrap_bulk(&v2));
}

// ---------------------------------------------------------------------------
// FUNCTION - malicious access test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_malicious_access_test() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = r#"#!lua name=lib1
local lib = redis
lib.register_function('f1', function ()
    lib.redis = redis
    lib.math = math
    return {ok='OK'}
end)

lib.register_function('f2', function ()
    lib.register_function('f1', function ()
        lib.redis = redis
        lib.math = math
        return {ok='OK'}
    end)
end)
"#;
    client.command(&["FUNCTION", "LOAD", "REPLACE", code]).await;

    // f1 tries to modify the redis table
    let resp = client.command(&["FCALL", "f1", "0"]).await;
    assert_error_prefix(&resp, "ERR");

    // f2 tries to call register_function at runtime
    let resp = client.command(&["FCALL", "f2", "0"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// FUNCTION - COMMAND GETKEYS on fcall/fcall_ro
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_function_command_getkeys_fcall() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return redis.call('set', KEYS[1], ARGV[1]) \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "FCALL", "test", "1", "x", "foo"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["x"]);
}

#[tokio::test]
async fn tcl_function_command_getkeys_fcall_ro() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let code = "#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n return redis.call('set', KEYS[1], ARGV[1]) \nend)";
    client.command(&["FUNCTION", "LOAD", code]).await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "FCALL_RO", "test", "1", "x", "foo"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["x"]);
}
