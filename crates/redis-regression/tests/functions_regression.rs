use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn function_load_and_fcall_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Load a simple function
    let code = r#"#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
    return 'hello'
end)
"#;
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert_bulk_eq(&resp, b"mylib");

    // Call the function
    let resp = client.command(&["FCALL", "myfunc", "0"]).await;
    assert_bulk_eq(&resp, b"hello");
}

#[tokio::test]
async fn lua_sandbox_blocks_global_assignment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Attempting to set a global should fail
    let resp = client
        .command(&["EVAL", "foo = 123; return foo", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn lua_sandbox_blocks_debug_os_io() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // debug should not be available
    let resp = client.command(&["EVAL", "return type(debug)", "0"]).await;
    // debug is nil in sandboxed Lua
    assert_bulk_eq(&resp, b"nil");

    // io should not be available
    let resp = client.command(&["EVAL", "return type(io)", "0"]).await;
    assert_bulk_eq(&resp, b"nil");
}

#[tokio::test]
async fn lua_sandbox_blocks_loadstring() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // loadstring/load should not be available
    let resp = client
        .command(&["EVAL", "return type(loadstring)", "0"])
        .await;
    assert_bulk_eq(&resp, b"nil");

    let resp = client.command(&["EVAL", "return type(load)", "0"]).await;
    assert_bulk_eq(&resp, b"nil");
}

#[tokio::test]
async fn lua_require_rejects_dangerous_modules() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // require should not be available in sandbox
    let resp = client.command(&["EVAL", "return type(require)", "0"]).await;
    assert_bulk_eq(&resp, b"nil");
}
