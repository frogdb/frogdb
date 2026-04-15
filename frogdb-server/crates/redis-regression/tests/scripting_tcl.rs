//! Rust port of Redis 8.6.0 `unit/scripting.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (require DEBUG command)
//! - `needs:repl` tests (require replication)
//! - `needs:config-maxmemory` tests (require CONFIG SET maxmemory)
//! - `needs:reset` tests
//! - `needs:save` tests
//! - `external:skip` tests (script eviction, timedout scripts, SHUTDOWN, etc.)
//! - SCRIPT DEBUG tests
//! - Replication/replica tests
//! - CONFIG SET tests
//! - Large-memory / stress / fuzzing tests
//! - `singledb:skip` tests (DB SELECT tests)
//! - ACL-related script tests (require ACL commands)
//!
//! ## Intentional exclusions
//!
//! Strict-key-validation behavior diff (FrogDB errors immediately on undeclared
//! keys; upstream tests rely on Redis's lazier validation that lets scripts
//! rewrite/expand `client->argv` at runtime):
//! - `SORT BY <constant> output gets ordered for scripting` — intentional-incompatibility:scripting — intentional behavioral diff (strict key validation)
//! - `SORT BY <constant> with GET gets ordered for scripting` — intentional-incompatibility:scripting — intentional behavioral diff (strict key validation)
//! - `SPOP: We can call scripts rewriting client->argv from Lua` — intentional-incompatibility:scripting — intentional behavioral diff (strict key validation)
//! - `EXPIRE: We can call scripts rewriting client->argv from Lua` — intentional-incompatibility:scripting — intentional behavioral diff (strict key validation)
//! - `INCRBYFLOAT: We can call scripts expanding client->argv from Lua` — intentional-incompatibility:scripting — intentional behavioral diff (strict key validation)
//!
//! Script timeout / SCRIPT KILL (FrogDB has a different script-execution model):
//! - `Timedout read-only scripts can be killed by SCRIPT KILL` — redis-specific — Redis-internal feature
//! - `Timedout read-only scripts can be killed by SCRIPT KILL even when use pcall` — redis-specific — Redis-internal feature
//! - `Timedout script does not cause a false dead client` — redis-specific — Redis-internal feature
//! - `Timedout script link is still usable after Lua returns` — redis-specific — Redis-internal feature
//! - `Timedout scripts and unblocked command` — redis-specific — Redis-internal feature
//! - `Timedout scripts that modified data can't be killed by SCRIPT KILL` — redis-specific — Redis-internal feature
//! - `SHUTDOWN NOSAVE can kill a timedout script anyway` — redis-specific — Redis-internal feature
//!
//! Lua deprecated-API toggle (`lua-enable-deprecated-api`):
//! - `Test setfenv availability lua-enable-deprecated-api=$enabled` — redis-specific — Redis-internal feature
//! - `Test getfenv availability lua-enable-deprecated-api=$enabled` — redis-specific — Redis-internal feature
//! - `Test newproxy availability lua-enable-deprecated-api=$enabled` — redis-specific — Redis-internal feature
//!
//! Lua GC / stack / runtime internals:
//! - `Verify Lua performs GC correctly after script loading` — redis-specific — Redis-internal Lua runtime
//! - `reject script do not cause a Lua stack leak` — redis-specific — Redis-internal Lua runtime
//! - `LUA test trim string as expected` — redis-specific — Redis-internal Lua runtime
//! - `Functions in the Redis namespace are able to report errors` — redis-specific — Redis-internal Lua runtime
//!
//! Replication / propagation behavior in scripts:
//! - `MGET: mget shouldn't be propagated in Lua` — intentional-incompatibility:replication — replication-internal
//! - `not enough good replicas` — intentional-incompatibility:replication — needs:repl (min-slaves-to-write)
//! - `not enough good replicas state change during long script` — intentional-incompatibility:replication — needs:repl
//!
//! RESP3 / OOM / ACL within scripts:
//! - `Script with RESP3 map` — intentional-incompatibility:protocol — RESP3-only
//! - `Script return recursive object` — intentional-incompatibility:protocol — RESP3-only
//! - `Script - disallow write on OOM` — intentional-incompatibility:config — needs:config-maxmemory
//! - `Script ACL check` — intentional-incompatibility:scripting — needs:ACL (script-level ACL filtering)
//! - `resp3` tagged tests (require HELLO 3 protocol switching)
//! - Tests requiring `readraw` / raw protocol inspection
//! - `cmsgpack` tests (FrogDB may not ship cmsgpack)

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// EVAL basics — Lua interpreter and type conversions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_lua_interpreter_replies() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return 'hello'", "0"]).await;
    assert_bulk_eq(&resp, b"hello");
}

#[tokio::test]
async fn tcl_eval_return_g_is_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // _G returns as empty (nil/false -> nil in RESP)
    let resp = client.command(&["EVAL", "return _G", "0"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_return_table_with_metatable_raise_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "local a = {}; setmetatable(a,{__index=function() foo() end}) return a",
            "0",
        ])
        .await;
    // Empty table -> nil
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_lua_integer_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Lua number 100.5 truncates to integer 100
    let resp = client.command(&["EVAL", "return 100.5", "0"]).await;
    assert_integer_eq(&resp, 100);
}

#[tokio::test]
async fn tcl_eval_lua_string_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return 'hello world'", "0"]).await;
    assert_bulk_eq(&resp, b"hello world");
}

#[tokio::test]
async fn tcl_eval_lua_true_boolean_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return true", "0"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn tcl_eval_lua_false_boolean_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return false", "0"]).await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_lua_status_reply_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return {ok='fine'}", "0"]).await;
    match &resp {
        Response::Simple(s) => assert_eq!(s.as_ref(), b"fine"),
        other => panic!("expected Simple status reply, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_lua_error_reply_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return {err='ERR this is an error'}", "0"])
        .await;
    assert_error_prefix(&resp, "ERR this is an error");
}

#[tokio::test]
async fn tcl_eval_lua_table_to_redis_protocol() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return {1,2,3,'ciao',{1,2}}", "0"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 2);
    assert_integer_eq(&items[2], 3);
    assert_bulk_eq(&items[3], b"ciao");
    // nested table {1,2}
    let nested = match &items[4] {
        Response::Array(a) => a,
        other => panic!("expected nested Array, got {other:?}"),
    };
    assert_eq!(nested.len(), 2);
    assert_integer_eq(&nested[0], 1);
    assert_integer_eq(&nested[1], 2);
}

#[tokio::test]
async fn tcl_eval_keys_and_argv_populated() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
            "2",
            "{t}a",
            "{t}b",
            "c",
            "d",
        ])
        .await;
    let strs = extract_bulk_strings(&resp);
    assert_eq!(strs, vec!["{t}a", "{t}b", "c", "d"]);
}

#[tokio::test]
async fn tcl_eval_lua_able_to_call_redis_api() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    let resp = client
        .command(&["EVAL", "return redis.call('get',KEYS[1])", "1", "mykey"])
        .await;
    assert_bulk_eq(&resp, b"myval");
}

// ---------------------------------------------------------------------------
// EVALSHA
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_evalsha_call_sha1_if_already_defined() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    // First EVAL to cache the script
    client
        .command(&["EVAL", "return redis.call('get',KEYS[1])", "1", "mykey"])
        .await;
    // SHA1 of "return redis.call('get',KEYS[1])"
    let resp = client
        .command(&[
            "EVALSHA",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_bulk_eq(&resp, b"myval");
}

#[tokio::test]
async fn tcl_evalsha_ro_call_sha1_if_already_defined() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    client
        .command(&["EVAL", "return redis.call('get',KEYS[1])", "1", "mykey"])
        .await;
    let resp = client
        .command(&[
            "EVALSHA_RO",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_bulk_eq(&resp, b"myval");
}

#[tokio::test]
async fn tcl_evalsha_uppercase_sha1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    client
        .command(&["EVAL", "return redis.call('get',KEYS[1])", "1", "mykey"])
        .await;
    let resp = client
        .command(&[
            "EVALSHA",
            "FD758D1589D044DD850A6F05D52F2EEFD27F033F",
            "1",
            "mykey",
        ])
        .await;
    assert_bulk_eq(&resp, b"myval");
}

#[tokio::test]
async fn tcl_evalsha_error_on_invalid_sha1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVALSHA", "NotValidShaSUM", "0"]).await;
    assert_error_prefix(&resp, "NOSCRIPT");
}

#[tokio::test]
async fn tcl_evalsha_error_on_non_defined_sha1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVALSHA", "ffd632c7d33e571e9f24556ebed26c3479a87130", "0"])
        .await;
    assert_error_prefix(&resp, "NOSCRIPT");
}

// ---------------------------------------------------------------------------
// EVAL — Redis type -> Lua type conversions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_redis_integer_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "0"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo}",
            "1",
            "x",
        ])
        .await;
    let strs = extract_bulk_strings(&resp);
    // type is "number", value converted to integer 1
    assert_eq!(strs[0], "number");
    // The second element is integer 1
    let items = unwrap_array(resp);
    assert_integer_eq(&items[1], 1);
}

#[tokio::test]
async fn tcl_eval_lua_number_to_redis_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "hash"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('hincrby','hash','field',200000000); return {type(foo),foo}",
            "0",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"number");
    assert_integer_eq(&items[1], 200000000);
}

#[tokio::test]
async fn tcl_eval_redis_bulk_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo}",
            "1",
            "mykey",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"string");
    assert_bulk_eq(&items[1], b"myval");
}

#[tokio::test]
async fn tcl_eval_redis_multi_bulk_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a"]).await;
    client.command(&["RPUSH", "mylist", "b"]).await;
    client.command(&["RPUSH", "mylist", "c"]).await;

    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('lrange',KEYS[1],0,-1); return {type(foo),foo[1],foo[2],foo[3],# foo}",
            "1",
            "mylist",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"table");
    assert_bulk_eq(&items[1], b"a");
    assert_bulk_eq(&items[2], b"b");
    assert_bulk_eq(&items[3], b"c");
    assert_integer_eq(&items[4], 3);
}

#[tokio::test]
async fn tcl_eval_redis_status_reply_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('set',KEYS[1],'myval'); return {type(foo),foo['ok']}",
            "1",
            "mykey",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"table");
    assert_bulk_eq(&items[1], b"OK");
}

#[tokio::test]
async fn tcl_eval_redis_error_reply_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('incr',KEYS[1]); return {type(foo),foo['err']}",
            "1",
            "mykey",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"table");
    // The error message should contain the INCR error
    let err_msg = unwrap_bulk(&items[1]);
    let err_str = std::str::from_utf8(err_msg).unwrap();
    assert!(
        err_str.contains("not an integer"),
        "expected error about 'not an integer', got: {err_str}"
    );
}

#[tokio::test]
async fn tcl_eval_redis_nil_bulk_reply_to_lua_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mykey"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "local foo = redis.pcall('get',KEYS[1]); return {type(foo),foo == false}",
            "1",
            "mykey",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_bulk_eq(&items[0], b"boolean");
    assert_integer_eq(&items[1], 1);
}

// ---------------------------------------------------------------------------
// EVAL — blocking commands in scripts return nil
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_blpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "l", "1"]).await;
    client.command(&["LPOP", "l"]).await;
    let resp = client
        .command(&["EVAL", "return redis.pcall('blpop','l',0)", "1", "l"])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_brpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "l", "1"]).await;
    client.command(&["LPOP", "l"]).await;
    let resp = client
        .command(&["EVAL", "return redis.pcall('brpop','l',0)", "1", "l"])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_brpoplpush() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "{t}empty_list1", "1"]).await;
    client.command(&["LPOP", "{t}empty_list1"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.pcall('brpoplpush','{t}empty_list1', '{t}empty_list2',0)",
            "2",
            "{t}empty_list1",
            "{t}empty_list2",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_blmove() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["LPUSH", "{t}empty_list1", "1"]).await;
    client.command(&["LPOP", "{t}empty_list1"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.pcall('blmove','{t}empty_list1', '{t}empty_list2', 'LEFT', 'LEFT', 0)",
            "2",
            "{t}empty_list1",
            "{t}empty_list2",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_bzpopmin() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "empty_zset", "10", "foo"]).await;
    client.command(&["ZMPOP", "1", "empty_zset", "MIN"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.pcall('bzpopmin','empty_zset', 0)",
            "1",
            "empty_zset",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_bzpopmax() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["ZADD", "empty_zset", "10", "foo"]).await;
    client.command(&["ZMPOP", "1", "empty_zset", "MIN"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.pcall('bzpopmax','empty_zset', 0)",
            "1",
            "empty_zset",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_scripts_do_not_block_on_wait() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return redis.pcall('wait','1','0')", "0"])
        .await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// EVAL — scripts can run non-deterministic commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_scripts_can_run_non_deterministic_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "redis.pcall('randomkey'); return redis.pcall('set','x','ciao')",
            "1",
            "x",
        ])
        .await;
    assert_ok(&resp);
}

// ---------------------------------------------------------------------------
// EVAL — error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_no_arguments_to_redis_call_is_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return redis.call()", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("one argument") || msg.contains("wrong number"),
                "expected error about arguments, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_redis_call_raises_error_on_unknown_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.call('nosuchcommand')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("Unknown") || msg.contains("unknown"),
                "expected unknown command error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_redis_call_raises_error_on_wrong_arg_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.call('get','a','b','c')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("number of arg") || msg.contains("wrong number"),
                "expected wrong args error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_redis_call_raises_error_on_wrongtype() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["EVAL", "redis.call('lpush',KEYS[1],'val')", "1", "foo"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("against a key") || msg.contains("WRONGTYPE"),
                "expected wrong type error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — JSON (cjson) support
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_json_numeric_decoding() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"return table.concat(cjson.decode("[0.0, -5e3, -1, 0.3e-3, 1023.2, 0e10]"), " ")"#,
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"0 -5000 -1 0.0003 1023.2 0");
}

#[tokio::test]
async fn tcl_eval_json_string_decoding() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"local decoded = cjson.decode('{"keya": "a", "keyb": "b"}'); return {decoded.keya, decoded.keyb}"#,
            "0",
        ])
        .await;
    let strs = extract_bulk_strings(&resp);
    assert_eq!(strs, vec!["a", "b"]);
}

#[tokio::test]
async fn tcl_eval_json_smoke_test() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Encode + decode round-trip without error
    let resp = client
        .command(&[
            "EVAL",
            r#"
            local some_map = {
                s1="Some string",
                n1=100,
                a1={"Some","String","Array"},
                b1=true,
                b2=false}
            local encoded = cjson.encode(some_map)
            local decoded = cjson.decode(encoded)
            assert(decoded.s1 == some_map.s1)
            assert(decoded.n1 == some_map.n1)
            return 'OK'
            "#,
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"OK");
}

// ---------------------------------------------------------------------------
// EVAL — bit library
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_numerical_sanity_check_from_bitop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"assert(0x7fffffff == 2147483647, "broken hex literals");
               assert(0xffffffff == -1 or 0xffffffff == 2^32-1, "broken hex literals");
               assert(tostring(-1) == "-1", "broken tostring()");
               assert(tostring(0xffffffff) == "-1" or tostring(0xffffffff) == "4294967295", "broken tostring()")"#,
            "0",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_verify_minimal_bitop_functionality() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"assert(bit.tobit(1) == 1);
               assert(bit.band(1) == 1);
               assert(bit.bxor(1,2) == 3);
               assert(bit.bor(1,2,4,8,16,32,64,128) == 255)"#,
            "0",
        ])
        .await;
    assert_nil(&resp);
}

#[tokio::test]
async fn tcl_eval_lua_bit_tohex_bug() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return bit.tohex(65535, -2147483648)", "0"])
        .await;
    assert_bulk_eq(&resp, b"0000FFFF");
    // Make sure connection is still alive
    let ping = client.command(&["PING"]).await;
    match &ping {
        Response::Simple(s) => assert_eq!(s.as_ref(), b"PONG"),
        other => panic!("expected PONG, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — trailing comments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_able_to_parse_trailing_comments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return 'hello' --trailing comment", "0"])
        .await;
    assert_bulk_eq(&resp, b"hello");
}

// ---------------------------------------------------------------------------
// EVAL_RO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_ro_successful_case() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["EVAL_RO", "return redis.call('get', KEYS[1]);", "1", "foo"])
        .await;
    assert_bulk_eq(&resp, b"bar");
}

#[tokio::test]
async fn tcl_eval_ro_cannot_run_write_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client
        .command(&["EVAL_RO", "redis.call('del', KEYS[1]);", "1", "foo"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("Write commands are not allowed")
                    || msg.contains("write")
                    || msg.contains("read-only"),
                "expected write-not-allowed error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SCRIPT FLUSH, SCRIPT EXISTS, SCRIPT LOAD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_script_flush_clears_cache() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Load a script
    client
        .command(&["SCRIPT", "LOAD", "return redis.call('get',KEYS[1])"])
        .await;
    // Verify it works via EVALSHA
    let resp = client
        .command(&[
            "EVALSHA",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_bulk_eq(&resp, b"myval");

    // Flush scripts
    client.command(&["SCRIPT", "FLUSH"]).await;

    // Now EVALSHA should fail
    let resp = client
        .command(&[
            "EVALSHA",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_error_prefix(&resp, "NOSCRIPT");

    // Re-cache via EVAL
    client
        .command(&["EVAL", "return redis.call('get',KEYS[1])", "1", "mykey"])
        .await;
    let resp = client
        .command(&[
            "EVALSHA",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_bulk_eq(&resp, b"myval");

    // Flush again
    client.command(&["SCRIPT", "FLUSH"]).await;
    let resp = client
        .command(&[
            "EVALSHA",
            "fd758d1589d044dd850a6f05d52f2eefd27f033f",
            "1",
            "mykey",
        ])
        .await;
    assert_error_prefix(&resp, "NOSCRIPT");
}

#[tokio::test]
async fn tcl_script_flush_async() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Load some scripts
    for j in 0..10 {
        client
            .command(&["SCRIPT", "LOAD", &format!("return {j}")])
            .await;
    }
    // Flush async
    let resp = client.command(&["SCRIPT", "FLUSH", "ASYNC"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn tcl_script_exists_detects_defined_scripts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // "return 1+1" should be cached after EVAL
    client.command(&["EVAL", "return 1+1", "0"]).await;

    let resp = client
        .command(&[
            "SCRIPT",
            "EXISTS",
            "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bd9",
            "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bda",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_integer_eq(&items[0], 1); // exists
    assert_integer_eq(&items[1], 0); // does not exist
}

#[tokio::test]
async fn tcl_script_load_registers_in_cache() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SCRIPT", "LOAD", "return 'loaded'"]).await;
    assert_bulk_eq(&resp, b"b534286061d4b9e4026607613b95c06c06015ae8");

    let resp = client
        .command(&["EVALSHA", "b534286061d4b9e4026607613b95c06c06015ae8", "0"])
        .await;
    assert_bulk_eq(&resp, b"loaded");
}

// ---------------------------------------------------------------------------
// redis.sha1hex()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_redis_sha1hex_implementation() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return redis.sha1hex('')", "0"])
        .await;
    assert_bulk_eq(&resp, b"da39a3ee5e6b4b0d3255bfef95601890afd80709");

    let resp = client
        .command(&["EVAL", "return redis.sha1hex('Pizza & Mandolino')", "0"])
        .await;
    assert_bulk_eq(&resp, b"74822d82031af7493c20eefa13bd07ec4fada82f");
}

// ---------------------------------------------------------------------------
// EVAL — practical script example: DECR_IF_GT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_decr_if_gt_example() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let decr_if_gt = r#"
        local current
        current = redis.call('get',KEYS[1])
        if not current then return nil end
        if current > ARGV[1] then
            return redis.call('decr',KEYS[1])
        else
            return redis.call('get',KEYS[1])
        end
    "#;

    client.command(&["SET", "foo", "5"]).await;

    let r1 = client.command(&["EVAL", decr_if_gt, "1", "foo", "2"]).await;
    assert_integer_eq(&r1, 4);
    let r2 = client.command(&["EVAL", decr_if_gt, "1", "foo", "2"]).await;
    assert_integer_eq(&r2, 3);
    let r3 = client.command(&["EVAL", decr_if_gt, "1", "foo", "2"]).await;
    assert_integer_eq(&r3, 2);
    // At "2", not greater than "2", so GET returns "2"
    let r4 = client.command(&["EVAL", decr_if_gt, "1", "foo", "2"]).await;
    assert_bulk_eq(&r4, b"2");
    let r5 = client.command(&["EVAL", decr_if_gt, "1", "foo", "2"]).await;
    assert_bulk_eq(&r5, b"2");
}

// ---------------------------------------------------------------------------
// EVAL — call Redis with many args (issue #1764)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_call_redis_with_many_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            local i
            local x={}
            redis.call('del','mylist')
            for i=1,100 do
                table.insert(x,i)
            end
            redis.call('rpush','mylist',unpack(x))
            return redis.call('lrange','mylist',0,-1)
            "#,
            "1",
            "mylist",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 100);
    // Check first and last
    assert_bulk_eq(&items[0], b"1");
    assert_bulk_eq(&items[99], b"100");
}

// ---------------------------------------------------------------------------
// EVAL — number conversion precision (issue #1118)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_number_conversion_precision() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            local value = 9007199254740991
            redis.call("set","foo",value)
            return redis.call("get","foo")
            "#,
            "1",
            "foo",
        ])
        .await;
    assert_bulk_eq(&resp, b"9007199254740991");
}

#[tokio::test]
async fn tcl_eval_string_containing_number_precision() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            redis.call("set", "key", "12039611435714932082")
            return redis.call("get", "key")
            "#,
            "1",
            "key",
        ])
        .await;
    assert_bulk_eq(&resp, b"12039611435714932082");
}

// ---------------------------------------------------------------------------
// EVAL — negative arg count is error (issue #1842)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_negative_numkeys_is_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return 'hello'", "-12"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// EVAL — incorrect arity from scripts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_scripts_handle_commands_with_incorrect_arity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.call('set','invalid')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("wrong number") || msg.contains("Wrong number"),
                "expected wrong number of args error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }

    let resp = client.command(&["EVAL", "redis.call('incr')", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("wrong number") || msg.contains("Wrong number"),
                "expected wrong number of args error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — correct handling of reused argv (issue #1939)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_correct_handling_of_reused_argv() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Should complete without error
    let resp = client
        .command(&[
            "EVAL",
            r#"
            for i = 0, 10 do
                redis.call('SET', '{t}a', '1')
                redis.call('MGET', '{t}a', '{t}b', '{t}c')
                redis.call('EXPIRE', '{t}a', 0)
                redis.call('GET', '{t}a')
                redis.call('MGET', '{t}a', '{t}b', '{t}c')
            end
            "#,
            "3",
            "{t}a",
            "{t}b",
            "{t}c",
        ])
        .await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// EVAL — redis.sha1hex() error on wrong args
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_sha1hex_wrong_number_of_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "redis.sha1hex()", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("wrong number") || msg.contains("argument"),
                "expected wrong number error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — CLUSTER RESET cannot be invoked from script
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_cluster_reset_not_allowed_from_script() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.call('cluster', 'reset', 'hard')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("not allowed") || msg.contains("denied"),
                "expected not-allowed error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — script check unpack with massive arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_script_unpack_massive_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            local a = {}
            for i=1,7999 do
                a[i] = 1
            end
            return redis.call("lpush", "l", unpack(a))
            "#,
            "1",
            "l",
        ])
        .await;
    assert_integer_eq(&resp, 7999);
}

// ---------------------------------------------------------------------------
// EVAL — script read key with expiration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_script_read_key_with_expiration_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key", "value", "EX", "10"]).await;
    let resp = client
        .command(&[
            "EVAL",
            r#"
            if redis.call("EXISTS", "key") then
                return redis.call("GET", "key")
            else
                return redis.call("EXISTS", "key")
            end
            "#,
            "1",
            "key",
        ])
        .await;
    assert_bulk_eq(&resp, b"value");
}

#[tokio::test]
async fn tcl_eval_script_del_key_with_expiration_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "key", "value", "EX", "10"]).await;
    let resp = client
        .command(&[
            "EVAL",
            r#"
            redis.call("DEL", "key")
            return redis.call("EXISTS", "key")
            "#,
            "1",
            "key",
        ])
        .await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// Globals protection / Lua sandbox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_globals_protection_reading_undeclared_variable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "return a", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("global") || msg.contains("access"),
                "expected global access error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_globals_protection_setting_undeclared_variable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "a=10", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly table error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Global protection tricks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_try_trick_global_protection_setmetatable_g() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "setmetatable(_G, {})", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_global_protection_modify_metatable_index() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "local g = getmetatable(_G); g.__index = {}", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_global_protection_replace_redis() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis = function() return 1 end", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_global_protection_replace_g() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "_G = {}", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_readonly_redis_table() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.call = function() return 1 end", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_readonly_cjson_table() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "cjson.encode = function() return 1 end", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_try_trick_readonly_bit_table() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "bit.lshift = function() return 1 end", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Sandbox: loadfile, dofile, print not available
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_loadfile_not_available() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "loadfile('some file')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("global") || msg.contains("nonexistent") || msg.contains("nil"),
                "expected global access error for loadfile, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_dofile_not_available() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "dofile('some file')", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("global") || msg.contains("nonexistent") || msg.contains("nil"),
                "expected global access error for dofile, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_print_not_available() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "print('some data')", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("global") || msg.contains("nonexistent") || msg.contains("nil"),
                "expected global access error for print, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Sandbox: dangerous os methods
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_prohibit_dangerous_lua_os_methods() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // os.execute should not be available
    let resp = client.command(&["EVAL", "os.execute()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // os.exit should not be available
    let resp = client.command(&["EVAL", "os.exit()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // os.getenv should not be available
    let resp = client.command(&["EVAL", "os.getenv()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // os.remove should not be available
    let resp = client.command(&["EVAL", "os.remove()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // os.rename should not be available
    let resp = client.command(&["EVAL", "os.rename()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // os.tmpname should not be available
    let resp = client.command(&["EVAL", "os.tmpname()", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));
}

// ---------------------------------------------------------------------------
// Shebang support
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_shebang_support_for_lua_engine() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Unsupported engine
    let resp = client.command(&["EVAL", "#!not-lua\nreturn 1", "0"]).await;
    assert!(matches!(&resp, Response::Error(_)));

    // Supported lua engine
    let resp = client.command(&["EVAL", "#!lua\nreturn 1", "0"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn tcl_eval_unknown_shebang_option() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "#!lua badger=data\nreturn 1", "0"])
        .await;
    assert!(matches!(&resp, Response::Error(_)));
}

#[tokio::test]
async fn tcl_eval_unknown_shebang_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "#!lua flags=allow-oom,what?\nreturn 1", "0"])
        .await;
    assert!(matches!(&resp, Response::Error(_)));
}

#[tokio::test]
async fn tcl_eval_no_writes_shebang_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "#!lua flags=no-writes\nredis.call('set','x',1)\nreturn 1",
            "1",
            "x",
        ])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("Write commands are not allowed")
                    || msg.contains("write")
                    || msg.contains("read-only"),
                "expected write-not-allowed error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — redis.error_reply and redis.status_reply
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_redis_error_reply_api() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "return redis.error_reply('MY_ERR_CODE custom msg')",
            "0",
        ])
        .await;
    assert_error_prefix(&resp, "MY_ERR_CODE custom msg");
}

#[tokio::test]
async fn tcl_eval_redis_error_reply_api_empty_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return redis.error_reply('')", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_eval_redis_status_reply_api() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "return redis.status_reply('MY_OK_CODE custom msg')",
            "0",
        ])
        .await;
    match &resp {
        Response::Simple(s) => {
            assert_eq!(s.as_ref(), b"MY_OK_CODE custom msg");
        }
        other => panic!("expected Simple status reply, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — pcall behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_lua_pcall() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "local status, res = pcall(function() return 1 end); return 'status: ' .. tostring(status) .. ' result: ' .. res",
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"status: true result: 1");
}

#[tokio::test]
async fn tcl_eval_lua_pcall_with_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "local status, res = pcall(function() return foo end); return 'status: ' .. tostring(status) .. ' result: ' .. res",
            "0",
        ])
        .await;
    let val = std::str::from_utf8(unwrap_bulk(&resp)).unwrap();
    assert!(
        val.starts_with("status: false result:"),
        "unexpected: {val}"
    );
    assert!(
        val.contains("global") || val.contains("nonexistent") || val.contains("foo"),
        "expected error about accessing 'foo', got: {val}"
    );
}

#[tokio::test]
async fn tcl_eval_lua_pcall_with_non_string_arg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            local x={}
            return redis.call("ping", x)
            "#,
            "0",
        ])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("string") || msg.contains("integer"),
                "expected arg type error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }

    // Make sure connection still works afterward
    let resp = client
        .command(&["EVAL", r#"return redis.call("ping", "asdf")"#, "0"])
        .await;
    assert_bulk_eq(&resp, b"asdf");
}

// ---------------------------------------------------------------------------
// EVAL — explicit error() call handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_explicit_error_call_simple_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "error('simple string error')", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("simple string error"),
                "expected error containing 'simple string error', got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_explicit_error_call_table_err() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "error({err='ERR table error'})", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("table error"),
                "expected error containing 'table error', got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_explicit_error_call_empty_table() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["EVAL", "error({})", "0"]).await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("unknown error") || msg.contains("ERR"),
                "expected unknown error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — random numbers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_random_numbers_are_random() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r1 = client
        .command(&["EVAL", "return tostring(math.random())", "0"])
        .await;
    let v1 = std::str::from_utf8(unwrap_bulk(&r1)).unwrap().to_string();

    // Try several times to get a different value (it is random, so one should differ)
    let mut found_different = false;
    for _ in 0..20 {
        let r2 = client
            .command(&["EVAL", "return tostring(math.random())", "0"])
            .await;
        let v2 = std::str::from_utf8(unwrap_bulk(&r2)).unwrap().to_string();
        if v1 != v2 {
            found_different = true;
            break;
        }
    }
    assert!(
        found_different,
        "random numbers should vary, but got {v1} repeatedly"
    );
}

#[tokio::test]
async fn tcl_eval_prng_can_be_seeded_correctly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r1 = client
        .command(&[
            "EVAL",
            "math.randomseed(ARGV[1]); return tostring(math.random())",
            "0",
            "10",
        ])
        .await;
    let v1 = std::str::from_utf8(unwrap_bulk(&r1)).unwrap().to_string();

    let r2 = client
        .command(&[
            "EVAL",
            "math.randomseed(ARGV[1]); return tostring(math.random())",
            "0",
            "10",
        ])
        .await;
    let v2 = std::str::from_utf8(unwrap_bulk(&r2)).unwrap().to_string();

    let r3 = client
        .command(&[
            "EVAL",
            "math.randomseed(ARGV[1]); return tostring(math.random())",
            "0",
            "20",
        ])
        .await;
    let v3 = std::str::from_utf8(unwrap_bulk(&r3)).unwrap().to_string();

    assert_eq!(v1, v2, "same seed should produce same random value");
    assert_ne!(v2, v3, "different seeds should produce different values");
}

// ---------------------------------------------------------------------------
// EVAL — SORT from scripts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_sort_not_alpha_reordered_for_scripting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client
        .command(&["SADD", "myset", "1", "2", "3", "4", "10"])
        .await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('sort',KEYS[1],'desc')",
            "1",
            "myset",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);
    // Numeric desc: 10, 4, 3, 2, 1
    assert_bulk_eq(&items[0], b"10");
    assert_bulk_eq(&items[1], b"4");
    assert_bulk_eq(&items[2], b"3");
    assert_bulk_eq(&items[3], b"2");
    assert_bulk_eq(&items[4], b"1");
}

// ---------------------------------------------------------------------------
// EVAL — table unpack with invalid indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_table_unpack_with_invalid_indexes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Unpack with huge range should error
    let resp = client
        .command(&["EVAL", "return {unpack({1,2,3}, -2, 2147483647)}", "0"])
        .await;
    assert!(matches!(&resp, Response::Error(_)));

    let resp = client
        .command(&["EVAL", "return {unpack({1,2,3}, 0, 2147483647)}", "0"])
        .await;
    assert!(matches!(&resp, Response::Error(_)));

    // Valid negative range producing empty result
    let resp = client
        .command(&["EVAL", "return {unpack({1,2,3}, -1, -2)}", "0"])
        .await;
    assert_nil(&resp);

    let resp = client
        .command(&["EVAL", "return {unpack({1,2,3}, 1, -1)}", "0"])
        .await;
    assert_nil(&resp);
}

// ---------------------------------------------------------------------------
// EVAL — cjson empty array handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_json_empty_array_default_behavior() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Default behavior: empty JSON array decodes as empty Lua table, encodes as {}
    let resp = client
        .command(&["EVAL", "return cjson.encode(cjson.decode('[]'))", "0"])
        .await;
    assert_bulk_eq(&resp, b"{}");
}

#[tokio::test]
async fn tcl_eval_json_empty_array_with_array_mt() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // With array metatable, empty array stays as []
    let resp = client
        .command(&[
            "EVAL",
            "cjson.decode_array_with_array_mt(true); return cjson.encode(cjson.decode('[]'))",
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"[]");
}

#[tokio::test]
async fn tcl_eval_cjson_array_metatable_is_readonly() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            cjson.decode_array_with_array_mt(true)
            local t = cjson.decode('[]')
            getmetatable(t).__is_cjson_array = function() return 1 end
            return cjson.encode(t)
            "#,
            "0",
        ])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("readonly") || msg.contains("modify"),
                "expected readonly error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — cmsgpack basic tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_cmsgpack_can_pack_double() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"local encoded = cmsgpack.pack(0.1)
               local h = ""
               for i = 1, #encoded do
                   h = h .. string.format("%02x",string.byte(encoded,i))
               end
               return h"#,
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"cb3fb999999999999a");
}

#[tokio::test]
async fn tcl_eval_cmsgpack_can_pack_negative_int64() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"local encoded = cmsgpack.pack(-1099511627776)
               local h = ""
               for i = 1, #encoded do
                   h = h .. string.format("%02x",string.byte(encoded,i))
               end
               return h"#,
            "0",
        ])
        .await;
    assert_bulk_eq(&resp, b"d3ffffff0000000000");
}

// ---------------------------------------------------------------------------
// EVAL — os.clock is available
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_os_clock_available() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // os.clock() should be available and return a number
    let resp = client
        .command(&[
            "EVAL",
            "local t = os.clock(); if type(t) == 'number' then return 1 else return 0 end",
            "0",
        ])
        .await;
    assert_integer_eq(&resp, 1);
}

// ---------------------------------------------------------------------------
// EVAL — redis.set_repl invalid values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_redis_set_repl_invalid_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "redis.set_repl(12345);", "0"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("Invalid") || msg.contains("flags"),
                "expected invalid flags error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// EVAL — redis.replicate_commands() compat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_redis_replicate_commands_returns_1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "redis.call('set','foo','bar'); return redis.replicate_commands();",
            "0",
        ])
        .await;
    assert_integer_eq(&resp, 1);
}

// ---------------------------------------------------------------------------
// EVAL — binary code loading fails
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_binary_code_loading_fails() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            "return loadstring(string.dump(function() return 1 end))()",
            "0",
        ])
        .await;
    assert!(matches!(&resp, Response::Error(_)));
}

// ---------------------------------------------------------------------------
// EVAL — consistent error reporting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_eval_error_on_db_index_out_of_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL", "return redis.call('select',99)", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_eval_pcall_returns_error_table_for_select_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL",
            r#"
            local t = redis.pcall('select',99)
            if t['err'] ~= nil and string.find(t['err'], 'DB index') then
                return 1
            else
                return 0
            end
            "#,
            "0",
        ])
        .await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn tcl_eval_ro_write_command_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["EVAL_RO", "return redis.call('set','x','y')", "1", "x"])
        .await;
    match &resp {
        Response::Error(e) => {
            let msg = String::from_utf8_lossy(e);
            assert!(
                msg.contains("Write commands are not allowed")
                    || msg.contains("write")
                    || msg.contains("read-only"),
                "expected write-not-allowed error, got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn tcl_eval_ro_pcall_write_command_returns_error_table() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "EVAL_RO",
            r#"
            local t = redis.pcall('set','x','y')
            if t['err'] ~= nil and (string.find(t['err'], 'Write commands') or string.find(t['err'], 'read%-only')) then
                return 1
            else
                return 0
            end
            "#,
            "1",
            "x",
        ])
        .await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn tcl_eval_wrongtype_error_from_script() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "Sicily", "1"]).await;
    let resp = client
        .command(&[
            "EVAL",
            "return redis.call('GEOADD', 'Sicily', '13.361389', '38.115556', 'Palermo', '15.087269', '37.502669', 'Catania')",
            "1",
            "Sicily",
        ])
        .await;
    assert_error_prefix(&resp, "WRONGTYPE");
}
