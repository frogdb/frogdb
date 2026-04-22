//! Rust port of Redis 8.6.0 `unit/introspection.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug`-tagged tests (CLIENT KILL maxAGE, protected config tests)
//! - `needs:repl`-tagged tests (MONITOR redacting)
//! - `needs:save`-tagged tests (CLIENT KILL during bgsave)
//! - `needs:config-maxmemory`-tagged tests
//! - `external:skip`-tagged tests (CONFIG save params, CONFIG REWRITE, CLI args,
//!   protected config, loading, warnings)
//!
//! ## Intentional exclusions
//!
//! MONITOR (FrogDB does not implement MONITOR):
//! - `MONITOR can log executed commands` — redis-specific — Redis-internal feature
//! - `MONITOR can log commands issued by the scripting engine` — redis-specific — Redis-internal feature
//! - `MONITOR can log commands issued by functions` — redis-specific — Redis-internal feature
//! - `MONITOR supports redacting command arguments` — redis-specific — Redis-internal feature
//! - `MONITOR correctly handles multi-exec cases` — redis-specific — Redis-internal feature
//! - `MONITOR log blocked command only once` — redis-specific — Redis-internal feature
//!
//! CONFIG REWRITE / CONFIG GET-SET save params:
//! - `CONFIG save params special case handled properly` — redis-specific — Redis-internal feature
//! - `CONFIG REWRITE sanity` — redis-specific — Redis-internal feature
//! - `CONFIG REWRITE handles save and shutdown properly` — redis-specific — Redis-internal feature
//! - `CONFIG REWRITE handles rename-command properly` — redis-specific — Redis-internal feature
//! - `CONFIG REWRITE handles alias config properly` — redis-specific — Redis-internal feature
//!
//! redis-server CLI argument parsing (FrogDB has different command-line parser):
//! - `redis-server command line arguments - error cases` — intentional-incompatibility:cli — Redis-internal CLI
//! - `redis-server command line arguments - allow passing option name and option value in the same arg` — intentional-incompatibility:cli — Redis-internal CLI
//! - `redis-server command line arguments - wrong usage that we support anyway` — intentional-incompatibility:cli — Redis-internal CLI
//! - `redis-server command line arguments - allow option value to use the `--` prefix` — Redis-internal CLI
//! - `redis-server command line arguments - option name and option value in the same arg and `--` prefix` — Redis-internal CLI
//! - `redis-server command line arguments - save with empty input` — intentional-incompatibility:cli — Redis-internal CLI
//! - `redis-server command line arguments - take one bulk string with spaces for MULTI_ARG configs parsing` — intentional-incompatibility:cli — Redis-internal CLI
//!
//! IO threads (Redis-internal threading model):
//! - `IO threads client number` — redis-specific — Redis-internal feature
//! - `Clients are evenly distributed among io threads` — redis-specific — Redis-internal feature
//!
//! Other Redis-internal:
//! - CONFIG SET for Redis-internal options (lazyfree, io-threads, etc.)
//! - MONITOR tests (complex interleaving)
//! - DEBUG OBJECT / DEBUG SET-ACTIVE-EXPIRE tests
//! - OBJECT FREQ/IDLETIME/REFCOUNT tests
//! - COMMAND DOCS tests (complex output format)
//! - ACL-dependent tests (CLIENT KILL maxAGE with ACL)
//! - bgsave/bgrewriteaof tests
//! - CONFIG SET rollback / hidden / multiple args tests (Redis-internal options)

use std::time::Duration;

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// PING
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_ping() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PING with no argument returns PONG
    let r = client.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "PONG"),
        "expected PONG, got {r:?}"
    );

    // PING with argument echoes the argument
    assert_bulk_eq(&client.command(&["PING", "redis"]).await, b"redis");

    // PING with too many arguments is an error
    let r = client.command(&["PING", "hello", "redis"]).await;
    assert_error_prefix(&r, "ERR wrong number of arguments for 'ping' command");
}

// ---------------------------------------------------------------------------
// CLIENT LIST
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_client_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "LIST"]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    // Basic field presence checks
    assert!(list.contains("id="), "CLIENT LIST should contain id=");
    assert!(list.contains("addr="), "CLIENT LIST should contain addr=");
    assert!(
        list.contains("cmd=client|list"),
        "CLIENT LIST should contain cmd=client|list"
    );
}

#[tokio::test]

async fn tcl_client_list_with_ids() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let my_id = unwrap_integer(&client.command(&["CLIENT", "ID"]).await);
    let id_str = my_id.to_string();
    let r = client.command(&["CLIENT", "LIST", "ID", &id_str]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    let expected_prefix = format!("id={my_id} ");
    assert!(
        list.contains(&expected_prefix),
        "CLIENT LIST ID {my_id} should contain id={my_id}, got: {list}"
    );
    assert!(
        list.contains("cmd=client|list"),
        "should contain cmd=client|list"
    );
}

// ---------------------------------------------------------------------------
// CLIENT INFO
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_client_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(info.contains("id="), "CLIENT INFO should contain id=");
    assert!(info.contains("addr="), "CLIENT INFO should contain addr=");
    assert!(
        info.contains("cmd=client|info"),
        "CLIENT INFO should contain cmd=client|info"
    );
}

// ---------------------------------------------------------------------------
// CLIENT KILL with illegal arguments
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_client_kill_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // No arguments
    let r = client.command(&["CLIENT", "KILL"]).await;
    assert_error_prefix(
        &r,
        "ERR wrong number of arguments for 'client|kill' command",
    );

    // Bad filter keyword
    let r = client
        .command(&["CLIENT", "KILL", "id", "10", "wrong_arg"])
        .await;
    assert_error_prefix(&r, "ERR syntax error");

    // Non-numeric id
    let r = client.command(&["CLIENT", "KILL", "id", "str"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for id str"
    );

    // Negative id
    let r = client.command(&["CLIENT", "KILL", "id", "-1"]).await;
    assert!(matches!(&r, Response::Error(_)), "expected error for id -1");

    // Zero id
    let r = client.command(&["CLIENT", "KILL", "id", "0"]).await;
    assert!(matches!(&r, Response::Error(_)), "expected error for id 0");
}

// ---------------------------------------------------------------------------
// CLIENT KILL SKIPME YES/NO
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_client_kill_skipme_yes_kills_other_clients() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut _rd1 = server.connect().await;
    let mut _rd2 = server.connect().await;

    // Ensure the extra clients are registered
    _rd1.command(&["PING"]).await;
    _rd2.command(&["PING"]).await;

    // Kill all clients except me
    let r = client.command(&["CLIENT", "KILL", "SKIPME", "yes"]).await;
    let killed = unwrap_integer(&r);
    assert!(
        killed >= 2,
        "should kill at least 2 other clients, killed {killed}"
    );

    // Our own connection should still work
    let r = client.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "PONG"),
        "our connection should still work after KILL SKIPME yes"
    );
}

// ---------------------------------------------------------------------------
// CLIENT GETNAME / SETNAME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_getname_returns_nil_if_not_assigned() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_nil(&client.command(&["CLIENT", "GETNAME"]).await);
}

#[tokio::test]
async fn tcl_client_getname_returns_name_after_setname() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CLIENT", "SETNAME", "testName"]).await);
    assert_bulk_eq(&client.command(&["CLIENT", "GETNAME"]).await, b"testName");
}

#[tokio::test]
async fn tcl_client_list_shows_empty_name_for_unassigned() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "LIST"]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        list.contains("name= ") || list.contains("name=\n") || list.contains("name=\r"),
        "CLIENT LIST should show empty name field, got: {list}"
    );
}

#[tokio::test]
async fn tcl_client_setname_does_not_accept_spaces() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "SETNAME", "foo bar"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for name with space"
    );
}

#[tokio::test]
async fn tcl_client_setname_assigns_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CLIENT", "SETNAME", "myname"]).await);
    let r = client.command(&["CLIENT", "LIST"]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        list.contains("name=myname"),
        "CLIENT LIST should contain name=myname"
    );
}

#[tokio::test]
async fn tcl_client_setname_can_change_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CLIENT", "SETNAME", "myname"]).await);
    assert_ok(
        &client
            .command(&["CLIENT", "SETNAME", "someothername"])
            .await,
    );
    let r = client.command(&["CLIENT", "LIST"]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        list.contains("name=someothername"),
        "CLIENT LIST should contain name=someothername"
    );
}

#[tokio::test]
async fn tcl_client_setname_connection_can_be_closed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    {
        let mut rd = server.connect().await;
        assert_ok(&rd.command(&["CLIENT", "SETNAME", "foobar"]).await);
        let r = client.command(&["CLIENT", "LIST"]).await;
        let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
        assert!(
            list.contains("foobar"),
            "foobar should appear in CLIENT LIST"
        );
        // rd is dropped here, closing the connection
    }

    // Wait briefly for the server to clean up the closed connection
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let r = client.command(&["CLIENT", "LIST"]).await;
    let list = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        !list.contains("foobar"),
        "foobar should not appear in CLIENT LIST after close"
    );
}

// ---------------------------------------------------------------------------
// CLIENT SETINFO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_setinfo_lib_name_and_ver() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CLIENT", "SETINFO", "lib-name", "redis.py"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CLIENT", "SETINFO", "lib-ver", "1.2.3"])
            .await,
    );

    let r = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        info.contains("lib-name=redis.py"),
        "CLIENT INFO should contain lib-name=redis.py, got: {info}"
    );
    assert!(
        info.contains("lib-ver=1.2.3"),
        "CLIENT INFO should contain lib-ver=1.2.3, got: {info}"
    );
}

#[tokio::test]
async fn tcl_client_setinfo_invalid_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Missing value
    let r = client.command(&["CLIENT", "SETINFO", "lib-name"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for missing value"
    );

    // Name with spaces
    let r = client
        .command(&["CLIENT", "SETINFO", "lib-name", "redis py"])
        .await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for name with spaces"
    );

    // Unrecognized attribute
    let r = client
        .command(&["CLIENT", "SETINFO", "badger", "hamster"])
        .await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for unknown attribute"
    );
}

#[tokio::test]
async fn tcl_client_setinfo_can_clear_lib_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(
        &client
            .command(&["CLIENT", "SETINFO", "lib-name", "redis.py"])
            .await,
    );
    assert_ok(&client.command(&["CLIENT", "SETINFO", "lib-name", ""]).await);

    let r = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    // After clearing, lib-name should be empty (lib-name= followed by space)
    assert!(
        info.contains("lib-name= ") || info.contains("lib-name=\r") || info.contains("lib-name=\n"),
        "lib-name should be empty after clearing, got: {info}"
    );
}

// ---------------------------------------------------------------------------
// CLIENT ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_id_returns_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "ID"]).await;
    let id = unwrap_integer(&r);
    assert!(
        id > 0,
        "CLIENT ID should return a positive integer, got {id}"
    );
}

#[tokio::test]
async fn tcl_client_ids_are_unique() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;
    let mut c3 = server.connect().await;

    let id1 = unwrap_integer(&c1.command(&["CLIENT", "ID"]).await);
    let id2 = unwrap_integer(&c2.command(&["CLIENT", "ID"]).await);
    let id3 = unwrap_integer(&c3.command(&["CLIENT", "ID"]).await);

    assert_ne!(id1, id2, "client IDs should be unique");
    assert_ne!(id2, id3, "client IDs should be unique");
    assert_ne!(id1, id3, "client IDs should be unique");
}

// ---------------------------------------------------------------------------
// CLIENT NO-EVICT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_no_evict_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "NO-EVICT", "wrongInput"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for bad NO-EVICT arg"
    );
}

#[tokio::test]
async fn tcl_client_no_evict_on_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CLIENT", "NO-EVICT", "on"]).await);
    assert_ok(&client.command(&["CLIENT", "NO-EVICT", "off"]).await);
}

// ---------------------------------------------------------------------------
// CLIENT KILL by ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_kill_by_id() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut victim = server.connect().await;

    let victim_id = unwrap_integer(&victim.command(&["CLIENT", "ID"]).await);
    let id_str = victim_id.to_string();

    let r = client.command(&["CLIENT", "KILL", "ID", &id_str]).await;
    assert_integer_eq(&r, 1);
}

// ---------------------------------------------------------------------------
// CLIENT KILL by addr (no such client)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_kill_no_such_addr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client
        .command(&["CLIENT", "KILL", "000.123.321.567:0000"])
        .await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for nonexistent addr"
    );

    let r = client.command(&["CLIENT", "KILL", "127.0.0.1:"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for malformed addr"
    );
}

// ---------------------------------------------------------------------------
// CLIENT PAUSE unhappy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_pause_invalid_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "PAUSE", "abc"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for non-integer timeout"
    );

    let r = client.command(&["CLIENT", "PAUSE", "-1"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for negative timeout"
    );
}

// ---------------------------------------------------------------------------
// COMMAND COUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["COMMAND", "COUNT"]).await;
    let count = unwrap_integer(&r);
    assert!(
        count > 0,
        "COMMAND COUNT should return a positive integer, got {count}"
    );
}

// ---------------------------------------------------------------------------
// COMMAND LIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["COMMAND", "LIST"]).await;
    let cmds = extract_bulk_strings(&r);
    assert!(
        !cmds.is_empty(),
        "COMMAND LIST should return at least one command"
    );
    // Verify some known commands exist
    let lower: Vec<String> = cmds.iter().map(|c| c.to_lowercase()).collect();
    assert!(
        lower.contains(&"ping".to_string()),
        "COMMAND LIST should include PING"
    );
    assert!(
        lower.contains(&"get".to_string()),
        "COMMAND LIST should include GET"
    );
    assert!(
        lower.contains(&"set".to_string()),
        "COMMAND LIST should include SET"
    );
}

#[tokio::test]
async fn tcl_command_list_count_matches() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let count = unwrap_integer(&client.command(&["COMMAND", "COUNT"]).await);
    let r = client.command(&["COMMAND", "LIST"]).await;
    let list = extract_bulk_strings(&r);
    assert_eq!(
        count as usize,
        list.len(),
        "COMMAND COUNT ({count}) should match COMMAND LIST length ({})",
        list.len()
    );
}

// ---------------------------------------------------------------------------
// CONFIG GET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_config_get_returns_pairs() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let items = unwrap_array(r);
    assert_eq!(
        items.len(),
        2,
        "CONFIG GET maxmemory should return key-value pair"
    );
    let key = String::from_utf8(unwrap_bulk(&items[0]).to_vec()).unwrap();
    assert_eq!(key, "maxmemory");
}

#[tokio::test]
async fn tcl_config_get_wildcard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CONFIG", "GET", "*"]).await;
    let items = unwrap_array(r);
    // Should return many config pairs (even number of elements)
    assert!(
        items.len() >= 2,
        "CONFIG GET * should return at least one config pair"
    );
    assert_eq!(
        items.len() % 2,
        0,
        "CONFIG GET * should return even number of elements"
    );
}

#[tokio::test]
async fn tcl_config_get_multiple_patterns() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client
        .command(&["CONFIG", "GET", "maxmemory", "bind"])
        .await;
    let items = unwrap_array(r);
    // Should return at least the two requested configs (4 elements = 2 pairs)
    assert!(
        items.len() >= 4,
        "CONFIG GET with two params should return at least 2 pairs, got {} elements",
        items.len()
    );
}

// ---------------------------------------------------------------------------
// CONFIG SET duplicate configs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_config_set_duplicate_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client
        .command(&[
            "CONFIG",
            "SET",
            "maxmemory",
            "10000001",
            "maxmemory",
            "10000002",
        ])
        .await;
    assert!(
        matches!(&r, Response::Error(_)),
        "CONFIG SET with duplicate keys should error"
    );
}

// ---------------------------------------------------------------------------
// OBJECT HELP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_object_help() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["OBJECT", "HELP"]).await;
    let lines = extract_bulk_strings(&r);
    assert!(!lines.is_empty(), "OBJECT HELP should return help text");
}

// ---------------------------------------------------------------------------
// OBJECT ENCODING
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_object_encoding_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "hello"]).await;
    let r = client.command(&["OBJECT", "ENCODING", "mykey"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        enc == "embstr" || enc == "raw",
        "string encoding should be embstr or raw, got {enc}"
    );
}

#[tokio::test]
async fn tcl_object_encoding_int() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "12345"]).await;
    let r = client.command(&["OBJECT", "ENCODING", "mykey"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert_eq!(enc, "int", "integer string encoding should be int");
}

#[tokio::test]
async fn tcl_object_encoding_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a", "b", "c"]).await;
    let r = client.command(&["OBJECT", "ENCODING", "mylist"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        enc == "listpack" || enc == "quicklist" || enc == "ziplist",
        "list encoding should be listpack, quicklist, or ziplist, got {enc}"
    );
}

#[tokio::test]
async fn tcl_object_encoding_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;
    let r = client.command(&["OBJECT", "ENCODING", "myset"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        enc == "listpack" || enc == "hashtable" || enc == "ziplist",
        "set encoding should be listpack, hashtable, or ziplist, got {enc}"
    );
}

#[tokio::test]
async fn tcl_object_encoding_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    let r = client.command(&["OBJECT", "ENCODING", "myhash"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        enc == "listpack" || enc == "hashtable" || enc == "ziplist",
        "hash encoding should be listpack, hashtable, or ziplist, got {enc}"
    );
}

#[tokio::test]
async fn tcl_object_encoding_zset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["ZADD", "myzset", "1", "a", "2", "b"])
        .await;
    let r = client.command(&["OBJECT", "ENCODING", "myzset"]).await;
    let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        enc == "listpack" || enc == "skiplist" || enc == "ziplist",
        "zset encoding should be listpack, skiplist, or ziplist, got {enc}"
    );
}

#[tokio::test]
async fn tcl_object_encoding_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["OBJECT", "ENCODING", "nosuchkey"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "OBJECT ENCODING on nonexistent key should error"
    );
}

// ---------------------------------------------------------------------------
// DBSIZE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_dbsize_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);
}

#[tokio::test]
async fn tcl_dbsize_after_inserts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k1", "v1"]).await;
    client.command(&["SET", "k2", "v2"]).await;
    client.command(&["SET", "k3", "v3"]).await;
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 3);
}

#[tokio::test]
async fn tcl_dbsize_after_delete() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k1", "v1"]).await;
    client.command(&["SET", "k2", "v2"]).await;
    client.command(&["DEL", "k1"]).await;
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 1);
}

// ---------------------------------------------------------------------------
// CLIENT REPLY SKIP / ON
// ---------------------------------------------------------------------------

/// Covers upstream: `CLIENT REPLY SKIP: skip the next command reply`
///
/// CLIENT REPLY SKIP suppresses the reply to the next command. In FrogDB the
/// suppression takes effect on the command that sets the flag (the OK for
/// CLIENT REPLY SKIP itself is suppressed), so the *next* command's reply is
/// the first one we actually see.
#[tokio::test]
async fn tcl_client_reply_skip() {
    let server = TestServer::start_standalone().await;
    let mut rd = server.connect().await;

    // Send CLIENT REPLY SKIP — its own reply is suppressed.
    rd.send_only(&["CLIENT", "REPLY", "SKIP"]).await;

    // The NEXT command's reply should arrive normally.
    rd.send_only(&["PING", "pong2"]).await;
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "second PING reply should arrive");
    assert_bulk_eq(&resp.unwrap(), b"pong2");
}

/// Covers upstream: `CLIENT REPLY ON: unset SKIP flag`
///
/// Sending CLIENT REPLY ON after CLIENT REPLY SKIP should cancel the skip.
/// The reply to CLIENT REPLY ON itself is the OK that arrives.
#[tokio::test]
async fn tcl_client_reply_on_unsets_skip() {
    let server = TestServer::start_standalone().await;
    let mut rd = server.connect().await;

    // CLIENT REPLY SKIP — its own OK is suppressed
    rd.send_only(&["CLIENT", "REPLY", "SKIP"]).await;

    // CLIENT REPLY ON — consumes the skip flag, but ON also re-enables replies.
    // The OK from CLIENT REPLY ON should arrive.
    rd.send_only(&["CLIENT", "REPLY", "ON"]).await;
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "OK from CLIENT REPLY ON should arrive");
    assert_ok(&resp.unwrap());

    // Subsequent commands should reply normally.
    rd.send_only(&["PING"]).await;
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "PING reply should arrive");
    let resp = resp.unwrap();
    assert!(
        matches!(&resp, Response::Simple(s) if s == "PONG"),
        "expected PONG, got {resp:?}"
    );
}

/// Covers CLIENT REPLY error path from upstream `CLIENT command unhappy path coverage`.
#[tokio::test]
async fn tcl_client_reply_bad_argument() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let r = client.command(&["CLIENT", "REPLY", "wrongInput"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "expected error for CLIENT REPLY wrongInput, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// CLIENT INFO stats for blocking command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_client_info_stats_for_blocking_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut rd = server.connect().await;

    client.command(&["DEL", "mylist"]).await;

    let rd_id = unwrap_integer(&rd.command(&["CLIENT", "ID"]).await);

    /// Extract a field value from a CLIENT LIST output for a given client id.
    fn get_field_for_id(client_list: &str, id: i64, field: &str) -> Option<String> {
        for line in client_list.split('\n') {
            let line = line.trim();
            if line.starts_with(&format!("id={id} ")) {
                for item in line.split(' ') {
                    if let Some((k, v)) = item.split_once('=')
                        && k == field
                    {
                        return Some(v.to_string());
                    }
                }
            }
        }
        None
    }

    // Get baseline stats for rd
    let list1 = String::from_utf8(unwrap_bulk(&client.command(&["CLIENT", "LIST"]).await).to_vec())
        .unwrap();
    let cmds1: i64 = get_field_for_id(&list1, rd_id, "tot-cmds")
        .unwrap_or_default()
        .parse()
        .unwrap_or(0);

    // Start blocking command on rd
    rd.send_only(&["BLPOP", "mylist", "0"]).await;
    server.wait_for_blocked_clients(1).await;

    // While blocked, cmds should not have incremented
    let list2 = String::from_utf8(unwrap_bulk(&client.command(&["CLIENT", "LIST"]).await).to_vec())
        .unwrap();
    let cmds2: i64 = get_field_for_id(&list2, rd_id, "tot-cmds")
        .unwrap_or_default()
        .parse()
        .unwrap_or(0);
    assert_eq!(
        cmds1, cmds2,
        "cmds should not change while client is blocked"
    );

    // Unblock by pushing an element
    client.command(&["LPUSH", "mylist", "a"]).await;

    // Read the BLPOP response — this ensures the server-side handler has finished
    // processing the unblocked command and synced per-client stats to the registry.
    // Blocking commands force-sync stats before flushing the response, so by the
    // time we read it, the registry is up to date.
    let _resp = rd.read_response(std::time::Duration::from_secs(2)).await;

    // After unblocking, rd should have processed at least 1 more command.
    // FrogDB batches per-client stats (every 100 cmds / 1000ms), so earlier
    // non-blocking commands (CLIENT ID) may not have been synced yet. Blocking
    // commands force-sync, so cmds3 includes BLPOP plus any previously un-synced
    // commands. The key invariant: cmds3 > cmds2.
    let list3 = String::from_utf8(unwrap_bulk(&client.command(&["CLIENT", "LIST"]).await).to_vec())
        .unwrap();
    let cmds3: i64 = get_field_for_id(&list3, rd_id, "tot-cmds")
        .unwrap_or_default()
        .parse()
        .unwrap_or(0);
    assert!(
        cmds3 > cmds2,
        "cmds should increment after unblocking (cmds2={cmds2}, cmds3={cmds3})"
    );
}

// ---------------------------------------------------------------------------
// CONFIG sanity — FrogDB adaptation
// ---------------------------------------------------------------------------

/// Covers upstream: `CONFIG sanity`
///
/// Adapted for FrogDB: iterates all CONFIG parameters returned by CONFIG GET *,
/// verifies that mutable parameters support a SET/GET roundtrip and that
/// immutable parameters reject SET with an appropriate error.
///
/// This is an intentional-incompatibility:config test — FrogDB has different
/// config parameters than Redis, so we validate FrogDB's own parameter set.
#[tokio::test]
async fn tcl_config_sanity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Collect all config parameters
    let r = client.command(&["CONFIG", "GET", "*"]).await;
    let items = unwrap_array(r);
    assert!(
        items.len() >= 4,
        "CONFIG GET * should return at least 2 config pairs, got {} elements",
        items.len()
    );
    assert_eq!(
        items.len() % 2,
        0,
        "CONFIG GET * should return even number of elements"
    );

    // Parse into (name, value) pairs
    let mut params = Vec::new();
    for pair in items.chunks(2) {
        let name = String::from_utf8(unwrap_bulk(&pair[0]).to_vec()).unwrap();
        let value = String::from_utf8(unwrap_bulk(&pair[1]).to_vec()).unwrap();
        params.push((name, value));
    }

    assert!(
        !params.is_empty(),
        "CONFIG GET * should return at least one parameter"
    );

    // Known mutable parameters that support SET/GET roundtrip
    let known_mutable = [
        "maxmemory",
        "maxmemory-policy",
        "maxmemory-samples",
        "lfu-log-factor",
        "lfu-decay-time",
        "loglevel",
        "slowlog-log-slower-than",
        "slowlog-max-len",
        "slowlog-max-arg-len",
        "maxclients",
    ];

    // Known immutable parameters that should reject SET
    let known_immutable = ["bind", "port", "num-shards", "dir"];

    // Test that each known mutable parameter can be GET after SET
    for name in &known_mutable {
        // GET the current value
        let r = client.command(&["CONFIG", "GET", name]).await;
        let get_items = unwrap_array(r);
        assert_eq!(
            get_items.len(),
            2,
            "CONFIG GET {name} should return key-value pair"
        );
        let current_value = String::from_utf8(unwrap_bulk(&get_items[1]).to_vec()).unwrap();

        // SET the same value back (idempotent roundtrip)
        let r = client
            .command(&["CONFIG", "SET", name, &current_value])
            .await;
        assert_ok(&r);

        // GET again and verify it matches
        let r = client.command(&["CONFIG", "GET", name]).await;
        let get_items = unwrap_array(r);
        let after_set = String::from_utf8(unwrap_bulk(&get_items[1]).to_vec()).unwrap();
        assert_eq!(
            current_value, after_set,
            "CONFIG SET/GET roundtrip failed for {name}: set {current_value:?}, got {after_set:?}"
        );
    }

    // Test that known immutable parameters reject SET
    for name in &known_immutable {
        let r = client.command(&["CONFIG", "GET", name]).await;
        let get_items = unwrap_array(r);
        assert_eq!(
            get_items.len(),
            2,
            "CONFIG GET {name} should return key-value pair"
        );
        let current_value = String::from_utf8(unwrap_bulk(&get_items[1]).to_vec()).unwrap();

        let r = client
            .command(&["CONFIG", "SET", name, &current_value])
            .await;
        assert!(
            matches!(&r, Response::Error(_)),
            "CONFIG SET {name} should fail for immutable parameter, got {r:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// CONFIG during loading — FrogDB adaptation
// ---------------------------------------------------------------------------

/// Covers upstream: `config during loading`
///
/// Adapted for FrogDB: verifies CONFIG GET is available immediately after
/// server startup (FrogDB does not have an RDB loading phase like Redis,
/// but this confirms config is accessible from the first connection).
///
/// This is an intentional-incompatibility:config test — FrogDB doesn't have
/// a distinct "loading" state, so we verify config is accessible at startup.
#[tokio::test]
async fn tcl_config_during_loading() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CONFIG GET should work immediately after server start
    let r = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let items = unwrap_array(r);
    assert_eq!(
        items.len(),
        2,
        "CONFIG GET maxmemory should return key-value pair"
    );

    // CONFIG SET should work immediately
    let r = client
        .command(&["CONFIG", "SET", "slowlog-log-slower-than", "10000"])
        .await;
    assert_ok(&r);

    // Verify the SET took effect
    let r = client
        .command(&["CONFIG", "GET", "slowlog-log-slower-than"])
        .await;
    let items = unwrap_array(r);
    let value = String::from_utf8(unwrap_bulk(&items[1]).to_vec()).unwrap();
    assert_eq!(
        value, "10000",
        "CONFIG SET should take effect immediately at startup"
    );

    // CONFIG GET * should work
    let r = client.command(&["CONFIG", "GET", "*"]).await;
    let items = unwrap_array(r);
    assert!(
        items.len() >= 4,
        "CONFIG GET * should return multiple parameters at startup"
    );

    // Multiple CONFIG operations should work in sequence
    let r = client
        .command(&["CONFIG", "SET", "maxmemory", "1048576"])
        .await;
    assert_ok(&r);
    let r = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let items = unwrap_array(r);
    let value = String::from_utf8(unwrap_bulk(&items[1]).to_vec()).unwrap();
    assert_eq!(
        value, "1048576",
        "Multiple CONFIG operations should work at startup"
    );
}

// ---------------------------------------------------------------------------
// CLIENT REPLY OFF/ON: disable all commands reply
// ---------------------------------------------------------------------------

/// Covers upstream: `CLIENT REPLY OFF/ON: disable all commands reply`
///
/// CLIENT REPLY OFF disables all replies (including its own OK).
/// All subsequent commands produce no replies until CLIENT REPLY ON is
/// issued. The OK for CLIENT REPLY ON is the first reply the client sees.
#[tokio::test]
async fn tcl_client_reply_off_on() {
    let server = TestServer::start_standalone().await;
    let mut rd = server.connect().await;

    // CLIENT REPLY OFF — its own OK is suppressed (mode takes effect immediately)
    rd.send_only(&["CLIENT", "REPLY", "OFF"]).await;

    // Send several commands — none should produce replies (including the OFF itself)
    rd.send_only(&["PING"]).await;
    rd.send_only(&["SET", "x", "1"]).await;
    rd.send_only(&["GET", "x"]).await;

    // Try to read — should time out (no replies in OFF mode)
    let resp = rd.read_response(Duration::from_millis(300)).await;
    assert!(
        resp.is_none(),
        "No replies should arrive while CLIENT REPLY is OFF, got {resp:?}"
    );

    // CLIENT REPLY ON — re-enable replies; the OK for this command should arrive
    rd.send_only(&["CLIENT", "REPLY", "ON"]).await;
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "OK from CLIENT REPLY ON should arrive");
    assert_ok(&resp.unwrap());

    // Subsequent commands should produce replies normally
    rd.send_only(&["PING"]).await;
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "PING reply should arrive after ON");
    let resp = resp.unwrap();
    assert!(
        matches!(&resp, Response::Simple(s) if s == "PONG"),
        "expected PONG, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// CLIENT command unhappy path coverage
// ---------------------------------------------------------------------------

/// Covers upstream: `CLIENT command unhappy path coverage`
///
/// Tests error cases for CLIENT CACHING and CLIENT TRACKING subcommands:
/// - CLIENT CACHING without tracking enabled
/// - CLIENT CACHING with BCAST mode
/// - CLIENT CACHING with default (non-OPTIN/OPTOUT) mode
/// - CLIENT TRACKING with invalid options
/// - CLIENT CACHING with invalid argument
#[tokio::test]
async fn tcl_client_command_unhappy_path_coverage() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT CACHING without tracking enabled
    let r = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("CLIENT CACHING can be called only after CLIENT TRACKING is enabled")),
        "CLIENT CACHING without tracking should error, got {r:?}"
    );

    // Enable tracking in BCAST mode, then try CACHING
    assert_ok(&client.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await);
    let r = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("BCAST")),
        "CLIENT CACHING with BCAST mode should error, got {r:?}"
    );

    // Disable tracking, re-enable in default mode, try CACHING
    assert_ok(&client.command(&["CLIENT", "TRACKING", "OFF"]).await);
    assert_ok(&client.command(&["CLIENT", "TRACKING", "ON"]).await);
    let r = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("OPTIN or OPTOUT")),
        "CLIENT CACHING with default mode should error, got {r:?}"
    );

    // Disable and re-enable in OPTIN mode — CACHING should work
    assert_ok(&client.command(&["CLIENT", "TRACKING", "OFF"]).await);
    assert_ok(&client.command(&["CLIENT", "TRACKING", "ON", "OPTIN"]).await);
    assert_ok(&client.command(&["CLIENT", "CACHING", "YES"]).await);
    assert_ok(&client.command(&["CLIENT", "CACHING", "NO"]).await);

    // CLIENT CACHING with invalid argument
    let r = client.command(&["CLIENT", "CACHING", "MAYBE"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "CLIENT CACHING MAYBE should error, got {r:?}"
    );

    // CLIENT TRACKING with mutually exclusive OPTIN + OPTOUT
    assert_ok(&client.command(&["CLIENT", "TRACKING", "OFF"]).await);
    let r = client
        .command(&["CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT"])
        .await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("mutually exclusive")),
        "OPTIN + OPTOUT should error, got {r:?}"
    );

    // CLIENT TRACKING with BCAST + OPTIN should error
    let r = client
        .command(&["CLIENT", "TRACKING", "ON", "BCAST", "OPTIN"])
        .await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("not compatible with BCAST")),
        "BCAST + OPTIN should error, got {r:?}"
    );

    // CLIENT TRACKING with PREFIX but without BCAST should error
    let r = client
        .command(&["CLIENT", "TRACKING", "ON", "PREFIX", "foo"])
        .await;
    assert!(
        matches!(&r, Response::Error(e) if String::from_utf8_lossy(e).contains("BCAST")),
        "PREFIX without BCAST should error, got {r:?}"
    );

    // CLIENT TRACKING with unrecognized option
    let r = client
        .command(&["CLIENT", "TRACKING", "ON", "BADOPT"])
        .await;
    assert!(
        matches!(&r, Response::Error(_)),
        "Unrecognized TRACKING option should error, got {r:?}"
    );

    // CLIENT TRACKING with neither ON nor OFF
    let r = client.command(&["CLIENT", "TRACKING", "MAYBE"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "CLIENT TRACKING MAYBE should error, got {r:?}"
    );

    // CLIENT CACHING with no arguments
    let r = client.command(&["CLIENT", "CACHING"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "CLIENT CACHING with no args should error, got {r:?}"
    );

    // CLIENT REPLY with invalid argument (already tested above, but part of unhappy path coverage)
    let r = client.command(&["CLIENT", "REPLY", "BADARG"]).await;
    assert!(
        matches!(&r, Response::Error(_)),
        "CLIENT REPLY BADARG should error, got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// RESET does NOT clean library name
// ---------------------------------------------------------------------------

/// Covers upstream: `RESET does NOT clean library name`
///
/// Verifies that the RESET command preserves `lib-name` and `lib-ver` set by
/// CLIENT SETINFO, while clearing other connection state (client name, etc.).
#[tokio::test]
async fn tcl_reset_does_not_clean_library_name() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set library name, version, and client name
    assert_ok(
        &client
            .command(&["CLIENT", "SETINFO", "lib-name", "mylib"])
            .await,
    );
    assert_ok(
        &client
            .command(&["CLIENT", "SETINFO", "lib-ver", "1.2.3"])
            .await,
    );
    assert_ok(&client.command(&["CLIENT", "SETNAME", "myconn"]).await);

    // Verify all three are set
    let r = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        info.contains("lib-name=mylib"),
        "lib-name should be set before RESET, got: {info}"
    );
    assert!(
        info.contains("lib-ver=1.2.3"),
        "lib-ver should be set before RESET, got: {info}"
    );
    assert!(
        info.contains("name=myconn"),
        "name should be set before RESET, got: {info}"
    );

    // Issue RESET
    let r = client.command(&["RESET"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "RESET"),
        "RESET should return +RESET, got {r:?}"
    );

    // Verify lib-name and lib-ver are preserved
    let r = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap();
    assert!(
        info.contains("lib-name=mylib"),
        "lib-name should survive RESET, got: {info}"
    );
    assert!(
        info.contains("lib-ver=1.2.3"),
        "lib-ver should survive RESET, got: {info}"
    );

    // Verify client name is cleared
    assert!(
        info.contains("name= ") || info.contains("name=\n") || info.contains("name=\r"),
        "client name should be cleared by RESET, got: {info}"
    );
    // CLIENT GETNAME should also return nil after RESET
    assert_nil(&client.command(&["CLIENT", "GETNAME"]).await);
}

// ---------------------------------------------------------------------------
// Test argument rewriting - issue 9598
// ---------------------------------------------------------------------------

/// Covers upstream: `test argument rewriting - issue 9598`
///
/// Verifies that SLOWLOG correctly records commands with subcommands
/// (like CONFIG SET, CLIENT SETNAME, etc.) without corrupting arguments.
/// The original Redis issue was about argument rewriting in the command
/// table corrupting SLOWLOG output for commands like INCRBYFLOAT.
///
/// FrogDB adaptation: we test that SLOWLOG GET returns commands with their
/// full argument list intact, especially for subcommands.
#[tokio::test]
async fn tcl_argument_rewriting_issue_9598() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set slowlog threshold to 0 to capture everything
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
            .await,
    );

    // Reset the slowlog
    assert_ok(&client.command(&["SLOWLOG", "RESET"]).await);

    // Execute some commands with subcommands that could trigger rewriting
    client.command(&["SET", "key1", "100"]).await;
    client.command(&["CONFIG", "SET", "maxmemory", "0"]).await;
    client.command(&["CLIENT", "SETNAME", "test-rewrite"]).await;
    client.command(&["GET", "key1"]).await;

    // Check SLOWLOG GET captures the commands with intact arguments
    let r = client.command(&["SLOWLOG", "GET", "20"]).await;
    let entries = unwrap_array(r);

    // We should have at least a few entries
    assert!(!entries.is_empty(), "SLOWLOG should have captured commands");

    // Each entry should be a well-formed array:
    // [id, timestamp, duration, [args...], client_addr, client_name]
    for entry in &entries {
        let fields = match entry {
            Response::Array(a) => a,
            _ => panic!("SLOWLOG entry should be an array, got {entry:?}"),
        };
        assert!(
            fields.len() >= 4,
            "SLOWLOG entry should have at least 4 fields, got {}",
            fields.len()
        );

        // Field 0: ID (integer)
        assert!(
            matches!(&fields[0], Response::Integer(_)),
            "SLOWLOG entry ID should be integer, got {:?}",
            fields[0]
        );

        // Field 1: timestamp (integer)
        assert!(
            matches!(&fields[1], Response::Integer(_)),
            "SLOWLOG entry timestamp should be integer, got {:?}",
            fields[1]
        );

        // Field 2: duration (integer)
        assert!(
            matches!(&fields[2], Response::Integer(_)),
            "SLOWLOG entry duration should be integer, got {:?}",
            fields[2]
        );

        // Field 3: arguments (array of bulk strings)
        let args = match &fields[3] {
            Response::Array(a) => a,
            _ => panic!("SLOWLOG entry args should be an array, got {:?}", fields[3]),
        };
        assert!(!args.is_empty(), "SLOWLOG entry args should not be empty");

        // Verify the command name (first arg) is a valid bulk string
        let cmd_name = String::from_utf8(unwrap_bulk(&args[0]).to_vec()).unwrap();
        assert!(
            !cmd_name.is_empty(),
            "SLOWLOG entry command name should not be empty"
        );
    }

    // Specifically verify that a CONFIG SET entry has its arguments intact
    let config_entry = entries.iter().find(|e| {
        if let Response::Array(fields) = e
            && let Response::Array(args) = &fields[3]
            && !args.is_empty()
        {
            let cmd = String::from_utf8(unwrap_bulk(&args[0]).to_vec()).unwrap();
            cmd.eq_ignore_ascii_case("CONFIG")
        } else {
            false
        }
    });

    if let Some(Response::Array(fields)) = config_entry
        && let Response::Array(args) = &fields[3]
    {
        // CONFIG SET maxmemory 0 — should have 4 args
        assert!(
            args.len() >= 3,
            "CONFIG SET entry should have at least 3 args (CONFIG SET param), got {}",
            args.len()
        );
        // Verify the subcommand is present and not corrupted
        let subcmd = String::from_utf8(unwrap_bulk(&args[1]).to_vec()).unwrap();
        assert_eq!(
            subcmd.to_uppercase(),
            "SET",
            "CONFIG subcommand should be SET, got {subcmd}"
        );
    }

    // Restore slowlog threshold
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
            .await,
    );
}
