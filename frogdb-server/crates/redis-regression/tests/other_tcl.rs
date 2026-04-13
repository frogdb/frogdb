//! Rust port of Redis 8.6.0 `unit/other.tcl` test suite.
//!
//! This file ports the subset of `unit/other.tcl` that is in-scope for
//! FrogDB. The TCL file mixes general coverage tests (APPEND, HELP
//! commands, FLUSHDB) with Redis-specific infrastructure tests (RDB/AOF
//! persistence, DEBUG reload, SELECT multi-DB, RESET, cluster
//! compatibility) that cannot be cleanly distributed across existing
//! ports because the audit's `PORT_MAP` is file-level. We therefore
//! port the general tests here and document the rest as
//! `## Intentional exclusions`.
//!
//! ## Intentional exclusions
//!
//! Test-infrastructure / developer-only:
//! - `Failing test` — redis-specific — `$::force_failure` helper, not a real test
//!
//! Jemalloc-specific (FrogDB does not link against jemalloc):
//! - `Coverage: MEMORY MALLOC-STATS` — redis-specific — jemalloc-only subcommand
//!
//! RDB persistence (FrogDB uses WAL + RocksDB snapshots, not Redis RDB):
//! - `SAVE - make sure there are all the types as values` — intentional-incompatibility:persistence — needs:save — requires RDB
//! - `FLUSHALL should not reset the dirty counter if we disable save` — intentional-incompatibility:persistence — needs:save — RDB dirty counter
//! - `FLUSHALL should reset the dirty counter to 0 if we enable save` — intentional-incompatibility:persistence — needs:save — RDB dirty counter
//! - `FLUSHALL and bgsave` — intentional-incompatibility:persistence — needs:save — requires BGSAVE+DEBUG SLEEP
//! - `BGSAVE` — intentional-incompatibility:persistence — needs:debug needs:save — DEBUG RELOAD required
//! - `Perform a final SAVE to leave a clean DB on disk` — intentional-incompatibility:persistence — needs:save — RDB save
//!
//! DEBUG RELOAD / DEBUG LOADAOF / DEBUG DIGEST (FrogDB's DEBUG surface
//! is limited to SLEEP/TRACING/STRUCTSIZE/PUBSUB/BUNDLE/HASHING):
//! - `Check consistency of different data types after a reload` — intentional-incompatibility:persistence — needs:debug — DEBUG RELOAD
//! - `Same dataset digest if saving/reloading as AOF?` — intentional-incompatibility:debug — needs:debug — DEBUG DIGEST + AOF
//! - `EXPIRES after a reload (snapshot + append only file rewrite)` — intentional-incompatibility:persistence — needs:debug needs:save — DEBUG RELOAD + LOADAOF
//! - `EXPIRES after AOF reload (without rewrite)` — intentional-incompatibility:debug — needs:debug — DEBUG LOADAOF
//!
//! Slow / fuzz / stress (excluded from the fast test lane):
//! - `FUZZ stresser with data model $fuzztype` — tested-elsewhere — slow — fuzz stresser
//!
//! Multi-DB SELECT (FrogDB uses a single database per instance):
//! - `SELECT an out of range DB` — intentional-incompatibility:single-db — singledb — SELECT non-zero rejected outright
//!
//! RESET command (not implemented in FrogDB):
//! - `RESET clears client state` — intentional-incompatibility:protocol — needs:reset — RESET not implemented
//! - `RESET clears MONITOR state` — intentional-incompatibility:protocol — needs:reset — RESET not implemented
//! - `RESET clears and discards MULTI state` — intentional-incompatibility:protocol — needs:reset — RESET not implemented
//! - `RESET clears Pub/Sub state` — intentional-incompatibility:protocol — needs:reset — RESET not implemented
//! - `RESET clears authenticated state` — intentional-incompatibility:protocol — needs:reset — RESET not implemented
//!
//! DEBUG HTSTATS / dict-resizing / hashtable internals (Redis-internal
//! data structures; FrogDB stores keys in RocksDB):
//! - `Don't rehash if redis has child process` — intentional-incompatibility:debug — needs:debug — DEBUG HTSTATS + fork
//! - `Redis can trigger resizing` — intentional-incompatibility:debug — needs:debug — DEBUG HTSTATS + dict-resizing
//! - `Redis can rewind and trigger smaller slot resizing` — intentional-incompatibility:debug — needs:debug — DEBUG HTSTATS
//! - `Redis can resize empty dict` — redis-specific — MEMORY STATS `db.9 overhead.hashtable.main` Redis-internal
//!
//! Platform-specific (Linux-only /proc):
//! - `Process title set as expected` — redis-specific — platform-specific — /proc/self/cmdline
//!
//! Cluster-mode `cluster_incompatible_ops` counter (FrogDB's cluster
//! compat metric is different / not exposed):
//! - `Cross DB command is incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — multi-DB cluster metric
//! - `Function no-cluster flag is incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — no-cluster flag metric
//! - `Script no-cluster flag is incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — no-cluster flag metric
//! - `SORT command incompatible operations with cluster mode` — intentional-incompatibility:cluster — cluster:skip — SORT BY cluster metric
//! - `Normal cross slot commands are incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — cross-slot metric
//! - `Transaction is incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — transaction cluster metric
//! - `Lua scripts are incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — Lua cluster metric
//! - `Shard subscribe commands are incompatible with cluster mode` — intentional-incompatibility:cluster — cluster:skip — SSUBSCRIBE cluster metric
//! - `cluster-compatibility-sample-ratio configuration can work` — intentional-incompatibility:cluster — cluster:skip — sample-ratio config

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Coverage: HELP commands — verifies that major subcommand groups respond
// to `HELP` with a non-empty array whose first line advertises the
// `<group> <subcommand>` usage.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_coverage_help_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Each group must respond to HELP with an array whose first element
    // contains a usage header mentioning the group's name. This mirrors
    // the upstream `assert_match "*OBJECT <subcommand> *" [r OBJECT HELP]`.
    for group in [
        "OBJECT", "MEMORY", "PUBSUB", "SLOWLOG", "CLIENT", "COMMAND", "CONFIG", "FUNCTION",
        "MODULE",
    ] {
        let resp = client.command(&[group, "HELP"]).await;
        let lines = extract_bulk_strings(&resp);
        assert!(!lines.is_empty(), "{group} HELP returned no lines");
        let header = &lines[0];
        assert!(
            header.to_uppercase().contains(group) && header.contains("<subcommand>"),
            "{group} HELP header should advertise '<subcommand>', got {header:?}",
        );
    }
}

// ---------------------------------------------------------------------------
// Coverage: MEMORY PURGE — returns OK on FrogDB regardless of allocator
// (the upstream test is conditional on jemalloc, so the allocator check
// is a no-op here).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_coverage_memory_purge() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MEMORY", "PURGE"]).await);
}

// ---------------------------------------------------------------------------
// PIPELINING stresser — writes N `SET`/`GET` pairs in a single pipeline
// burst and verifies every response round-trips. The upstream version
// uses 100k iterations and doubles as a regression for an old epoll bug;
// we scale it down to 2k to keep the fast test lane fast while still
// exercising the pipelined command path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_pipelining_stresser() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    const N: usize = 2000;

    // Pipeline the writes.
    for i in 0..N {
        let key = format!("key:{i}");
        let val = format!("0000{i}0000");
        client.send_only(&["SET", &key, &val]).await;
        client.send_only(&["GET", &key]).await;
    }

    // Drain the replies in order. Each SET should be OK, each GET should
    // return the value we just wrote.
    for i in 0..N {
        let set_resp = client
            .read_response(std::time::Duration::from_secs(5))
            .await
            .expect("SET reply");
        assert_ok(&set_resp);

        let get_resp = client
            .read_response(std::time::Duration::from_secs(5))
            .await
            .expect("GET reply");
        let expected = format!("0000{i}0000");
        assert_bulk_eq(&get_resp, expected.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// APPEND basics — creates a key via APPEND and extends it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_append_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo"]).await;

    // First append creates the key.
    assert_integer_eq(&client.command(&["APPEND", "foo", "bar"]).await, 3);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");

    // Second append extends it.
    assert_integer_eq(&client.command(&["APPEND", "foo", "100"]).await, 6);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar100");
}

// ---------------------------------------------------------------------------
// APPEND basics, integer encoded values — APPEND onto a freshly-created
// key *and* onto a key that was first SET with an integer-like value.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_append_basics_integer_encoded_values() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Case 1: APPEND 1 then APPEND 2 (starts from empty).
    client.command(&["DEL", "foo"]).await;
    client.command(&["APPEND", "foo", "1"]).await;
    client.command(&["APPEND", "foo", "2"]).await;
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"12");

    // Case 2: SET 1, then APPEND 2 (starts from an int-encoded value).
    client.command(&["SET", "foo", "1"]).await;
    client.command(&["APPEND", "foo", "2"]).await;
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"12");
}

// ---------------------------------------------------------------------------
// APPEND fuzzing — appends many random chunks and verifies that the
// concatenated result matches the reference buffer.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_append_fuzzing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Deterministic PRNG so the test is reproducible.
    fn next(state: &mut u64) -> u64 {
        // xorshift64
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x
    }

    for (type_id, charset) in [
        (0, b"abcdefghijklmnop".as_ref()),
        (1, b"ABCDEFGHIJKLMNOP".as_ref()),
        (2, b"xyzxyzxyzxyzxyzxyz".as_ref()),
    ] {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (type_id as u64);
        let mut expected: Vec<u8> = Vec::new();

        client.command(&["DEL", "x"]).await;
        for _ in 0..200 {
            let len = (next(&mut state) % 11) as usize; // 0..=10 bytes
            let mut chunk = Vec::with_capacity(len);
            for _ in 0..len {
                chunk.push(charset[(next(&mut state) as usize) % charset.len()]);
            }
            expected.extend_from_slice(&chunk);

            // Send as a raw bytes command so we can tolerate any byte.
            let chunk_str = String::from_utf8_lossy(&chunk).into_owned();
            client.command(&["APPEND", "x", &chunk_str]).await;
        }

        let resp = client.command(&["GET", "x"]).await;
        assert_bulk_eq(&resp, &expected);
    }
}

// ---------------------------------------------------------------------------
// FLUSHDB — empties the current database and verifies DBSIZE is 0.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_flushdb() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Populate some keys.
    client.command(&["SET", "a", "1"]).await;
    client.command(&["SET", "b", "2"]).await;
    client.command(&["SET", "c", "3"]).await;

    assert_ok(&client.command(&["FLUSHDB"]).await);
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);
}

// ---------------------------------------------------------------------------
// Subcommand syntax error crash (issue #10070) — malformed `GROUP|SUB`
// invocations must error cleanly (originally crashed Redis). Both
// unknown commands and unknown subcommands are acceptable as long as
// the server doesn't crash.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_subcommand_syntax_error_crash_issue_10070() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // `GET|`, `GET|SET`, `GET|SET|OTHER` should all error as unknown
    // commands (the pipe-delimited form is not a real command in FrogDB).
    for bad_cmd in ["GET|", "GET|SET", "GET|SET|OTHER", "CONFIG|GET"] {
        let resp = client.command(&[bad_cmd, "x"]).await;
        let err = match &resp {
            Response::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected error for {bad_cmd:?}, got {other:?}"),
        };
        assert!(
            err.contains("unknown command") || err.contains("unknown subcommand"),
            "expected unknown command/subcommand for {bad_cmd:?}, got {err:?}",
        );
    }

    // `CONFIG GET_XX` is a real command with a bad subcommand — it
    // should error as "unknown subcommand".
    let resp = client.command(&["CONFIG", "GET_XX"]).await;
    let err = match &resp {
        Response::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("unknown subcommand") || err.contains("unknown command"),
        "expected unknown (sub)command error, got {err:?}",
    );

    // Server is still alive.
    assert_ok(&client.command(&["SET", "sanity", "ok"]).await);
}
