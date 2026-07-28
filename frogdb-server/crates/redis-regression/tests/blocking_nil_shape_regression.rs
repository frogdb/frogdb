//! Regression tests pinning the RESP2 nil *shape* returned when a blocking
//! command times out.
//!
//! Redis distinguishes a null *array* (`*-1\r\n`) from a null *bulk* (`$-1\r\n`)
//! in RESP2. The array-returning blocking family (BLPOP/BRPOP/BLMPOP/BZPOPMIN/
//! BZPOPMAX/BZMPOP/XREAD) must time out with `*-1`; the single-value family
//! (BLMOVE/BRPOPLPUSH) with `$-1`. The high-level RESP2 codec collapses both to
//! a single `Null`, so these assertions read the raw socket bytes directly.
//!
//! See `todo/proposals/12-blocking-wait-coordinator.md` (Correctness flags).

use std::time::{Duration, Instant};

use frogdb_protocol::Response;
use frogdb_test_harness::response::assert_error_prefix;
use frogdb_test_harness::server::{TestServer, TestServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Encode a command as a RESP2 multi-bulk array.
fn encode_cmd(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Send a single blocking command on a fresh raw RESP2 socket and return the
/// first chunk of the raw reply bytes (which, for a timeout, is the whole nil).
async fn raw_timeout_reply(server: &TestServer, block_cmd: &[&str]) -> String {
    let mut stream = TcpStream::connect(server.socket_addr()).await.unwrap();
    stream.write_all(&encode_cmd(block_cmd)).await.unwrap();
    let mut buf = vec![0u8; 64];
    let n = timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("blocking command did not reply before the read timeout")
        .expect("read error");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn blpop_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BLPOP", "nlist", "0.05"]).await;
    assert_eq!(reply, "*-1\r\n", "BLPOP timeout must be a null array");
}

#[tokio::test]
async fn brpop_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BRPOP", "nlist", "0.05"]).await;
    assert_eq!(reply, "*-1\r\n", "BRPOP timeout must be a null array");
}

#[tokio::test]
async fn blmpop_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BLMPOP", "0.05", "1", "nlist", "LEFT"]).await;
    assert_eq!(reply, "*-1\r\n", "BLMPOP timeout must be a null array");
}

#[tokio::test]
async fn bzpopmin_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BZPOPMIN", "nzset", "0.05"]).await;
    assert_eq!(reply, "*-1\r\n", "BZPOPMIN timeout must be a null array");
}

#[tokio::test]
async fn bzpopmax_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BZPOPMAX", "nzset", "0.05"]).await;
    assert_eq!(reply, "*-1\r\n", "BZPOPMAX timeout must be a null array");
}

#[tokio::test]
async fn bzmpop_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BZMPOP", "0.05", "1", "nzset", "MIN"]).await;
    assert_eq!(reply, "*-1\r\n", "BZMPOP timeout must be a null array");
}

#[tokio::test]
async fn xread_timeout_is_null_array() {
    let server = TestServer::start_standalone().await;
    // Make the stream exist so `$` resolves to its last id; XREAD then blocks
    // on no-new-entries and times out.
    let mut client = server.connect().await;
    client.command(&["XADD", "nstream", "*", "f", "v"]).await;
    let reply = raw_timeout_reply(
        &server,
        &["XREAD", "BLOCK", "50", "STREAMS", "nstream", "$"],
    )
    .await;
    assert_eq!(reply, "*-1\r\n", "XREAD timeout must be a null array");
}

#[tokio::test]
async fn blmove_timeout_is_null_bulk() {
    let server = TestServer::start_standalone().await;
    // Hash-tag both keys onto the same slot so the command validates.
    let reply = raw_timeout_reply(
        &server,
        &["BLMOVE", "{t}src", "{t}dst", "LEFT", "RIGHT", "0.05"],
    )
    .await;
    assert_eq!(
        reply, "$-1\r\n",
        "BLMOVE timeout must be a null bulk string"
    );
}

#[tokio::test]
async fn brpoplpush_timeout_is_null_bulk() {
    let server = TestServer::start_standalone().await;
    let reply = raw_timeout_reply(&server, &["BRPOPLPUSH", "{t}src", "{t}dst", "0.05"]).await;
    assert_eq!(
        reply, "$-1\r\n",
        "BRPOPLPUSH timeout must be a null bulk string"
    );
}

// ---------------------------------------------------------------------------
// CROSSSLOT negative path for blocking multi-key commands (testing-gap
// issue #31), standalone/cross-internal-shard variant.
//
// FrogDB shards its keyspace even in standalone mode (`internal_shard =
// CRC16(key) % 16384 % num_shards`, `core/src/shard/partition.rs`), and
// `ConnectionHandler::route_and_execute` (`server/src/connection/routing.rs`)
// rejects any multi-key command whose keys don't all land on the same
// internal shard with the very same `CROSSSLOT` wire error cluster mode
// uses -- that's exactly why every test above hash-tags its keys ("so the
// command validates"). That negative path -- a blocking command given keys
// that hash to *different* shards -- had no dedicated coverage, standalone
// or cluster. The cluster-mode coverage lives in
// `server/tests/integration_cluster.rs`
// (`test_bl{pop,mpop}_cross_slot_returns_crossslot_immediately` and
// friends); this section is the standalone/internal-shard analogue.
// ---------------------------------------------------------------------------

/// Find two keys that hash to different internal shards out of `num_shards`
/// (which is always possible once `num_shards >= 2`, since keys are
/// effectively uniformly distributed over the shard space).
fn two_keys_on_different_shards(num_shards: usize) -> (String, String) {
    assert!(
        num_shards >= 2,
        "need at least 2 shards to find a cross-shard pair"
    );
    let mut by_shard: Vec<Option<String>> = vec![None; num_shards];
    for i in 0..100_000 {
        let key = format!("xshardkey{i}");
        let shard = frogdb_core::shard_for_key(key.as_bytes(), num_shards);
        if by_shard[shard].is_none() {
            by_shard[shard] = Some(key);
        }
        if by_shard.iter().filter(|s| s.is_some()).count() >= 2 {
            break;
        }
    }
    let mut found = by_shard.into_iter().flatten();
    let a = found.next().expect("should find a key for some shard");
    let b = found.next().expect("should find a key for a second shard");
    (a, b)
}

/// Shared body: start a standalone server with a known shard count, pick two
/// keys landing on different internal shards (no hash tag), send the
/// command built by `build_args(key_a, key_b)` with an effectively-infinite
/// blocking timeout, and assert it's rejected with CROSSSLOT immediately --
/// never entering the blocking wait path, and never leaving a registered
/// waiter behind.
async fn assert_blocking_multishard_crossslot_no_block(
    build_args: impl Fn(&str, &str) -> Vec<String>,
    context: &str,
) {
    const NUM_SHARDS: usize = 4;
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(NUM_SHARDS),
        ..Default::default()
    })
    .await;

    let (key_a, key_b) = two_keys_on_different_shards(NUM_SHARDS);
    assert_ne!(
        frogdb_core::shard_for_key(key_a.as_bytes(), NUM_SHARDS),
        frogdb_core::shard_for_key(key_b.as_bytes(), NUM_SHARDS),
        "{context}: key_a/key_b must land on different internal shards"
    );

    let mut client = server.connect().await;
    let args = build_args(&key_a, &key_b);
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let start = Instant::now();
    client.send_only(&arg_refs).await;
    let resp = client
        .read_response(Duration::from_secs(3))
        .await
        .unwrap_or_else(|| {
            panic!(
                "{context}: no response within 3s -- looks like it entered the blocking wait \
                 path instead of rejecting cross-shard keys immediately"
            )
        });
    let elapsed = start.elapsed();

    assert_error_prefix(&resp, "CROSSSLOT");
    assert!(
        elapsed < Duration::from_millis(1500),
        "{context}: CROSSSLOT reply took {elapsed:?}, expected an immediate (non-blocking) \
         rejection well under the 3s bound"
    );

    assert_eq!(
        server.blocked_client_count(),
        0,
        "{context}: a cross-shard blocking command must never register a waiter"
    );

    let ping = client.command(&["PING"]).await;
    assert_eq!(
        ping,
        Response::Simple(bytes::Bytes::from_static(b"PONG")),
        "{context}: connection must still be clean (no stray frames) after the CROSSSLOT reply"
    );
}

#[tokio::test]
async fn blpop_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BLPOP".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BLPOP with cross-shard keys (standalone)",
    )
    .await;
}

#[tokio::test]
async fn blmpop_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BLMPOP".to_string(),
                "0".to_string(),
                "2".to_string(),
                a.to_string(),
                b.to_string(),
                "LEFT".to_string(),
            ]
        },
        "BLMPOP with cross-shard keys (standalone)",
    )
    .await;
}

#[tokio::test]
async fn bzpopmin_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BZPOPMIN".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BZPOPMIN with cross-shard keys (standalone)",
    )
    .await;
}

#[tokio::test]
async fn bzpopmax_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BZPOPMAX".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BZPOPMAX with cross-shard keys (standalone)",
    )
    .await;
}

#[tokio::test]
async fn bzmpop_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BZMPOP".to_string(),
                "0".to_string(),
                "2".to_string(),
                a.to_string(),
                b.to_string(),
                "MIN".to_string(),
            ]
        },
        "BZMPOP with cross-shard keys (standalone)",
    )
    .await;
}

#[tokio::test]
async fn blmove_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BLMOVE".to_string(),
                a.to_string(),
                b.to_string(),
                "LEFT".to_string(),
                "RIGHT".to_string(),
                "0".to_string(),
            ]
        },
        "BLMOVE with cross-shard source/destination (standalone)",
    )
    .await;
}

#[tokio::test]
async fn brpoplpush_cross_shard_returns_crossslot_immediately() {
    assert_blocking_multishard_crossslot_no_block(
        |a, b| {
            vec![
                "BRPOPLPUSH".to_string(),
                a.to_string(),
                b.to_string(),
                "0".to_string(),
            ]
        },
        "BRPOPLPUSH with cross-shard source/destination (standalone)",
    )
    .await;
}
