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

use std::time::Duration;

use frogdb_test_harness::server::TestServer;
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
