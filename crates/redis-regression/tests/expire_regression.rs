use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn expire_conflicting_nx_xx_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);
    let resp = client
        .command(&["EXPIRE", "mykey", "100", "NX", "XX"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn expire_conflicting_gt_lt_returns_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);
    let resp = client
        .command(&["EXPIRE", "mykey", "100", "GT", "LT"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn pexpire_large_ms_no_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);

    // Very large ms value that would overflow when added to current time
    let resp = client
        .command(&["PEXPIRE", "mykey", "9223372036854775807"])
        .await;
    // Should return error about invalid expire time, not panic/overflow
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn expiretime_returns_correct_seconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);

    // Set expiry 1000 seconds from now
    assert_eq!(
        unwrap_integer(&client.command(&["EXPIRE", "mykey", "1000"]).await),
        1
    );

    // EXPIRETIME returns unix timestamp in seconds
    let resp = client.command(&["EXPIRETIME", "mykey"]).await;
    let expire_secs = unwrap_integer(&resp);
    assert!(
        expire_secs > 0,
        "EXPIRETIME should return positive timestamp"
    );

    // PEXPIRETIME returns unix timestamp in milliseconds
    let resp = client.command(&["PEXPIRETIME", "mykey"]).await;
    let expire_ms = unwrap_integer(&resp);
    assert!(
        expire_ms > 0,
        "PEXPIRETIME should return positive timestamp"
    );

    // ms should be approximately secs * 1000 (within 1 second tolerance)
    let diff = (expire_ms - expire_secs * 1000).abs();
    assert!(
        diff < 1000,
        "PEXPIRETIME and EXPIRETIME should be consistent: diff={diff}ms"
    );
}

#[tokio::test]
async fn pexpiretime_returns_exact_milliseconds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);

    // Set expiry in milliseconds
    assert_eq!(
        unwrap_integer(&client.command(&["PEXPIRE", "mykey", "100000"]).await),
        1
    );

    let resp = client.command(&["PEXPIRETIME", "mykey"]).await;
    let expire_ms = unwrap_integer(&resp);
    assert!(
        expire_ms > 0,
        "PEXPIRETIME should return positive timestamp"
    );

    // Verify it's not returning -1 or -2
    assert!(
        expire_ms > 1000000000000i64,
        "should be a unix timestamp in ms"
    );
}
