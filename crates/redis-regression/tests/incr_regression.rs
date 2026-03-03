use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn incr_on_nonexistent_key_returns_one() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INCR", "counter"]).await;
    assert_eq!(unwrap_integer(&resp), 1);
}

#[tokio::test]
async fn incr_against_key_created_by_incr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["INCR", "novar"]).await;
    let resp = client.command(&["INCR", "novar"]).await;
    assert_eq!(unwrap_integer(&resp), 2);
}

#[tokio::test]
async fn decr_against_key_created_by_incr() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["INCR", "novar"]).await;
    let resp = client.command(&["DECR", "novar"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn decr_on_nonexistent_key_returns_negative_one() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DECR", "novar"]).await;
    assert_eq!(unwrap_integer(&resp), -1);
}

#[tokio::test]
async fn incr_against_key_set_with_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "100"]).await;
    let resp = client.command(&["INCR", "novar"]).await;
    assert_eq!(unwrap_integer(&resp), 101);
}

#[tokio::test]
async fn incr_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    let resp = client.command(&["INCR", "novar"]).await;
    assert_eq!(unwrap_integer(&resp), 17179869185);
}

#[tokio::test]
async fn incrby_over_32bit_value_with_over_32bit_increment() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    let resp = client.command(&["INCRBY", "novar", "17179869184"]).await;
    assert_eq!(unwrap_integer(&resp), 34359738368);
}

#[tokio::test]
async fn incr_fails_against_key_with_spaces() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Left-padded spaces
    client.command(&["SET", "{k}novar", "    11"]).await;
    let resp = client.command(&["INCR", "{k}novar"]).await;
    assert_error_prefix(&resp, "ERR");

    // Right-padded spaces
    client.command(&["SET", "{k}novar", "11    "]).await;
    let resp = client.command(&["INCR", "{k}novar"]).await;
    assert_error_prefix(&resp, "ERR");

    // Both sides
    client.command(&["SET", "{k}novar", "    11    "]).await;
    let resp = client.command(&["INCR", "{k}novar"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn decrby_negation_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "0"]).await;
    let resp = client
        .command(&["DECRBY", "x", "-9223372036854775808"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn incr_fails_against_list_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "1"]).await;
    let resp = client.command(&["INCR", "mylist"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn decrby_over_32bit_value_negative_result() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    let resp = client.command(&["DECRBY", "novar", "17179869185"]).await;
    assert_eq!(unwrap_integer(&resp), -1);
}

#[tokio::test]
async fn decrby_on_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DECRBY", "key_not_exist", "1"]).await;
    assert_eq!(unwrap_integer(&resp), -1);
}

#[tokio::test]
async fn incrbyfloat_on_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INCRBYFLOAT", "novar", "1"]).await;
    assert_bulk_eq(&resp, b"1");

    let resp = client.command(&["INCRBYFLOAT", "novar", "0.25"]).await;
    assert_bulk_eq(&resp, b"1.25");
}

#[tokio::test]
async fn incrbyfloat_against_set_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "1.5"]).await;
    let resp = client.command(&["INCRBYFLOAT", "novar", "1.5"]).await;
    assert_bulk_eq(&resp, b"3");
}

#[tokio::test]
async fn incrbyfloat_over_32bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "novar", "17179869184"]).await;
    let resp = client.command(&["INCRBYFLOAT", "novar", "1.5"]).await;
    assert_bulk_eq(&resp, b"17179869185.5");
}

#[tokio::test]
async fn incrbyfloat_fails_against_key_with_spaces() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{k}novar", "    11"]).await;
    let resp = client.command(&["INCRBYFLOAT", "{k}novar", "1.0"]).await;
    assert_error_prefix(&resp, "ERR");

    client.command(&["SET", "{k}novar", "11    "]).await;
    let resp = client.command(&["INCRBYFLOAT", "{k}novar", "1.0"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn incrbyfloat_fails_against_list_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "1"]).await;
    let resp = client.command(&["INCRBYFLOAT", "mylist", "1.0"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

#[tokio::test]
async fn incrbyfloat_rejects_infinity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "0"]).await;
    let resp = client.command(&["INCRBYFLOAT", "foo", "+inf"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn incrbyfloat_decrement() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "1"]).await;
    let resp = client.command(&["INCRBYFLOAT", "foo", "-1.1"]).await;
    let val: f64 = String::from_utf8_lossy(unwrap_bulk(&resp))
        .parse()
        .unwrap();
    assert!((val - (-0.1)).abs() < 1e-10);
}

#[tokio::test]
async fn incrbyfloat_no_negative_zero() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1/41 + (-1/41) should give "0", not "-0"
    let inc = format!("{}", 1.0_f64 / 41.0);
    let dec = format!("{}", -1.0_f64 / 41.0);
    client.command(&["INCRBYFLOAT", "foo", &inc]).await;
    let resp = client.command(&["INCRBYFLOAT", "foo", &dec]).await;
    assert_bulk_eq(&resp, b"0");
}
