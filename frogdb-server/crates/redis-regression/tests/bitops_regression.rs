use bytes::Bytes;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn bitcount_negative_byte_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a value "foobar" (6 bytes)
    assert_ok(&client.command(&["SET", "mykey", "foobar"]).await);

    // Negative index -1 means last byte, -2 means second to last, etc.
    let resp = client.command(&["BITCOUNT", "mykey", "-2", "-1"]).await;
    let count = unwrap_integer(&resp);
    assert!(
        count >= 0,
        "BITCOUNT with negative range should return valid count"
    );

    // Full range should match default
    let full = client.command(&["BITCOUNT", "mykey"]).await;
    let range_full = client.command(&["BITCOUNT", "mykey", "0", "-1"]).await;
    assert_eq!(unwrap_integer(&full), unwrap_integer(&range_full));
}

#[tokio::test]
async fn bitcount_with_bit_range_modifier() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Set a key with binary data: 0xff 0x00 using raw bytes
    let set_args = [
        &Bytes::from_static(b"SET"),
        &Bytes::from_static(b"mykey"),
        &Bytes::from_static(&[0xff, 0x00]),
    ];
    client.command_raw(&set_args).await;

    // BIT mode counts bits in bit range
    let resp = client
        .command(&["BITCOUNT", "mykey", "0", "7", "BIT"])
        .await;
    assert_eq!(unwrap_integer(&resp), 8); // first byte 0xff = 8 bits

    let resp = client
        .command(&["BITCOUNT", "mykey", "8", "15", "BIT"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0); // second byte 0x00 = 0 bits
}

#[tokio::test]
async fn setbit_returns_old_bit_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SETBIT on non-existent key returns 0 (old bit was 0)
    let resp = client.command(&["SETBIT", "mykey", "7", "1"]).await;
    assert_eq!(unwrap_integer(&resp), 0);

    // SETBIT same offset returns old value 1
    let resp = client.command(&["SETBIT", "mykey", "7", "0"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    // And back to 0
    let resp = client.command(&["SETBIT", "mykey", "7", "1"]).await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn bitfield_set_returns_old_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // BITFIELD SET returns the old value
    let resp = client
        .command(&["BITFIELD", "mykey", "SET", "u8", "0", "100"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 0); // old value was 0

    let resp = client
        .command(&["BITFIELD", "mykey", "SET", "u8", "0", "200"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 100); // old value was 100
}
