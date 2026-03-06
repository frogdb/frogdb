use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

fn parse_stream_id(resp: &frogdb_protocol::Response) -> String {
    String::from_utf8(unwrap_bulk(resp).to_vec()).unwrap()
}

fn id_ms(id: &str) -> u64 {
    id.split('-').next().unwrap().parse().unwrap()
}

fn id_seq(id: &str) -> u64 {
    id.split('-').nth(1).unwrap().parse().unwrap()
}

#[tokio::test]
async fn xadd_auto_ids_monotonic_ordering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id1 = parse_stream_id(&client.command(&["XADD", "stream", "*", "k", "v1"]).await);
    let id2 = parse_stream_id(&client.command(&["XADD", "stream", "*", "k", "v2"]).await);
    let id3 = parse_stream_id(&client.command(&["XADD", "stream", "*", "k", "v3"]).await);

    // IDs must be strictly increasing
    assert!(id1 < id2 || (id_ms(&id1) == id_ms(&id2) && id_seq(&id1) < id_seq(&id2)));
    assert!(id2 < id3 || (id_ms(&id2) == id_ms(&id3) && id_seq(&id2) < id_seq(&id3)));

    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        3
    );
}

#[tokio::test]
async fn xadd_explicit_ids_and_xrange() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XADD", "stream", "1-1", "item", "1", "value", "a"])
        .await;
    client
        .command(&["XADD", "stream", "2-1", "item", "2", "value", "b"])
        .await;

    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        2
    );

    let resp = client.command(&["XRANGE", "stream", "-", "+"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);

    // Each entry is [id, [fields]]
    let first = unwrap_array(entries[0].clone());
    assert_bulk_eq(&first[0], b"1-1");
}

#[tokio::test]
async fn xadd_rejects_non_monotonic_explicit_ids() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "5-0", "k", "v"]).await;
    // Trying to add with lower ID should error
    let resp = client.command(&["XADD", "stream", "3-0", "k", "v"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn xrange_and_xrevrange_bounds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "1-0", "k", "a"]).await;
    client.command(&["XADD", "stream", "2-0", "k", "b"]).await;
    client.command(&["XADD", "stream", "3-0", "k", "c"]).await;

    // XRANGE with range
    let resp = client.command(&["XRANGE", "stream", "1-0", "2-0"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);

    // XREVRANGE returns reverse order
    let resp = client.command(&["XREVRANGE", "stream", "+", "-"]).await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 3);
    let first = unwrap_array(entries[0].clone());
    assert_bulk_eq(&first[0], b"3-0");

    // XREVRANGE with COUNT
    let resp = client
        .command(&["XREVRANGE", "stream", "+", "-", "COUNT", "2"])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn xlen_tracks_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        0
    );
    client.command(&["XADD", "stream", "*", "k", "v"]).await;
    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        1
    );
}

#[tokio::test]
async fn xdel_removes_entries() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id = parse_stream_id(&client.command(&["XADD", "stream", "*", "k", "v"]).await);
    client.command(&["XADD", "stream", "*", "k", "v2"]).await;

    let resp = client.command(&["XDEL", "stream", &id]).await;
    assert_eq!(unwrap_integer(&resp), 1);
    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        1
    );
}

#[tokio::test]
async fn xtrim_maxlen() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..10 {
        client
            .command(&["XADD", "stream", "*", "i", &i.to_string()])
            .await;
    }

    let resp = client.command(&["XTRIM", "stream", "MAXLEN", "5"]).await;
    assert_eq!(unwrap_integer(&resp), 5);
    assert_eq!(
        unwrap_integer(&client.command(&["XLEN", "stream"]).await),
        5
    );
}

#[tokio::test]
async fn xadd_nomkstream_does_not_create_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["XADD", "stream", "NOMKSTREAM", "*", "k", "v"])
        .await;
    // Returns nil when stream does not exist
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
    assert_eq!(
        unwrap_integer(&client.command(&["EXISTS", "stream"]).await),
        0
    );
}

#[tokio::test]
async fn xinfo_stream_returns_metadata() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "1-0", "k", "v"]).await;

    let resp = client.command(&["XINFO", "STREAM", "stream"]).await;
    let items = unwrap_array(resp);
    // XINFO STREAM returns key-value pairs; find "length"
    let strings = items
        .iter()
        .filter_map(|r| match r {
            frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
            frogdb_protocol::Response::Simple(s) => String::from_utf8(s.to_vec()).ok(),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(strings.iter().any(|s| s == "length"));
}
