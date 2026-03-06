use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

fn parse_id(resp: &frogdb_protocol::Response) -> String {
    String::from_utf8(unwrap_bulk(resp).to_vec()).unwrap()
}

#[tokio::test]
async fn xgroup_create_and_xreadgroup_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add entries
    let id1 = parse_id(&client.command(&["XADD", "stream", "*", "k", "v1"]).await);
    client.command(&["XADD", "stream", "*", "k", "v2"]).await;

    // Create consumer group
    assert_ok(
        &client
            .command(&["XGROUP", "CREATE", "stream", "grp", "0"])
            .await,
    );

    // Read via consumer group
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "consumer1",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;
    let streams = unwrap_array(resp);
    assert_eq!(streams.len(), 1);

    let stream_entry = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_entry[1].clone());
    assert_eq!(entries.len(), 2);

    // First entry id matches
    let first = unwrap_array(entries[0].clone());
    assert_bulk_eq(&first[0], id1.as_bytes());
}

#[tokio::test]
async fn multiple_consumers_track_independently() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "*", "k", "v1"]).await;
    client.command(&["XADD", "stream", "*", "k", "v2"]).await;
    client
        .command(&["XGROUP", "CREATE", "stream", "grp", "0"])
        .await;

    // Consumer 1 reads 1 entry
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;
    let streams = unwrap_array(resp);
    let stream_entry = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_entry[1].clone());
    assert_eq!(entries.len(), 1);

    // Consumer 2 reads remaining entry
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c2",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;
    let streams = unwrap_array(resp);
    let stream_entry = unwrap_array(streams[0].clone());
    let entries = unwrap_array(stream_entry[1].clone());
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn xack_acknowledges_entries() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id1 = parse_id(&client.command(&["XADD", "stream", "*", "k", "v"]).await);
    client
        .command(&["XGROUP", "CREATE", "stream", "grp", "0"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "consumer1",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;

    // ACK
    let resp = client.command(&["XACK", "stream", "grp", &id1]).await;
    assert_eq!(unwrap_integer(&resp), 1);
}

#[tokio::test]
async fn xpending_shows_unacknowledged_entries() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "*", "k", "v1"]).await;
    client.command(&["XADD", "stream", "*", "k", "v2"]).await;
    client
        .command(&["XGROUP", "CREATE", "stream", "grp", "0"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;

    // XPENDING summary: [count, min_id, max_id, consumers]
    let resp = client.command(&["XPENDING", "stream", "grp"]).await;
    let items = unwrap_array(resp);
    assert_eq!(unwrap_integer(&items[0]), 2);
}

#[tokio::test]
async fn xgroup_setid_resets_position() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["XADD", "stream", "1-0", "k", "v"]).await;
    client.command(&["XADD", "stream", "2-0", "k", "v"]).await;
    client
        .command(&["XGROUP", "CREATE", "stream", "grp", "$"])
        .await;

    // Nothing to read at $
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;
    // Should be nil or empty array
    let is_empty = match &resp {
        frogdb_protocol::Response::Bulk(None) => true,
        frogdb_protocol::Response::Array(v) => v.is_empty(),
        _ => false,
    };
    assert!(
        is_empty || {
            let streams = unwrap_array(resp);
            let se = unwrap_array(streams[0].clone());
            let entries = unwrap_array(se[1].clone());
            entries.is_empty()
        }
    );

    // Reset to 0
    assert_ok(
        &client
            .command(&["XGROUP", "SETID", "stream", "grp", "0"])
            .await,
    );

    // Now both entries visible
    let resp = client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;
    let streams = unwrap_array(resp);
    let se = unwrap_array(streams[0].clone());
    let entries = unwrap_array(se[1].clone());
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn xclaim_transfers_ownership() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let id = parse_id(&client.command(&["XADD", "stream", "*", "k", "v"]).await);
    client
        .command(&["XGROUP", "CREATE", "stream", "grp", "0"])
        .await;
    client
        .command(&[
            "XREADGROUP",
            "GROUP",
            "grp",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "stream",
            ">",
        ])
        .await;

    // Claim with 0 min-idle-time (always succeeds)
    let resp = client
        .command(&["XCLAIM", "stream", "grp", "c2", "0", &id])
        .await;
    let entries = unwrap_array(resp);
    assert_eq!(entries.len(), 1);
    let entry = unwrap_array(entries[0].clone());
    assert_bulk_eq(&entry[0], id.as_bytes());
}
