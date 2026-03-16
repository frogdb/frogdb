use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// CLIENT ID infrastructure tests

#[tokio::test]
async fn client_id_returns_non_negative_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "ID"]).await;
    let id = unwrap_integer(&resp);
    assert!(
        id >= 0,
        "CLIENT ID should return a non-negative integer, got {id}"
    );
}

#[tokio::test]
async fn client_ids_are_unique_across_connections() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;
    let mut c3 = server.connect().await;

    let id1 = unwrap_integer(&c1.command(&["CLIENT", "ID"]).await);
    let id2 = unwrap_integer(&c2.command(&["CLIENT", "ID"]).await);
    let id3 = unwrap_integer(&c3.command(&["CLIENT", "ID"]).await);

    assert_ne!(id1, id2, "c1 and c2 should have different CLIENT IDs");
    assert_ne!(id2, id3, "c2 and c3 should have different CLIENT IDs");
    assert_ne!(id1, id3, "c1 and c3 should have different CLIENT IDs");
}

#[tokio::test]
async fn client_info_contains_id_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "INFO"]).await;
    let info = String::from_utf8_lossy(unwrap_bulk(&resp));
    assert!(info.contains("id="), "CLIENT INFO should contain id= field");
}

// CLIENT TRACKING on/off tests

#[tokio::test]
async fn client_tracking_on_off() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "TRACKING", "ON"]).await;
    assert_ok(&resp);

    let resp = client.command(&["CLIENT", "TRACKING", "OFF"]).await;
    assert_ok(&resp);
}

// CLIENT TRACKINGINFO tests

#[tokio::test]
async fn client_trackinginfo_default() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CLIENT", "TRACKING", "ON"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    let arr = unwrap_array(resp);
    // arr = ["flags", [flags...], "redirect", -1, "prefixes", []]
    assert_eq!(arr.len(), 6);
    assert_bulk_eq(&arr[0], b"flags");
    let flags = unwrap_array(arr[1].clone());
    let flag_strs = extract_bulk_strings(&frogdb_protocol::Response::Array(flags));
    assert!(flag_strs.contains(&"on".to_string()), "Should contain 'on'");
    // Default mode doesn't emit a mode flag (matches Redis behavior)
    assert!(
        !flag_strs.contains(&"optin".to_string()) && !flag_strs.contains(&"optout".to_string()),
        "Default mode should not contain optin or optout"
    );
    assert_bulk_eq(&arr[2], b"redirect");
    assert_eq!(unwrap_integer(&arr[3]), -1);
    assert_bulk_eq(&arr[4], b"prefixes");
}

#[tokio::test]
async fn client_trackinginfo_optin() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CLIENT", "TRACKING", "ON", "OPTIN"]).await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    let arr = unwrap_array(resp);
    let flags = unwrap_array(arr[1].clone());
    let flag_strs = extract_bulk_strings(&frogdb_protocol::Response::Array(flags));
    assert!(
        flag_strs.contains(&"optin".to_string()),
        "Should contain 'optin', got {:?}",
        flag_strs
    );
}

#[tokio::test]
async fn client_trackinginfo_noloop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CLIENT", "TRACKING", "ON", "NOLOOP"])
        .await;
    let resp = client.command(&["CLIENT", "TRACKINGINFO"]).await;
    let arr = unwrap_array(resp);
    let flags = unwrap_array(arr[1].clone());
    let flag_strs = extract_bulk_strings(&frogdb_protocol::Response::Array(flags));
    assert!(
        flag_strs.contains(&"noloop".to_string()),
        "Should contain 'noloop', got {:?}",
        flag_strs
    );
}

#[tokio::test]
async fn client_caching_requires_tracking() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CACHING without tracking enabled should error
    let resp = client.command(&["CLIENT", "CACHING", "YES"]).await;
    assert_error_prefix(&resp, "ERR CLIENT CACHING");
}

#[tokio::test]
async fn client_tracking_rejects_bcast() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CLIENT", "TRACKING", "ON", "BCAST"]).await;
    assert_error_prefix(&resp, "ERR");
}
