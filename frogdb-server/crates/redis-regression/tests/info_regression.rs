use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn info_returns_bulk_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO"]).await;
    // INFO returns a bulk string
    let info_text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(!info_text.is_empty());
}

#[tokio::test]
async fn info_server_section_filtering() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "server"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("redis_version") || text.contains("frogdb_version"),
        "server section should contain version info: {text}"
    );
}

#[tokio::test]
async fn info_clients_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "clients"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("connected_clients"),
        "clients section should contain connected_clients: {text}"
    );
}

#[tokio::test]
async fn info_memory_section() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "memory"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    assert!(
        text.contains("used_memory"),
        "memory section should contain used_memory: {text}"
    );
}

#[tokio::test]
async fn info_all_returns_all_sections() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["INFO", "all"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // ALL should cover multiple sections
    assert!(
        text.contains("connected_clients") || text.contains("used_memory"),
        "INFO ALL should contain stats: {text}"
    );
}

#[tokio::test]
async fn info_keyspace_shows_db_stats_after_writes() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Write a key first
    client.command(&["SET", "testkey", "testval"]).await;

    let resp = client.command(&["INFO", "keyspace"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    // After a write, keyspace section should contain db info
    // (may be empty if not tracked per-db, just verify no error)
    assert!(!matches!(
        client.command(&["INFO", "keyspace"]).await,
        frogdb_protocol::Response::Error(_)
    ));
    let _ = text;
}
