use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn large_mset_succeeds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1000 key-value pairs
    let mut args = vec!["MSET"];
    let pairs: Vec<String> = (0..1000)
        .flat_map(|i| [format!("{{k}}key{i}"), format!("val{i}")])
        .collect();
    for p in &pairs {
        args.push(p.as_str());
    }
    let resp = client.command(&args).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn very_large_value_set_get_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // 1 MB value
    let large_val = "x".repeat(1024 * 1024);
    assert_ok(&client.command(&["SET", "bigkey", &large_val]).await);

    let resp = client.command(&["GET", "bigkey"]).await;
    let got = unwrap_bulk(&resp);
    assert_eq!(got.len(), large_val.len());
    assert_eq!(&got[..10], b"xxxxxxxxxx");
}

#[tokio::test]
async fn large_mget_succeeds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Pre-populate
    let mut set_args = vec!["MSET"];
    let pairs: Vec<String> = (0..100)
        .flat_map(|i| [format!("{{k}}key{i}"), format!("val{i}")])
        .collect();
    for p in &pairs {
        set_args.push(p.as_str());
    }
    client.command(&set_args).await;

    // MGET all 100
    let mut get_args = vec!["MGET"];
    let keys: Vec<String> = (0..100).map(|i| format!("{{k}}key{i}")).collect();
    for k in &keys {
        get_args.push(k.as_str());
    }
    let resp = client.command(&get_args).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 100);
}
