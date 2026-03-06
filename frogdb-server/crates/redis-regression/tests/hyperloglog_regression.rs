use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn pfcount_variable_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Single key
    client.command(&["PFADD", "{k}hll1", "a", "b", "c"]).await;
    let resp = client.command(&["PFCOUNT", "{k}hll1"]).await;
    assert!(unwrap_integer(&resp) >= 3);

    // Multiple keys (union)
    client.command(&["PFADD", "{k}hll2", "c", "d", "e"]).await;
    let resp = client.command(&["PFCOUNT", "{k}hll1", "{k}hll2"]).await;
    assert!(unwrap_integer(&resp) >= 5);
}

#[tokio::test]
async fn pfmerge_with_multiple_sources() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["PFADD", "{k}src1", "a", "b", "c"]).await;
    client.command(&["PFADD", "{k}src2", "d", "e", "f"]).await;
    client.command(&["PFADD", "{k}src3", "g", "h", "i"]).await;

    let resp = client
        .command(&["PFMERGE", "{k}dest", "{k}src1", "{k}src2", "{k}src3"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["PFCOUNT", "{k}dest"]).await;
    assert!(unwrap_integer(&resp) >= 9);
}

#[tokio::test]
async fn pfdebug_todense_converts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["PFADD", "myhll", "a"]).await;
    let resp = client.command(&["PFDEBUG", "TODENSE", "myhll"]).await;
    // Returns 1 indicating success (FrogDB HLL is always dense, so this is a no-op)
    assert_eq!(unwrap_integer(&resp), 1);
}
