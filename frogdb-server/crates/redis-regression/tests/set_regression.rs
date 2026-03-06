use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn sadd_scard_sismember_smembers_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_eq!(
        unwrap_integer(&client.command(&["SADD", "myset", "a", "b", "c"]).await),
        3
    );
    // Adding existing element returns 0
    assert_eq!(
        unwrap_integer(&client.command(&["SADD", "myset", "a"]).await),
        0
    );

    assert_eq!(
        unwrap_integer(&client.command(&["SCARD", "myset"]).await),
        3
    );
    assert_eq!(
        unwrap_integer(&client.command(&["SISMEMBER", "myset", "a"]).await),
        1
    );
    assert_eq!(
        unwrap_integer(&client.command(&["SISMEMBER", "myset", "missing"]).await),
        0
    );

    let members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    let mut sorted = members.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn smismember_batch_membership() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "foo", "bar"]).await;

    let resp = client
        .command(&["SMISMEMBER", "myset", "foo", "baz", "bar"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_eq!(unwrap_integer(&items[0]), 1);
    assert_eq!(unwrap_integer(&items[1]), 0);
    assert_eq!(unwrap_integer(&items[2]), 1);
}

#[tokio::test]
async fn sinter_sunion_sdiff_correctness() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{s}a", "1", "2", "3"]).await;
    client.command(&["SADD", "{s}b", "2", "3", "4"]).await;

    let inter = extract_bulk_strings(&client.command(&["SINTER", "{s}a", "{s}b"]).await);
    let mut inter_sorted = inter.clone();
    inter_sorted.sort();
    assert_eq!(inter_sorted, vec!["2", "3"]);

    let union = extract_bulk_strings(&client.command(&["SUNION", "{s}a", "{s}b"]).await);
    let mut union_sorted = union.clone();
    union_sorted.sort();
    assert_eq!(union_sorted, vec!["1", "2", "3", "4"]);

    let diff = extract_bulk_strings(&client.command(&["SDIFF", "{s}a", "{s}b"]).await);
    assert_eq!(diff, vec!["1"]);
}

#[tokio::test]
async fn sintercard_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{s}a", "1", "2", "3", "4"]).await;
    client.command(&["SADD", "{s}b", "2", "3", "4", "5"]).await;

    // Without limit
    let resp = client.command(&["SINTERCARD", "2", "{s}a", "{s}b"]).await;
    assert_eq!(unwrap_integer(&resp), 3);

    // With LIMIT 2
    let resp = client
        .command(&["SINTERCARD", "2", "{s}a", "{s}b", "LIMIT", "2"])
        .await;
    assert_eq!(unwrap_integer(&resp), 2);
}

#[tokio::test]
async fn sinterstore_sunionstore_sdiffstore() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{s}a", "1", "2", "3"]).await;
    client.command(&["SADD", "{s}b", "2", "3", "4"]).await;

    let resp = client
        .command(&["SINTERSTORE", "{s}dest", "{s}a", "{s}b"])
        .await;
    assert_eq!(unwrap_integer(&resp), 2);

    let resp = client
        .command(&["SUNIONSTORE", "{s}udest", "{s}a", "{s}b"])
        .await;
    assert_eq!(unwrap_integer(&resp), 4);

    let resp = client
        .command(&["SDIFFSTORE", "{s}ddest", "{s}a", "{s}b"])
        .await;
    assert_eq!(unwrap_integer(&resp), 1);
}

#[tokio::test]
async fn spop_returns_and_removes_member() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "only"]).await;

    let resp = client.command(&["SPOP", "myset"]).await;
    assert_bulk_eq(&resp, b"only");

    // Set should be empty
    assert_eq!(
        unwrap_integer(&client.command(&["SCARD", "myset"]).await),
        0
    );

    // SPOP on empty set returns nil
    let resp = client.command(&["SPOP", "myset"]).await;
    assert!(matches!(resp, frogdb_protocol::Response::Bulk(None)));
}

#[tokio::test]
async fn srandmember_positive_count_no_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    // Count > size returns all elements without duplicates
    let resp = client.command(&["SRANDMEMBER", "myset", "10"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn srandmember_negative_count_allows_duplicates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b"]).await;

    // Negative count allows repeats
    let resp = client.command(&["SRANDMEMBER", "myset", "-5"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn smove_between_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "{s}src", "a", "b"]).await;
    client.command(&["SADD", "{s}dst", "c"]).await;

    // Move existing member
    let resp = client.command(&["SMOVE", "{s}src", "{s}dst", "a"]).await;
    assert_eq!(unwrap_integer(&resp), 1);

    assert_eq!(
        unwrap_integer(&client.command(&["SISMEMBER", "{s}src", "a"]).await),
        0
    );
    assert_eq!(
        unwrap_integer(&client.command(&["SISMEMBER", "{s}dst", "a"]).await),
        1
    );

    // Move non-existing member returns 0
    let resp = client
        .command(&["SMOVE", "{s}src", "{s}dst", "missing"])
        .await;
    assert_eq!(unwrap_integer(&resp), 0);
}

#[tokio::test]
async fn set_commands_against_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "item"]).await;

    let resp = client.command(&["SADD", "mylist", "x"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let resp = client.command(&["SCARD", "mylist"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let resp = client.command(&["SMEMBERS", "mylist"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}
