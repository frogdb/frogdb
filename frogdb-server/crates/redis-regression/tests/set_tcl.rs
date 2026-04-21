//! Rust port of Redis 8.6.0 `unit/type/set.tcl` test suite.
//!
//! Excludes: encoding-specific loops, random fuzzing, chi-square distribution,
//! readraw, large-memory, `needs:repl`, `needs:debug`, WATCH/MULTI tests.
//!
//! ## Intentional exclusions
//!
//! Encoding-specific tests (FrogDB has a single internal encoding):
//! - `SADD overflows the maximum allowed elements in a listpack - $type` — intentional-incompatibility:encoding — internal-encoding (listpack)
//! - `Set encoding after DEBUG RELOAD` — intentional-incompatibility:encoding — internal-encoding + needs:debug
//! - `Generated sets must be encoded correctly - $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SDIFFSTORE with three sets - $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SUNION hashtable and listpack` — intentional-incompatibility:encoding — internal-encoding
//! - `SRANDMEMBER - $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SPOP integer from listpack set` — intentional-incompatibility:encoding — internal-encoding (listpack)
//! - `SPOP new implementation: code path #1 $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SPOP new implementation: code path #2 $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SPOP new implementation: code path #3 $type` — intentional-incompatibility:encoding — internal-encoding
//! - `SRANDMEMBER histogram distribution - $type` — intentional-incompatibility:encoding — internal-encoding (chi-square)
//! - `SRANDMEMBER with a dict containing long chain` — intentional-incompatibility:encoding — internal-encoding (hash collision)
//!
//! Fuzzing/stress:
//! - `SDIFF fuzzing` — tested-elsewhere — fuzzing/stress
//!
//! Replication-propagation:
//! - `SPOP new implementation: code path #1 propagate as DEL or UNLINK` — intentional-incompatibility:replication — replication-internal
//!
//! Pub/Sub keyspace notification interaction:
//! - `SMOVE only notify dstset when the addition is successful` — intentional-incompatibility:config — needs:config (notify-keyspace-events)
//!
//! Argument-validation edge case (real but minor):
//! (none — SMISMEMBER arity test now passes)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// SADD / SCARD / SISMEMBER / SMISMEMBER / SMEMBERS basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sadd_scard_sismember_smismember_smembers_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "foo"]).await;
    assert_integer_eq(&client.command(&["SADD", "myset", "bar"]).await, 1);
    assert_integer_eq(&client.command(&["SADD", "myset", "bar"]).await, 0);
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 2);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "foo"]).await, 1);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "bar"]).await, 1);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "bla"]).await, 0);

    // SMISMEMBER
    let resp = client.command(&["SMISMEMBER", "myset", "foo"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(&items[0], 1);

    let resp = client.command(&["SMISMEMBER", "myset", "foo", "bar"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 1);

    let resp = client.command(&["SMISMEMBER", "myset", "foo", "bla"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 0);

    // SMEMBERS
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["bar", "foo"]);
}

#[tokio::test]
async fn tcl_sadd_scard_sismember_intset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "17"]).await;
    assert_integer_eq(&client.command(&["SADD", "myset", "16"]).await, 1);
    assert_integer_eq(&client.command(&["SADD", "myset", "16"]).await, 0);
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 2);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "16"]).await, 1);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "17"]).await, 1);
    assert_integer_eq(&client.command(&["SISMEMBER", "myset", "18"]).await, 0);

    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["16", "17"]);
}

// ---------------------------------------------------------------------------
// SMISMEMBER requires one or more members
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_smismember_requires_one_or_more_members() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SMISMEMBER with key only (no members) should fail with arity error
    let resp = client.command(&["SMISMEMBER", "myset"]).await;
    assert_error_prefix(
        &resp,
        "ERR wrong number of arguments for 'smismember' command",
    );
}

#[tokio::test]
async fn tcl_smismember_smembers_scard_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "foo"]).await;
    assert_error_prefix(
        &client.command(&["SMISMEMBER", "mylist", "bar"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(&client.command(&["SMEMBERS", "mylist"]).await, "WRONGTYPE");
    assert_error_prefix(&client.command(&["SCARD", "mylist"]).await, "WRONGTYPE");
}

#[tokio::test]
async fn tcl_smismember_smembers_scard_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SMISMEMBER", "myset1", "foo"]).await;
    let items = unwrap_array(resp);
    assert_integer_eq(&items[0], 0);

    let resp = client
        .command(&["SMISMEMBER", "myset1", "foo", "bar"])
        .await;
    let items = unwrap_array(resp);
    assert_integer_eq(&items[0], 0);
    assert_integer_eq(&items[1], 0);

    let resp = client.command(&["SMEMBERS", "myset1"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());

    assert_integer_eq(&client.command(&["SCARD", "myset1"]).await, 0);
}

#[tokio::test]
async fn tcl_sadd_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["LPUSH", "mylist", "foo"]).await;
    assert_error_prefix(
        &client.command(&["SADD", "mylist", "bar"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_sadd_an_integer_larger_than_64_bits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client
        .command(&["SADD", "myset", "213244124402402314402033402"])
        .await;
    assert_integer_eq(
        &client
            .command(&["SISMEMBER", "myset", "213244124402402314402033402"])
            .await,
        1,
    );
}

#[tokio::test]
async fn tcl_variadic_sadd() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    assert_integer_eq(&client.command(&["SADD", "myset", "a", "b", "c"]).await, 3);
    assert_integer_eq(
        &client
            .command(&["SADD", "myset", "A", "a", "b", "c", "B"])
            .await,
        2,
    );
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["A", "B", "a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// SREM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_srem_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "foo", "ciao"]).await;
    assert_integer_eq(&client.command(&["SREM", "myset", "qux"]).await, 0);
    assert_integer_eq(&client.command(&["SREM", "myset", "ciao"]).await, 1);
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["foo"]);
}

#[tokio::test]
async fn tcl_srem_intset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "3", "4", "5"]).await;
    assert_integer_eq(&client.command(&["SREM", "myset", "6"]).await, 0);
    assert_integer_eq(&client.command(&["SREM", "myset", "4"]).await, 1);
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["3", "5"]);
}

#[tokio::test]
async fn tcl_srem_with_multiple_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "a", "b", "c", "d"]).await;
    assert_integer_eq(&client.command(&["SREM", "myset", "k", "k", "k"]).await, 0);
    assert_integer_eq(
        &client.command(&["SREM", "myset", "b", "d", "x", "y"]).await,
        2,
    );
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "myset"]).await);
    members.sort();
    assert_eq!(members, vec!["a", "c"]);
}

#[tokio::test]
async fn tcl_srem_variadic_version_with_more_args_needed_to_destroy_the_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "1", "2", "3"]).await;
    assert_integer_eq(
        &client
            .command(&["SREM", "myset", "1", "2", "3", "4", "5", "6", "7", "8"])
            .await,
        3,
    );
}

// ---------------------------------------------------------------------------
// SINTERCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sintercard_with_illegal_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_error_prefix(&client.command(&["SINTERCARD"]).await, "ERR");
    assert_error_prefix(&client.command(&["SINTERCARD", "1"]).await, "ERR");
    assert_error_prefix(
        &client.command(&["SINTERCARD", "0", "myset{t}"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client.command(&["SINTERCARD", "a", "myset{t}"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client.command(&["SINTERCARD", "2", "myset{t}"]).await,
        "ERR",
    );
    assert_error_prefix(
        &client
            .command(&["SINTERCARD", "1", "myset{t}", "LIMIT", "-1"])
            .await,
        "ERR",
    );
}

#[tokio::test]
async fn tcl_sintercard_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set{t}", "key1{t}"]).await;
    client.command(&["SADD", "set{t}", "a", "b", "c"]).await;
    client.command(&["SET", "key1{t}", "x"]).await;

    assert_error_prefix(
        &client.command(&["SINTERCARD", "1", "key1{t}"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client
            .command(&["SINTERCARD", "2", "set{t}", "key1{t}"])
            .await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_sintercard_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "1", "non-existing-key"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "1", "non-existing-key", "LIMIT", "0"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "1", "non-existing-key", "LIMIT", "10"])
            .await,
        0,
    );
}

// ---------------------------------------------------------------------------
// SINTER / SINTERSTORE / SUNION / SUNIONSTORE / SDIFF / SDIFFSTORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sinter_with_two_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "set2{t}"]).await;
    for i in 0..200 {
        client.command(&["SADD", "set1{t}", &i.to_string()]).await;
        client
            .command(&["SADD", "set2{t}", &(i + 195).to_string()])
            .await;
    }
    let mut result = extract_bulk_strings(&client.command(&["SINTER", "set1{t}", "set2{t}"]).await);
    result.sort_by_key(|a| a.parse::<i64>().unwrap());
    assert_eq!(result, vec!["195", "196", "197", "198", "199"]);
}

#[tokio::test]
async fn tcl_sintercard_with_two_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "set2{t}"]).await;
    for i in 0..200 {
        client.command(&["SADD", "set1{t}", &i.to_string()]).await;
        client
            .command(&["SADD", "set2{t}", &(i + 195).to_string()])
            .await;
    }
    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "2", "set1{t}", "set2{t}"])
            .await,
        5,
    );
    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "2", "set1{t}", "set2{t}", "LIMIT", "3"])
            .await,
        3,
    );
    assert_integer_eq(
        &client
            .command(&["SINTERCARD", "2", "set1{t}", "set2{t}", "LIMIT", "10"])
            .await,
        5,
    );
}

#[tokio::test]
async fn tcl_sinterstore_with_two_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "setres{t}"])
        .await;
    for i in 0..200 {
        client.command(&["SADD", "set1{t}", &i.to_string()]).await;
        client
            .command(&["SADD", "set2{t}", &(i + 195).to_string()])
            .await;
    }
    client
        .command(&["SINTERSTORE", "setres{t}", "set1{t}", "set2{t}"])
        .await;
    let mut result = extract_bulk_strings(&client.command(&["SMEMBERS", "setres{t}"]).await);
    result.sort_by_key(|a| a.parse::<i64>().unwrap());
    assert_eq!(result, vec!["195", "196", "197", "198", "199"]);
}

#[tokio::test]
async fn tcl_sdiff_with_two_sets() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "set4{t}"]).await;
    for i in 0..200 {
        client.command(&["SADD", "set1{t}", &i.to_string()]).await;
    }
    for i in 5..200 {
        client.command(&["SADD", "set4{t}", &i.to_string()]).await;
    }
    let mut result = extract_bulk_strings(&client.command(&["SDIFF", "set1{t}", "set4{t}"]).await);
    result.sort_by_key(|a| a.parse::<i64>().unwrap());
    assert_eq!(result, vec!["0", "1", "2", "3", "4"]);
}

#[tokio::test]
async fn tcl_sdiff_with_first_set_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    client
        .command(&["SADD", "set2{t}", "1", "2", "3", "4"])
        .await;
    client
        .command(&["SADD", "set3{t}", "a", "b", "c", "d"])
        .await;
    let result = client
        .command(&["SDIFF", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    let items = unwrap_array(result);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_sdiff_with_same_set_two_times() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1"]).await;
    client
        .command(&["SADD", "set1", "a", "b", "c", "1", "2", "3", "4", "5", "6"])
        .await;
    let result = client.command(&["SDIFF", "set1", "set1"]).await;
    let items = unwrap_array(result);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_sdiff_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "key1{t}"]).await;
    client.command(&["SET", "key1{t}", "x"]).await;
    assert_error_prefix(
        &client.command(&["SDIFF", "key1{t}", "noset{t}"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["SDIFF", "noset{t}", "key1{t}"]).await,
        "WRONGTYPE",
    );

    client.command(&["SADD", "set1{t}", "a", "b", "c"]).await;
    assert_error_prefix(
        &client.command(&["SDIFF", "key1{t}", "set1{t}"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["SDIFF", "set1{t}", "key1{t}"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_sdiff_should_handle_non_existing_key_as_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    client.command(&["SADD", "set1{t}", "a", "b", "c"]).await;
    client.command(&["SADD", "set2{t}", "b", "c", "d"]).await;
    let mut result = extract_bulk_strings(
        &client
            .command(&["SDIFF", "set1{t}", "set2{t}", "set3{t}"])
            .await,
    );
    result.sort();
    assert_eq!(result, vec!["a"]);
}

#[tokio::test]
async fn tcl_sunion_should_handle_non_existing_key_as_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    client.command(&["SADD", "set1{t}", "a", "b", "c"]).await;
    client.command(&["SADD", "set2{t}", "b", "c", "d"]).await;
    let mut result = extract_bulk_strings(
        &client
            .command(&["SUNION", "set1{t}", "set2{t}", "set3{t}"])
            .await,
    );
    result.sort();
    assert_eq!(result, vec!["a", "b", "c", "d"]);
}

#[tokio::test]
async fn tcl_sinter_should_handle_non_existing_key_as_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    client.command(&["SADD", "set1{t}", "a", "b", "c"]).await;
    client.command(&["SADD", "set2{t}", "b", "c", "d"]).await;
    let result = client
        .command(&["SINTER", "set1{t}", "set2{t}", "set3{t}"])
        .await;
    let items = unwrap_array(result);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_sinter_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "key1{t}"]).await;
    client.command(&["SET", "key1{t}", "x"]).await;
    assert_error_prefix(
        &client.command(&["SINTER", "key1{t}", "noset{t}"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["SINTER", "noset{t}", "key1{t}"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_sinter_with_same_integer_elements_but_different_encoding() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set1{t}", "set2{t}"]).await;
    client.command(&["SADD", "set1{t}", "1", "2", "3"]).await;
    client
        .command(&["SADD", "set2{t}", "1", "2", "3", "a"])
        .await;
    client.command(&["SREM", "set2{t}", "a"]).await;
    let mut result = extract_bulk_strings(&client.command(&["SINTER", "set1{t}", "set2{t}"]).await);
    result.sort();
    assert_eq!(result, vec!["1", "2", "3"]);
}

// ---------------------------------------------------------------------------
// SPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_spop_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    let mut popped = Vec::new();
    for _ in 0..3 {
        let resp = client.command(&["SPOP", "myset"]).await;
        popped.push(String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap());
    }
    popped.sort();
    assert_eq!(popped, vec!["a", "b", "c"]);
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 0);
}

#[tokio::test]
async fn tcl_spop_with_count() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    for c in b'a'..=b'z' {
        client
            .command(&["SADD", "myset", &String::from(c as char)])
            .await;
    }
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 26);

    let resp = client.command(&["SPOP", "myset", "11"]).await;
    assert_array_len(&resp, 11);

    let resp = client.command(&["SPOP", "myset", "9"]).await;
    assert_array_len(&resp, 9);

    let resp = client.command(&["SPOP", "myset", "0"]).await;
    assert_array_len(&resp, 0);

    // Remaining 6 elements
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 6);
}

#[tokio::test]
async fn tcl_spop_using_integers_knuth_and_floyd() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    for i in 1..=20 {
        client.command(&["SADD", "myset", &i.to_string()]).await;
    }
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 20);
    client.command(&["SPOP", "myset", "1"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 19);
    client.command(&["SPOP", "myset", "2"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 17);
    client.command(&["SPOP", "myset", "3"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 14);
    client.command(&["SPOP", "myset", "10"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 4);
    client.command(&["SPOP", "myset", "10"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 0);
    client.command(&["SPOP", "myset", "1"]).await;
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 0);
}

#[tokio::test]
async fn tcl_spop_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["SPOP", "nonexisting_key", "100"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

// ---------------------------------------------------------------------------
// SRANDMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_srandmember_count_of_0() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "a"]).await;
    let resp = client.command(&["SRANDMEMBER", "myset", "0"]).await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_srandmember_with_count_against_non_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["SRANDMEMBER", "nonexisting_key", "100"])
        .await;
    let items = unwrap_array(resp);
    assert!(items.is_empty());
}

#[tokio::test]
async fn tcl_srandmember_count_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset"]).await;
    client.command(&["SADD", "myset", "a"]).await;
    assert_error_prefix(
        &client
            .command(&["SRANDMEMBER", "myset", "-9223372036854775808"])
            .await,
        "ERR",
    );
}

// ---------------------------------------------------------------------------
// SMOVE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_smove_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "myset1{t}", "myset2{t}", "myset3{t}"])
        .await;
    client.command(&["SADD", "myset1{t}", "1", "a", "b"]).await;
    client.command(&["SADD", "myset2{t}", "2", "3", "4"]).await;

    // Move element between sets
    assert_integer_eq(
        &client
            .command(&["SMOVE", "myset1{t}", "myset2{t}", "a"])
            .await,
        1,
    );
    let mut s1 = extract_bulk_strings(&client.command(&["SMEMBERS", "myset1{t}"]).await);
    s1.sort();
    assert_eq!(s1, vec!["1", "b"]);
    let mut s2 = extract_bulk_strings(&client.command(&["SMEMBERS", "myset2{t}"]).await);
    s2.sort();
    assert_eq!(s2, vec!["2", "3", "4", "a"]);
}

#[tokio::test]
async fn tcl_smove_non_existing_element() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset1{t}", "myset2{t}"]).await;
    client.command(&["SADD", "myset1{t}", "1", "a", "b"]).await;
    client.command(&["SADD", "myset2{t}", "2", "3", "4"]).await;

    assert_integer_eq(
        &client
            .command(&["SMOVE", "myset1{t}", "myset2{t}", "foo"])
            .await,
        0,
    );
    assert_integer_eq(
        &client
            .command(&["SMOVE", "myset1{t}", "myset1{t}", "foo"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_smove_non_existing_src_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "noset{t}", "myset2{t}"]).await;
    client.command(&["SADD", "myset2{t}", "2", "3", "4"]).await;
    assert_integer_eq(
        &client
            .command(&["SMOVE", "noset{t}", "myset2{t}", "foo"])
            .await,
        0,
    );
}

#[tokio::test]
async fn tcl_smove_to_non_existing_destination_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "myset1{t}", "myset3{t}"]).await;
    client.command(&["SADD", "myset1{t}", "1", "a", "b"]).await;
    assert_integer_eq(
        &client
            .command(&["SMOVE", "myset1{t}", "myset3{t}", "a"])
            .await,
        1,
    );
    let mut s1 = extract_bulk_strings(&client.command(&["SMEMBERS", "myset1{t}"]).await);
    s1.sort();
    assert_eq!(s1, vec!["1", "b"]);
    let s3 = extract_bulk_strings(&client.command(&["SMEMBERS", "myset3{t}"]).await);
    assert_eq!(s3, vec!["a"]);
}

#[tokio::test]
async fn tcl_smove_wrong_src_key_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x{t}", "myset2{t}"]).await;
    client.command(&["SET", "x{t}", "10"]).await;
    client.command(&["SADD", "myset2{t}", "a"]).await;
    assert_error_prefix(
        &client.command(&["SMOVE", "x{t}", "myset2{t}", "foo"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_smove_wrong_dst_key_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x{t}", "myset1{t}"]).await;
    client.command(&["SET", "x{t}", "10"]).await;
    client.command(&["SADD", "myset1{t}", "a"]).await;
    assert_error_prefix(
        &client.command(&["SMOVE", "myset1{t}", "x{t}", "foo"]).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn tcl_smove_with_identical_source_and_destination() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "set{t}"]).await;
    client.command(&["SADD", "set{t}", "a", "b", "c"]).await;
    client.command(&["SMOVE", "set{t}", "set{t}", "b"]).await;
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "set{t}"]).await);
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// SDIFFSTORE / SINTERSTORE / SUNIONSTORE non-existing key handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sdiffstore_should_handle_non_existing_key_as_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "set3{t}", "setres{t}"])
        .await;

    // Both non-existing → result is empty, dstkey deleted
    client.command(&["SET", "setres{t}", "xxx"]).await;
    assert_integer_eq(
        &client
            .command(&["SDIFFSTORE", "setres{t}", "foo111{t}", "bar222{t}"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "setres{t}"]).await, 0);

    // set1 has elements, set2 empty
    client.command(&["SADD", "set1{t}", "a", "b", "c"]).await;
    assert_integer_eq(
        &client
            .command(&["SDIFFSTORE", "set3{t}", "set1{t}", "set2{t}"])
            .await,
        3,
    );
    let mut members = extract_bulk_strings(&client.command(&["SMEMBERS", "set3{t}"]).await);
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn tcl_sinterstore_against_non_existing_keys_should_delete_dstkey() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["DEL", "set1{t}", "set2{t}", "setres{t}"])
        .await;

    client.command(&["SET", "setres{t}", "xxx"]).await;
    assert_integer_eq(
        &client
            .command(&["SINTERSTORE", "setres{t}", "foo111{t}", "bar222{t}"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "setres{t}"]).await, 0);
}

#[tokio::test]
async fn tcl_sunionstore_against_non_existing_keys_should_delete_dstkey() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "setres{t}"]).await;
    client.command(&["SET", "setres{t}", "xxx"]).await;
    assert_integer_eq(
        &client
            .command(&["SUNIONSTORE", "setres{t}", "foo111{t}", "bar222{t}"])
            .await,
        0,
    );
    assert_integer_eq(&client.command(&["EXISTS", "setres{t}"]).await, 0);
}

// ---------------------------------------------------------------------------
// Wrong type checks for STORE variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sunion_against_non_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "key1{t}", "set1{t}"]).await;
    client.command(&["SET", "key1{t}", "x"]).await;
    assert_error_prefix(
        &client.command(&["SUNION", "key1{t}", "noset{t}"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["SUNION", "noset{t}", "key1{t}"]).await,
        "WRONGTYPE",
    );
}
