//! Regression tests for Bloom filter (BF.*) and Cuckoo filter (CF.*) commands.

use bytes::Bytes;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Bloom Filter tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bf_reserve_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["BF.RESERVE", "bf1", "0.01", "100"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn bf_reserve_with_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reserve with EXPANSION option
    let resp = client
        .command(&["BF.RESERVE", "bf_exp", "0.01", "100", "EXPANSION", "4"])
        .await;
    assert_ok(&resp);

    // Reserve with NONSCALING option
    let resp = client
        .command(&["BF.RESERVE", "bf_ns", "0.01", "100", "NONSCALING"])
        .await;
    assert_ok(&resp);
}

#[tokio::test]
async fn bf_reserve_duplicate_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["BF.RESERVE", "bf_dup", "0.01", "100"])
        .await;
    assert_ok(&resp);

    // Reserving the same key again should error
    let resp = client
        .command(&["BF.RESERVE", "bf_dup", "0.01", "100"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn bf_add_and_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add a new item — should return 1
    let resp = client.command(&["BF.ADD", "bf_ae", "hello"]).await;
    assert_integer_eq(&resp, 1);

    // Add the same item again — should return 0 (already exists)
    let resp = client.command(&["BF.ADD", "bf_ae", "hello"]).await;
    assert_integer_eq(&resp, 0);

    // EXISTS on added item
    let resp = client.command(&["BF.EXISTS", "bf_ae", "hello"]).await;
    assert_integer_eq(&resp, 1);

    // EXISTS on non-added item
    let resp = client.command(&["BF.EXISTS", "bf_ae", "world"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn bf_add_auto_creates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // BF.ADD on a non-existent key should auto-create the filter
    let resp = client.command(&["BF.ADD", "bf_auto", "item1"]).await;
    assert_integer_eq(&resp, 1);

    // Verify the key exists by checking the item
    let resp = client.command(&["BF.EXISTS", "bf_auto", "item1"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn bf_madd_and_mexists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MADD multiple items
    let resp = client.command(&["BF.MADD", "bf_m", "a", "b", "c"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    // All should be newly added
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 1);
    assert_integer_eq(&items[2], 1);

    // MADD again with overlap
    let resp = client.command(&["BF.MADD", "bf_m", "b", "c", "d"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_integer_eq(&items[0], 0); // b already existed
    assert_integer_eq(&items[1], 0); // c already existed
    assert_integer_eq(&items[2], 1); // d is new

    // MEXISTS
    let resp = client.command(&["BF.MEXISTS", "bf_m", "a", "d", "z"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_integer_eq(&items[0], 1); // a exists
    assert_integer_eq(&items[1], 1); // d exists
    assert_integer_eq(&items[2], 0); // z does not exist
}

#[tokio::test]
async fn bf_insert_with_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "BF.INSERT",
            "bf_ins",
            "CAPACITY",
            "200",
            "ERROR",
            "0.001",
            "ITEMS",
            "x",
            "y",
            "z",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 1);
    assert_integer_eq(&items[2], 1);

    // Verify items exist
    let resp = client.command(&["BF.EXISTS", "bf_ins", "y"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn bf_insert_nocreate_missing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // NOCREATE on a missing key should return an error
    let resp = client
        .command(&["BF.INSERT", "bf_nocreate", "NOCREATE", "ITEMS", "a"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn bf_info_all_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["BF.RESERVE", "bf_info", "0.01", "100"])
        .await;
    client.command(&["BF.ADD", "bf_info", "item1"]).await;
    client.command(&["BF.ADD", "bf_info", "item2"]).await;

    let resp = client.command(&["BF.INFO", "bf_info"]).await;
    let items = unwrap_array(resp);
    // Should contain key-value pairs: Capacity, Size, Number of filters,
    // Number of items inserted, Expansion rate => 10 elements
    assert_eq!(items.len(), 10);

    // Verify the field names
    assert_bulk_eq(&items[0], b"Capacity");
    assert_bulk_eq(&items[2], b"Size");
    assert_bulk_eq(&items[4], b"Number of filters");
    assert_bulk_eq(&items[6], b"Number of items inserted");
    assert_bulk_eq(&items[8], b"Expansion rate");

    // Number of items inserted should be 2
    assert_integer_eq(&items[7], 2);
}

#[tokio::test]
async fn bf_info_specific_field() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["BF.RESERVE", "bf_info2", "0.01", "100"])
        .await;
    client.command(&["BF.ADD", "bf_info2", "a"]).await;

    let resp = client.command(&["BF.INFO", "bf_info2", "CAPACITY"]).await;
    let cap = unwrap_integer(&resp);
    assert!(cap >= 100);

    let resp = client.command(&["BF.INFO", "bf_info2", "ITEMS"]).await;
    assert_integer_eq(&resp, 1);

    let resp = client.command(&["BF.INFO", "bf_info2", "FILTERS"]).await;
    assert!(unwrap_integer(&resp) >= 1);
}

#[tokio::test]
async fn bf_card_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["BF.ADD", "bf_card", "a"]).await;
    client.command(&["BF.ADD", "bf_card", "b"]).await;
    client.command(&["BF.ADD", "bf_card", "c"]).await;

    let resp = client.command(&["BF.CARD", "bf_card"]).await;
    assert_integer_eq(&resp, 3);
}

#[tokio::test]
async fn bf_card_nonexistent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["BF.CARD", "bf_noexist"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn bf_exists_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // BF.EXISTS on a key that does not exist should return 0
    let resp = client.command(&["BF.EXISTS", "bf_ghost", "anything"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn bf_scandump_loadchunk_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create and populate a bloom filter
    client
        .command(&["BF.RESERVE", "bf_dump", "0.01", "100"])
        .await;
    client.command(&["BF.ADD", "bf_dump", "alpha"]).await;
    client.command(&["BF.ADD", "bf_dump", "beta"]).await;
    client.command(&["BF.ADD", "bf_dump", "gamma"]).await;

    // FrogDB uses single-chunk dump: SCANDUMP with iterator 0 returns [0, data]
    let resp = client.command(&["BF.SCANDUMP", "bf_dump", "0"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_eq!(unwrap_integer(&arr[0]), 0); // iterator 0 = done (single chunk)
    let data = unwrap_bulk(&arr[1]).to_vec();
    assert!(!data.is_empty(), "SCANDUMP should return non-empty data");

    // Load into a new key with iterator 0
    let cmd = Bytes::from_static(b"BF.LOADCHUNK");
    let key = Bytes::from_static(b"bf_restored");
    let iter_arg = Bytes::from_static(b"0");
    let data_bytes = Bytes::from(data);
    let resp = client
        .command_raw(&[&cmd, &key, &iter_arg, &data_bytes])
        .await;
    assert_ok(&resp);

    // Verify the restored filter has the same items
    let resp = client.command(&["BF.EXISTS", "bf_restored", "alpha"]).await;
    assert_integer_eq(&resp, 1);
    let resp = client.command(&["BF.EXISTS", "bf_restored", "beta"]).await;
    assert_integer_eq(&resp, 1);
    let resp = client.command(&["BF.EXISTS", "bf_restored", "gamma"]).await;
    assert_integer_eq(&resp, 1);
}

// ---------------------------------------------------------------------------
// Cuckoo Filter tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cf_reserve_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["CF.RESERVE", "cf1", "1024"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn cf_reserve_with_options() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "CF.RESERVE",
            "cf_opts",
            "1024",
            "BUCKETSIZE",
            "4",
            "MAXITERATIONS",
            "20",
            "EXPANSION",
            "2",
        ])
        .await;
    assert_ok(&resp);
}

#[tokio::test]
async fn cf_add_and_exists() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CF.ADD auto-creates and returns OK
    let resp = client.command(&["CF.ADD", "cf_ae", "hello"]).await;
    assert_ok(&resp);

    // EXISTS should return 1
    let resp = client.command(&["CF.EXISTS", "cf_ae", "hello"]).await;
    assert_integer_eq(&resp, 1);

    // Non-existent item
    let resp = client.command(&["CF.EXISTS", "cf_ae", "world"]).await;
    assert_integer_eq(&resp, 0);

    // EXISTS on non-existent key returns 0
    let resp = client.command(&["CF.EXISTS", "cf_ghost", "anything"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn cf_addnx_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // First ADDNX should return 1 (added)
    let resp = client.command(&["CF.ADDNX", "cf_nx", "item"]).await;
    assert_integer_eq(&resp, 1);

    // Second ADDNX for same item should return 0 (already exists)
    let resp = client.command(&["CF.ADDNX", "cf_nx", "item"]).await;
    assert_integer_eq(&resp, 0);

    // Different item should return 1
    let resp = client.command(&["CF.ADDNX", "cf_nx", "other"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn cf_insert_multiple() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CF.INSERT", "cf_ins", "ITEMS", "a", "b", "c"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    // All should be successfully added (1)
    assert_integer_eq(&items[0], 1);
    assert_integer_eq(&items[1], 1);
    assert_integer_eq(&items[2], 1);

    // Verify items exist
    let resp = client.command(&["CF.EXISTS", "cf_ins", "b"]).await;
    assert_integer_eq(&resp, 1);
}

#[tokio::test]
async fn cf_insertnx_nocreate() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // INSERTNX with NOCREATE on missing key should error
    let resp = client
        .command(&["CF.INSERTNX", "cf_nocreate", "NOCREATE", "ITEMS", "a"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn cf_del_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CF.ADD", "cf_del", "target"]).await;

    // Verify it exists
    let resp = client.command(&["CF.EXISTS", "cf_del", "target"]).await;
    assert_integer_eq(&resp, 1);

    // Delete it
    let resp = client.command(&["CF.DEL", "cf_del", "target"]).await;
    assert_integer_eq(&resp, 1);

    // Should no longer exist
    let resp = client.command(&["CF.EXISTS", "cf_del", "target"]).await;
    assert_integer_eq(&resp, 0);

    // Deleting again should return 0
    let resp = client.command(&["CF.DEL", "cf_del", "target"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn cf_count_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CF.ADD allows duplicates (unlike ADDNX)
    client.command(&["CF.ADD", "cf_cnt", "item"]).await;
    client.command(&["CF.ADD", "cf_cnt", "item"]).await;
    client.command(&["CF.ADD", "cf_cnt", "item"]).await;

    let resp = client.command(&["CF.COUNT", "cf_cnt", "item"]).await;
    assert!(unwrap_integer(&resp) >= 1);

    // COUNT on non-existent item returns 0
    let resp = client.command(&["CF.COUNT", "cf_cnt", "missing"]).await;
    assert_integer_eq(&resp, 0);

    // COUNT on non-existent key returns 0
    let resp = client.command(&["CF.COUNT", "cf_nokey", "anything"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn cf_info_all_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CF.RESERVE", "cf_info", "1024"]).await;
    client.command(&["CF.ADD", "cf_info", "x"]).await;
    client.command(&["CF.ADD", "cf_info", "y"]).await;

    let resp = client.command(&["CF.INFO", "cf_info"]).await;
    let items = unwrap_array(resp);
    // Should contain key-value pairs: Size, Number of buckets, Number of filters,
    // Number of items inserted, Number of items deleted, Bucket size,
    // Expansion rate, Max iterations => 16 elements
    assert_eq!(items.len(), 16);

    assert_bulk_eq(&items[0], b"Size");
    assert_bulk_eq(&items[2], b"Number of buckets");
    assert_bulk_eq(&items[4], b"Number of filters");
    assert_bulk_eq(&items[6], b"Number of items inserted");
    assert_bulk_eq(&items[8], b"Number of items deleted");
    assert_bulk_eq(&items[10], b"Bucket size");
    assert_bulk_eq(&items[12], b"Expansion rate");
    assert_bulk_eq(&items[14], b"Max iterations");

    // Number of items inserted should be 2
    assert_integer_eq(&items[7], 2);
    // Number of items deleted should be 0
    assert_integer_eq(&items[9], 0);
}

#[tokio::test]
async fn cf_mexists_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CF.ADD", "cf_me", "a"]).await;
    client.command(&["CF.ADD", "cf_me", "b"]).await;

    let resp = client
        .command(&["CF.MEXISTS", "cf_me", "a", "b", "c"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    assert_integer_eq(&items[0], 1); // a exists
    assert_integer_eq(&items[1], 1); // b exists
    assert_integer_eq(&items[2], 0); // c does not exist
}

#[tokio::test]
async fn cf_scandump_loadchunk_roundtrip() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create and populate a cuckoo filter
    client.command(&["CF.RESERVE", "cf_dump", "1024"]).await;
    client.command(&["CF.ADD", "cf_dump", "alpha"]).await;
    client.command(&["CF.ADD", "cf_dump", "beta"]).await;
    client.command(&["CF.ADD", "cf_dump", "gamma"]).await;

    // FrogDB uses single-chunk dump: SCANDUMP with iterator 0 returns [0, data]
    let resp = client.command(&["CF.SCANDUMP", "cf_dump", "0"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_eq!(unwrap_integer(&arr[0]), 0); // iterator 0 = done
    let data = unwrap_bulk(&arr[1]).to_vec();
    assert!(!data.is_empty(), "CF.SCANDUMP should return non-empty data");

    // Load into a new key with iterator 0
    let cmd = Bytes::from_static(b"CF.LOADCHUNK");
    let key = Bytes::from_static(b"cf_restored");
    let iter_arg = Bytes::from_static(b"0");
    let data_bytes = Bytes::from(data);
    let resp = client
        .command_raw(&[&cmd, &key, &iter_arg, &data_bytes])
        .await;
    assert_ok(&resp);

    // Verify the restored filter has the same items
    let resp = client.command(&["CF.EXISTS", "cf_restored", "alpha"]).await;
    assert_integer_eq(&resp, 1);
    let resp = client.command(&["CF.EXISTS", "cf_restored", "beta"]).await;
    assert_integer_eq(&resp, 1);
    let resp = client.command(&["CF.EXISTS", "cf_restored", "gamma"]).await;
    assert_integer_eq(&resp, 1);
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bf_reserve_invalid_error_rate() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // error_rate = 0 should be rejected
    let resp = client.command(&["BF.RESERVE", "bf_bad1", "0", "100"]).await;
    assert_error_prefix(&resp, "ERR");

    // error_rate = 1 should be rejected
    let resp = client.command(&["BF.RESERVE", "bf_bad2", "1", "100"]).await;
    assert_error_prefix(&resp, "ERR");

    // Negative error_rate
    let resp = client
        .command(&["BF.RESERVE", "bf_bad3", "-0.5", "100"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn bf_wrong_type() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a string key
    client.command(&["SET", "mystr", "value"]).await;

    // BF.ADD on a string key should return WRONGTYPE error
    let resp = client.command(&["BF.ADD", "mystr", "item"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    // BF.EXISTS on a string key should also return WRONGTYPE
    let resp = client.command(&["BF.EXISTS", "mystr", "item"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");
}

// ---------------------------------------------------------------------------
// False positive rate verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bf_false_positive_rate() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reserve with 1% error rate, capacity 1000
    let resp = client
        .command(&["BF.RESERVE", "bf_fp", "0.01", "1000"])
        .await;
    assert_ok(&resp);

    // Add 1000 items
    for i in 0..1000 {
        client
            .command(&["BF.ADD", "bf_fp", &format!("item:{i}")])
            .await;
    }

    // Check 10000 non-added items for false positives
    let mut false_positives = 0;
    for i in 0..10000 {
        let resp = client
            .command(&["BF.EXISTS", "bf_fp", &format!("other:{i}")])
            .await;
        if unwrap_integer(&resp) == 1 {
            false_positives += 1;
        }
    }

    // Generous bound: 5% (configured at 1%, expansion may increase it)
    let fp_rate = false_positives as f64 / 10000.0;
    assert!(
        fp_rate < 0.05,
        "false positive rate {fp_rate:.4} exceeds 5% threshold"
    );
}

// ---------------------------------------------------------------------------
// Capacity and scaling tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bf_nonscaling_single_filter() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reserve with NONSCALING — should never create sub-filters
    let resp = client
        .command(&["BF.RESERVE", "bf_ns2", "0.1", "50", "NONSCALING"])
        .await;
    assert_ok(&resp);

    // Add items beyond initial capacity
    for i in 0..100 {
        client
            .command(&["BF.ADD", "bf_ns2", &format!("x:{i}")])
            .await;
    }

    // With NONSCALING, the number of filters should remain 1
    let resp = client.command(&["BF.INFO", "bf_ns2"]).await;
    let items = unwrap_array(resp);
    for i in (0..items.len()).step_by(2) {
        if let frogdb_protocol::Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == "Number of filters"
        {
            let num_filters = unwrap_integer(&items[i + 1]);
            assert_eq!(
                num_filters, 1,
                "NONSCALING should keep 1 filter, got {num_filters}"
            );
            return;
        }
    }
    panic!("'Number of filters' field not found in BF.INFO response");
}

#[tokio::test]
async fn bf_expansion_grows_capacity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Small initial capacity with default expansion
    let resp = client
        .command(&["BF.RESERVE", "bf_exp2", "0.01", "10", "EXPANSION", "2"])
        .await;
    assert_ok(&resp);

    // Add many items to force expansion
    for i in 0..100 {
        client
            .command(&["BF.ADD", "bf_exp2", &format!("item:{i}")])
            .await;
    }

    // BF.INFO should show multiple sub-filters
    let resp = client.command(&["BF.INFO", "bf_exp2"]).await;
    let items = unwrap_array(resp);
    // Find "Number of filters" field
    for i in (0..items.len()).step_by(2) {
        if let frogdb_protocol::Response::Bulk(Some(b)) = &items[i]
            && std::str::from_utf8(b).unwrap() == "Number of filters"
        {
            let num_filters = unwrap_integer(&items[i + 1]);
            assert!(
                num_filters > 1,
                "expected multiple sub-filters after expansion, got {num_filters}"
            );
            return;
        }
    }
    panic!("'Number of filters' field not found in BF.INFO response");
}

// ---------------------------------------------------------------------------
// Cuckoo filter edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cf_del_nonexistent_item() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CF.RESERVE", "cf_dne", "100"]).await;
    client.command(&["CF.ADD", "cf_dne", "exists"]).await;

    // Delete item that was never added
    let resp = client.command(&["CF.DEL", "cf_dne", "never_added"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
async fn cf_count_after_multiple_adds() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CF.RESERVE", "cf_cnt", "100"]).await;

    // Add same item 3 times
    client.command(&["CF.ADD", "cf_cnt", "hello"]).await;
    client.command(&["CF.ADD", "cf_cnt", "hello"]).await;
    client.command(&["CF.ADD", "cf_cnt", "hello"]).await;

    let resp = client.command(&["CF.COUNT", "cf_cnt", "hello"]).await;
    let count = unwrap_integer(&resp);
    assert_eq!(count, 3, "CF.COUNT should reflect 3 inserts");
}
