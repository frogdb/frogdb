//! Regression tests for Count-Min Sketch (CMS.*) and Top-K (TOPK.*) commands.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Count-Min Sketch tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cms_initbydim_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.INITBYDIM", "cms1", "1000", "5"])
        .await;
    assert_ok(&resp);

    // Verify INFO returns correct dimensions.
    let resp = client.command(&["CMS.INFO", "cms1"]).await;
    let items = unwrap_array(resp);
    // Expected: ["width", 1000, "depth", 5, "count", 0]
    assert_eq!(items.len(), 6);
    assert_bulk_eq(&items[0], b"width");
    assert_eq!(unwrap_integer(&items[1]), 1000);
    assert_bulk_eq(&items[2], b"depth");
    assert_eq!(unwrap_integer(&items[3]), 5);
    assert_bulk_eq(&items[4], b"count");
    assert_eq!(unwrap_integer(&items[5]), 0);
}

#[tokio::test]
async fn cms_initbyprob_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.INITBYPROB", "cms1", "0.001", "0.01"])
        .await;
    assert_ok(&resp);

    // Verify INFO reports non-zero width and depth.
    let resp = client.command(&["CMS.INFO", "cms1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 6);
    assert_bulk_eq(&items[0], b"width");
    let width = unwrap_integer(&items[1]);
    assert!(width > 0, "width should be positive, got {width}");
    assert_bulk_eq(&items[2], b"depth");
    let depth = unwrap_integer(&items[3]);
    assert!(depth > 0, "depth should be positive, got {depth}");
    assert_bulk_eq(&items[4], b"count");
    assert_eq!(unwrap_integer(&items[5]), 0);
}

#[tokio::test]
async fn cms_incrby_single() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "cms1", "1000", "5"])
        .await;

    // Increment "apple" by 3.
    let resp = client
        .command(&["CMS.INCRBY", "cms1", "apple", "3"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    let count = unwrap_integer(&items[0]);
    assert!(count >= 3, "CMS count should be >= 3, got {count}");

    // QUERY should also return >= 3.
    let resp = client.command(&["CMS.QUERY", "cms1", "apple"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    let count = unwrap_integer(&items[0]);
    assert!(count >= 3, "CMS query should be >= 3, got {count}");
}

#[tokio::test]
async fn cms_incrby_multiple() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "cms1", "2000", "5"])
        .await;

    // Increment multiple items at once.
    let resp = client
        .command(&["CMS.INCRBY", "cms1", "a", "10", "b", "20", "c", "30"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);

    // Each returned count should be >= the true increment.
    assert!(unwrap_integer(&items[0]) >= 10);
    assert!(unwrap_integer(&items[1]) >= 20);
    assert!(unwrap_integer(&items[2]) >= 30);
}

#[tokio::test]
async fn cms_query_overestimates() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "cms1", "2000", "10"])
        .await;

    // Add known counts.
    client
        .command(&["CMS.INCRBY", "cms1", "x", "100"])
        .await;
    client
        .command(&["CMS.INCRBY", "cms1", "y", "50"])
        .await;

    let resp = client.command(&["CMS.QUERY", "cms1", "x", "y"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);

    // CMS may overestimate but never underestimate.
    let x_count = unwrap_integer(&items[0]);
    let y_count = unwrap_integer(&items[1]);
    assert!(
        x_count >= 100,
        "x count should be >= 100, got {x_count}"
    );
    assert!(
        y_count >= 50,
        "y count should be >= 50, got {y_count}"
    );
}

#[tokio::test]
async fn cms_query_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Querying a key that does not exist should return 0s.
    let resp = client
        .command(&["CMS.QUERY", "nokey", "a", "b"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert_eq!(unwrap_integer(&items[1]), 0);
}

#[tokio::test]
async fn cms_merge_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create two sketches with the same dimensions (hash tags for same slot).
    client
        .command(&["CMS.INITBYDIM", "{m}src1", "1000", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{m}src2", "1000", "5"])
        .await;

    client
        .command(&["CMS.INCRBY", "{m}src1", "a", "10"])
        .await;
    client
        .command(&["CMS.INCRBY", "{m}src2", "b", "20"])
        .await;

    // Merge into a new key.
    client
        .command(&["CMS.INITBYDIM", "{m}dest", "1000", "5"])
        .await;
    let resp = client
        .command(&["CMS.MERGE", "{m}dest", "2", "{m}src1", "{m}src2"])
        .await;
    assert_ok(&resp);

    // Verify merged counts.
    let resp = client.command(&["CMS.QUERY", "{m}dest", "a", "b"]).await;
    let items = unwrap_array(resp);
    assert!(unwrap_integer(&items[0]) >= 10);
    assert!(unwrap_integer(&items[1]) >= 20);
}

#[tokio::test]
async fn cms_merge_with_weights() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "{w}src1", "1000", "5"])
        .await;
    client
        .command(&["CMS.INITBYDIM", "{w}src2", "1000", "5"])
        .await;

    client
        .command(&["CMS.INCRBY", "{w}src1", "item", "10"])
        .await;
    client
        .command(&["CMS.INCRBY", "{w}src2", "item", "10"])
        .await;

    client
        .command(&["CMS.INITBYDIM", "{w}dest", "1000", "5"])
        .await;
    let resp = client
        .command(&[
            "CMS.MERGE", "{w}dest", "2", "{w}src1", "{w}src2", "WEIGHTS", "2", "3",
        ])
        .await;
    assert_ok(&resp);

    // With weights 2 and 3: expected count >= 2*10 + 3*10 = 50.
    let resp = client.command(&["CMS.QUERY", "{w}dest", "item"]).await;
    let items = unwrap_array(resp);
    let count = unwrap_integer(&items[0]);
    assert!(
        count >= 50,
        "merged weighted count should be >= 50, got {count}"
    );
}

#[tokio::test]
async fn cms_info_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "cms1", "500", "7"])
        .await;
    client
        .command(&["CMS.INCRBY", "cms1", "foo", "42"])
        .await;

    let resp = client.command(&["CMS.INFO", "cms1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 6);

    assert_bulk_eq(&items[0], b"width");
    assert_eq!(unwrap_integer(&items[1]), 500);
    assert_bulk_eq(&items[2], b"depth");
    assert_eq!(unwrap_integer(&items[3]), 7);
    assert_bulk_eq(&items[4], b"count");
    // count should reflect the total increments (>= 42 due to possible overcount).
    let count = unwrap_integer(&items[5]);
    assert!(count >= 42, "total count should be >= 42, got {count}");
}

#[tokio::test]
async fn cms_incrby_requires_pairs() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["CMS.INITBYDIM", "cms1", "1000", "5"])
        .await;

    // Odd number of item/increment arguments should error.
    let resp = client
        .command(&["CMS.INCRBY", "cms1", "item_only"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Count-Min Sketch error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cms_initbydim_zero_width() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["CMS.INITBYDIM", "cms1", "0", "5"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Top-K tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn topk_reserve_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.RESERVE", "tk1", "3"]).await;
    assert_ok(&resp);

    // Verify INFO.
    let resp = client.command(&["TOPK.INFO", "tk1"]).await;
    let items = unwrap_array(resp);
    // Expected: ["k", 3, "width", ..., "depth", ..., "decay", ...]
    assert_eq!(items.len(), 8);
    assert_bulk_eq(&items[0], b"k");
    assert_eq!(unwrap_integer(&items[1]), 3);
}

#[tokio::test]
async fn topk_reserve_with_params() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["TOPK.RESERVE", "tk1", "5", "100", "10", "0.8"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["TOPK.INFO", "tk1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 8);
    assert_bulk_eq(&items[0], b"k");
    assert_eq!(unwrap_integer(&items[1]), 5);
    assert_bulk_eq(&items[2], b"width");
    assert_eq!(unwrap_integer(&items[3]), 100);
    assert_bulk_eq(&items[4], b"depth");
    assert_eq!(unwrap_integer(&items[5]), 10);
    assert_bulk_eq(&items[6], b"decay");
    // Decay is returned as a bulk string float.
    assert_bulk_eq(&items[7], b"0.8");
}

#[tokio::test]
async fn topk_add_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    // Add items; each returns either Null (no expulsion) or Bulk (expelled item).
    let resp = client
        .command(&["TOPK.ADD", "tk1", "a", "b", "c"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);
    for item in &items {
        // Each element must be either Null or a Bulk string.
        assert!(
            matches!(item, frogdb_protocol::Response::Bulk(_)),
            "expected Bulk or Null, got {item:?}"
        );
    }
}

#[tokio::test]
async fn topk_add_tracks_frequent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    // Add "hot" many times so it becomes a top item.
    for _ in 0..50 {
        client.command(&["TOPK.ADD", "tk1", "hot"]).await;
    }
    // Add some noise.
    for i in 0..10 {
        client
            .command(&["TOPK.ADD", "tk1", &format!("noise{i}")])
            .await;
    }

    // QUERY should report "hot" as a top-k item.
    let resp = client.command(&["TOPK.QUERY", "tk1", "hot"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_eq!(unwrap_integer(&items[0]), 1);
}

#[tokio::test]
async fn topk_incrby_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    let resp = client
        .command(&["TOPK.INCRBY", "tk1", "a", "100", "b", "200"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    // Each element is either Null (no expulsion) or Bulk (expelled item name).
    for item in &items {
        assert!(
            matches!(item, frogdb_protocol::Response::Bulk(_)),
            "expected Bulk or Null, got {item:?}"
        );
    }
}

#[tokio::test]
async fn topk_query_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    // Add items with varying frequencies.
    client
        .command(&["TOPK.INCRBY", "tk1", "high", "1000", "med", "500", "low", "100"])
        .await;

    // Query known top items.
    let resp = client
        .command(&["TOPK.QUERY", "tk1", "high", "med"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(unwrap_integer(&items[0]), 1);
    assert_eq!(unwrap_integer(&items[1]), 1);

    // Query an item that was never added.
    let resp = client
        .command(&["TOPK.QUERY", "tk1", "nonexistent"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_eq!(unwrap_integer(&items[0]), 0);
}

#[tokio::test]
async fn topk_count_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    client
        .command(&["TOPK.INCRBY", "tk1", "item1", "50", "item2", "100"])
        .await;

    let resp = client
        .command(&["TOPK.COUNT", "tk1", "item1", "item2", "missing"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 3);

    // Counts are approximate but should be reasonable.
    let c1 = unwrap_integer(&items[0]);
    let c2 = unwrap_integer(&items[1]);
    let c3 = unwrap_integer(&items[2]);
    assert!(c1 >= 1, "item1 count should be positive, got {c1}");
    assert!(c2 >= 1, "item2 count should be positive, got {c2}");
    assert_eq!(c3, 0, "missing item count should be 0");
}

#[tokio::test]
async fn topk_list_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    client
        .command(&["TOPK.INCRBY", "tk1", "a", "100", "b", "200", "c", "300"])
        .await;

    let resp = client.command(&["TOPK.LIST", "tk1"]).await;
    let items = unwrap_array(resp);
    // Should have at most k items.
    assert!(
        items.len() <= 3,
        "LIST should return at most k items, got {}",
        items.len()
    );
    // All returned items should be bulk strings.
    for item in &items {
        assert!(
            matches!(item, frogdb_protocol::Response::Bulk(Some(_))),
            "expected Bulk(Some(_)), got {item:?}"
        );
    }
}

#[tokio::test]
async fn topk_list_withcount() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk1", "3"]).await;

    client
        .command(&["TOPK.INCRBY", "tk1", "x", "100", "y", "200", "z", "300"])
        .await;

    let resp = client
        .command(&["TOPK.LIST", "tk1", "WITHCOUNT"])
        .await;
    let items = unwrap_array(resp);
    // WITHCOUNT doubles the entries: alternating item, count pairs.
    assert!(
        items.len() % 2 == 0,
        "WITHCOUNT should return even number of elements, got {}",
        items.len()
    );
    assert!(
        items.len() <= 6,
        "WITHCOUNT should return at most 2*k elements, got {}",
        items.len()
    );

    // Verify alternating pattern: bulk string, then integer.
    let mut i = 0;
    while i < items.len() {
        assert!(
            matches!(&items[i], frogdb_protocol::Response::Bulk(Some(_))),
            "even index {i} should be Bulk(Some(_)), got {:?}",
            items[i]
        );
        let count = unwrap_integer(&items[i + 1]);
        assert!(count > 0, "count at index {} should be positive", i + 1);
        i += 2;
    }
}

#[tokio::test]
async fn topk_info_fields() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["TOPK.RESERVE", "tk1", "10", "50", "4", "0.95"])
        .await;

    let resp = client.command(&["TOPK.INFO", "tk1"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 8);

    assert_bulk_eq(&items[0], b"k");
    assert_eq!(unwrap_integer(&items[1]), 10);
    assert_bulk_eq(&items[2], b"width");
    assert_eq!(unwrap_integer(&items[3]), 50);
    assert_bulk_eq(&items[4], b"depth");
    assert_eq!(unwrap_integer(&items[5]), 4);
    assert_bulk_eq(&items[6], b"decay");
    assert_bulk_eq(&items[7], b"0.95");
}

// ---------------------------------------------------------------------------
// Top-K error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn topk_reserve_zero_k() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TOPK.RESERVE", "tk1", "0"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn topk_query_nonexistent_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Querying a key that does not exist should return 0s.
    let resp = client
        .command(&["TOPK.QUERY", "nokey", "a", "b"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert_eq!(unwrap_integer(&items[1]), 0);
}

// ---------------------------------------------------------------------------
// CMS merge edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cms_merge_into_self() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["CMS.INITBYDIM", "cms_self", "10", "5"]).await;
    client
        .command(&["CMS.INCRBY", "cms_self", "a", "10"])
        .await;

    // Self-merge: reads source data first, then overwrites dest.
    // With weight=1, the result should preserve the same counts.
    let resp = client
        .command(&["CMS.MERGE", "cms_self", "1", "cms_self"])
        .await;
    assert_ok(&resp);

    let resp = client.command(&["CMS.QUERY", "cms_self", "a"]).await;
    let items = unwrap_array(resp);
    let count = unwrap_integer(&items[0]);
    assert!(
        count >= 10,
        "self-merge should preserve counts: expected >= 10, got {count}"
    );
}

#[tokio::test]
async fn cms_merge_nonexistent_source() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Use hash tags so both keys land on the same slot
    client
        .command(&["CMS.INITBYDIM", "{cms}dst", "10", "5"])
        .await;

    let resp = client
        .command(&["CMS.MERGE", "{cms}dst", "1", "{cms}nope"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// TopK eviction and accuracy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn topk_eviction_returns_expelled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Small top-k (k=2)
    client.command(&["TOPK.RESERVE", "tk_ev", "2"]).await;

    // Add initial items
    client
        .command(&["TOPK.INCRBY", "tk_ev", "a", "100", "b", "90"])
        .await;

    // Add a new heavy hitter — should eventually evict one of the weaker items
    let resp = client
        .command(&["TOPK.ADD", "tk_ev", "c", "c", "c", "c", "c"])
        .await;
    // TOPK.ADD returns expelled items or nil for each input
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 5); // 5 adds of "c"
}

#[tokio::test]
async fn topk_heavy_hitter_accuracy() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["TOPK.RESERVE", "tk_hh", "3"]).await;

    // Add "heavy" 100 times
    for _ in 0..10 {
        client
            .command(&[
                "TOPK.INCRBY", "tk_hh", "heavy", "10",
            ])
            .await;
    }
    // Add "light_X" items 1 time each
    for i in 0..20 {
        client
            .command(&["TOPK.ADD", "tk_hh", &format!("light_{i}")])
            .await;
    }

    // "heavy" should be in the top-k list
    let resp = client.command(&["TOPK.QUERY", "tk_hh", "heavy"]).await;
    let items = unwrap_array(resp);
    assert_eq!(
        unwrap_integer(&items[0]),
        1,
        "heavy hitter should be in top-k"
    );
}
