//! Integration tests for FrogDB debug web UI endpoints.
//!
//! These tests verify that the debug HTTP server exposes the debug dashboard,
//! static assets, JSON APIs, and HTML partials correctly.

mod common;

use common::TestServer;
use reqwest::StatusCode;

// ============================================================================
// Static Asset Tests
// ============================================================================

#[tokio::test]
async fn test_debug_index_loads() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("FrogDB"), "Page should contain FrogDB title");
    assert!(body.contains("htmx"), "Page should reference HTMX");
    assert!(body.contains("Alpine") || body.contains("alpine"), "Page should reference Alpine.js");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_index_trailing_slash() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("FrogDB"), "Page should contain FrogDB title");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_htmx_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/js/htmx.min.js"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "HTMX asset should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("javascript"), "HTMX asset should be JavaScript");

    // Verify it's not empty
    let body = resp.text().await.unwrap();
    assert!(!body.is_empty(), "HTMX asset should not be empty");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_alpine_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/js/alpine.min.js"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "Alpine asset should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("javascript"), "Alpine asset should be JavaScript");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_uplot_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/js/uPlot.min.js"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "uPlot asset should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("javascript"), "uPlot asset should be JavaScript");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_css_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/css/style.css"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "style.css should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/css"), "style.css should be CSS");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_chota_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/css/chota.min.css"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "chota.min.css should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/css"), "chota.min.css should be CSS");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_uplot_css_asset() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/css/uPlot.min.css"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "uPlot.min.css should exist");
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/css"), "uPlot.min.css should be CSS");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_asset_not_found() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/assets/nonexistent.js"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "Nonexistent asset should return 404");

    server.shutdown().await;
}

// ============================================================================
// JSON API Tests
// ============================================================================

#[tokio::test]
async fn test_api_cluster() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/cluster"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("role").is_some(), "Should have role field");
    assert!(json.get("uptime_seconds").is_some(), "Should have uptime_seconds field");
    assert!(json.get("num_shards").is_some(), "Should have num_shards field");
    assert!(json.get("version").is_some(), "Should have version field");
    assert!(json.get("bind_addr").is_some(), "Should have bind_addr field");
    assert!(json.get("port").is_some(), "Should have port field");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_config() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/config"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("entries").is_some(), "Should have entries field");
    assert!(json["entries"].is_array(), "entries should be an array");

    // Verify at least one config entry exists
    let entries = json["entries"].as_array().unwrap();
    assert!(!entries.is_empty(), "Should have at least one config entry");

    // Verify config entry structure
    let first_entry = &entries[0];
    assert!(first_entry.get("name").is_some(), "Config entry should have name");
    assert!(first_entry.get("value").is_some(), "Config entry should have value");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_metrics() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("timestamp").is_some(), "Should have timestamp field");
    assert!(json.get("throughput").is_some(), "Should have throughput field");
    assert!(json.get("connections_current").is_some(), "Should have connections_current field");
    assert!(json.get("keys_total").is_some(), "Should have keys_total field");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_slowlog() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/slowlog"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("entries").is_some(), "Should have entries field");
    assert!(json["entries"].is_array(), "entries should be an array");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_latency() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/latency"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("data").is_some(), "Should have data field");
    assert!(json["data"].is_array(), "data should be an array");

    server.shutdown().await;
}

// ============================================================================
// HTML Partial Tests
// ============================================================================

#[tokio::test]
async fn test_partial_cluster() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/cluster"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("Uptime"), "Cluster partial should contain Uptime");
    assert!(body.contains("Shards"), "Cluster partial should contain Shards");

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_config() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/config"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("Configuration"), "Config partial should contain Configuration header");
    assert!(body.contains("<table"), "Config partial should contain a table");

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_metrics() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("Metrics") || body.contains("metrics"), "Metrics partial should contain metrics content");

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_slowlog() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/slowlog"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("Slow") || body.contains("slow"), "Slowlog partial should contain slowlog content");

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_latency() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/latency"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));

    let body = resp.text().await.unwrap();
    assert!(body.contains("Latency") || body.contains("latency"), "Latency partial should contain latency content");

    server.shutdown().await;
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_debug_invalid_path() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/invalid"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "Invalid path should return 404");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_invalid_api_path() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/api/invalid"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "Invalid API path should return 404");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_invalid_partial_path() {
    let server = TestServer::start().await;

    let resp = reqwest::get(server.metrics_url("/debug/partials/invalid"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "Invalid partial path should return 404");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_method_not_allowed() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let resp = client
        .post(server.metrics_url("/debug"))
        .send()
        .await
        .unwrap();

    // POST to debug should return 404 (only GET is handled for /debug paths)
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "POST to /debug should return 404");

    server.shutdown().await;
}

// ============================================================================
// Batch Asset Tests (for efficiency)
// ============================================================================

#[tokio::test]
async fn test_all_js_assets() {
    let server = TestServer::start().await;

    let assets = &[
        "js/htmx.min.js",
        "js/alpine.min.js",
        "js/uPlot.min.js",
    ];

    for asset in assets {
        let url = server.metrics_url(&format!("/debug/assets/{}", asset));
        let resp = reqwest::get(&url).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "Asset {} should exist", asset);
        assert!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("javascript"),
            "Asset {} should be JavaScript",
            asset
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_all_css_assets() {
    let server = TestServer::start().await;

    let assets = &[
        "css/style.css",
        "css/chota.min.css",
        "css/uPlot.min.css",
    ];

    for asset in assets {
        let url = server.metrics_url(&format!("/debug/assets/{}", asset));
        let resp = reqwest::get(&url).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "Asset {} should exist", asset);
        assert!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("text/css"),
            "Asset {} should be CSS",
            asset
        );
    }

    server.shutdown().await;
}
