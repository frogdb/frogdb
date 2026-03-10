//! Integration tests for FrogDB debug web UI HTTP endpoints.
//!
//! These tests verify that the debug HTTP server exposes the debug dashboard,
//! static assets, JSON APIs, and HTML partials correctly.


use crate::common::test_server::TestServer;
use frogdb_protocol::Response;
use reqwest::StatusCode;

// ============================================================================
// Static Asset Tests
// ============================================================================

#[tokio::test]
async fn test_debug_index_loads() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(body.contains("FrogDB"), "Page should contain FrogDB title");
    assert!(body.contains("htmx"), "Page should reference HTMX");
    assert!(
        body.contains("Alpine") || body.contains("alpine"),
        "Page should reference Alpine.js"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_index_trailing_slash() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(body.contains("FrogDB"), "Page should contain FrogDB title");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_htmx_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/js/htmx.min.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "HTMX asset should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("javascript"),
        "HTMX asset should be JavaScript"
    );

    // Verify it's not empty
    let body = resp.text().await.unwrap();
    assert!(!body.is_empty(), "HTMX asset should not be empty");

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_alpine_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/js/alpine.min.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "Alpine asset should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("javascript"),
        "Alpine asset should be JavaScript"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_uplot_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/js/uPlot.min.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "uPlot asset should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("javascript"),
        "uPlot asset should be JavaScript"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_css_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/css/style.css"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "style.css should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/css"),
        "style.css should be CSS"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_chota_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/css/chota.min.css"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "chota.min.css should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/css"),
        "chota.min.css should be CSS"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_uplot_css_asset() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/css/uPlot.min.css"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "uPlot.min.css should exist");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/css"),
        "uPlot.min.css should be CSS"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_asset_not_found() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/assets/nonexistent.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "Nonexistent asset should return 404"
    );

    server.shutdown().await;
}

// ============================================================================
// JSON API Tests
// ============================================================================

#[tokio::test]
async fn test_api_cluster() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/cluster"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("role").is_some(), "Should have role field");
    assert!(
        json.get("uptime_seconds").is_some(),
        "Should have uptime_seconds field"
    );
    assert!(
        json.get("num_shards").is_some(),
        "Should have num_shards field"
    );
    assert!(json.get("version").is_some(), "Should have version field");
    assert!(
        json.get("bind_addr").is_some(),
        "Should have bind_addr field"
    );
    assert!(json.get("port").is_some(), "Should have port field");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_config() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("entries").is_some(), "Should have entries field");
    assert!(json["entries"].is_array(), "entries should be an array");

    // Verify at least one config entry exists
    let entries = json["entries"].as_array().unwrap();
    assert!(!entries.is_empty(), "Should have at least one config entry");

    // Verify config entry structure
    let first_entry = &entries[0];
    assert!(
        first_entry.get("name").is_some(),
        "Config entry should have name"
    );
    assert!(
        first_entry.get("value").is_some(),
        "Config entry should have value"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_metrics() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(
        json.get("timestamp").is_some(),
        "Should have timestamp field"
    );
    assert!(
        json.get("throughput").is_some(),
        "Should have throughput field"
    );
    assert!(
        json.get("connections_current").is_some(),
        "Should have connections_current field"
    );
    assert!(
        json.get("keys_total").is_some(),
        "Should have keys_total field"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_slowlog() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/slowlog"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("entries").is_some(), "Should have entries field");
    assert!(json["entries"].is_array(), "entries should be an array");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_latency() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/latency"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

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
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/cluster"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Uptime"),
        "Cluster partial should contain Uptime"
    );
    assert!(
        body.contains("Shards"),
        "Cluster partial should contain Shards"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_config() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Configuration"),
        "Config partial should contain Configuration header"
    );
    assert!(
        body.contains("<table"),
        "Config partial should contain a table"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_metrics() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Metrics") || body.contains("metrics"),
        "Metrics partial should contain metrics content"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_slowlog() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/slowlog"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Slow") || body.contains("slow"),
        "Slowlog partial should contain slowlog content"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_partial_latency() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/latency"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Latency") || body.contains("latency"),
        "Latency partial should contain latency content"
    );

    server.shutdown().await;
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_debug_invalid_path() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/invalid"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "Invalid path should return 404"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_invalid_api_path() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/invalid"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "Invalid API path should return 404"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_invalid_partial_path() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/partials/invalid"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "Invalid partial path should return 404"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_method_not_allowed() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .post(server.metrics_url("/debug"))
        .send()
        .await
        .unwrap();

    // POST to debug should return 404 (only GET is handled for /debug paths)
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "POST to /debug should return 404"
    );

    server.shutdown().await;
}

// ============================================================================
// Batch Asset Tests (for efficiency)
// ============================================================================

#[tokio::test]
async fn test_all_js_assets() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let assets = &["js/htmx.min.js", "js/alpine.min.js", "js/uPlot.min.js"];

    for asset in assets {
        let url = server.metrics_url(&format!("/debug/assets/{}", asset));
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Asset {} should exist",
            asset
        );
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
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let assets = &["css/style.css", "css/chota.min.css", "css/uPlot.min.css"];

    for asset in assets {
        let url = server.metrics_url(&format!("/debug/assets/{}", asset));
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Asset {} should exist",
            asset
        );
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

// ============================================================================
// Bundle API Tests
// ============================================================================
//
// Note: Bundle support requires explicit configuration. These tests handle
// both cases:
// - 503 SERVICE_UNAVAILABLE: Bundle support not enabled (acceptable)
// - 200 OK: Bundle support enabled, verify response

#[tokio::test]
async fn test_api_bundle_list() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/list"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/json")
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json.get("bundles").is_some(), "Should have bundles field");
    assert!(json["bundles"].is_array(), "bundles should be an array");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_list_empty() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/list"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(resp.status(), StatusCode::OK);

    let json: serde_json::Value = resp.json().await.unwrap();
    let bundles = json["bundles"].as_array().unwrap();
    // On a fresh server, bundles list should be empty or have very few entries
    assert!(bundles.len() < 100, "Should not have excessive bundles");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_generate_instant() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/generate"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    // Should return OK with ZIP content
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/zip")
    );

    // Verify ZIP data is valid
    let data = resp.bytes().await.unwrap();
    assert!(!data.is_empty(), "ZIP data should not be empty");
    // ZIP files start with PK (0x50 0x4B)
    assert_eq!(&data[0..2], b"PK", "Should be a valid ZIP file");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_generate_with_duration() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/generate?duration=1"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    // Should return OK with ZIP content
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("application/zip")
    );

    // Verify ZIP data is valid
    let data = resp.bytes().await.unwrap();
    assert!(!data.is_empty(), "ZIP data should not be empty");
    assert_eq!(&data[0..2], b"PK", "Should be a valid ZIP file");

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_generate_content_type() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/generate"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(resp.status(), StatusCode::OK);

    let content_type = resp
        .headers()
        .get("content-type")
        .expect("Should have Content-Type header")
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("application/zip"),
        "Content-Type should be application/zip, got: {}",
        content_type
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_generate_content_disposition() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/generate"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(resp.status(), StatusCode::OK);

    let disposition = resp
        .headers()
        .get("content-disposition")
        .expect("Should have Content-Disposition header")
        .to_str()
        .unwrap();
    assert!(
        disposition.contains("attachment"),
        "Content-Disposition should contain 'attachment', got: {}",
        disposition
    );
    assert!(
        disposition.contains("frogdb-bundle-"),
        "Content-Disposition should contain 'frogdb-bundle-', got: {}",
        disposition
    );
    assert!(
        disposition.contains(".zip"),
        "Content-Disposition should contain '.zip', got: {}",
        disposition
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_download_by_id() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    // Generate a new bundle via RESP to store it
    let mut resp_client = server.connect().await;
    let response = resp_client.command(&["DEBUG", "BUNDLE", "GENERATE"]).await;

    // Extract the bundle ID if generation succeeded
    if let Response::Bulk(Some(id_bytes)) = response {
        let bundle_id = String::from_utf8_lossy(&id_bytes);

        // Now download by ID
        let download_resp = client
            .get(server.metrics_url(&format!("/debug/api/bundle/{}", bundle_id)))
            .send()
            .await
            .unwrap();

        // Bundle support may not be enabled in HTTP layer - 503 is acceptable
        if download_resp.status() == StatusCode::SERVICE_UNAVAILABLE {
            server.shutdown().await;
            return;
        }

        assert_eq!(
            download_resp.status(),
            StatusCode::OK,
            "Should be able to download bundle by ID"
        );
        assert!(
            download_resp
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("application/zip")
        );

        let data = download_resp.bytes().await.unwrap();
        assert!(!data.is_empty(), "Downloaded ZIP should not be empty");
        assert_eq!(
            &data[0..2],
            b"PK",
            "Downloaded data should be a valid ZIP file"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_download_not_found() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/nonexistent-bundle-id"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "Nonexistent bundle should return 404"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_api_bundle_zip_structure() {
    let server = TestServer::start_standalone().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let resp = client
        .get(server.metrics_url("/debug/api/bundle/generate"))
        .send()
        .await
        .unwrap();

    // Bundle support may not be enabled - 503 is acceptable
    if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
        server.shutdown().await;
        return;
    }

    assert_eq!(resp.status(), StatusCode::OK);

    let data = resp.bytes().await.unwrap();

    // Parse ZIP and verify contents
    let cursor = std::io::Cursor::new(data.as_ref());
    let archive = zip::ZipArchive::new(cursor).expect("Should be valid ZIP");

    let file_names: Vec<_> = archive.file_names().collect();

    // Check for expected files (they may be in a subdirectory)
    let has_manifest = file_names.iter().any(|n| n.ends_with("manifest.json"));
    let has_config = file_names.iter().any(|n| n.ends_with("config.json"));
    let has_cluster = file_names.iter().any(|n| n.ends_with("cluster.json"));

    assert!(
        has_manifest,
        "ZIP should contain manifest.json, found files: {:?}",
        file_names
    );
    assert!(
        has_config,
        "ZIP should contain config.json, found files: {:?}",
        file_names
    );
    assert!(
        has_cluster,
        "ZIP should contain cluster.json, found files: {:?}",
        file_names
    );

    server.shutdown().await;
}
