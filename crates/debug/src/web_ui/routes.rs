//! Route dispatcher for the debug web UI.
//!
//! This module handles routing requests to the appropriate handlers.

use super::handlers;
use super::state::DebugState;
use bytes::Bytes;
use frogdb_telemetry::PrometheusRecorder;
use http_body_util::Full;
use hyper::{Response, StatusCode, Uri};
use rust_embed::RustEmbed;
use std::sync::Arc;

/// Embedded assets from the assets directory.
#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

/// Handle a request to the debug endpoints.
///
/// Routes:
/// - GET /debug -> index.html
/// - GET /debug/ -> index.html
/// - GET /debug/assets/* -> static assets
/// - GET /debug/api/* -> JSON API
/// - GET /debug/api/bundle/list -> list bundles
/// - GET /debug/api/bundle/generate -> generate and download bundle
/// - GET /debug/api/bundle/:id -> download stored bundle
/// - GET /debug/partials/* -> HTML partials
pub async fn handle_debug_request(
    uri: &Uri,
    state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let full_path = uri.path();
    let query = uri.query();

    // Strip /debug prefix
    let path = full_path.strip_prefix("/debug").unwrap_or(full_path);
    let path = if path.is_empty() { "/" } else { path };

    match path {
        // Main page
        "/" | "/index.html" => serve_index(),

        // Static assets
        p if p.starts_with("/assets/") => {
            let asset_path = p.strip_prefix("/assets/").unwrap_or("");
            serve_asset(asset_path)
        }

        // Bundle API endpoints
        "/api/bundle/list" => handlers::handle_api_bundle_list(state),
        "/api/bundle/generate" => handlers::handle_api_bundle_generate(state, query).await,

        // Bundle download by ID (pattern match)
        p if p.starts_with("/api/bundle/") => {
            let id = p.strip_prefix("/api/bundle/").unwrap_or("");
            if !id.is_empty() && id != "list" && id != "generate" {
                handlers::handle_api_bundle_download(state, id)
            } else {
                not_found()
            }
        }

        // JSON API endpoints
        "/api/cluster" => handlers::handle_api_cluster(state),
        "/api/config" => handlers::handle_api_config(state),
        "/api/metrics" => handlers::handle_api_metrics(state, recorder),
        "/api/slowlog" => handlers::handle_api_slowlog(state).await,
        "/api/latency" => handlers::handle_api_latency(state).await,

        // HTML partials
        "/partials/cluster" => handlers::handle_partial_cluster(state),
        "/partials/config" => handlers::handle_partial_config(state),
        "/partials/metrics" => handlers::handle_partial_metrics(state, recorder),
        "/partials/slowlog" => handlers::handle_partial_slowlog(state).await,
        "/partials/latency" => handlers::handle_partial_latency(state).await,
        "/partials/bundles" => handlers::handle_partial_bundles(state),

        // Not found
        _ => not_found(),
    }
}

/// Serve the main index.html page.
fn serve_index() -> Response<Full<Bytes>> {
    match Assets::get("index.html") {
        Some(content) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/html; charset=utf-8")
            .body(Full::new(Bytes::from(content.data.into_owned())))
            .unwrap(),
        None => not_found(),
    }
}

/// Serve a static asset.
fn serve_asset(path: &str) -> Response<Full<Bytes>> {
    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime)
                .header("Cache-Control", "public, max-age=3600")
                .body(Full::new(Bytes::from(content.data.into_owned())))
                .unwrap()
        }
        None => not_found(),
    }
}

/// Return a 404 not found response.
fn not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serve_index() {
        let response = serve_index();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("text/html")
        );
    }

    #[test]
    fn test_serve_css_asset() {
        let response = serve_asset("css/style.css");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("text/css")
        );
    }

    #[test]
    fn test_serve_js_asset() {
        let response = serve_asset("js/htmx.min.js");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("javascript")
        );
    }

    #[test]
    fn test_not_found_asset() {
        let response = serve_asset("nonexistent.js");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
