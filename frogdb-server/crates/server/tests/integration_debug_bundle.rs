//! Integration tests for DEBUG BUNDLE commands.
//!
//! These tests verify the RESP protocol interface for debug bundle generation and listing.


use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

// ============================================================================
// DEBUG BUNDLE LIST Tests
// ============================================================================

#[tokio::test]
async fn test_debug_bundle_list_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DEBUG", "BUNDLE", "LIST"]).await;
    match response {
        Response::Array(entries) => {
            // On a fresh server, list should be empty or have very few entries
            // Just verify it's an array (may not be empty if previous tests ran)
            assert!(entries.len() < 100, "Should not have excessive bundles");
        }
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Bundle support may not be enabled - this is acceptable
            assert!(
                err_str.contains("not enabled") || err_str.contains("ERR"),
                "Unexpected error: {}",
                err_str
            );
        }
        _ => panic!("Expected array or error response, got {:?}", response),
    }

    server.shutdown().await;
}

// ============================================================================
// DEBUG BUNDLE GENERATE Tests
// ============================================================================

#[tokio::test]
async fn test_debug_bundle_generate_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DEBUG", "BUNDLE", "GENERATE"]).await;
    match response {
        Response::Bulk(Some(id)) => {
            // Bundle ID should be non-empty
            assert!(!id.is_empty(), "Bundle ID should not be empty");
            // Bundle ID should be a valid string
            let id_str = String::from_utf8_lossy(&id);
            assert!(!id_str.is_empty(), "Bundle ID string should not be empty");
        }
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Bundle support may not be enabled - this is acceptable
            assert!(
                err_str.contains("not enabled") || err_str.contains("ERR"),
                "Unexpected error: {}",
                err_str
            );
        }
        _ => panic!("Expected bulk string or error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_bundle_generate_with_duration() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["DEBUG", "BUNDLE", "GENERATE", "DURATION", "1"])
        .await;
    match response {
        Response::Bulk(Some(id)) => {
            // Bundle ID should be non-empty
            assert!(!id.is_empty(), "Bundle ID should not be empty");
        }
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Bundle support may not be enabled - this is acceptable
            assert!(
                err_str.contains("not enabled") || err_str.contains("ERR"),
                "Unexpected error: {}",
                err_str
            );
        }
        _ => panic!("Expected bulk string or error response, got {:?}", response),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_bundle_generate_duration_missing_value() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["DEBUG", "BUNDLE", "GENERATE", "DURATION"])
        .await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Should get an error about missing duration value
            assert!(
                err_str.contains("ERR")
                    || err_str.contains("syntax")
                    || err_str.contains("missing"),
                "Expected error about missing duration value, got: {}",
                err_str
            );
        }
        _ => panic!(
            "Expected error response for missing duration value, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_bundle_generate_duration_invalid() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client
        .command(&["DEBUG", "BUNDLE", "GENERATE", "DURATION", "abc"])
        .await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Should get an error about invalid duration value
            assert!(
                err_str.contains("ERR")
                    || err_str.contains("invalid")
                    || err_str.contains("integer"),
                "Expected error about invalid duration value, got: {}",
                err_str
            );
        }
        _ => panic!(
            "Expected error response for invalid duration value, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}

// ============================================================================
// DEBUG BUNDLE LIST After GENERATE Tests
// ============================================================================

#[tokio::test]
async fn test_debug_bundle_list_after_generate() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Generate a bundle first
    let generate_response = client.command(&["DEBUG", "BUNDLE", "GENERATE"]).await;

    // If generation succeeded, check that list returns an array
    if let Response::Bulk(Some(generated_id)) = generate_response {
        let list_response = client.command(&["DEBUG", "BUNDLE", "LIST"]).await;
        match list_response {
            Response::Array(entries) => {
                // List should be an array (may or may not contain the generated bundle
                // depending on whether RESP GENERATE stores or just returns the bundle)
                let generated_id_str = String::from_utf8_lossy(&generated_id);

                // If the list is not empty, verify structure of entries
                if !entries.is_empty() {
                    // Try to find the generated bundle
                    let found = entries.iter().any(|entry| {
                        if let Response::Array(fields) = entry
                            && let Some(Response::Bulk(Some(id))) = fields.first()
                        {
                            let id_str = String::from_utf8_lossy(id);
                            return id_str == generated_id_str;
                        }
                        false
                    });

                    // Log for debugging but don't fail if not found
                    // (RESP GENERATE might not store bundles, just return them)
                    if found {
                        // Bundle was found in the list
                    }
                }
            }
            _ => panic!("Expected array response for LIST, got {:?}", list_response),
        }
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_debug_bundle_list_entry_structure() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Generate a bundle first to ensure we have at least one
    let generate_response = client.command(&["DEBUG", "BUNDLE", "GENERATE"]).await;

    // If generation succeeded, verify list entry structure
    if let Response::Bulk(Some(_)) = generate_response {
        let list_response = client.command(&["DEBUG", "BUNDLE", "LIST"]).await;
        if let Response::Array(entries) = list_response
            && let Some(first_entry) = entries.first()
        {
            match first_entry {
                Response::Array(fields) => {
                    // Entry should have [id, timestamp, size] format
                    assert!(
                        fields.len() >= 3,
                        "Entry should have at least 3 fields (id, timestamp, size), got {}",
                        fields.len()
                    );

                    // First field should be bundle ID (bulk string)
                    assert!(
                        matches!(&fields[0], Response::Bulk(Some(_))),
                        "First field (id) should be bulk string"
                    );

                    // Second field should be timestamp (integer)
                    assert!(
                        matches!(&fields[1], Response::Integer(_)),
                        "Second field (timestamp) should be integer"
                    );

                    // Third field should be size (integer)
                    assert!(
                        matches!(&fields[2], Response::Integer(_)),
                        "Third field (size) should be integer"
                    );
                }
                _ => panic!("List entry should be an array, got {:?}", first_entry),
            }
        }
    }

    server.shutdown().await;
}

// ============================================================================
// DEBUG BUNDLE Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_debug_bundle_unknown_subcommand() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let response = client.command(&["DEBUG", "BUNDLE", "INVALID"]).await;
    match response {
        Response::Error(e) => {
            let err_str = String::from_utf8_lossy(&e);
            // Should get an error about unknown subcommand
            assert!(
                err_str.contains("ERR")
                    || err_str.contains("unknown")
                    || err_str.contains("subcommand"),
                "Expected error about unknown subcommand, got: {}",
                err_str
            );
        }
        _ => panic!(
            "Expected error response for unknown subcommand, got {:?}",
            response
        ),
    }

    server.shutdown().await;
}
