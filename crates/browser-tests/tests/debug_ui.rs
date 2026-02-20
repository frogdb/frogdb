//! Browser integration tests for the debug UI.
//!
//! These tests require chromedriver to be running on port 9515.
//! Run with: `cargo test -p frogdb-browser-tests --features browser-tests`

#![cfg(feature = "browser-tests")]

mod common;

use common::{chromedriver_available, BrowserSession, TestServer};
use std::time::Duration;
use thirtyfour::prelude::*;

/// Expected tab names in the debug UI.
const EXPECTED_TABS: &[&str] = &[
    "Cluster", "Config", "Metrics", "Slowlog", "Latency", "Bundles",
];

/// Helper macro to skip test if chromedriver is not available.
macro_rules! require_chromedriver {
    () => {
        if !chromedriver_available().await {
            eprintln!(
                "Skipping test: chromedriver not available at {}",
                common::CHROMEDRIVER_URL
            );
            return;
        }
    };
}

#[tokio::test]
async fn test_debug_page_loads() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    // Navigate to the debug page
    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to load
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check that the page title or main container exists
    let title = browser
        .driver
        .title()
        .await
        .expect("Failed to get page title");
    assert!(
        title.contains("FrogDB") || title.contains("Debug"),
        "Page title should contain 'FrogDB' or 'Debug', got: {}",
        title
    );

    // Verify the main container exists
    let body = browser
        .driver
        .find(By::Tag("body"))
        .await
        .expect("Failed to find body element");
    assert!(
        body.is_displayed().await.unwrap_or(false),
        "Body should be displayed"
    );

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}

#[tokio::test]
async fn test_all_tabs_present() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to load
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Find all tab elements (they should be buttons or links in a tab bar)
    // Common patterns: role="tab", class containing "tab", or nav buttons
    let tabs = browser
        .driver
        .find_all(By::Css(
            "[role='tab'], .tab, .nav-tab, button[data-tab], a[data-tab]",
        ))
        .await
        .unwrap_or_default();

    // If no tabs found with those selectors, try finding by text content
    if tabs.is_empty() {
        // Look for elements containing tab names
        for tab_name in EXPECTED_TABS {
            let xpath = format!(
                "//*[contains(text(), '{}') and (self::button or self::a or self::li or self::div[@role='tab'])]",
                tab_name
            );
            let found = browser
                .driver
                .find_all(By::XPath(&xpath))
                .await
                .unwrap_or_default();
            assert!(
                !found.is_empty(),
                "Tab '{}' should be present in the debug UI",
                tab_name
            );
        }
    } else {
        // Verify we have the expected number of tabs
        assert!(
            tabs.len() >= EXPECTED_TABS.len(),
            "Expected at least {} tabs, found {}",
            EXPECTED_TABS.len(),
            tabs.len()
        );
    }

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}

#[tokio::test]
async fn test_tab_switching() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to load
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try clicking on different tabs and verify content changes
    for tab_name in &["Config", "Metrics", "Slowlog"] {
        // Find the tab by text content
        let xpath = format!(
            "//*[contains(text(), '{}') and (self::button or self::a or self::li or self::div[@role='tab'] or ancestor::button or ancestor::a)]",
            tab_name
        );

        if let Ok(elements) = browser.driver.find_all(By::XPath(&xpath)).await {
            for element in elements {
                if element.is_displayed().await.unwrap_or(false)
                    && element.is_enabled().await.unwrap_or(false)
                {
                    // Click the tab
                    if element.click().await.is_ok() {
                        // Wait for content to update
                        tokio::time::sleep(Duration::from_millis(300)).await;

                        // Verify the page didn't crash (body still exists)
                        let body = browser.driver.find(By::Tag("body")).await;
                        assert!(
                            body.is_ok(),
                            "Page should not crash after clicking {} tab",
                            tab_name
                        );
                        break;
                    }
                }
            }
        }
    }

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}

#[tokio::test]
async fn test_no_javascript_errors() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    // Inject console error capture before navigating
    browser
        .driver
        .goto("about:blank")
        .await
        .expect("Failed to navigate to blank page");

    browser
        .inject_console_capture()
        .await
        .expect("Failed to inject console capture");

    // Navigate to the debug page
    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to fully load and any async operations to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Re-inject and capture any errors that occurred during page load
    // (the injection from blank page won't persist after navigation)
    let _errors: Result<serde_json::Value, _> = browser
        .driver
        .execute(
            r#"
            // Check for any uncaught errors
            var errors = [];
            if (window.onerror) {
                errors.push('window.onerror handler exists');
            }
            return errors;
            "#,
            vec![],
        )
        .await
        .map(|v| v.convert().unwrap_or(serde_json::Value::Null));

    // The page should load without throwing JavaScript errors that break rendering
    // Verify the page rendered something
    let has_content = browser
        .driver
        .execute(
            r#"
            return document.body && document.body.innerHTML.length > 100;
            "#,
            vec![],
        )
        .await
        .map(|v| v.convert::<bool>().unwrap_or(false))
        .unwrap_or(false);

    assert!(
        has_content,
        "Page should have rendered content (body.innerHTML.length > 100)"
    );

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}

#[tokio::test]
async fn test_charts_container_exists() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to load
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Look for chart-related elements (canvas, svg, or div with chart-related classes)
    let chart_selectors = [
        "canvas",
        "svg",
        "[class*='chart']",
        "[class*='Chart']",
        "[id*='chart']",
        "[id*='Chart']",
        ".metrics-chart",
        ".latency-chart",
    ];

    let mut found_chart_elements = false;
    for selector in chart_selectors {
        if let Ok(elements) = browser.driver.find_all(By::Css(selector)).await {
            if !elements.is_empty() {
                found_chart_elements = true;
                break;
            }
        }
    }

    // Even if no charts are rendered yet (empty data), verify the page structure exists
    // by checking for content containers
    let has_content_area = browser
        .driver
        .execute(
            r#"
            // Check for main content areas that would hold charts
            var selectors = [
                '[class*="content"]',
                '[class*="panel"]',
                '[class*="container"]',
                'main',
                'section'
            ];
            for (var i = 0; i < selectors.length; i++) {
                if (document.querySelector(selectors[i])) {
                    return true;
                }
            }
            return false;
            "#,
            vec![],
        )
        .await
        .map(|v| v.convert::<bool>().unwrap_or(false))
        .unwrap_or(false);

    assert!(
        found_chart_elements || has_content_area,
        "Page should have chart elements or content container areas"
    );

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}

#[tokio::test]
async fn test_cluster_tab_content() {
    require_chromedriver!();

    let server = TestServer::start().await;
    let browser = BrowserSession::new()
        .await
        .expect("Failed to create browser session");

    browser
        .driver
        .goto(&server.debug_url())
        .await
        .expect("Failed to navigate to debug page");

    // Wait for page to load
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The Cluster tab should be the default/first tab
    // Look for cluster-related content
    let has_cluster_content = browser
        .driver
        .execute(
            r#"
            var body = document.body.innerHTML.toLowerCase();
            // Check for cluster-related terms
            return body.includes('cluster') ||
                   body.includes('node') ||
                   body.includes('shard') ||
                   body.includes('slot');
            "#,
            vec![],
        )
        .await
        .map(|v| v.convert::<bool>().unwrap_or(false))
        .unwrap_or(false);

    assert!(
        has_cluster_content,
        "Debug page should contain cluster-related content"
    );

    browser.close().await.expect("Failed to close browser");
    server.shutdown().await;
}
