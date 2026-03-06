//! Test utilities for browser integration tests.

use frogdb_server::{Config, Server};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use thirtyfour::prelude::*;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Default chromedriver URL.
pub const CHROMEDRIVER_URL: &str = "http://localhost:9515";

/// Helper struct for managing a test server.
pub struct TestServer {
    #[allow(dead_code)]
    pub addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestServer {
    /// Start a new test server on an available port.
    pub async fn start() -> Self {
        let temp_dir = TempDir::new().unwrap();

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = 0;
        config.server.num_shards = 4;
        config.logging.level = "warn".to_string();
        config.persistence.data_dir = temp_dir.path().to_path_buf();
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = 0;

        // Construct server before spawning to read actual bound addresses (no TOCTOU)
        let server = Server::new(
            config,
            frogdb_server::runtime_config::LogReloadHandle::noop(),
        )
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let metrics_addr = server.metrics_addr().unwrap().unwrap();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let _ = server
                .run_until(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(200)).await;

        TestServer {
            addr,
            metrics_addr,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
            temp_dir,
        }
    }

    /// Get the debug page URL.
    pub fn debug_url(&self) -> String {
        format!("http://{}/debug", self.metrics_addr)
    }

    /// Shutdown the test server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Send shutdown signal if not already sent
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Helper struct for managing a WebDriver session.
pub struct BrowserSession {
    pub driver: WebDriver,
}

impl BrowserSession {
    /// Create a new browser session with headless Chrome.
    pub async fn new() -> WebDriverResult<Self> {
        let mut caps = DesiredCapabilities::chrome();
        // Run headless for CI environments
        caps.add_arg("--headless")?;
        caps.add_arg("--no-sandbox")?;
        caps.add_arg("--disable-dev-shm-usage")?;
        caps.add_arg("--disable-gpu")?;
        caps.add_arg("--window-size=1920,1080")?;

        let driver = WebDriver::new(CHROMEDRIVER_URL, caps).await?;
        Ok(BrowserSession { driver })
    }

    /// Close the browser session.
    pub async fn close(self) -> WebDriverResult<()> {
        self.driver.quit().await
    }

    /// Get the browser's console logs (JavaScript errors).
    #[allow(dead_code)]
    pub async fn get_console_errors(&self) -> WebDriverResult<Vec<String>> {
        // Execute JavaScript to capture console errors
        let errors: serde_json::Value = self
            .driver
            .execute(
                r#"
                return window._consoleErrors || [];
                "#,
                vec![],
            )
            .await?
            .convert()?;

        let error_strings = errors
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(error_strings)
    }

    /// Inject console error capture script.
    pub async fn inject_console_capture(&self) -> WebDriverResult<()> {
        self.driver
            .execute(
                r#"
                window._consoleErrors = [];
                const originalError = console.error;
                console.error = function(...args) {
                    window._consoleErrors.push(args.map(a => String(a)).join(' '));
                    originalError.apply(console, args);
                };
                "#,
                vec![],
            )
            .await?;
        Ok(())
    }
}

/// Check if chromedriver is running.
pub async fn chromedriver_available() -> bool {
    match reqwest::get(format!("{}/status", CHROMEDRIVER_URL)).await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}
