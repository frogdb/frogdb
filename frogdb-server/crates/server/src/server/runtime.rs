//! Server runtime: `run_until()` body (thin orchestrator for subsystem lifecycle).

use tracing::info;

use super::Server;

use anyhow::Result;

impl Server {
    /// Run the server until the provided future completes.
    ///
    /// Use this for testing where OS signals aren't available (e.g., Turmoil simulation).
    pub async fn run_until<F>(mut self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        // Startup checks
        self.check_split_brain_logs();
        self.run_startup_latency_test();

        // Start all subsystems
        let handles = self.start_subsystems()?;

        info!(addr = %self.config.bind_addr(), "FrogDB server ready");

        // Wait for shutdown signal
        shutdown.await;

        info!("Shutdown signal received, stopping server...");

        // Clean shutdown
        self.shutdown_subsystems(handles).await;

        info!("Server shutdown complete");
        Ok(())
    }
}
