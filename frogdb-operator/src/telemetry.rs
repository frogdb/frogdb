//! Operator telemetry setup.

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Initialize tracing for the operator.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json())
        .init();
}
