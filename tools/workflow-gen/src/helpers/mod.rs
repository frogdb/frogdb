//! Helper functions for building workflow components.

pub(crate) mod actions;
mod matrix;
mod steps;

#[allow(unused_imports)]
pub use actions::*;
pub use matrix::*;
pub use steps::*;

/// Default GitHub Actions runner for jobs.
///
/// Change to `"ubuntu-latest"` to use GitHub-hosted runners.
pub const RUNNER: &str = "self-hosted";
