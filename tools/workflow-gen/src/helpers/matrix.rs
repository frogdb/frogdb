//! Matrix strategy helpers for GitHub Actions workflows.

use gh_workflow::Strategy;
use serde_json::{json, Value};

use crate::types::{targets_to_matrix_include, RustTarget};

/// Creates a matrix strategy with include entries.
pub fn matrix_with_includes(includes: Vec<Value>) -> Strategy {
    Strategy::default()
        .matrix(json!({
            "include": includes
        }))
        .fail_fast(false)
}

/// Creates a build matrix for Linux targets.
pub fn linux_build_matrix() -> Strategy {
    let targets: Vec<Value> = targets_to_matrix_include(&[
        RustTarget::X86_64Linux,
        RustTarget::Aarch64Linux,
    ])
    .into_iter()
    .map(|t| serde_json::to_value(t).unwrap())
    .collect();

    matrix_with_includes(targets)
}

/// Creates a build matrix for release targets (Linux + macOS).
pub fn release_build_matrix() -> Strategy {
    let targets: Vec<Value> = targets_to_matrix_include(&[
        RustTarget::X86_64Linux,
        RustTarget::Aarch64Linux,
        RustTarget::X86_64MacOs,
        RustTarget::Aarch64MacOs,
    ])
    .into_iter()
    .map(|t| serde_json::to_value(t).unwrap())
    .collect();

    matrix_with_includes(targets)
}
