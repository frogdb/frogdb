//! Test to verify all defined metrics are used in the codebase.
//!
//! This test ensures that:
//! 1. All metrics defined in `definitions.rs` are actually used somewhere
//! 2. No stale metric definitions exist that aren't being recorded
//!
//! Note: This test searches for metric NAME constants in the codebase,
//! not necessarily the typed metric struct usage. Both the old-style
//! `metric_names::*` constants and new typed metrics count as usage.

use frogdb_metrics::ALL_METRICS;
use std::process::Command;

/// Check that all defined metrics are referenced somewhere in the codebase.
///
/// This test uses grep to search for metric name strings in the crates/ directory,
/// excluding the definitions file itself.
#[test]
#[ignore = "requires grep and may be slow"]
fn all_metrics_are_used() {
    let mut unused = Vec::new();

    for metric in ALL_METRICS {
        // Search for the metric name in the crates directory
        let output = Command::new("grep")
            .args([
                "-r",
                "-l",
                "--include=*.rs",
                metric.name,
                "crates/",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR").to_owned() + "/../..")
            .output()
            .expect("Failed to execute grep");

        let files = String::from_utf8_lossy(&output.stdout);

        // Filter out the definitions file and metric_names module
        let usage_count = files
            .lines()
            .filter(|f| {
                !f.contains("definitions.rs")
                    && !f.contains("metrics/src/lib.rs")  // metric_names module
            })
            .count();

        if usage_count == 0 {
            unused.push(metric.name);
        }
    }

    if !unused.is_empty() {
        panic!(
            "The following {} metric(s) are defined but never used:\n  - {}",
            unused.len(),
            unused.join("\n  - ")
        );
    }
}

/// Verify the ALL_METRICS registry is populated correctly.
#[test]
fn metrics_registry_is_populated() {
    use frogdb_metrics::METRICS_COUNT;

    // We should have a reasonable number of metrics defined
    assert!(METRICS_COUNT > 40, "Expected at least 40 metrics, found {}", METRICS_COUNT);
    assert_eq!(ALL_METRICS.len(), METRICS_COUNT);

    // Each metric should have a non-empty name and valid type
    for metric in ALL_METRICS {
        assert!(!metric.name.is_empty(), "Metric has empty name");
        assert!(
            metric.name.starts_with("frogdb_"),
            "Metric '{}' doesn't start with 'frogdb_' prefix",
            metric.name
        );
    }
}

/// Verify metric names are unique.
#[test]
fn metric_names_are_unique() {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();

    for metric in ALL_METRICS {
        if !seen.insert(metric.name) {
            duplicates.push(metric.name);
        }
    }

    if !duplicates.is_empty() {
        panic!(
            "Duplicate metric names found:\n  - {}",
            duplicates.join("\n  - ")
        );
    }
}

/// Verify all metrics have help text.
#[test]
fn all_metrics_have_help_text() {
    let mut missing_help = Vec::new();

    for metric in ALL_METRICS {
        if metric.help.is_empty() {
            missing_help.push(metric.name);
        }
    }

    if !missing_help.is_empty() {
        panic!(
            "The following metrics are missing help text:\n  - {}",
            missing_help.join("\n  - ")
        );
    }
}

/// Print all metrics for documentation purposes.
#[test]
fn list_all_metrics() {
    println!("\n=== FrogDB Metrics ({} total) ===\n", ALL_METRICS.len());

    let mut by_category: std::collections::BTreeMap<&str, Vec<_>> = std::collections::BTreeMap::new();
    for metric in ALL_METRICS {
        by_category
            .entry(metric.category())
            .or_default()
            .push(metric);
    }

    for (category, metrics) in &by_category {
        println!("## {}", category.to_uppercase());
        for metric in metrics {
            println!(
                "  {} ({}) - {}{}",
                metric.name,
                metric.metric_type,
                metric.help,
                if metric.labels.is_empty() {
                    String::new()
                } else {
                    format!(" [labels: {}]", metric.labels.join(", "))
                }
            );
        }
        println!();
    }
}
