//! Verifies the typed-metric registry against the codebase.
//!
//! Together with `just lint-metrics-chokepoint` (which rejects raw
//! `increment_counter`/`record_gauge`/`record_histogram` calls outside the
//! backend seam), these tests close the registration/emission drift loop:
//!
//! 1. Every metric in `ALL_METRICS` is emitted somewhere through its typed
//!    handle (`Handle::inc/inc_by/set/observe`) — no dead definitions.
//! 2. The registry itself is well-formed (unique names, help text, prefix).
//!
//! Metrics whose only emitters live in files currently owned by other agents
//! (still on raw string emission) are listed in `RAW_EMISSION_EXEMPT` with
//! the file that emits them; the test then requires the raw name literal in
//! that file, so the exemption self-expires when the file is migrated.

use frogdb_telemetry::{ALL_METRICS, MetricType};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Metrics emitted only from files excluded from the proposal-35 migration
/// (other agents were editing them). Each entry pins the raw emission site;
/// when that site migrates to the typed handle, the literal disappears from
/// the file and this test fails, prompting removal of the exemption.
const RAW_EMISSION_EXEMPT: &[(&str, &str)] = &[
    ("frogdb_scatter_gather_total", "vll/src/coordinator.rs"),
    (
        "frogdb_scatter_gather_duration_seconds",
        "vll/src/coordinator.rs",
    ),
    ("frogdb_scatter_gather_shards", "vll/src/coordinator.rs"),
];

/// Root of the crates/ directory containing all FrogDB source.
fn crates_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("telemetry crate lives in crates/")
        .to_path_buf()
}

/// Recursively collect the contents of every .rs file under `dir`, keyed by
/// path relative to the crates root.
fn collect_rust_sources(dir: &Path, root: &Path, out: &mut HashMap<String, String>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if name == "target" || name == ".git" {
                continue;
            }
            collect_rust_sources(&path, root, out);
        } else if name.ends_with(".rs")
            && let Ok(contents) = std::fs::read_to_string(&path)
        {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            out.insert(rel, contents);
        }
    }
}

/// Files that may mention handles/names without that counting as emission:
/// the registry itself and the macro that generates it.
fn is_registry_source(rel_path: &str) -> bool {
    rel_path.starts_with("types/src/metrics/") || rel_path.starts_with("metrics-derive/")
}

/// Every defined metric must be emitted through its typed handle somewhere
/// in the codebase (or carry a pinned raw-site exemption).
#[test]
fn every_metric_is_emitted_through_its_typed_handle() {
    let root = crates_root();
    let mut sources = HashMap::new();
    collect_rust_sources(&root, &root, &mut sources);
    assert!(
        sources.len() > 100,
        "source walk looks broken: only {} files under {}",
        sources.len(),
        root.display()
    );

    let exempt: HashMap<&str, &str> = RAW_EMISSION_EXEMPT.iter().copied().collect();
    let mut unused = Vec::new();
    let mut stale_exemptions = Vec::new();

    for metric in ALL_METRICS {
        if let Some(raw_file) = exempt.get(metric.name) {
            let still_raw = sources
                .get(*raw_file)
                .is_some_and(|contents| contents.contains(metric.name));
            if !still_raw {
                stale_exemptions.push((metric.name, *raw_file));
            }
            continue;
        }

        // The emission methods generated for this metric's type.
        let patterns: &[&str] = match metric.metric_type {
            MetricType::Counter => &["::inc(", "::inc_by("],
            MetricType::Gauge => &["::set("],
            MetricType::Histogram => &["::observe("],
        };
        let needles: Vec<String> = patterns
            .iter()
            .map(|p| format!("{}{}", metric.handle, p))
            .collect();

        let emitted = sources.iter().any(|(rel, contents)| {
            !is_registry_source(rel) && needles.iter().any(|n| contents.contains(n.as_str()))
        });

        if !emitted {
            unused.push(metric);
        }
    }

    if !stale_exemptions.is_empty() {
        panic!(
            "Stale RAW_EMISSION_EXEMPT entries (the pinned file no longer emits the raw name;\n\
             the site was migrated — remove the exemption):\n  - {}",
            stale_exemptions
                .iter()
                .map(|(m, f)| format!("{m} (was {f})"))
                .collect::<Vec<_>>()
                .join("\n  - ")
        );
    }

    if !unused.is_empty() {
        panic!(
            "The following {} metric(s) are defined but never emitted via their typed handle\n\
             (delete the definition, or emit it — do not leave registry entries that lie):\n  - {}",
            unused.len(),
            unused
                .iter()
                .map(|m| format!("{} (handle {})", m.name, m.handle))
                .collect::<Vec<_>>()
                .join("\n  - ")
        );
    }
}

/// Verify the ALL_METRICS registry is populated correctly.
#[test]
fn metrics_registry_is_populated() {
    use frogdb_telemetry::METRICS_COUNT;

    // We should have a reasonable number of metrics defined
    #[allow(clippy::assertions_on_constants)]
    {
        assert!(
            METRICS_COUNT > 40,
            "Expected at least 40 metrics, found {}",
            METRICS_COUNT
        );
    }
    assert_eq!(ALL_METRICS.len(), METRICS_COUNT);

    // Each metric should have a non-empty name, a handle, and the frogdb_ prefix
    for metric in ALL_METRICS {
        assert!(!metric.name.is_empty(), "Metric has empty name");
        assert!(
            metric.name.starts_with("frogdb_"),
            "Metric '{}' doesn't start with 'frogdb_' prefix",
            metric.name
        );
        assert!(
            !metric.handle.is_empty(),
            "Metric '{}' has no handle name",
            metric.name
        );
    }
}

/// Verify metric names and handles are unique.
#[test]
fn metric_names_are_unique() {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut seen_handles = HashSet::new();
    let mut duplicates = Vec::new();

    for metric in ALL_METRICS {
        if !seen.insert(metric.name) {
            duplicates.push(metric.name);
        }
        if !seen_handles.insert(metric.handle) {
            duplicates.push(metric.handle);
        }
    }

    if !duplicates.is_empty() {
        panic!(
            "Duplicate metric names/handles found:\n  - {}",
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

    let mut by_category: std::collections::BTreeMap<&str, Vec<_>> =
        std::collections::BTreeMap::new();
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
