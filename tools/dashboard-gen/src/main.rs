//! Grafana dashboard generator for FrogDB.
//!
//! Generates a Grafana dashboard JSON from the typed metrics registry.
//! The dashboard is organized into collapsible rows by metric category.

use anyhow::{Context, Result};
use clap::Parser;
use frogdb_metrics::{MetricDefinition, MetricType, ALL_METRICS};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "dashboard-gen",
    about = "Generate Grafana dashboard from FrogDB metrics"
)]
struct Args {
    /// Output file for the dashboard JSON
    #[arg(short, long, default_value = "deploy/dashboards/frogdb-overview.json")]
    output: PathBuf,

    /// Check mode - verify generated dashboard matches existing
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let dashboard = generate_dashboard()?;
    let json = serde_json::to_string_pretty(&dashboard)?;

    if args.check {
        let existing = fs::read_to_string(&args.output)
            .with_context(|| format!("Failed to read {}", args.output.display()))?;
        if existing != json {
            anyhow::bail!(
                "Dashboard differs from generated. Run 'just dashboard-gen' to regenerate."
            );
        }
        println!("Dashboard is up to date.");
    } else {
        fs::write(&args.output, &json)?;
        println!("Generated: {}", args.output.display());
    }

    Ok(())
}

/// Generate the complete Grafana dashboard.
fn generate_dashboard() -> Result<Value> {
    // Group metrics by category
    let mut categories: BTreeMap<String, Vec<&MetricDefinition>> = BTreeMap::new();
    for metric in ALL_METRICS {
        let category = metric.category().to_string();
        categories.entry(category).or_default().push(metric);
    }

    let mut panels: Vec<Value> = Vec::new();
    let mut panel_id = 1;
    let mut grid_y = 0;

    // Add overview stats row at the top
    panels.push(create_stat_panel(
        panel_id,
        "Uptime",
        "frogdb_uptime_seconds",
        0,
        grid_y,
        6,
        3,
        "s",
    ));
    panel_id += 1;

    panels.push(create_stat_panel(
        panel_id,
        "Keys",
        "frogdb_keys_total",
        6,
        grid_y,
        6,
        3,
        "short",
    ));
    panel_id += 1;

    panels.push(create_stat_panel(
        panel_id,
        "Connections",
        "frogdb_connections_current",
        12,
        grid_y,
        6,
        3,
        "short",
    ));
    panel_id += 1;

    panels.push(create_stat_panel(
        panel_id,
        "Memory",
        "frogdb_memory_used_bytes",
        18,
        grid_y,
        6,
        3,
        "bytes",
    ));
    panel_id += 1;

    grid_y += 3;

    // Create panels for each category
    for (category, metrics) in &categories {
        // Add a row panel for the category
        panels.push(json!({
            "id": panel_id,
            "type": "row",
            "title": capitalize_category(category),
            "collapsed": category != "commands",  // Commands row expanded by default
            "gridPos": { "x": 0, "y": grid_y, "w": 24, "h": 1 },
            "panels": []
        }));
        panel_id += 1;
        grid_y += 1;

        // Create panels for each metric in the category
        let mut col = 0;
        for metric in metrics {
            let (panel, width, height) = create_metric_panel(metric, panel_id, col, grid_y);
            panels.push(panel);
            panel_id += 1;
            col += width;
            if col >= 24 {
                col = 0;
                grid_y += height;
            }
        }
        if col > 0 {
            grid_y += 8; // Move to next row after partial row
        }
    }

    Ok(json!({
        "__inputs": [],
        "__requires": [
            {
                "type": "grafana",
                "id": "grafana",
                "name": "Grafana",
                "version": "9.0.0"
            },
            {
                "type": "panel",
                "id": "timeseries",
                "name": "Time series",
                "version": ""
            },
            {
                "type": "panel",
                "id": "stat",
                "name": "Stat",
                "version": ""
            },
            {
                "type": "datasource",
                "id": "prometheus",
                "name": "Prometheus",
                "version": "1.0.0"
            }
        ],
        "annotations": {
            "list": []
        },
        "description": "FrogDB server metrics dashboard - auto-generated from metric definitions",
        "editable": true,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": null,
        "links": [],
        "liveNow": false,
        "panels": panels,
        "refresh": "30s",
        "schemaVersion": 38,
        "tags": ["frogdb", "database", "redis"],
        "templating": {
            "list": [
                {
                    "current": {
                        "selected": false,
                        "text": "Prometheus",
                        "value": "prometheus"
                    },
                    "hide": 0,
                    "includeAll": false,
                    "label": "Data Source",
                    "multi": false,
                    "name": "datasource",
                    "options": [],
                    "query": "prometheus",
                    "refresh": 1,
                    "regex": "",
                    "skipUrlSync": false,
                    "type": "datasource"
                },
                {
                    "allValue": ".*",
                    "current": {
                        "selected": true,
                        "text": "All",
                        "value": "$__all"
                    },
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${datasource}"
                    },
                    "definition": "label_values(frogdb_uptime_seconds, instance)",
                    "hide": 0,
                    "includeAll": true,
                    "label": "Instance",
                    "multi": true,
                    "name": "instance",
                    "options": [],
                    "query": {
                        "query": "label_values(frogdb_uptime_seconds, instance)",
                        "refId": "StandardVariableQuery"
                    },
                    "refresh": 2,
                    "regex": "",
                    "skipUrlSync": false,
                    "sort": 1,
                    "type": "query"
                }
            ]
        },
        "time": {
            "from": "now-1h",
            "to": "now"
        },
        "timepicker": {},
        "timezone": "",
        "title": "FrogDB Overview",
        "uid": "frogdb-overview",
        "version": 1,
        "weekStart": ""
    }))
}

/// Create a panel for a metric based on its type.
fn create_metric_panel(metric: &MetricDefinition, id: i32, x: i32, y: i32) -> (Value, i32, i32) {
    let width = 12;
    let height = 8;

    let panel = match metric.metric_type {
        MetricType::Counter => create_counter_panel(metric, id, x, y, width, height),
        MetricType::Gauge => create_gauge_panel(metric, id, x, y, width, height),
        MetricType::Histogram => create_histogram_panel(metric, id, x, y, width, height),
    };

    (panel, width, height)
}

/// Create a time series panel showing rate() for a counter.
fn create_counter_panel(
    metric: &MetricDefinition,
    id: i32,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
) -> Value {
    let query = if metric.labels.is_empty() {
        format!("rate({}{{instance=~\"$instance\"}}[5m])", metric.name)
    } else {
        format!(
            "sum by ({}) (rate({}{{instance=~\"$instance\"}}[5m]))",
            metric.labels.join(", "),
            metric.name
        )
    };

    json!({
        "id": id,
        "type": "timeseries",
        "title": format_title(metric.name),
        "description": metric.help,
        "gridPos": { "x": x, "y": y, "w": width, "h": height },
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "fieldConfig": {
            "defaults": {
                "color": { "mode": "palette-classic" },
                "custom": {
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "drawStyle": "line",
                    "fillOpacity": 10,
                    "gradientMode": "none",
                    "hideFrom": { "legend": false, "tooltip": false, "viz": false },
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": { "type": "linear" },
                    "showPoints": "never",
                    "spanNulls": false,
                    "stacking": { "group": "A", "mode": "none" },
                    "thresholdsStyle": { "mode": "off" }
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{ "color": "green", "value": null }]
                },
                "unit": "short"
            },
            "overrides": []
        },
        "options": {
            "legend": { "calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
        },
        "targets": [{
            "datasource": { "type": "prometheus", "uid": "${datasource}" },
            "editorMode": "code",
            "expr": query,
            "legendFormat": "{{__name__}}",
            "range": true,
            "refId": "A"
        }]
    })
}

/// Create a time series panel for a gauge.
fn create_gauge_panel(
    metric: &MetricDefinition,
    id: i32,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
) -> Value {
    let query = if metric.labels.is_empty() {
        format!("{}{{instance=~\"$instance\"}}", metric.name)
    } else {
        format!(
            "sum by ({}) ({}{{instance=~\"$instance\"}})",
            metric.labels.join(", "),
            metric.name
        )
    };

    let unit = if metric.name.contains("bytes") {
        "bytes"
    } else if metric.name.contains("seconds") {
        "s"
    } else {
        "short"
    };

    json!({
        "id": id,
        "type": "timeseries",
        "title": format_title(metric.name),
        "description": metric.help,
        "gridPos": { "x": x, "y": y, "w": width, "h": height },
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "fieldConfig": {
            "defaults": {
                "color": { "mode": "palette-classic" },
                "custom": {
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "drawStyle": "line",
                    "fillOpacity": 10,
                    "gradientMode": "none",
                    "hideFrom": { "legend": false, "tooltip": false, "viz": false },
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": { "type": "linear" },
                    "showPoints": "never",
                    "spanNulls": false,
                    "stacking": { "group": "A", "mode": "none" },
                    "thresholdsStyle": { "mode": "off" }
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{ "color": "green", "value": null }]
                },
                "unit": unit
            },
            "overrides": []
        },
        "options": {
            "legend": { "calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
        },
        "targets": [{
            "datasource": { "type": "prometheus", "uid": "${datasource}" },
            "editorMode": "code",
            "expr": query,
            "legendFormat": "{{__name__}}",
            "range": true,
            "refId": "A"
        }]
    })
}

/// Create panels for a histogram (p50, p95, p99 percentiles).
fn create_histogram_panel(
    metric: &MetricDefinition,
    id: i32,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
) -> Value {
    // Use the full metric name for histogram bucket queries
    let bucket_name = format!("{}_bucket", metric.name.trim_end_matches("_seconds"));

    json!({
        "id": id,
        "type": "timeseries",
        "title": format_title(metric.name),
        "description": metric.help,
        "gridPos": { "x": x, "y": y, "w": width, "h": height },
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "fieldConfig": {
            "defaults": {
                "color": { "mode": "palette-classic" },
                "custom": {
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "drawStyle": "line",
                    "fillOpacity": 0,
                    "gradientMode": "none",
                    "hideFrom": { "legend": false, "tooltip": false, "viz": false },
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": { "type": "linear" },
                    "showPoints": "never",
                    "spanNulls": false,
                    "stacking": { "group": "A", "mode": "none" },
                    "thresholdsStyle": { "mode": "off" }
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{ "color": "green", "value": null }]
                },
                "unit": "s"
            },
            "overrides": []
        },
        "options": {
            "legend": { "calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": true },
            "tooltip": { "mode": "multi", "sort": "desc" }
        },
        "targets": [
            {
                "datasource": { "type": "prometheus", "uid": "${datasource}" },
                "editorMode": "code",
                "expr": format!("histogram_quantile(0.50, sum(rate({}[5m])) by (le))", bucket_name),
                "legendFormat": "p50",
                "range": true,
                "refId": "A"
            },
            {
                "datasource": { "type": "prometheus", "uid": "${datasource}" },
                "editorMode": "code",
                "expr": format!("histogram_quantile(0.95, sum(rate({}[5m])) by (le))", bucket_name),
                "legendFormat": "p95",
                "range": true,
                "refId": "B"
            },
            {
                "datasource": { "type": "prometheus", "uid": "${datasource}" },
                "editorMode": "code",
                "expr": format!("histogram_quantile(0.99, sum(rate({}[5m])) by (le))", bucket_name),
                "legendFormat": "p99",
                "range": true,
                "refId": "C"
            }
        ]
    })
}

/// Create a stat panel for overview metrics.
fn create_stat_panel(
    id: i32,
    title: &str,
    metric_name: &str,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
    unit: &str,
) -> Value {
    json!({
        "id": id,
        "type": "stat",
        "title": title,
        "gridPos": { "x": x, "y": y, "w": width, "h": height },
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "fieldConfig": {
            "defaults": {
                "color": { "mode": "thresholds" },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        { "color": "green", "value": null }
                    ]
                },
                "unit": unit
            },
            "overrides": []
        },
        "options": {
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "orientation": "auto",
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": false
            },
            "textMode": "auto"
        },
        "targets": [{
            "datasource": { "type": "prometheus", "uid": "${datasource}" },
            "editorMode": "code",
            "expr": format!("{}{{instance=~\"$instance\"}}", metric_name),
            "legendFormat": "{{__name__}}",
            "range": true,
            "refId": "A"
        }]
    })
}

/// Format a metric name into a readable title.
fn format_title(name: &str) -> String {
    // Remove frogdb_ prefix and _total/_seconds suffixes
    let name = name
        .strip_prefix("frogdb_")
        .unwrap_or(name)
        .strip_suffix("_total")
        .unwrap_or(name)
        .strip_suffix("_seconds")
        .unwrap_or(name);

    // Convert snake_case to Title Case
    name.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Capitalize a category name.
fn capitalize_category(category: &str) -> String {
    let mut chars = category.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().chain(chars).collect(),
    }
}
