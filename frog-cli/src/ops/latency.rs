use anyhow::{Context, Result};
use serde::Serialize;

use crate::util::{extract_int, extract_string};

/// A single data point from LATENCY HISTORY.
#[derive(Debug, Clone, Serialize)]
pub struct LatencyHistoryPoint {
    pub timestamp: i64,
    pub latency_ms: u64,
}

/// Per-command latency histogram bucket.
#[derive(Debug, Clone, Serialize)]
pub struct CommandHistogramEntry {
    pub command: String,
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistogramBucket {
    pub usec: u64,
    pub count: u64,
}

/// Fetch LATENCY DOCTOR output (returns formatted text from the server).
pub async fn latency_doctor(conn: &mut redis::aio::MultiplexedConnection) -> Result<String> {
    let result: String = redis::cmd("LATENCY")
        .arg("DOCTOR")
        .query_async(conn)
        .await
        .context("LATENCY DOCTOR failed")?;
    Ok(result)
}

/// Fetch LATENCY HISTORY for a given event name.
pub async fn latency_history(
    conn: &mut redis::aio::MultiplexedConnection,
    event: &str,
) -> Result<Vec<LatencyHistoryPoint>> {
    let value: redis::Value = redis::cmd("LATENCY")
        .arg("HISTORY")
        .arg(event)
        .query_async(conn)
        .await
        .context("LATENCY HISTORY failed")?;

    let mut points = Vec::new();
    if let redis::Value::Array(items) = value {
        for item in items {
            if let redis::Value::Array(pair) = item
                && pair.len() >= 2
            {
                let timestamp = extract_int(&pair[0]);
                let latency_ms = extract_int(&pair[1]) as u64;
                points.push(LatencyHistoryPoint {
                    timestamp,
                    latency_ms,
                });
            }
        }
    }

    Ok(points)
}

/// Fetch LATENCY HISTOGRAM for specified commands (or all if empty).
pub async fn latency_histogram(
    conn: &mut redis::aio::MultiplexedConnection,
    commands: &[String],
) -> Result<Vec<CommandHistogramEntry>> {
    let mut cmd = redis::cmd("LATENCY");
    cmd.arg("HISTOGRAM");
    for c in commands {
        cmd.arg(c);
    }

    let value: redis::Value = cmd
        .query_async(conn)
        .await
        .context("LATENCY HISTOGRAM failed")?;

    parse_histogram_response(&value)
}

/// Parse the LATENCY HISTOGRAM response.
///
/// The response is an alternating array: [command_name, histogram_data, ...]
/// where histogram_data is a map with "calls" and "histogram_usec" keys.
fn parse_histogram_response(value: &redis::Value) -> Result<Vec<CommandHistogramEntry>> {
    let mut entries = Vec::new();

    if let redis::Value::Array(items) = value {
        let mut i = 0;
        while i + 1 < items.len() {
            let command = extract_string(&items[i]);
            i += 1;

            if let redis::Value::Array(ref map_items) = items[i] {
                let buckets = parse_histogram_map(map_items);
                entries.push(CommandHistogramEntry { command, buckets });
            }
            i += 1;
        }
    }

    Ok(entries)
}

fn parse_histogram_map(items: &[redis::Value]) -> Vec<HistogramBucket> {
    let mut buckets = Vec::new();

    // Look for "histogram_usec" key in the alternating key-value map
    let mut i = 0;
    while i + 1 < items.len() {
        let key = extract_string(&items[i]);
        if key == "histogram_usec"
            && let redis::Value::Array(ref bucket_pairs) = items[i + 1]
        {
            let mut j = 0;
            while j + 1 < bucket_pairs.len() {
                let usec = extract_int(&bucket_pairs[j]) as u64;
                let count = extract_int(&bucket_pairs[j + 1]) as u64;
                buckets.push(HistogramBucket { usec, count });
                j += 2;
            }
        }
        i += 2;
    }

    buckets
}

/// Render an ASCII latency graph from history points.
pub fn render_ascii_graph(event: &str, points: &[LatencyHistoryPoint], width: usize) -> String {
    if points.is_empty() {
        return format!("{event}: no data\n");
    }

    let max_latency = points.iter().map(|p| p.latency_ms).max().unwrap_or(1);
    let height = 10;

    let mut out = format!(
        "{event} latency (last {} events, max {max_latency}ms):\n",
        points.len()
    );

    // Take at most `width` points, sampling evenly if needed
    let display_points: Vec<&LatencyHistoryPoint> = if points.len() <= width {
        points.iter().collect()
    } else {
        let step = points.len() as f64 / width as f64;
        (0..width)
            .map(|i| &points[(i as f64 * step) as usize])
            .collect()
    };

    // Render top-down
    for row in (0..height).rev() {
        let _threshold = (max_latency as f64 * (row as f64 + 1.0) / height as f64) as u64;
        if row == height - 1 {
            out.push_str(&format!("{max_latency:>6}ms |"));
        } else if row == 0 {
            out.push_str(&format!("{:>6}ms |", 0));
        } else {
            out.push_str("        |");
        }

        for point in &display_points {
            let bar_threshold = (max_latency as f64 * row as f64 / height as f64) as u64;
            if point.latency_ms > bar_threshold {
                out.push('#');
            } else {
                out.push(' ');
            }
        }
        out.push('\n');
    }

    out.push_str("        +");
    out.push_str(&"-".repeat(display_points.len()));
    out.push('\n');

    out
}

/// Render histogram entries as a table string.
pub fn render_histogram_table(entries: &[CommandHistogramEntry]) -> String {
    if entries.is_empty() {
        return "No latency histogram data.\n".to_string();
    }

    let mut out = String::new();
    for entry in entries {
        out.push_str(&format!("Command: {}\n", entry.command));
        out.push_str(&format!("{:<12} {}\n", "USEC", "COUNT"));
        for bucket in &entry.buckets {
            out.push_str(&format!("{:<12} {}\n", bucket.usec, bucket.count));
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_ascii_graph_empty() {
        let result = render_ascii_graph("test", &[], 40);
        assert!(result.contains("no data"));
    }

    #[test]
    fn test_render_ascii_graph() {
        let points = vec![
            LatencyHistoryPoint {
                timestamp: 1,
                latency_ms: 10,
            },
            LatencyHistoryPoint {
                timestamp: 2,
                latency_ms: 50,
            },
            LatencyHistoryPoint {
                timestamp: 3,
                latency_ms: 30,
            },
        ];
        let result = render_ascii_graph("command", &points, 40);
        assert!(result.contains("command latency"));
        assert!(result.contains("50ms"));
    }

    #[test]
    fn test_render_histogram_table_empty() {
        let result = render_histogram_table(&[]);
        assert!(result.contains("No latency"));
    }

    #[test]
    fn test_render_histogram_table() {
        let entries = vec![CommandHistogramEntry {
            command: "GET".into(),
            buckets: vec![
                HistogramBucket {
                    usec: 1,
                    count: 100,
                },
                HistogramBucket {
                    usec: 10,
                    count: 50,
                },
            ],
        }];
        let result = render_histogram_table(&entries);
        assert!(result.contains("GET"));
        assert!(result.contains("100"));
    }
}
