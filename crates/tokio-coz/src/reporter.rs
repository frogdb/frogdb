use std::io::Write;

use serde::Serialize;

use crate::results::{SpanProfile, compute_impact};

/// JSON output format for the full profiling report.
#[derive(Debug, Serialize)]
pub struct ProfileReport {
    pub spans: Vec<SpanProfile>,
}

/// Write a JSON report to a file.
pub fn write_json_report(profiles: &[SpanProfile], path: &str) -> std::io::Result<()> {
    let report = ProfileReport {
        spans: profiles.to_vec(),
    };
    let json = serde_json::to_string_pretty(&report).map_err(std::io::Error::other)?;
    let mut file = std::fs::File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Print a terminal summary table of profiling results.
pub fn print_summary(profiles: &[SpanProfile]) {
    if profiles.is_empty() {
        println!("[tokio-coz] No profiling data collected.");
        return;
    }

    println!();
    println!("[tokio-coz] Causal Profiling Results");
    println!("{}", "=".repeat(72));

    for profile in profiles {
        let impacts = compute_impact(profile);

        println!();
        println!("  Span: {}", profile.span_name);
        println!("  {}", "-".repeat(60));

        if impacts.is_empty() {
            println!("    (no progress points recorded)");
            continue;
        }

        // Sort progress points by impact (descending).
        let mut sorted_impacts: Vec<_> = impacts.iter().collect();
        sorted_impacts.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));

        for (progress_name, impact) in sorted_impacts {
            let bar_len = (impact.abs() / 5.0).min(20.0) as usize;
            let bar = if *impact >= 0.0 {
                "+".repeat(bar_len)
            } else {
                "-".repeat(bar_len)
            };
            println!("    {:<30} {:>+7.1}%  [{}]", progress_name, impact, bar);
        }

        // Print the speedup curve for the highest-impact progress point.
        if let Some((best_name, _)) = impacts
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            && let Some(data_points) = profile.curves.get(best_name)
        {
            println!();
            println!("    Speedup curve for '{}':", best_name);
            for dp in data_points {
                println!(
                    "      {:>3}% speedup -> {:.1} ops/sec ({} samples)",
                    dp.speedup_pct, dp.avg_throughput_rate, dp.sample_count
                );
            }
        }
    }

    println!();
    println!("{}", "=".repeat(72));
}
