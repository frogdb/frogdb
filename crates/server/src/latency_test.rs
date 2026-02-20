//! Intrinsic latency testing for FrogDB.
//!
//! This module measures the system's inherent scheduling latency - the "performance floor"
//! that FrogDB can never exceed. High intrinsic latency indicates OS kernel, hypervisor,
//! or system contention issues.
//!
//! The test runs a tight loop measuring the time between iterations. Any delay beyond
//! the minimum loop time indicates that the process was preempted by the scheduler.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Global storage for the latency baseline result and configuration.
static LATENCY_BASELINE: OnceLock<LatencyBaselineInfo> = OnceLock::new();

/// Stored latency baseline information including result and threshold.
#[derive(Debug, Clone)]
pub struct LatencyBaselineInfo {
    /// The test results.
    pub result: LatencyTestResult,
    /// The warning threshold in microseconds.
    pub warning_threshold_us: u64,
}

/// Store the latency baseline result globally for INFO command access.
pub fn set_global_baseline(result: LatencyTestResult, warning_threshold_us: u64) {
    let _ = LATENCY_BASELINE.set(LatencyBaselineInfo {
        result,
        warning_threshold_us,
    });
}

/// Get the globally stored latency baseline, if available.
pub fn get_global_baseline() -> Option<&'static LatencyBaselineInfo> {
    LATENCY_BASELINE.get()
}

/// Results from an intrinsic latency test.
#[derive(Debug, Clone)]
pub struct LatencyTestResult {
    /// Duration of the test in seconds.
    pub duration_secs: u64,
    /// Total number of samples taken.
    pub samples: u64,
    /// Minimum latency observed in microseconds.
    pub min_us: u64,
    /// Maximum latency observed in microseconds.
    pub max_us: u64,
    /// Average latency in microseconds.
    pub avg_us: f64,
    /// 99th percentile latency in microseconds.
    pub p99_us: u64,
}

/// Callback for reporting progress during long-running tests.
pub type ProgressCallback = Box<dyn Fn(u64) + Send>;

/// Run an intrinsic latency test for the specified duration.
///
/// This test measures the scheduling jitter of the system by running a tight loop
/// and measuring the time between iterations. The minimum latency represents the
/// loop overhead, while higher latencies indicate scheduler preemption.
///
/// # Arguments
/// * `duration_secs` - How long to run the test in seconds
/// * `progress_callback` - Optional callback called when max latency increases
///
/// # Returns
/// A `LatencyTestResult` containing statistics about observed latencies.
pub fn run_intrinsic_latency_test(
    duration_secs: u64,
    progress_callback: Option<ProgressCallback>,
) -> LatencyTestResult {
    // Histogram buckets for percentile calculation
    // Buckets: 0-1us, 1-2us, 2-4us, 4-8us, ..., 2^20us (~1s)
    const NUM_BUCKETS: usize = 21;
    let mut histogram = [0u64; NUM_BUCKETS];

    let mut samples: u64 = 0;
    let mut min_us: u64 = u64::MAX;
    let mut max_us: u64 = 0;
    let mut sum_us: u64 = 0;

    let test_duration = Duration::from_secs(duration_secs);
    let start = Instant::now();
    let mut prev = start;

    while start.elapsed() < test_duration {
        let now = Instant::now();
        let delta = now.duration_since(prev);
        let delta_us = delta.as_micros() as u64;
        prev = now;

        // Update statistics
        samples += 1;
        sum_us = sum_us.saturating_add(delta_us);

        if delta_us < min_us {
            min_us = delta_us;
        }

        if delta_us > max_us {
            max_us = delta_us;
            if let Some(ref callback) = progress_callback {
                callback(max_us);
            }
        }

        // Update histogram bucket
        let bucket = if delta_us == 0 {
            0
        } else {
            // log2(delta_us) + 1, capped at NUM_BUCKETS - 1
            let log2 = 64 - delta_us.leading_zeros();
            (log2 as usize).min(NUM_BUCKETS - 1)
        };
        histogram[bucket] += 1;
    }

    // Handle edge case of no samples
    if samples == 0 {
        return LatencyTestResult {
            duration_secs,
            samples: 0,
            min_us: 0,
            max_us: 0,
            avg_us: 0.0,
            p99_us: 0,
        };
    }

    // Ensure min_us is valid
    if min_us == u64::MAX {
        min_us = 0;
    }

    // Calculate average
    let avg_us = sum_us as f64 / samples as f64;

    // Calculate p99 from histogram
    let p99_us = calculate_percentile(&histogram, samples, 99.0);

    LatencyTestResult {
        duration_secs,
        samples,
        min_us,
        max_us,
        avg_us,
        p99_us,
    }
}

/// Calculate a percentile from the histogram.
fn calculate_percentile(histogram: &[u64; 21], total_samples: u64, percentile: f64) -> u64 {
    let target = ((total_samples as f64) * (percentile / 100.0)).ceil() as u64;
    let mut cumulative = 0u64;

    for (bucket, &count) in histogram.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            // Return the upper bound of this bucket
            return if bucket == 0 {
                1 // 0-1us bucket
            } else {
                1u64 << bucket // 2^bucket microseconds
            };
        }
    }

    // Fallback to max bucket value
    1u64 << 20
}

/// Format a number with thousand separators.
pub fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}

/// Print a formatted latency test report to stdout.
pub fn print_latency_report(result: &LatencyTestResult) {
    println!();
    println!("Results:");
    println!("  Duration: {} seconds", result.duration_secs);
    println!("  Samples:  {}", format_with_commas(result.samples));
    println!("  Min:      {} us", result.min_us);
    println!("  Max:      {} us", result.max_us);
    println!("  Avg:      {:.1} us", result.avg_us);
    println!("  P99:      {} us", result.p99_us);
    println!();
    println!("Interpretation:");
    if result.max_us < 500 {
        println!("  Excellent - typical for bare metal systems with proper tuning.");
    } else if result.max_us < 2000 {
        println!("  Good - acceptable for most workloads.");
    } else if result.max_us < 10000 {
        println!(
            "  Warning - elevated latency suggests virtualization overhead or system contention."
        );
    } else {
        println!("  Critical - very high latency indicates severe system issues.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_latency_test() {
        let result = run_intrinsic_latency_test(1, None);
        assert_eq!(result.duration_secs, 1);
        assert!(result.samples > 0);
        assert!(result.min_us <= result.max_us);
        assert!(result.avg_us >= result.min_us as f64);
        assert!(result.avg_us <= result.max_us as f64);
    }

    #[test]
    fn test_format_with_commas() {
        assert_eq!(format_with_commas(0), "0");
        assert_eq!(format_with_commas(123), "123");
        assert_eq!(format_with_commas(1234), "1,234");
        assert_eq!(format_with_commas(12345), "12,345");
        assert_eq!(format_with_commas(123456), "123,456");
        assert_eq!(format_with_commas(1234567), "1,234,567");
        assert_eq!(format_with_commas(1234567890), "1,234,567,890");
    }

    #[test]
    fn test_percentile_calculation() {
        // All samples in bucket 0 (0-1us)
        let mut histogram = [0u64; 21];
        histogram[0] = 100;
        assert_eq!(calculate_percentile(&histogram, 100, 99.0), 1);

        // 98 samples in bucket 0, 2 samples in bucket 10 (1024us)
        // p99 target is ceil(100 * 0.99) = 99 samples
        // cumulative after bucket 0 = 98, still < 99
        // cumulative after bucket 10 = 100, >= 99, so p99 = 1024
        let mut histogram2 = [0u64; 21];
        histogram2[0] = 98;
        histogram2[10] = 2;
        assert_eq!(calculate_percentile(&histogram2, 100, 99.0), 1024);
    }

    #[test]
    fn test_progress_callback() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let max_seen = Arc::new(AtomicU64::new(0));
        let max_seen_clone = max_seen.clone();

        let callback: ProgressCallback = Box::new(move |max_us| {
            max_seen_clone.store(max_us, Ordering::SeqCst);
        });

        let result = run_intrinsic_latency_test(1, Some(callback));

        // The callback should have been called with the max value
        assert!(max_seen.load(Ordering::SeqCst) > 0);
        assert_eq!(max_seen.load(Ordering::SeqCst), result.max_us);
    }
}
