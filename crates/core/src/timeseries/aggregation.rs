//! Aggregation functions for time series data.
//!
//! Supports various aggregation types compatible with Redis TimeSeries:
//! - Statistical: avg, sum, min, max, range, count
//! - Positional: first, last
//! - Advanced: std.p, std.s, var.p, var.s, twa

/// Aggregation type for time series queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    /// Average of values.
    Avg,
    /// Sum of values.
    Sum,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Range (max - min).
    Range,
    /// Count of samples.
    Count,
    /// First value in the bucket.
    First,
    /// Last value in the bucket.
    Last,
    /// Population standard deviation.
    StdP,
    /// Sample standard deviation.
    StdS,
    /// Population variance.
    VarP,
    /// Sample variance.
    VarS,
    /// Time-weighted average.
    Twa,
}

impl Aggregation {
    /// Parse aggregation type from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "AVG" => Some(Aggregation::Avg),
            "SUM" => Some(Aggregation::Sum),
            "MIN" => Some(Aggregation::Min),
            "MAX" => Some(Aggregation::Max),
            "RANGE" => Some(Aggregation::Range),
            "COUNT" => Some(Aggregation::Count),
            "FIRST" => Some(Aggregation::First),
            "LAST" => Some(Aggregation::Last),
            "STD.P" => Some(Aggregation::StdP),
            "STD.S" => Some(Aggregation::StdS),
            "VAR.P" => Some(Aggregation::VarP),
            "VAR.S" => Some(Aggregation::VarS),
            "TWA" => Some(Aggregation::Twa),
            _ => None,
        }
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Aggregation::Avg => "AVG",
            Aggregation::Sum => "SUM",
            Aggregation::Min => "MIN",
            Aggregation::Max => "MAX",
            Aggregation::Range => "RANGE",
            Aggregation::Count => "COUNT",
            Aggregation::First => "FIRST",
            Aggregation::Last => "LAST",
            Aggregation::StdP => "STD.P",
            Aggregation::StdS => "STD.S",
            Aggregation::VarP => "VAR.P",
            Aggregation::VarS => "VAR.S",
            Aggregation::Twa => "TWA",
        }
    }
}

/// Compute an aggregation over a set of samples.
///
/// Returns None if the samples are empty (except for COUNT which returns 0).
pub fn aggregate(samples: &[(i64, f64)], agg: Aggregation) -> Option<f64> {
    if samples.is_empty() {
        return match agg {
            Aggregation::Count => Some(0.0),
            _ => None,
        };
    }

    match agg {
        Aggregation::Avg => {
            let sum: f64 = samples.iter().map(|(_, v)| v).sum();
            Some(sum / samples.len() as f64)
        }
        Aggregation::Sum => {
            Some(samples.iter().map(|(_, v)| v).sum())
        }
        Aggregation::Min => {
            samples.iter().map(|(_, v)| *v).min_by(|a, b| a.partial_cmp(b).unwrap())
        }
        Aggregation::Max => {
            samples.iter().map(|(_, v)| *v).max_by(|a, b| a.partial_cmp(b).unwrap())
        }
        Aggregation::Range => {
            let min = samples.iter().map(|(_, v)| *v).min_by(|a, b| a.partial_cmp(b).unwrap())?;
            let max = samples.iter().map(|(_, v)| *v).max_by(|a, b| a.partial_cmp(b).unwrap())?;
            Some(max - min)
        }
        Aggregation::Count => {
            Some(samples.len() as f64)
        }
        Aggregation::First => {
            samples.first().map(|(_, v)| *v)
        }
        Aggregation::Last => {
            samples.last().map(|(_, v)| *v)
        }
        Aggregation::StdP => {
            compute_std_dev(samples, true)
        }
        Aggregation::StdS => {
            compute_std_dev(samples, false)
        }
        Aggregation::VarP => {
            compute_variance(samples, true)
        }
        Aggregation::VarS => {
            compute_variance(samples, false)
        }
        Aggregation::Twa => {
            compute_twa(samples)
        }
    }
}

/// Compute variance (population or sample).
fn compute_variance(samples: &[(i64, f64)], population: bool) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if samples.len() == 1 {
        return if population { Some(0.0) } else { None };
    }

    let n = samples.len() as f64;
    let mean: f64 = samples.iter().map(|(_, v)| v).sum::<f64>() / n;
    let sum_sq_diff: f64 = samples.iter().map(|(_, v)| (v - mean).powi(2)).sum();

    let divisor = if population { n } else { n - 1.0 };
    Some(sum_sq_diff / divisor)
}

/// Compute standard deviation (population or sample).
fn compute_std_dev(samples: &[(i64, f64)], population: bool) -> Option<f64> {
    compute_variance(samples, population).map(|v| v.sqrt())
}

/// Compute time-weighted average.
///
/// Each value is weighted by the time interval until the next sample.
/// The last sample's value is used with weight proportional to the bucket end.
fn compute_twa(samples: &[(i64, f64)]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    if samples.len() == 1 {
        return Some(samples[0].1);
    }

    let mut weighted_sum = 0.0;
    let mut total_time = 0i64;

    for i in 0..samples.len() - 1 {
        let (ts, val) = samples[i];
        let next_ts = samples[i + 1].0;
        let duration = next_ts - ts;

        weighted_sum += val * duration as f64;
        total_time += duration;
    }

    if total_time == 0 {
        // All samples at the same timestamp, use simple average
        return Some(samples.iter().map(|(_, v)| v).sum::<f64>() / samples.len() as f64);
    }

    Some(weighted_sum / total_time as f64)
}

/// Aggregate samples into time buckets.
///
/// Groups samples by `bucket_duration_ms` and applies the aggregation to each bucket.
/// Returns (bucket_timestamp, aggregated_value) pairs.
pub fn aggregate_by_bucket(
    samples: &[(i64, f64)],
    bucket_duration_ms: i64,
    agg: Aggregation,
) -> Vec<(i64, f64)> {
    if samples.is_empty() || bucket_duration_ms <= 0 {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut bucket_start = (samples[0].0 / bucket_duration_ms) * bucket_duration_ms;
    let mut bucket_samples: Vec<(i64, f64)> = Vec::new();

    for &(ts, val) in samples {
        let sample_bucket = (ts / bucket_duration_ms) * bucket_duration_ms;

        if sample_bucket != bucket_start {
            // Emit the current bucket
            if !bucket_samples.is_empty() {
                if let Some(agg_val) = aggregate(&bucket_samples, agg) {
                    result.push((bucket_start, agg_val));
                }
            }
            bucket_start = sample_bucket;
            bucket_samples.clear();
        }

        bucket_samples.push((ts, val));
    }

    // Emit the last bucket
    if !bucket_samples.is_empty() {
        if let Some(agg_val) = aggregate(&bucket_samples, agg) {
            result.push((bucket_start, agg_val));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_aggregation() {
        assert_eq!(Aggregation::parse("avg"), Some(Aggregation::Avg));
        assert_eq!(Aggregation::parse("AVG"), Some(Aggregation::Avg));
        assert_eq!(Aggregation::parse("std.p"), Some(Aggregation::StdP));
        assert_eq!(Aggregation::parse("STD.S"), Some(Aggregation::StdS));
        assert_eq!(Aggregation::parse("unknown"), None);
    }

    #[test]
    fn test_aggregate_empty() {
        let samples: Vec<(i64, f64)> = vec![];
        assert_eq!(aggregate(&samples, Aggregation::Count), Some(0.0));
        assert_eq!(aggregate(&samples, Aggregation::Avg), None);
        assert_eq!(aggregate(&samples, Aggregation::Sum), None);
    }

    #[test]
    fn test_aggregate_single() {
        let samples = vec![(1000i64, 42.0f64)];
        assert_eq!(aggregate(&samples, Aggregation::Count), Some(1.0));
        assert_eq!(aggregate(&samples, Aggregation::Avg), Some(42.0));
        assert_eq!(aggregate(&samples, Aggregation::Sum), Some(42.0));
        assert_eq!(aggregate(&samples, Aggregation::Min), Some(42.0));
        assert_eq!(aggregate(&samples, Aggregation::Max), Some(42.0));
        assert_eq!(aggregate(&samples, Aggregation::Range), Some(0.0));
        assert_eq!(aggregate(&samples, Aggregation::First), Some(42.0));
        assert_eq!(aggregate(&samples, Aggregation::Last), Some(42.0));
    }

    #[test]
    fn test_aggregate_multiple() {
        let samples = vec![
            (1000i64, 10.0f64),
            (2000i64, 20.0f64),
            (3000i64, 30.0f64),
            (4000i64, 40.0f64),
        ];

        assert_eq!(aggregate(&samples, Aggregation::Count), Some(4.0));
        assert_eq!(aggregate(&samples, Aggregation::Sum), Some(100.0));
        assert_eq!(aggregate(&samples, Aggregation::Avg), Some(25.0));
        assert_eq!(aggregate(&samples, Aggregation::Min), Some(10.0));
        assert_eq!(aggregate(&samples, Aggregation::Max), Some(40.0));
        assert_eq!(aggregate(&samples, Aggregation::Range), Some(30.0));
        assert_eq!(aggregate(&samples, Aggregation::First), Some(10.0));
        assert_eq!(aggregate(&samples, Aggregation::Last), Some(40.0));
    }

    #[test]
    fn test_variance_population() {
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Variance = 4
        let samples: Vec<(i64, f64)> = vec![
            (1, 2.0), (2, 4.0), (3, 4.0), (4, 4.0),
            (5, 5.0), (6, 5.0), (7, 7.0), (8, 9.0),
        ];
        let var = aggregate(&samples, Aggregation::VarP).unwrap();
        assert!((var - 4.0).abs() < 0.0001);
    }

    #[test]
    fn test_variance_sample() {
        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Sample Variance = 4.571...
        let samples: Vec<(i64, f64)> = vec![
            (1, 2.0), (2, 4.0), (3, 4.0), (4, 4.0),
            (5, 5.0), (6, 5.0), (7, 7.0), (8, 9.0),
        ];
        let var = aggregate(&samples, Aggregation::VarS).unwrap();
        assert!((var - 4.571428571).abs() < 0.0001);
    }

    #[test]
    fn test_std_dev() {
        let samples: Vec<(i64, f64)> = vec![
            (1, 2.0), (2, 4.0), (3, 4.0), (4, 4.0),
            (5, 5.0), (6, 5.0), (7, 7.0), (8, 9.0),
        ];
        let std_p = aggregate(&samples, Aggregation::StdP).unwrap();
        assert!((std_p - 2.0).abs() < 0.0001); // sqrt(4) = 2
    }

    #[test]
    fn test_twa() {
        // Time-weighted average
        // t=0: v=10, duration=10 -> weight=100
        // t=10: v=20, duration=20 -> weight=400
        // Total time = 30, weighted sum = 500
        // TWA = 500/30 = 16.67
        let samples = vec![
            (0i64, 10.0f64),
            (10i64, 20.0f64),
            (30i64, 30.0f64), // Last value's duration is 0 in simple TWA
        ];
        let twa = aggregate(&samples, Aggregation::Twa).unwrap();
        assert!((twa - 16.666666).abs() < 0.001);
    }

    #[test]
    fn test_aggregate_by_bucket() {
        let samples = vec![
            (1000i64, 10.0f64),
            (1500i64, 15.0f64),
            (2000i64, 20.0f64),
            (2500i64, 25.0f64),
            (3000i64, 30.0f64),
        ];

        // 1-second buckets
        let buckets = aggregate_by_bucket(&samples, 1000, Aggregation::Avg);
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0], (1000, 12.5)); // avg(10, 15)
        assert_eq!(buckets[1], (2000, 22.5)); // avg(20, 25)
        assert_eq!(buckets[2], (3000, 30.0)); // avg(30)

        // Sum buckets
        let buckets = aggregate_by_bucket(&samples, 1000, Aggregation::Sum);
        assert_eq!(buckets[0], (1000, 25.0)); // 10 + 15
        assert_eq!(buckets[1], (2000, 45.0)); // 20 + 25
    }

    #[test]
    fn test_aggregate_by_bucket_empty() {
        let samples: Vec<(i64, f64)> = vec![];
        let buckets = aggregate_by_bucket(&samples, 1000, Aggregation::Avg);
        assert!(buckets.is_empty());
    }
}
