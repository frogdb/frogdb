use crate::client::BundleInfo;

/// Print a table of bundles to stdout.
pub fn print_bundle_table(bundles: &[BundleInfo]) {
    if bundles.is_empty() {
        println!("No bundles found.");
        return;
    }

    println!("{:<38}  {:<24}  {:>8}", "ID", "Created", "Size");
    println!(
        "{:<38}  {:<24}  {:>8}",
        "──────────────────────────────────────", "────────────────────────", "────────"
    );

    for b in bundles {
        println!(
            "{:<38}  {:<24}  {:>8}",
            b.id,
            format_timestamp(b.created_at),
            format_bytes(b.size_bytes),
        );
    }

    println!();
    let count = bundles.len();
    println!("{count} bundle{}", if count == 1 { "" } else { "s" });
}

/// Format a byte count as a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Format a Unix timestamp (seconds) as a UTC datetime string.
pub fn format_timestamp(unix_secs: u64) -> String {
    // Days in each month (non-leap)
    const DAYS_IN_MONTH: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let secs = unix_secs % 60;
    let mins = (unix_secs / 60) % 60;
    let hours = (unix_secs / 3600) % 24;
    let mut days = unix_secs / 86400;

    // Compute year
    let mut year: u64 = 1970;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    // Compute month and day
    let mut month: u64 = 0;
    for m in 0..12 {
        let dim = if m == 1 && is_leap_year(year) {
            29
        } else {
            DAYS_IN_MONTH[m as usize]
        };
        if days < dim {
            month = m;
            break;
        }
        days -= dim;
    }

    format!(
        "{year:04}-{:02}-{:02} {hours:02}:{mins:02}:{secs:02} UTC",
        month + 1,
        days + 1,
    )
}

fn is_leap_year(y: u64) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(12_800), "12.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_timestamp_epoch() {
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00 UTC");
    }

    #[test]
    fn test_format_timestamp_known() {
        // 2026-03-17 14:30:02 UTC = 1773757802
        assert_eq!(format_timestamp(1_773_757_802), "2026-03-17 14:30:02 UTC");
    }

    #[test]
    fn test_format_timestamp_leap_year() {
        // 2024-02-29 00:00:00 UTC = 1709164800
        assert_eq!(format_timestamp(1_709_164_800), "2024-02-29 00:00:00 UTC");
    }

    #[test]
    fn test_format_timestamp_end_of_year() {
        // 2023-12-31 23:59:59 UTC = 1704067199
        assert_eq!(format_timestamp(1_704_067_199), "2023-12-31 23:59:59 UTC");
    }
}
