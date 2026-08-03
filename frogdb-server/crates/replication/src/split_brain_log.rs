//! Split-brain discarded-writes log.
//!
//! During network partitions, a split-brain window exists where the old primary
//! accepts writes before discovering it's been demoted. When the partition heals,
//! those divergent writes are destroyed during full resync. This module logs the
//! discarded writes before resync, giving operators an audit trail and the option
//! to manually replay critical operations.
//!
//! Log format:
//! ```text
//! # split_brain_discarded_20240115T103045Z.log
//! timestamp=2024-01-15T10:30:45Z
//! old_primary=<node_id_hex_or_repl_id>
//! new_primary=<node_id_hex_or_unknown>
//! epoch_old=41
//! epoch_new=42
//! seq_diverge_start=12345
//! seq_diverge_end=12400
//! ops_discarded=55
//!
//! *3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n
//! ```

use bytes::Bytes;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Header metadata for a split-brain discarded-writes log.
pub struct SplitBrainLogHeader {
    /// ISO 8601 timestamp of when the split-brain was detected.
    pub timestamp: String,
    /// Node ID (hex) or replication ID of the old (demoted) primary.
    pub old_primary: String,
    /// Node ID (hex) of the new primary, or "unknown".
    pub new_primary: String,
    /// Configuration epoch of the old primary.
    pub epoch_old: u64,
    /// Configuration epoch of the new primary.
    pub epoch_new: u64,
    /// First unacked replication offset (start of divergence).
    pub seq_diverge_start: u64,
    /// Current replication offset (end of divergence).
    pub seq_diverge_end: u64,
    /// Number of discarded operations.
    pub ops_discarded: usize,
}

/// Format a `SystemTime` as an ISO 8601 UTC timestamp string (e.g. `20240115T103045Z`).
fn format_timestamp_compact(t: SystemTime) -> String {
    let dur = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();

    // Manual UTC calendar calculation (no chrono dependency)
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Days since epoch to Y-M-D (simplified Gregorian)
    let (year, month, day) = days_to_ymd(days);

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Format a `SystemTime` as a human-readable ISO 8601 UTC timestamp.
fn format_timestamp_readable(t: SystemTime) -> String {
    let dur = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();

    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    let (year, month, day) = days_to_ymd(days);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Epoch is 1970-01-01
    let mut year = 1970;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = is_leap(year);
    let month_days: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    let day = days + 1;
    (year, month, day)
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

/// Write a split-brain discarded-writes log file.
///
/// Returns the path of the written file.
pub fn write_log(
    data_dir: &Path,
    header: SplitBrainLogHeader,
    entries: &[(u64, Bytes)],
) -> io::Result<PathBuf> {
    let now = SystemTime::now();
    let compact_ts = format_timestamp_compact(now);
    let readable_ts = if header.timestamp.is_empty() {
        format_timestamp_readable(now)
    } else {
        header.timestamp.clone()
    };

    let filename = format!("split_brain_discarded_{}.log", compact_ts);
    let path = data_dir.join(&filename);

    let mut file = std::fs::File::create(&path)?;

    // Write header
    writeln!(file, "timestamp={}", readable_ts)?;
    writeln!(file, "old_primary={}", header.old_primary)?;
    writeln!(file, "new_primary={}", header.new_primary)?;
    writeln!(file, "epoch_old={}", header.epoch_old)?;
    writeln!(file, "epoch_new={}", header.epoch_new)?;
    writeln!(file, "seq_diverge_start={}", header.seq_diverge_start)?;
    writeln!(file, "seq_diverge_end={}", header.seq_diverge_end)?;
    writeln!(file, "ops_discarded={}", header.ops_discarded)?;
    writeln!(file)?;

    // Write RESP entries
    for (_offset, resp_bytes) in entries {
        file.write_all(resp_bytes)?;
        // Ensure separation between entries (RESP is self-delimiting, but
        // an extra newline improves readability for operators)
        if !resp_bytes.ends_with(b"\n") {
            writeln!(file)?;
        }
    }

    file.sync_all()?;

    Ok(path)
}

/// Check if any unprocessed split-brain log files exist in `data_dir`.
pub fn has_pending_logs(data_dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return false;
    };
    entries.filter_map(|e| e.ok()).any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with("split_brain_discarded_")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-REPLICATION-024
    #[test]
    fn test_write_and_read_log() {
        let dir = tempfile::tempdir().unwrap();

        let entries = vec![
            (
                12345,
                Bytes::from("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"),
            ),
            (12350, Bytes::from("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")),
        ];

        let header = SplitBrainLogHeader {
            timestamp: "2024-01-15T10:30:45Z".to_string(),
            old_primary: "abc123".to_string(),
            new_primary: "def456".to_string(),
            epoch_old: 41,
            epoch_new: 42,
            seq_diverge_start: 12345,
            seq_diverge_end: 12400,
            ops_discarded: 2,
        };

        let path = write_log(dir.path(), header, &entries).unwrap();
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("timestamp=2024-01-15T10:30:45Z"));
        assert!(content.contains("old_primary=abc123"));
        assert!(content.contains("new_primary=def456"));
        assert!(content.contains("epoch_old=41"));
        assert!(content.contains("epoch_new=42"));
        assert!(content.contains("seq_diverge_start=12345"));
        assert!(content.contains("seq_diverge_end=12400"));
        assert!(content.contains("ops_discarded=2"));
        assert!(content.contains("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"));
        assert!(content.contains("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"));
    }

    // FM-REPLICATION-024
    #[test]
    fn test_write_log_empty_entries() {
        let dir = tempfile::tempdir().unwrap();

        let header = SplitBrainLogHeader {
            timestamp: "2024-01-15T10:30:45Z".to_string(),
            old_primary: "node1".to_string(),
            new_primary: "unknown".to_string(),
            epoch_old: 1,
            epoch_new: 2,
            seq_diverge_start: 100,
            seq_diverge_end: 100,
            ops_discarded: 0,
        };

        let path = write_log(dir.path(), header, &[]).unwrap();
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("ops_discarded=0"));
    }

    // FM-REPLICATION-024
    #[test]
    fn test_has_pending_logs() {
        let dir = tempfile::tempdir().unwrap();

        // No logs yet
        assert!(!has_pending_logs(dir.path()));

        // Create a log file
        std::fs::write(
            dir.path()
                .join("split_brain_discarded_20240115T103045Z.log"),
            "test",
        )
        .unwrap();

        assert!(has_pending_logs(dir.path()));
    }

    // FM-REPLICATION-024
    #[test]
    fn test_has_pending_logs_ignores_other_files() {
        let dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("some_other_file.log"), "test").unwrap();
        assert!(!has_pending_logs(dir.path()));
    }

    #[test]
    fn test_has_pending_logs_nonexistent_dir() {
        assert!(!has_pending_logs(Path::new("/nonexistent/path")));
    }

    #[test]
    fn test_filename_uses_compact_timestamp() {
        let dir = tempfile::tempdir().unwrap();

        let header = SplitBrainLogHeader {
            timestamp: String::new(), // Let it auto-generate
            old_primary: "n1".to_string(),
            new_primary: "n2".to_string(),
            epoch_old: 0,
            epoch_new: 0,
            seq_diverge_start: 0,
            seq_diverge_end: 0,
            ops_discarded: 0,
        };

        let path = write_log(dir.path(), header, &[]).unwrap();
        let filename = path.file_name().unwrap().to_string_lossy();
        assert!(filename.starts_with("split_brain_discarded_"));
        assert!(filename.ends_with(".log"));
        // Compact timestamp format: YYYYMMDDTHHMMSSz
        assert!(filename.contains('T'));
        assert!(filename.contains('Z'));
    }

    #[test]
    fn test_days_to_ymd() {
        // 1970-01-01
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        // 1970-01-02
        assert_eq!(days_to_ymd(1), (1970, 1, 2));
        // 2024-01-01 = 19723 days since epoch
        assert_eq!(days_to_ymd(19723), (2024, 1, 1));
    }

    // --- the hand-rolled UTC calendar ---------------------------------------
    //
    // The log's filename *is* its identity — an operator finds the audit of a
    // partition by its timestamp, and two windows that render to the same name
    // overwrite one another. There is no chrono here to lean on, so the
    // calendar is pinned against known instants rather than against itself.

    /// Every instant below was cross-checked against an independent
    /// implementation (Python's `datetime.fromtimestamp(s, timezone.utc)`), so
    /// the table is a reference, not a transcription of this module's output.
    fn calendar_vectors() -> Vec<(u64, &'static str, &'static str)> {
        vec![
            // The epoch itself, and the three times of day the arithmetic
            // splits differently (midnight, noon, the last second).
            (0, "19700101T000000Z", "1970-01-01T00:00:00Z"),
            (43_200, "19700101T120000Z", "1970-01-01T12:00:00Z"),
            (86_399, "19700101T235959Z", "1970-01-01T23:59:59Z"),
            // First day of the second month: day-of-year exactly equals
            // January's length, the boundary the month loop rounds wrong if it
            // breaks on `<=`.
            (2_678_400, "19700201T000000Z", "1970-02-01T00:00:00Z"),
            // Year rollover: the last second of 1970 and the first of 1971 —
            // day-of-year exactly equals the year length at the seam.
            (31_535_999, "19701231T235959Z", "1970-12-31T23:59:59Z"),
            (31_536_000, "19710101T000000Z", "1971-01-01T00:00:00Z"),
            // Feb 28 -> Mar 1 in a common year (no Feb 29 in between).
            (1_677_628_799, "20230228T235959Z", "2023-02-28T23:59:59Z"),
            (1_677_628_800, "20230301T000000Z", "2023-03-01T00:00:00Z"),
            // The same seam in a leap year: Feb 29 exists and Mar 1 is a day later.
            (1_709_164_799, "20240228T235959Z", "2024-02-28T23:59:59Z"),
            (1_709_208_000, "20240229T120000Z", "2024-02-29T12:00:00Z"),
            (1_709_251_200, "20240301T000000Z", "2024-03-01T00:00:00Z"),
            // Dec 31 / Jan 1 across a leap-year boundary.
            (1_704_067_199, "20231231T235959Z", "2023-12-31T23:59:59Z"),
            (1_704_067_200, "20240101T000000Z", "2024-01-01T00:00:00Z"),
            // 2000 is a leap year (divisible by 400) — Feb 29 exists.
            (951_782_400, "20000229T000000Z", "2000-02-29T00:00:00Z"),
            // 2100 is *not* (divisible by 100 but not 400) — Feb 28 is followed
            // directly by Mar 1. The century rule is the one a naive `% 4`
            // implementation gets wrong, and it is only 75 years out.
            (4_107_542_399, "21000228T235959Z", "2100-02-28T23:59:59Z"),
            (4_107_542_400, "21000301T000000Z", "2100-03-01T00:00:00Z"),
            // A mid-everything instant: no component is 0, 12 or 59.
            (1_705_314_645, "20240115T103045Z", "2024-01-15T10:30:45Z"),
            // 2^31-1: the classic 32-bit `time_t` rollover instant.
            (2_147_483_647, "20380119T031407Z", "2038-01-19T03:14:07Z"),
        ]
    }

    #[test]
    fn timestamps_render_known_utc_instants_in_both_formats() {
        for (secs, compact, readable) in calendar_vectors() {
            let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs);
            assert_eq!(
                format_timestamp_compact(t),
                compact,
                "compact rendering of epoch second {secs}"
            );
            assert_eq!(
                format_timestamp_readable(t),
                readable,
                "readable rendering of epoch second {secs}"
            );
        }
    }

    #[test]
    fn the_two_timestamp_formats_describe_the_same_instant() {
        // The compact form names the file, the readable form goes in the
        // header: an operator matches one against the other, so they must not
        // be able to disagree. Cross-check by punctuating one into the other
        // rather than by re-deriving either.
        for (secs, _, _) in calendar_vectors() {
            let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs);
            let compact = format_timestamp_compact(t);
            let readable = format_timestamp_readable(t);
            let punctuated = format!(
                "{}-{}-{}T{}:{}:{}Z",
                &compact[0..4],
                &compact[4..6],
                &compact[6..8],
                &compact[9..11],
                &compact[11..13],
                &compact[13..15],
            );
            assert_eq!(
                punctuated, readable,
                "formats disagree at epoch second {secs}"
            );
        }
    }

    #[test]
    fn a_clock_before_the_epoch_renders_as_the_epoch() {
        // `duration_since(UNIX_EPOCH).unwrap_or_default()` clamps rather than
        // panicking: a node whose clock is set before 1970 still writes its
        // audit log (under a colliding name, which is the lesser evil).
        let before = SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1);
        assert_eq!(format_timestamp_compact(before), "19700101T000000Z");
        assert_eq!(format_timestamp_readable(before), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn days_to_ymd_pins_the_calendar_seams() {
        let cases: &[(u64, (u64, u64, u64))] = &[
            (0, (1970, 1, 1)),
            (1, (1970, 1, 2)),
            // Day-of-year 31 is Feb 1, not Jan 32.
            (31, (1970, 2, 1)),
            (58, (1970, 2, 28)),
            (59, (1970, 3, 1)), // 1970 is common: no Feb 29
            (364, (1970, 12, 31)),
            (365, (1971, 1, 1)), // days == year length rolls the year
            (11_016, (2000, 2, 29)),
            (19_722, (2023, 12, 31)),
            (19_723, (2024, 1, 1)),
            (19_781, (2024, 2, 28)),
            (19_782, (2024, 2, 29)),
            (19_783, (2024, 3, 1)),
            (47_540, (2100, 2, 28)),
            (47_541, (2100, 3, 1)), // 2100 is common despite being divisible by 4
        ];
        for &(days, expected) in cases {
            assert_eq!(days_to_ymd(days), expected, "days_to_ymd({days})");
        }
    }

    #[test]
    fn is_leap_follows_the_full_gregorian_rule() {
        for (year, leap) in [
            (1970, false),
            (1972, true),
            (1900, false), // century, not divisible by 400
            (2000, true),  // century, divisible by 400
            (2023, false),
            (2024, true),
            (2100, false),
            (2400, true),
        ] {
            assert_eq!(is_leap(year), leap, "is_leap({year})");
        }
    }

    #[test]
    fn days_to_ymd_advances_one_day_at_a_time_for_a_century() {
        // A cross-check that does not restate the algorithm: walk every day
        // from the epoch to 2070 and assert each result is exactly one day
        // after the previous one, with month lengths taken from `is_leap`.
        // Any off-by-one in the year or month loops shows up as a skipped or
        // repeated date rather than as a wrong constant.
        let (mut y, mut m, mut d) = (1970u64, 1u64, 1u64);
        for days in 1..=36_525u64 {
            let month_len = match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 if is_leap(y) => 29,
                2 => 28,
                _ => unreachable!("month out of range: {m}"),
            };
            if d == month_len {
                d = 1;
                if m == 12 {
                    m = 1;
                    y += 1;
                } else {
                    m += 1;
                }
            } else {
                d += 1;
            }
            assert_eq!(days_to_ymd(days), (y, m, d), "day {days} since the epoch");
        }
        // The walk really did cover a century's worth of leap-rule cases:
        // 100 years = 100*365 + 25 leap days.
        assert_eq!(days_to_ymd(36_525), (2070, 1, 1));
    }

    // FM-REPLICATION-024
    #[test]
    fn every_discarded_entry_is_newline_separated_exactly_once() {
        // The audit is read by eye and by line-oriented tooling, so each
        // discarded write must start on its own line — but a RESP payload
        // already ends in `\r\n`, and appending unconditionally would put a
        // blank line between every pair of entries. The separator is added
        // only when the entry does not already end in a newline.
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (1, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n")),
            (2, Bytes::from_static(b"NOT-RESP-NO-NEWLINE")),
            (3, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n")),
        ];
        let header = SplitBrainLogHeader {
            timestamp: "2024-01-15T10:30:45Z".to_string(),
            old_primary: "n1".to_string(),
            new_primary: "n2".to_string(),
            epoch_old: 1,
            epoch_new: 2,
            seq_diverge_start: 10,
            seq_diverge_end: 13,
            ops_discarded: 3,
        };

        let path = write_log(dir.path(), header, &entries).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        let body = content.split_once("ops_discarded=3\n\n").unwrap().1;

        assert_eq!(
            body, "*1\r\n$4\r\nPING\r\nNOT-RESP-NO-NEWLINE\n*1\r\n$4\r\nPING\r\n",
            "each entry ends on a line boundary, and none is padded twice"
        );
        // Restated as the property, so a reader does not have to count `\n`s:
        // every entry occupies whole lines and no blank line separates them.
        assert!(!body.contains("\n\n"), "no entry is separated twice");
        assert!(body.ends_with('\n'), "the last entry is terminated");
    }
}
