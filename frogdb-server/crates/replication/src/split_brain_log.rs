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
}
