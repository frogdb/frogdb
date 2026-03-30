use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Extract a string from a redis::Value.
pub fn extract_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Extract an integer from a redis::Value, defaulting to 0.
pub fn extract_int(v: &redis::Value) -> i64 {
    match v {
        redis::Value::Int(n) => *n,
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).parse().unwrap_or(0),
        _ => 0,
    }
}

/// Extract an optional integer from a redis::Value (Nil → None).
pub fn extract_int_opt(v: &redis::Value) -> Option<i64> {
    match v {
        redis::Value::Int(n) => Some(*n),
        redis::Value::Nil => None,
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).parse().ok(),
        _ => None,
    }
}

/// Extract a command array into a joined string.
pub fn extract_command(v: &redis::Value) -> String {
    match v {
        redis::Value::Array(parts) => parts
            .iter()
            .map(extract_string)
            .collect::<Vec<_>>()
            .join(" "),
        _ => extract_string(v),
    }
}

/// Human-readable byte formatting (e.g. "1.5GB", "256KB").
pub fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes}B")
    }
}

/// Format a Unix timestamp as a relative time string (e.g. "5m ago").
pub fn format_unix_time(ts: i64) -> String {
    let time = UNIX_EPOCH + Duration::from_secs(ts as u64);
    let elapsed = SystemTime::now()
        .duration_since(time)
        .unwrap_or(Duration::ZERO);
    let secs = elapsed.as_secs();
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86400)
    }
}

/// Format microseconds as a human-readable duration (e.g. "1.5ms", "250us").
pub fn format_duration_us(us: i64) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    } else if us >= 1000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{us}us")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_string_bulk() {
        let val = redis::Value::BulkString(b"hello".to_vec());
        assert_eq!(extract_string(&val), "hello");
    }

    #[test]
    fn test_extract_string_simple() {
        let val = redis::Value::SimpleString("world".to_string());
        assert_eq!(extract_string(&val), "world");
    }

    #[test]
    fn test_extract_string_int() {
        let val = redis::Value::Int(42);
        assert_eq!(extract_string(&val), "42");
    }

    #[test]
    fn test_extract_string_nil() {
        assert_eq!(extract_string(&redis::Value::Nil), "");
    }

    #[test]
    fn test_extract_int_from_int() {
        assert_eq!(extract_int(&redis::Value::Int(7)), 7);
    }

    #[test]
    fn test_extract_int_from_bulk_string() {
        let val = redis::Value::BulkString(b"123".to_vec());
        assert_eq!(extract_int(&val), 123);
    }

    #[test]
    fn test_extract_int_from_nil() {
        assert_eq!(extract_int(&redis::Value::Nil), 0);
    }

    #[test]
    fn test_extract_int_opt_some() {
        assert_eq!(extract_int_opt(&redis::Value::Int(5)), Some(5));
    }

    #[test]
    fn test_extract_int_opt_nil() {
        assert_eq!(extract_int_opt(&redis::Value::Nil), None);
    }

    #[test]
    fn test_extract_int_opt_bulk_string() {
        let val = redis::Value::BulkString(b"99".to_vec());
        assert_eq!(extract_int_opt(&val), Some(99));
    }

    #[test]
    fn test_extract_command_array() {
        let val = redis::Value::Array(vec![
            redis::Value::BulkString(b"SET".to_vec()),
            redis::Value::BulkString(b"key".to_vec()),
            redis::Value::BulkString(b"value".to_vec()),
        ]);
        assert_eq!(extract_command(&val), "SET key value");
    }

    #[test]
    fn test_extract_command_single() {
        let val = redis::Value::BulkString(b"PING".to_vec());
        assert_eq!(extract_command(&val), "PING");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1048576), "1.0MB");
        assert_eq!(format_bytes(1073741824), "1.0GB");
    }

    #[test]
    fn test_format_duration_us() {
        assert_eq!(format_duration_us(500), "500us");
        assert_eq!(format_duration_us(1500), "1.5ms");
        assert_eq!(format_duration_us(2_500_000), "2.50s");
    }
}
