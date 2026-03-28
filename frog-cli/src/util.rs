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
