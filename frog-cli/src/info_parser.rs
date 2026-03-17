use std::collections::HashMap;
use std::str::FromStr;

/// Parsed Redis INFO response, organized by section.
pub struct InfoResponse {
    sections: HashMap<String, HashMap<String, String>>,
}

impl InfoResponse {
    pub fn parse(raw: &str) -> Self {
        let mut sections = HashMap::new();
        let mut current_section = String::new();

        for line in raw.lines() {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            if let Some(name) = line.strip_prefix("# ") {
                current_section = name.to_lowercase();
                sections
                    .entry(current_section.clone())
                    .or_insert_with(HashMap::new);
            } else if let Some((key, value)) = line.split_once(':') {
                sections
                    .entry(current_section.clone())
                    .or_insert_with(HashMap::new)
                    .insert(key.to_string(), value.to_string());
            }
        }

        Self { sections }
    }

    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(key))
            .map(|s| s.as_str())
    }

    pub fn get_parsed<T: FromStr>(&self, section: &str, key: &str) -> Option<T> {
        self.get(section, key).and_then(|v| v.parse().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_info() {
        let raw = "# Server\r\n\
                    frogdb_version:0.1.0\r\n\
                    uptime_in_seconds:3600\r\n\
                    \r\n\
                    # Memory\r\n\
                    used_memory:1048576\r\n\
                    maxmemory:8589934592\r\n\
                    \r\n\
                    # Keyspace\r\n\
                    db0:keys=1000,expires=0,avg_ttl=0\r\n";

        let info = InfoResponse::parse(raw);

        assert_eq!(info.get("server", "frogdb_version"), Some("0.1.0"));
        assert_eq!(
            info.get_parsed::<u64>("server", "uptime_in_seconds"),
            Some(3600)
        );
        assert_eq!(
            info.get_parsed::<u64>("memory", "used_memory"),
            Some(1048576)
        );
        assert_eq!(
            info.get("keyspace", "db0"),
            Some("keys=1000,expires=0,avg_ttl=0")
        );
        assert_eq!(info.get("server", "nonexistent"), None);
        assert_eq!(info.get("nonexistent", "key"), None);
    }

    #[test]
    fn test_parse_empty() {
        let info = InfoResponse::parse("");
        assert_eq!(info.get("server", "anything"), None);
    }
}
