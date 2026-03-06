//! ACL security log.
//!
//! Records security events such as failed authentication attempts
//! and permission denials.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use frogdb_types::sync::MutexExt;

/// Default maximum number of log entries.
pub const DEFAULT_ACL_LOG_MAX_LEN: usize = 128;

/// Type of ACL log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclLogEntryType {
    /// Failed authentication attempt.
    Auth,
    /// Command permission denied.
    Command,
    /// Key permission denied.
    Key,
    /// Channel permission denied.
    Channel,
}

impl AclLogEntryType {
    /// Get the string name for this entry type.
    pub fn name(&self) -> &'static str {
        match self {
            AclLogEntryType::Auth => "auth",
            AclLogEntryType::Command => "command",
            AclLogEntryType::Key => "key",
            AclLogEntryType::Channel => "channel",
        }
    }
}

/// An entry in the ACL log.
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    /// Unique entry ID (monotonically increasing).
    pub entry_id: u64,
    /// Type of security event.
    pub entry_type: AclLogEntryType,
    /// Number of times this event occurred (collapsed duplicates).
    pub count: u64,
    /// Reason/context for the event.
    pub reason: String,
    /// Username involved (if known).
    pub username: String,
    /// Client address.
    pub client_info: String,
    /// Object that caused the denial (command, key, or channel).
    pub object: String,
    /// Unix timestamp in microseconds.
    pub timestamp_usec: u64,
}

impl AclLogEntry {
    /// Create a new log entry.
    pub fn new(
        entry_id: u64,
        entry_type: AclLogEntryType,
        reason: impl Into<String>,
        username: impl Into<String>,
        client_info: impl Into<String>,
        object: impl Into<String>,
    ) -> Self {
        let timestamp_usec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            entry_id,
            entry_type,
            count: 1,
            reason: reason.into(),
            username: username.into(),
            client_info: client_info.into(),
            object: object.into(),
            timestamp_usec,
        }
    }

    /// Check if this entry can be collapsed with another similar event.
    pub fn can_collapse(
        &self,
        entry_type: AclLogEntryType,
        reason: &str,
        username: &str,
        object: &str,
    ) -> bool {
        self.entry_type == entry_type
            && self.reason == reason
            && self.username == username
            && self.object == object
    }

    /// Increment the count for this entry.
    pub fn increment(&mut self) {
        self.count += 1;
        self.timestamp_usec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(self.timestamp_usec);
    }

    /// Convert to RESP array format for ACL LOG output.
    pub fn to_resp_fields(&self) -> Vec<(&'static str, AclLogValue)> {
        vec![
            ("count", AclLogValue::Integer(self.count as i64)),
            ("reason", AclLogValue::String(self.reason.clone())),
            (
                "context",
                AclLogValue::String(self.entry_type.name().to_string()),
            ),
            ("object", AclLogValue::String(self.object.clone())),
            ("username", AclLogValue::String(self.username.clone())),
            ("age-seconds", AclLogValue::Float(self.age_seconds())),
            ("client-info", AclLogValue::String(self.client_info.clone())),
            ("entry-id", AclLogValue::Integer(self.entry_id as i64)),
            (
                "timestamp-created",
                AclLogValue::Integer(self.timestamp_usec as i64 / 1_000_000),
            ),
            (
                "timestamp-last-updated",
                AclLogValue::Integer(self.timestamp_usec as i64 / 1_000_000),
            ),
        ]
    }

    /// Calculate age in seconds.
    fn age_seconds(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        if now > self.timestamp_usec {
            (now - self.timestamp_usec) as f64 / 1_000_000.0
        } else {
            0.0
        }
    }
}

/// Value types for ACL log response.
#[derive(Debug, Clone)]
pub enum AclLogValue {
    String(String),
    Integer(i64),
    Float(f64),
}

/// ACL security log.
#[derive(Debug)]
pub struct AclLog {
    /// Log entries (newest first).
    entries: Mutex<VecDeque<AclLogEntry>>,
    /// Maximum number of entries to keep.
    max_len: usize,
    /// Next entry ID.
    next_id: Mutex<u64>,
}

impl AclLog {
    /// Create a new ACL log with the specified maximum length.
    pub fn new(max_len: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(max_len)),
            max_len,
            next_id: Mutex::new(0),
        }
    }

    /// Log a security event.
    pub fn log(
        &self,
        entry_type: AclLogEntryType,
        reason: &str,
        username: &str,
        client_info: &str,
        object: &str,
    ) {
        let mut entries = self.entries.lock_or_panic("AclLog::log");

        // Check if we can collapse with the most recent entry
        if let Some(last) = entries.front_mut()
            && last.can_collapse(entry_type, reason, username, object)
        {
            last.increment();
            return;
        }

        // Create new entry
        let entry_id = {
            let mut id = self.next_id.lock_or_panic("AclLog::log::next_id");
            let current = *id;
            *id += 1;
            current
        };

        let entry = AclLogEntry::new(entry_id, entry_type, reason, username, client_info, object);
        entries.push_front(entry);

        // Trim to max length
        while entries.len() > self.max_len {
            entries.pop_back();
        }
    }

    /// Log an authentication failure.
    pub fn log_auth_failure(&self, username: &str, client_info: &str) {
        self.log(
            AclLogEntryType::Auth,
            "AUTH failure",
            username,
            client_info,
            "auth",
        );
    }

    /// Log a command permission denial.
    pub fn log_command_denied(&self, username: &str, client_info: &str, command: &str) {
        self.log(
            AclLogEntryType::Command,
            "command not allowed",
            username,
            client_info,
            command,
        );
    }

    /// Log a key permission denial.
    pub fn log_key_denied(&self, username: &str, client_info: &str, key: &str) {
        self.log(
            AclLogEntryType::Key,
            "key not allowed",
            username,
            client_info,
            key,
        );
    }

    /// Log a channel permission denial.
    pub fn log_channel_denied(&self, username: &str, client_info: &str, channel: &str) {
        self.log(
            AclLogEntryType::Channel,
            "channel not allowed",
            username,
            client_info,
            channel,
        );
    }

    /// Get log entries (newest first).
    pub fn get(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let entries = self.entries.lock_or_panic("AclLog::get");
        let count = count.unwrap_or(10).min(entries.len());
        entries.iter().take(count).cloned().collect()
    }

    /// Reset the log.
    pub fn reset(&self) {
        let mut entries = self.entries.lock_or_panic("AclLog::reset");
        entries.clear();
    }

    /// Get the number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.lock_or_panic("AclLog::len").len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock_or_panic("AclLog::is_empty").is_empty()
    }
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new(DEFAULT_ACL_LOG_MAX_LEN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_creation() {
        let entry = AclLogEntry::new(
            1,
            AclLogEntryType::Auth,
            "AUTH failure",
            "alice",
            "127.0.0.1:12345",
            "auth",
        );

        assert_eq!(entry.entry_id, 1);
        assert_eq!(entry.entry_type, AclLogEntryType::Auth);
        assert_eq!(entry.count, 1);
        assert_eq!(entry.reason, "AUTH failure");
        assert_eq!(entry.username, "alice");
        assert!(entry.timestamp_usec > 0);
    }

    #[test]
    fn test_log_entry_collapse() {
        let entry = AclLogEntry::new(
            1,
            AclLogEntryType::Auth,
            "AUTH failure",
            "alice",
            "127.0.0.1:12345",
            "auth",
        );

        assert!(entry.can_collapse(AclLogEntryType::Auth, "AUTH failure", "alice", "auth"));
        assert!(!entry.can_collapse(AclLogEntryType::Command, "AUTH failure", "alice", "auth"));
        assert!(!entry.can_collapse(AclLogEntryType::Auth, "different reason", "alice", "auth"));
    }

    #[test]
    fn test_acl_log_basic() {
        let log = AclLog::new(10);
        assert!(log.is_empty());

        log.log_auth_failure("alice", "127.0.0.1:12345");
        assert_eq!(log.len(), 1);

        let entries = log.get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, AclLogEntryType::Auth);
        assert_eq!(entries[0].username, "alice");
    }

    #[test]
    fn test_acl_log_collapse() {
        let log = AclLog::new(10);

        // Same event should collapse
        log.log_auth_failure("alice", "127.0.0.1:12345");
        log.log_auth_failure("alice", "127.0.0.1:12345");
        log.log_auth_failure("alice", "127.0.0.1:12345");

        assert_eq!(log.len(), 1);
        let entries = log.get(None);
        assert_eq!(entries[0].count, 3);

        // Different username shouldn't collapse
        log.log_auth_failure("bob", "127.0.0.1:12346");
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn test_acl_log_max_len() {
        let log = AclLog::new(3);

        for i in 0..5 {
            log.log_command_denied(&format!("user{}", i), "127.0.0.1:12345", "GET");
        }

        assert_eq!(log.len(), 3);
        let entries = log.get(None);
        // Should have the 3 most recent (user4, user3, user2)
        assert_eq!(entries[0].username, "user4");
        assert_eq!(entries[1].username, "user3");
        assert_eq!(entries[2].username, "user2");
    }

    #[test]
    fn test_acl_log_reset() {
        let log = AclLog::new(10);

        log.log_auth_failure("alice", "127.0.0.1:12345");
        log.log_command_denied("bob", "127.0.0.1:12346", "SET");
        assert_eq!(log.len(), 2);

        log.reset();
        assert!(log.is_empty());
    }

    #[test]
    fn test_acl_log_get_count() {
        let log = AclLog::new(10);

        for i in 0..5 {
            log.log_command_denied(
                &format!("user{}", i),
                "127.0.0.1:12345",
                &format!("CMD{}", i),
            );
        }

        // Get specific count
        let entries = log.get(Some(3));
        assert_eq!(entries.len(), 3);

        // Get more than available
        let entries = log.get(Some(100));
        assert_eq!(entries.len(), 5);

        // Get default (10)
        let entries = log.get(None);
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn test_entry_type_names() {
        assert_eq!(AclLogEntryType::Auth.name(), "auth");
        assert_eq!(AclLogEntryType::Command.name(), "command");
        assert_eq!(AclLogEntryType::Key.name(), "key");
        assert_eq!(AclLogEntryType::Channel.name(), "channel");
    }

    #[test]
    fn test_different_entry_types() {
        let log = AclLog::new(10);

        log.log_auth_failure("alice", "127.0.0.1:12345");
        log.log_command_denied("alice", "127.0.0.1:12345", "FLUSHALL");
        log.log_key_denied("alice", "127.0.0.1:12345", "admin:*");
        log.log_channel_denied("alice", "127.0.0.1:12345", "private:*");

        assert_eq!(log.len(), 4);

        let entries = log.get(None);
        assert_eq!(entries[0].entry_type, AclLogEntryType::Channel);
        assert_eq!(entries[1].entry_type, AclLogEntryType::Key);
        assert_eq!(entries[2].entry_type, AclLogEntryType::Command);
        assert_eq!(entries[3].entry_type, AclLogEntryType::Auth);
    }
}
