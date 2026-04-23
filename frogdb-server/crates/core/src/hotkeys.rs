//! Hotkey detection session types.
//!
//! HOTKEYS is a server-wide observability feature that tracks which keys are
//! accessed most frequently during a sampling session. It operates at the
//! connection handler level, similar to LATENCY and SLOWLOG.
//!
//! Session state machine:
//! ```text
//! [Idle] --START--> [Active] --STOP--> [Stopped]
//!   ^                                       |
//!   |                RESET                  |
//!   +---------------------------------------+
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Metric types that can be tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HotkeyMetric {
    /// CPU time: command execution duration in microseconds.
    Cpu,
    /// NET bytes: request + response size.
    Net,
}

impl HotkeyMetric {
    /// Parse a metric name from a string (case-insensitive).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CPU" => Some(Self::Cpu),
            "NET" => Some(Self::Net),
            _ => None,
        }
    }

    /// Get the lowercase string name of this metric.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Net => "net",
        }
    }
}

/// Configuration for a hotkey sampling session.
#[derive(Debug, Clone)]
pub struct HotkeySessionConfig {
    /// Which metrics to track (1 or 2).
    pub metrics: Vec<HotkeyMetric>,
    /// Maximum number of top keys to keep (default 16).
    pub count: usize,
    /// Maximum duration of the session in milliseconds (0 = unlimited).
    pub duration_ms: u64,
    /// Sampling ratio: 1-in-N accesses are sampled (1 = every access).
    pub sample_ratio: u64,
    /// Selected slots to track. `None` means all 16384 slots.
    pub selected_slots: Option<Vec<u16>>,
}

impl Default for HotkeySessionConfig {
    fn default() -> Self {
        Self {
            metrics: vec![],
            count: 16,
            duration_ms: 0,
            sample_ratio: 1,
            selected_slots: None,
        }
    }
}

/// Per-key access statistics.
#[derive(Debug, Clone, Default)]
pub struct HotkeyEntry {
    /// Number of sampled accesses.
    pub access_count: u64,
    /// Total CPU time in microseconds across sampled accesses.
    pub total_cpu_us: u64,
    /// Total NET bytes across sampled accesses.
    pub total_net_bytes: u64,
}

/// State of a hotkey sampling session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotkeySessionState {
    /// No session is active.
    Idle,
    /// A session is actively sampling.
    Active,
    /// A session has been stopped but data is still available.
    Stopped,
}

/// A hotkey sampling session.
#[derive(Debug)]
pub struct HotkeySession {
    /// Session configuration.
    pub config: HotkeySessionConfig,
    /// Current state.
    pub state: HotkeySessionState,
    /// When the session was started.
    pub started_at: Instant,
    /// When the session was stopped (if stopped).
    pub stopped_at: Option<Instant>,
    /// Per-key entry map (key bytes -> entry).
    pub entries: HashMap<Vec<u8>, HotkeyEntry>,
    /// Total number of sampled accesses (incremented each time we actually record).
    pub total_samples: u64,
    /// Total number of key accesses seen (before sampling filter).
    pub total_accesses: u64,
    /// Sampling counter for probabilistic sampling.
    sampling_counter: u64,
}

impl HotkeySession {
    /// Create a new active session with the given config.
    pub fn new(config: HotkeySessionConfig) -> Self {
        Self {
            config,
            state: HotkeySessionState::Active,
            started_at: Instant::now(),
            stopped_at: None,
            entries: HashMap::new(),
            total_samples: 0,
            total_accesses: 0,
            sampling_counter: 0,
        }
    }

    /// Check if the session is active.
    pub fn is_active(&self) -> bool {
        self.state == HotkeySessionState::Active
    }

    /// Check if the session has expired based on duration.
    pub fn is_expired(&self) -> bool {
        if self.config.duration_ms == 0 {
            return false;
        }
        self.started_at.elapsed().as_millis() as u64 >= self.config.duration_ms
    }

    /// Check if a slot is in the selected set.
    pub fn slot_matches(&self, slot: u16) -> bool {
        match &self.config.selected_slots {
            None => true,
            Some(slots) => slots.contains(&slot),
        }
    }

    /// Determine if this access should be sampled.
    /// Uses a simple counter-based approach: sample every Nth access.
    pub fn should_sample(&mut self) -> bool {
        self.sampling_counter += 1;
        if self.sampling_counter >= self.config.sample_ratio {
            self.sampling_counter = 0;
            true
        } else {
            false
        }
    }

    /// Record a key access.
    pub fn record_access(&mut self, key: &[u8], cpu_us: u64, net_bytes: u64) {
        self.total_accesses += 1;

        if !self.should_sample() {
            return;
        }

        self.total_samples += 1;

        let entry = self.entries.entry(key.to_vec()).or_default();
        entry.access_count += 1;
        if self.config.metrics.contains(&HotkeyMetric::Cpu) {
            entry.total_cpu_us += cpu_us;
        }
        if self.config.metrics.contains(&HotkeyMetric::Net) {
            entry.total_net_bytes += net_bytes;
        }
    }

    /// Stop the session.
    pub fn stop(&mut self) {
        self.state = HotkeySessionState::Stopped;
        self.stopped_at = Some(Instant::now());
    }

    /// Get the top N keys sorted by access count (descending).
    pub fn top_keys(&self) -> Vec<(&[u8], &HotkeyEntry)> {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v))
            .collect();
        entries.sort_by(|a, b| b.1.access_count.cmp(&a.1.access_count));
        entries.truncate(self.config.count);
        entries
    }
}

/// Shared hotkey session state.
///
/// `None` means no session exists (idle state).
/// `Some(session)` with `Active` state means sampling is in progress.
/// `Some(session)` with `Stopped` state means data is available for GET.
pub type SharedHotkeySession = Arc<Mutex<Option<HotkeySession>>>;

/// Create a new shared hotkey session (initially idle).
pub fn new_shared_hotkey_session() -> SharedHotkeySession {
    Arc::new(Mutex::new(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hotkey_metric_parsing() {
        assert_eq!(HotkeyMetric::from_str("cpu"), Some(HotkeyMetric::Cpu));
        assert_eq!(HotkeyMetric::from_str("CPU"), Some(HotkeyMetric::Cpu));
        assert_eq!(HotkeyMetric::from_str("net"), Some(HotkeyMetric::Net));
        assert_eq!(HotkeyMetric::from_str("NET"), Some(HotkeyMetric::Net));
        assert_eq!(HotkeyMetric::from_str("foo"), None);
    }

    #[test]
    fn test_session_lifecycle() {
        let config = HotkeySessionConfig {
            metrics: vec![HotkeyMetric::Cpu],
            count: 10,
            ..Default::default()
        };
        let mut session = HotkeySession::new(config);
        assert!(session.is_active());

        session.record_access(b"key1", 100, 0);
        session.record_access(b"key1", 200, 0);
        session.record_access(b"key2", 50, 0);

        assert_eq!(session.total_accesses, 3);
        assert_eq!(session.total_samples, 3);
        assert_eq!(session.entries.len(), 2);
        assert_eq!(session.entries[b"key1".as_slice()].access_count, 2);
        assert_eq!(session.entries[b"key1".as_slice()].total_cpu_us, 300);

        session.stop();
        assert!(!session.is_active());
        assert!(session.stopped_at.is_some());
    }

    #[test]
    fn test_sampling_ratio() {
        let config = HotkeySessionConfig {
            metrics: vec![HotkeyMetric::Cpu],
            sample_ratio: 3,
            ..Default::default()
        };
        let mut session = HotkeySession::new(config);

        // With ratio 3, every 3rd access is sampled
        for _ in 0..9 {
            session.record_access(b"key1", 100, 0);
        }
        assert_eq!(session.total_accesses, 9);
        assert_eq!(session.total_samples, 3);
    }

    #[test]
    fn test_slot_filtering() {
        let config = HotkeySessionConfig {
            metrics: vec![HotkeyMetric::Cpu],
            selected_slots: Some(vec![0, 1, 2]),
            ..Default::default()
        };
        let session = HotkeySession::new(config);

        assert!(session.slot_matches(0));
        assert!(session.slot_matches(1));
        assert!(session.slot_matches(2));
        assert!(!session.slot_matches(3));
    }

    #[test]
    fn test_top_keys() {
        let config = HotkeySessionConfig {
            metrics: vec![HotkeyMetric::Cpu],
            count: 2,
            ..Default::default()
        };
        let mut session = HotkeySession::new(config);

        session.record_access(b"key1", 100, 0);
        session.record_access(b"key2", 100, 0);
        session.record_access(b"key2", 100, 0);
        session.record_access(b"key3", 100, 0);
        session.record_access(b"key3", 100, 0);
        session.record_access(b"key3", 100, 0);

        let top = session.top_keys();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, b"key3");
        assert_eq!(top[1].0, b"key2");
    }
}
