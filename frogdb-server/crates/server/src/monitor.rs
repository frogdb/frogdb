//! MONITOR command broadcasting infrastructure.
//!
//! Uses a bounded `tokio::sync::broadcast` channel for fan-out to all MONITOR
//! subscribers. Slow subscribers receive `Lagged` and skip ahead (natural
//! backpressure). Zero overhead when no subscribers: single `AtomicUsize` load
//! per command (the broadcast channel's internal receiver count).

use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use tokio::sync::broadcast;

/// A single command event captured for MONITOR output.
pub struct MonitorEvent {
    pub timestamp: SystemTime,
    pub client_addr: SocketAddr,
    pub cmd_name: String,
    pub args: Vec<Bytes>,
}

impl MonitorEvent {
    /// Create a new event, redacting AUTH arguments.
    pub fn new(addr: SocketAddr, cmd_name: &str, args: &[Bytes]) -> Self {
        let args = if cmd_name == "AUTH" {
            args.iter()
                .map(|_| Bytes::from_static(b"(redacted)"))
                .collect()
        } else {
            args.to_vec()
        };

        Self {
            timestamp: SystemTime::now(),
            client_addr: addr,
            cmd_name: cmd_name.to_string(),
            args,
        }
    }
}

/// Fan-out broadcaster for MONITOR events.
pub struct MonitorBroadcaster {
    tx: broadcast::Sender<Arc<MonitorEvent>>,
}

impl MonitorBroadcaster {
    /// Create a new broadcaster with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to the MONITOR event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<MonitorEvent>> {
        self.tx.subscribe()
    }

    /// Broadcast an event. No-op if there are no subscribers.
    pub fn send(&self, event: MonitorEvent) {
        let _ = self.tx.send(Arc::new(event));
    }

    /// Check if any clients are subscribed to MONITOR.
    pub fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// Format a MonitorEvent to the Redis MONITOR output format:
    /// `1339844074.277166 [0 127.0.0.1:56604] "SET" "key" "value"`
    pub fn format_event(event: &MonitorEvent) -> String {
        let timestamp = event
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = timestamp.as_secs();
        let micros = timestamp.subsec_micros();

        let mut out = format!(
            "{}.{:06} [0 {}] \"{}\"",
            secs, micros, event.client_addr, event.cmd_name
        );
        for arg in &event.args {
            let _ = write!(out, " \"{}\"", String::from_utf8_lossy(arg));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_format_event() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 56604);
        let event = MonitorEvent {
            timestamp: SystemTime::UNIX_EPOCH + std::time::Duration::new(1339844074, 277166_000),
            client_addr: addr,
            cmd_name: "SET".to_string(),
            args: vec![Bytes::from("key"), Bytes::from("value")],
        };
        let formatted = MonitorBroadcaster::format_event(&event);
        assert_eq!(
            formatted,
            "1339844074.277166 [0 127.0.0.1:56604] \"SET\" \"key\" \"value\""
        );
    }

    #[test]
    fn test_auth_args_redacted() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234);
        let event = MonitorEvent::new(addr, "AUTH", &[Bytes::from("secret_password")]);
        assert_eq!(event.args.len(), 1);
        assert_eq!(event.args[0].as_ref(), b"(redacted)");
    }

    #[test]
    fn test_no_subscribers_send_is_noop() {
        let broadcaster = MonitorBroadcaster::new(16);
        assert!(!broadcaster.has_subscribers());
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234);
        // Should not panic
        broadcaster.send(MonitorEvent::new(addr, "GET", &[Bytes::from("key")]));
    }

    #[test]
    fn test_subscriber_receives_event() {
        let broadcaster = MonitorBroadcaster::new(16);
        let mut rx = broadcaster.subscribe();
        assert!(broadcaster.has_subscribers());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234);
        broadcaster.send(MonitorEvent::new(addr, "GET", &[Bytes::from("key")]));

        let event = rx.try_recv().unwrap();
        assert_eq!(event.cmd_name, "GET");
    }
}
