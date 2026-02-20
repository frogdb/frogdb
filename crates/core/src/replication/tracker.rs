//! Replication tracker for managing replica acknowledgments.
//!
//! The tracker maintains information about connected replicas and their
//! acknowledged offsets. This enables the WAIT command for synchronous
//! replication.

use crate::noop::ReplicationTracker;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;

/// Information about a connected replica.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Unique ID for this replica connection
    pub id: u64,

    /// Replica's address
    pub address: SocketAddr,

    /// Replica's listening port (from REPLCONF listening-port)
    pub listening_port: u16,

    /// Last acknowledged offset
    pub acked_offset: u64,

    /// Timestamp of last ACK
    pub last_ack_time: Instant,

    /// Connection timestamp
    pub connected_at: Instant,

    /// Replication state
    pub state: ReplicaState,

    /// Capabilities negotiated during handshake
    pub capabilities: ReplicaCapabilities,
}

/// Replication state of a replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Connecting - initial state
    Connecting,
    /// Handshaking - REPLCONF exchange
    Handshaking,
    /// Syncing - receiving FULLRESYNC data
    Syncing,
    /// Streaming - receiving incremental WAL updates
    Streaming,
    /// Disconnected - connection lost
    Disconnected,
}

impl std::fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaState::Connecting => write!(f, "connecting"),
            ReplicaState::Handshaking => write!(f, "handshaking"),
            ReplicaState::Syncing => write!(f, "syncing"),
            ReplicaState::Streaming => write!(f, "online"),
            ReplicaState::Disconnected => write!(f, "disconnected"),
        }
    }
}

/// Capabilities negotiated with replica.
#[derive(Debug, Clone, Default)]
pub struct ReplicaCapabilities {
    /// Supports EOF marker in RDB transfer
    pub eof: bool,
    /// Supports PSYNC2 protocol
    pub psync2: bool,
}

impl ReplicaCapabilities {
    pub fn parse_capa(capabilities: &[&str]) -> Self {
        let mut caps = Self::default();
        for cap in capabilities {
            match *cap {
                "eof" => caps.eof = true,
                "psync2" => caps.psync2 = true,
                _ => {}
            }
        }
        caps
    }
}

/// Concrete implementation of ReplicationTracker.
pub struct ReplicationTrackerImpl {
    /// Connected replicas
    replicas: RwLock<HashMap<u64, ReplicaInfo>>,

    /// Next replica ID
    next_replica_id: AtomicU64,

    /// Current replication offset (primary's write position)
    current_offset: AtomicU64,

    /// Channel for notifying waiters about ACKs
    ack_notify: broadcast::Sender<(u64, u64)>, // (replica_id, offset)
}

impl Default for ReplicationTrackerImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationTrackerImpl {
    /// Create a new replication tracker.
    pub fn new() -> Self {
        let (ack_notify, _) = broadcast::channel(1024);
        Self {
            replicas: RwLock::new(HashMap::new()),
            next_replica_id: AtomicU64::new(1),
            current_offset: AtomicU64::new(0),
            ack_notify,
        }
    }

    /// Create a new tracker as Arc (for shared ownership).
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Register a new replica connection.
    pub fn register_replica(&self, address: SocketAddr) -> u64 {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let info = ReplicaInfo {
            id,
            address,
            listening_port: 0,
            acked_offset: 0,
            last_ack_time: Instant::now(),
            connected_at: Instant::now(),
            state: ReplicaState::Connecting,
            capabilities: ReplicaCapabilities::default(),
        };

        self.replicas.write().insert(id, info);

        tracing::info!(
            replica_id = id,
            address = %address,
            "Registered new replica"
        );

        id
    }

    /// Unregister a replica connection.
    pub fn unregister_replica(&self, replica_id: u64) {
        if let Some(info) = self.replicas.write().remove(&replica_id) {
            tracing::info!(
                replica_id = replica_id,
                address = %info.address,
                "Unregistered replica"
            );
        }
    }

    /// Update replica's listening port (from REPLCONF listening-port).
    pub fn set_listening_port(&self, replica_id: u64, port: u16) {
        if let Some(info) = self.replicas.write().get_mut(&replica_id) {
            info.listening_port = port;
        }
    }

    /// Update replica's capabilities (from REPLCONF capa).
    pub fn set_capabilities(&self, replica_id: u64, caps: ReplicaCapabilities) {
        if let Some(info) = self.replicas.write().get_mut(&replica_id) {
            info.capabilities = caps;
        }
    }

    /// Update replica's state.
    pub fn set_state(&self, replica_id: u64, state: ReplicaState) {
        if let Some(info) = self.replicas.write().get_mut(&replica_id) {
            tracing::debug!(
                replica_id = replica_id,
                old_state = %info.state,
                new_state = %state,
                "Replica state change"
            );
            info.state = state;
        }
    }

    /// Get replica info.
    pub fn get_replica(&self, replica_id: u64) -> Option<ReplicaInfo> {
        self.replicas.read().get(&replica_id).cloned()
    }

    /// Get all replica info.
    pub fn get_all_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas.read().values().cloned().collect()
    }

    /// Get replicas in streaming state.
    pub fn get_streaming_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas
            .read()
            .values()
            .filter(|r| r.state == ReplicaState::Streaming)
            .cloned()
            .collect()
    }

    /// Set the current replication offset.
    pub fn set_offset(&self, offset: u64) {
        self.current_offset.store(offset, Ordering::Release);
    }

    /// Increment the current replication offset.
    pub fn increment_offset(&self, bytes: u64) -> u64 {
        self.current_offset.fetch_add(bytes, Ordering::Release) + bytes
    }

    /// Get the current replication offset.
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::Acquire)
    }

    /// Get the minimum acknowledged offset across all streaming replicas.
    pub fn min_acked_offset(&self) -> Option<u64> {
        self.replicas
            .read()
            .values()
            .filter(|r| r.state == ReplicaState::Streaming)
            .map(|r| r.acked_offset)
            .min()
    }

    /// Count replicas that have acknowledged at least the given offset.
    pub fn count_acked(&self, offset: u64) -> u32 {
        self.replicas
            .read()
            .values()
            .filter(|r| r.state == ReplicaState::Streaming && r.acked_offset >= offset)
            .count() as u32
    }

    /// Calculate lag in bytes for a specific replica.
    pub fn replica_lag(&self, replica_id: u64) -> Option<u64> {
        let current = self.current_offset();
        self.replicas
            .read()
            .get(&replica_id)
            .map(|r| current.saturating_sub(r.acked_offset))
    }

    /// Subscribe to ACK notifications.
    pub fn subscribe_acks(&self) -> broadcast::Receiver<(u64, u64)> {
        self.ack_notify.subscribe()
    }
}

impl ReplicationTracker for ReplicationTrackerImpl {
    /// Wait for replicas to acknowledge up to the given sequence number.
    async fn wait_for_acks(&self, sequence: u64, min_replicas: u32) -> u32 {
        // Check current state first
        let current_count = self.count_acked(sequence);
        if current_count >= min_replicas {
            return current_count;
        }

        // Subscribe to ACK notifications
        let mut rx = self.ack_notify.subscribe();

        loop {
            // Recheck after potential ACKs
            let count = self.count_acked(sequence);
            if count >= min_replicas {
                return count;
            }

            // Wait for next ACK
            match rx.recv().await {
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return self.count_acked(sequence);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    }

    /// Record an acknowledgment from a replica.
    fn record_ack(&self, replica_id: u64, sequence: u64) {
        let mut replicas = self.replicas.write();
        if let Some(info) = replicas.get_mut(&replica_id) {
            // Only update if this is a newer ACK
            if sequence > info.acked_offset {
                info.acked_offset = sequence;
                info.last_ack_time = Instant::now();

                // Notify waiters
                let _ = self.ack_notify.send((replica_id, sequence));

                tracing::trace!(
                    replica_id = replica_id,
                    offset = sequence,
                    "Recorded replica ACK"
                );
            }
        }
    }

    /// Get the number of connected replicas.
    fn replica_count(&self) -> usize {
        self.replicas
            .read()
            .values()
            .filter(|r| r.state == ReplicaState::Streaming)
            .count()
    }
}

// Make it safe to share across threads
unsafe impl Send for ReplicationTrackerImpl {}
unsafe impl Sync for ReplicationTrackerImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:6380".parse().unwrap()
    }

    #[test]
    fn test_register_unregister_replica() {
        let tracker = ReplicationTrackerImpl::new();

        let id = tracker.register_replica(test_addr());
        assert_eq!(tracker.replica_count(), 0); // Not streaming yet

        tracker.set_state(id, ReplicaState::Streaming);
        assert_eq!(tracker.replica_count(), 1);

        tracker.unregister_replica(id);
        assert_eq!(tracker.replica_count(), 0);
    }

    #[test]
    fn test_record_ack() {
        let tracker = ReplicationTrackerImpl::new();

        let id = tracker.register_replica(test_addr());
        tracker.set_state(id, ReplicaState::Streaming);

        tracker.record_ack(id, 100);
        assert_eq!(tracker.count_acked(100), 1);
        assert_eq!(tracker.count_acked(101), 0);

        tracker.record_ack(id, 200);
        assert_eq!(tracker.count_acked(200), 1);
    }

    #[test]
    fn test_replica_lag() {
        let tracker = ReplicationTrackerImpl::new();

        let id = tracker.register_replica(test_addr());
        tracker.set_state(id, ReplicaState::Streaming);
        tracker.set_offset(1000);

        tracker.record_ack(id, 800);
        assert_eq!(tracker.replica_lag(id), Some(200));

        tracker.record_ack(id, 1000);
        assert_eq!(tracker.replica_lag(id), Some(0));
    }

    #[test]
    fn test_min_acked_offset() {
        let tracker = ReplicationTrackerImpl::new();

        // No replicas
        assert_eq!(tracker.min_acked_offset(), None);

        let id1 = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        let id2 = tracker.register_replica("127.0.0.1:6381".parse().unwrap());

        tracker.set_state(id1, ReplicaState::Streaming);
        tracker.set_state(id2, ReplicaState::Streaming);

        tracker.record_ack(id1, 100);
        tracker.record_ack(id2, 200);

        assert_eq!(tracker.min_acked_offset(), Some(100));
    }

    #[test]
    fn test_capabilities_parsing() {
        let caps = ReplicaCapabilities::parse_capa(&["eof", "psync2"]);
        assert!(caps.eof);
        assert!(caps.psync2);

        let caps = ReplicaCapabilities::parse_capa(&["eof"]);
        assert!(caps.eof);
        assert!(!caps.psync2);

        let caps = ReplicaCapabilities::parse_capa(&["unknown"]);
        assert!(!caps.eof);
        assert!(!caps.psync2);
    }

    #[tokio::test]
    async fn test_wait_for_acks_immediate() {
        let tracker = ReplicationTrackerImpl::new();

        let id = tracker.register_replica(test_addr());
        tracker.set_state(id, ReplicaState::Streaming);
        tracker.record_ack(id, 100);

        // Should return immediately
        let count = tracker.wait_for_acks(100, 1).await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_wait_for_acks_with_timeout() {
        let tracker = Arc::new(ReplicationTrackerImpl::new());

        let id = tracker.register_replica(test_addr());
        tracker.set_state(id, ReplicaState::Streaming);

        let tracker_clone = tracker.clone();
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                tracker_clone.wait_for_acks(100, 1),
            )
            .await
        });

        // Send ACK after a short delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        tracker.record_ack(id, 100);

        let result = wait_handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_replica_states() {
        let tracker = ReplicationTrackerImpl::new();

        let id = tracker.register_replica(test_addr());
        assert_eq!(
            tracker.get_replica(id).unwrap().state,
            ReplicaState::Connecting
        );

        tracker.set_state(id, ReplicaState::Handshaking);
        assert_eq!(
            tracker.get_replica(id).unwrap().state,
            ReplicaState::Handshaking
        );

        tracker.set_state(id, ReplicaState::Syncing);
        assert_eq!(
            tracker.get_replica(id).unwrap().state,
            ReplicaState::Syncing
        );

        tracker.set_state(id, ReplicaState::Streaming);
        assert_eq!(
            tracker.get_replica(id).unwrap().state,
            ReplicaState::Streaming
        );
    }

    #[test]
    fn test_get_streaming_replicas() {
        let tracker = ReplicationTrackerImpl::new();

        let id1 = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        let id2 = tracker.register_replica("127.0.0.1:6381".parse().unwrap());
        let id3 = tracker.register_replica("127.0.0.1:6382".parse().unwrap());

        tracker.set_state(id1, ReplicaState::Streaming);
        tracker.set_state(id2, ReplicaState::Syncing);
        tracker.set_state(id3, ReplicaState::Streaming);

        let streaming = tracker.get_streaming_replicas();
        assert_eq!(streaming.len(), 2);
    }
}
