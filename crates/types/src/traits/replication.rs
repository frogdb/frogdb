//! Replication tracking traits.
//!
//! These traits define the interface for replication between primary and
//! replica nodes. The tracker manages acknowledgments from replicas to
//! implement synchronous replication (WAIT command).

/// Replication configuration.
#[derive(Debug, Clone, Default)]
pub enum ReplicationConfig {
    /// Standalone mode - no replication.
    #[default]
    Standalone,
    /// Primary (master) mode.
    Primary {
        /// Minimum replicas required to acknowledge writes.
        min_replicas_to_write: u32,
    },
    /// Replica (slave) mode.
    Replica {
        /// Primary address.
        primary_addr: String,
    },
}

impl ReplicationConfig {
    /// Check if this node is a primary.
    pub fn is_primary(&self) -> bool {
        matches!(self, ReplicationConfig::Primary { .. })
    }

    /// Check if this node is a replica.
    pub fn is_replica(&self) -> bool {
        matches!(self, ReplicationConfig::Replica { .. })
    }

    /// Check if this node is standalone.
    pub fn is_standalone(&self) -> bool {
        matches!(self, ReplicationConfig::Standalone)
    }
}

/// Replication tracker for synchronous replication.
///
/// This trait tracks acknowledgments from replicas to support the WAIT command
/// and synchronous replication guarantees.
pub trait ReplicationTracker: Send + Sync {
    /// Wait for replicas to acknowledge up to the given sequence number.
    ///
    /// Returns the number of replicas that acknowledged.
    fn wait_for_acks(
        &self,
        sequence: u64,
        min_replicas: u32,
    ) -> impl std::future::Future<Output = u32> + Send;

    /// Record an acknowledgment from a replica.
    fn record_ack(&self, replica_id: u64, sequence: u64);

    /// Get the number of connected replicas.
    fn replica_count(&self) -> usize;
}

/// Noop replication tracker.
///
/// Use this when replication is disabled or for testing.
#[derive(Debug, Default)]
pub struct NoopReplicationTracker;

impl NoopReplicationTracker {
    /// Create a new noop replication tracker.
    pub fn new() -> Self {
        Self
    }
}

impl ReplicationTracker for NoopReplicationTracker {
    async fn wait_for_acks(&self, _sequence: u64, _min_replicas: u32) -> u32 {
        tracing::trace!("Noop replication wait_for_acks");
        0
    }

    fn record_ack(&self, _replica_id: u64, _sequence: u64) {
        tracing::trace!("Noop replication record_ack");
    }

    fn replica_count(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_config() {
        assert!(ReplicationConfig::Standalone.is_standalone());
        assert!(!ReplicationConfig::Standalone.is_primary());
        assert!(!ReplicationConfig::Standalone.is_replica());

        let primary = ReplicationConfig::Primary {
            min_replicas_to_write: 1,
        };
        assert!(primary.is_primary());

        let replica = ReplicationConfig::Replica {
            primary_addr: "localhost:6379".to_string(),
        };
        assert!(replica.is_replica());
    }

    #[test]
    fn test_noop_replication_tracker() {
        let tracker = NoopReplicationTracker::new();
        assert_eq!(tracker.replica_count(), 0);
        tracker.record_ack(1, 100); // Should not panic
    }
}
