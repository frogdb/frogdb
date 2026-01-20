//! Noop abstractions for future features.
//!
//! These traits and structs define interfaces for features that will be
//! implemented in later phases. The noop implementations allow the code
//! to compile and run without these features.

use bytes::Bytes;
use std::time::Instant;

// ============================================================================
// Persistence & WAL
// ============================================================================

/// WAL (Write-Ahead Log) writer trait.
///
/// In future phases, this will write operations to persistent storage
/// before applying them to the in-memory store.
pub trait WalWriter: Send + Sync {
    /// Append an operation to the WAL.
    ///
    /// Returns the sequence number assigned to this operation.
    fn append(&mut self, operation: &WalOperation) -> u64;

    /// Flush pending writes to disk.
    fn flush(&mut self) -> std::io::Result<()>;

    /// Get the current sequence number.
    fn current_sequence(&self) -> u64;
}

/// Operation to be written to the WAL.
#[derive(Debug, Clone)]
pub enum WalOperation {
    /// SET key value
    Set { key: Bytes, value: Bytes },
    /// DEL key
    Delete { key: Bytes },
    /// Expire a key
    Expire { key: Bytes, at: Instant },
}

/// Noop WAL writer that does nothing.
#[derive(Debug, Default)]
pub struct NoopWalWriter {
    sequence: u64,
}

impl NoopWalWriter {
    pub fn new() -> Self {
        Self { sequence: 0 }
    }
}

impl WalWriter for NoopWalWriter {
    fn append(&mut self, _operation: &WalOperation) -> u64 {
        self.sequence += 1;
        tracing::trace!(seq = self.sequence, "Noop WAL append");
        self.sequence
    }

    fn flush(&mut self) -> std::io::Result<()> {
        tracing::trace!("Noop WAL flush");
        Ok(())
    }

    fn current_sequence(&self) -> u64 {
        self.sequence
    }
}

// ============================================================================
// Replication
// ============================================================================

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
pub trait ReplicationTracker: Send + Sync {
    /// Wait for replicas to acknowledge up to the given sequence number.
    ///
    /// Returns the number of replicas that acknowledged.
    fn wait_for_acks(&self, sequence: u64, min_replicas: u32) -> impl std::future::Future<Output = u32> + Send;

    /// Record an acknowledgment from a replica.
    fn record_ack(&self, replica_id: u64, sequence: u64);

    /// Get the number of connected replicas.
    fn replica_count(&self) -> usize;
}

/// Noop replication tracker.
#[derive(Debug, Default)]
pub struct NoopReplicationTracker;

impl NoopReplicationTracker {
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

// ============================================================================
// Security / ACL
// ============================================================================

/// ACL (Access Control List) checker trait.
pub trait AclChecker: Send + Sync {
    /// Check if a user has permission to execute a command.
    fn check_permission(&self, user: &str, command: &str, keys: &[&[u8]]) -> AclResult;

    /// Check if a user is authenticated.
    fn is_authenticated(&self, user: &str) -> bool;
}

/// Result of an ACL check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclResult {
    /// Permission granted.
    Allowed,
    /// Permission denied.
    Denied { reason: String },
    /// Authentication required.
    NoAuth,
}

impl AclResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, AclResult::Allowed)
    }
}

/// ACL checker that always allows access.
#[derive(Debug, Default)]
pub struct AlwaysAllowAcl;

impl AlwaysAllowAcl {
    pub fn new() -> Self {
        Self
    }
}

impl AclChecker for AlwaysAllowAcl {
    fn check_permission(&self, _user: &str, _command: &str, _keys: &[&[u8]]) -> AclResult {
        tracing::trace!("AlwaysAllow ACL check");
        AclResult::Allowed
    }

    fn is_authenticated(&self, _user: &str) -> bool {
        true
    }
}

// ============================================================================
// Expiry
// ============================================================================

/// Index for tracking key expiration times.
#[derive(Debug, Default)]
pub struct ExpiryIndex {
    // In future phases, this will contain:
    // by_time: BTreeMap<(Instant, Bytes), ()>,
    // by_key: HashMap<Bytes, Instant>,
}

impl ExpiryIndex {
    /// Create a new empty expiry index.
    pub fn new() -> Self {
        Self {}
    }

    /// Add or update expiry for a key.
    pub fn set(&mut self, _key: Bytes, _expires_at: Instant) {
        tracing::trace!("Noop ExpiryIndex set");
    }

    /// Remove expiry for a key.
    pub fn remove(&mut self, _key: &[u8]) {
        tracing::trace!("Noop ExpiryIndex remove");
    }

    /// Get expired keys up to `now`.
    pub fn get_expired(&self, _now: Instant) -> Vec<Bytes> {
        tracing::trace!("Noop ExpiryIndex get_expired");
        vec![]
    }

    /// Sample N keys for active expiry.
    pub fn sample(&self, _n: usize) -> Vec<Bytes> {
        tracing::trace!("Noop ExpiryIndex sample");
        vec![]
    }

    /// Number of keys with expiry set.
    pub fn len(&self) -> usize {
        0
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        true
    }
}

// ============================================================================
// Observability
// ============================================================================

/// Metrics recorder trait (OpenTelemetry-ready).
pub trait MetricsRecorder: Send + Sync {
    /// Increment a counter.
    fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]);

    /// Record a gauge value.
    fn record_gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Record a histogram observation.
    fn record_histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);
}

/// Noop metrics recorder.
#[derive(Debug, Default)]
pub struct NoopMetricsRecorder;

impl NoopMetricsRecorder {
    pub fn new() -> Self {
        Self
    }
}

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop counter increment");
    }

    fn record_gauge(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop gauge record");
    }

    fn record_histogram(&self, name: &str, value: f64, _labels: &[(&str, &str)]) {
        tracing::trace!(name, value, "Noop histogram record");
    }
}

/// Tracer trait for distributed tracing.
pub trait Tracer: Send + Sync {
    /// Start a new span.
    fn start_span(&self, name: &str) -> Box<dyn Span>;
}

/// A tracing span.
pub trait Span: Send {
    /// Add an attribute to the span.
    fn set_attribute(&mut self, key: &str, value: &str);

    /// End the span.
    fn end(self: Box<Self>);
}

/// Noop tracer.
#[derive(Debug, Default)]
pub struct NoopTracer;

impl NoopTracer {
    pub fn new() -> Self {
        Self
    }
}

impl Tracer for NoopTracer {
    fn start_span(&self, name: &str) -> Box<dyn Span> {
        tracing::trace!(name, "Noop span start");
        Box::new(NoopSpan)
    }
}

/// Noop span.
#[derive(Debug)]
pub struct NoopSpan;

impl Span for NoopSpan {
    fn set_attribute(&mut self, _key: &str, _value: &str) {}

    fn end(self: Box<Self>) {
        tracing::trace!("Noop span end");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_wal_writer() {
        let mut wal = NoopWalWriter::new();
        assert_eq!(wal.current_sequence(), 0);

        let seq = wal.append(&WalOperation::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });
        assert_eq!(seq, 1);
        assert_eq!(wal.current_sequence(), 1);

        assert!(wal.flush().is_ok());
    }

    #[test]
    fn test_replication_config() {
        assert!(ReplicationConfig::Standalone.is_standalone());
        assert!(ReplicationConfig::Primary { min_replicas_to_write: 0 }.is_primary());
        assert!(ReplicationConfig::Replica { primary_addr: "localhost:6379".into() }.is_replica());
    }

    #[test]
    fn test_always_allow_acl() {
        let acl = AlwaysAllowAcl::new();
        assert!(acl.check_permission("user", "GET", &[b"key"]).is_allowed());
        assert!(acl.is_authenticated("any"));
    }

    #[test]
    fn test_expiry_index() {
        let mut index = ExpiryIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        index.set(Bytes::from("key"), Instant::now());
        assert!(index.get_expired(Instant::now()).is_empty());
    }

    #[test]
    fn test_noop_metrics() {
        let recorder = NoopMetricsRecorder::new();
        recorder.increment_counter("test", 1, &[]);
        recorder.record_gauge("test", 1.0, &[]);
        recorder.record_histogram("test", 1.0, &[]);
    }

    #[test]
    fn test_noop_tracer() {
        let tracer = NoopTracer::new();
        let mut span = tracer.start_span("test");
        span.set_attribute("key", "value");
        span.end();
    }
}
