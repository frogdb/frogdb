//! Turmoil simulation test harness for FrogDB.
//!
//! This module provides utilities for running FrogDB under Turmoil's
//! deterministic network simulation, enabling testing of:
//! - Scatter-gather operations under network delays
//! - Message ordering and timing
//! - Future: Network partitions and fault injection

use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use turmoil::{Builder, Sim};

/// Default server address for simulation.
pub const SERVER_ADDR: (IpAddr, u16) = (IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 6379);

/// Default server hostname in simulation.
pub const SERVER_HOST: &str = "server";

/// Operation counter for generating unique operation IDs.
static OP_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique operation ID for history tracking.
pub fn next_op_id() -> u64 {
    OP_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Configuration for simulation tests.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of simulated clients.
    pub num_clients: usize,
    /// Number of shards in the server.
    pub num_shards: usize,
    /// Whether to enable network latency simulation.
    pub enable_latency: bool,
    /// Base latency in milliseconds.
    pub base_latency_ms: u64,
    /// Random seed for deterministic simulation.
    pub seed: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            num_clients: 1,
            num_shards: 4,
            enable_latency: false,
            base_latency_ms: 0,
            seed: 42,
        }
    }
}

/// Build a simulation with the given configuration.
pub fn build_sim(config: &SimConfig) -> Sim<'static> {
    let mut builder = Builder::new();
    builder.simulation_duration(Duration::from_secs(60));

    // Set deterministic seed for reproducibility
    // Note: turmoil uses the build() method which doesn't take a seed parameter
    // The simulation is deterministic by default

    builder.build()
}

/// Operation kind for history recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// Operation invocation.
    Invoke,
    /// Operation return.
    Return,
}

/// A recorded operation for linearizability checking.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Operation ID (unique per invocation).
    pub op_id: u64,
    /// Client that performed the operation.
    pub client_id: u64,
    /// Whether this is an invoke or return record.
    pub kind: OpKind,
    /// Command name (e.g., "GET", "SET", "MSET").
    pub command: String,
    /// Command arguments.
    pub args: Vec<Bytes>,
    /// Result of the operation (only set for Return records).
    pub result: Option<OperationResult>,
    /// Logical timestamp (monotonic within simulation).
    pub timestamp: u64,
}

/// Result of an operation.
#[derive(Debug, Clone)]
pub enum OperationResult {
    /// Simple string response.
    Ok,
    /// Nil response.
    Nil,
    /// String value.
    String(Bytes),
    /// Integer value.
    Integer(i64),
    /// Array of results.
    Array(Vec<OperationResult>),
    /// Error response.
    Error(String),
}

/// History of operations for linearizability checking.
#[derive(Debug, Default)]
pub struct OperationHistory {
    /// All operations in timestamp order.
    operations: Vec<Operation>,
    /// Current logical timestamp.
    current_time: AtomicU64,
}

impl OperationHistory {
    /// Create a new empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next timestamp.
    fn next_timestamp(&self) -> u64 {
        self.current_time.fetch_add(1, Ordering::SeqCst)
    }

    /// Record an operation invocation.
    pub fn record_invoke(
        &mut self,
        client_id: u64,
        command: impl Into<String>,
        args: Vec<Bytes>,
    ) -> u64 {
        let op_id = next_op_id();
        let timestamp = self.next_timestamp();

        self.operations.push(Operation {
            op_id,
            client_id,
            kind: OpKind::Invoke,
            command: command.into(),
            args,
            result: None,
            timestamp,
        });

        op_id
    }

    /// Record an operation return.
    pub fn record_return(&mut self, op_id: u64, client_id: u64, result: OperationResult) {
        let timestamp = self.next_timestamp();

        // Find the corresponding invoke to get the command info
        let invoke = self
            .operations
            .iter()
            .find(|op| op.op_id == op_id && op.kind == OpKind::Invoke)
            .expect("Return without matching invoke");

        self.operations.push(Operation {
            op_id,
            client_id,
            kind: OpKind::Return,
            command: invoke.command.clone(),
            args: invoke.args.clone(),
            result: Some(result),
            timestamp,
        });
    }

    /// Get all operations in the history.
    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    /// Get operations for a specific client.
    pub fn client_operations(&self, client_id: u64) -> Vec<&Operation> {
        self.operations
            .iter()
            .filter(|op| op.client_id == client_id)
            .collect()
    }

    /// Check if all operations have matching invoke/return pairs.
    pub fn is_complete(&self) -> bool {
        let invokes: std::collections::HashSet<_> = self
            .operations
            .iter()
            .filter(|op| op.kind == OpKind::Invoke)
            .map(|op| op.op_id)
            .collect();

        let returns: std::collections::HashSet<_> = self
            .operations
            .iter()
            .filter(|op| op.kind == OpKind::Return)
            .map(|op| op.op_id)
            .collect();

        invokes == returns
    }
}

/// Simulated client for FrogDB operations.
pub struct SimClient {
    /// Client identifier.
    pub id: u64,
    /// Server address.
    pub server_addr: (IpAddr, u16),
}

impl SimClient {
    /// Create a new simulated client.
    pub fn new(id: u64, server_addr: (IpAddr, u16)) -> Self {
        Self { id, server_addr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_recording() {
        let mut history = OperationHistory::new();

        // Record a SET operation
        let op1 = history.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("value")]);
        history.record_return(op1, 1, OperationResult::Ok);

        // Record a GET operation
        let op2 = history.record_invoke(1, "GET", vec![Bytes::from("key")]);
        history.record_return(op2, 1, OperationResult::String(Bytes::from("value")));

        assert!(history.is_complete());
        assert_eq!(history.operations().len(), 4); // 2 invokes + 2 returns
    }

    #[test]
    fn test_incomplete_history() {
        let mut history = OperationHistory::new();

        // Record only invoke
        history.record_invoke(1, "SET", vec![Bytes::from("key"), Bytes::from("value")]);

        assert!(!history.is_complete());
    }
}
