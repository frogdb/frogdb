//! Turmoil simulation test harness for FrogDB.
//!
//! This module provides utilities for running FrogDB under Turmoil's
//! deterministic network simulation, enabling testing of:
//! - Scatter-gather operations under network delays
//! - Message ordering and timing
//! - Future: Network partitions and fault injection

use std::collections::HashMap;
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
pub fn build_sim(_config: &SimConfig) -> Sim<'static> {
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

    /// Convert to frogdb_testing::History for linearizability checking.
    ///
    /// This method bridges the sim_harness history format with the testing
    /// crate's linearizability checker.
    pub fn to_testing_history(&self) -> frogdb_testing::History {
        let mut history = frogdb_testing::History::new();

        // Group operations by ID to pair invoke/return
        let mut invokes: HashMap<u64, &Operation> = HashMap::new();

        for op in &self.operations {
            match op.kind {
                OpKind::Invoke => {
                    invokes.insert(op.op_id, op);
                }
                OpKind::Return => {
                    if let Some(invoke) = invokes.remove(&op.op_id) {
                        // Convert command name to lowercase for model matching
                        let function = invoke.command.to_lowercase();
                        let op_id = history.invoke(invoke.client_id, &function, invoke.args.clone());
                        let result = self.convert_result(&op.result);
                        history.respond(op_id, result);
                    }
                }
            }
        }

        history
    }

    /// Convert OperationResult to Option<Bytes> for the testing crate.
    fn convert_result(&self, result: &Option<OperationResult>) -> Option<Bytes> {
        match result {
            None => None,
            Some(OperationResult::Ok) => Some(Bytes::from("OK")),
            Some(OperationResult::Nil) => None,
            Some(OperationResult::String(b)) => Some(b.clone()),
            Some(OperationResult::Integer(n)) => Some(Bytes::from(n.to_string())),
            Some(OperationResult::Array(arr)) => Some(Self::encode_array_result(arr)),
            Some(OperationResult::Error(_)) => None, // Errors are not linearizable results
        }
    }

    /// Encode an array of results as a pipe-delimited string for linearizability checking.
    ///
    /// Format: "value1|nil|value2|OK|5"
    /// - String values are encoded as-is
    /// - Nil values are encoded as "nil"
    /// - OK is encoded as "OK"
    /// - Integers are encoded as their string representation
    pub fn encode_array_result(results: &[OperationResult]) -> Bytes {
        let parts: Vec<String> = results
            .iter()
            .map(|r| match r {
                OperationResult::Ok => "OK".to_string(),
                OperationResult::Nil => "nil".to_string(),
                OperationResult::String(b) => String::from_utf8_lossy(b).to_string(),
                OperationResult::Integer(n) => n.to_string(),
                OperationResult::Array(arr) => {
                    // Nested arrays are encoded recursively
                    String::from_utf8_lossy(&Self::encode_array_result(arr)).to_string()
                }
                OperationResult::Error(e) => format!("ERR:{}", e),
            })
            .collect();
        Bytes::from(parts.join("|"))
    }

    /// Record a transaction (EXEC) as a single atomic operation.
    ///
    /// Commands are encoded as: [num_cmds, cmd1_name, cmd1_num_args, cmd1_args..., cmd2_name, ...]
    pub fn record_exec_invoke(
        &mut self,
        client_id: u64,
        commands: &[(String, Vec<Bytes>)],
    ) -> u64 {
        let mut args = Vec::new();
        args.push(Bytes::from(commands.len().to_string()));

        for (cmd_name, cmd_args) in commands {
            args.push(Bytes::from(cmd_name.clone()));
            args.push(Bytes::from(cmd_args.len().to_string()));
            args.extend(cmd_args.clone());
        }

        self.record_invoke(client_id, "EXEC", args)
    }

    /// Record a transaction return with array of results.
    ///
    /// Results are encoded as a pipe-delimited string.
    pub fn record_exec_return(&mut self, op_id: u64, client_id: u64, results: &[OperationResult]) {
        let encoded = Self::encode_array_result(results);
        self.record_return(op_id, client_id, OperationResult::String(encoded));
    }

    /// Record an MGET return with array of results.
    ///
    /// Results are encoded as a pipe-delimited string (value or "nil").
    pub fn record_mget_return(&mut self, op_id: u64, client_id: u64, results: &[OperationResult]) {
        let encoded = Self::encode_array_result(results);
        self.record_return(op_id, client_id, OperationResult::String(encoded));
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

// =============================================================================
// Sharding Utilities
// =============================================================================

/// Total number of hash slots (Redis-compatible).
pub const HASH_SLOTS: usize = 16384;

/// Calculate the hash slot for a key using CRC16 (XMODEM).
///
/// This matches Redis's hash slot calculation. If the key contains a hash tag
/// (e.g., `{tag}key`), only the contents of the tag are hashed.
pub fn hash_slot(key: &[u8]) -> u16 {
    let key_to_hash = extract_hash_tag(key).unwrap_or(key);
    crc16_xmodem(key_to_hash) % HASH_SLOTS as u16
}

/// Calculate which shard owns a key given a number of shards.
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let slot = hash_slot(key) as usize;
    slot * num_shards / HASH_SLOTS
}

/// Extract the hash tag from a key, if present.
///
/// Hash tags are enclosed in curly braces: `{tag}key` -> `tag`
/// Only the first occurrence of `{...}` is used.
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|&b| b == b'{')?;
    let end = key[start + 1..].iter().position(|&b| b == b'}')?;
    if end > 0 {
        Some(&key[start + 1..start + 1 + end])
    } else {
        None
    }
}

/// CRC16 XMODEM implementation (matches Redis).
fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Message sent to a shard worker.
#[derive(Debug)]
pub enum ShardMessage {
    /// GET operation.
    Get {
        key: Bytes,
        response_tx: tokio::sync::oneshot::Sender<Option<Bytes>>,
    },
    /// SET operation.
    Set {
        key: Bytes,
        value: Bytes,
        response_tx: tokio::sync::oneshot::Sender<bool>,
    },
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

    #[test]
    fn test_encode_array_result() {
        let results = vec![
            OperationResult::Ok,
            OperationResult::String(Bytes::from("value1")),
            OperationResult::Nil,
            OperationResult::Integer(42),
        ];
        let encoded = OperationHistory::encode_array_result(&results);
        assert_eq!(encoded.as_ref(), b"OK|value1|nil|42");
    }

    #[test]
    fn test_record_exec_invoke() {
        let mut history = OperationHistory::new();

        let commands = vec![
            ("SET".to_string(), vec![Bytes::from("key"), Bytes::from("value")]),
            ("GET".to_string(), vec![Bytes::from("key")]),
        ];
        let _op_id = history.record_exec_invoke(1, &commands);

        let ops = history.operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].command, "EXEC");
        // Args: [num_cmds=2, "SET", 2, "key", "value", "GET", 1, "key"]
        assert_eq!(ops[0].args.len(), 8);
        assert_eq!(ops[0].args[0].as_ref(), b"2"); // num_cmds
    }

    #[test]
    fn test_record_exec_return() {
        let mut history = OperationHistory::new();

        let op_id = history.record_invoke(1, "EXEC", vec![Bytes::from("1"), Bytes::from("SET"), Bytes::from("2"), Bytes::from("key"), Bytes::from("val")]);
        history.record_exec_return(op_id, 1, &[OperationResult::Ok]);

        assert!(history.is_complete());

        let testing_history = history.to_testing_history();
        // The result should be encoded as "OK"
        assert_eq!(testing_history.operations().len(), 2); // 1 invoke + 1 return
    }
}
