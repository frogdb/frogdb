//! Operation history recording for linearizability checking.
//!
//! This module provides types for recording and representing operation histories
//! in a format suitable for linearizability checking.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global operation ID counter.
static NEXT_OP_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique operation ID.
fn next_op_id() -> u64 {
    NEXT_OP_ID.fetch_add(1, Ordering::SeqCst)
}

/// Kind of operation record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OpKind {
    /// Operation was invoked but not yet returned.
    Invoke,
    /// Operation returned with a result.
    Return,
}

/// A recorded operation in the history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// Unique operation ID (same for invoke/return pair).
    pub id: u64,
    /// Client that performed the operation.
    pub client_id: u64,
    /// Whether this is invoke or return.
    pub kind: OpKind,
    /// Operation function name (e.g., "read", "write", "cas").
    pub function: String,
    /// Operation arguments.
    #[serde(with = "bytes_vec_serde")]
    pub args: Vec<Bytes>,
    /// Operation result (only for Return records).
    #[serde(with = "bytes_option_serde")]
    pub result: Option<Bytes>,
    /// Logical timestamp for ordering.
    pub timestamp: u64,
}

/// Custom serialization for Vec<Bytes>.
mod bytes_vec_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let strings: Vec<String> = bytes
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        strings.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let strings: Vec<String> = Vec::deserialize(deserializer)?;
        Ok(strings.into_iter().map(Bytes::from).collect())
    }
}

/// Custom serialization for Option<Bytes>.
mod bytes_option_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let string: Option<String> = bytes
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());
        string.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string: Option<String> = Option::deserialize(deserializer)?;
        Ok(string.map(Bytes::from))
    }
}

/// A history of operations for linearizability checking.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct History {
    /// All operations in timestamp order.
    operations: Vec<Operation>,
    /// Current logical timestamp.
    #[serde(skip)]
    current_time: u64,
    /// Pending operations (invoke without return).
    #[serde(skip)]
    pending: HashMap<u64, usize>,
}

impl History {
    /// Create a new empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next timestamp.
    fn next_timestamp(&mut self) -> u64 {
        let t = self.current_time;
        self.current_time += 1;
        t
    }

    /// Record an operation invocation.
    ///
    /// Returns the operation ID to be used when recording the response.
    pub fn invoke(&mut self, client_id: u64, function: impl Into<String>, args: Vec<Bytes>) -> u64 {
        let id = next_op_id();
        let timestamp = self.next_timestamp();

        let op = Operation {
            id,
            client_id,
            kind: OpKind::Invoke,
            function: function.into(),
            args,
            result: None,
            timestamp,
        };

        let idx = self.operations.len();
        self.operations.push(op);
        self.pending.insert(id, idx);

        id
    }

    /// Record an operation response.
    pub fn respond(&mut self, op_id: u64, result: Option<Bytes>) {
        let invoke_idx = self
            .pending
            .remove(&op_id)
            .expect("respond called for unknown operation");

        // Clone data from invoke before calling next_timestamp (which borrows self mutably)
        let invoke = &self.operations[invoke_idx];
        let client_id = invoke.client_id;
        let function = invoke.function.clone();
        let args = invoke.args.clone();

        let timestamp = self.next_timestamp();

        let op = Operation {
            id: op_id,
            client_id,
            kind: OpKind::Return,
            function,
            args,
            result,
            timestamp,
        };

        self.operations.push(op);
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
        self.pending.is_empty()
    }

    /// Get pending operation IDs.
    pub fn pending_ops(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }

    /// Get invocations in timestamp order.
    pub fn invocations(&self) -> Vec<&Operation> {
        self.operations
            .iter()
            .filter(|op| op.kind == OpKind::Invoke)
            .collect()
    }

    /// Get the return operation for a given invocation.
    pub fn get_return(&self, op_id: u64) -> Option<&Operation> {
        self.operations
            .iter()
            .find(|op| op.id == op_id && op.kind == OpKind::Return)
    }

    /// Get completed operations (invoke-return pairs).
    pub fn completed_operations(&self) -> Vec<CompletedOperation> {
        self.invocations()
            .into_iter()
            .filter_map(|invoke| {
                self.get_return(invoke.id).map(|ret| CompletedOperation {
                    id: invoke.id,
                    client_id: invoke.client_id,
                    function: invoke.function.clone(),
                    args: invoke.args.clone(),
                    result: ret.result.clone(),
                    invoke_time: invoke.timestamp,
                    return_time: ret.timestamp,
                })
            })
            .collect()
    }

    /// Export history to JSON format.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Import history from JSON format.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// A completed operation (invoke-return pair).
#[derive(Debug, Clone)]
pub struct CompletedOperation {
    /// Operation ID.
    pub id: u64,
    /// Client ID.
    pub client_id: u64,
    /// Function name.
    pub function: String,
    /// Arguments.
    pub args: Vec<Bytes>,
    /// Result.
    pub result: Option<Bytes>,
    /// Invocation timestamp.
    pub invoke_time: u64,
    /// Return timestamp.
    pub return_time: u64,
}

impl CompletedOperation {
    /// Check if this operation could have happened before another.
    ///
    /// Operation A could happen before B if A returns before B is invoked.
    pub fn could_precede(&self, other: &CompletedOperation) -> bool {
        self.return_time < other.invoke_time
    }

    /// Check if this operation is concurrent with another.
    ///
    /// Operations are concurrent if neither could precede the other.
    pub fn is_concurrent_with(&self, other: &CompletedOperation) -> bool {
        !self.could_precede(other) && !other.could_precede(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_history() {
        let mut history = History::new();

        let op1 = history.invoke(1, "write", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let op2 = history.invoke(1, "read", vec![Bytes::from("x")]);
        history.respond(op2, Some(Bytes::from("1")));

        assert!(history.is_complete());
        assert_eq!(history.operations().len(), 4);
    }

    #[test]
    fn test_concurrent_operations() {
        let mut history = History::new();

        // Client 1 writes
        let op1 = history.invoke(1, "write", vec![Bytes::from("x"), Bytes::from("1")]);
        // Client 2 reads (concurrent with client 1's write)
        let op2 = history.invoke(2, "read", vec![Bytes::from("x")]);
        history.respond(op1, Some(Bytes::from("OK")));
        history.respond(op2, Some(Bytes::from("1"))); // Could read 1 or nil

        assert!(history.is_complete());

        let completed = history.completed_operations();
        assert_eq!(completed.len(), 2);

        // Operations are concurrent
        assert!(completed[0].is_concurrent_with(&completed[1]));
    }

    #[test]
    fn test_sequential_operations() {
        let mut history = History::new();

        let op1 = history.invoke(1, "write", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        // op2 starts after op1 completes
        let op2 = history.invoke(1, "read", vec![Bytes::from("x")]);
        history.respond(op2, Some(Bytes::from("1")));

        let completed = history.completed_operations();
        assert!(completed[0].could_precede(&completed[1]));
        assert!(!completed[0].is_concurrent_with(&completed[1]));
    }

    #[test]
    fn test_json_serialization() {
        let mut history = History::new();
        let op1 = history.invoke(1, "write", vec![Bytes::from("key"), Bytes::from("val")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let json = history.to_json();
        let restored = History::from_json(&json).unwrap();

        assert_eq!(restored.operations().len(), 2);
    }
}
