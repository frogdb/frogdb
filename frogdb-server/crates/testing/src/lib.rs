//! FrogDB Testing Utilities
//!
//! This crate provides testing infrastructure for FrogDB, including:
//! - Operation history recording
//! - Linearizability checking using the WGL algorithm
//! - Sequential specification models for key-value operations
//! - Per-key history partitioning (`partition`) for scalable checking
//! - Conservation checkers (`conservation`): exactly-once delivery, FIFO wake
//!   order, transaction-sum conservation, WATCH no-false-negative
//! - Fault-injection self-tests (`fault_injection`) guarding against
//!   silent-green checker bugs
//! - Strict per-type models: lists, hashes, sorted sets, streams
//!
//! # Linearizability Checking
//!
//! Linearizability is a correctness criterion for concurrent systems.
//! An execution is linearizable if we can assign a linearization point
//! to each operation such that:
//! 1. The linearization point is between the invocation and response
//! 2. The sequential execution defined by the linearization points
//!    satisfies the sequential specification
//!
//! # Example
//!
//! ```
//! use frogdb_testing::{History, KVModel, check_linearizability};
//! use bytes::Bytes;
//!
//! let mut history = History::new();
//!
//! // Record operations
//! let op1 = history.invoke(1, "write", vec![Bytes::from("x"), Bytes::from("1")]);
//! history.respond(op1, Some(Bytes::from("OK")));
//!
//! let op2 = history.invoke(2, "read", vec![Bytes::from("x")]);
//! history.respond(op2, Some(Bytes::from("1")));
//!
//! // Check linearizability
//! let result = check_linearizability::<KVModel>(&history);
//! assert!(result.is_linearizable);
//! ```

pub mod checker;
pub mod conservation;
pub mod fault_injection;
pub mod history;
pub mod models;
pub mod partition;

pub use checker::{LinearizabilityResult, check_linearizability, check_linearizability_bounded};
pub use conservation::{
    ConservationViolation, check_exactly_once_delivery, check_fifo_wake_order,
    check_tx_sum_conservation, check_watch_no_false_negative,
};
pub use history::{History, OpKind, Operation};
pub use models::{
    HashModel, HashState, KVModel, KVState, ListModel, ListState, Model, RegisterModel,
    RegisterState, StreamData, StreamId, StreamModel, StreamState, ZSetModel, ZSetState,
};
pub use partition::{default_keys_of, partition_by_key};
