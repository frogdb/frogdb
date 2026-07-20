//! FrogDB Testing Utilities
//!
//! This crate provides testing infrastructure for FrogDB, including:
//! - Operation history recording
//! - Linearizability checking using the WGL algorithm
//! - Sequential specification models for key-value operations
//! - Per-key history partitioning (`partition`) for scalable checking
//! - Conservation checkers (`conservation`): exactly-once delivery, FIFO wake
//!   order, transaction-sum conservation, WATCH no-false-negative
//! - Quiescence checkers (`quiescence`): lock-table-empty, wait-queue-empty,
//!   memory-accounting, expiry-index-consistent — consuming parsed DEBUG-reply
//!   snapshots
//! - Fault-injection self-tests (`fault_injection`) guarding against
//!   silent-green checker bugs
//! - Strict per-type models: lists, hashes, sorted sets, streams
//!
//! # Encoding convention
//!
//! Pipe (`|`) is the reserved multi-value delimiter used across the
//! result encodings in this crate (e.g. blocking-pop hits as
//! `"served_key|elem"`, `HGETALL`/`LRANGE` field/value joins). Generated
//! keys and values must not themselves contain `|` — a value that does can
//! silently false-accept by being misparsed as an extra delimited field.
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
pub mod quiescence;
pub mod workload;

pub use checker::{LinearizabilityResult, check_linearizability, check_linearizability_bounded};
pub use conservation::{
    ConservationViolation, check_exactly_once_delivery, check_fifo_wake_order,
    check_pel_conservation, check_tx_sum_conservation, check_watch_no_false_negative,
};
pub use history::{History, OpKind, Operation};
pub use models::{
    Group, HashModel, HashState, KVModel, KVState, ListModel, ListState, Model, PelEntry,
    RegisterModel, RegisterState, StreamData, StreamGroupData, StreamGroupModel, StreamGroupState,
    StreamId, StreamModel, StreamState, ZSetModel, ZSetState,
};
pub use partition::{default_keys_of, is_errored_exec_result, partition_by_key};
pub use quiescence::{
    ExpiryIndexSnapshot, LockTableSnapshot, MemoryCheckSnapshot, QuiescenceViolation,
    WaitQueueSnapshot, check_expiry_index_consistent, check_locktable_empty,
    check_memory_accounting, check_waitqueue_empty,
};
pub use workload::{ClientScript, Profile, ScriptedOp, Workload};
