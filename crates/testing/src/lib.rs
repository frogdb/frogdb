//! FrogDB Testing Utilities
//!
//! This crate provides testing infrastructure for FrogDB, including:
//! - Operation history recording
//! - Linearizability checking using the WGL algorithm
//! - Sequential specification models for key-value operations
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
pub mod history;
pub mod models;

pub use checker::{check_linearizability, LinearizabilityResult};
pub use history::{History, Operation, OpKind};
pub use models::{KVModel, Model, RegisterModel};
