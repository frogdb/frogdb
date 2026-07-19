//! Sequential specification models for linearizability checking.
//!
//! A model defines the sequential behavior of a data structure.
//! The linearizability checker uses these models to verify that
//! a concurrent execution could be explained by some sequential ordering.

use bytes::Bytes;

mod kv;
mod list;
mod register;

pub use kv::{KVModel, KVState};
pub use list::{ListModel, ListState};
pub use register::{RegisterModel, RegisterState};

/// A sequential specification model.
///
/// Models define the expected behavior of a data structure when operations
/// are executed sequentially. The linearizability checker uses this to verify
/// that a concurrent history can be linearized to a valid sequential execution.
pub trait Model: Clone + Default {
    /// The state type for this model.
    type State: Clone + Default;

    /// Initial state for the model.
    fn init() -> Self::State {
        Self::State::default()
    }

    /// Execute an operation on the current state.
    ///
    /// Returns `Some((new_state, expected_result))` if the operation is valid,
    /// or `None` if the operation cannot be applied to this state.
    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State>;

    /// Check if a result matches the expected result for an operation.
    ///
    /// This is used when we want to verify that a specific result is consistent
    /// with the model's behavior.
    fn check_result(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> bool {
        Self::step(state, function, args, result).is_some()
    }
}

/// Shared helper: true iff `result` is an integer reply equal to `expected`.
pub(crate) fn expect_int(result: Option<&Bytes>, expected: i64) -> bool {
    result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(expected))
}
