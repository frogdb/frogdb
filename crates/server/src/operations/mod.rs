//! Multi-phase operation state machines.
//!
//! This module provides abstractions for operations that require multiple
//! async phases (like MIGRATE and COPY) where each phase has clear inputs,
//! outputs, and error handling.
//!
//! # Why State Machines?
//!
//! Multi-phase operations often have scattered error handling and unclear
//! control flow. By modeling them as explicit state machines:
//! - Each phase has clear inputs/outputs
//! - Error handling becomes obvious (each phase returns success or failure)
//! - Testing is easier (phases can be tested in isolation)
//! - The flow is documented by the state types themselves
//!
//! # Example
//!
//! ```rust,ignore
//! use frogdb_server::operations::{PhaseResult, Operation};
//!
//! enum MyState {
//!     Phase1,
//!     Phase2 { data: Vec<u8> },
//!     Phase3 { processed: String },
//! }
//!
//! struct MyOperation;
//!
//! impl Operation for MyOperation {
//!     type State = MyState;
//!     type Output = String;
//!     type Error = Response;
//!
//!     async fn run_phase(&self, state: Self::State) -> PhaseResult<Self::Output, Self::State, Self::Error> {
//!         match state {
//!             MyState::Phase1 => {
//!                 // Do phase 1 work
//!                 PhaseResult::Continue(MyState::Phase2 { data: vec![] })
//!             }
//!             MyState::Phase2 { data } => {
//!                 // Do phase 2 work
//!                 PhaseResult::Continue(MyState::Phase3 { processed: String::new() })
//!             }
//!             MyState::Phase3 { processed } => {
//!                 PhaseResult::Complete(processed)
//!             }
//!         }
//!     }
//! }
//! ```

pub mod migrate;

use std::future::Future;

/// Result of running a single phase of an operation.
///
/// This enum explicitly models the three possible outcomes of a phase:
/// - Continue to the next state
/// - Complete with a successful output
/// - Fail with an error
#[derive(Debug)]
pub enum PhaseResult<T, S, E> {
    /// Phase completed, continue to next state.
    Continue(S),
    /// Operation completed successfully with output.
    Complete(T),
    /// Operation failed with error.
    Failed(E),
}

impl<T, S, E> PhaseResult<T, S, E> {
    /// Returns true if this is a Continue result.
    pub fn is_continue(&self) -> bool {
        matches!(self, PhaseResult::Continue(_))
    }

    /// Returns true if this is a Complete result.
    pub fn is_complete(&self) -> bool {
        matches!(self, PhaseResult::Complete(_))
    }

    /// Returns true if this is a Failed result.
    pub fn is_failed(&self) -> bool {
        matches!(self, PhaseResult::Failed(_))
    }

    /// Map the success type.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> PhaseResult<U, S, E> {
        match self {
            PhaseResult::Continue(s) => PhaseResult::Continue(s),
            PhaseResult::Complete(t) => PhaseResult::Complete(f(t)),
            PhaseResult::Failed(e) => PhaseResult::Failed(e),
        }
    }

    /// Map the error type.
    pub fn map_err<U, F: FnOnce(E) -> U>(self, f: F) -> PhaseResult<T, S, U> {
        match self {
            PhaseResult::Continue(s) => PhaseResult::Continue(s),
            PhaseResult::Complete(t) => PhaseResult::Complete(t),
            PhaseResult::Failed(e) => PhaseResult::Failed(f(e)),
        }
    }
}

/// Trait for multi-phase operations.
///
/// Implement this trait to define an operation that proceeds through
/// multiple states before completing or failing.
pub trait Operation {
    /// The state type that tracks progress through phases.
    type State;
    /// The successful output type.
    type Output;
    /// The error type.
    type Error;

    /// Run a single phase of the operation.
    ///
    /// This method takes the current state and returns the result:
    /// - `PhaseResult::Continue(next_state)` to proceed to the next phase
    /// - `PhaseResult::Complete(output)` when the operation succeeds
    /// - `PhaseResult::Failed(error)` when the operation fails
    fn run_phase(
        &self,
        state: Self::State,
    ) -> impl Future<Output = PhaseResult<Self::Output, Self::State, Self::Error>> + Send;
}

/// Run an operation to completion.
///
/// This helper function repeatedly calls `run_phase` until the operation
/// either completes successfully or fails.
///
/// # Example
///
/// ```rust,ignore
/// let op = MyMigration::new(deps);
/// let initial_state = MigrateState::ParseArgs { args };
///
/// match run_to_completion(&op, initial_state).await {
///     Ok(output) => Response::Simple("OK".into()),
///     Err(error) => error,
/// }
/// ```
pub async fn run_to_completion<O: Operation>(
    op: &O,
    initial_state: O::State,
) -> Result<O::Output, O::Error> {
    let mut state = initial_state;

    loop {
        match op.run_phase(state).await {
            PhaseResult::Continue(next_state) => {
                state = next_state;
            }
            PhaseResult::Complete(output) => {
                return Ok(output);
            }
            PhaseResult::Failed(error) => {
                return Err(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum TestState {
        Start,
        Middle(i32),
        End,
    }

    struct TestOp;

    impl Operation for TestOp {
        type State = TestState;
        type Output = String;
        type Error = String;

        async fn run_phase(
            &self,
            state: Self::State,
        ) -> PhaseResult<Self::Output, Self::State, Self::Error> {
            match state {
                TestState::Start => PhaseResult::Continue(TestState::Middle(42)),
                TestState::Middle(n) if n > 0 => {
                    PhaseResult::Continue(TestState::End)
                }
                TestState::Middle(_) => PhaseResult::Failed("negative".to_string()),
                TestState::End => PhaseResult::Complete("done".to_string()),
            }
        }
    }

    #[tokio::test]
    async fn test_run_to_completion_success() {
        let op = TestOp;
        let result = run_to_completion(&op, TestState::Start).await;
        assert_eq!(result, Ok("done".to_string()));
    }

    #[tokio::test]
    async fn test_phase_result_helpers() {
        let continue_result: PhaseResult<i32, &str, ()> = PhaseResult::Continue("next");
        assert!(continue_result.is_continue());
        assert!(!continue_result.is_complete());
        assert!(!continue_result.is_failed());

        let complete_result: PhaseResult<i32, (), ()> = PhaseResult::Complete(42);
        assert!(complete_result.is_complete());

        let failed_result: PhaseResult<(), (), &str> = PhaseResult::Failed("error");
        assert!(failed_result.is_failed());
    }

    #[tokio::test]
    async fn test_phase_result_map() {
        let result: PhaseResult<i32, (), ()> = PhaseResult::Complete(5);
        let mapped = result.map(|n| n * 2);
        assert!(matches!(mapped, PhaseResult::Complete(10)));
    }

    #[tokio::test]
    async fn test_phase_result_map_err() {
        let result: PhaseResult<(), (), i32> = PhaseResult::Failed(5);
        let mapped = result.map_err(|n| n * 2);
        assert!(matches!(mapped, PhaseResult::Failed(10)));
    }
}
