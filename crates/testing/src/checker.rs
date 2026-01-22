//! Linearizability checker using the WGL (Wing-Gong-Larus) algorithm.
//!
//! This module implements a linearizability checker that verifies whether
//! a concurrent execution history can be linearized according to a given
//! sequential specification model.
//!
//! # Algorithm Overview
//!
//! The WGL algorithm works by:
//! 1. Building a graph of all possible linearization orders
//! 2. Exploring the graph via backtracking search
//! 3. For each candidate linearization, checking if it satisfies the model
//!
//! The algorithm is NP-complete in general, but works well for small histories
//! typical of testing scenarios.

use crate::history::{CompletedOperation, History};
use crate::models::Model;
use std::collections::HashSet;

/// Result of linearizability checking.
#[derive(Debug, Clone)]
pub struct LinearizabilityResult {
    /// Whether the history is linearizable.
    pub is_linearizable: bool,
    /// A valid linearization order (if linearizable).
    pub linearization: Option<Vec<u64>>,
    /// Operations that could not be linearized (if not linearizable).
    pub problematic_ops: Vec<u64>,
    /// Number of states explored during checking.
    pub states_explored: u64,
}

/// Check if a history is linearizable according to a model.
///
/// This uses a backtracking search to find a valid linearization.
/// For small histories (< 20 operations), this is typically fast enough.
pub fn check_linearizability<M: Model>(history: &History) -> LinearizabilityResult {
    if !history.is_complete() {
        // Handle incomplete histories by treating pending operations as having
        // unknown results (which could be anything)
        // For now, we require complete histories
        return LinearizabilityResult {
            is_linearizable: false,
            linearization: None,
            problematic_ops: history.pending_ops(),
            states_explored: 0,
        };
    }

    let operations = history.completed_operations();
    if operations.is_empty() {
        return LinearizabilityResult {
            is_linearizable: true,
            linearization: Some(vec![]),
            problematic_ops: vec![],
            states_explored: 1,
        };
    }

    let mut checker = Checker::<M>::new(operations);
    checker.check()
}

/// Internal checker state.
struct Checker<M: Model> {
    /// Operations to linearize.
    operations: Vec<CompletedOperation>,
    /// Current linearization order.
    linearization: Vec<u64>,
    /// Operations that have been linearized.
    linearized: HashSet<u64>,
    /// Current model state.
    state: M::State,
    /// Number of states explored.
    states_explored: u64,
}

impl<M: Model> Checker<M> {
    fn new(operations: Vec<CompletedOperation>) -> Self {
        Self {
            operations,
            linearization: Vec::new(),
            linearized: HashSet::new(),
            state: M::init(),
            states_explored: 0,
        }
    }

    fn check(&mut self) -> LinearizabilityResult {
        if self.search() {
            LinearizabilityResult {
                is_linearizable: true,
                linearization: Some(self.linearization.clone()),
                problematic_ops: vec![],
                states_explored: self.states_explored,
            }
        } else {
            // Find problematic operations (those that couldn't be linearized)
            let linearized_set: HashSet<_> = self.linearization.iter().copied().collect();
            let problematic: Vec<_> = self
                .operations
                .iter()
                .filter(|op| !linearized_set.contains(&op.id))
                .map(|op| op.id)
                .collect();

            LinearizabilityResult {
                is_linearizable: false,
                linearization: None,
                problematic_ops: problematic,
                states_explored: self.states_explored,
            }
        }
    }

    /// Backtracking search for a valid linearization.
    fn search(&mut self) -> bool {
        self.states_explored += 1;

        // Base case: all operations linearized
        if self.linearization.len() == self.operations.len() {
            return true;
        }

        // Find operations that can be linearized next (collect indices to avoid borrow issues)
        let candidate_indices = self.get_candidate_indices();

        for idx in candidate_indices {
            let op = &self.operations[idx];
            let op_id = op.id;
            let function = op.function.clone();
            let args = op.args.clone();
            let result = op.result.clone();

            // Try applying this operation to the model
            if let Some(new_state) = M::step(
                &self.state,
                &function,
                &args,
                result.as_ref(),
            ) {
                // Save state for backtracking
                let old_state = self.state.clone();

                // Apply the operation
                self.state = new_state;
                self.linearization.push(op_id);
                self.linearized.insert(op_id);

                // Recurse
                if self.search() {
                    return true;
                }

                // Backtrack
                self.state = old_state;
                self.linearization.pop();
                self.linearized.remove(&op_id);
            }
        }

        false
    }

    /// Get indices of operations that can be linearized next.
    fn get_candidate_indices(&self) -> Vec<usize> {
        self.operations
            .iter()
            .enumerate()
            .filter(|(_, op)| {
                // Not already linearized
                if self.linearized.contains(&op.id) {
                    return false;
                }

                // All operations that must precede this one are linearized
                // An operation A must precede B if A.return_time < B.invoke_time
                for other in &self.operations {
                    if other.id != op.id
                        && !self.linearized.contains(&other.id)
                        && other.could_precede(op)
                    {
                        return false;
                    }
                }

                true
            })
            .map(|(idx, _)| idx)
            .collect()
    }
}

/// Check linearizability with a custom maximum number of states to explore.
///
/// This is useful for large histories where you want to limit the search.
/// If the limit is reached without finding a linearization or proving
/// non-linearizability, the result will indicate unknown.
pub fn check_linearizability_bounded<M: Model>(
    history: &History,
    max_states: u64,
) -> LinearizabilityResult {
    if !history.is_complete() {
        return LinearizabilityResult {
            is_linearizable: false,
            linearization: None,
            problematic_ops: history.pending_ops(),
            states_explored: 0,
        };
    }

    let operations = history.completed_operations();
    if operations.is_empty() {
        return LinearizabilityResult {
            is_linearizable: true,
            linearization: Some(vec![]),
            problematic_ops: vec![],
            states_explored: 1,
        };
    }

    let mut checker = BoundedChecker::<M>::new(operations, max_states);
    checker.check()
}

/// Bounded checker that limits exploration.
struct BoundedChecker<M: Model> {
    inner: Checker<M>,
    max_states: u64,
    limit_reached: bool,
}

impl<M: Model> BoundedChecker<M> {
    fn new(operations: Vec<CompletedOperation>, max_states: u64) -> Self {
        Self {
            inner: Checker::new(operations),
            max_states,
            limit_reached: false,
        }
    }

    fn check(&mut self) -> LinearizabilityResult {
        if self.search() {
            LinearizabilityResult {
                is_linearizable: true,
                linearization: Some(self.inner.linearization.clone()),
                problematic_ops: vec![],
                states_explored: self.inner.states_explored,
            }
        } else if self.limit_reached {
            // Inconclusive - limit reached
            LinearizabilityResult {
                is_linearizable: false, // Unknown, but we report false to be conservative
                linearization: None,
                problematic_ops: vec![], // Can't identify specific problems
                states_explored: self.inner.states_explored,
            }
        } else {
            let linearized_set: HashSet<_> = self.inner.linearization.iter().copied().collect();
            let problematic: Vec<_> = self
                .inner
                .operations
                .iter()
                .filter(|op| !linearized_set.contains(&op.id))
                .map(|op| op.id)
                .collect();

            LinearizabilityResult {
                is_linearizable: false,
                linearization: None,
                problematic_ops: problematic,
                states_explored: self.inner.states_explored,
            }
        }
    }

    fn search(&mut self) -> bool {
        if self.inner.states_explored >= self.max_states {
            self.limit_reached = true;
            return false;
        }

        self.inner.states_explored += 1;

        if self.inner.linearization.len() == self.inner.operations.len() {
            return true;
        }

        let candidate_indices = self.inner.get_candidate_indices();

        for idx in candidate_indices {
            let op = &self.inner.operations[idx];
            let op_id = op.id;
            let function = op.function.clone();
            let args = op.args.clone();
            let result = op.result.clone();

            if let Some(new_state) = M::step(
                &self.inner.state,
                &function,
                &args,
                result.as_ref(),
            ) {
                let old_state = self.inner.state.clone();

                self.inner.state = new_state;
                self.inner.linearization.push(op_id);
                self.inner.linearized.insert(op_id);

                if self.search() {
                    return true;
                }

                self.inner.state = old_state;
                self.inner.linearization.pop();
                self.inner.linearized.remove(&op_id);
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{KVModel, RegisterModel};
    use bytes::Bytes;

    #[test]
    fn test_empty_history() {
        let history = History::new();
        let result = check_linearizability::<RegisterModel>(&history);
        assert!(result.is_linearizable);
    }

    #[test]
    fn test_single_write_read() {
        let mut history = History::new();

        let op1 = history.invoke(1, "write", vec![Bytes::from("value")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let op2 = history.invoke(1, "read", vec![]);
        history.respond(op2, Some(Bytes::from("value")));

        let result = check_linearizability::<RegisterModel>(&history);
        assert!(result.is_linearizable);
        assert_eq!(result.linearization.unwrap().len(), 2);
    }

    #[test]
    fn test_non_linearizable_read() {
        let mut history = History::new();

        // Read returns a value that was never written
        let op1 = history.invoke(1, "read", vec![]);
        history.respond(op1, Some(Bytes::from("ghost_value")));

        let result = check_linearizability::<RegisterModel>(&history);
        assert!(!result.is_linearizable);
    }

    #[test]
    fn test_concurrent_writes() {
        let mut history = History::new();

        // Two concurrent writes
        let op1 = history.invoke(1, "write", vec![Bytes::from("a")]);
        let op2 = history.invoke(2, "write", vec![Bytes::from("b")]);
        history.respond(op1, Some(Bytes::from("OK")));
        history.respond(op2, Some(Bytes::from("OK")));

        // Read returns one of the written values
        let op3 = history.invoke(1, "read", vec![]);
        history.respond(op3, Some(Bytes::from("b"))); // Could be "a" or "b"

        let result = check_linearizability::<RegisterModel>(&history);
        assert!(result.is_linearizable);
    }

    #[test]
    fn test_kv_linearizable() {
        let mut history = History::new();

        let op1 = history.invoke(1, "set", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let op2 = history.invoke(2, "get", vec![Bytes::from("x")]);
        history.respond(op2, Some(Bytes::from("1")));

        let result = check_linearizability::<KVModel>(&history);
        assert!(result.is_linearizable);
    }

    #[test]
    fn test_kv_non_linearizable() {
        let mut history = History::new();

        // GET before SET should return nil
        let op1 = history.invoke(1, "get", vec![Bytes::from("x")]);
        history.respond(op1, Some(Bytes::from("value"))); // But it returns a value!

        let result = check_linearizability::<KVModel>(&history);
        assert!(!result.is_linearizable);
    }

    #[test]
    fn test_bounded_checker() {
        let mut history = History::new();

        let op1 = history.invoke(1, "set", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let result = check_linearizability_bounded::<KVModel>(&history, 100);
        assert!(result.is_linearizable);
    }
}
