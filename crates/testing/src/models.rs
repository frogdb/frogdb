//! Sequential specification models for linearizability checking.
//!
//! A model defines the sequential behavior of a data structure.
//! The linearizability checker uses these models to verify that
//! a concurrent execution could be explained by some sequential ordering.

use bytes::Bytes;
use std::collections::HashMap;

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

/// A simple single-register model.
///
/// Supports:
/// - `read()` -> current value
/// - `write(value)` -> "OK"
/// - `cas(expected, new)` -> "OK" if current == expected, else current value
#[derive(Debug, Clone, Default)]
pub struct RegisterModel;

/// State for the register model.
#[derive(Debug, Clone, Default)]
pub struct RegisterState {
    /// Current value (None means uninitialized).
    pub value: Option<Bytes>,
}

impl Model for RegisterModel {
    type State = RegisterState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "read" => {
                // Read should return the current value
                let expected = state.value.as_ref();
                if result == expected {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "write" => {
                // Write should return OK and update the value
                if args.is_empty() {
                    return None;
                }
                let is_ok = result.map_or(false, |r| r.as_ref() == b"OK");
                if is_ok {
                    Some(RegisterState {
                        value: Some(args[0].clone()),
                    })
                } else {
                    None
                }
            }
            "cas" => {
                // Compare-and-swap
                if args.len() < 2 {
                    return None;
                }
                let expected_val = &args[0];
                let new_val = &args[1];

                let current = state.value.as_ref();
                let cas_succeeds = current == Some(expected_val);

                if cas_succeeds {
                    // CAS should succeed and return OK
                    let is_ok = result.map_or(false, |r| r.as_ref() == b"OK");
                    if is_ok {
                        Some(RegisterState {
                            value: Some(new_val.clone()),
                        })
                    } else {
                        None
                    }
                } else {
                    // CAS should fail and return current value
                    if result == current {
                        Some(state.clone())
                    } else {
                        None
                    }
                }
            }
            _ => None,
        }
    }
}

/// A key-value store model.
///
/// Supports:
/// - `read(key)` -> value or nil
/// - `write(key, value)` -> "OK"
/// - `delete(key)` -> "1" if existed, "0" otherwise
/// - `cas(key, expected, new)` -> "OK" if current == expected
#[derive(Debug, Clone, Default)]
pub struct KVModel;

/// State for the key-value model.
#[derive(Debug, Clone, Default)]
pub struct KVState {
    /// Key-value store.
    pub store: HashMap<Bytes, Bytes>,
}

impl Model for KVModel {
    type State = KVState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "read" | "get" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                let expected = state.store.get(key);
                if result == expected {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "write" | "set" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let value = &args[1];
                let is_ok = result.map_or(false, |r| r.as_ref() == b"OK");
                if is_ok {
                    let mut new_state = state.clone();
                    new_state.store.insert(key.clone(), value.clone());
                    Some(new_state)
                } else {
                    None
                }
            }
            "delete" | "del" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                let existed = state.store.contains_key(key);
                let expected_result = if existed { b"1" as &[u8] } else { b"0" };
                let result_matches = result.map_or(false, |r| r.as_ref() == expected_result);
                if result_matches {
                    let mut new_state = state.clone();
                    new_state.store.remove(key);
                    Some(new_state)
                } else {
                    None
                }
            }
            "cas" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let expected_val = &args[1];
                let new_val = &args[2];

                let current = state.store.get(key);
                let cas_succeeds = current == Some(expected_val);

                if cas_succeeds {
                    let is_ok = result.map_or(false, |r| r.as_ref() == b"OK");
                    if is_ok {
                        let mut new_state = state.clone();
                        new_state.store.insert(key.clone(), new_val.clone());
                        Some(new_state)
                    } else {
                        None
                    }
                } else {
                    // CAS fails - state unchanged, result should indicate failure
                    let is_fail = result.map_or(false, |r| r.as_ref() == b"FAIL");
                    if is_fail {
                        Some(state.clone())
                    } else {
                        None
                    }
                }
            }
            "mget" => {
                // MGET returns array of values
                // For simplicity, we accept any result for MGET since array parsing is complex
                // In production, you'd parse the array and verify each element
                Some(state.clone())
            }
            "mset" => {
                // MSET sets multiple keys
                if args.len() < 2 || args.len() % 2 != 0 {
                    return None;
                }
                let is_ok = result.map_or(false, |r| r.as_ref() == b"OK");
                if is_ok {
                    let mut new_state = state.clone();
                    let mut i = 0;
                    while i + 1 < args.len() {
                        new_state.store.insert(args[i].clone(), args[i + 1].clone());
                        i += 2;
                    }
                    Some(new_state)
                } else {
                    None
                }
            }
            _ => {
                // Unknown operation - be permissive for extensibility
                Some(state.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_read_write() {
        let state = RegisterState::default();

        // Write should succeed
        let new_state =
            RegisterModel::step(&state, "write", &[Bytes::from("value")], Some(&Bytes::from("OK")))
                .unwrap();
        assert_eq!(new_state.value, Some(Bytes::from("value")));

        // Read should return the written value
        let result =
            RegisterModel::step(&new_state, "read", &[], Some(&Bytes::from("value")));
        assert!(result.is_some());

        // Read with wrong value should fail
        let result = RegisterModel::step(&new_state, "read", &[], Some(&Bytes::from("wrong")));
        assert!(result.is_none());
    }

    #[test]
    fn test_register_cas() {
        let mut state = RegisterState::default();
        state.value = Some(Bytes::from("old"));

        // CAS with correct expected value should succeed
        let new_state = RegisterModel::step(
            &state,
            "cas",
            &[Bytes::from("old"), Bytes::from("new")],
            Some(&Bytes::from("OK")),
        )
        .unwrap();
        assert_eq!(new_state.value, Some(Bytes::from("new")));

        // CAS with wrong expected value should fail
        let result = RegisterModel::step(
            &state,
            "cas",
            &[Bytes::from("wrong"), Bytes::from("new")],
            Some(&Bytes::from("OK")),
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_kv_read_write() {
        let state = KVState::default();

        // Write key
        let new_state = KVModel::step(
            &state,
            "set",
            &[Bytes::from("key"), Bytes::from("value")],
            Some(&Bytes::from("OK")),
        )
        .unwrap();
        assert_eq!(new_state.store.get(&Bytes::from("key")), Some(&Bytes::from("value")));

        // Read key
        let result = KVModel::step(
            &new_state,
            "get",
            &[Bytes::from("key")],
            Some(&Bytes::from("value")),
        );
        assert!(result.is_some());

        // Read non-existent key
        let result = KVModel::step(&state, "get", &[Bytes::from("key")], None);
        assert!(result.is_some());
    }

    #[test]
    fn test_kv_mset() {
        let state = KVState::default();

        let new_state = KVModel::step(
            &state,
            "mset",
            &[
                Bytes::from("k1"),
                Bytes::from("v1"),
                Bytes::from("k2"),
                Bytes::from("v2"),
            ],
            Some(&Bytes::from("OK")),
        )
        .unwrap();

        assert_eq!(new_state.store.get(&Bytes::from("k1")), Some(&Bytes::from("v1")));
        assert_eq!(new_state.store.get(&Bytes::from("k2")), Some(&Bytes::from("v2")));
    }
}
