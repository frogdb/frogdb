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
                let is_ok = result.is_some_and(|r| r.as_ref() == b"OK");
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
                    let is_ok = result.is_some_and(|r| r.as_ref() == b"OK");
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
                let is_ok = result.is_some_and(|r| r.as_ref() == b"OK");
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
                let result_matches = result.is_some_and(|r| r.as_ref() == expected_result);
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
                    let is_ok = result.is_some_and(|r| r.as_ref() == b"OK");
                    if is_ok {
                        let mut new_state = state.clone();
                        new_state.store.insert(key.clone(), new_val.clone());
                        Some(new_state)
                    } else {
                        None
                    }
                } else {
                    // CAS fails - state unchanged, result should indicate failure
                    let is_fail = result.is_some_and(|r| r.as_ref() == b"FAIL");
                    if is_fail { Some(state.clone()) } else { None }
                }
            }
            "mget" => {
                // MGET returns array of values
                // Result format: pipe-delimited string "value1|nil|value2" (nil for null)
                if args.is_empty() {
                    return None;
                }

                // Build expected result from state
                let expected_values: Vec<String> = args
                    .iter()
                    .map(|key| {
                        state
                            .store
                            .get(key)
                            .map(|v| String::from_utf8_lossy(v).to_string())
                            .unwrap_or_else(|| "nil".to_string())
                    })
                    .collect();
                let expected = expected_values.join("|");

                // If result is None, check if all expected values are nil
                match result {
                    None => {
                        // None result only valid if all keys are missing
                        if expected_values.iter().all(|v| v == "nil") {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                    Some(result_bytes) => {
                        let result_str = String::from_utf8_lossy(result_bytes);
                        if result_str == expected {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            "mset" => {
                // MSET sets multiple keys
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return None;
                }
                let is_ok = result.is_some_and(|r| r.as_ref() == b"OK");
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
            "incr" => {
                // INCR increments an integer value
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                let current = state
                    .store
                    .get(key)
                    .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                    .unwrap_or(0);
                let new_value = current + 1;

                // Result should be the new value
                let result_matches = result.is_some_and(|r| {
                    String::from_utf8_lossy(r).parse::<i64>().ok() == Some(new_value)
                });
                if result_matches {
                    let mut new_state = state.clone();
                    new_state
                        .store
                        .insert(key.clone(), Bytes::from(new_value.to_string()));
                    Some(new_state)
                } else {
                    None
                }
            }
            "exec" => {
                // EXEC executes a transaction atomically
                // Args format: [num_cmds, cmd1_name, cmd1_num_args, cmd1_args..., cmd2_name, ...]
                // Result format: pipe-delimited results "OK|value1|nil|5"
                if args.is_empty() {
                    return None;
                }

                let num_cmds: usize = String::from_utf8_lossy(&args[0]).parse().unwrap_or(0);
                if num_cmds == 0 {
                    // Empty transaction returns empty array
                    let is_empty = result.is_none_or(|r| r.is_empty() || r.as_ref() == b"");
                    return if is_empty { Some(state.clone()) } else { None };
                }

                // Parse commands from args
                let mut idx = 1;
                let mut commands: Vec<(String, Vec<Bytes>)> = Vec::new();
                for _ in 0..num_cmds {
                    if idx >= args.len() {
                        return None;
                    }
                    let cmd_name = String::from_utf8_lossy(&args[idx]).to_lowercase();
                    idx += 1;

                    if idx >= args.len() {
                        return None;
                    }
                    let cmd_num_args: usize =
                        String::from_utf8_lossy(&args[idx]).parse().unwrap_or(0);
                    idx += 1;

                    let mut cmd_args = Vec::new();
                    for _ in 0..cmd_num_args {
                        if idx >= args.len() {
                            return None;
                        }
                        cmd_args.push(args[idx].clone());
                        idx += 1;
                    }
                    commands.push((cmd_name, cmd_args));
                }

                // Execute all commands atomically on state, collecting expected results
                let mut current_state = state.clone();
                let mut expected_results: Vec<String> = Vec::new();

                for (cmd, cmd_args) in &commands {
                    match cmd.as_str() {
                        "set" => {
                            if cmd_args.len() >= 2 {
                                current_state
                                    .store
                                    .insert(cmd_args[0].clone(), cmd_args[1].clone());
                                expected_results.push("OK".to_string());
                            } else {
                                return None;
                            }
                        }
                        "get" => {
                            if !cmd_args.is_empty() {
                                let value = current_state
                                    .store
                                    .get(&cmd_args[0])
                                    .map(|v| String::from_utf8_lossy(v).to_string())
                                    .unwrap_or_else(|| "nil".to_string());
                                expected_results.push(value);
                            } else {
                                return None;
                            }
                        }
                        "incr" => {
                            if !cmd_args.is_empty() {
                                let key = &cmd_args[0];
                                let current = current_state
                                    .store
                                    .get(key)
                                    .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                                    .unwrap_or(0);
                                let new_value = current + 1;
                                current_state
                                    .store
                                    .insert(key.clone(), Bytes::from(new_value.to_string()));
                                expected_results.push(new_value.to_string());
                            } else {
                                return None;
                            }
                        }
                        "del" | "delete" => {
                            if !cmd_args.is_empty() {
                                let existed = current_state.store.remove(&cmd_args[0]).is_some();
                                expected_results.push(if existed { "1" } else { "0" }.to_string());
                            } else {
                                return None;
                            }
                        }
                        _ => {
                            // Unknown command in transaction
                            return None;
                        }
                    }
                }

                let expected = expected_results.join("|");

                // Verify result matches expected
                match result {
                    None => None, // EXEC should always return something
                    Some(result_bytes) => {
                        let result_str = String::from_utf8_lossy(result_bytes);
                        if result_str == expected {
                            Some(current_state)
                        } else {
                            None
                        }
                    }
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
        let new_state = RegisterModel::step(
            &state,
            "write",
            &[Bytes::from("value")],
            Some(&Bytes::from("OK")),
        )
        .unwrap();
        assert_eq!(new_state.value, Some(Bytes::from("value")));

        // Read should return the written value
        let result = RegisterModel::step(&new_state, "read", &[], Some(&Bytes::from("value")));
        assert!(result.is_some());

        // Read with wrong value should fail
        let result = RegisterModel::step(&new_state, "read", &[], Some(&Bytes::from("wrong")));
        assert!(result.is_none());
    }

    #[test]
    fn test_register_cas() {
        let state = RegisterState {
            value: Some(Bytes::from("old")),
        };

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
        assert_eq!(
            new_state.store.get(&Bytes::from("key")),
            Some(&Bytes::from("value"))
        );

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

        assert_eq!(
            new_state.store.get(&Bytes::from("k1")),
            Some(&Bytes::from("v1"))
        );
        assert_eq!(
            new_state.store.get(&Bytes::from("k2")),
            Some(&Bytes::from("v2"))
        );
    }

    #[test]
    fn test_kv_mget_validates_result() {
        let mut state = KVState::default();
        state.store.insert(Bytes::from("k1"), Bytes::from("v1"));
        state.store.insert(Bytes::from("k2"), Bytes::from("v2"));

        // Correct result
        let result = KVModel::step(
            &state,
            "mget",
            &[Bytes::from("k1"), Bytes::from("k2")],
            Some(&Bytes::from("v1|v2")),
        );
        assert!(result.is_some());

        // With missing key
        let result = KVModel::step(
            &state,
            "mget",
            &[Bytes::from("k1"), Bytes::from("missing")],
            Some(&Bytes::from("v1|nil")),
        );
        assert!(result.is_some());

        // Wrong result should fail
        let result = KVModel::step(
            &state,
            "mget",
            &[Bytes::from("k1"), Bytes::from("k2")],
            Some(&Bytes::from("v1|wrong")),
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_kv_incr() {
        let mut state = KVState::default();
        state.store.insert(Bytes::from("counter"), Bytes::from("5"));

        // INCR existing key
        let new_state = KVModel::step(
            &state,
            "incr",
            &[Bytes::from("counter")],
            Some(&Bytes::from("6")),
        )
        .unwrap();
        assert_eq!(
            new_state.store.get(&Bytes::from("counter")),
            Some(&Bytes::from("6"))
        );

        // INCR non-existent key (starts from 0)
        let state = KVState::default();
        let new_state = KVModel::step(
            &state,
            "incr",
            &[Bytes::from("newkey")],
            Some(&Bytes::from("1")),
        )
        .unwrap();
        assert_eq!(
            new_state.store.get(&Bytes::from("newkey")),
            Some(&Bytes::from("1"))
        );
    }

    #[test]
    fn test_kv_exec_transaction() {
        let state = KVState::default();

        // Transaction: SET counter 0, INCR counter, INCR counter
        // Args: [num_cmds, cmd1_name, cmd1_num_args, cmd1_args..., ...]
        // = [3, "set", 2, "counter", "0", "incr", 1, "counter", "incr", 1, "counter"]
        let new_state = KVModel::step(
            &state,
            "exec",
            &[
                Bytes::from("3"),       // num_cmds
                Bytes::from("set"),     // cmd1_name
                Bytes::from("2"),       // cmd1_num_args
                Bytes::from("counter"), // cmd1_arg1
                Bytes::from("0"),       // cmd1_arg2
                Bytes::from("incr"),    // cmd2_name
                Bytes::from("1"),       // cmd2_num_args
                Bytes::from("counter"), // cmd2_arg1
                Bytes::from("incr"),    // cmd3_name
                Bytes::from("1"),       // cmd3_num_args
                Bytes::from("counter"), // cmd3_arg1
            ],
            Some(&Bytes::from("OK|1|2")), // SET returns OK, first INCR returns 1, second INCR returns 2
        )
        .unwrap();

        assert_eq!(
            new_state.store.get(&Bytes::from("counter")),
            Some(&Bytes::from("2"))
        );
    }

    #[test]
    fn test_kv_exec_read_own_writes() {
        let state = KVState::default();

        // Transaction: SET key val, GET key
        let new_state = KVModel::step(
            &state,
            "exec",
            &[
                Bytes::from("2"),   // num_cmds
                Bytes::from("set"), // cmd1_name
                Bytes::from("2"),   // cmd1_num_args
                Bytes::from("key"), // cmd1_arg1
                Bytes::from("val"), // cmd1_arg2
                Bytes::from("get"), // cmd2_name
                Bytes::from("1"),   // cmd2_num_args
                Bytes::from("key"), // cmd2_arg1
            ],
            Some(&Bytes::from("OK|val")), // SET returns OK, GET returns val
        )
        .unwrap();

        assert_eq!(
            new_state.store.get(&Bytes::from("key")),
            Some(&Bytes::from("val"))
        );
    }
}
