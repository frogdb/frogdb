//! Key-value store sequential model (strings + MULTI/EXEC).

use super::Model;
use bytes::Bytes;
use std::collections::HashMap;

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
                    // A nil EXEC is a WATCH-aborted transaction under optimistic
                    // concurrency: it is legal and leaves state unchanged. (The
                    // "should-have-aborted" direction — an EXEC that committed
                    // despite a watched-key write — is checked separately by
                    // `check_watch_no_false_negative`.)
                    None => Some(state.clone()),
                    Some(result_bytes) => {
                        // A CROSSSLOT/EXECABORT error EXEC (recorder marker
                        // "ERR:...") is a rejected transaction: also a legal
                        // no-op that leaves state unchanged.
                        if crate::partition::is_errored_exec_result(result_bytes) {
                            return Some(state.clone());
                        }
                        let result_str = String::from_utf8_lossy(result_bytes);
                        if result_str == expected {
                            Some(current_state)
                        } else {
                            None
                        }
                    }
                }
            }
            "script_getset" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let old = state.store.get(key).cloned();
                if result == old.as_ref() {
                    let mut new = state.clone();
                    new.store.insert(key.clone(), args[1].clone());
                    Some(new)
                } else {
                    None
                }
            }
            "script_cincr" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                if let Some(cur) = state.store.get(key) {
                    let n = String::from_utf8_lossy(cur)
                        .parse::<i64>()
                        .ok()?
                        .checked_add(1)?;
                    if result
                        .is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(n))
                    {
                        let mut new = state.clone();
                        new.store.insert(key.clone(), Bytes::from(n.to_string()));
                        Some(new)
                    } else {
                        None
                    }
                } else if result.is_some_and(|r| r.as_ref() == b"-1") {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "script_setnx_get" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                if !new.store.contains_key(key) {
                    new.store.insert(key.clone(), args[1].clone());
                }
                let cur = new.store.get(key).cloned();
                if result == cur.as_ref() {
                    Some(new)
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
    use bytes::Bytes;

    #[test]
    fn test_kv_exec_errored_is_legal_and_unchanged() {
        // A CROSSSLOT/EXECABORT EXEC lands in the history as Some("ERR:...").
        // It is a rejected transaction: legal, state unchanged (like a nil abort).
        let mut state = KVState::default();
        state.store.insert(Bytes::from("k"), Bytes::from("orig"));
        let new_state = KVModel::step(
            &state,
            "exec",
            &[
                Bytes::from("1"),
                Bytes::from("set"),
                Bytes::from("2"),
                Bytes::from("k"),
                Bytes::from("new"),
            ],
            Some(&Bytes::from(
                "ERR:EXECABORT Transaction discarded because of previous errors.",
            )),
        )
        .expect("errored EXEC must be accepted as a no-op");
        assert_eq!(
            new_state.store.get(&Bytes::from("k")),
            Some(&Bytes::from("orig"))
        );
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
    fn test_kv_exec_nil_abort_is_legal_and_unchanged() {
        // A WATCH-aborted EXEC returns nil (result None). It is a legal outcome
        // that must leave state unchanged, not a non-linearizable rejection.
        let mut state = KVState::default();
        state.store.insert(Bytes::from("k"), Bytes::from("orig"));

        let new_state = KVModel::step(
            &state,
            "exec",
            &[
                Bytes::from("1"),   // num_cmds
                Bytes::from("set"), // cmd1_name
                Bytes::from("2"),   // cmd1_num_args
                Bytes::from("k"),   // cmd1_arg1
                Bytes::from("new"), // cmd1_arg2
            ],
            None, // aborted → nil
        )
        .expect("nil abort must be accepted");

        // State is unchanged: the aborted SET did not apply.
        assert_eq!(
            new_state.store.get(&Bytes::from("k")),
            Some(&Bytes::from("orig"))
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

    #[test]
    fn script_getset_returns_old_sets_new() {
        let mut s = KVState::default();
        s.store.insert(Bytes::from("k"), Bytes::from("old"));
        let new_state = KVModel::step(
            &s,
            "script_getset",
            &[Bytes::from("k"), Bytes::from("new")],
            Some(&Bytes::from("old")),
        )
        .unwrap();
        assert_eq!(
            new_state.store.get(&Bytes::from("k")),
            Some(&Bytes::from("new"))
        );
        // Absent key -> old is nil.
        let s2 = KVState::default();
        assert!(
            KVModel::step(
                &s2,
                "script_getset",
                &[Bytes::from("x"), Bytes::from("v")],
                None
            )
            .is_some()
        );
        // Wrong recorded "old" value is rejected (strict, not the permissive
        // `_ => Some(state.clone())` fallthrough).
        assert!(
            KVModel::step(
                &s,
                "script_getset",
                &[Bytes::from("k"), Bytes::from("new")],
                Some(&Bytes::from("wrong")),
            )
            .is_none()
        );
    }

    #[test]
    fn script_cincr_only_when_present() {
        let mut s = KVState::default();
        s.store.insert(Bytes::from("k"), Bytes::from("5"));
        let new_state = KVModel::step(
            &s,
            "script_cincr",
            &[Bytes::from("k")],
            Some(&Bytes::from("6")),
        )
        .unwrap();
        assert_eq!(
            new_state.store.get(&Bytes::from("k")),
            Some(&Bytes::from("6"))
        );
        // Wrong recorded result is rejected.
        assert!(
            KVModel::step(
                &s,
                "script_cincr",
                &[Bytes::from("k")],
                Some(&Bytes::from("7"))
            )
            .is_none()
        );
        // Absent -> -1, unchanged.
        let s2 = KVState::default();
        let s2 = KVModel::step(
            &s2,
            "script_cincr",
            &[Bytes::from("x")],
            Some(&Bytes::from("-1")),
        )
        .unwrap();
        assert!(!s2.store.contains_key(&Bytes::from("x")));
        // Absent key claiming an incremented result is rejected.
        let s3 = KVState::default();
        assert!(
            KVModel::step(
                &s3,
                "script_cincr",
                &[Bytes::from("x")],
                Some(&Bytes::from("1"))
            )
            .is_none()
        );
    }

    #[test]
    fn script_cincr_non_integer_rejected() {
        // A stored non-integer value cannot be incremented: the parse fails
        // closed and the step is rejected regardless of the recorded result.
        let mut s = KVState::default();
        s.store.insert(Bytes::from("k"), Bytes::from("abc"));
        assert!(
            KVModel::step(
                &s,
                "script_cincr",
                &[Bytes::from("k")],
                Some(&Bytes::from("1")),
            )
            .is_none()
        );
    }

    #[test]
    fn script_cincr_overflow_rejected() {
        // i64::MAX + 1 must not silently wrap; checked_add fails closed.
        let mut s = KVState::default();
        s.store
            .insert(Bytes::from("k"), Bytes::from(i64::MAX.to_string()));
        assert!(
            KVModel::step(
                &s,
                "script_cincr",
                &[Bytes::from("k")],
                Some(&Bytes::from(i64::MIN.to_string())),
            )
            .is_none()
        );
    }

    #[test]
    fn script_setnx_get_sets_when_absent_noop_when_present() {
        // Absent key: SETNX applies, GET reflects the new value.
        let s = KVState::default();
        let new_state = KVModel::step(
            &s,
            "script_setnx_get",
            &[Bytes::from("k"), Bytes::from("v")],
            Some(&Bytes::from("v")),
        )
        .unwrap();
        assert_eq!(
            new_state.store.get(&Bytes::from("k")),
            Some(&Bytes::from("v"))
        );

        // Present key: SETNX is a no-op, GET reflects the existing value.
        let mut s2 = KVState::default();
        s2.store.insert(Bytes::from("k"), Bytes::from("old"));
        let new_state2 = KVModel::step(
            &s2,
            "script_setnx_get",
            &[Bytes::from("k"), Bytes::from("new")],
            Some(&Bytes::from("old")),
        )
        .unwrap();
        assert_eq!(
            new_state2.store.get(&Bytes::from("k")),
            Some(&Bytes::from("old"))
        );

        // Recorded result claiming the overwrite happened is rejected.
        assert!(
            KVModel::step(
                &s2,
                "script_setnx_get",
                &[Bytes::from("k"), Bytes::from("new")],
                Some(&Bytes::from("new")),
            )
            .is_none()
        );
    }
}
