//! Single-register sequential model.

use super::Model;
use bytes::Bytes;

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
}
