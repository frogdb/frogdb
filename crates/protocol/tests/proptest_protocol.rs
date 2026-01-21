//! Property-based tests for RESP protocol parsing.
//!
//! Tests that:
//! - ParsedCommand::try_from never panics on arbitrary frames
//! - Edge cases like empty arrays, null frames, etc. are handled

use bytes::Bytes;
use proptest::prelude::*;
use std::convert::TryFrom;

use frogdb_protocol::{BytesFrame, ParsedCommand, ProtocolError};

/// Configuration for proptest - run more cases than default for fuzzing
fn config() -> ProptestConfig {
    ProptestConfig::with_cases(1000)
}

/// Generate an arbitrary BytesFrame.
fn arb_frame() -> impl Strategy<Value = BytesFrame> {
    let leaf = prop_oneof![
        // BulkString with arbitrary bytes
        prop::collection::vec(any::<u8>(), 0..200)
            .prop_map(|v| BytesFrame::BulkString(Bytes::from(v))),
        // SimpleString with arbitrary bytes
        prop::collection::vec(any::<u8>(), 0..100)
            .prop_map(|v| BytesFrame::SimpleString(Bytes::from(v))),
        // Integer
        any::<i64>().prop_map(BytesFrame::Integer),
        // Null
        Just(BytesFrame::Null),
    ];

    leaf.prop_recursive(
        4,   // depth
        64,  // max nodes
        10,  // items per collection
        |inner| {
            prop::collection::vec(inner, 0..10)
                .prop_map(BytesFrame::Array)
        },
    )
}

/// Generate a command-like frame (array of bulk strings).
fn arb_command_frame() -> impl Strategy<Value = BytesFrame> {
    prop::collection::vec(
        prop::collection::vec(any::<u8>(), 0..100),
        0..20,
    )
    .prop_map(|args| {
        BytesFrame::Array(
            args.into_iter()
                .map(|a| BytesFrame::BulkString(Bytes::from(a)))
                .collect(),
        )
    })
}

proptest! {
    #![proptest_config(config())]

    /// ParsedCommand::try_from should never panic on arbitrary frames.
    #[test]
    fn parse_command_never_panics(frame in arb_frame()) {
        let _: Result<ParsedCommand, ProtocolError> = ParsedCommand::try_from(frame);
    }

    /// Command-like frames should parse without panic.
    #[test]
    fn parse_command_frame_never_panics(frame in arb_command_frame()) {
        let _: Result<ParsedCommand, ProtocolError> = ParsedCommand::try_from(frame);
    }

    /// Empty array should return EmptyCommand error.
    #[test]
    fn empty_array_returns_error(_dummy: bool) {
        let frame = BytesFrame::Array(vec![]);
        let result = ParsedCommand::try_from(frame);
        prop_assert!(matches!(result, Err(ProtocolError::EmptyCommand)));
    }

    /// Non-array frame should return ExpectedArray error.
    #[test]
    fn non_array_returns_error(data: Vec<u8>) {
        let frame = BytesFrame::BulkString(Bytes::from(data));
        let result = ParsedCommand::try_from(frame);
        prop_assert!(matches!(result, Err(ProtocolError::ExpectedArray)));
    }

    /// SimpleString as non-array should return ExpectedArray error.
    #[test]
    fn simple_string_returns_error(data: Vec<u8>) {
        let frame = BytesFrame::SimpleString(Bytes::from(data));
        let result = ParsedCommand::try_from(frame);
        prop_assert!(matches!(result, Err(ProtocolError::ExpectedArray)));
    }

    /// Integer as non-array should return ExpectedArray error.
    #[test]
    fn integer_returns_error(i: i64) {
        let frame = BytesFrame::Integer(i);
        let result = ParsedCommand::try_from(frame);
        prop_assert!(matches!(result, Err(ProtocolError::ExpectedArray)));
    }

    /// Null frame should return ExpectedArray error.
    #[test]
    fn null_returns_error(_dummy: bool) {
        let frame = BytesFrame::Null;
        let result = ParsedCommand::try_from(frame);
        prop_assert!(matches!(result, Err(ProtocolError::ExpectedArray)));
    }

    /// Valid command with arbitrary arguments should parse.
    #[test]
    fn valid_command_parses(
        name in "[A-Z]{1,20}",
        args in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..100), 0..20)
    ) {
        let mut frames = vec![BytesFrame::BulkString(Bytes::from(name.clone()))];
        frames.extend(args.iter().map(|a| BytesFrame::BulkString(Bytes::from(a.clone()))));

        let frame = BytesFrame::Array(frames);
        let result = ParsedCommand::try_from(frame);

        prop_assert!(result.is_ok());
        let cmd = result.unwrap();
        prop_assert_eq!(cmd.name.as_ref(), name.as_bytes());
        prop_assert_eq!(cmd.args.len(), args.len());
    }

    /// Null elements in array should be filtered out.
    #[test]
    fn null_elements_filtered(name in "[A-Z]{1,20}") {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from(name.clone())),
            BytesFrame::Null,
            BytesFrame::BulkString(Bytes::from("arg1")),
            BytesFrame::Null,
            BytesFrame::BulkString(Bytes::from("arg2")),
        ]);

        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
        let cmd = result.unwrap();
        prop_assert_eq!(cmd.args.len(), 2); // Nulls filtered out
    }

    /// Array with only nulls after command name should have empty args.
    #[test]
    fn only_null_args(name in "[A-Z]{1,20}") {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from(name.clone())),
            BytesFrame::Null,
            BytesFrame::Null,
            BytesFrame::Null,
        ]);

        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
        let cmd = result.unwrap();
        prop_assert!(cmd.args.is_empty()); // All nulls filtered
    }

    /// Command name is uppercased correctly.
    #[test]
    fn name_uppercase_works(name in "[a-zA-Z]{1,20}") {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from(name.clone())),
        ]);

        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
        let cmd = result.unwrap();
        let upper = cmd.name_uppercase();
        let expected = name.to_uppercase();
        prop_assert_eq!(upper, expected.as_bytes());
    }

    /// Large number of arguments should not panic.
    #[test]
    fn many_arguments_dont_panic(
        name in "[A-Z]{1,10}",
        num_args in 0usize..500
    ) {
        let mut frames = vec![BytesFrame::BulkString(Bytes::from(name))];
        for i in 0..num_args {
            frames.push(BytesFrame::BulkString(Bytes::from(format!("arg{}", i))));
        }

        let frame = BytesFrame::Array(frames);
        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
        prop_assert_eq!(result.unwrap().args.len(), num_args);
    }

    /// Very large bulk strings should not panic.
    #[test]
    fn large_bulk_string_doesnt_panic(size in 0usize..100_000) {
        let data = vec![b'x'; size];
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("GET")),
            BytesFrame::BulkString(Bytes::from(data)),
        ]);

        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
    }

    /// Binary data in command name should parse.
    #[test]
    fn binary_command_name_parses(data: Vec<u8>) {
        prop_assume!(!data.is_empty()); // Empty would be different error

        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from(data.clone())),
        ]);

        let result = ParsedCommand::try_from(frame);
        prop_assert!(result.is_ok());
        let cmd = result.unwrap();
        prop_assert_eq!(cmd.name.as_ref(), &data[..]);
    }

    /// Deeply nested arrays in frame should not panic.
    #[test]
    fn deeply_nested_array_doesnt_panic(depth in 1usize..20) {
        let mut frame = BytesFrame::BulkString(Bytes::from("GET"));
        for _ in 0..depth {
            frame = BytesFrame::Array(vec![frame]);
        }

        let _: Result<ParsedCommand, ProtocolError> = ParsedCommand::try_from(frame);
    }

    /// Mixed frame types in array should not panic.
    #[test]
    fn mixed_frame_types_dont_panic(_dummy: bool) {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("GET")),
            BytesFrame::SimpleString(Bytes::from("simple")),
            BytesFrame::Integer(42),
            BytesFrame::Null,
            BytesFrame::BulkString(Bytes::from("bulk")),
            BytesFrame::Array(vec![BytesFrame::Integer(1)]),
        ]);

        let _: Result<ParsedCommand, ProtocolError> = ParsedCommand::try_from(frame);
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_simple_get_command() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("GET")),
            BytesFrame::BulkString(Bytes::from("mykey")),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"GET");
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0].as_ref(), b"mykey");
    }

    #[test]
    fn test_lowercase_command_name() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("get")),
            BytesFrame::BulkString(Bytes::from("mykey")),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"get");
        assert_eq!(cmd.name_uppercase(), b"GET");
    }

    #[test]
    fn test_command_with_no_args() {
        let frame = BytesFrame::Array(vec![BytesFrame::BulkString(Bytes::from("PING"))]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"PING");
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_set_command_with_options() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("SET")),
            BytesFrame::BulkString(Bytes::from("key")),
            BytesFrame::BulkString(Bytes::from("value")),
            BytesFrame::BulkString(Bytes::from("EX")),
            BytesFrame::BulkString(Bytes::from("60")),
            BytesFrame::BulkString(Bytes::from("NX")),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.name.as_ref(), b"SET");
        assert_eq!(cmd.args.len(), 5);
    }

    #[test]
    fn test_empty_bulk_string() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("SET")),
            BytesFrame::BulkString(Bytes::from("")),
            BytesFrame::BulkString(Bytes::from("value")),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.args.len(), 2);
        assert!(cmd.args[0].is_empty());
    }

    #[test]
    fn test_binary_key() {
        let binary_key: Vec<u8> = (0..=255).collect();
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from("GET")),
            BytesFrame::BulkString(Bytes::from(binary_key.clone())),
        ]);

        let cmd = ParsedCommand::try_from(frame).unwrap();
        assert_eq!(cmd.args[0].as_ref(), &binary_key[..]);
    }
}
