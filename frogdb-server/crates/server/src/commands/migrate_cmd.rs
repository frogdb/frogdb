//! MIGRATE command implementation.
//!
//! MIGRATE moves keys atomically from source to target Redis server.
//! Since this requires async network I/O and the command interface is synchronous,
//! this command is handled specially by the connection handler.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, ServerWideOp, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// MIGRATE command - move keys to another Redis instance.
///
/// Format: MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key...]
pub struct MigrateCommand;

impl Command for MigrateCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "MIGRATE",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE
                .union(CommandFlags::NOSCRIPT)
                .union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::UniformRW,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::ServerWide(ServerWideOp::Migrate),
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Executes via ConnectionHandler::dispatch_server_wide (handle_migrate
        // does its own parsing and async network I/O), never on a shard.
        // Reaching this shard-side executor is a routing regression (or a Lua
        // redis.call, which cannot perform the async migration) -- fail loudly
        // rather than leak an internal MigrateNeeded signal.
        Err(CommandError::Internal {
            message: "internal: server-wide command reached shard executor".to_string(),
        })
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key selection follows the shared MIGRATE grammar walker so the
        // dispatcher (slot validation, ACL checks, locking) always guards the
        // exact key set MigrateArgs::parse migrates.
        crate::migrate::key_positions(args)
            .into_iter()
            .map(|i| args[i].as_ref())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::MigrateArgs;

    /// Build a MIGRATE arg vector (everything after the command name) from
    /// string literals.
    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    /// Extract keys via the dispatcher path and return them as owned byte vecs
    /// for easy comparison.
    fn keys(parts: &[&str]) -> Vec<Vec<u8>> {
        let args = args(parts);
        MigrateCommand
            .dynamic_keys(&args)
            .into_iter()
            .map(|k| k.to_vec())
            .collect()
    }

    /// Helper: expected key set as owned byte vecs.
    fn expect(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn dynamic_keys_bare_single_key() {
        // Bare 5-arg single-key form: the positional key is the only key.
        assert_eq!(
            keys(&["127.0.0.1", "6380", "mykey", "0", "5000"]),
            expect(&["mykey"])
        );
    }

    #[test]
    fn dynamic_keys_empty_positional_no_keys_is_zero_keys() {
        // Empty positional and no KEYS clause -> no keys at all.
        assert_eq!(
            keys(&["127.0.0.1", "6380", "", "0", "5000"]),
            Vec::<Vec<u8>>::new()
        );
        // ... even with trailing options present.
        assert_eq!(
            keys(&["127.0.0.1", "6380", "", "0", "5000", "COPY", "REPLACE"]),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn dynamic_keys_empty_positional_with_keys_list() {
        // Empty single-key form ("" positional + KEYS list).
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "",
                "0",
                "5000",
                "KEYS",
                "k1",
                "k2",
                "k3"
            ]),
            expect(&["k1", "k2", "k3"])
        );
    }

    #[test]
    fn dynamic_keys_positional_plus_keys_list() {
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "firstkey",
                "0",
                "5000",
                "KEYS",
                "k1",
                "k2"
            ]),
            expect(&["firstkey", "k1", "k2"])
        );
    }

    #[test]
    fn dynamic_keys_key_literally_named_auth_in_tail() {
        // Adversarial: after KEYS, everything is a key -- including a key whose
        // literal value is "AUTH" or "AUTH2". These must NOT be treated as
        // options.
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "",
                "0",
                "5000",
                "KEYS",
                "AUTH",
                "AUTH2",
                "real"
            ]),
            expect(&["AUTH", "AUTH2", "real"])
        );
    }

    #[test]
    fn dynamic_keys_auth2_mid_args_before_keys() {
        // AUTH2 (3-wide) appears before KEYS; its username/password must be
        // skipped, not collected as keys.
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "AUTH2",
                "user",
                "pass",
                "KEYS",
                "k1",
            ]),
            expect(&["mykey", "k1"])
        );
    }

    #[test]
    fn dynamic_keys_auth_consumes_following_token() {
        // AUTH is 2-wide: it consumes the next token as a password even when
        // that token spells "KEYS", so no KEYS clause is recognized.
        assert_eq!(
            keys(&["127.0.0.1", "6380", "", "0", "5000", "AUTH", "KEYS"]),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn dynamic_keys_copy_replace_interleaving() {
        // COPY/REPLACE in any order, mixed with AUTH, then KEYS.
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "REPLACE",
                "COPY",
                "AUTH",
                "pw",
                "KEYS",
                "k1",
                "k2",
            ]),
            expect(&["mykey", "k1", "k2"])
        );
    }

    #[test]
    fn dynamic_keys_case_insensitive_options() {
        // Option tokens are matched case-insensitively.
        assert_eq!(
            keys(&[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "copy",
                "auth2",
                "u",
                "p",
                "keys",
                "k1",
            ]),
            expect(&["mykey", "k1"])
        );
    }

    #[test]
    fn dynamic_keys_too_few_args_no_positional() {
        // Fewer than 3 args -> no positional key can be read; no panic.
        assert_eq!(keys(&["127.0.0.1", "6380"]), Vec::<Vec<u8>>::new());
    }

    /// Differential test: whenever `MigrateArgs::parse` succeeds, the
    /// dispatcher's `dynamic_keys` must select exactly the same keys the
    /// executor migrates. This is the invariant the shared walker guarantees.
    #[test]
    fn dynamic_keys_matches_parse_keys() {
        let cases: &[&[&str]] = &[
            // Bare single-key form.
            &["127.0.0.1", "6380", "mykey", "0", "5000"],
            // Empty positional, zero keys.
            &["127.0.0.1", "6380", "", "0", "5000"],
            &["127.0.0.1", "6380", "", "0", "5000", "COPY"],
            &["127.0.0.1", "6380", "", "0", "5000", "COPY", "REPLACE"],
            // KEYS clause, empty positional.
            &[
                "127.0.0.1",
                "6380",
                "",
                "0",
                "5000",
                "KEYS",
                "k1",
                "k2",
                "k3",
            ],
            // KEYS clause, non-empty positional (positional first).
            &[
                "127.0.0.1",
                "6380",
                "first",
                "0",
                "5000",
                "KEYS",
                "k1",
                "k2",
            ],
            // AUTH (2-wide) before KEYS.
            &[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "AUTH",
                "pw",
                "KEYS",
                "k1",
            ],
            // AUTH2 (3-wide) before KEYS.
            &[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "AUTH2",
                "u",
                "p",
                "KEYS",
                "k1",
            ],
            // AUTH consuming a "KEYS"-spelled password.
            &["127.0.0.1", "6380", "", "0", "5000", "AUTH", "KEYS"],
            // Key literally named AUTH/AUTH2 in the tail.
            &[
                "127.0.0.1",
                "6380",
                "",
                "0",
                "5000",
                "KEYS",
                "AUTH",
                "AUTH2",
                "real",
            ],
            // COPY/REPLACE/AUTH interleaving then KEYS.
            &[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "REPLACE",
                "COPY",
                "AUTH",
                "pw",
                "KEYS",
                "k1",
                "k2",
            ],
            // Case-insensitive options.
            &[
                "127.0.0.1",
                "6380",
                "mykey",
                "0",
                "5000",
                "copy",
                "keys",
                "k1",
            ],
            // COPY/REPLACE with a non-empty positional, no KEYS.
            &["127.0.0.1", "6380", "mykey", "0", "5000", "COPY", "REPLACE"],
        ];

        for parts in cases {
            let args = args(parts);
            let parsed = MigrateArgs::parse(&args)
                .unwrap_or_else(|e| panic!("expected parse success for {parts:?}, got {e}"));

            let dispatcher_keys: Vec<Vec<u8>> = MigrateCommand
                .dynamic_keys(&args)
                .into_iter()
                .map(|k| k.to_vec())
                .collect();
            let executor_keys: Vec<Vec<u8>> = parsed.keys.iter().map(|k| k.to_vec()).collect();

            assert_eq!(
                dispatcher_keys, executor_keys,
                "dynamic_keys disagreed with parse keys for {parts:?}"
            );
        }
    }
}
