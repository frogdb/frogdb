//! FT.* search command definitions.
//!
//! ServerWide commands (FT.CREATE, FT.SEARCH, etc.) have stub execute() methods;
//! the actual logic lives in scatter handlers.
//!
//! Key-based commands (FT.SUGADD, FT.SUGGET, etc.) use Standard execution and
//! implement their logic directly in execute().

use bytes::Bytes;
use frogdb_commands::utils::get_or_create_hash;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, ExecutionStrategy, ServerWideOp,
    WalStrategy,
};
use frogdb_protocol::Response;

// =============================================================================
// FT.CREATE
// =============================================================================

/// FT.CREATE index ON HASH [PREFIX count prefix ...] SCHEMA field type [options] ...
pub struct FtCreateCommand;

impl Command for FtCreateCommand {
    fn name(&self) -> &'static str {
        "FT.CREATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // FT.CREATE idx ON HASH SCHEMA ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtCreate)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.ALTER
// =============================================================================

/// FT.ALTER index SCHEMA ADD field type [options] ...
pub struct FtAlterCommand;

impl Command for FtAlterCommand {
    fn name(&self) -> &'static str {
        "FT.ALTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // FT.ALTER idx SCHEMA ADD field type
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtAlter)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SEARCH
// =============================================================================

/// FT.SEARCH index query [NOCONTENT] [WITHSCORES] [LIMIT offset num]
///   [RETURN count field ...] [SORTBY field [ASC|DESC]]
pub struct FtSearchCommand;

impl Command for FtSearchCommand {
    fn name(&self) -> &'static str {
        "FT.SEARCH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // FT.SEARCH idx query
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSearch)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.DROPINDEX
// =============================================================================

/// FT.DROPINDEX index [DD]
pub struct FtDropIndexCommand;

impl Command for FtDropIndexCommand {
    fn name(&self) -> &'static str {
        "FT.DROPINDEX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // FT.DROPINDEX idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtDropIndex)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.INFO
// =============================================================================

/// FT.INFO index
pub struct FtInfoCommand;

impl Command for FtInfoCommand {
    fn name(&self) -> &'static str {
        "FT.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FT.INFO idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtInfo)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT._LIST
// =============================================================================

/// FT._LIST
pub struct FtListCommand;

impl Command for FtListCommand {
    fn name(&self) -> &'static str {
        "FT._LIST"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0) // FT._LIST
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtList)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SYNUPDATE
// =============================================================================

/// FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term1 term2 ...
pub struct FtSynupdateCommand;

impl Command for FtSynupdateCommand {
    fn name(&self) -> &'static str {
        "FT.SYNUPDATE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // FT.SYNUPDATE idx group_id term1
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSynupdate)
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::ok())
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SYNDUMP
// =============================================================================

/// FT.SYNDUMP index
pub struct FtSyndumpCommand;

impl Command for FtSyndumpCommand {
    fn name(&self) -> &'static str {
        "FT.SYNDUMP"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FT.SYNDUMP idx
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::ServerWide(ServerWideOp::FtSyndump)
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Handled via ServerWide dispatch
        Ok(Response::Array(vec![]))
    }

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// =============================================================================
// FT.SUGADD
// =============================================================================

const PAYLOAD_PREFIX: &[u8] = b"__pl__";

/// FT.SUGADD key string score [INCR] [PAYLOAD payload]
pub struct FtSugaddCommand;

impl Command for FtSugaddCommand {
    fn name(&self) -> &'static str {
        "FT.SUGADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // FT.SUGADD key string score
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() < 3 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGADD",
            });
        }

        let key = &args[0];
        let suggestion = std::str::from_utf8(&args[1]).map_err(|_| {
            CommandError::InvalidArgument {
                message: "invalid suggestion string".into(),
            }
        })?;
        let score: f64 = std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| CommandError::InvalidArgument {
                message: "invalid score".into(),
            })?;

        let mut incr = false;
        let mut payload: Option<&[u8]> = None;
        let mut i = 3;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"INCR" => {
                    incr = true;
                    i += 1;
                }
                b"PAYLOAD" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    payload = Some(&args[i + 1]);
                    i += 2;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        let hash = get_or_create_hash(ctx, key)?;

        let new_score = if incr {
            let existing: f64 = hash
                .get(suggestion.as_bytes())
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            existing + score
        } else {
            score
        };

        hash.set(
            Bytes::copy_from_slice(suggestion.as_bytes()),
            Bytes::copy_from_slice(new_score.to_string().as_bytes()),
        );

        if let Some(pl) = payload {
            let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + suggestion.len());
            pl_key.extend_from_slice(PAYLOAD_PREFIX);
            pl_key.extend_from_slice(suggestion.as_bytes());
            hash.set(Bytes::from(pl_key), Bytes::copy_from_slice(pl));
        }

        let count = hash
            .iter()
            .filter(|(k, _)| !k.starts_with(PAYLOAD_PREFIX))
            .count();
        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// FT.SUGGET
// =============================================================================

/// FT.SUGGET key prefix [FUZZY] [WITHSCORES] [WITHPAYLOADS] [MAX num]
pub struct FtSuggetCommand;

impl Command for FtSuggetCommand {
    fn name(&self) -> &'static str {
        "FT.SUGGET"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // FT.SUGGET key prefix
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGGET",
            });
        }

        let prefix = std::str::from_utf8(&args[1]).map_err(|_| {
            CommandError::InvalidArgument {
                message: "invalid prefix string".into(),
            }
        })?;

        let mut fuzzy = false;
        let mut with_scores = false;
        let mut with_payloads = false;
        let mut max_results: usize = 5;
        let mut i = 2;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"FUZZY" => {
                    fuzzy = true;
                    i += 1;
                }
                b"WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                b"WITHPAYLOADS" => {
                    with_payloads = true;
                    i += 1;
                }
                b"MAX" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    max_results = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or(CommandError::NotInteger)?;
                    i += 2;
                }
                _ => {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        let key = &args[0];
        let value = match ctx.store.get_with_expiry_check(key) {
            Some(v) => v,
            None => return Ok(Response::Null),
        };
        let hash = match value.as_hash() {
            Some(h) => h,
            None => return Ok(Response::Null),
        };

        let prefix_lower = prefix.to_lowercase();
        let mut matches: Vec<(String, f64, Option<Bytes>)> = Vec::new();

        for (k, v) in hash.iter() {
            if k.starts_with(PAYLOAD_PREFIX) {
                continue;
            }
            let suggestion = match std::str::from_utf8(k) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let suggestion_lower = suggestion.to_lowercase();

            let is_match = if fuzzy {
                suggestion_lower.starts_with(&prefix_lower)
                    || frogdb_search::suggest::levenshtein_distance(
                        &prefix_lower,
                        &suggestion_lower[..suggestion_lower.len().min(prefix_lower.len())],
                    ) <= 1
            } else {
                suggestion_lower.starts_with(&prefix_lower)
            };

            if is_match {
                let score: f64 = std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                let payload = if with_payloads {
                    let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + k.len());
                    pl_key.extend_from_slice(PAYLOAD_PREFIX);
                    pl_key.extend_from_slice(k);
                    hash.get(&pl_key).map(|v| Bytes::copy_from_slice(v))
                } else {
                    None
                };

                matches.push((suggestion.to_string(), score, payload));
            }
        }

        matches.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        matches.truncate(max_results);

        if matches.is_empty() {
            return Ok(Response::Null);
        }

        let mut result = Vec::new();
        for (suggestion, score, payload) in matches {
            result.push(Response::bulk(Bytes::copy_from_slice(
                suggestion.as_bytes(),
            )));
            if with_scores {
                result.push(Response::bulk(Bytes::copy_from_slice(
                    score.to_string().as_bytes(),
                )));
            }
            if with_payloads {
                match payload {
                    Some(pl) => result.push(Response::bulk(pl)),
                    None => result.push(Response::Null),
                }
            }
        }

        Ok(Response::Array(result))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// FT.SUGDEL
// =============================================================================

/// FT.SUGDEL key string
pub struct FtSugdelCommand;

impl Command for FtSugdelCommand {
    fn name(&self) -> &'static str {
        "FT.SUGDEL"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // FT.SUGDEL key string
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::NoOp
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "FT.SUGDEL",
            });
        }

        let key = &args[0];
        let suggestion = &args[1];

        let hash = match ctx.store.get_mut(key) {
            Some(value) => match value.as_hash_mut() {
                Some(h) => h,
                None => return Ok(Response::Integer(0)),
            },
            None => return Ok(Response::Integer(0)),
        };

        let removed = hash.remove(suggestion.as_ref());

        // Also remove any associated payload
        let mut pl_key = Vec::with_capacity(PAYLOAD_PREFIX.len() + suggestion.len());
        pl_key.extend_from_slice(PAYLOAD_PREFIX);
        pl_key.extend_from_slice(suggestion);
        hash.remove(pl_key.as_slice());

        Ok(Response::Integer(if removed { 1 } else { 0 }))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// =============================================================================
// FT.SUGLEN
// =============================================================================

/// FT.SUGLEN key
pub struct FtSuglenCommand;

impl Command for FtSuglenCommand {
    fn name(&self) -> &'static str {
        "FT.SUGLEN"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FT.SUGLEN key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity {
                command: "FT.SUGLEN",
            });
        }

        let key = &args[0];
        let value = match ctx.store.get_with_expiry_check(key) {
            Some(v) => v,
            None => return Ok(Response::Integer(0)),
        };
        let hash = match value.as_hash() {
            Some(h) => h,
            None => return Ok(Response::Integer(0)),
        };

        let count = hash
            .iter()
            .filter(|(k, _)| !k.starts_with(PAYLOAD_PREFIX))
            .count();
        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
